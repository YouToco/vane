package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/server/types"
)

const profileEpochCheckpointSchema = 1

type profileEpochProjection struct {
	Industry              string   `json:"industry"`
	Occupation            string   `json:"occupation"`
	Tags                  []string `json:"tags"`
	RemovedTags           []string `json:"removed_tags"`
	Summary               string   `json:"summary"`
	LastEvolvedFeedbackID int64    `json:"last_evolved_feedback_id"`
}

type profileEpochCheckpointPayload struct {
	Schema             int                      `json:"schema"`
	TenantID           int64                    `json:"tenant_id"`
	UserID             int64                    `json:"user_id"`
	ProfileEpoch       int64                    `json:"profile_epoch"`
	StateVersion       int64                    `json:"state_version"`
	EvidenceGeneration int64                    `json:"evidence_generation"`
	ClaimHighWater     int64                    `json:"claim_high_water"`
	EventHighWater     int64                    `json:"event_high_water"`
	Projection         profileEpochProjection   `json:"projection"`
	Claims             []profileClaimCheckpoint `json:"claims"`
	Events             []profileEventCheckpoint `json:"events"`
	PreviousAnchor     string                   `json:"previous_anchor,omitempty"`
}

type profileClaimCheckpoint struct {
	ID            int64   `json:"id"`
	Field         string  `json:"field"`
	Value         string  `json:"value"`
	SourceState   string  `json:"source_state"`
	SourceRefType *string `json:"source_ref_type,omitempty"`
	SourceRef     *string `json:"source_ref,omitempty"`
	Generation    int64   `json:"generation"`
	SupersedesID  *int64  `json:"supersedes_id,omitempty"`
}

type profileEventCheckpoint struct {
	ID            int64  `json:"id"`
	Kind          string `json:"kind"`
	TargetClaimID *int64 `json:"target_claim_id,omitempty"`
	ResultClaimID *int64 `json:"result_claim_id,omitempty"`
	TargetEventID *int64 `json:"target_event_id,omitempty"`
}

type profileEpochCreator struct {
	ID                            int64
	Kind                          string
	PredecessorEpoch              int64
	CompensatedResetEventID       *int64
	FeedbackBoundaryID            int64
	CheckpointID                  *int64
	PredecessorClaimHighWater     int64
	PredecessorEventHighWater     int64
	PredecessorEvidenceGeneration int64
	PredecessorRemovedTags        []string
	PredecessorClaimLedgerDigest  string
	PredecessorEventLedgerDigest  string
	PredecessorProjectionDigest   string
	ResultProjectionDigest        string
}

type profileEpochInitial struct {
	FeedbackCursor     int64
	ClaimHighWater     int64
	EventHighWater     int64
	EvidenceGeneration int64
	Version            int64
	ProjectionDigest   string
}

// ApplyProfileEpochAction is the reset/restore linearization point. Receipt
// replay precedes epoch/version comparison so a response-loss retry remains
// exact even after later transitions.
func (s *Store) ApplyProfileEpochAction(
	ctx context.Context,
	tenantID, userID int64,
	action types.ProfileEpochAction,
	idempotencyKey, requestDigest string,
) (*types.ProfileEpochActionResult, error) {
	if tenantID <= 0 || userID <= 0 || action.ExpectedEpoch < 0 ||
		action.ExpectedVersion < 0 || idempotencyKey == "" ||
		requestDigest == "" {
		return nil, types.NewAppError(
			types.CodeValidation, "画像学习操作范围或幂等凭据无效", nil)
	}
	if (action.Action != "reset" && action.Action != "restore") ||
		(action.Action == "reset" && action.Scope != "history_learning") ||
		(action.Action == "restore" && action.Scope != "") {
		return nil, types.NewAppError(types.CodeValidation, "画像学习操作无效", nil)
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, profileClaimDBError("begin profile epoch transition", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	// Feedback producers take this process-wide admission before tenant and
	// subject fences. Taking it here first preserves the total order.
	if _, err := tx.Exec(ctx,
		`SELECT pg_advisory_xact_lock($1,$2)
		   /* profile epoch action admission */`,
		agentSessionFactAdmissionClass, agentSessionFactAdmissionKey,
	); err != nil {
		return nil, profileClaimDBError("lock feedback admission", err)
	}
	exists, err := lockTenantAdmissionRoot(ctx, tx, tenantID)
	if err != nil {
		return nil, profileClaimDBError("lock profile epoch tenant admission", err)
	}
	if !exists {
		return nil, types.NewAppError(types.CodeNotFound, "租户不存在", nil)
	}
	if err := lockExactProfileMembershipRoot(ctx, tx, tenantID, userID); err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx,
		`SELECT set_config('app.tenant_id',$1,true),
		        set_config('app.user_id',$2,true)`,
		strconv.FormatInt(tenantID, 10), strconv.FormatInt(userID, 10),
	); err != nil {
		return nil, profileClaimDBError("set profile epoch subject", err)
	}
	if _, err := tx.Exec(ctx, `SET LOCAL ROLE vane_profile_epoch_editor`); err != nil {
		return nil, profileClaimDBError("enter profile epoch editor role", err)
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO profile_feedback_epoch_fences
		    (tenant_id,user_id,last_feedback_id)
		 VALUES($1,$2,0) ON CONFLICT (tenant_id,user_id) DO NOTHING`,
		tenantID, userID,
	); err != nil {
		return nil, profileClaimDBError("initialize profile feedback fence", err)
	}
	var feedbackBoundary int64
	if err := tx.QueryRow(ctx,
		`SELECT last_feedback_id FROM profile_feedback_epoch_fences
		  WHERE tenant_id=$1 AND user_id=$2 FOR UPDATE`,
		tenantID, userID,
	).Scan(&feedbackBoundary); err != nil {
		return nil, profileClaimDBError("lock profile feedback epoch fence", err)
	}
	p, err := lockProfileTx(ctx, tx, tenantID, userID)
	if err != nil {
		return nil, err
	}
	if result, found, err := replayProfileEpochActionTx(
		ctx, tx, tenantID, userID, idempotencyKey, requestDigest,
	); err != nil {
		return nil, err
	} else if found {
		if err := tx.Commit(ctx); err != nil {
			return nil, profileClaimDBError("commit profile epoch replay", err)
		}
		return result, nil
	}
	version, generation, epoch, err := lockProfileClaimStateTx(
		ctx, tx, tenantID, userID)
	if err != nil {
		return nil, err
	}
	if epoch != action.ExpectedEpoch || version != action.ExpectedVersion {
		return nil, types.NewAppError(
			types.CodeConflict, "画像 epoch 或版本已变化，请刷新后重试", nil)
	}
	if err := bindProfileEpochTx(ctx, tx, epoch+1); err != nil {
		return nil, err
	}
	var result *types.ProfileEpochActionResult
	switch action.Action {
	case "reset":
		result, err = resetProfileEpochTx(
			ctx, tx, tenantID, userID, p, epoch, version, generation,
			feedbackBoundary)
	case "restore":
		result, err = restoreProfileEpochTx(
			ctx, tx, tenantID, userID, p, epoch, version, generation,
			feedbackBoundary)
	}
	if err != nil {
		return nil, err
	}
	payload, err := json.Marshal(result)
	if err != nil {
		return nil, types.NewAppError(
			types.CodeInternal, "画像学习回执编码失败", err)
	}
	eventID, err := strconv.ParseInt(result.EventID, 10, 64)
	if err != nil {
		return nil, types.NewAppError(
			types.CodeInternal, "画像学习事件标识损坏", err)
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO profile_epoch_receipts
		    (tenant_id,user_id,idempotency_key,request_digest,event_id,response_payload)
		 VALUES($1,$2,$3,$4,$5,$6)`,
		tenantID, userID, idempotencyKey, requestDigest, eventID, payload,
	); err != nil {
		return nil, profileClaimDBError("insert profile epoch receipt", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, profileClaimDBError("commit profile epoch transition", err)
	}
	return result, nil
}

func resetProfileEpochTx(
	ctx context.Context, tx pgx.Tx,
	tenantID, userID int64, p *types.Profile,
	epoch, version, generation, feedbackBoundary int64,
) (*types.ProfileEpochActionResult, error) {
	claims, events, err := loadProfileClaimLedgerTx(
		ctx, tx, tenantID, userID, epoch)
	if err != nil {
		return nil, err
	}
	cursor, err := lockedProfileCursorTx(ctx, tx, tenantID, userID)
	if err != nil {
		return nil, err
	}
	oldProjection := projectProfileClaims(claims, events, generation)
	// The stored projection is authority-derived. A mismatch is corruption,
	// not a reason to silently checkpoint guessed state.
	if !profileMatchesProjection(p, oldProjection) {
		return nil, types.NewAppError(
			types.CodeConflict, "画像投影与主张账本不一致，拒绝重置", nil)
	}
	claimLedgerDigest, eventLedgerDigest, err :=
		digestProfileClaimLedger(claims, events)
	if err != nil {
		return nil, err
	}
	p.LastEvolvedFeedbackID = cursor
	checkpointID, oldDigest, err := appendProfileEpochCheckpointTx(
		ctx, tx, tenantID, userID, epoch, version, generation, p, claims, events)
	if err != nil {
		return nil, err
	}
	newEpoch := epoch + 1
	if _, err := tx.Exec(ctx,
		`INSERT INTO profile_epochs(tenant_id,user_id,profile_epoch)
		 VALUES($1,$2,$3)`, tenantID, userID, newEpoch); err != nil {
		return nil, profileClaimDBError("insert reset result epoch", err)
	}
	carried := resetCarryClaims(claims, events, generation)
	if err := insertCarriedProfileClaimsTx(
		ctx, tx, tenantID, userID, newEpoch, epoch, carried); err != nil {
		return nil, err
	}
	newClaims, _, err := loadProfileClaimLedgerTx(
		ctx, tx, tenantID, userID, newEpoch)
	if err != nil {
		return nil, err
	}
	newProjection := projectProfileClaims(newClaims, nil, 0)
	if activeSummaryRunes(newClaims, newProjection) > maxProjectedSummaryRunes ||
		len(newProjection.tags) > maxProfileTags {
		return nil, types.NewAppError(
			types.CodeValidation, "重置后人工画像超过摘要或标签上限", nil)
	}
	resultProjection := profileFromClaimProjection(
		newProjection, append([]string{}, p.RemovedTags...), feedbackBoundary)
	resultDigest, _, err := digestProfileEpochProjection(resultProjection)
	if err != nil {
		return nil, err
	}
	claimHighWater := maxProfileClaimID(newClaims)
	if err := initializeProfileEpochTx(
		ctx, tx, tenantID, userID, newEpoch, feedbackBoundary,
		claimHighWater, 0, 0, version+1, resultDigest); err != nil {
		return nil, err
	}
	var eventID int64
	err = tx.QueryRow(ctx,
		`INSERT INTO profile_epoch_events (
		   tenant_id,user_id,actor_user_id,event_kind,
		   predecessor_epoch,result_epoch,expected_version,result_version,
		   predecessor_claim_high_water,predecessor_event_high_water,
		   predecessor_feedback_cursor,predecessor_evidence_generation,
		   predecessor_removed_tags,feedback_boundary_id,checkpoint_id,
		   predecessor_claim_ledger_digest,predecessor_event_ledger_digest,
		   predecessor_projection_digest,result_projection_digest
		 ) VALUES (
		   $1,$2,$2,'reset',$3::bigint,$3::bigint+1,
		   $4::bigint,$4::bigint+1,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15
		 ) RETURNING id`,
		tenantID, userID, epoch, version, maxProfileClaimID(claims),
		maxProfileClaimEventID(events), cursor, generation, p.RemovedTags,
		feedbackBoundary, checkpointID, claimLedgerDigest, eventLedgerDigest,
		oldDigest, resultDigest,
	).Scan(&eventID)
	if err != nil {
		return nil, profileClaimDBError("insert profile reset event", err)
	}
	if err := switchProfileEpochStateTx(
		ctx, tx, tenantID, userID, epoch, version, newEpoch, 0); err != nil {
		return nil, err
	}
	updated, err := writeExactProfileEpochProjectionTx(
		ctx, tx, tenantID, userID, resultProjection)
	if err != nil {
		return nil, err
	}
	return &types.ProfileEpochActionResult{
		Action: "reset", ProfileEpoch: newEpoch, Version: version + 1,
		EventID: strconv.FormatInt(eventID, 10), Profile: publicProfile(updated),
		RestoreAllowed: true,
	}, nil
}

func restoreProfileEpochTx(
	ctx context.Context, tx pgx.Tx,
	tenantID, userID int64, p *types.Profile,
	epoch, version, generation, feedbackBoundary int64,
) (*types.ProfileEpochActionResult, error) {
	allowed, creator, initial, err := profileRestoreStateTx(
		ctx, tx, tenantID, userID, epoch, version, true)
	if err != nil {
		return nil, err
	}
	if !allowed || creator.Kind != "reset" {
		return nil, types.NewAppError(
			types.CodeConflict, "当前画像已经产生新学习，不能恢复", nil)
	}
	cursor, err := lockedProfileCursorTx(ctx, tx, tenantID, userID)
	if err != nil {
		return nil, err
	}
	p.LastEvolvedFeedbackID = cursor
	currentClaims, currentEvents, err := loadProfileClaimLedgerTx(
		ctx, tx, tenantID, userID, epoch)
	if err != nil {
		return nil, err
	}
	checkpointID, currentDigest, err := appendProfileEpochCheckpointTx(
		ctx, tx, tenantID, userID, epoch, version, generation,
		p, currentClaims, currentEvents)
	if err != nil {
		return nil, err
	}
	if currentDigest != initial.ProjectionDigest ||
		currentDigest != creator.ResultProjectionDigest {
		return nil, types.NewAppError(
			types.CodeConflict, "当前 reset 投影身份不一致，拒绝恢复", nil)
	}
	_, predecessorClaims, predecessorEvents, err :=
		loadAndVerifyCheckpointTx(
			ctx, tx, tenantID, userID, creator.CheckpointID,
			creator.PredecessorEpoch, creator.PredecessorClaimHighWater,
			creator.PredecessorEventHighWater,
			creator.PredecessorEvidenceGeneration,
			creator.PredecessorRemovedTags,
			creator.PredecessorClaimLedgerDigest,
			creator.PredecessorEventLedgerDigest,
			creator.PredecessorProjectionDigest)
	if err != nil {
		return nil, err
	}
	rawProjection := projectProfileClaims(
		predecessorClaims, predecessorEvents,
		creator.PredecessorEvidenceGeneration)
	restoredProjection := profileFromClaimProjection(
		rawProjection,
		append([]string{}, creator.PredecessorRemovedTags...),
		creator.FeedbackBoundaryID)
	restoredDigest, _, err := digestProfileEpochProjection(restoredProjection)
	if err != nil {
		return nil, err
	}
	if restoredDigest != creator.PredecessorProjectionDigest {
		return nil, types.NewAppError(
			types.CodeConflict, "原始画像账本无法重放到 reset 前投影", nil)
	}
	newEpoch := epoch + 1
	if _, err := tx.Exec(ctx,
		`INSERT INTO profile_epochs(tenant_id,user_id,profile_epoch)
		 VALUES($1,$2,$3)`, tenantID, userID, newEpoch); err != nil {
		return nil, profileClaimDBError("insert restore result epoch", err)
	}
	if err := insertRestoredProfileClaimsTx(
		ctx, tx, tenantID, userID, newEpoch, creator.PredecessorEpoch,
		predecessorClaims, rawProjection); err != nil {
		return nil, err
	}
	newClaims, _, err := loadProfileClaimLedgerTx(
		ctx, tx, tenantID, userID, newEpoch)
	if err != nil {
		return nil, err
	}
	if err := initializeProfileEpochTx(
		ctx, tx, tenantID, userID, newEpoch, creator.FeedbackBoundaryID,
		maxProfileClaimID(newClaims), 0, creator.PredecessorEvidenceGeneration,
		version+1, restoredDigest); err != nil {
		return nil, err
	}
	currentClaimLedgerDigest, currentEventLedgerDigest, err :=
		digestProfileClaimLedger(currentClaims, currentEvents)
	if err != nil {
		return nil, err
	}
	var eventID int64
	err = tx.QueryRow(ctx,
		`INSERT INTO profile_epoch_events (
		   tenant_id,user_id,actor_user_id,event_kind,
		   predecessor_epoch,result_epoch,compensated_reset_event_id,
		   expected_version,result_version,
		   predecessor_claim_high_water,predecessor_event_high_water,
		   predecessor_feedback_cursor,predecessor_evidence_generation,
		   predecessor_removed_tags,feedback_boundary_id,checkpoint_id,
		   predecessor_claim_ledger_digest,predecessor_event_ledger_digest,
		   predecessor_projection_digest,result_projection_digest
		 ) VALUES (
		   $1,$2,$2,'restore',$3::bigint,$3::bigint+1,$4,
		   $5::bigint,$5::bigint+1,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16
		 ) RETURNING id`,
		tenantID, userID, epoch, creator.ID, version,
		maxProfileClaimID(currentClaims), maxProfileClaimEventID(currentEvents),
		cursor, generation, p.RemovedTags, creator.FeedbackBoundaryID,
		checkpointID, currentClaimLedgerDigest, currentEventLedgerDigest,
		currentDigest, restoredDigest,
	).Scan(&eventID)
	if err != nil {
		return nil, profileClaimDBError("insert profile restore event", err)
	}
	if err := switchProfileEpochStateTx(
		ctx, tx, tenantID, userID, epoch, version, newEpoch,
		creator.PredecessorEvidenceGeneration); err != nil {
		return nil, err
	}
	updated, err := writeExactProfileEpochProjectionTx(
		ctx, tx, tenantID, userID, restoredProjection)
	if err != nil {
		return nil, err
	}
	_ = feedbackBoundary // the original reset boundary is authoritative.
	return &types.ProfileEpochActionResult{
		Action: "restore", ProfileEpoch: newEpoch, Version: version + 1,
		EventID: strconv.FormatInt(eventID, 10), Profile: publicProfile(updated),
		RestoreAllowed: false,
	}, nil
}

func profileRestoreAllowedTx(
	ctx context.Context, tx pgx.Tx,
	tenantID, userID, epoch, version int64,
) (bool, error) {
	var transitionSchemaAvailable bool
	if err := tx.QueryRow(ctx,
		`SELECT to_regclass('public.profile_epoch_events') IS NOT NULL`,
	).Scan(&transitionSchemaAvailable); err != nil {
		return false, profileClaimDBError(
			"check profile epoch transition schema", err)
	}
	if !transitionSchemaAvailable {
		// A current binary may finish a read while the safely reversible 067
		// migration has rolled back. Phase A has no restore capability.
		return false, nil
	}
	allowed, _, _, err := profileRestoreStateTx(
		ctx, tx, tenantID, userID, epoch, version, false)
	return allowed, err
}

func profileRestoreStateTx(
	ctx context.Context, tx pgx.Tx,
	tenantID, userID, epoch, version int64, locked bool,
) (bool, profileEpochCreator, profileEpochInitial, error) {
	var creator profileEpochCreator
	err := tx.QueryRow(ctx,
		`SELECT e.id,e.event_kind,e.predecessor_epoch,
		        e.compensated_reset_event_id,e.feedback_boundary_id,
		        e.checkpoint_id,e.predecessor_claim_high_water,
		        e.predecessor_event_high_water,
		        e.predecessor_evidence_generation,e.predecessor_removed_tags,
		        e.predecessor_claim_ledger_digest,
		        e.predecessor_event_ledger_digest,
		        e.predecessor_projection_digest,
		        e.result_projection_digest
		   FROM profile_epoch_events e
		  WHERE e.tenant_id=$1 AND e.user_id=$2 AND e.result_epoch=$3`,
		tenantID, userID, epoch,
	).Scan(
		&creator.ID, &creator.Kind, &creator.PredecessorEpoch,
		&creator.CompensatedResetEventID, &creator.FeedbackBoundaryID,
		&creator.CheckpointID, &creator.PredecessorClaimHighWater,
		&creator.PredecessorEventHighWater,
		&creator.PredecessorEvidenceGeneration, &creator.PredecessorRemovedTags,
		&creator.PredecessorClaimLedgerDigest,
		&creator.PredecessorEventLedgerDigest,
		&creator.PredecessorProjectionDigest,
		&creator.ResultProjectionDigest,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, creator, profileEpochInitial{}, nil
	}
	if err != nil {
		return false, creator, profileEpochInitial{},
			profileClaimDBError("read current profile epoch creator", err)
	}
	if creator.Kind != "reset" || creator.CompensatedResetEventID != nil {
		return false, creator, profileEpochInitial{}, nil
	}
	var compensated bool
	if err := tx.QueryRow(ctx,
		`SELECT EXISTS(
		   SELECT 1 FROM profile_epoch_events
		    WHERE tenant_id=$1 AND user_id=$2
		      AND event_kind='restore' AND compensated_reset_event_id=$3
		 )`, tenantID, userID, creator.ID).Scan(&compensated); err != nil {
		return false, creator, profileEpochInitial{},
			profileClaimDBError("check reset compensation", err)
	}
	if compensated {
		return false, creator, profileEpochInitial{}, nil
	}
	var initial profileEpochInitial
	err = tx.QueryRow(ctx,
		`SELECT initial_feedback_cursor,initial_claim_high_water,
		        initial_event_high_water,initial_evidence_generation,
		        initial_version,initial_projection_digest
		   FROM profile_epochs
		  WHERE tenant_id=$1 AND user_id=$2 AND profile_epoch=$3`,
		tenantID, userID, epoch,
	).Scan(
		&initial.FeedbackCursor, &initial.ClaimHighWater,
		&initial.EventHighWater, &initial.EvidenceGeneration,
		&initial.Version, &initial.ProjectionDigest,
	)
	if err != nil {
		return false, creator, initial,
			profileClaimDBError("read current epoch pristine identity", err)
	}
	if version != initial.Version {
		return false, creator, initial, nil
	}
	var (
		cursor, generation, claimHigh, eventHigh int64
		feedbackCount, activityCount             int64
	)
	lockClause := ""
	if locked {
		lockClause = " FOR SHARE"
	}
	if err := tx.QueryRow(ctx,
		`SELECT last_evolved_feedback_id FROM profiles
		  WHERE tenant_id=$1 AND user_id=$2`+lockClause,
		tenantID, userID).Scan(&cursor); err != nil {
		return false, creator, initial,
			profileClaimDBError("read current epoch cursor", err)
	}
	if err := tx.QueryRow(ctx,
		`SELECT evidence_generation FROM profile_claim_states
		  WHERE tenant_id=$1 AND user_id=$2`,
		tenantID, userID).Scan(&generation); err != nil {
		return false, creator, initial,
			profileClaimDBError("read current epoch generation", err)
	}
	if err := tx.QueryRow(ctx,
		`SELECT COALESCE(max(id),0) FROM profile_claims
		  WHERE tenant_id=$1 AND user_id=$2 AND profile_epoch=$3`,
		tenantID, userID, epoch).Scan(&claimHigh); err != nil {
		return false, creator, initial,
			profileClaimDBError("read current epoch claim watermark", err)
	}
	if err := tx.QueryRow(ctx,
		`SELECT COALESCE(max(id),0) FROM profile_claim_events
		  WHERE tenant_id=$1 AND user_id=$2 AND profile_epoch=$3`,
		tenantID, userID, epoch).Scan(&eventHigh); err != nil {
		return false, creator, initial,
			profileClaimDBError("read current epoch event watermark", err)
	}
	if err := tx.QueryRow(ctx,
		`SELECT count(*) FROM feedbacks
		  WHERE tenant_id=$1 AND user_id=$2 AND profile_epoch=$3`,
		tenantID, userID, epoch).Scan(&feedbackCount); err != nil {
		return false, creator, initial,
			profileClaimDBError("check current epoch feedback", err)
	}
	var activitiesAvailable bool
	if err := tx.QueryRow(ctx,
		`SELECT to_regclass('public.profile_epoch_activities') IS NOT NULL`,
	).Scan(&activitiesAvailable); err != nil {
		return false, creator, initial,
			profileClaimDBError("check epoch activity capability", err)
	}
	if activitiesAvailable {
		if err := tx.QueryRow(ctx,
			`SELECT count(profile_epoch) FROM profile_epoch_activities
			  WHERE tenant_id=$1 AND user_id=$2 AND profile_epoch=$3`,
			tenantID, userID, epoch).Scan(&activityCount); err != nil {
			return false, creator, initial,
				profileClaimDBError("check current epoch activity", err)
		}
	}
	if cursor != initial.FeedbackCursor ||
		generation != initial.EvidenceGeneration ||
		claimHigh != initial.ClaimHighWater ||
		eventHigh != initial.EventHighWater ||
		feedbackCount != 0 || activityCount != 0 {
		return false, creator, initial, nil
	}
	var projection profileEpochProjection
	err = tx.QueryRow(ctx,
		`SELECT industry,occupation,tags,removed_tags,summary,
		        last_evolved_feedback_id
		   FROM profiles WHERE tenant_id=$1 AND user_id=$2`,
		tenantID, userID,
	).Scan(
		&projection.Industry, &projection.Occupation, &projection.Tags,
		&projection.RemovedTags, &projection.Summary,
		&projection.LastEvolvedFeedbackID)
	if err != nil {
		return false, creator, initial,
			profileClaimDBError("read current epoch projection", err)
	}
	digest, _, err := digestProfileEpochProjection(projection)
	if err != nil {
		return false, creator, initial, err
	}
	return digest == initial.ProjectionDigest, creator, initial, nil
}

func appendProfileEpochCheckpointTx(
	ctx context.Context, tx pgx.Tx,
	tenantID, userID, epoch, version, generation int64,
	p *types.Profile, claims []profileClaimRow, events []profileClaimEventRow,
) (int64, string, error) {
	projection := profileEpochProjection{
		Industry: p.Industry, Occupation: p.Occupation,
		Tags:        append([]string{}, p.Tags...),
		RemovedTags: append([]string{}, p.RemovedTags...),
		Summary:     p.Summary, LastEvolvedFeedbackID: p.LastEvolvedFeedbackID,
	}
	projectionDigest, _, err := digestProfileEpochProjection(projection)
	if err != nil {
		return 0, "", err
	}
	previousAnchor := ""
	err = tx.QueryRow(ctx,
		`SELECT c.anchor_digest
		   FROM profile_epoch_events e
		   JOIN profile_epoch_checkpoints c
		     ON c.tenant_id=e.tenant_id AND c.user_id=e.user_id
		    AND c.id=e.checkpoint_id
		  WHERE e.tenant_id=$1 AND e.user_id=$2
		  ORDER BY e.id DESC LIMIT 1`,
		tenantID, userID).Scan(&previousAnchor)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return 0, "", profileClaimDBError("read previous epoch anchor", err)
	}
	payload := profileEpochCheckpointPayload{
		Schema: profileEpochCheckpointSchema, TenantID: tenantID, UserID: userID,
		ProfileEpoch: epoch, StateVersion: version,
		EvidenceGeneration: generation,
		ClaimHighWater:     maxProfileClaimID(claims),
		EventHighWater:     maxProfileClaimEventID(events),
		Projection:         projection, PreviousAnchor: previousAnchor,
		Claims: make([]profileClaimCheckpoint, 0, len(claims)),
		Events: make([]profileEventCheckpoint, 0, len(events)),
	}
	for _, claim := range claims {
		payload.Claims = append(payload.Claims, profileClaimCheckpoint{
			ID: claim.ID, Field: claim.Field, Value: claim.Value,
			SourceState: claim.SourceState, SourceRefType: claim.SourceRefType,
			SourceRef: claim.SourceRef, Generation: claim.Generation,
			SupersedesID: claim.SupersedesID,
		})
	}
	for _, event := range events {
		payload.Events = append(payload.Events, profileEventCheckpoint{
			ID: event.ID, Kind: event.Kind, TargetClaimID: event.TargetClaimID,
			ResultClaimID: event.ResultClaimID, TargetEventID: event.TargetEventID,
		})
	}
	canonical, err := json.Marshal(payload)
	if err != nil {
		return 0, "", types.NewAppError(
			types.CodeInternal, "画像 checkpoint 编码失败", err)
	}
	anchorInput := append(append([]byte{}, canonical...), []byte(previousAnchor)...)
	anchorSum := sha256.Sum256(anchorInput)
	anchor := hex.EncodeToString(anchorSum[:])
	var checkpointID int64
	err = tx.QueryRow(ctx,
		`INSERT INTO profile_epoch_checkpoints (
		   tenant_id,user_id,profile_epoch,schema_version,state_version,
		   evidence_generation,claim_high_water,event_high_water,feedback_cursor,
		   canonical_payload,projection_digest,previous_anchor_digest,anchor_digest
		 ) VALUES ($1,$2,$3,1,$4,$5,$6,$7,$8,$9,$10,NULLIF($11,''),$12)
		 RETURNING id`,
		tenantID, userID, epoch, version, generation,
		payload.ClaimHighWater, payload.EventHighWater,
		projection.LastEvolvedFeedbackID, canonical, projectionDigest,
		previousAnchor, anchor,
	).Scan(&checkpointID)
	if err != nil {
		return 0, "", profileClaimDBError("insert profile epoch checkpoint", err)
	}
	return checkpointID, projectionDigest, nil
}

func loadAndVerifyCheckpointTx(
	ctx context.Context, tx pgx.Tx,
	tenantID, userID int64, checkpointID *int64, epoch, claimHighWater,
	eventHighWater, evidenceGeneration int64, removedTags []string,
	expectedClaimLedgerDigest, expectedEventLedgerDigest, expectedDigest string,
) (profileEpochCheckpointPayload, []profileClaimRow, []profileClaimEventRow, error) {
	var canonical []byte
	var storedDigest, previousAnchor, anchor string
	err := pgx.ErrNoRows
	if checkpointID != nil {
		err = tx.QueryRow(ctx,
			`SELECT canonical_payload,projection_digest,
			        COALESCE(previous_anchor_digest,''),anchor_digest
			   FROM profile_epoch_checkpoints
			  WHERE tenant_id=$1 AND user_id=$2 AND id=$3 AND profile_epoch=$4`,
			tenantID, userID, *checkpointID, epoch,
		).Scan(&canonical, &storedDigest, &previousAnchor, &anchor)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return profileEpochCheckpointPayload{}, nil, nil,
				profileClaimDBError("read profile epoch checkpoint", err)
		}
	}
	// Checkpoint absence/corruption is recoverable: raw immutable ledger is the
	// fact source. Transition identity remains mandatory and is checked below.
	var payload profileEpochCheckpointPayload
	checkpointValid := err == nil
	if checkpointValid {
		sum := sha256.Sum256(append(append([]byte{}, canonical...), []byte(previousAnchor)...))
		checkpointValid = hex.EncodeToString(sum[:]) == anchor &&
			storedDigest == expectedDigest &&
			json.Unmarshal(canonical, &payload) == nil &&
			payload.Schema == profileEpochCheckpointSchema &&
			payload.ProfileEpoch == epoch
	}
	if checkpointValid {
		checkpointValid = payload.EvidenceGeneration == evidenceGeneration &&
			payload.ClaimHighWater == claimHighWater &&
			payload.EventHighWater == eventHighWater
	}
	claims, events, err := loadProfileClaimLedgerTx(
		ctx, tx, tenantID, userID, epoch)
	if err != nil {
		return payload, nil, nil, err
	}
	if maxProfileClaimID(claims) != claimHighWater ||
		maxProfileClaimEventID(events) != eventHighWater {
		return payload, nil, nil, types.NewAppError(
			types.CodeConflict, "画像原始账本水位与 transition 身份不一致", nil)
	}
	claimLedgerDigest, eventLedgerDigest, digestErr :=
		digestProfileClaimLedger(claims, events)
	if digestErr != nil {
		return payload, nil, nil, digestErr
	}
	if claimLedgerDigest != expectedClaimLedgerDigest ||
		eventLedgerDigest != expectedEventLedgerDigest {
		return payload, nil, nil, types.NewAppError(
			types.CodeConflict, "画像原始账本完整性与 transition 身份不一致", nil)
	}
	rawProjection := projectProfileClaims(claims, events, evidenceGeneration)
	raw := profileFromClaimProjection(rawProjection, removedTags, 0)
	digest, _, digestErr := digestProfileEpochProjection(raw)
	if digestErr != nil {
		return payload, nil, nil, digestErr
	}
	if digest != expectedDigest {
		return payload, nil, nil, types.NewAppError(
			types.CodeConflict, "画像原始账本或 transition 身份损坏", nil)
	}
	if !checkpointValid {
		payload = profileEpochCheckpointPayload{
			Schema: profileEpochCheckpointSchema, TenantID: tenantID,
			UserID: userID, ProfileEpoch: epoch,
			EvidenceGeneration: evidenceGeneration,
			ClaimHighWater:     claimHighWater, EventHighWater: eventHighWater,
			Projection: raw,
		}
	}
	return payload, claims, events, nil
}

func resetCarryClaims(
	claims []profileClaimRow, events []profileClaimEventRow, generation int64,
) []profileClaimRow {
	projection := projectProfileClaims(claims, events, generation)
	byID := make(map[int64]profileClaimRow, len(claims))
	for _, claim := range claims {
		byID[claim.ID] = claim
	}
	carry := make(map[int64]bool)
	for _, claim := range claims {
		if !projection.active[claim.ID] {
			continue
		}
		if (claim.SourceState == "manual" && claim.SupersedesID == nil) ||
			claim.SourceState == "manual" || projection.pinned[claim.ID] {
			carry[claim.ID] = true
		}
	}
	out := make([]profileClaimRow, 0, len(carry))
	for id := range carry {
		out = append(out, byID[id])
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Field != out[j].Field {
			return out[i].Field < out[j].Field
		}
		if out[i].Value != out[j].Value {
			return out[i].Value < out[j].Value
		}
		return out[i].ID < out[j].ID
	})
	return dedupeProfileClaims(out)
}

func activeClaimsForProjection(
	claims []profileClaimRow, projection profileClaimProjection,
) []profileClaimRow {
	out := make([]profileClaimRow, 0)
	for _, claim := range claims {
		if projection.active[claim.ID] {
			out = append(out, claim)
		}
	}
	return dedupeProfileClaims(out)
}

func dedupeProfileClaims(in []profileClaimRow) []profileClaimRow {
	seen := make(map[string]bool)
	out := make([]profileClaimRow, 0, len(in))
	for _, claim := range in {
		key := profileClaimSemanticKey(claim)
		if !seen[key] {
			seen[key] = true
			out = append(out, claim)
		}
	}
	return out
}

func insertCarriedProfileClaimsTx(
	ctx context.Context, tx pgx.Tx,
	tenantID, userID, newEpoch, fromEpoch int64,
	claims []profileClaimRow,
) error {
	if err := validateProfileEpochMaterializedClaims(claims); err != nil {
		return err
	}
	for _, claim := range claims {
		// A carried fact is a new manual authority root with explicit lineage;
		// old events remain in their inactive epoch and never become revocable.
		if _, err := tx.Exec(ctx,
			`INSERT INTO profile_claims (
			   tenant_id,user_id,profile_epoch,field_name,claim_value,
			   source_state,carried_from_epoch,carried_from_claim_id
			 ) VALUES ($1,$2,$3,$4,$5,'manual',$6,$7)`,
			tenantID, userID, newEpoch, claim.Field, claim.Value,
			fromEpoch, claim.ID,
		); err != nil {
			return profileClaimDBError("insert carried profile claim", err)
		}
	}
	return nil
}

func insertRestoredProfileClaimsTx(
	ctx context.Context, tx pgx.Tx,
	tenantID, userID, newEpoch, fromEpoch int64,
	claims []profileClaimRow, projection profileClaimProjection,
) error {
	activeClaims := activeClaimsForProjection(claims, projection)
	if err := validateProfileEpochMaterializedClaims(activeClaims); err != nil {
		return err
	}
	for _, claim := range activeClaims {
		sourceState := claim.SourceState
		sourceRefType, sourceRef := claim.SourceRefType, claim.SourceRef
		generation := claim.Generation
		if claim.SourceState == "manual" || projection.pinned[claim.ID] {
			sourceState = "manual"
			sourceRefType, sourceRef = nil, nil
			generation = 0
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO profile_claims (
			   tenant_id,user_id,profile_epoch,field_name,claim_value,
			   source_state,source_ref_type,source_ref,generation,
			   carried_from_epoch,carried_from_claim_id
			 ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
			tenantID, userID, newEpoch, claim.Field, claim.Value,
			sourceState, sourceRefType, sourceRef, generation,
			fromEpoch, claim.ID,
		); err != nil {
			return profileClaimDBError("insert restored profile claim", err)
		}
	}
	return nil
}

func initializeProfileEpochTx(
	ctx context.Context, tx pgx.Tx,
	tenantID, userID, epoch, cursor, claimHigh, eventHigh,
	generation, version int64, digest string,
) error {
	tag, err := tx.Exec(ctx,
		`UPDATE profile_epochs
		    SET initial_feedback_cursor=$4,initial_claim_high_water=$5,
		        initial_event_high_water=$6,initial_evidence_generation=$7,
		        initial_version=$8,initial_projection_digest=$9
		  WHERE tenant_id=$1 AND user_id=$2 AND profile_epoch=$3
		    AND initial_projection_digest IS NULL`,
		tenantID, userID, epoch, cursor, claimHigh, eventHigh,
		generation, version, digest)
	if err != nil {
		return profileClaimDBError("initialize profile epoch identity", err)
	}
	if tag.RowsAffected() != 1 {
		return types.NewAppError(types.CodeConflict, "画像 epoch 已被初始化", nil)
	}
	return nil
}

func switchProfileEpochStateTx(
	ctx context.Context, tx pgx.Tx,
	tenantID, userID, oldEpoch, oldVersion, newEpoch, generation int64,
) error {
	tag, err := tx.Exec(ctx,
		`UPDATE profile_claim_states
		    SET active_epoch=$5,version=version+1,
		        evidence_generation=$6,updated_at=clock_timestamp()
		  WHERE tenant_id=$1 AND user_id=$2
		    AND active_epoch=$3 AND version=$4`,
		tenantID, userID, oldEpoch, oldVersion, newEpoch, generation)
	if err != nil {
		return profileClaimDBError("switch active profile epoch", err)
	}
	if tag.RowsAffected() != 1 {
		return types.NewAppError(
			types.CodeConflict, "画像 epoch 或版本已变化，请刷新后重试", nil)
	}
	return nil
}

func writeExactProfileEpochProjectionTx(
	ctx context.Context, tx pgx.Tx,
	tenantID, userID int64, projection profileEpochProjection,
) (*types.Profile, error) {
	var p types.Profile
	err := scanProfileEdit(tx.QueryRow(ctx,
		`UPDATE profiles
		    SET industry=$3,occupation=$4,tags=$5,removed_tags=$6,
		        summary=$7,last_evolved_feedback_id=$8,
		        updated_at=GREATEST(clock_timestamp(),updated_at+interval '1 microsecond')
		  WHERE tenant_id=$1 AND user_id=$2
		  RETURNING `+profileEditColumns,
		tenantID, userID, projection.Industry, projection.Occupation,
		projection.Tags, projection.RemovedTags, projection.Summary,
		projection.LastEvolvedFeedbackID,
	), &p)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, types.NewAppError(types.CodeNotFound, "画像不存在", nil)
	}
	if err != nil {
		return nil, profileClaimDBError("write exact profile epoch projection", err)
	}
	return &p, nil
}

func lockedProfileCursorTx(
	ctx context.Context, tx pgx.Tx, tenantID, userID int64,
) (int64, error) {
	var cursor int64
	if err := tx.QueryRow(ctx,
		`SELECT last_evolved_feedback_id FROM profiles
		  WHERE tenant_id=$1 AND user_id=$2`,
		tenantID, userID).Scan(&cursor); err != nil {
		return 0, profileClaimDBError("read locked profile cursor", err)
	}
	return cursor, nil
}

func profileFromClaimProjection(
	p profileClaimProjection, removed []string, cursor int64,
) profileEpochProjection {
	if p.tags == nil {
		p.tags = []string{}
	}
	if removed == nil {
		removed = []string{}
	}
	return profileEpochProjection{
		Industry: p.industry, Occupation: p.occupation,
		Tags: p.tags, RemovedTags: removed, Summary: p.summary,
		LastEvolvedFeedbackID: cursor,
	}
}

func profileMatchesProjection(p *types.Profile, projection profileClaimProjection) bool {
	expected := profileFromClaimProjection(
		projection, append([]string{}, p.RemovedTags...), p.LastEvolvedFeedbackID)
	actual := profileEpochProjection{
		Industry: p.Industry, Occupation: p.Occupation,
		Tags:        append([]string{}, p.Tags...),
		RemovedTags: append([]string{}, p.RemovedTags...),
		Summary:     p.Summary, LastEvolvedFeedbackID: p.LastEvolvedFeedbackID,
	}
	expectedDigest, _, err1 := digestProfileEpochProjection(expected)
	actualDigest, _, err2 := digestProfileEpochProjection(actual)
	return err1 == nil && err2 == nil && expectedDigest == actualDigest
}

func digestProfileEpochProjection(
	projection profileEpochProjection,
) (string, []byte, error) {
	if projection.Tags == nil {
		projection.Tags = []string{}
	}
	if projection.RemovedTags == nil {
		projection.RemovedTags = []string{}
	}
	// Cursor is transition/pristine metadata, not business projection identity.
	// Restore intentionally advances it to the reset fence boundary.
	businessProjection := struct {
		Industry    string   `json:"industry"`
		Occupation  string   `json:"occupation"`
		Tags        []string `json:"tags"`
		RemovedTags []string `json:"removed_tags"`
		Summary     string   `json:"summary"`
	}{
		Industry: projection.Industry, Occupation: projection.Occupation,
		Tags: projection.Tags, RemovedTags: projection.RemovedTags,
		Summary: projection.Summary,
	}
	canonical, err := json.Marshal(businessProjection)
	if err != nil {
		return "", nil, types.NewAppError(
			types.CodeInternal, "画像投影编码失败", err)
	}
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:]), canonical, nil
}

func digestProfileClaimLedger(
	claims []profileClaimRow,
	events []profileClaimEventRow,
) (string, string, error) {
	type canonicalClaim struct {
		ID                 int64   `json:"id"`
		Field              string  `json:"field"`
		Value              string  `json:"value"`
		SourceState        string  `json:"source_state"`
		SourceRefType      *string `json:"source_ref_type,omitempty"`
		SourceRef          *string `json:"source_ref,omitempty"`
		Generation         int64   `json:"generation"`
		SupersedesID       *int64  `json:"supersedes_id,omitempty"`
		CarriedFromEpoch   *int64  `json:"carried_from_epoch,omitempty"`
		CarriedFromClaimID *int64  `json:"carried_from_claim_id,omitempty"`
		CreatedAt          string  `json:"created_at"`
	}
	type canonicalEvent struct {
		ID              int64  `json:"id"`
		Kind            string `json:"kind"`
		TargetClaimID   *int64 `json:"target_claim_id,omitempty"`
		ResultClaimID   *int64 `json:"result_claim_id,omitempty"`
		TargetEventID   *int64 `json:"target_event_id,omitempty"`
		ActorUserID     int64  `json:"actor_user_id"`
		ExpectedVersion int64  `json:"expected_version"`
		ResultVersion   int64  `json:"result_version"`
		CreatedAt       string `json:"created_at"`
	}
	canonicalClaims := make([]canonicalClaim, 0, len(claims))
	for _, claim := range claims {
		canonicalClaims = append(canonicalClaims, canonicalClaim{
			ID: claim.ID, Field: claim.Field, Value: claim.Value,
			SourceState: claim.SourceState, SourceRefType: claim.SourceRefType,
			SourceRef: claim.SourceRef, Generation: claim.Generation,
			SupersedesID:       claim.SupersedesID,
			CarriedFromEpoch:   claim.CarriedFromEpoch,
			CarriedFromClaimID: claim.CarriedFromClaimID,
			CreatedAt:          claim.CreatedAt.UTC().Format(time.RFC3339Nano),
		})
	}
	canonicalEvents := make([]canonicalEvent, 0, len(events))
	for _, event := range events {
		canonicalEvents = append(canonicalEvents, canonicalEvent{
			ID: event.ID, Kind: event.Kind,
			TargetClaimID:   event.TargetClaimID,
			ResultClaimID:   event.ResultClaimID,
			TargetEventID:   event.TargetEventID,
			ActorUserID:     event.ActorUserID,
			ExpectedVersion: event.ExpectedVersion,
			ResultVersion:   event.ResultVersion,
			CreatedAt:       event.CreatedAt.UTC().Format(time.RFC3339Nano),
		})
	}
	claimJSON, err := json.Marshal(canonicalClaims)
	if err != nil {
		return "", "", types.NewAppError(
			types.CodeInternal, "画像主张账本摘要编码失败", err)
	}
	eventJSON, err := json.Marshal(canonicalEvents)
	if err != nil {
		return "", "", types.NewAppError(
			types.CodeInternal, "画像事件账本摘要编码失败", err)
	}
	claimSum, eventSum := sha256.Sum256(claimJSON), sha256.Sum256(eventJSON)
	return hex.EncodeToString(claimSum[:]), hex.EncodeToString(eventSum[:]), nil
}

func validateProfileEpochMaterializedClaims(claims []profileClaimRow) error {
	for _, claim := range claims {
		limit := 0
		switch claim.Field {
		case "industry", "occupation":
			limit = 200
		case "tag":
			limit = 20
		case "summary":
			limit = maxSummaryClaimRunes
		default:
			return types.NewAppError(
				types.CodeConflict, "画像账本包含未知字段，拒绝 epoch 转换", nil)
		}
		if utf8.RuneCountInString(claim.Value) > limit {
			return types.NewAppError(
				types.CodeValidation, "画像账本单条主张超过字段长度上限", nil)
		}
	}
	return nil
}

func maxProfileClaimID(claims []profileClaimRow) int64 {
	var maxID int64
	for _, claim := range claims {
		if claim.ID > maxID {
			maxID = claim.ID
		}
	}
	return maxID
}

func replayProfileEpochActionTx(
	ctx context.Context, tx pgx.Tx,
	tenantID, userID int64, key, digest string,
) (*types.ProfileEpochActionResult, bool, error) {
	var storedDigest string
	var payload []byte
	err := tx.QueryRow(ctx,
		`SELECT request_digest,response_payload
		   FROM profile_epoch_receipts
		  WHERE tenant_id=$1 AND user_id=$2 AND idempotency_key=$3`,
		tenantID, userID, key,
	).Scan(&storedDigest, &payload)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, profileClaimDBError("read profile epoch receipt", err)
	}
	if storedDigest != digest {
		return nil, false, types.NewAppError(
			types.CodeConflict, "Idempotency-Key 已用于另一画像学习请求", nil)
	}
	var result types.ProfileEpochActionResult
	if err := json.Unmarshal(payload, &result); err != nil {
		return nil, false, types.NewAppError(
			types.CodeInternal, "画像学习回执损坏", err)
	}
	return &result, true, nil
}

func (c profileEpochCreator) String() string {
	return fmt.Sprintf("%s:%d->%d", c.Kind, c.PredecessorEpoch, c.PredecessorEpoch+1)
}
