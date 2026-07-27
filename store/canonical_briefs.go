package store

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/types"
)

const canonicalBriefLockNamespaceV1 = "vane/canonical-brief/v1/"
const canonicalBriefLockSeedV1 = int64(0x42524631)

const runOutcomeColumnsV1 = `id,tenant_id,user_id,task_id,run_snapshot_id,
	schema_version,status,result,source_coverage,processing,failure_code,
	failure_message,finalized_at,outcome_digest,created_at`

type runOutcomeRowV1 struct {
	ID             int64
	TenantID       int64
	UserID         int64
	TaskID         string
	RunSnapshotID  int64
	SchemaVersion  string
	Status         string
	Result         sql.NullString
	SourceCoverage sql.NullString
	Processing     sql.NullString
	FailureCode    string
	FailureMessage string
	FinalizedAt    sql.NullTime
	OutcomeDigest  sql.NullString
}

func scanRunOutcomeRowV1(row pgx.Row) (runOutcomeRowV1, error) {
	var stored runOutcomeRowV1
	var createdAt sql.NullTime
	err := row.Scan(
		&stored.ID, &stored.TenantID, &stored.UserID, &stored.TaskID,
		&stored.RunSnapshotID, &stored.SchemaVersion, &stored.Status,
		&stored.Result, &stored.SourceCoverage, &stored.Processing,
		&stored.FailureCode, &stored.FailureMessage, &stored.FinalizedAt,
		&stored.OutcomeDigest, &createdAt,
	)
	return stored, err
}

func (r runOutcomeRowV1) marker() types.RunOutcomeMarkerV1 {
	return types.RunOutcomeMarkerV1{
		ID: r.ID, SchemaVersion: r.SchemaVersion,
		RunSnapshotID: r.RunSnapshotID, TenantID: r.TenantID,
		UserID: r.UserID, TaskID: r.TaskID,
	}
}

func (r runOutcomeRowV1) finalized() (types.RunOutcomeV1, error) {
	if r.Status != "finalized" || !r.Result.Valid ||
		!r.SourceCoverage.Valid || !r.Processing.Valid ||
		!r.FinalizedAt.Valid || !r.OutcomeDigest.Valid {
		return types.RunOutcomeV1{}, canonicalBriefIntegrityError()
	}
	outcome := types.RunOutcomeV1{
		RunOutcomeMarkerV1: r.marker(),
		Result:             types.RunResultV1(r.Result.String),
		SourceCoverage:     types.RunCompletenessV1(r.SourceCoverage.String),
		Processing:         types.RunCompletenessV1(r.Processing.String),
		FailureCode:        r.FailureCode,
		FailureMessage:     r.FailureMessage,
		FinalizedAt:        r.FinalizedAt.Time.Round(0).UTC().Truncate(time.Microsecond),
		Digest:             r.OutcomeDigest.String,
	}
	if err := outcome.Validate(); err != nil {
		return types.RunOutcomeV1{}, canonicalBriefIntegrityError()
	}
	return outcome, nil
}

// CreatePendingRunOutcomeV1 creates or recovers the one pending marker owned
// by an exact immutable run. P1-A intentionally has no production caller.
func (s *Store) CreatePendingRunOutcomeV1(
	ctx context.Context,
	expected types.RunIdentity,
	ref types.RunSnapshotRef,
) (types.RunOutcomeMarkerV1, error) {
	tx, err := s.beginCanonicalBriefTxV1(ctx, expected, ref)
	if err != nil {
		return types.RunOutcomeMarkerV1{}, err
	}
	defer rollbackCompiledTaskTx(ctx, tx)
	if err := lockCanonicalBriefRunV1(ctx, tx, ref.SnapshotID); err != nil {
		return types.RunOutcomeMarkerV1{}, err
	}

	stored, found, err := loadRunOutcomeForSnapshotV1(
		ctx, tx, ref.SnapshotID, false)
	if err != nil {
		return types.RunOutcomeMarkerV1{}, err
	}
	if found {
		marker := stored.marker()
		if err := marker.Validate(); err != nil ||
			marker.TenantID != expected.TenantID ||
			marker.UserID != expected.UserID ||
			marker.TaskID != expected.TaskID {
			return types.RunOutcomeMarkerV1{}, canonicalBriefIntegrityError()
		}
		if err := commitCanonicalBriefTxV1(ctx, tx, "recover run outcome marker"); err != nil {
			return types.RunOutcomeMarkerV1{}, err
		}
		return marker, nil
	}

	stored, err = scanRunOutcomeRowV1(tx.QueryRow(ctx,
		`INSERT INTO task_run_outcomes (
		    tenant_id,user_id,task_id,run_snapshot_id,schema_version
		 ) VALUES ($1,$2,$3,$4,$5)
		 RETURNING `+runOutcomeColumnsV1,
		expected.TenantID, expected.UserID, expected.TaskID,
		ref.SnapshotID, types.RunOutcomeSchemaVersionV1,
	))
	if err != nil {
		return types.RunOutcomeMarkerV1{},
			canonicalBriefDatabaseError("create run outcome marker", err)
	}
	marker := stored.marker()
	if err := marker.Validate(); err != nil {
		return types.RunOutcomeMarkerV1{}, canonicalBriefIntegrityError()
	}
	if err := commitCanonicalBriefTxV1(ctx, tx, "commit run outcome marker"); err != nil {
		return types.RunOutcomeMarkerV1{}, err
	}
	return marker, nil
}

// FinalizeRunOutcomeV1 performs the pending->finalized CAS. An exact replay
// returns the stored immutable outcome; a different terminal claim conflicts.
func (s *Store) FinalizeRunOutcomeV1(
	ctx context.Context,
	expected types.RunIdentity,
	ref types.RunSnapshotRef,
	outcome types.RunOutcomeV1,
) (types.RunOutcomeV1, error) {
	if err := outcome.Validate(); err != nil ||
		outcome.RunSnapshotID != ref.SnapshotID ||
		outcome.TenantID != expected.TenantID ||
		outcome.UserID != expected.UserID ||
		outcome.TaskID != expected.TaskID {
		return types.RunOutcomeV1{},
			canonicalBriefValidationError("run outcome finalization is invalid")
	}
	tx, err := s.beginCanonicalBriefTxV1(ctx, expected, ref)
	if err != nil {
		return types.RunOutcomeV1{}, err
	}
	defer rollbackCompiledTaskTx(ctx, tx)
	if err := lockCanonicalBriefRunV1(ctx, tx, ref.SnapshotID); err != nil {
		return types.RunOutcomeV1{}, err
	}
	stored, found, err := loadRunOutcomeForSnapshotV1(
		ctx, tx, ref.SnapshotID, true)
	if err != nil {
		return types.RunOutcomeV1{}, err
	}
	if !found || stored.ID != outcome.ID {
		return types.RunOutcomeV1{},
			canonicalBriefNotFoundError("run outcome marker is unavailable")
	}
	if stored.Status == "finalized" {
		replayed, err := stored.finalized()
		if err != nil {
			return types.RunOutcomeV1{}, err
		}
		if replayed.Digest != outcome.Digest {
			return types.RunOutcomeV1{},
				canonicalBriefConflictError("run outcome already finalized differently")
		}
		if err := commitCanonicalBriefTxV1(ctx, tx, "replay run outcome finalization"); err != nil {
			return types.RunOutcomeV1{}, err
		}
		return replayed, nil
	}
	if stored.Status != "pending" {
		return types.RunOutcomeV1{}, canonicalBriefIntegrityError()
	}

	tag, err := tx.Exec(ctx,
		`UPDATE task_run_outcomes
		    SET status='finalized',result=$2,source_coverage=$3,
		        processing=$4,failure_code=$5,failure_message=$6,
		        finalized_at=$7,outcome_digest=$8
		  WHERE id=$1 AND status='pending'`,
		outcome.ID, outcome.Result, outcome.SourceCoverage,
		outcome.Processing, outcome.FailureCode, outcome.FailureMessage,
		outcome.FinalizedAt, outcome.Digest,
	)
	if err != nil {
		return types.RunOutcomeV1{},
			canonicalBriefDatabaseError("finalize run outcome", err)
	}
	if tag.RowsAffected() != 1 {
		return types.RunOutcomeV1{},
			canonicalBriefConflictError("run outcome finalization lost its CAS")
	}
	if err := commitCanonicalBriefTxV1(ctx, tx, "commit run outcome finalization"); err != nil {
		return types.RunOutcomeV1{}, err
	}
	return outcome, nil
}

// FreezeBriefV1 persists one immutable, channel-neutral whole-Brief snapshot.
// The exact finalized content outcome, compiled batch, and complete set of
// delivery IDs are verified before the payload is admitted.
func (s *Store) FreezeBriefV1(
	ctx context.Context,
	expected types.RunIdentity,
	ref types.RunSnapshotRef,
	draft types.BriefDraftV1,
) (types.BriefV1, error) {
	canonical, err := draft.Canonical()
	if err != nil ||
		canonical.RunSnapshotID != ref.SnapshotID ||
		canonical.TenantID != expected.TenantID ||
		canonical.UserID != expected.UserID ||
		canonical.TaskID != expected.TaskID {
		return types.BriefV1{},
			canonicalBriefValidationError("brief draft is invalid")
	}
	requestDigest, err := canonical.RequestDigest()
	if err != nil {
		return types.BriefV1{},
			canonicalBriefValidationError("brief request cannot be sealed")
	}
	tx, err := s.beginCanonicalBriefTxV1(ctx, expected, ref)
	if err != nil {
		return types.BriefV1{}, err
	}
	defer rollbackCompiledTaskTx(ctx, tx)
	if err := lockCanonicalBriefRunV1(ctx, tx, ref.SnapshotID); err != nil {
		return types.BriefV1{}, err
	}

	outcomeRow, found, err := loadRunOutcomeForSnapshotV1(
		ctx, tx, ref.SnapshotID, false)
	if err != nil {
		return types.BriefV1{}, err
	}
	if !found || outcomeRow.ID != canonical.RunOutcomeID {
		return types.BriefV1{},
			canonicalBriefNotFoundError("content run outcome is unavailable")
	}
	outcome, err := outcomeRow.finalized()
	if err != nil {
		return types.BriefV1{}, err
	}
	if outcome.Result != types.RunResultContent {
		return types.BriefV1{},
			canonicalBriefConflictError("only a content outcome may own a brief")
	}

	if existing, found, err := loadBriefForOutcomeV1(
		ctx, tx, canonical.RunOutcomeID); err != nil {
		return types.BriefV1{}, err
	} else if found {
		if existing.requestDigest != requestDigest {
			return types.BriefV1{},
				canonicalBriefConflictError("brief already frozen differently")
		}
		if err := commitCanonicalBriefTxV1(ctx, tx, "replay frozen brief"); err != nil {
			return types.BriefV1{}, err
		}
		return existing.brief, nil
	}

	var briefState string
	err = tx.QueryRow(ctx,
		`SELECT brief_state
		   FROM push_batches
		  WHERE id=$1 AND tenant_id=$2 AND user_id=$3
		    AND schedule_id=$4 AND run_snapshot_id=$5
		  FOR UPDATE`,
		canonical.PushBatchID, expected.TenantID, expected.UserID,
		expected.TaskID, ref.SnapshotID,
	).Scan(&briefState)
	if errors.Is(err, pgx.ErrNoRows) {
		return types.BriefV1{},
			canonicalBriefNotFoundError("compiled brief batch is unavailable")
	}
	if err != nil {
		return types.BriefV1{},
			canonicalBriefDatabaseError("lock compiled brief batch", err)
	}
	if briefState != "open" {
		return types.BriefV1{},
			canonicalBriefConflictError("compiled brief batch is already sealed")
	}
	if err := verifyBriefDeliveriesV1(ctx, tx, canonical); err != nil {
		return types.BriefV1{}, err
	}
	tag, err := tx.Exec(ctx,
		`UPDATE push_batches SET brief_state='sealed'
		  WHERE id=$1 AND brief_state='open'`, canonical.PushBatchID)
	if err != nil {
		return types.BriefV1{},
			canonicalBriefDatabaseError("seal compiled brief batch", err)
	}
	if tag.RowsAffected() != 1 {
		return types.BriefV1{},
			canonicalBriefConflictError("compiled brief batch seal lost its CAS")
	}

	var briefID int64
	if err := tx.QueryRow(
		ctx, `SELECT nextval('brief_snapshots_id_seq')`).Scan(&briefID); err != nil {
		return types.BriefV1{},
			canonicalBriefDatabaseError("allocate brief id", err)
	}
	brief, err := canonical.Seal(briefID)
	if err != nil {
		return types.BriefV1{}, canonicalBriefIntegrityError()
	}
	payload, err := json.Marshal(brief)
	if err != nil {
		return types.BriefV1{}, canonicalBriefIntegrityError()
	}
	_, err = tx.Exec(ctx,
		`INSERT INTO brief_snapshots (
		    id,tenant_id,user_id,task_id,run_outcome_id,run_snapshot_id,
		    push_batch_id,schema_version,request_digest,payload_digest,
		    payload,insight_count,generated_at
		 ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`,
		brief.ID, brief.TenantID, brief.UserID, brief.TaskID,
		brief.RunOutcomeID, brief.RunSnapshotID, brief.PushBatchID,
		brief.SchemaVersion, requestDigest, brief.Digest, payload,
		len(brief.Insights), brief.GeneratedAt,
	)
	if err != nil {
		return types.BriefV1{},
			canonicalBriefDatabaseError("freeze canonical brief", err)
	}
	if err := commitCanonicalBriefTxV1(ctx, tx, "commit canonical brief"); err != nil {
		return types.BriefV1{}, err
	}
	return brief, nil
}

// LoadBriefV1 reads the exact immutable Brief behind the same tenant/user/run
// boundary. P1-D may build a separate read model; P1-A keeps this dark.
func (s *Store) LoadBriefV1(
	ctx context.Context,
	expected types.RunIdentity,
	ref types.RunSnapshotRef,
) (types.BriefV1, bool, error) {
	tx, err := s.beginCanonicalBriefTxV1(ctx, expected, ref)
	if err != nil {
		return types.BriefV1{}, false, err
	}
	defer rollbackCompiledTaskTx(ctx, tx)
	stored, found, err := loadBriefForSnapshotV1(ctx, tx, ref.SnapshotID)
	if err != nil {
		return types.BriefV1{}, false, err
	}
	if err := commitCanonicalBriefTxV1(ctx, tx, "load canonical brief"); err != nil {
		return types.BriefV1{}, false, err
	}
	if !found {
		return types.BriefV1{}, false, nil
	}
	return stored.brief, true, nil
}

func (s *Store) beginCanonicalBriefTxV1(
	ctx context.Context,
	expected types.RunIdentity,
	ref types.RunSnapshotRef,
) (pgx.Tx, error) {
	if _, err := validateTaskRunSnapshotReferenceForExpectedV1(
		ref, expected); err != nil || ref.SnapshotID <= 0 {
		return nil, canonicalBriefValidationError(
			"canonical brief run reference is invalid")
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, canonicalBriefDatabaseError(
			"begin canonical brief transaction", err)
	}
	if _, err := tx.Exec(
		ctx, `SET LOCAL search_path=pg_catalog,public,pg_temp`); err != nil {
		rollbackCompiledTaskTx(ctx, tx)
		return nil, canonicalBriefDatabaseError(
			"pin canonical brief search path", err)
	}
	exists, err := lockTenantAdmissionRoot(ctx, tx, expected.TenantID)
	if err != nil {
		rollbackCompiledTaskTx(ctx, tx)
		return nil, canonicalBriefDatabaseError(
			"lock canonical brief tenant admission", err)
	}
	if !exists {
		rollbackCompiledTaskTx(ctx, tx)
		return nil, canonicalBriefNotFoundError("canonical brief tenant is unavailable")
	}
	if _, err := tx.Exec(ctx,
		`SELECT set_config('app.tenant_id',$1,true)`,
		strconv.FormatInt(expected.TenantID, 10)); err != nil {
		rollbackCompiledTaskTx(ctx, tx)
		return nil, canonicalBriefDatabaseError(
			"set canonical brief tenant", err)
	}
	if _, err := tx.Exec(ctx,
		`SELECT set_config('app.user_id',$1,true)`,
		strconv.FormatInt(expected.UserID, 10)); err != nil {
		rollbackCompiledTaskTx(ctx, tx)
		return nil, canonicalBriefDatabaseError(
			"set canonical brief user", err)
	}
	if _, err := tx.Exec(ctx, `SET LOCAL ROLE vane_brief_writer`); err != nil {
		rollbackCompiledTaskTx(ctx, tx)
		return nil, canonicalBriefDatabaseError(
			"enter canonical brief writer role", err)
	}
	var exact bool
	err = tx.QueryRow(ctx,
		`SELECT true
		   FROM task_run_snapshots
		  WHERE id=$1 AND tenant_id=$2 AND user_id=$3 AND task_id=$4
		    AND temporal_workflow_id=$5 AND temporal_run_id=$6
		    AND reference_schema_version=$7
		    AND reference_digest=$8 AND payload_digest=$9`,
		ref.SnapshotID, expected.TenantID, expected.UserID, expected.TaskID,
		expected.TemporalWorkflowID, expected.TemporalRunID,
		ref.SchemaVersion, ref.ReferenceDigest, ref.PayloadDigest,
	).Scan(&exact)
	if errors.Is(err, pgx.ErrNoRows) || !exact {
		rollbackCompiledTaskTx(ctx, tx)
		return nil, canonicalBriefNotFoundError(
			"canonical brief run snapshot is unavailable")
	}
	if err != nil {
		rollbackCompiledTaskTx(ctx, tx)
		return nil, canonicalBriefDatabaseError(
			"lock canonical brief run snapshot", err)
	}
	return tx, nil
}

func lockCanonicalBriefRunV1(
	ctx context.Context, tx pgx.Tx, snapshotID int64,
) error {
	key := canonicalBriefLockNamespaceV1 + strconv.FormatInt(snapshotID, 10)
	if _, err := tx.Exec(ctx,
		`SELECT pg_advisory_xact_lock(hashtextextended($1,$2))`,
		key, canonicalBriefLockSeedV1); err != nil {
		return canonicalBriefDatabaseError("lock canonical brief run", err)
	}
	return nil
}

func loadRunOutcomeForSnapshotV1(
	ctx context.Context, tx pgx.Tx, snapshotID int64, forUpdate bool,
) (runOutcomeRowV1, bool, error) {
	query := `SELECT ` + runOutcomeColumnsV1 + `
	    FROM task_run_outcomes WHERE run_snapshot_id=$1`
	if forUpdate {
		query += ` FOR UPDATE`
	}
	stored, err := scanRunOutcomeRowV1(tx.QueryRow(ctx, query, snapshotID))
	if errors.Is(err, pgx.ErrNoRows) {
		return runOutcomeRowV1{}, false, nil
	}
	if err != nil {
		return runOutcomeRowV1{}, false,
			canonicalBriefDatabaseError("load run outcome", err)
	}
	return stored, true, nil
}

type storedBriefV1 struct {
	brief         types.BriefV1
	requestDigest string
}

func loadBriefForOutcomeV1(
	ctx context.Context, tx pgx.Tx, outcomeID int64,
) (storedBriefV1, bool, error) {
	return loadBriefV1(ctx, tx,
		`FROM brief_snapshots WHERE run_outcome_id=$1`, outcomeID)
}

func loadBriefForSnapshotV1(
	ctx context.Context, tx pgx.Tx, snapshotID int64,
) (storedBriefV1, bool, error) {
	return loadBriefV1(ctx, tx,
		`FROM brief_snapshots WHERE run_snapshot_id=$1`, snapshotID)
}

func loadBriefV1(
	ctx context.Context, tx pgx.Tx, predicate string, argument int64,
) (storedBriefV1, bool, error) {
	var (
		id, tenantID, userID, outcomeID, snapshotID, batchID int64
		taskID, schemaVersion, requestDigest, payloadDigest  string
		payload                                              []byte
		insightCount                                         int
		generatedAt                                          sql.NullTime
	)
	err := tx.QueryRow(ctx,
		`SELECT id,tenant_id,user_id,task_id,run_outcome_id,run_snapshot_id,
		        push_batch_id,schema_version,request_digest,payload_digest,
		        payload,insight_count,generated_at `+predicate,
		argument,
	).Scan(
		&id, &tenantID, &userID, &taskID, &outcomeID, &snapshotID,
		&batchID, &schemaVersion, &requestDigest, &payloadDigest,
		&payload, &insightCount, &generatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return storedBriefV1{}, false, nil
	}
	if err != nil {
		return storedBriefV1{}, false,
			canonicalBriefDatabaseError("load canonical brief", err)
	}
	var brief types.BriefV1
	if err := json.Unmarshal(payload, &brief); err != nil ||
		brief.Validate() != nil || brief.ID != id ||
		brief.TenantID != tenantID || brief.UserID != userID ||
		brief.TaskID != taskID || brief.RunOutcomeID != outcomeID ||
		brief.RunSnapshotID != snapshotID || brief.PushBatchID != batchID ||
		brief.SchemaVersion != schemaVersion ||
		brief.Digest != payloadDigest || len(brief.Insights) != insightCount ||
		!generatedAt.Valid || !brief.GeneratedAt.Equal(generatedAt.Time) {
		return storedBriefV1{}, false, canonicalBriefIntegrityError()
	}
	canonicalPayload, err := json.Marshal(brief)
	if err != nil || !bytes.Equal(canonicalPayload, payload) {
		return storedBriefV1{}, false, canonicalBriefIntegrityError()
	}
	computedRequest, err := brief.BriefDraftV1.RequestDigest()
	if err != nil || computedRequest != requestDigest {
		return storedBriefV1{}, false, canonicalBriefIntegrityError()
	}
	return storedBriefV1{
		brief: brief, requestDigest: requestDigest,
	}, true, nil
}

func verifyBriefDeliveriesV1(
	ctx context.Context, tx pgx.Tx, draft types.BriefDraftV1,
) error {
	rows, err := tx.Query(ctx,
		`SELECT id,content_item_id,body_md,created_at
		   FROM deliveries
		  WHERE batch_id=$1 AND tenant_id=$2 AND user_id=$3
		  ORDER BY id`,
		draft.PushBatchID, draft.TenantID, draft.UserID,
	)
	if err != nil {
		return canonicalBriefDatabaseError("lock brief deliveries", err)
	}
	defer rows.Close()
	type evidence struct {
		bodyMD       string
		discoveredAt time.Time
	}
	deliveries := make(map[int64]evidence, len(draft.Insights))
	for rows.Next() {
		var id int64
		var contentItemID sql.NullInt64
		var bodyMD string
		var discoveredAt time.Time
		if err := rows.Scan(
			&id, &contentItemID, &bodyMD, &discoveredAt); err != nil {
			return canonicalBriefDatabaseError("scan brief delivery", err)
		}
		if !contentItemID.Valid {
			return canonicalBriefConflictError(
				"brief delivery has no source content identity")
		}
		deliveries[id] = evidence{
			bodyMD:       bodyMD,
			discoveredAt: discoveredAt.Round(0).UTC().Truncate(time.Microsecond),
		}
	}
	if err := rows.Err(); err != nil {
		return canonicalBriefDatabaseError("read brief deliveries", err)
	}
	if len(deliveries) != len(draft.Insights) {
		return canonicalBriefConflictError(
			"brief must freeze the complete batch delivery set")
	}
	for _, insight := range draft.Insights {
		stored, exists := deliveries[insight.ID]
		if !exists {
			return canonicalBriefConflictError(
				"brief insight does not belong to the exact batch")
		}
		if insight.BodyMD != stored.bodyMD ||
			insight.DiscoveredAt != stored.discoveredAt {
			return canonicalBriefConflictError(
				"brief insight does not match frozen delivery content")
		}
	}
	return nil
}

func commitCanonicalBriefTxV1(
	ctx context.Context, tx pgx.Tx, action string,
) error {
	if err := tx.Commit(ctx); err != nil {
		return canonicalBriefDatabaseError(action, err)
	}
	return nil
}

func canonicalBriefValidationError(message string) error {
	return types.NewAppError(types.CodeValidation, message, nil)
}

func canonicalBriefNotFoundError(message string) error {
	return types.NewAppError(types.CodeNotFound, message, nil)
}

func canonicalBriefConflictError(message string) error {
	return types.NewAppError(types.CodeConflict, message, nil)
}

func canonicalBriefIntegrityError() error {
	return types.NewAppError(
		types.CodeInternal, "canonical outcome/brief integrity check failed", nil)
}

func canonicalBriefDatabaseError(action string, cause error) error {
	return taskRunDatabaseError(fmt.Sprintf("canonical brief: %s", action), cause)
}
