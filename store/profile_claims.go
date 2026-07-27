package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/types"
)

type profileClaimRow struct {
	ID            int64
	Field         string
	Value         string
	SourceState   string
	SourceRefType *string
	SourceRef     *string
	Generation    int64
	SupersedesID  *int64
	CreatedAt     time.Time
}

type profileClaimEventRow struct {
	ID            int64
	Kind          string
	TargetClaimID *int64
	ResultClaimID *int64
	TargetEventID *int64
	CreatedAt     time.Time
}

type profileClaimProjection struct {
	industry   string
	occupation string
	tags       []string
	summary    string
	active     map[int64]bool
	pinned     map[int64]bool
}

func (s *Store) ListProfileClaims(
	ctx context.Context, tenantID, userID int64,
) (*types.ProfileClaimList, error) {
	if tenantID <= 0 || userID <= 0 {
		return nil, types.NewAppError(types.CodeValidation, "画像主张读取范围无效", nil)
	}
	tx, err := s.beginProfileClaimScopedTx(ctx, tenantID, false, userID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var version, generation int64
	err = tx.QueryRow(ctx,
		`SELECT version,evidence_generation FROM profile_claim_states
		  WHERE tenant_id=$1 AND user_id=$2`,
		tenantID, userID).Scan(&version, &generation)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, types.NewAppError(types.CodeNotFound, "画像主张尚未初始化", nil)
	}
	if err != nil {
		return nil, profileClaimDBError("read profile claim state", err)
	}
	claims, events, err := loadProfileClaimLedgerTx(ctx, tx, tenantID, userID)
	if err != nil {
		return nil, err
	}
	projection := projectProfileClaims(claims, events, generation)
	out := &types.ProfileClaimList{
		Version: version,
		Claims:  publicProfileClaims(claims, projection, events),
		Events:  publicProfileClaimEvents(events),
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, profileClaimDBError("commit profile claim read", err)
	}
	return out, nil
}

// GetProfileEvolutionBase returns only non-manual summary/tag claims. Evolver
// must never feed the derived manual authority segment back to the model.
func (s *Store) GetProfileEvolutionBase(
	ctx context.Context, tenantID, userID int64,
) (string, []string, error) {
	if userID <= 0 {
		return "", nil, types.NewAppError(types.CodeValidation, "画像演化基线范围无效", nil)
	}
	if tenantID <= 0 {
		err := s.pool.QueryRow(ctx,
			`SELECT tenant_id FROM profiles WHERE user_id=$1`, userID,
		).Scan(&tenantID)
		if errors.Is(err, pgx.ErrNoRows) {
			return "", nil, types.NewAppError(types.CodeNotFound, "画像不存在", nil)
		}
		if err != nil {
			return "", nil, profileClaimDBError("resolve profile evolution tenant", err)
		}
		var matchingMemberships int
		if err := s.pool.QueryRow(ctx,
			`SELECT count(*) FROM memberships
			  WHERE user_id=$1 AND tenant_id=$2`,
			userID, tenantID,
		).Scan(&matchingMemberships); err != nil {
			return "", nil, profileClaimDBError("validate profile evolution owner", err)
		}
		if matchingMemberships != 1 {
			return "", nil, types.NewAppError(
				types.CodeConflict, "画像租户归属与当前 membership 不一致", nil)
		}
	}
	tx, err := s.beginProfileClaimScopedTx(ctx, tenantID, false, userID)
	if err != nil {
		return "", nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var generation int64
	err = tx.QueryRow(ctx,
		`SELECT evidence_generation FROM profile_claim_states
		  WHERE tenant_id=$1 AND user_id=$2`, tenantID, userID,
	).Scan(&generation)
	if errors.Is(err, pgx.ErrNoRows) {
		var profileExists bool
		if existsErr := tx.QueryRow(ctx,
			`SELECT EXISTS(
			   SELECT 1 FROM profiles WHERE tenant_id=$1 AND user_id=$2
			 )`, tenantID, userID,
		).Scan(&profileExists); existsErr != nil {
			return "", nil, profileClaimDBError(
				"distinguish missing evolution claim state", existsErr)
		}
		if !profileExists {
			return "", nil, types.NewAppError(types.CodeNotFound, "画像不存在", nil)
		}
		return "", nil, types.NewAppError(
			types.CodeConflict, "画像主张状态缺失，拒绝从派生投影推断基线", nil)
	}
	if err != nil {
		return "", nil, profileClaimDBError("read profile evolution generation", err)
	}
	claims, events, err := loadProfileClaimLedgerTx(
		ctx, tx, tenantID, userID)
	if err != nil {
		return "", nil, err
	}
	projection := projectProfileClaims(claims, events, generation)
	summary, tags := projectedNonManualEvolutionBase(claims, projection)
	return summary, tags, nil
}

func stripDerivedManualSegment(summary string) string {
	if marker := strings.Index(summary, "\n人工纠正："); marker >= 0 {
		return strings.TrimSpace(summary[:marker])
	}
	if strings.HasPrefix(strings.TrimSpace(summary), "人工纠正：") {
		return ""
	}
	return strings.TrimSpace(summary)
}

func (s *Store) ApplyProfileClaimAction(
	ctx context.Context,
	tenantID, userID int64,
	action types.ProfileClaimAction,
	idempotencyKey, requestDigest string,
) (*types.ProfileClaimActionResult, error) {
	if tenantID <= 0 || userID <= 0 || action.ExpectedVersion < 0 ||
		idempotencyKey == "" || requestDigest == "" {
		return nil, types.NewAppError(types.CodeValidation, "画像主张操作范围或幂等凭据无效", nil)
	}
	tx, err := s.beginProfileClaimScopedTx(ctx, tenantID, true, userID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	_, err = lockProfileTx(ctx, tx, tenantID, userID)
	if err != nil {
		return nil, err
	}
	version, generation, err := lockProfileClaimStateTx(ctx, tx, tenantID, userID)
	if err != nil {
		return nil, err
	}
	// The profile/state locks serialize same-key first attempts. Replay remains
	// before expected-version comparison so response-loss retries return the
	// exact first response even after that response advanced the version.
	if result, found, err := replayProfileClaimActionTx(
		ctx, tx, tenantID, userID, idempotencyKey, requestDigest,
	); err != nil {
		return nil, err
	} else if found {
		if err := tx.Commit(ctx); err != nil {
			return nil, profileClaimDBError("commit profile claim replay", err)
		}
		return result, nil
	}
	if version != action.ExpectedVersion {
		return nil, types.NewAppError(types.CodeConflict, "画像主张版本已变化，请刷新后重试", nil)
	}
	claims, events, err := loadProfileClaimLedgerTx(ctx, tx, tenantID, userID)
	if err != nil {
		return nil, err
	}
	beforeProjection := projectProfileClaims(claims, events, generation)
	claimByID := make(map[int64]profileClaimRow, len(claims))
	for _, claim := range claims {
		claimByID[claim.ID] = claim
	}

	var targetClaimID, resultClaimID, targetEventID *int64
	switch action.Action {
	case "correct", "suppress", "pin":
		target, ok := claimByID[action.ClaimID]
		if !ok {
			return nil, types.NewAppError(types.CodeNotFound, "画像主张不存在", nil)
		}
		if action.Action == "pin" {
			targetKey := profileClaimSemanticKey(target)
			for _, claim := range claims {
				if beforeProjection.active[claim.ID] &&
					beforeProjection.pinned[claim.ID] &&
					profileClaimSemanticKey(claim) == targetKey {
					return nil, types.NewAppError(
						types.CodeValidation, "同值画像主张已经固定", nil)
				}
			}
		}
		if !beforeProjection.active[target.ID] {
			return nil, types.NewAppError(types.CodeConflict, "只能操作当前生效的画像主张", nil)
		}
		targetClaimID = &target.ID
		if action.Action == "correct" {
			value := strings.TrimSpace(action.Value)
			if value == "" {
				return nil, types.NewAppError(types.CodeValidation, "纠正值不能为空", nil)
			}
			limit := 200
			if target.Field == "summary" {
				limit = maxSummaryClaimRunes
			}
			if target.Field == "tag" {
				limit = 20
			}
			if utf8.RuneCountInString(value) > limit {
				return nil, types.NewAppError(types.CodeValidation, "纠正值超过字段长度上限", nil)
			}
			var id int64
			err := tx.QueryRow(ctx,
				`INSERT INTO profile_claims
				    (tenant_id,user_id,field_name,claim_value,source_state,supersedes_claim_id)
				 VALUES ($1,$2,$3,$4,'manual',$5) RETURNING id`,
				tenantID, userID, target.Field, value, target.ID,
			).Scan(&id)
			if err != nil {
				return nil, profileClaimDBError("insert manual profile claim", err)
			}
			resultClaimID = &id
		}
	case "revoke":
		targetEventID = &action.EventID
		var target profileClaimEventRow
		found := false
		for _, event := range events {
			if event.ID == action.EventID {
				target, found = event, true
				break
			}
		}
		if !found {
			return nil, types.NewAppError(types.CodeNotFound, "待撤销的人工事件不存在", nil)
		}
		if target.Kind == "revoke" {
			return nil, types.NewAppError(types.CodeValidation, "revoke 只能补偿人工纠正事件", nil)
		}
		revoked, dependent := profileClaimEventAuthority(events)
		if revoked[target.ID] {
			return nil, types.NewAppError(types.CodeConflict, "该人工事件已经撤销", nil)
		}
		if dependent[target.ID] {
			return nil, types.NewAppError(types.CodeConflict, "该人工事件已有后续依赖，不能直接撤销", nil)
		}
	default:
		return nil, types.NewAppError(types.CodeValidation, "未知的画像主张操作", nil)
	}

	var eventID int64
	err = tx.QueryRow(ctx,
		`INSERT INTO profile_claim_events
		    (tenant_id,user_id,actor_user_id,event_kind,target_claim_id,
		     result_claim_id,target_event_id,expected_version,result_version)
		 VALUES ($1,$2,$2,$3,$4,$5,$6,$7::bigint,$7::bigint+1) RETURNING id`,
		tenantID, userID, action.Action, targetClaimID, resultClaimID,
		targetEventID, version,
	).Scan(&eventID)
	if err != nil {
		return nil, profileClaimDBError("insert profile claim event", err)
	}
	claims, events, err = loadProfileClaimLedgerTx(ctx, tx, tenantID, userID)
	if err != nil {
		return nil, err
	}
	projection := projectProfileClaims(claims, events, generation)
	if activeSummaryRunes(claims, projection) > maxProjectedSummaryRunes {
		return nil, types.NewAppError(
			types.CodeValidation, "生效画像摘要不能超过 500 字", nil)
	}
	tag, err := tx.Exec(ctx,
		`UPDATE profile_claim_states
		    SET version=version+1,updated_at=clock_timestamp()
		  WHERE tenant_id=$1 AND user_id=$2 AND version=$3`,
		tenantID, userID, version)
	if err != nil {
		return nil, profileClaimDBError("advance profile claim version", err)
	}
	if tag.RowsAffected() != 1 {
		return nil, types.NewAppError(types.CodeConflict, "画像主张版本已变化，请刷新后重试", nil)
	}
	updated, err := writeProfileClaimProjectionTx(ctx, tx, tenantID, userID, projection)
	if err != nil {
		return nil, err
	}
	result := &types.ProfileClaimActionResult{
		Version: version + 1,
		EventID: strconv.FormatInt(eventID, 10),
		Profile: publicProfile(updated),
		Claims:  publicProfileClaims(claims, projection, events),
	}
	payload, err := json.Marshal(result)
	if err != nil {
		return nil, types.NewAppError(types.CodeInternal, "画像主张回执编码失败", err)
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO profile_claim_receipts
		    (tenant_id,user_id,idempotency_key,request_digest,event_id,response_payload)
		 VALUES ($1,$2,$3,$4,$5,$6)`,
		tenantID, userID, idempotencyKey, requestDigest, eventID, payload,
	); err != nil {
		return nil, profileClaimDBError("insert profile claim receipt", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, profileClaimDBError("commit profile claim action", err)
	}
	return result, nil
}

func (s *Store) beginProfileClaimScopedTx(
	ctx context.Context, tenantID int64, write bool, userID int64,
) (pgx.Tx, error) {
	if tenantID <= 0 || userID <= 0 {
		return nil, types.NewAppError(types.CodeValidation, "画像主张用户范围无效", nil)
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, profileClaimDBError("begin profile claim transaction", err)
	}
	if _, err := tx.Exec(ctx,
		`SET LOCAL search_path = pg_catalog, public, pg_temp`); err != nil {
		_ = tx.Rollback(ctx)
		return nil, profileClaimDBError("pin profile claim search path", err)
	}
	if write {
		exists, err := lockTenantAdmissionRoot(ctx, tx, tenantID)
		if err != nil {
			_ = tx.Rollback(ctx)
			return nil, profileClaimDBError("lock profile claim tenant admission", err)
		}
		if !exists {
			_ = tx.Rollback(ctx)
			return nil, types.NewAppError(types.CodeNotFound, "租户不存在", nil)
		}
	}
	if err := lockExactProfileMembershipRoot(
		ctx, tx, tenantID, userID); err != nil {
		_ = tx.Rollback(ctx)
		return nil, err
	}
	if _, err := tx.Exec(ctx,
		`SELECT set_config('app.tenant_id',$1,true)`,
		strconv.FormatInt(tenantID, 10)); err != nil {
		_ = tx.Rollback(ctx)
		return nil, profileClaimDBError("set profile claim tenant", err)
	}
	if _, err := tx.Exec(ctx,
		`SELECT set_config('app.user_id',$1,true)`,
		strconv.FormatInt(userID, 10)); err != nil {
		_ = tx.Rollback(ctx)
		return nil, profileClaimDBError("set profile claim user", err)
	}
	if _, err := tx.Exec(ctx, `SET LOCAL ROLE vane_profile_claim_editor`); err != nil {
		_ = tx.Rollback(ctx)
		return nil, profileClaimDBError("enter profile claim editor role", err)
	}
	return tx, nil
}

// lockExactProfileMembershipRoot is taken before entering either restricted
// profile role.  FOR KEY SHARE makes revocation serialize with the whole
// read/write transaction: either DELETE wins and this gate rejects, or DELETE
// waits until the already-authorized operation commits.
func lockExactProfileMembershipRoot(
	ctx context.Context, tx pgx.Tx, tenantID, userID int64,
) error {
	var present bool
	err := tx.QueryRow(ctx,
		`SELECT true
		   FROM memberships
		  WHERE tenant_id=$1 AND user_id=$2
		  FOR KEY SHARE`,
		tenantID, userID,
	).Scan(&present)
	if errors.Is(err, pgx.ErrNoRows) || !present {
		return types.NewAppError(
			types.CodeNotFound, "用户不再属于该画像租户", nil)
	}
	if err != nil {
		return profileClaimDBError("lock exact profile membership", err)
	}
	return nil
}

func lockProfileClaimStateTx(
	ctx context.Context, tx pgx.Tx, tenantID, userID int64,
) (version, generation int64, err error) {
	err = tx.QueryRow(ctx,
		`SELECT version,evidence_generation FROM profile_claim_states
		  WHERE tenant_id=$1 AND user_id=$2 FOR UPDATE`,
		tenantID, userID).Scan(&version, &generation)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, 0, types.NewAppError(
			types.CodeConflict, "画像主张状态缺失，拒绝从派生投影补写", nil)
	}
	if err != nil {
		return 0, 0, profileClaimDBError("lock profile claim state", err)
	}
	return version, generation, nil
}

func loadProfileClaimLedgerTx(
	ctx context.Context, tx pgx.Tx, tenantID, userID int64,
) ([]profileClaimRow, []profileClaimEventRow, error) {
	// 6.3-C retained risk: projection still reads the complete private ledger.
	// Public responses are bounded and projection is linear, but durable
	// compaction/pagination is intentionally a later authority-preserving cut.
	rows, err := tx.Query(ctx,
		`SELECT id,field_name,claim_value,source_state,source_ref_type,
		        source_ref,generation,supersedes_claim_id,created_at
		   FROM profile_claims
		  WHERE tenant_id=$1 AND user_id=$2 ORDER BY id`,
		tenantID, userID)
	if err != nil {
		return nil, nil, profileClaimDBError("load profile claims", err)
	}
	var claims []profileClaimRow
	for rows.Next() {
		var claim profileClaimRow
		if err := rows.Scan(
			&claim.ID, &claim.Field, &claim.Value, &claim.SourceState,
			&claim.SourceRefType, &claim.SourceRef, &claim.Generation,
			&claim.SupersedesID, &claim.CreatedAt,
		); err != nil {
			rows.Close()
			return nil, nil, profileClaimDBError("scan profile claim", err)
		}
		claims = append(claims, claim)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, nil, profileClaimDBError("iterate profile claims", err)
	}
	rows.Close()
	eventRows, err := tx.Query(ctx,
		`SELECT id,event_kind,target_claim_id,result_claim_id,target_event_id,created_at
		   FROM profile_claim_events
		  WHERE tenant_id=$1 AND user_id=$2 ORDER BY id`,
		tenantID, userID)
	if err != nil {
		return nil, nil, profileClaimDBError("load profile claim events", err)
	}
	defer eventRows.Close()
	var events []profileClaimEventRow
	for eventRows.Next() {
		var event profileClaimEventRow
		if err := eventRows.Scan(
			&event.ID, &event.Kind, &event.TargetClaimID,
			&event.ResultClaimID, &event.TargetEventID, &event.CreatedAt,
		); err != nil {
			return nil, nil, profileClaimDBError("scan profile claim event", err)
		}
		events = append(events, event)
	}
	if err := eventRows.Err(); err != nil {
		return nil, nil, profileClaimDBError("iterate profile claim events", err)
	}
	return claims, events, nil
}

func publicProfileClaimEvents(
	events []profileClaimEventRow,
) []types.ProfileClaimEvent {
	revoked, dependent := profileClaimEventAuthority(events)
	all := make([]types.ProfileClaimEvent, 0, len(events))
	include := make(map[string]bool)
	for i := len(events) - 1; i >= 0; i-- {
		event := events[i]
		item := types.ProfileClaimEvent{
			ID:        strconv.FormatInt(event.ID, 10),
			Kind:      event.Kind,
			CreatedAt: event.CreatedAt,
			Revoked:   event.Kind != "revoke" && revoked[event.ID],
			Revocable: event.Kind != "revoke" && !revoked[event.ID] &&
				!dependent[event.ID],
		}
		if event.TargetClaimID != nil {
			item.TargetClaimID = strconv.FormatInt(*event.TargetClaimID, 10)
		}
		if event.ResultClaimID != nil {
			item.ResultClaimID = strconv.FormatInt(*event.ResultClaimID, 10)
		}
		all = append(all, item)
		if item.Revocable {
			include[item.ID] = true
		}
	}
	for _, item := range all {
		if len(include) >= 50 {
			break
		}
		include[item.ID] = true
	}
	out := make([]types.ProfileClaimEvent, 0, len(include))
	for _, item := range all {
		if include[item.ID] {
			out = append(out, item)
		}
	}
	if out == nil {
		out = []types.ProfileClaimEvent{}
	}
	return out
}

// profileClaimEventAuthority is the single source of truth for whether a
// manual event remains effective and whether a later effective event depends
// on a correction result. GET revocability and POST revoke validation must
// agree, including after the dependent event has itself been revoked.
func profileClaimEventAuthority(
	events []profileClaimEventRow,
) (revoked, dependent map[int64]bool) {
	revoked = make(map[int64]bool)
	dependent = make(map[int64]bool)
	for _, event := range events {
		if event.Kind == "revoke" && event.TargetEventID != nil {
			revoked[*event.TargetEventID] = true
		}
	}
	producerByClaim := make(map[int64]int64)
	for _, event := range events {
		if event.ResultClaimID != nil {
			producerByClaim[*event.ResultClaimID] = event.ID
		}
	}
	for _, later := range events {
		if later.Kind == "revoke" || revoked[later.ID] ||
			later.TargetClaimID == nil {
			continue
		}
		if producerID, ok := producerByClaim[*later.TargetClaimID]; ok &&
			later.ID > producerID {
			dependent[producerID] = true
		}
	}
	return revoked, dependent
}

func projectProfileClaims(
	claims []profileClaimRow, events []profileClaimEventRow, evidenceGeneration int64,
) profileClaimProjection {
	out := profileClaimProjection{
		active: make(map[int64]bool, len(claims)),
		pinned: make(map[int64]bool),
		tags:   []string{},
	}
	byID := make(map[int64]profileClaimRow, len(claims))
	resultClaims := make(map[int64]bool)
	for _, event := range events {
		if event.ResultClaimID != nil {
			resultClaims[*event.ResultClaimID] = true
		}
	}
	maxGeneration := map[string]int64{}
	for _, claim := range claims {
		byID[claim.ID] = claim
		if claim.SourceState != "manual" && claim.Generation > maxGeneration[claim.Field] {
			maxGeneration[claim.Field] = claim.Generation
		}
	}
	for _, claim := range claims {
		if claim.SourceState == "manual" {
			// Manual seed claims (initial profile intake / added tag) are facts
			// in their own right. Correction result claims are activated only
			// by their effective event so revoke can remove them.
			if !resultClaims[claim.ID] {
				out.active[claim.ID] = true
			}
			continue
		}
		wanted := maxGeneration[claim.Field]
		if (claim.Field == "tag" || claim.Field == "summary") && evidenceGeneration > 0 {
			wanted = evidenceGeneration
		}
		if claim.Generation == wanted {
			out.active[claim.ID] = true
		}
	}
	activeByField := make(map[string]map[int64]bool)
	activeBySemantic := make(map[string]map[int64]bool)
	claimByID := func(id int64) profileClaimRow { return byID[id] }
	setActive := func(id int64, active bool) {
		claim := claimByID(id)
		fieldSet := activeByField[claim.Field]
		if fieldSet == nil {
			fieldSet = make(map[int64]bool)
			activeByField[claim.Field] = fieldSet
		}
		key := profileClaimSemanticKey(claim)
		semanticSet := activeBySemantic[key]
		if semanticSet == nil {
			semanticSet = make(map[int64]bool)
			activeBySemantic[key] = semanticSet
		}
		out.active[id] = active
		if active {
			fieldSet[id] = true
			semanticSet[id] = true
		} else {
			delete(fieldSet, id)
			delete(semanticSet, id)
		}
	}
	for id, active := range out.active {
		if active {
			setActive(id, true)
		}
	}
	deactivateIDs := func(ids map[int64]bool) {
		for id := range ids {
			setActive(id, false)
		}
	}
	deactivateSemanticKey := func(key string) {
		deactivateIDs(activeBySemantic[key])
	}
	deactivateFieldKey := func(field string) {
		deactivateIDs(activeByField[field])
	}
	revoked := make(map[int64]bool)
	for _, event := range events {
		if event.Kind == "revoke" && event.TargetEventID != nil {
			revoked[*event.TargetEventID] = true
		}
	}
	effectiveSuppress := make(map[string]profileClaimRow)
	for _, event := range events {
		if event.Kind == "revoke" || revoked[event.ID] {
			continue
		}
		target, ok := derefClaim(byID, event.TargetClaimID)
		if !ok {
			continue
		}
		switch event.Kind {
		case "correct":
			result, ok := derefClaim(byID, event.ResultClaimID)
			if !ok {
				continue
			}
			deactivateSemanticKey(profileClaimSemanticKey(target))
			if target.Field == "industry" || target.Field == "occupation" {
				deactivateFieldKey(target.Field)
			}
			setActive(result.ID, true)
		case "suppress":
			deactivateSemanticKey(profileClaimSemanticKey(target))
			effectiveSuppress[target.Field+"\x00"+target.Value] = target
		case "pin":
			if target.Field == "industry" || target.Field == "occupation" {
				deactivateFieldKey(target.Field)
			}
			setActive(target.ID, true)
			out.pinned[target.ID] = true
		}
	}
	// A semantic summary value has one active representative.  Authority wins
	// over evidence, and newer evidence wins within the same authority class.
	// Marking losers inactive (instead of merely suppressing duplicate output)
	// keeps GET/action semantics aligned with the compiled projection.
	summaryWinner := make(map[string]profileClaimRow)
	for _, claim := range claims {
		if claim.Field != "summary" || !out.active[claim.ID] {
			continue
		}
		winner, exists := summaryWinner[claim.Value]
		if !exists || preferSummaryClaim(claim, winner, out.pinned) {
			if exists {
				setActive(winner.ID, false)
			}
			summaryWinner[claim.Value] = claim
		} else {
			setActive(claim.ID, false)
		}
	}
	var pinnedTags, manualTags, baseTags, summaryClaims []profileClaimRow
	var selectedIndustry, selectedOccupation *profileClaimRow
	for _, claim := range claims {
		if !out.active[claim.ID] {
			continue
		}
		switch claim.Field {
		case "industry":
			out.industry = claim.Value
			selected := claim
			selectedIndustry = &selected
		case "occupation":
			out.occupation = claim.Value
			selected := claim
			selectedOccupation = &selected
		case "summary":
			summaryClaims = append(summaryClaims, claim)
		case "tag":
			switch {
			case out.pinned[claim.ID]:
				pinnedTags = append(pinnedTags, claim)
			case claim.SourceState == "manual":
				manualTags = append(manualTags, claim)
			default:
				baseTags = append(baseTags, claim)
			}
		}
	}
	sort.SliceStable(pinnedTags, func(i, j int) bool { return pinnedTags[i].ID < pinnedTags[j].ID })
	sort.SliceStable(manualTags, func(i, j int) bool { return manualTags[i].ID < manualTags[j].ID })
	sort.SliceStable(baseTags, func(i, j int) bool { return baseTags[i].ID < baseTags[j].ID })
	rootByID := profileClaimRootIDs(claims, byID)
	sort.SliceStable(summaryClaims, func(i, j int) bool {
		left := rootByID[summaryClaims[i].ID]
		right := rootByID[summaryClaims[j].ID]
		if left == right {
			return summaryClaims[i].ID < summaryClaims[j].ID
		}
		return left < right
	})
	summarySeen := make(map[string]bool)
	var activeSummaryParts []string
	for _, claim := range summaryClaims {
		if !summarySeen[claim.Value] {
			summarySeen[claim.Value] = true
			activeSummaryParts = append(activeSummaryParts, claim.Value)
		}
	}
	out.summary = strings.Join(activeSummaryParts, "")
	seen := make(map[string]bool)
	var selectedTags []profileClaimRow
	for _, group := range [][]profileClaimRow{pinnedTags, manualTags, baseTags} {
		for _, claim := range group {
			if !seen[claim.Value] && len(out.tags) < maxProfileTags {
				seen[claim.Value] = true
				out.tags = append(out.tags, claim.Value)
				selectedTags = append(selectedTags, claim)
			}
		}
	}
	selectedTagIDs := make(map[int64]bool, len(selectedTags))
	for _, claim := range selectedTags {
		selectedTagIDs[claim.ID] = true
	}
	for _, claim := range claims {
		if claim.Field == "tag" && out.active[claim.ID] &&
			!selectedTagIDs[claim.ID] {
			setActive(claim.ID, false)
		}
	}
	// Summary authority is a bounded view of final state, never an event log.
	// Superseded corrections and revoked events therefore cannot accumulate
	// stale text across evolutions.
	var manualNotes []string
	noteSeen := make(map[string]bool)
	addAuthorityNote := func(claim profileClaimRow) {
		if claim.Field == "summary" {
			return
		}
		var note string
		switch {
		case out.pinned[claim.ID]:
			note = fmt.Sprintf("固定%s=%s",
				profileClaimFieldLabel(claim.Field), claim.Value)
		case claim.SourceState == "manual" && resultClaims[claim.ID]:
			note = fmt.Sprintf("%s=%s",
				profileClaimFieldLabel(claim.Field), claim.Value)
		}
		if note != "" && !noteSeen[note] && len(manualNotes) < 16 {
			noteSeen[note] = true
			manualNotes = append(manualNotes, note)
		}
	}
	if selectedIndustry != nil {
		addAuthorityNote(*selectedIndustry)
	}
	if selectedOccupation != nil {
		addAuthorityNote(*selectedOccupation)
	}
	for _, claim := range selectedTags {
		addAuthorityNote(claim)
	}
	suppressedKeys := make([]string, 0, len(effectiveSuppress))
	for key := range effectiveSuppress {
		suppressedKeys = append(suppressedKeys, key)
	}
	sort.Strings(suppressedKeys)
	for _, key := range suppressedKeys {
		target := effectiveSuppress[key]
		if target.Field == "summary" {
			continue
		}
		stillActive := len(activeBySemantic[profileClaimSemanticKey(target)]) > 0
		note := fmt.Sprintf("排除%s=%s",
			profileClaimFieldLabel(target.Field), target.Value)
		if !stillActive && !noteSeen[note] && len(manualNotes) < 16 {
			noteSeen[note] = true
			manualNotes = append(manualNotes, note)
		}
	}
	if len(manualNotes) > 0 {
		note := "人工纠正：" + strings.Join(manualNotes, "；")
		if out.summary == "" {
			out.summary = note
		} else {
			out.summary = strings.TrimSpace(out.summary) + "\n" + note
		}
	}
	return out
}

func preferSummaryClaim(
	candidate, incumbent profileClaimRow, pinned map[int64]bool,
) bool {
	rank := func(claim profileClaimRow) int {
		switch {
		case pinned[claim.ID]:
			return 3
		case claim.SourceState == "manual":
			return 2
		default:
			return 1
		}
	}
	candidateRank, incumbentRank := rank(candidate), rank(incumbent)
	if candidateRank != incumbentRank {
		return candidateRank > incumbentRank
	}
	if candidate.Generation != incumbent.Generation {
		return candidate.Generation > incumbent.Generation
	}
	return candidate.ID > incumbent.ID
}

func profileClaimRootIDs(
	claims []profileClaimRow, byID map[int64]profileClaimRow,
) map[int64]int64 {
	roots := make(map[int64]int64, len(claims))
	visiting := make(map[int64]bool)
	var resolve func(int64) int64
	resolve = func(id int64) int64 {
		if root, ok := roots[id]; ok {
			return root
		}
		if visiting[id] {
			// Cycles are not produced by the API; fail deterministic if a
			// privileged operator ever corrupts the chain.
			return id
		}
		visiting[id] = true
		root := id
		if claim, ok := byID[id]; ok && claim.SupersedesID != nil {
			if _, parentExists := byID[*claim.SupersedesID]; parentExists {
				root = resolve(*claim.SupersedesID)
			}
		}
		delete(visiting, id)
		roots[id] = root
		return root
	}
	for _, claim := range claims {
		resolve(claim.ID)
	}
	return roots
}

func projectedNonManualEvolutionBase(
	claims []profileClaimRow, projection profileClaimProjection,
) (string, []string) {
	byID := make(map[int64]profileClaimRow, len(claims))
	for _, claim := range claims {
		byID[claim.ID] = claim
	}
	roots := profileClaimRootIDs(claims, byID)
	var summaries, pinnedTags, baseTags []profileClaimRow
	for _, claim := range claims {
		if !projection.active[claim.ID] || claim.SourceState == "manual" {
			continue
		}
		switch claim.Field {
		case "summary":
			summaries = append(summaries, claim)
		case "tag":
			if projection.pinned[claim.ID] {
				pinnedTags = append(pinnedTags, claim)
			} else {
				baseTags = append(baseTags, claim)
			}
		}
	}
	sort.SliceStable(summaries, func(i, j int) bool {
		if roots[summaries[i].ID] == roots[summaries[j].ID] {
			return summaries[i].ID < summaries[j].ID
		}
		return roots[summaries[i].ID] < roots[summaries[j].ID]
	})
	sort.SliceStable(pinnedTags, func(i, j int) bool {
		return pinnedTags[i].ID < pinnedTags[j].ID
	})
	sort.SliceStable(baseTags, func(i, j int) bool {
		return baseTags[i].ID < baseTags[j].ID
	})
	summarySeen := make(map[string]bool)
	var summaryParts []string
	for _, claim := range summaries {
		if !summarySeen[claim.Value] {
			summarySeen[claim.Value] = true
			summaryParts = append(summaryParts, claim.Value)
		}
	}
	tagSeen := make(map[string]bool)
	tags := []string{}
	for _, group := range [][]profileClaimRow{pinnedTags, baseTags} {
		for _, claim := range group {
			if !tagSeen[claim.Value] {
				tagSeen[claim.Value] = true
				tags = append(tags, claim.Value)
			}
		}
	}
	return strings.Join(summaryParts, ""), tags
}

func derefClaim(
	claims map[int64]profileClaimRow, id *int64,
) (profileClaimRow, bool) {
	if id == nil {
		return profileClaimRow{}, false
	}
	claim, ok := claims[*id]
	return claim, ok
}

func profileClaimSemanticKey(claim profileClaimRow) string {
	return claim.Field + "\x00" + claim.Value
}

func profileClaimFieldLabel(field string) string {
	switch field {
	case "industry":
		return "行业"
	case "occupation":
		return "职业"
	case "tag":
		return "标签"
	default:
		return field
	}
}

func publicProfileClaims(
	claims []profileClaimRow, projection profileClaimProjection,
	events []profileClaimEventRow,
) []types.ProfileClaim {
	revoked, dependent := profileClaimEventAuthority(events)
	include := make(map[int64]bool)
	for _, claim := range claims {
		if projection.active[claim.ID] {
			include[claim.ID] = true
		}
	}
	// Retain the context required to understand every authority event the UI
	// can still revoke, even when its claims are older than the history window.
	for _, event := range events {
		if event.Kind == "revoke" || revoked[event.ID] || dependent[event.ID] {
			continue
		}
		if event.TargetClaimID != nil {
			include[*event.TargetClaimID] = true
		}
		if event.ResultClaimID != nil {
			include[*event.ResultClaimID] = true
		}
	}
	// Fill the bounded response with the newest inactive history. Mandatory
	// active/revocable context may exceed the normal 50-claim display budget.
	for i := len(claims) - 1; i >= 0 && len(include) < 50; i-- {
		include[claims[i].ID] = true
	}

	out := make([]types.ProfileClaim, 0, len(include))
	for _, claim := range claims {
		if !include[claim.ID] {
			continue
		}
		item := types.ProfileClaim{
			ID:        strconv.FormatInt(claim.ID, 10),
			Field:     claim.Field,
			Value:     claim.Value,
			Source:    types.ProfileClaimSource{State: claim.SourceState},
			Active:    projection.active[claim.ID],
			Pinned:    projection.pinned[claim.ID],
			CreatedAt: claim.CreatedAt,
		}
		if claim.SourceRefType != nil {
			item.Source.RefType = *claim.SourceRefType
		}
		if claim.SourceRef != nil {
			item.Source.Ref = *claim.SourceRef
		}
		if claim.SupersedesID != nil {
			item.SupersedesID = strconv.FormatInt(*claim.SupersedesID, 10)
		}
		out = append(out, item)
	}
	if out == nil {
		out = []types.ProfileClaim{}
	}
	return out
}

func writeProfileClaimProjectionTx(
	ctx context.Context, tx pgx.Tx, tenantID, userID int64,
	projection profileClaimProjection,
) (*types.Profile, error) {
	var p types.Profile
	err := scanProfileEdit(tx.QueryRow(ctx,
		`UPDATE profiles
		    SET industry=$3,occupation=$4,tags=$5,summary=$6,
		        removed_tags=(
		          SELECT COALESCE(array_agg(t ORDER BY t),'{}'::text[])
		            FROM (
		              SELECT unnest(removed_tags || tags) AS t
		              EXCEPT SELECT unnest($5::text[])
		            ) diff(t)
		        ),
		        updated_at=GREATEST(clock_timestamp(),updated_at+interval '1 microsecond')
		  WHERE tenant_id=$1 AND user_id=$2
		  RETURNING `+profileEditColumns,
		tenantID, userID, projection.industry, projection.occupation,
		projection.tags, projection.summary,
	), &p)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, types.NewAppError(types.CodeNotFound, "画像不存在", nil)
	}
	if err != nil {
		return nil, profileClaimDBError("write profile claim projection", err)
	}
	return &p, nil
}

func replayProfileClaimActionTx(
	ctx context.Context, tx pgx.Tx, tenantID, userID int64, key, digest string,
) (*types.ProfileClaimActionResult, bool, error) {
	var storedDigest string
	var payload []byte
	err := tx.QueryRow(ctx,
		`SELECT request_digest,response_payload
		   FROM profile_claim_receipts
		  WHERE tenant_id=$1 AND user_id=$2 AND idempotency_key=$3`,
		tenantID, userID, key,
	).Scan(&storedDigest, &payload)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, profileClaimDBError("read profile claim receipt", err)
	}
	if storedDigest != digest {
		return nil, false, types.NewAppError(types.CodeConflict, "Idempotency-Key 已用于另一画像主张请求", nil)
	}
	var result types.ProfileClaimActionResult
	if err := json.Unmarshal(payload, &result); err != nil {
		return nil, false, types.NewAppError(types.CodeInternal, "画像主张回执损坏", err)
	}
	return &result, true, nil
}

func profileClaimDBError(op string, err error) error {
	return types.NewAppError(types.CodeDatabase, op, err)
}

func seedInitialManualProfileClaimsTx(
	ctx context.Context, tx pgx.Tx, tenantID, userID int64, p *types.Profile,
) error {
	if _, err := tx.Exec(ctx,
		`INSERT INTO profile_claim_states(tenant_id,user_id)
		 VALUES($1,$2)`, tenantID, userID); err != nil {
		return profileClaimDBError("initialize manual profile claim state", err)
	}
	insert := func(field, value string) error {
		value = strings.TrimSpace(value)
		if value == "" {
			return nil
		}
		_, err := tx.Exec(ctx,
			`INSERT INTO profile_claims
			    (tenant_id,user_id,field_name,claim_value,source_state)
			 VALUES($1,$2,$3,$4,'manual')`,
			tenantID, userID, field, value)
		return err
	}
	if err := insert("industry", p.Industry); err != nil {
		return profileClaimDBError("seed manual industry claim", err)
	}
	if err := insert("occupation", p.Occupation); err != nil {
		return profileClaimDBError("seed manual occupation claim", err)
	}
	for _, value := range p.Tags {
		if err := insert("tag", value); err != nil {
			return profileClaimDBError("seed manual tag claim", err)
		}
	}
	return nil
}

// evolveProfileClaimsTx appends an evidence generation and recompiles profiles
// from the active ledger. Its feedback_range source_ref records the processing
// batch that produced the generation; it does not assert sentence-level
// entailment by every feedback row in that range. The caller must already have
// entered the appropriate tenant admission protocol; this function locks
// profile before claim state, exactly like the manual authority path.
func evolveProfileClaimsTx(
	ctx context.Context, tx pgx.Tx,
	tenantID, userID int64,
	summary string, tags []string, newCursor int64,
	expectedAt time.Time, expectedCursor int64,
	restoreCompiledRole bool,
) error {
	if _, err := tx.Exec(ctx,
		`SET LOCAL search_path = pg_catalog, public, pg_temp`); err != nil {
		return profileClaimDBError("pin evolved profile claim search path", err)
	}
	if _, err := tx.Exec(ctx,
		`SELECT set_config('app.tenant_id',$1,true),
		        set_config('app.user_id',$2,true)`,
		strconv.FormatInt(tenantID, 10), strconv.FormatInt(userID, 10)); err != nil {
		return profileClaimDBError("set evolved profile claim scope", err)
	}
	if _, err := tx.Exec(ctx, `SET LOCAL ROLE vane_profile_claim_editor`); err != nil {
		return profileClaimDBError("enter evolved profile claim role", err)
	}
	var current types.Profile
	current.UserID = userID
	err := tx.QueryRow(ctx,
		`SELECT industry,occupation,tags,removed_tags,summary,
		        last_evolved_feedback_id,created_at,updated_at
		   FROM profiles
		  WHERE tenant_id=$1 AND user_id=$2 FOR UPDATE`,
		tenantID, userID,
	).Scan(
		&current.Industry, &current.Occupation, &current.Tags,
		&current.RemovedTags, &current.Summary,
		&current.LastEvolvedFeedbackID, &current.CreatedAt, &current.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return types.NewAppError(types.CodeConflict, "画像演化 CAS 未命中", nil)
	}
	if err != nil {
		return profileClaimDBError("lock profile for claim evolution", err)
	}
	if !current.UpdatedAt.Equal(expectedAt) ||
		current.LastEvolvedFeedbackID != expectedCursor {
		return types.NewAppError(types.CodeConflict, "画像演化 CAS 未命中", nil)
	}
	version, generation, err := lockProfileClaimStateTx(
		ctx, tx, tenantID, userID)
	if err != nil {
		return err
	}
	claims, events, err := loadProfileClaimLedgerTx(ctx, tx, tenantID, userID)
	if err != nil {
		return err
	}
	beforeProjection := projectProfileClaims(claims, events, generation)
	authorityValues, authorityRunes := activeSummaryAuthority(
		claims, beforeProjection)
	if authorityRunes > maxProjectedSummaryRunes {
		return types.NewAppError(
			types.CodeInternal,
			"生效人工/固定摘要权威超过 500 字，拒绝覆盖损坏状态",
			nil,
		)
	}
	ref := fmt.Sprintf("feedbacks:(%d,%d]", expectedCursor, newCursor)
	insert := func(field, value string) error {
		value = strings.TrimSpace(value)
		if value == "" {
			return nil
		}
		_, err := tx.Exec(ctx,
			`INSERT INTO profile_claims
			    (tenant_id,user_id,field_name,claim_value,source_state,
			     source_ref_type,source_ref,generation)
			 VALUES ($1,$2,$3,$4,'evidence','feedback_range',$5,$6)`,
			tenantID, userID, field, value, ref, newCursor)
		return err
	}
	// Evolver is expected to pass pure non-manual evidence summary. Strip the
	// deterministic segment defensively so no caller can promote projection
	// authority text back into evidence.
	summary = stripDerivedManualSegment(summary)
	for _, statement := range fitEvidenceSummaryClaims(
		summary, authorityValues,
		maxProjectedSummaryRunes-authorityRunes,
	) {
		if err := insert("summary", statement); err != nil {
			return profileClaimDBError("insert evolved summary claim", err)
		}
	}
	for _, tag := range tags {
		if err := insert("tag", tag); err != nil {
			return profileClaimDBError("insert evolved tag claim", err)
		}
	}
	tag, err := tx.Exec(ctx,
		`UPDATE profile_claim_states
		    SET version=version+1,evidence_generation=$3,
		        updated_at=clock_timestamp()
		  WHERE tenant_id=$1 AND user_id=$2 AND version=$4`,
		tenantID, userID, newCursor, version)
	if err != nil {
		return profileClaimDBError("advance evolved claim generation", err)
	}
	if tag.RowsAffected() != 1 {
		return types.NewAppError(types.CodeConflict, "画像主张版本已并发变化", nil)
	}
	claims, events, err = loadProfileClaimLedgerTx(ctx, tx, tenantID, userID)
	if err != nil {
		return err
	}
	projection := projectProfileClaims(claims, events, newCursor)
	if activeSummaryRunes(claims, projection) > maxProjectedSummaryRunes {
		return types.NewAppError(
			types.CodeInternal, "画像摘要预算拟合不变量失效", nil)
	}
	updateTag, err := tx.Exec(ctx,
		`UPDATE profiles
		    SET industry=$3,occupation=$4,tags=$5,summary=$6,
		        last_evolved_feedback_id=$7,
		        updated_at=GREATEST(clock_timestamp(),updated_at+interval '1 microsecond')
		  WHERE tenant_id=$1 AND user_id=$2
		    AND updated_at=$8 AND last_evolved_feedback_id=$9`,
		tenantID, userID, projection.industry, projection.occupation,
		projection.tags, projection.summary, newCursor, expectedAt, expectedCursor)
	if err != nil {
		return profileClaimDBError("write evolved claim projection", err)
	}
	if updateTag.RowsAffected() != 1 {
		return types.NewAppError(types.CodeConflict, "画像演化 CAS 未命中", nil)
	}
	restoreSQL := `RESET ROLE`
	if restoreCompiledRole {
		restoreSQL = `SET LOCAL ROLE vane_app`
	}
	if _, err := tx.Exec(ctx, restoreSQL); err != nil {
		return profileClaimDBError("restore role after profile claim evolution", err)
	}
	return nil
}

const (
	maxSummaryClaimRunes     = 240
	maxProjectedSummaryRunes = 500
)

func activeSummaryRunes(
	claims []profileClaimRow, projection profileClaimProjection,
) int {
	seen := make(map[string]bool)
	total := 0
	for _, claim := range claims {
		if claim.Field == "summary" && projection.active[claim.ID] &&
			!seen[claim.Value] {
			seen[claim.Value] = true
			total += utf8.RuneCountInString(claim.Value)
		}
	}
	return total
}

func activeSummaryAuthority(
	claims []profileClaimRow, projection profileClaimProjection,
) (map[string]bool, int) {
	values := make(map[string]bool)
	total := 0
	for _, claim := range claims {
		if claim.Field != "summary" || !projection.active[claim.ID] ||
			(claim.SourceState != "manual" && !projection.pinned[claim.ID]) ||
			values[claim.Value] {
			continue
		}
		values[claim.Value] = true
		total += utf8.RuneCountInString(claim.Value)
	}
	return values, total
}

func fitEvidenceSummaryClaims(
	summary string, authorityValues map[string]bool, budget int,
) []string {
	if budget <= 0 {
		return nil
	}
	seen := make(map[string]bool, len(authorityValues))
	for value := range authorityValues {
		seen[value] = true
	}
	var fitted []string
	for _, statement := range splitSummaryClaims(summary) {
		if seen[statement] {
			continue
		}
		runes := []rune(statement)
		if len(runes) > budget {
			statement = strings.TrimSpace(string(runes[:budget]))
			runes = []rune(statement)
		}
		if statement == "" || seen[statement] {
			continue
		}
		fitted = append(fitted, statement)
		seen[statement] = true
		budget -= len(runes)
		if budget == 0 {
			break
		}
	}
	return fitted
}

func splitSummaryClaims(summary string) []string {
	summary = strings.TrimSpace(summary)
	if summary == "" {
		return nil
	}
	var out []string
	var current []rune
	flush := func() {
		part := strings.TrimSpace(string(current))
		if part != "" {
			out = append(out, part)
		}
		current = current[:0]
	}
	for _, r := range []rune(summary) {
		current = append(current, r)
		if len(current) == maxSummaryClaimRunes ||
			strings.ContainsRune("。！？!?；;\n.", r) {
			flush()
		}
	}
	flush()
	return out
}
