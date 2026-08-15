package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/server/types"
)

func (s *Store) beginWorkspaceMemoryTx(
	ctx context.Context, tenantID, actorUserID int64, write bool,
) (pgx.Tx, types.MembershipRole, error) {
	if tenantID <= 0 || actorUserID <= 0 {
		return nil, "", memoryValidationError("团队记忆身份无效")
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, "", memoryDBError("开启团队记忆事务", err)
	}
	fail := func(cause error) (pgx.Tx, types.MembershipRole, error) {
		_ = tx.Rollback(context.WithoutCancel(ctx))
		return nil, "", cause
	}
	if _, err := tx.Exec(ctx, `SET LOCAL search_path=pg_catalog,public,pg_temp`); err != nil {
		return fail(memoryDBError("固定团队记忆搜索路径", err))
	}
	// Join tenant erasure's shared admission root before the workspace-local
	// corpus lock. Purge takes the exclusive root in the same root -> child
	// order, so it cannot race or form a 40P01 cycle with a team-memory turn.
	exists, err := lockTenantAdmissionRootShared(ctx, tx, tenantID)
	if err != nil {
		return fail(memoryDBError("获取团队记忆租户准入根", err))
	}
	if !exists {
		return fail(types.NewAppError(types.CodeNotFound,
			"团队工作区不存在或正在清除", nil))
	}
	// Every actor in a workspace uses the same advisory admission key. Shared
	// recall locks can overlap; writes are exclusive so cross-member quota and
	// active-corpus decisions cannot race.
	admissionFunction := "pg_catalog.pg_advisory_xact_lock_shared"
	if write {
		admissionFunction = "pg_catalog.pg_advisory_xact_lock"
	}
	if _, err := tx.Exec(ctx, fmt.Sprintf(`SELECT %s(
		pg_catalog.hashtextextended('vane.workspace_memory:' || ($1::bigint)::text,0))`,
		admissionFunction), tenantID); err != nil {
		return fail(memoryDBError("获取团队记忆工作区准入锁", err))
	}
	lock := "FOR KEY SHARE OF m,t"
	if write {
		lock = "FOR UPDATE OF m,t"
	}
	var role types.MembershipRole
	var kind types.WorkspaceKind
	err = tx.QueryRow(ctx, fmt.Sprintf(`
        SELECT m.role,t.workspace_kind
          FROM memberships m JOIN tenants t ON t.id=m.tenant_id
         WHERE m.tenant_id=$1 AND m.user_id=$2
           AND t.status='active' AND t.deleted_at IS NULL %s`, lock),
		tenantID, actorUserID).Scan(&role, &kind)
	if errors.Is(err, pgx.ErrNoRows) {
		return fail(types.NewAppError(types.CodeNotFound,
			"团队工作区不存在或无权访问记忆", err))
	}
	if err != nil {
		return fail(memoryDBError("校验团队记忆成员", err))
	}
	if kind != types.WorkspaceKindTeam || !role.Valid() {
		return fail(types.NewAppError(types.CodeForbidden,
			"个人工作区必须使用个人长期记忆账本", types.ErrForbidden))
	}
	if _, err := tx.Exec(ctx, `SELECT set_config('app.tenant_id',$1,true),
        set_config('app.user_id',$2,true),
        set_config('app.membership_role',$3,true),
        set_config('app.workspace_kind',$4,true)`, strconv.FormatInt(tenantID, 10),
		strconv.FormatInt(actorUserID, 10), string(role), string(kind)); err != nil {
		return fail(memoryDBError("设置团队记忆范围", err))
	}
	if _, err := tx.Exec(ctx, `SET LOCAL ROLE vane_workspace_memory_editor`); err != nil {
		return fail(memoryDBError("进入团队记忆最小权限角色", err))
	}
	return tx, role, nil
}

// PrepareWorkspaceMemoryAuthorization persists explicit user intent before a
// team memory action. It never reads or copies a personal memory row.
func (s *Store) PrepareWorkspaceMemoryAuthorization(
	ctx context.Context, tenantID, actorUserID, sessionID int64,
	action types.MemoryAction,
) (string, error) {
	if sessionID <= 0 || action.Evidence.AuthorizationID != "" {
		return "", memoryValidationError("团队记忆授权范围无效")
	}
	canonical, digest, err := validateMemoryAction(strings.Repeat("0", 64), action)
	if err != nil {
		return "", err
	}
	tx, role, err := s.beginWorkspaceMemoryTx(ctx, tenantID, actorUserID, true)
	if err != nil {
		return "", err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	var targetCreator *int64
	if canonical.Action != types.MemoryActionRemember {
		target, err := loadActiveWorkspaceMemoryTx(ctx, tx, tenantID, canonical.MemoryID)
		if err != nil {
			return "", err
		}
		if err := authorizeWorkspaceMemoryMutation(role, actorUserID, canonical.Action, target); err != nil {
			return "", err
		}
		targetCreator = int64Pointer(target.CreatorUserID)
	}
	id := uuid.NewSHA1(uuid.NameSpaceOID, []byte(fmt.Sprintf(
		"vane-workspace-memory-authorization/v2:%d:%d:%d:%s:%s:%s",
		tenantID, actorUserID, sessionID, role, canonical.Evidence.SourceID, digest))).String()
	if _, err := tx.Exec(ctx, `INSERT INTO workspace_memory_authorizations(
        id,tenant_id,actor_user_id,actor_role,workspace_kind,action_kind,
        target_memory_id,target_creator_user_id,session_id,trace_id,owner_request,
        authorization_digest,request_digest)
        VALUES($1,$2,$3,$4,'team',$5,$6,$7,$8,$9,$10,$11,$12)
		ON CONFLICT(id) DO NOTHING`, id, tenantID, actorUserID, role,
		canonical.Action, nullableMemoryID(canonical), targetCreator, sessionID,
		canonical.Evidence.SourceID, canonical.Evidence.OwnerRequest,
		canonical.Evidence.AuthorizationDigest, digest); err != nil {
		return "", memoryDBError("写入团队记忆授权", err)
	}
	var stored, storedRole, storedKind, storedAction, storedTrace,
		storedOwnerRequest, storedAuthorizationDigest string
	var storedSession int64
	var storedTarget, storedCreator *int64
	if err := tx.QueryRow(ctx, `SELECT request_digest,actor_role,workspace_kind,
        action_kind,target_memory_id,target_creator_user_id,session_id,
        trace_id::text,owner_request,authorization_digest
        FROM workspace_memory_authorizations
        WHERE tenant_id=$1 AND actor_user_id=$2 AND id=$3`,
		tenantID, actorUserID, id).Scan(&stored, &storedRole, &storedKind,
		&storedAction, &storedTarget, &storedCreator, &storedSession, &storedTrace,
		&storedOwnerRequest, &storedAuthorizationDigest); err != nil {
		return "", memoryDBError("验证团队记忆授权", err)
	}
	if stored != digest || storedRole != string(role) || storedKind != "team" ||
		storedAction != canonical.Action || !sameOptionalInt64(storedTarget, nullableMemoryID(canonical)) ||
		!sameOptionalInt64(storedCreator, targetCreator) || storedSession != sessionID ||
		storedTrace != canonical.Evidence.SourceID ||
		storedOwnerRequest != canonical.Evidence.OwnerRequest ||
		storedAuthorizationDigest != canonical.Evidence.AuthorizationDigest {
		return "", types.NewAppError(types.CodeConflict, "团队记忆授权重放不一致", nil)
	}
	if err := tx.Commit(ctx); err != nil {
		return "", memoryDBError("提交团队记忆授权", err)
	}
	return id, nil
}

// ApplyWorkspaceMemoryAction appends an explicit team mutation. Members may
// remember; a record creator may correct their own record; Admin/Owner may
// correct or forget any record. No caller can update/delete retained history.
func (s *Store) ApplyWorkspaceMemoryAction(
	ctx context.Context, tenantID, actorUserID int64, idempotencyKey string,
	action types.MemoryAction,
) (*types.MemoryActionResult, error) {
	canonical, authorizationDigest, err := validateMemoryAction(idempotencyKey, action)
	if err != nil {
		return nil, err
	}
	receiptDigest, err := memoryActionDigest(canonical, true)
	if err != nil {
		return nil, err
	}
	tx, role, err := s.beginWorkspaceMemoryTx(ctx, tenantID, actorUserID, true)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if result, found, err := loadWorkspaceMemoryReceiptTx(
		ctx, tx, tenantID, actorUserID, idempotencyKey, receiptDigest,
	); err != nil || found {
		return result, err
	}
	if canonical.Evidence.AuthorizationID == "" {
		return nil, memoryValidationError("团队记忆缺少耐久授权")
	}
	var authorizedDigest, authorizedRole, authorizedKind, authorizedAction string
	var authorizedTarget, authorizedTargetCreator *int64
	var consumed *int64
	if err := tx.QueryRow(ctx, `SELECT request_digest,consumed_event_id,actor_role,
        workspace_kind,action_kind,target_memory_id,target_creator_user_id
        FROM workspace_memory_authorizations
        WHERE tenant_id=$1 AND actor_user_id=$2 AND id=$3 FOR UPDATE`,
		tenantID, actorUserID, canonical.Evidence.AuthorizationID).Scan(
		&authorizedDigest, &consumed, &authorizedRole, &authorizedKind,
		&authorizedAction, &authorizedTarget, &authorizedTargetCreator); errors.Is(err, pgx.ErrNoRows) {
		return nil, types.NewAppError(types.CodeConflict, "团队记忆授权不存在", nil)
	} else if err != nil {
		return nil, memoryDBError("读取团队记忆授权", err)
	}
	if authorizedDigest != authorizationDigest || consumed != nil ||
		authorizedRole != string(role) || authorizedKind != "team" ||
		authorizedAction != canonical.Action ||
		!sameOptionalInt64(authorizedTarget, nullableMemoryID(canonical)) {
		return nil, types.NewAppError(types.CodeConflict,
			"团队记忆授权与请求、角色或目标不一致，或已消费", nil)
	}

	activeCount, corpusBytes, err := workspaceMemoryBoundsTx(ctx, tx, tenantID)
	if err != nil {
		return nil, err
	}
	var target types.MemoryRecord
	if canonical.Action != types.MemoryActionRemember {
		target, err = loadActiveWorkspaceMemoryTx(ctx, tx, tenantID, canonical.MemoryID)
		if err != nil {
			return nil, err
		}
		if authorizedTargetCreator == nil || *authorizedTargetCreator != target.CreatorUserID {
			return nil, types.NewAppError(types.CodeConflict,
				"团队记忆目标创建者已与冻结授权不一致", nil)
		}
		if err := authorizeWorkspaceMemoryMutation(role, actorUserID, canonical.Action, target); err != nil {
			return nil, err
		}
	}
	if canonical.Action == types.MemoryActionRemember && activeCount >= maxActiveMemories {
		return nil, memoryValidationError("团队记忆条目已达到上限")
	}
	projected := corpusBytes
	if canonical.Action == types.MemoryActionRemember {
		projected += len(canonical.Text)
	} else if canonical.Action == types.MemoryActionCorrect {
		projected += len(canonical.Text) - len(target.Text)
	}
	if projected > maxActiveMemoryCorpusBytes {
		return nil, memoryValidationError("团队记忆文本总量已达到上限")
	}

	resultMemory := target
	if canonical.Action != types.MemoryActionForget {
		creatorID := actorUserID
		var supersedes any
		if canonical.Action == types.MemoryActionCorrect {
			supersedes = canonical.MemoryID
			creatorID = target.CreatorUserID
		}
		err = tx.QueryRow(ctx, `INSERT INTO workspace_memory_records(
            tenant_id,creator_user_id,created_by_user_id,memory_text,evidence_source_type,
            evidence_source_id,authorization_id,owner_request,authorization_digest,
            supersedes_memory_id) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
			RETURNING id,created_at`, tenantID, creatorID, actorUserID, canonical.Text,
			canonical.Evidence.SourceType, canonical.Evidence.SourceID,
			canonical.Evidence.AuthorizationID, canonical.Evidence.OwnerRequest,
			canonical.Evidence.AuthorizationDigest, supersedes).Scan(
			&resultMemory.ID, &resultMemory.CreatedAt)
		if err != nil {
			return nil, memoryDBError("写入团队记忆记录", err)
		}
		resultMemory.Text = canonical.Text
		resultMemory.CreatorUserID = creatorID
		resultMemory.Evidence = canonical.Evidence
		resultMemory.Active = true
		if canonical.Action == types.MemoryActionCorrect {
			resultMemory.SupersedesMemoryID = canonical.MemoryID
		}
	} else {
		resultMemory.Active = false
	}
	var targetID, resultID any
	if canonical.Action != types.MemoryActionRemember {
		targetID = canonical.MemoryID
	}
	if canonical.Action != types.MemoryActionForget {
		resultID = resultMemory.ID
	}
	event := types.MemoryEvent{Action: canonical.Action,
		ActorUserID: actorUserID, TargetMemoryID: canonical.MemoryID,
		Evidence: canonical.Evidence}
	if canonical.Action == types.MemoryActionRemember {
		event.TargetMemoryID = 0
	}
	if canonical.Action != types.MemoryActionForget {
		event.ResultMemoryID = resultMemory.ID
	}
	err = tx.QueryRow(ctx, `INSERT INTO workspace_memory_events(
        tenant_id,actor_user_id,actor_role,event_kind,target_memory_id,result_memory_id,
        evidence_source_type,evidence_source_id,authorization_id,owner_request,
        authorization_digest) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
		RETURNING id,created_at`, tenantID, actorUserID, role, canonical.Action,
		targetID, resultID, canonical.Evidence.SourceType,
		canonical.Evidence.SourceID, canonical.Evidence.AuthorizationID,
		canonical.Evidence.OwnerRequest, canonical.Evidence.AuthorizationDigest).Scan(
		&event.ID, &event.CreatedAt)
	if err != nil {
		return nil, memoryDBError("写入团队记忆事件", err)
	}
	result := &types.MemoryActionResult{Memory: resultMemory, Event: event}
	payload, err := json.Marshal(result)
	if err != nil {
		return nil, types.NewAppError(types.CodeInternal, "编码团队记忆回执", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO workspace_memory_receipts(
        tenant_id,actor_user_id,idempotency_key,request_digest,event_id,response_payload)
        VALUES($1,$2,$3,$4,$5,$6)`, tenantID, actorUserID, idempotencyKey,
		receiptDigest, event.ID, payload); err != nil {
		return nil, memoryDBError("写入团队记忆回执", err)
	}
	if tag, err := tx.Exec(ctx, `UPDATE workspace_memory_authorizations
        SET consumed_event_id=$1 WHERE tenant_id=$2 AND actor_user_id=$3 AND id=$4
        AND consumed_event_id IS NULL`, event.ID, tenantID, actorUserID,
		canonical.Evidence.AuthorizationID); err != nil || tag.RowsAffected() != 1 {
		if err == nil {
			err = errors.New("workspace memory authorization consumption lost")
		}
		return nil, memoryDBError("消费团队记忆授权", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, memoryDBError("提交团队记忆操作", err)
	}
	return result, nil
}

// RecallWorkspaceMemories builds BM25 exclusively from the exact team's
// active ledger. Personal memory tables are never referenced by this path.
func (s *Store) RecallWorkspaceMemories(
	ctx context.Context, tenantID, actorUserID int64, query types.MemoryRecallQuery,
) (*types.MemoryRecallResult, error) {
	query.Query = strings.TrimSpace(query.Query)
	if !utf8.ValidString(query.Query) || query.Query == "" ||
		len(query.Query) > maxMemoryRecallQueryBytes || query.Limit < 1 ||
		query.Limit > maxMemoryRecallResults {
		return nil, memoryValidationError("团队记忆查询参数无效")
	}
	tx, _, err := s.beginWorkspaceMemoryTx(ctx, tenantID, actorUserID, false)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	activeCount, corpusBytes, err := workspaceMemoryBoundsTx(ctx, tx, tenantID)
	if err != nil {
		return nil, err
	}
	rows, err := tx.Query(ctx, `SELECT r.id,r.creator_user_id,r.memory_text,
        r.evidence_source_type,r.evidence_source_id::text,r.owner_request,
        r.authorization_digest,COALESCE(r.supersedes_memory_id,0),r.created_at
        FROM workspace_memory_records r WHERE r.tenant_id=$1
        AND EXISTS(SELECT 1 FROM workspace_memory_events e
          WHERE e.tenant_id=r.tenant_id AND e.result_memory_id=r.id)
        AND NOT EXISTS(SELECT 1 FROM workspace_memory_events e
          WHERE e.tenant_id=r.tenant_id AND e.target_memory_id=r.id)
        ORDER BY r.id LIMIT $2`, tenantID, maxActiveMemories+1)
	if err != nil {
		return nil, memoryDBError("读取团队记忆", err)
	}
	defer rows.Close()
	records := make([]types.MemoryRecord, 0, activeCount)
	observedBytes := 0
	for rows.Next() {
		var record types.MemoryRecord
		if err := rows.Scan(&record.ID, &record.CreatorUserID, &record.Text,
			&record.Evidence.SourceType, &record.Evidence.SourceID,
			&record.Evidence.OwnerRequest, &record.Evidence.AuthorizationDigest,
			&record.SupersedesMemoryID, &record.CreatedAt); err != nil {
			return nil, memoryDBError("扫描团队记忆", err)
		}
		record.Active = true
		observedBytes += len(record.Text)
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, memoryDBError("遍历团队记忆", err)
	}
	if len(records) != activeCount || observedBytes != corpusBytes {
		return nil, types.NewAppError(types.CodeConflict, "团队记忆召回期间发生漂移", nil)
	}
	memories, err := rankMemoryRecords(records, query.Query, query.Limit)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, memoryDBError("提交团队记忆召回", err)
	}
	return &types.MemoryRecallResult{Memories: memories}, nil
}

func workspaceMemoryBoundsTx(
	ctx context.Context, tx pgx.Tx, tenantID int64,
) (int, int, error) {
	var count, bytes int
	err := tx.QueryRow(ctx, `SELECT count(*)::int,
        COALESCE(sum(octet_length(r.memory_text)),0)::int
        FROM workspace_memory_records r WHERE r.tenant_id=$1
        AND EXISTS(SELECT 1 FROM workspace_memory_events e
          WHERE e.tenant_id=r.tenant_id AND e.result_memory_id=r.id)
        AND NOT EXISTS(SELECT 1 FROM workspace_memory_events e
          WHERE e.tenant_id=r.tenant_id AND e.target_memory_id=r.id)`,
		tenantID).Scan(&count, &bytes)
	if err != nil {
		return 0, 0, memoryDBError("统计团队记忆边界", err)
	}
	if count > maxActiveMemories || bytes > maxActiveMemoryCorpusBytes {
		return 0, 0, types.NewAppError(types.CodeConflict, "团队记忆超过安全边界", nil)
	}
	return count, bytes, nil
}

func loadActiveWorkspaceMemoryTx(
	ctx context.Context, tx pgx.Tx, tenantID, memoryID int64,
) (types.MemoryRecord, error) {
	var record types.MemoryRecord
	err := tx.QueryRow(ctx, `SELECT r.id,r.creator_user_id,r.memory_text,
        r.evidence_source_type,r.evidence_source_id::text,r.owner_request,
        r.authorization_digest,COALESCE(r.supersedes_memory_id,0),r.created_at
        FROM workspace_memory_records r WHERE r.tenant_id=$1 AND r.id=$2
        AND EXISTS(SELECT 1 FROM workspace_memory_events e
          WHERE e.tenant_id=r.tenant_id AND e.result_memory_id=r.id)
        AND NOT EXISTS(SELECT 1 FROM workspace_memory_events e
          WHERE e.tenant_id=r.tenant_id AND e.target_memory_id=r.id)`,
		tenantID, memoryID).Scan(&record.ID, &record.CreatorUserID, &record.Text,
		&record.Evidence.SourceType, &record.Evidence.SourceID,
		&record.Evidence.OwnerRequest, &record.Evidence.AuthorizationDigest,
		&record.SupersedesMemoryID, &record.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return record, types.NewAppError(types.CodeConflict,
			"只能操作当前生效的团队记忆", nil)
	}
	if err != nil {
		return record, memoryDBError("读取团队记忆目标", err)
	}
	record.Active = true
	return record, nil
}

func loadWorkspaceMemoryReceiptTx(
	ctx context.Context, tx pgx.Tx, tenantID, actorUserID int64,
	key, digest string,
) (*types.MemoryActionResult, bool, error) {
	var stored string
	var payload []byte
	err := tx.QueryRow(ctx, `SELECT request_digest,response_payload
        FROM workspace_memory_receipts
        WHERE tenant_id=$1 AND actor_user_id=$2 AND idempotency_key=$3`,
		tenantID, actorUserID, key).Scan(&stored, &payload)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, memoryDBError("读取团队记忆回执", err)
	}
	if stored != digest {
		return nil, false, types.NewAppError(types.CodeConflict,
			"Idempotency-Key 已用于另一团队记忆请求", nil)
	}
	var result types.MemoryActionResult
	if err := json.Unmarshal(payload, &result); err != nil {
		return nil, false, types.NewAppError(types.CodeInternal, "团队记忆回执损坏", err)
	}
	return &result, true, nil
}

func authorizeWorkspaceMemoryMutation(
	role types.MembershipRole, actorUserID int64, action string, target types.MemoryRecord,
) error {
	admin := role == types.MembershipRoleAdmin || role == types.MembershipRoleOwner
	switch action {
	case types.MemoryActionCorrect:
		if !admin && target.CreatorUserID != actorUserID {
			return types.NewAppError(types.CodeForbidden,
				"只能纠正自己写入的团队记忆", types.ErrForbidden)
		}
	case types.MemoryActionForget:
		if !admin {
			return types.NewAppError(types.CodeForbidden,
				"只有工作区管理员可以遗忘团队记忆", types.ErrForbidden)
		}
	}
	return nil
}

func nullableMemoryID(action types.MemoryAction) *int64 {
	if action.Action == types.MemoryActionRemember {
		return nil
	}
	return int64Pointer(action.MemoryID)
}

func int64Pointer(value int64) *int64 { return &value }

func sameOptionalInt64(left, right *int64) bool {
	return (left == nil && right == nil) ||
		(left != nil && right != nil && *left == *right)
}
