package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/server/internal/credentialguard"
	"github.com/YouToco/vane/server/toolsearch"
	"github.com/YouToco/vane/server/types"
)

const (
	maxMemoryTextBytes         = 4096
	maxActiveMemories          = 256
	maxActiveMemoryCorpusBytes = 256 << 10
	maxMemoryRecallQueryBytes  = 512
	maxMemoryRecallResults     = 8
)

// PrepareMemoryAuthorization durably binds the exact owner request and
// authorizer decision digest before an active memory can be created. A crash
// after this call leaves only an unconsumed audit row, never an active memory.
func (s *Store) PrepareMemoryAuthorization(
	ctx context.Context, tenantID, userID, sessionID int64,
	action types.MemoryAction,
) (string, error) {
	if sessionID <= 0 || action.Evidence.AuthorizationID != "" {
		return "", memoryValidationError("长期记忆授权范围无效")
	}
	_, digest, err := validateMemoryAction(strings.Repeat("0", 64), action)
	if err != nil {
		return "", err
	}
	tx, err := s.beginMemoryScopedTx(ctx, tenantID, userID, true)
	if err != nil {
		return "", err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	id := uuid.NewSHA1(uuid.NameSpaceOID, []byte(fmt.Sprintf(
		"vane-memory-authorization/v1:%d:%d:%d:%s:%s",
		tenantID, userID, sessionID, action.Evidence.SourceID, digest,
	))).String()
	_, err = tx.Exec(ctx, `
		INSERT INTO memory_authorizations(
		 id,tenant_id,user_id,session_id,trace_id,owner_request,
		 authorization_digest,request_digest)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8)
		ON CONFLICT (id) DO NOTHING`, id, tenantID, userID, sessionID,
		action.Evidence.SourceID, action.Evidence.OwnerRequest,
		action.Evidence.AuthorizationDigest, digest,
	)
	if err != nil {
		return "", memoryDBError("写入长期记忆授权", err)
	}
	var storedSession int64
	var storedTrace, storedOwner, storedAuthorizationDigest, storedDigest string
	if err := tx.QueryRow(ctx, `
		SELECT session_id,trace_id::text,owner_request,
		       authorization_digest,request_digest
		  FROM memory_authorizations
		 WHERE tenant_id=$1 AND user_id=$2 AND id=$3`,
		tenantID, userID, id,
	).Scan(&storedSession, &storedTrace, &storedOwner,
		&storedAuthorizationDigest, &storedDigest); err != nil {
		return "", memoryDBError("验证长期记忆授权重放", err)
	}
	if storedSession != sessionID || storedTrace != action.Evidence.SourceID ||
		storedOwner != action.Evidence.OwnerRequest ||
		storedAuthorizationDigest != action.Evidence.AuthorizationDigest ||
		storedDigest != digest {
		return "", types.NewAppError(types.CodeConflict,
			"长期记忆授权重放内容不一致", nil)
	}
	if err := tx.Commit(ctx); err != nil {
		return "", memoryDBError("提交长期记忆授权", err)
	}
	return id, nil
}

// ApplyMemoryAction appends one explicit owner-authorized mutation and its
// exact response-loss receipt. It never updates or deletes retained history.
func (s *Store) ApplyMemoryAction(
	ctx context.Context, tenantID, userID int64, idempotencyKey string,
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
	tx, err := s.beginMemoryScopedTx(ctx, tenantID, userID, true)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if result, found, err := loadMemoryReceiptTx(
		ctx, tx, tenantID, userID, idempotencyKey, receiptDigest,
	); err != nil || found {
		return result, err
	}
	if canonical.Evidence.AuthorizationID == "" {
		return nil, memoryValidationError("长期记忆缺少耐久授权")
	}
	var authorizedDigest string
	var consumedEventID *int64
	if err := tx.QueryRow(ctx, `
		SELECT request_digest,consumed_event_id
		  FROM memory_authorizations
		 WHERE tenant_id=$1 AND user_id=$2 AND id=$3
		 FOR UPDATE`, tenantID, userID, canonical.Evidence.AuthorizationID,
	).Scan(&authorizedDigest, &consumedEventID); errors.Is(err, pgx.ErrNoRows) {
		return nil, types.NewAppError(types.CodeConflict,
			"长期记忆授权不存在", nil)
	} else if err != nil {
		return nil, memoryDBError("读取长期记忆授权", err)
	}
	if authorizedDigest != authorizationDigest || consumedEventID != nil {
		return nil, types.NewAppError(types.CodeConflict,
			"长期记忆授权与请求不一致或已消费", nil)
	}

	activeCount, corpusBytes, err := activeMemoryBoundsTx(ctx, tx, tenantID, userID)
	if err != nil {
		return nil, err
	}
	var target types.MemoryRecord
	if canonical.Action != types.MemoryActionRemember {
		target, err = loadActiveMemoryTx(
			ctx, tx, tenantID, userID, canonical.MemoryID)
		if err != nil {
			return nil, err
		}
	}
	if canonical.Action == types.MemoryActionRemember && activeCount >= maxActiveMemories {
		return nil, memoryValidationError("长期记忆条目已达到上限")
	}
	projectedCorpus := corpusBytes
	if canonical.Action == types.MemoryActionRemember {
		projectedCorpus += len(canonical.Text)
	} else if canonical.Action == types.MemoryActionCorrect {
		projectedCorpus += len(canonical.Text) - len(target.Text)
	}
	if projectedCorpus > maxActiveMemoryCorpusBytes {
		return nil, memoryValidationError("长期记忆文本总量已达到上限")
	}

	var resultMemory types.MemoryRecord
	if canonical.Action == types.MemoryActionRemember ||
		canonical.Action == types.MemoryActionCorrect {
		var supersedes any
		if canonical.Action == types.MemoryActionCorrect {
			supersedes = canonical.MemoryID
		}
		err = tx.QueryRow(ctx, `
			INSERT INTO memory_records(
			 tenant_id,user_id,memory_text,evidence_source_type,
			 evidence_source_id,authorization_id,owner_request,authorization_digest,
			 supersedes_memory_id)
			VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)
			RETURNING id,created_at`,
			tenantID, userID, canonical.Text, canonical.Evidence.SourceType,
			canonical.Evidence.SourceID, canonical.Evidence.AuthorizationID,
			canonical.Evidence.OwnerRequest,
			canonical.Evidence.AuthorizationDigest, supersedes,
		).Scan(&resultMemory.ID, &resultMemory.CreatedAt)
		if err != nil {
			return nil, memoryDBError("写入长期记忆记录", err)
		}
		resultMemory.Text = canonical.Text
		resultMemory.Evidence = canonical.Evidence
		resultMemory.Active = true
		if canonical.Action == types.MemoryActionCorrect {
			resultMemory.SupersedesMemoryID = canonical.MemoryID
		}
	} else {
		resultMemory = target
		resultMemory.Active = false
	}

	var targetID, resultID any
	if canonical.Action != types.MemoryActionRemember {
		targetID = canonical.MemoryID
	}
	if canonical.Action != types.MemoryActionForget {
		resultID = resultMemory.ID
	}
	event := types.MemoryEvent{
		Action: canonical.Action, TargetMemoryID: canonical.MemoryID,
		Evidence: canonical.Evidence,
	}
	if canonical.Action != types.MemoryActionForget {
		event.ResultMemoryID = resultMemory.ID
	}
	if canonical.Action == types.MemoryActionRemember {
		event.TargetMemoryID = 0
	}
	err = tx.QueryRow(ctx, `
		INSERT INTO memory_events(
		 tenant_id,user_id,actor_user_id,event_kind,target_memory_id,
		 result_memory_id,evidence_source_type,evidence_source_id,
		 authorization_id,owner_request,authorization_digest)
		VALUES($1,$2,$2,$3,$4,$5,$6,$7,$8,$9,$10)
		RETURNING id,created_at`,
		tenantID, userID, canonical.Action, targetID, resultID,
		canonical.Evidence.SourceType, canonical.Evidence.SourceID,
		canonical.Evidence.AuthorizationID,
		canonical.Evidence.OwnerRequest, canonical.Evidence.AuthorizationDigest,
	).Scan(&event.ID, &event.CreatedAt)
	if err != nil {
		return nil, memoryDBError("写入长期记忆事件", err)
	}
	result := &types.MemoryActionResult{Memory: resultMemory, Event: event}
	payload, err := json.Marshal(result)
	if err != nil {
		return nil, types.NewAppError(types.CodeInternal, "编码长期记忆回执", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO memory_receipts(
		 tenant_id,user_id,idempotency_key,request_digest,event_id,response_payload)
		VALUES($1,$2,$3,$4,$5,$6)`,
		tenantID, userID, idempotencyKey, receiptDigest, event.ID, payload,
	); err != nil {
		return nil, memoryDBError("写入长期记忆回执", err)
	}
	if tag, err := tx.Exec(ctx, `
		UPDATE memory_authorizations SET consumed_event_id=$1
		 WHERE tenant_id=$2 AND user_id=$3 AND id=$4
		   AND consumed_event_id IS NULL`, event.ID, tenantID, userID,
		canonical.Evidence.AuthorizationID,
	); err != nil || tag.RowsAffected() != 1 {
		if err == nil {
			err = errors.New("authorization consumption lost")
		}
		return nil, memoryDBError("消费长期记忆授权", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, memoryDBError("提交长期记忆操作", err)
	}
	return result, nil
}

// RecallMemories authorizes the exact tenant/user before loading any text or
// constructing BM25 state. Forgotten and superseded records never enter the
// corpus and therefore cannot affect scores or leak through ranking.
func (s *Store) RecallMemories(
	ctx context.Context, tenantID, userID int64, query types.MemoryRecallQuery,
) (*types.MemoryRecallResult, error) {
	query.Query = strings.TrimSpace(query.Query)
	if !utf8.ValidString(query.Query) || len(query.Query) == 0 ||
		len(query.Query) > maxMemoryRecallQueryBytes {
		return nil, memoryValidationError("长期记忆查询必须是 1..512 字节 UTF-8 文本")
	}
	if query.Limit < 1 || query.Limit > maxMemoryRecallResults {
		return nil, memoryValidationError("长期记忆查询数量必须在 1..8 之间")
	}
	tx, err := s.beginMemoryScopedTx(ctx, tenantID, userID, false)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	activeCount, corpusBytes, err := activeMemoryBoundsTx(
		ctx, tx, tenantID, userID,
	)
	if err != nil {
		return nil, err
	}
	rows, err := tx.Query(ctx, `
		SELECT r.id,r.memory_text,r.evidence_source_type,
		       r.evidence_source_id::text,r.owner_request,
		       r.authorization_digest,COALESCE(r.supersedes_memory_id,0),
		       r.created_at
		  FROM memory_records r
		 WHERE r.tenant_id=$1 AND r.user_id=$2
		   AND EXISTS (
		       SELECT 1 FROM memory_events introduced
		        WHERE introduced.tenant_id=r.tenant_id
		          AND introduced.user_id=r.user_id
		          AND introduced.result_memory_id=r.id)
		   AND NOT EXISTS (
		       SELECT 1 FROM memory_events consumed
		        WHERE consumed.tenant_id=r.tenant_id
		          AND consumed.user_id=r.user_id
		          AND consumed.target_memory_id=r.id)
		 ORDER BY r.id
		 LIMIT $3`, tenantID, userID, maxActiveMemories+1)
	if err != nil {
		return nil, memoryDBError("读取生效长期记忆", err)
	}
	defer rows.Close()
	records := make([]types.MemoryRecord, 0)
	observedBytes := 0
	for rows.Next() {
		var record types.MemoryRecord
		if err := rows.Scan(
			&record.ID, &record.Text, &record.Evidence.SourceType,
			&record.Evidence.SourceID, &record.Evidence.OwnerRequest,
			&record.Evidence.AuthorizationDigest, &record.SupersedesMemoryID,
			&record.CreatedAt,
		); err != nil {
			return nil, memoryDBError("扫描生效长期记忆", err)
		}
		record.Active = true
		observedBytes += len(record.Text)
		if len(records) >= maxActiveMemories ||
			observedBytes > maxActiveMemoryCorpusBytes {
			return nil, types.NewAppError(
				types.CodeConflict, "长期记忆语料超过安全边界，拒绝建立索引", nil)
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, memoryDBError("扫描生效长期记忆", err)
	}
	if len(records) != activeCount || observedBytes != corpusBytes {
		return nil, types.NewAppError(
			types.CodeConflict, "长期记忆语料在召回期间发生漂移", nil)
	}
	memories, err := rankMemoryRecords(records, query.Query, query.Limit)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, memoryDBError("提交长期记忆读取", err)
	}
	return &types.MemoryRecallResult{Memories: memories}, nil
}

// GetMemory returns one active record after exact tenant/user authorization.
// It exists so an owner-action authorizer can compare the trusted current
// target text with the owner's request before allowing correct/forget. Apply
// still revalidates active state transactionally after that decision.
func (s *Store) GetMemory(
	ctx context.Context, tenantID, userID, memoryID int64,
) (*types.MemoryRecord, error) {
	if memoryID <= 0 {
		return nil, memoryValidationError("长期记忆 ID 无效")
	}
	tx, err := s.beginMemoryScopedTx(ctx, tenantID, userID, false)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	record, err := loadActiveMemoryTx(ctx, tx, tenantID, userID, memoryID)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, memoryDBError("提交长期记忆目标读取", err)
	}
	return &record, nil
}

func rankMemoryRecords(
	records []types.MemoryRecord, query string, limit int,
) ([]types.MemoryRecallItem, error) {
	documents := make([]toolsearch.Document, len(records))
	byID := make(map[string]types.MemoryRecord, len(records))
	for i, record := range records {
		id := strconv.FormatInt(record.ID, 10)
		documents[i] = toolsearch.Document{ID: id, Text: record.Text}
		byID[id] = record
	}
	index, err := toolsearch.New(documents)
	if err != nil {
		return nil, types.NewAppError(types.CodeConflict, "长期记忆索引语料无效", err)
	}
	hits := index.Search(query, limit)
	out := make([]types.MemoryRecallItem, 0, len(hits))
	for _, hit := range hits {
		out = append(out, types.MemoryRecallItem{
			Memory: byID[hit.ID], Score: hit.Score,
		})
	}
	return out, nil
}

func validateMemoryAction(
	idempotencyKey string, action types.MemoryAction,
) (types.MemoryAction, string, error) {
	if !validMemoryDigest(idempotencyKey) {
		return action, "", memoryValidationError("长期记忆幂等键无效")
	}
	if action.Evidence.SourceType != types.MemoryEvidenceOwnerExplicitAgentTurn {
		return action, "", memoryValidationError("长期记忆只接受用户明确指令证据")
	}
	action.Evidence.OwnerRequest = strings.TrimSpace(action.Evidence.OwnerRequest)
	if !utf8.ValidString(action.Evidence.OwnerRequest) ||
		len(action.Evidence.OwnerRequest) == 0 ||
		len(action.Evidence.OwnerRequest) > 65536 ||
		!validMemoryDigest(action.Evidence.AuthorizationDigest) {
		return action, "", memoryValidationError(
			"长期记忆缺少当前用户原话或授权摘要")
	}
	if credentialguard.ContainsCredential(action.Evidence.OwnerRequest) {
		return action, "", memoryValidationError(
			"长期记忆授权原话包含认证凭证")
	}
	parsed, err := uuid.Parse(action.Evidence.SourceID)
	if err != nil || parsed.String() != action.Evidence.SourceID {
		return action, "", memoryValidationError("长期记忆证据 ID 必须是规范 UUID")
	}
	if action.Evidence.AuthorizationID != "" {
		parsedAuthorization, parseErr := uuid.Parse(action.Evidence.AuthorizationID)
		if parseErr != nil || parsedAuthorization.String() != action.Evidence.AuthorizationID {
			return action, "", memoryValidationError(
				"长期记忆授权 ID 必须是规范 UUID")
		}
	}
	if !utf8.ValidString(action.Text) {
		return action, "", memoryValidationError("长期记忆文本必须是有效 UTF-8")
	}
	action.Text = strings.TrimSpace(action.Text)
	switch action.Action {
	case types.MemoryActionRemember:
		if action.MemoryID != 0 || action.Text == "" {
			return action, "", memoryValidationError("remember 参数无效")
		}
	case types.MemoryActionCorrect:
		if action.MemoryID <= 0 || action.Text == "" {
			return action, "", memoryValidationError("correct 参数无效")
		}
	case types.MemoryActionForget:
		if action.MemoryID <= 0 || action.Text != "" {
			return action, "", memoryValidationError("forget 参数无效")
		}
	default:
		return action, "", memoryValidationError("未知的长期记忆操作")
	}
	if len(action.Text) > maxMemoryTextBytes {
		return action, "", memoryValidationError("长期记忆文本不能超过 4096 字节")
	}
	if credentialguard.ContainsCredential(action.Text) {
		return action, "", memoryValidationError("长期记忆不能保存认证凭证")
	}
	if action.Text != "" && len(toolsearch.Tokenize(action.Text)) == 0 {
		return action, "", memoryValidationError("长期记忆文本没有可检索内容")
	}
	digest, err := memoryActionDigest(action, false)
	if err != nil {
		return action, "", err
	}
	return action, digest, nil
}

// memoryActionDigest deliberately has two domains. The durable authorization
// binds the exact semantic action before an authorization ID exists. The
// response-loss receipt additionally binds the exact authorization row that
// was consumed, so replacing that ID can never replay an earlier success.
func memoryActionDigest(action types.MemoryAction, includeAuthorization bool) (string, error) {
	if !includeAuthorization {
		action.Evidence.AuthorizationID = ""
	}
	request, err := json.Marshal(action)
	if err != nil {
		return "", types.NewAppError(types.CodeInternal, "编码长期记忆请求", err)
	}
	digestBytes := sha256.Sum256(request)
	return hex.EncodeToString(digestBytes[:]), nil
}

func validMemoryDigest(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, char := range value {
		if char < '0' || char > '9' {
			if char < 'a' || char > 'f' {
				return false
			}
		}
	}
	return true
}

func (s *Store) beginMemoryScopedTx(
	ctx context.Context, tenantID, userID int64, write bool,
) (pgx.Tx, error) {
	if tenantID <= 0 || userID <= 0 {
		return nil, memoryValidationError("长期记忆用户范围无效")
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, memoryDBError("开启长期记忆事务", err)
	}
	fail := func(err error) (pgx.Tx, error) {
		_ = tx.Rollback(ctx)
		return nil, err
	}
	if _, err := tx.Exec(ctx, `SET LOCAL search_path=pg_catalog,public,pg_temp`); err != nil {
		return fail(memoryDBError("固定长期记忆搜索路径", err))
	}
	if write {
		exists, err := lockTenantAdmissionRoot(ctx, tx, tenantID)
		if err != nil {
			return fail(memoryDBError("锁定长期记忆租户准入", err))
		}
		if !exists {
			return fail(types.NewAppError(types.CodeNotFound, "租户不存在", nil))
		}
	}
	lockMode := "FOR KEY SHARE OF m,t"
	if write {
		lockMode = "FOR UPDATE OF m,t"
	}
	var present bool
	var workspaceKind types.WorkspaceKind
	err = tx.QueryRow(ctx, fmt.Sprintf(`
		SELECT true,t.workspace_kind
		  FROM memberships m JOIN tenants t ON t.id=m.tenant_id
		 WHERE m.tenant_id=$1 AND m.user_id=$2
		   AND t.status='active' AND t.deleted_at IS NULL %s`, lockMode),
		tenantID, userID).Scan(&present, &workspaceKind)
	if errors.Is(err, pgx.ErrNoRows) || !present {
		return fail(types.NewAppError(
			types.CodeNotFound, "用户不属于该长期记忆租户", nil))
	}
	if err != nil {
		return fail(memoryDBError("锁定长期记忆用户授权", err))
	}
	if workspaceKind != types.WorkspaceKindPersonal {
		return fail(types.NewAppError(types.CodeForbidden,
			"团队工作区必须使用团队长期记忆账本", types.ErrForbidden))
	}
	if _, err := tx.Exec(ctx, `
		SELECT set_config('app.tenant_id',$1,true),
		       set_config('app.user_id',$2,true)`,
		strconv.FormatInt(tenantID, 10), strconv.FormatInt(userID, 10)); err != nil {
		return fail(memoryDBError("设置长期记忆授权范围", err))
	}
	if _, err := tx.Exec(ctx, `SET LOCAL ROLE vane_memory_editor`); err != nil {
		return fail(memoryDBError("进入长期记忆最小权限角色", err))
	}
	return tx, nil
}

func activeMemoryBoundsTx(
	ctx context.Context, tx pgx.Tx, tenantID, userID int64,
) (int, int, error) {
	var count, bytes int
	err := tx.QueryRow(ctx, `
		SELECT count(*)::int,COALESCE(sum(octet_length(r.memory_text)),0)::int
		  FROM memory_records r
		 WHERE r.tenant_id=$1 AND r.user_id=$2
		   AND EXISTS (SELECT 1 FROM memory_events e
		                WHERE e.tenant_id=r.tenant_id AND e.user_id=r.user_id
		                  AND e.result_memory_id=r.id)
		   AND NOT EXISTS (SELECT 1 FROM memory_events e
		                    WHERE e.tenant_id=r.tenant_id AND e.user_id=r.user_id
		                      AND e.target_memory_id=r.id)`, tenantID, userID,
	).Scan(&count, &bytes)
	if err != nil {
		return 0, 0, memoryDBError("统计生效长期记忆边界", err)
	}
	if count > maxActiveMemories || bytes > maxActiveMemoryCorpusBytes {
		return 0, 0, types.NewAppError(
			types.CodeConflict, "长期记忆语料超过安全边界", nil)
	}
	return count, bytes, nil
}

func loadActiveMemoryTx(
	ctx context.Context, tx pgx.Tx, tenantID, userID, memoryID int64,
) (types.MemoryRecord, error) {
	var record types.MemoryRecord
	err := tx.QueryRow(ctx, `
		SELECT r.id,r.memory_text,r.evidence_source_type,
		       r.evidence_source_id::text,r.owner_request,
		       r.authorization_digest,COALESCE(r.supersedes_memory_id,0),
		       r.created_at
		  FROM memory_records r
		 WHERE r.tenant_id=$1 AND r.user_id=$2 AND r.id=$3
		   AND EXISTS (SELECT 1 FROM memory_events e
		                WHERE e.tenant_id=r.tenant_id AND e.user_id=r.user_id
		                  AND e.result_memory_id=r.id)
		   AND NOT EXISTS (SELECT 1 FROM memory_events e
		                    WHERE e.tenant_id=r.tenant_id AND e.user_id=r.user_id
		                      AND e.target_memory_id=r.id)`,
		tenantID, userID, memoryID,
	).Scan(&record.ID, &record.Text, &record.Evidence.SourceType,
		&record.Evidence.SourceID, &record.Evidence.OwnerRequest,
		&record.Evidence.AuthorizationDigest, &record.SupersedesMemoryID,
		&record.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return record, types.NewAppError(
			types.CodeConflict, "只能纠正或遗忘当前生效的长期记忆", nil)
	}
	if err != nil {
		return record, memoryDBError("读取目标长期记忆", err)
	}
	record.Active = true
	return record, nil
}

func loadMemoryReceiptTx(
	ctx context.Context, tx pgx.Tx, tenantID, userID int64,
	key, digest string,
) (*types.MemoryActionResult, bool, error) {
	var storedDigest string
	var payload []byte
	err := tx.QueryRow(ctx, `
		SELECT request_digest,response_payload
		  FROM memory_receipts
		 WHERE tenant_id=$1 AND user_id=$2 AND idempotency_key=$3`,
		tenantID, userID, key).Scan(&storedDigest, &payload)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, memoryDBError("读取长期记忆回执", err)
	}
	if storedDigest != digest {
		return nil, false, types.NewAppError(
			types.CodeConflict, "Idempotency-Key 已用于另一长期记忆请求", nil)
	}
	var result types.MemoryActionResult
	if err := json.Unmarshal(payload, &result); err != nil {
		return nil, false, types.NewAppError(types.CodeInternal, "长期记忆回执损坏", err)
	}
	return &result, true, nil
}

func memoryValidationError(message string) error {
	return types.NewAppError(types.CodeValidation, message, types.ErrValidation)
}

func memoryDBError(message string, err error) error {
	return types.NewAppError(types.CodeDatabase, message, err)
}
