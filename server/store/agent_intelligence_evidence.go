package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/YouToco/vane/server/types"
	"github.com/jackc/pgx/v5"
)

const (
	agentToolEvidenceSchema = "vane.agent-tool-evidence/v1"
	agentTurnRecordSchema   = "vane.agent-turn-record/v1"
	maxAgentEvidenceBytes   = 256 * 1024
)

// AgentToolEvidenceV1 is the exact result made visible to the model. Result is
// bounded before persistence; OriginalSize records the pre-bound byte count.
type AgentToolEvidenceV1 struct {
	InvocationID string
	ToolCall     types.ToolCall
	ToolName     string
	Arguments    json.RawMessage
	Result       []byte
	OriginalSize int
	TrustType    string
}

// AgentTurnRecordV1 is the user-visible, auditable unit of an Agent exchange.
// It intentionally excludes system prompts, hidden policies and reasoning.
type AgentTurnRecordV1 struct {
	SessionID        int64
	TurnID           string
	TraceID          string
	UserMessage      string
	AssistantMessage string
	ActionReceipts   json.RawMessage
	ToolEvidence     []AgentToolEvidenceV1
}

// AgentTurnReplayV1 is the minimum durable projection required to replay a
// completed authenticated ingress without asking the model or repeating a
// side effect. Hidden prompts, policy and tool bodies are deliberately absent.
type AgentTurnReplayV1 struct {
	UserMessage      string
	AssistantMessage string
}

// FindAgentTurnReplayV1 returns the exact completed turn for a stable trace.
// The authenticated tenant/user scope is enforced again through RLS, so the
// trace never becomes a bearer token. NotFound means the original turn did
// not reach its atomic evidence commit and may reuse the same stable trace.
func (s *Store) FindAgentTurnReplayV1(
	ctx context.Context,
	tenantID, userID int64,
	traceID string,
) (AgentTurnReplayV1, error) {
	if tenantID <= 0 || userID <= 0 || !validEvidenceID(traceID) {
		return AgentTurnReplayV1{}, types.NewAppError(
			types.CodeValidation, "Agent turn 重放范围无效", types.ErrValidation)
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return AgentTurnReplayV1{}, types.NewAppError(
			types.CodeDatabase, "开启 Agent turn 重放事务", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if _, err := tx.Exec(ctx,
		`SET LOCAL search_path = pg_catalog, public, pg_temp`); err != nil {
		return AgentTurnReplayV1{}, types.NewAppError(
			types.CodeDatabase, "固定 Agent turn 重放查询路径", err)
	}
	if _, err := tx.Exec(ctx,
		`SELECT set_config('app.tenant_id',$1,true),
		        set_config('app.user_id',$2,true)`,
		strconv.FormatInt(tenantID, 10), strconv.FormatInt(userID, 10)); err != nil {
		return AgentTurnReplayV1{}, types.NewAppError(
			types.CodeDatabase, "设置 Agent turn 重放身份范围", err)
	}
	if _, err := tx.Exec(ctx, `SET LOCAL ROLE vane_app`); err != nil {
		return AgentTurnReplayV1{}, types.NewAppError(
			types.CodeDatabase, "进入 Agent turn 重放运行角色", err)
	}
	var replay AgentTurnReplayV1
	err = tx.QueryRow(ctx,
		`SELECT user_message,assistant_message
		   FROM agent_turn_records
		  WHERE tenant_id=$1 AND user_id=$2 AND trace_id=$3`,
		tenantID, userID, traceID,
	).Scan(&replay.UserMessage, &replay.AssistantMessage)
	if errors.Is(err, pgx.ErrNoRows) {
		return AgentTurnReplayV1{}, types.NewAppError(
			types.CodeNotFound, "Agent turn 重放记录不存在", types.ErrNotFound)
	}
	if err != nil {
		return AgentTurnReplayV1{}, types.NewAppError(
			types.CodeDatabase, "读取 Agent turn 重放记录", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return AgentTurnReplayV1{}, types.NewAppError(
			types.CodeDatabase, "提交 Agent turn 重放事务", err)
	}
	return replay, nil
}

// CommitAgentTurnRecordV1 atomically seals every model-visible tool result and
// the final user/assistant turn. Exact replay is idempotent; a reused identity
// carrying different bytes fails closed.
func (s *Store) CommitAgentTurnRecordV1(
	ctx context.Context,
	tenantID, userID int64,
	record AgentTurnRecordV1,
) error {
	if err := validateAgentTurnRecord(tenantID, userID, record); err != nil {
		return err
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return types.NewAppError(types.CodeDatabase, "开启 Agent 证据事务", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if _, err := tx.Exec(ctx,
		`SET LOCAL search_path = pg_catalog, public, pg_temp`); err != nil {
		return types.NewAppError(types.CodeDatabase, "固定 Agent 证据查询路径", err)
	}
	if _, err := tx.Exec(ctx,
		`SELECT set_config('app.tenant_id',$1,true),
		        set_config('app.user_id',$2,true)`,
		strconv.FormatInt(tenantID, 10), strconv.FormatInt(userID, 10)); err != nil {
		return types.NewAppError(types.CodeDatabase, "设置 Agent 证据身份范围", err)
	}
	if _, err := tx.Exec(ctx, `SET LOCAL ROLE vane_app`); err != nil {
		return types.NewAppError(types.CodeDatabase, "进入 Agent 证据运行角色", err)
	}
	// Serialize a trace before checking its invocation receipts. Without this,
	// two first deliveries can both miss the evidence row, create two legacy
	// tool_call rows, and only discover the unique evidence key afterwards.
	// A single trace-scoped lock also avoids multi-invocation lock ordering
	// deadlocks while preserving parallelism across independent Agent turns.
	lockKey := fmt.Sprintf("agent-turn-evidence/v1:%d:%d:%s",
		tenantID, userID, record.TraceID)
	if _, err := tx.Exec(ctx,
		`SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, lockKey); err != nil {
		return types.NewAppError(types.CodeDatabase, "锁定 Agent turn 证据幂等范围", err)
	}

	invocationIDs := make([]string, 0, len(record.ToolEvidence))
	for _, evidence := range record.ToolEvidence {
		invocationIDs = append(invocationIDs, evidence.InvocationID)
		toolCallID, err := existingAgentEvidenceToolCallID(
			ctx, tx, tenantID, userID, record.TraceID, evidence.InvocationID)
		if err != nil {
			return err
		}
		if toolCallID == 0 {
			toolCallID, err = insertToolCallTx(ctx, tx, &evidence.ToolCall)
			if err != nil {
				return err
			}
		} else if err := verifyAgentToolCallReplay(
			ctx, tx, toolCallID, &evidence.ToolCall,
		); err != nil {
			return err
		}
		if err := insertAgentToolEvidenceV1(
			ctx, tx, tenantID, userID, record.SessionID, record.TraceID,
			toolCallID, evidence,
		); err != nil {
			return err
		}
	}
	if err := insertAgentTurnRecordV1(
		ctx, tx, tenantID, userID, record, invocationIDs,
	); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return types.NewAppError(types.CodeDatabase, "提交 Agent 证据事务", err)
	}
	return nil
}

func validateAgentTurnRecord(tenantID, userID int64, record AgentTurnRecordV1) error {
	if tenantID <= 0 || userID <= 0 || record.SessionID <= 0 ||
		!validEvidenceID(record.TurnID) || !validEvidenceID(record.TraceID) ||
		len(record.UserMessage) == 0 || len(record.UserMessage) > 65536 ||
		len(record.AssistantMessage) == 0 || len(record.AssistantMessage) > 262144 ||
		len(record.ToolEvidence) > 64 {
		return types.NewAppError(types.CodeValidation,
			"Agent turn 证据范围或大小无效", types.ErrValidation)
	}
	if len(record.ActionReceipts) == 0 {
		record.ActionReceipts = json.RawMessage(`[]`)
	}
	var receipts []json.RawMessage
	if err := json.Unmarshal(record.ActionReceipts, &receipts); err != nil {
		return types.NewAppError(types.CodeValidation,
			"Agent action receipts 必须是 JSON 数组", types.ErrValidation)
	}
	seen := make(map[string]struct{}, len(record.ToolEvidence))
	for i := range record.ToolEvidence {
		evidence := &record.ToolEvidence[i]
		if !validEvidenceID(evidence.InvocationID) ||
			strings.TrimSpace(evidence.ToolName) != evidence.ToolName ||
			len(evidence.ToolName) == 0 || len(evidence.ToolName) > 255 ||
			(evidence.TrustType != "local" && evidence.TrustType != "external") ||
			evidence.OriginalSize < len(evidence.Result) ||
			evidence.OriginalSize < 0 ||
			!json.Valid(evidence.Arguments) || !utf8.Valid(evidence.Result) ||
			bytes.IndexByte(evidence.Result, 0) >= 0 || len(evidence.Result) > maxAgentEvidenceBytes {
			return types.NewAppError(types.CodeValidation,
				"Agent tool 证据字段无效", types.ErrValidation)
		}
		call := &evidence.ToolCall
		if call.TenantID == nil || *call.TenantID != tenantID ||
			call.UserID == nil || *call.UserID != userID ||
			call.SessionID == nil || *call.SessionID != record.SessionID ||
			call.TraceID != record.TraceID || call.ToolName != evidence.ToolName ||
			call.ResultSize != evidence.OriginalSize {
			return types.NewAppError(types.CodeValidation,
				"Agent tool_calls 与 exact turn/evidence 范围不一致", types.ErrValidation)
		}
		callArgs, err := canonicalJSON(call.Arguments)
		if err != nil {
			return types.NewAppError(types.CodeValidation,
				"Agent tool_calls arguments 无效", types.ErrValidation)
		}
		evidenceArgs, _ := canonicalJSON(evidence.Arguments)
		if !bytes.Equal(callArgs, evidenceArgs) {
			return types.NewAppError(types.CodeValidation,
				"Agent tool_calls 与 exact evidence 参数不一致", types.ErrValidation)
		}
		if _, duplicate := seen[evidence.InvocationID]; duplicate {
			return types.NewAppError(types.CodeValidation,
				"Agent tool invocation_id 重复", types.ErrValidation)
		}
		seen[evidence.InvocationID] = struct{}{}
	}
	return nil
}

func validEvidenceID(value string) bool {
	return strings.TrimSpace(value) == value && len(value) >= 1 && len(value) <= 255
}

func insertAgentToolEvidenceV1(
	ctx context.Context,
	tx pgx.Tx,
	tenantID, userID, sessionID int64,
	traceID string,
	toolCallID int64,
	evidence AgentToolEvidenceV1,
) error {
	visible := append([]byte(nil), evidence.Result...)
	originalSize := evidence.OriginalSize
	if originalSize == 0 {
		originalSize = len(evidence.Result)
	}
	digestBytes := sha256.Sum256(visible)
	digest := hex.EncodeToString(digestBytes[:])
	var insertedID int64
	err := tx.QueryRow(ctx,
		`INSERT INTO agent_tool_evidence (
		     tenant_id,user_id,session_id,trace_id,invocation_id,tool_call_id,
		     tool_name,arguments,result_bytes,result_digest,original_size,
		     truncated,trust_type,schema_version
		 ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
		 ON CONFLICT (tenant_id,user_id,trace_id,invocation_id) DO NOTHING
		 RETURNING id`,
		tenantID, userID, sessionID, traceID, evidence.InvocationID,
		toolCallID, evidence.ToolName, evidence.Arguments, visible,
		digest, originalSize, originalSize > len(visible), evidence.TrustType,
		agentToolEvidenceSchema,
	).Scan(&insertedID)
	if err == nil {
		return nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return types.NewAppError(types.CodeDatabase, "写入 Agent tool 证据", err)
	}
	var (
		existingSession int64
		existingCall    *int64
		existingTool    string
		existingArgs    []byte
		existingResult  []byte
		existingDigest  string
		existingSize    int
		existingTrust   string
	)
	err = tx.QueryRow(ctx,
		`SELECT session_id,tool_call_id,tool_name,arguments,result_bytes,
		        result_digest,original_size,trust_type
		   FROM agent_tool_evidence
		  WHERE tenant_id=$1 AND user_id=$2 AND trace_id=$3 AND invocation_id=$4`,
		tenantID, userID, traceID, evidence.InvocationID,
	).Scan(&existingSession, &existingCall, &existingTool, &existingArgs,
		&existingResult, &existingDigest, &existingSize, &existingTrust)
	if err != nil {
		return types.NewAppError(types.CodeDatabase, "核验 Agent tool 证据幂等", err)
	}
	canonicalArgs, _ := canonicalJSON(evidence.Arguments)
	existingCanonicalArgs, _ := canonicalJSON(existingArgs)
	if existingSession != sessionID || existingCall == nil || *existingCall != toolCallID ||
		existingTool != evidence.ToolName || !bytes.Equal(existingCanonicalArgs, canonicalArgs) ||
		!bytes.Equal(existingResult, visible) || existingDigest != digest ||
		existingSize != originalSize || existingTrust != evidence.TrustType {
		return types.NewAppError(types.CodeConflict,
			"Agent tool invocation_id 已被不同证据占用", nil)
	}
	return nil
}

func existingAgentEvidenceToolCallID(
	ctx context.Context,
	tx pgx.Tx,
	tenantID, userID int64,
	traceID, invocationID string,
) (int64, error) {
	var toolCallID int64
	err := tx.QueryRow(ctx,
		`SELECT tool_call_id FROM agent_tool_evidence
		  WHERE tenant_id=$1 AND user_id=$2 AND trace_id=$3 AND invocation_id=$4`,
		tenantID, userID, traceID, invocationID,
	).Scan(&toolCallID)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, types.NewAppError(types.CodeDatabase,
			"查询 Agent tool evidence 幂等引用", err)
	}
	return toolCallID, nil
}

func verifyAgentToolCallReplay(
	ctx context.Context,
	tx pgx.Tx,
	toolCallID int64,
	want *types.ToolCall,
) error {
	var (
		got               types.ToolCall
		tenantID          int64
		userID, sessionID int64
		arguments         []byte
	)
	err := tx.QueryRow(ctx,
		`SELECT run_snapshot_id,tenant_id,trace_id,user_id,session_id,
		        tool_name,tool_kind,provider,endpoint_path,arguments,
		        result_preview,result_size,http_status,error_type,error,
		        duration_ms,retrieval_query,candidate_tools,cost_usd,
		        usage_quantity,source_id
		   FROM tool_calls WHERE id=$1`, toolCallID,
	).Scan(
		&got.RunSnapshotID, &tenantID, &got.TraceID, &userID, &sessionID,
		&got.ToolName, &got.ToolKind, &got.Provider, &got.EndpointPath, &arguments,
		&got.ResultPreview, &got.ResultSize, &got.HTTPStatus, &got.ErrorType,
		&got.Error, &got.DurationMs, &got.RetrievalQuery, &got.CandidateTools,
		&got.CostUSD, &got.UsageQuantity, &got.SourceID,
	)
	if err != nil {
		return types.NewAppError(types.CodeDatabase,
			"读取 Agent tool_calls 幂等回执", err)
	}
	got.ID = 0
	got.TenantID, got.UserID, got.SessionID = &tenantID, &userID, &sessionID
	got.Arguments = json.RawMessage(arguments)
	wantCopy := *want
	wantCopy.ID = 0
	wantCopy.CreatedAt = got.CreatedAt
	wantCopy.Provider = strings.ToLower(strings.TrimSpace(wantCopy.Provider))
	if wantCopy.UsageQuantity <= 0 {
		wantCopy.UsageQuantity = 1
	}
	if wantCopy.CandidateTools == nil {
		wantCopy.CandidateTools = []string{}
	}
	gotArgs, _ := canonicalJSON(got.Arguments)
	wantArgs, _ := canonicalJSON(wantCopy.Arguments)
	got.Arguments, wantCopy.Arguments = gotArgs, wantArgs
	if wantCopy.CostUSD == nil {
		// A nil input may have been priced by the immutable provider rule active
		// at first commit; provider/resource/quantity below are the replay input.
		got.CostUSD = nil
	}
	if !reflect.DeepEqual(got, wantCopy) {
		return types.NewAppError(types.CodeConflict,
			"Agent tool invocation_id 的 tool_calls 回执不一致", nil)
	}
	return nil
}

func insertAgentTurnRecordV1(
	ctx context.Context,
	tx pgx.Tx,
	tenantID, userID int64,
	record AgentTurnRecordV1,
	invocationIDs []string,
) error {
	receipts := record.ActionReceipts
	if len(receipts) == 0 {
		receipts = json.RawMessage(`[]`)
	}
	var insertedID int64
	err := tx.QueryRow(ctx,
		`INSERT INTO agent_turn_records (
		     tenant_id,user_id,session_id,turn_id,trace_id,user_message,
		     assistant_message,tool_invocation_ids,action_receipts,schema_version
		 ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
		 ON CONFLICT (tenant_id,user_id,session_id,turn_id) DO NOTHING
		 RETURNING id`,
		tenantID, userID, record.SessionID, record.TurnID, record.TraceID,
		record.UserMessage, record.AssistantMessage, invocationIDs, receipts,
		agentTurnRecordSchema,
	).Scan(&insertedID)
	if err == nil {
		return nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return types.NewAppError(types.CodeDatabase, "写入 Agent turn 记录", err)
	}
	var (
		existingTrace, existingUser, existingAssistant string
		existingInvocations                            []string
		existingReceipts                               []byte
	)
	err = tx.QueryRow(ctx,
		`SELECT trace_id,user_message,assistant_message,
		        tool_invocation_ids,action_receipts
		   FROM agent_turn_records
		  WHERE tenant_id=$1 AND user_id=$2 AND session_id=$3 AND turn_id=$4`,
		tenantID, userID, record.SessionID, record.TurnID,
	).Scan(&existingTrace, &existingUser, &existingAssistant,
		&existingInvocations, &existingReceipts)
	if err != nil {
		return types.NewAppError(types.CodeDatabase, "核验 Agent turn 幂等", err)
	}
	existingCanonical, _ := canonicalJSON(existingReceipts)
	wantedCanonical, _ := canonicalJSON(receipts)
	if existingTrace != record.TraceID || existingUser != record.UserMessage ||
		existingAssistant != record.AssistantMessage ||
		!equalStrings(existingInvocations, invocationIDs) ||
		!bytes.Equal(existingCanonical, wantedCanonical) {
		return types.NewAppError(types.CodeConflict,
			"Agent turn_id 已被不同记录占用", nil)
	}
	return nil
}

func canonicalJSON(raw []byte) ([]byte, error) {
	var value any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	return json.Marshal(value)
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}
