package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/types"
)

// agentSessionColumns 是 agent_sessions 表全列，SELECT 与 scanAgentSession 一一对应。
const agentSessionColumns = `id, user_id, status, messages, turn_count, activated_tools, created_at, updated_at`

// scanAgentSession 把一行 agent_sessions 扫进 types.AgentSession（复用于单行与 RETURNING）。
func scanAgentSession(row pgx.Row, as *types.AgentSession) error {
	return row.Scan(
		&as.ID, &as.UserID, &as.Status, &as.Messages,
		&as.TurnCount, &as.ActivatedTools, &as.CreatedAt, &as.UpdatedAt,
	)
}

// pendingActionColumns 是 pending_actions 表全列，SELECT 与 scanPendingAction 一一对应。
const pendingActionColumns = `id, user_id, session_id, tool_name, args, summary, status, expires_at, executed_at, created_at`

// scanPendingAction 把一行 pending_actions 扫进 types.PendingAction。
func scanPendingAction(row pgx.Row, pa *types.PendingAction) error {
	return row.Scan(
		&pa.ID, &pa.UserID, &pa.SessionID, &pa.ToolName, &pa.Args,
		&pa.Summary, &pa.Status, &pa.ExpiresAt, &pa.ExecutedAt, &pa.CreatedAt,
	)
}

// GetActiveAgentSession 取该用户最近的 active 会话；updated_at 早于 since 的视为
// 过期并顺带置 status='expired'（惰性过期，不引入后台清理任务）。
// 不存在或已过期均返回 CodeNotFound 的 AppError，errors.Is(err, types.ErrNotFound) 可命中。
//
// 过期翻转与读取用一条语句（数据修改型 CTE）完成：CTE 与主查询共享同一快照，
// 翻转集（updated_at < since）与读取集（>= since）不相交，互不干扰；两个分支都
// 命中 (user_id, status, updated_at DESC) 索引。并发考量：单 owner MVP 下同一
// 用户消息基本串行到达，两条消息同时触发时 UPDATE 靠行锁串行化，后到者空转一次，
// 不会产生错误结果，故不用 SELECT FOR UPDATE。翻转刻意不动 updated_at——
// 保留"最后活跃时间"语义供排查。
func (s *Store) GetActiveAgentSession(ctx context.Context, userID int64, since time.Time) (*types.AgentSession, error) {
	var as types.AgentSession
	err := scanAgentSession(s.pool.QueryRow(ctx,
		`WITH stale AS (
			UPDATE agent_sessions SET status = $3
			WHERE user_id = $1 AND status = $4 AND updated_at < $2
		 )
		 SELECT `+agentSessionColumns+`
		 FROM agent_sessions
		 WHERE user_id = $1 AND status = $4 AND updated_at >= $2
		 ORDER BY updated_at DESC
		 LIMIT 1`,
		userID, since, types.AgentSessionStatusExpired, types.AgentSessionStatusActive), &as)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, types.NewAppError(types.CodeNotFound,
				fmt.Sprintf("用户 %d 无活跃 agent 会话", userID), err)
		}
		return nil, types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("查询用户 %d 的活跃 agent 会话", userID), err)
	}
	return &as, nil
}

// CreateAgentSession 新建 active 会话（messages / turn_count 走表默认 '[]' / 0）。
// RETURNING 全列，调用方拿到的实体恒为数据库当前真实状态。
func (s *Store) CreateAgentSession(ctx context.Context, userID int64) (*types.AgentSession, error) {
	var as types.AgentSession
	err := scanAgentSession(s.pool.QueryRow(ctx,
		`INSERT INTO agent_sessions (tenant_id, user_id) VALUES (`+tenantOfUser+`$1), $1)
		 RETURNING `+agentSessionColumns, userID), &as)
	if err != nil {
		return nil, types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("新建 agent 会话（user=%d）", userID), err)
	}
	return &as, nil
}

// UpdateAgentSession 覆盖写 messages、turn_count 与 activated_tools，并刷新
// updated_at（即续 TTL）。messages/activatedTools 列 NOT NULL，nil/空归一为 '[]'
// （与 InsertSchedule 对 JSONB 列的处理一致）。
// 目标行不存在返回 CodeNotFound：调用方持有的 id 来自 Get/Create，更新不到行
// 说明会话已被并发清理，静默成功会掩盖 bug。
func (s *Store) UpdateAgentSession(ctx context.Context, id int64, messages json.RawMessage, turnCount int, activatedTools json.RawMessage) error {
	if len(messages) == 0 {
		messages = json.RawMessage("[]")
	}
	if len(activatedTools) == 0 {
		activatedTools = json.RawMessage("[]")
	}
	tag, err := s.pool.Exec(ctx,
		`UPDATE agent_sessions
		 SET messages = $2, turn_count = $3, activated_tools = $4, updated_at = now()
		 WHERE id = $1`,
		id, messages, turnCount, activatedTools)
	if err != nil {
		return types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("更新 agent 会话（id=%d）", id), err)
	}
	if tag.RowsAffected() == 0 {
		return types.NewAppError(types.CodeNotFound,
			fmt.Sprintf("agent 会话 id=%d 不存在", id), nil)
	}
	return nil
}

// AppendAgentSessionMessages 原子追加会话消息：msgs 必须是 JSON 数组
// （jsonb || 对两个数组才是拼接语义），在库内一条 UPDATE 完成。
// 不走"读出→内存 append→UpdateAgentSession"：卡片回调与 HandleMessage 分属
// 不同 goroutine，读改写会与 saveSession 的全量覆盖写产生竞态互吞消息，
// 库内原子拼接则与任意时刻的覆盖写都只有先后、没有丢失。
// 刻意不刷 updated_at：会话 TTL 以用户消息计（契约 §0 30 分钟），而确认卡
// 有效期 24h（取消更无时限）——点卡若续期，深夜一次点击就能把陈旧会话
// 连同全部上下文复活给下一条消息。通告落进已过期的会话行无害：新会话
// 历史里本就没有那张卡的"等待确认"记录，不存在要消除的幻觉。
// 目标行不存在返回 CodeNotFound（同 UpdateAgentSession：静默成功会掩盖 bug）。
func (s *Store) AppendAgentSessionMessages(ctx context.Context, sessionID int64, msgs json.RawMessage) error {
	tag, err := s.pool.Exec(ctx,
		`UPDATE agent_sessions
		 SET messages = messages || $2::jsonb
		 WHERE id = $1`,
		sessionID, msgs)
	if err != nil {
		return types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("追加 agent 会话消息（id=%d）", sessionID), err)
	}
	if tag.RowsAffected() == 0 {
		return types.NewAppError(types.CodeNotFound,
			fmt.Sprintf("agent 会话 id=%d 不存在", sessionID), nil)
	}
	return nil
}

// CreatePendingAction 落一条待确认动作。id（uuid）与 expires_at 由调用方给定；
// args NOT NULL DEFAULT '{}'，nil/空归一为 '{}'；status 空缺省 pending。
func (s *Store) CreatePendingAction(ctx context.Context, a *types.PendingAction) error {
	args := a.Args
	if len(args) == 0 {
		args = json.RawMessage("{}")
	}
	status := a.Status
	if status == "" {
		status = types.PendingActionStatusPending
	}
	_, err := s.pool.Exec(ctx,
		`INSERT INTO pending_actions
		     (id, tenant_id, user_id, session_id, tool_name, args, summary, status, expires_at)
		 VALUES ($1, `+tenantOfUser+`$2), $2, $3, $4, $5, $6, $7, $8)`,
		a.ID, a.UserID, a.SessionID, a.ToolName, args, a.Summary, status, a.ExpiresAt)
	if err != nil {
		return types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("插入待确认动作（id=%s, tool=%s）", a.ID, a.ToolName), err)
	}
	return nil
}

// ClaimPendingAction 原子领取：单条 UPDATE ... WHERE status='pending' AND
// expires_at > now() ... RETURNING。并发双击时行锁保证只有一次能改到行，
// 第二次 WHERE 不再命中（status 已 executed）走 NotFound——天然幂等，无需事务。
// 已执行/已取消/已过期/不存在/归属不符五种情况统一返回 CodeNotFound 的 AppError，
// 调用方（agent.ExecuteAction）据此回"人话"文案即可。
// userID 进 WHERE（安全红线 §10）：归属校验在领取原子操作内完成——若先领取后校验，
// 越权请求会把别人的 pending 动作置为 executed（作废），校验前置到谓词则完全无副作用。
// 时间过期只拦截领取，不顺带翻转 status（过期判定以 expires_at 为准，翻不翻转不影响语义）。
func (s *Store) ClaimPendingAction(ctx context.Context, id string, userID int64) (*types.PendingAction, error) {
	var pa types.PendingAction
	err := scanPendingAction(s.pool.QueryRow(ctx,
		`UPDATE pending_actions
		 SET status = $2, executed_at = now()
		 WHERE id = $1 AND user_id = $4 AND status = $3 AND expires_at > now()
		 RETURNING `+pendingActionColumns,
		id, types.PendingActionStatusExecuted, types.PendingActionStatusPending, userID), &pa)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, types.NewAppError(types.CodeNotFound,
				fmt.Sprintf("待确认动作 id=%s 不可领取（不存在/已执行/已取消/已过期/非本人）", id), err)
		}
		return nil, types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("领取待确认动作（id=%s）", id), err)
	}
	return &pa, nil
}

// CancelPendingAction pending → cancelled，RETURNING 全列（调用方回写会话通告
// 需要 Summary 等字段）；非 pending（已执行/已取消/不存在）或归属不符返回
// CodeNotFound 的 AppError。与 Claim 同样用单条条件 UPDATE 保证原子性，
// userID 同样进 WHERE（安全红线 §10）。
// 刻意不校验 expires_at：已超时但 status 仍为 pending 的动作允许取消——
// 语义无害，且用户点"取消"永远能得到明确反馈而非"动作不存在"。
func (s *Store) CancelPendingAction(ctx context.Context, id string, userID int64) (*types.PendingAction, error) {
	var pa types.PendingAction
	err := scanPendingAction(s.pool.QueryRow(ctx,
		`UPDATE pending_actions SET status = $2
		 WHERE id = $1 AND user_id = $4 AND status = $3
		 RETURNING `+pendingActionColumns,
		id, types.PendingActionStatusCancelled, types.PendingActionStatusPending, userID), &pa)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, types.NewAppError(types.CodeNotFound,
				fmt.Sprintf("待确认动作 id=%s 非 pending 或非本人，无法取消", id), err)
		}
		return nil, types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("取消待确认动作（id=%s）", id), err)
	}
	return &pa, nil
}
