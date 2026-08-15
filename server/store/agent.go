package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/server/types"
)

// agentSessionColumns 是 agent_sessions 表全列，SELECT 与 scanAgentSession 一一对应。
const agentSessionColumns = `id, tenant_id, user_id, conversation_scope, status, messages, turn_count, activated_tools, created_at, updated_at`

const ownerConversationScope = "owner"

// scanAgentSession 把一行 agent_sessions 扫进 types.AgentSession（复用于单行与 RETURNING）。
func scanAgentSession(row pgx.Row, as *types.AgentSession) error {
	return row.Scan(
		&as.ID, &as.TenantID, &as.UserID, &as.ConversationScope,
		&as.Status, &as.Messages,
		&as.TurnCount, &as.ActivatedTools, &as.CreatedAt, &as.UpdatedAt,
	)
}

func validAgentConversationScope(scope string) bool {
	if len(scope) < 1 || len(scope) > 128 || scope[0] < 'a' || scope[0] > 'z' {
		return false
	}
	for _, char := range scope[1:] {
		if (char < 'a' || char > 'z') && (char < '0' || char > '9') &&
			char != ':' && char != '_' && char != '-' {
			return false
		}
	}
	return true
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
	return s.GetActiveAgentSessionInScope(ctx, userID, ownerConversationScope, since)
}

// GetActiveAgentSessionInScope returns the active Agent session for one
// server-authoritative conversation scope. Provider IDs must never be used as
// the scope; channel adapters derive it from their internal authenticated route.
func (s *Store) GetActiveAgentSessionInScope(
	ctx context.Context, userID int64, conversationScope string, since time.Time,
) (*types.AgentSession, error) {
	if userID <= 0 || !validAgentConversationScope(conversationScope) {
		return nil, types.NewAppError(types.CodeValidation,
			"Agent 会话范围无效", types.ErrValidation)
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if err != nil {
		return nil, types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("开始查询用户 %d 的活跃 agent 会话", userID), err)
	}
	defer func() {
		_ = tx.Rollback(context.WithoutCancel(ctx))
	}()
	if _, err := tx.Exec(ctx,
		`SET LOCAL search_path = pg_catalog, public`); err != nil {
		return nil, types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("固定用户 %d 的 agent 会话查询路径", userID), err)
	}

	var as types.AgentSession
	err = scanAgentSession(tx.QueryRow(ctx,
		`WITH stale AS (
			UPDATE agent_sessions SET status = $4
			WHERE user_id = $1 AND conversation_scope = $2 AND
			      status = $5 AND updated_at < $3
		 )
		 SELECT `+agentSessionColumns+`
		 FROM agent_sessions
		 WHERE user_id = $1 AND conversation_scope = $2 AND
		       status = $5 AND updated_at >= $3
		 ORDER BY updated_at DESC
		 LIMIT 1`,
		userID, conversationScope, since,
		types.AgentSessionStatusExpired, types.AgentSessionStatusActive), &as)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// Preserve the legacy data-modifying CTE semantics: stale rows
			// remain expired even though the SELECT arm returned no active
			// session. Rolling this transaction back would silently resurrect
			// them on every lookup.
			if commitErr := tx.Commit(ctx); commitErr != nil {
				return nil, types.NewAppError(types.CodeDatabase,
					fmt.Sprintf("提交用户 %d 的 agent 会话过期翻转", userID),
					commitErr)
			}
			return nil, types.NewAppError(types.CodeNotFound,
				fmt.Sprintf("用户 %d 无活跃 agent 会话", userID), err)
		}
		return nil, types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("查询用户 %d 的活跃 agent 会话", userID), err)
	}
	if err := loadAuthoritativeActiveAgentSessionProjection(ctx, tx, &as); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("提交用户 %d 的活跃 agent 会话查询", userID), err)
	}
	return &as, nil
}

// CreateAgentSession 新建 active 会话（messages / turn_count 走表默认 '[]' / 0）。
// RETURNING 全列，调用方拿到的实体恒为数据库当前真实状态。
func (s *Store) CreateAgentSession(ctx context.Context, userID int64) (*types.AgentSession, error) {
	return s.CreateAgentSessionInScope(ctx, userID, ownerConversationScope)
}

// CreateAgentSessionInScope creates an active session inside one authenticated
// conversation scope. Multiple active sessions for different scopes are valid.
func (s *Store) CreateAgentSessionInScope(
	ctx context.Context, userID int64, conversationScope string,
) (*types.AgentSession, error) {
	if userID <= 0 || !validAgentConversationScope(conversationScope) {
		return nil, types.NewAppError(types.CodeValidation,
			"Agent 会话范围无效", types.ErrValidation)
	}
	var as types.AgentSession
	err := scanAgentSession(s.pool.QueryRow(ctx,
		`INSERT INTO agent_sessions (tenant_id,user_id,conversation_scope)
		 VALUES (`+tenantOfUser+`$1),$1,$2)
		 RETURNING `+agentSessionColumns, userID, conversationScope), &as)
	if err != nil {
		return nil, types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("新建 agent 会话（user=%d）", userID), err)
	}
	return &as, nil
}
