package store

import (
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/YouToco/vane/types"
)

// TestAgentStore 是 DATABASE_URL 门控的集成测试（无则跳过，与 pipeline_store_test.go
// 同一模式），覆盖 M4 agent store（契约 §2）的关键往返：会话建取改、TTL 过期惰性
// 翻转、ClaimPendingAction 原子幂等、过期 claim 拦截、Cancel 语义。
func TestAgentStore(t *testing.T) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("未设置 DATABASE_URL，跳过 agent store 集成测试")
	}
	ctx := t.Context()

	if err := Migrate(ctx, dbURL); err != nil {
		t.Fatalf("Migrate() 执行失败: %v", err)
	}
	st, err := New(ctx, dbURL)
	if err != nil {
		t.Fatalf("New() 建池失败: %v", err)
	}
	defer st.Close()

	// 测试数据用固定前缀 + uuid 后缀，结束时按 FK 逆序清理，避免污染共享测试库。
	u, err := st.UpsertUserByOpenID(ctx, "test_agent_"+uuid.NewString(), "agent-test")
	if err != nil {
		t.Fatalf("UpsertUserByOpenID() 失败: %v", err)
	}

	t.Cleanup(func() {
		// FK 逆序：pending_actions → agent_sessions → users。
		_, _ = st.pool.Exec(ctx, `DELETE FROM pending_actions WHERE user_id = $1`, u.ID)
		_, _ = st.pool.Exec(ctx, `DELETE FROM agent_sessions WHERE user_id = $1`, u.ID)
		_, _ = st.pool.Exec(ctx, `DELETE FROM users WHERE id = $1`, u.ID)
	})

	t.Run("会话建取改", func(t *testing.T) {
		created, err := st.CreateAgentSession(ctx, u.ID)
		if err != nil {
			t.Fatalf("CreateAgentSession() 失败: %v", err)
		}
		if created.Status != types.AgentSessionStatusActive {
			t.Errorf("新建会话 status 应为 active，实际 %q", created.Status)
		}
		if created.TurnCount != 0 {
			t.Errorf("新建会话 turn_count 应为 0，实际 %d", created.TurnCount)
		}

		// TTL 窗口内可取回同一会话。
		since := time.Now().Add(-30 * time.Minute)
		got, err := st.GetActiveAgentSession(ctx, u.ID, since)
		if err != nil {
			t.Fatalf("GetActiveAgentSession() 失败: %v", err)
		}
		if got.ID != created.ID {
			t.Errorf("应取回刚建的会话 %d，实际 %d", created.ID, got.ID)
		}

		msgs := json.RawMessage(`[{"role":"user","content":"你好"},{"role":"assistant","content":"你好，我是见微"}]`)
		if err := st.UpdateAgentSession(ctx, created.ID, msgs, 1); err != nil {
			t.Fatalf("UpdateAgentSession() 失败: %v", err)
		}

		again, err := st.GetActiveAgentSession(ctx, u.ID, since)
		if err != nil {
			t.Fatalf("更新后 GetActiveAgentSession() 失败: %v", err)
		}
		if again.TurnCount != 1 {
			t.Errorf("更新后 turn_count 应为 1，实际 %d", again.TurnCount)
		}
		// JSONB 不保证键序，语义比较而非字符串比较。
		var parsed []map[string]string
		if err := json.Unmarshal(again.Messages, &parsed); err != nil {
			t.Fatalf("回读 messages 解析失败: %v（原文 %s）", err, again.Messages)
		}
		if len(parsed) != 2 || parsed[0]["content"] != "你好" || parsed[1]["role"] != "assistant" {
			t.Errorf("messages 回读不一致: %s", again.Messages)
		}
		// updated_at 必须被刷新（TTL 续期依赖它）。
		if !again.UpdatedAt.After(created.UpdatedAt) {
			t.Errorf("UpdateAgentSession 应刷新 updated_at：建 %v，更 %v", created.UpdatedAt, again.UpdatedAt)
		}

		// 更新不存在的会话应返回 NotFound（防拿着被清理的 id 静默丢消息）。
		if err := st.UpdateAgentSession(ctx, -1, msgs, 2); !errors.Is(err, types.ErrNotFound) {
			t.Errorf("更新不存在会话应 ErrNotFound，实际: %v", err)
		}
	})

	t.Run("TTL过期返回NotFound且状态翻转", func(t *testing.T) {
		// 依赖上一子测试留下的 active 会话。since 取未来时刻，使其 updated_at < since
		// 而被判过期：应返回 NotFound，且状态被惰性翻转为 expired。
		_, err := st.GetActiveAgentSession(ctx, u.ID, time.Now().Add(time.Minute))
		if err == nil {
			t.Fatal("过期会话 GetActiveAgentSession() 应返回错误")
		}
		if !errors.Is(err, types.ErrNotFound) {
			t.Errorf("过期错误应满足 errors.Is(err, types.ErrNotFound)，实际: %v", err)
		}

		// 惰性翻转已发生：该用户不应再有 active 会话，且存在 expired 会话。
		var nActive, nExpired int
		if err := st.pool.QueryRow(ctx,
			`SELECT count(*) FILTER (WHERE status = $2),
			        count(*) FILTER (WHERE status = $3)
			 FROM agent_sessions WHERE user_id = $1`,
			u.ID, types.AgentSessionStatusActive, types.AgentSessionStatusExpired,
		).Scan(&nActive, &nExpired); err != nil {
			t.Fatalf("回查会话状态失败: %v", err)
		}
		if nActive != 0 {
			t.Errorf("过期翻转后不应残留 active 会话，实际 %d 条", nActive)
		}
		if nExpired == 0 {
			t.Error("过期翻转后应存在 status=expired 的会话，实际 0 条")
		}

		// 状态已翻转，即便放宽 since 也不再可取（active 过滤兜底）。
		if _, err := st.GetActiveAgentSession(ctx, u.ID, time.Now().Add(-24*time.Hour)); !errors.Is(err, types.ErrNotFound) {
			t.Errorf("翻转后放宽 since 仍应 ErrNotFound，实际: %v", err)
		}

		// 过期后新开会话，恢复正常取用。
		fresh, err := st.CreateAgentSession(ctx, u.ID)
		if err != nil {
			t.Fatalf("过期后 CreateAgentSession() 失败: %v", err)
		}
		got, err := st.GetActiveAgentSession(ctx, u.ID, time.Now().Add(-30*time.Minute))
		if err != nil {
			t.Fatalf("新会话 GetActiveAgentSession() 失败: %v", err)
		}
		if got.ID != fresh.ID {
			t.Errorf("应取回新会话 %d，实际 %d", fresh.ID, got.ID)
		}
	})

	t.Run("ClaimPendingAction首次成功二次NotFound", func(t *testing.T) {
		sess, err := st.CreateAgentSession(ctx, u.ID)
		if err != nil {
			t.Fatalf("建关联会话失败: %v", err)
		}
		id := uuid.NewString()
		pa := &types.PendingAction{
			ID:        id,
			UserID:    u.ID,
			SessionID: &sess.ID,
			ToolName:  "add_source",
			Args:      json.RawMessage(`{"type":"rss","url":"https://example.com/feed"}`),
			Summary:   "添加 RSS 信源 https://example.com/feed",
			ExpiresAt: time.Now().Add(24 * time.Hour),
		}
		if err := st.CreatePendingAction(ctx, pa); err != nil {
			t.Fatalf("CreatePendingAction() 失败: %v", err)
		}

		// 越权领取（错误 userID）：无副作用拒绝，动作保持 pending。
		if _, err := st.ClaimPendingAction(ctx, id, u.ID+9999); !errors.Is(err, types.ErrNotFound) {
			t.Errorf("非本人领取应 ErrNotFound，实际: %v", err)
		}

		got, err := st.ClaimPendingAction(ctx, id, u.ID)
		if err != nil {
			t.Fatalf("ClaimPendingAction() 首次领取失败（越权拒绝不应产生副作用）: %v", err)
		}
		if got.Status != types.PendingActionStatusExecuted {
			t.Errorf("领取后 status 应为 executed，实际 %q", got.Status)
		}
		if got.ExecutedAt == nil {
			t.Error("领取后 executed_at 应有值")
		}
		if got.UserID != u.ID || got.ToolName != "add_source" || got.Summary != pa.Summary {
			t.Errorf("领取回读不一致: %+v", got)
		}
		if got.SessionID == nil || *got.SessionID != sess.ID {
			t.Errorf("session_id 回读不一致：期望 %d，实际 %v", sess.ID, got.SessionID)
		}
		// 参数以库中为准（防篡改的根基）：JSONB 语义比较。
		var args map[string]string
		if err := json.Unmarshal(got.Args, &args); err != nil {
			t.Fatalf("回读 args 解析失败: %v（原文 %s）", err, got.Args)
		}
		if args["url"] != "https://example.com/feed" || args["type"] != "rss" {
			t.Errorf("args 回读不一致: %s", got.Args)
		}

		// 双击幂等：第二次领取 NotFound。
		if _, err := st.ClaimPendingAction(ctx, id, u.ID); !errors.Is(err, types.ErrNotFound) {
			t.Errorf("二次领取应 ErrNotFound，实际: %v", err)
		}
		// 不存在的 id 同样 NotFound。
		if _, err := st.ClaimPendingAction(ctx, uuid.NewString(), u.ID); !errors.Is(err, types.ErrNotFound) {
			t.Errorf("领取不存在动作应 ErrNotFound，实际: %v", err)
		}
	})

	t.Run("过期claim失败", func(t *testing.T) {
		id := uuid.NewString()
		pa := &types.PendingAction{
			ID:        id,
			UserID:    u.ID,
			ToolName:  "remove_source",
			Args:      json.RawMessage(`{"source_id":1}`),
			ExpiresAt: time.Now().Add(-time.Second), // 已过 expires_at
		}
		if err := st.CreatePendingAction(ctx, pa); err != nil {
			t.Fatalf("CreatePendingAction() 失败: %v", err)
		}
		if _, err := st.ClaimPendingAction(ctx, id, u.ID); !errors.Is(err, types.ErrNotFound) {
			t.Errorf("过期动作领取应 ErrNotFound，实际: %v", err)
		}
		// 记录当前设计：claim 只拦截不翻转 status，过期判定以 expires_at 为准。
		var status types.PendingActionStatus
		if err := st.pool.QueryRow(ctx,
			`SELECT status FROM pending_actions WHERE id = $1`, id).Scan(&status); err != nil {
			t.Fatalf("回查动作状态失败: %v", err)
		}
		if status != types.PendingActionStatusPending {
			t.Errorf("过期 claim 不应改动 status，期望 pending，实际 %q", status)
		}
	})

	t.Run("Cancel语义", func(t *testing.T) {
		id := uuid.NewString()
		pa := &types.PendingAction{
			ID:        id,
			UserID:    u.ID,
			ToolName:  "create_schedule",
			Args:      json.RawMessage(`{"spec":{"cron":"0 8 * * *","tz":"Asia/Shanghai"}}`),
			Summary:   "每天早 8 点推送",
			ExpiresAt: time.Now().Add(24 * time.Hour),
		}
		if err := st.CreatePendingAction(ctx, pa); err != nil {
			t.Fatalf("CreatePendingAction() 失败: %v", err)
		}
		// 越权取消（错误 userID）：拒绝且动作保持 pending。
		if err := st.CancelPendingAction(ctx, id, u.ID+9999); !errors.Is(err, types.ErrNotFound) {
			t.Errorf("非本人取消应 ErrNotFound，实际: %v", err)
		}
		if err := st.CancelPendingAction(ctx, id, u.ID); err != nil {
			t.Fatalf("CancelPendingAction() 失败（越权拒绝不应产生副作用）: %v", err)
		}
		// 取消后不可领取。
		if _, err := st.ClaimPendingAction(ctx, id, u.ID); !errors.Is(err, types.ErrNotFound) {
			t.Errorf("已取消动作领取应 ErrNotFound，实际: %v", err)
		}
		// 重复取消（非 pending）NotFound。
		if err := st.CancelPendingAction(ctx, id, u.ID); !errors.Is(err, types.ErrNotFound) {
			t.Errorf("重复取消应 ErrNotFound，实际: %v", err)
		}
		// 已执行的动作不可取消（先确认后点取消的竞态收敛为 NotFound）。
		id2 := uuid.NewString()
		pa2 := &types.PendingAction{
			ID:        id2,
			UserID:    u.ID,
			ToolName:  "remove_schedule",
			Args:      json.RawMessage(`{"schedule_id":"push-x"}`),
			ExpiresAt: time.Now().Add(24 * time.Hour),
		}
		if err := st.CreatePendingAction(ctx, pa2); err != nil {
			t.Fatalf("CreatePendingAction() 失败: %v", err)
		}
		if _, err := st.ClaimPendingAction(ctx, id2, u.ID); err != nil {
			t.Fatalf("ClaimPendingAction() 失败: %v", err)
		}
		if err := st.CancelPendingAction(ctx, id2, u.ID); !errors.Is(err, types.ErrNotFound) {
			t.Errorf("已执行动作取消应 ErrNotFound，实际: %v", err)
		}
		// 取消不存在的 id 同样 NotFound。
		if err := st.CancelPendingAction(ctx, uuid.NewString(), u.ID); !errors.Is(err, types.ErrNotFound) {
			t.Errorf("取消不存在动作应 ErrNotFound，实际: %v", err)
		}
	})
}
