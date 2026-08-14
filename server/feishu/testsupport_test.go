package feishu

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/server/store"
)

// 本文件是 store/testsupport_test.go 的同构副本，服务本包 DATABASE_URL 门控的
// 集成测试。两处各写一份而非共用：store.Store.pool 未导出，本包用不了那边的
// cleanupExec，而为测试在生产包上开一个导出面并不划算。两处若要改，请一起改。
//
// 各 helper 对应的坑与 store 侧完全相同（详见 store/testsupport_test.go）：
// t.Context() 在 t.Cleanup 前被取消、defer st.Close() 早于 t.Cleanup 把池关掉、
// `_, _ =` 把前两者的必然失败吞成静默。

// cleanupContext 返回测试清理阶段专用的独立 context。
//
// 清理里不能用 t.Context()：自 Go 1.24 起它在 t.Cleanup 注册的函数运行前就被取消。
func cleanupContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 10*time.Second)
}

// cleanupExec 在清理阶段执行一条语句，失败则让测试失败。
//
// 与 store 侧同名函数的唯一差别：这里自建连接而非复用被测的 *store.Store
// （pool 未导出）。清理只跑一条语句，单连接即可，不必建池。
// ctx 应来自 cleanupContext()。
func cleanupExec(ctx context.Context, t *testing.T, dbURL, query string, args ...any) {
	t.Helper()
	conn, err := pgx.Connect(ctx, dbURL)
	if err != nil {
		t.Errorf("清理连库失败: %v", err)
		return
	}
	defer func() { _ = conn.Close(ctx) }()
	if _, err := conn.Exec(ctx, query, args...); err != nil {
		t.Errorf("清理失败: %v\n  SQL: %s", err, query)
	}
}

// registerStoreClose 把 st.Close 注册进 t.Cleanup，取代 defer st.Close()。
//
// 本包的清理走独立连接、不依赖 st 的池，所以顺序当下并非正确性前提；仍统一用它
// 是为了和 store 侧保持同一形态，并且不给日后新增的、真用 st 的清理留下陷阱。
func registerStoreClose(t *testing.T, st *store.Store) {
	t.Helper()
	t.Cleanup(st.Close)
}

// cleanupTestUser 注册清理：删掉测试 open_id 对应的 users 行。
//
// 本包三个 DB 门控测试都必经 UpsertUserByOpenID 写 users，除此之外不落库
// （agent / feedback 侧注入的都是 fake），故按 open_id 删这一行即可，无需 FK 逆序。
// 实测依据：清空测试库后单跑 `go test ./feishu/`，只有 users 表长出 3 行。
func cleanupTestUser(t *testing.T, dbURL, openID string) {
	t.Helper()
	ctx, cancel := cleanupContext()
	t.Cleanup(cancel)
	t.Cleanup(func() {
		cleanupExec(ctx, t, dbURL, `DELETE FROM users WHERE feishu_open_id = $1`, openID)
	})
}
