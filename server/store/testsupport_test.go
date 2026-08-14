package store

import (
	"context"
	"testing"
	"time"
)

// cleanupContext 返回测试清理阶段专用的独立 context。
//
// 清理里**不能**用 t.Context()：自 Go 1.24 起它在 t.Cleanup 注册的函数运行前就被
// 取消，于是每条 DELETE 都以 "context canceled" 失败。配合早先 `_, _ =` 吞错误的
// 写法，清理长期静默失效——测试全绿，数据却泄漏进共享测试库。
func cleanupContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 10*time.Second)
}

// cleanupExec 在清理阶段执行一条语句，失败则让测试失败。
//
// 清理失败必须显性化：它删的是共享测试库里的数据，静默失败的代价是后续所有跑在
// 这个库上的用例都要面对上一轮的残留。ctx 应来自 cleanupContext()。
func cleanupExec(ctx context.Context, t *testing.T, st *Store, query string, args ...any) {
	t.Helper()
	if _, err := st.pool.Exec(ctx, query, args...); err != nil {
		t.Errorf("清理失败: %v\n  SQL: %s", err, query)
	}
}

// registerStoreClose 把 st.Close 注册进 t.Cleanup，必须在注册任何删除清理**之前**调用。
//
// 不能用 defer st.Close()：defer 在测试函数返回时就跑，而 t.Cleanup 注册的清理在那
// 之后才跑，删除清理会拿到一个已关闭的池。t.Cleanup 是 LIFO，本函数先注册，Close
// 反而最后执行，正好让删除清理跑在池关闭之前。
func registerStoreClose(t *testing.T, st *Store) {
	t.Helper()
	t.Cleanup(st.Close)
}
