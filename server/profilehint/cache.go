package profilehint

import (
	"context"
	"errors"
	"log/slog"
	"sync"

	"github.com/YouToco/vane/types"
)

// Cache 按 traceID 缓存画像提示：同一 pipeline（Score 50 次 + CardGen 5 次）
// 共用同一画像快照，画像中途被修改也不会出现"前 30 条按旧画像、后 20 条按新画像"
// 的撕裂打分。FIFO 上限 maxEntries，手动任务运行与定时 pipeline 并跑时互不挤兑。
type Cache struct {
	st Store

	mu      sync.Mutex
	entries map[cacheKey]string
	order   []cacheKey // FIFO 淘汰序（先进先出，非 LRU：per-trace 生命周期短，无需访问提权）
}

type cacheKey struct {
	tenantID int64
	userID   int64
	traceID  string
}

// NewCache 构造缓存。st 由装配层注入（生产为 *store.Store）。
func NewCache(st Store) *Cache {
	return &Cache{
		st:      st,
		entries: make(map[cacheKey]string, maxEntries),
	}
}

// Hint 返回该 trace 的画像提示快照。
//
// 降级铁律（画像是增强不是门槛，绝不返回 error）：
//   - 画像不存在（NotFound）→ ""（首采前的正常态，不告警）；
//   - 其他 DB 错误 → slog.Warn + ""；
//   - 空串同样入缓存：降级结果也是本 trace 的一致快照，且避免 50 次打分反复打失败查询。
//
// 锁内查库是刻意的：同 trace 并发首查只打一次 DB；不同 trace 间短暂互斥，
// 单实例低并发（手动任务运行 + 定时任务）下代价可忽略。
func (c *Cache) Hint(ctx context.Context, userID int64, traceID string) string {
	return c.hint(ctx, cacheKey{userID: userID, traceID: traceID})
}

// HintForTenant returns a profile only when the row belongs to the exact
// tenant/user frozen in a compiled run. Missing exact-reader support degrades
// to an empty hint; it must never fall back to the legacy user-only query.
func (c *Cache) HintForTenant(
	ctx context.Context,
	tenantID int64,
	userID int64,
	traceID string,
) string {
	return c.hint(ctx, cacheKey{tenantID: tenantID, userID: userID, traceID: traceID})
}

func (c *Cache) hint(ctx context.Context, key cacheKey) string {
	c.mu.Lock()
	defer c.mu.Unlock()

	if h, ok := c.entries[key]; ok {
		return h
	}

	hint := c.fetch(ctx, key)
	c.entries[key] = hint
	c.order = append(c.order, key)
	if len(c.order) > maxEntries {
		delete(c.entries, c.order[0])
		copy(c.order, c.order[1:])
		c.order = c.order[:len(c.order)-1]
	}
	return hint
}

// fetch 读画像并渲染，按降级铁律吞掉所有错误。
func (c *Cache) fetch(ctx context.Context, key cacheKey) string {
	var (
		p   *types.Profile
		err error
	)
	if key.tenantID > 0 {
		exact, ok := c.st.(TenantStore)
		if !ok {
			slog.Warn("profilehint: 精确租户画像读取器缺失，降级为空画像",
				"tenant_id", key.tenantID,
				"user_id", key.userID,
				"trace_id", key.traceID)
			return ""
		}
		p, err = exact.GetProfileForTenant(ctx, key.tenantID, key.userID)
	} else {
		p, err = c.st.GetProfile(ctx, key.userID)
	}
	if err == nil {
		return Build(p)
	}
	if errors.Is(err, types.ErrNotFound) {
		return ""
	}
	slog.Warn("profilehint: 画像读取失败，降级为空画像",
		"tenant_id", key.tenantID,
		"user_id", key.userID,
		"trace_id", key.traceID,
		"err", err)
	return ""
}
