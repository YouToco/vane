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
// 的撕裂打分。FIFO 上限 maxEntries，push_now 与定时 pipeline 并跑时互不挤兑。
type Cache struct {
	st Store

	mu      sync.Mutex
	entries map[string]string
	order   []string // FIFO 淘汰序（先进先出，非 LRU：per-trace 生命周期短，无需访问提权）
}

// NewCache 构造缓存。st 由装配层注入（生产为 *store.Store）。
func NewCache(st Store) *Cache {
	return &Cache{
		st:      st,
		entries: make(map[string]string, maxEntries),
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
// 单实例低并发（至多 push_now + 定时两条 pipeline）下代价可忽略。
func (c *Cache) Hint(ctx context.Context, userID int64, traceID string) string {
	c.mu.Lock()
	defer c.mu.Unlock()

	if h, ok := c.entries[traceID]; ok {
		return h
	}

	hint := c.fetch(ctx, userID, traceID)
	c.entries[traceID] = hint
	c.order = append(c.order, traceID)
	if len(c.order) > maxEntries {
		delete(c.entries, c.order[0])
		copy(c.order, c.order[1:])
		c.order = c.order[:len(c.order)-1]
	}
	return hint
}

// fetch 读画像并渲染，按降级铁律吞掉所有错误。
func (c *Cache) fetch(ctx context.Context, userID int64, traceID string) string {
	p, err := c.st.GetProfile(ctx, userID)
	if err == nil {
		return Build(p)
	}
	if errors.Is(err, types.ErrNotFound) {
		return ""
	}
	slog.Warn("profilehint: 画像读取失败，降级为空画像",
		"user_id", userID,
		"trace_id", traceID,
		"err", err)
	return ""
}
