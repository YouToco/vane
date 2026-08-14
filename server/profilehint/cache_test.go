package profilehint

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/YouToco/vane/server/types"
)

// fakeStore 可数调用次数的 Store 桩，err 非 nil 时恒返回该错误。
type fakeStore struct {
	mu            sync.Mutex
	calls         int
	tenantCalls   int
	lastTenantID  int64
	lastUserID    int64
	profile       *types.Profile
	tenantProfile *types.Profile
	err           error
}

func (f *fakeStore) GetProfile(_ context.Context, _ int64) (*types.Profile, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	return f.profile, nil
}

func (f *fakeStore) GetProfileForTenant(
	_ context.Context,
	tenantID int64,
	userID int64,
) (*types.Profile, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.tenantCalls++
	f.lastTenantID = tenantID
	f.lastUserID = userID
	if f.err != nil {
		return nil, f.err
	}
	if f.tenantProfile != nil {
		return f.tenantProfile, nil
	}
	return f.profile, nil
}

func (f *fakeStore) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func TestCache_HitDoesNotRequery(t *testing.T) {
	fs := &fakeStore{profile: &types.Profile{Industry: "软件"}}
	c := NewCache(fs)
	ctx := context.Background()

	first := c.Hint(ctx, 1, "trace-a")
	second := c.Hint(ctx, 1, "trace-a")
	if want := "行业：软件"; first != want || second != want {
		t.Errorf("期望两次均返回 %q，实际 %q / %q", want, first, second)
	}
	if got := fs.callCount(); got != 1 {
		t.Errorf("同 trace 命中不应重查，期望 1 次查询，实际 %d", got)
	}
}

func TestCache_HintForTenantUsesExactScopeAndCacheKey(t *testing.T) {
	fs := &fakeStore{tenantProfile: &types.Profile{Industry: "精确租户"}}
	c := NewCache(fs)
	ctx := context.Background()

	if got := c.HintForTenant(ctx, 7, 9, "same-trace"); got != "行业：精确租户" {
		t.Fatalf("exact tenant hint = %q", got)
	}
	if got := c.HintForTenant(ctx, 7, 9, "same-trace"); got != "行业：精确租户" {
		t.Fatalf("cached exact tenant hint = %q", got)
	}
	fs.mu.Lock()
	tenantCalls, calls := fs.tenantCalls, fs.calls
	lastTenantID, lastUserID := fs.lastTenantID, fs.lastUserID
	fs.mu.Unlock()
	if tenantCalls != 1 || calls != 0 || lastTenantID != 7 || lastUserID != 9 {
		t.Fatalf("exact reads=%d legacy reads=%d scope=%d/%d",
			tenantCalls, calls, lastTenantID, lastUserID)
	}

	// The same trace string in another tenant is a distinct cache identity.
	_ = c.HintForTenant(ctx, 8, 9, "same-trace")
	fs.mu.Lock()
	tenantCalls = fs.tenantCalls
	fs.mu.Unlock()
	if tenantCalls != 2 {
		t.Fatalf("cross-tenant cache collision: exact reads=%d, want 2", tenantCalls)
	}
}

type legacyOnlyStore struct{ calls int }

func (s *legacyOnlyStore) GetProfile(context.Context, int64) (*types.Profile, error) {
	s.calls++
	return &types.Profile{Industry: "不得回退"}, nil
}

func TestCache_HintForTenantNeverFallsBackToUserOnlyRead(t *testing.T) {
	st := new(legacyOnlyStore)
	if got := NewCache(st).HintForTenant(context.Background(), 7, 9, "trace"); got != "" {
		t.Fatalf("missing exact reader returned %q, want empty hint", got)
	}
	if st.calls != 0 {
		t.Fatalf("compiled hint performed %d legacy reads", st.calls)
	}
}

func TestCache_SnapshotPerTrace(t *testing.T) {
	fs := &fakeStore{profile: &types.Profile{Industry: "软件"}}
	c := NewCache(fs)
	ctx := context.Background()

	old := c.Hint(ctx, 1, "trace-a")
	fs.mu.Lock()
	fs.profile = &types.Profile{Industry: "金融"}
	fs.mu.Unlock()

	if got := c.Hint(ctx, 1, "trace-a"); got != old {
		t.Errorf("同 trace 应保持快照 %q，实际 %q", old, got)
	}
	if got, want := c.Hint(ctx, 1, "trace-b"), "行业：金融"; got != want {
		t.Errorf("新 trace 应看到新画像 %q，实际 %q", want, got)
	}
}

func TestCache_NotFoundCachesEmpty(t *testing.T) {
	fs := &fakeStore{err: types.NewAppError(types.CodeNotFound, "画像不存在", nil)}
	c := NewCache(fs)
	ctx := context.Background()

	if got := c.Hint(ctx, 1, "trace-a"); got != "" {
		t.Errorf("NotFound 应降级为空串，实际 %q", got)
	}
	_ = c.Hint(ctx, 1, "trace-a")
	if got := fs.callCount(); got != 1 {
		t.Errorf("空串结果也应入缓存，期望 1 次查询，实际 %d", got)
	}
}

func TestCache_DBErrorDegradesAndCaches(t *testing.T) {
	fs := &fakeStore{err: types.NewAppError(types.CodeDatabase, "连接断开", nil)}
	c := NewCache(fs)
	ctx := context.Background()

	if got := c.Hint(ctx, 1, "trace-a"); got != "" {
		t.Errorf("DB 错误应降级为空串（绝不上抛），实际 %q", got)
	}
	_ = c.Hint(ctx, 1, "trace-a")
	if got := fs.callCount(); got != 1 {
		t.Errorf("降级结果也应入缓存，期望 1 次查询，实际 %d", got)
	}
}

func TestCache_FIFOEviction(t *testing.T) {
	fs := &fakeStore{profile: &types.Profile{Industry: "软件"}}
	c := NewCache(fs)
	ctx := context.Background()

	// 填满 16 条后再插第 17 条，最早的 trace-0 应被淘汰。
	for i := 0; i <= maxEntries; i++ {
		c.Hint(ctx, 1, fmt.Sprintf("trace-%d", i))
	}
	base := fs.callCount()
	if base != maxEntries+1 {
		t.Fatalf("前置：期望 %d 次查询，实际 %d", maxEntries+1, base)
	}

	// 未被淘汰的仍命中。
	c.Hint(ctx, 1, fmt.Sprintf("trace-%d", maxEntries))
	c.Hint(ctx, 1, "trace-1")
	if got := fs.callCount(); got != base {
		t.Errorf("留存条目应命中缓存，期望 %d 次查询，实际 %d", base, got)
	}

	// 最早的 trace-0 已被淘汰，再取触发重查。
	c.Hint(ctx, 1, "trace-0")
	if got := fs.callCount(); got != base+1 {
		t.Errorf("FIFO 应淘汰最早条目，期望 %d 次查询，实际 %d", base+1, got)
	}
}

func TestCache_ConcurrentSafe(t *testing.T) {
	fs := &fakeStore{profile: &types.Profile{Industry: "软件"}}
	c := NewCache(fs)
	ctx := context.Background()

	// push_now 与定时 pipeline 并跑的模拟：多 goroutine 混合命中/未命中，
	// -race 下验证无数据竞争，且同 trace 返回值一致。
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				trace := fmt.Sprintf("trace-%d", (g+i)%4)
				if got, want := c.Hint(ctx, 1, trace), "行业：软件"; got != want {
					t.Errorf("并发下期望 %q，实际 %q", want, got)
				}
			}
		}(g)
	}
	wg.Wait()
}
