package acquisitiontool

import (
	"testing"

	"github.com/YouToco/vane/types"
)

// TestInvariant_UnavailableEntriesHaveReason 锁死契约 §2.2 的核心不变量：
// 任何非 Available 条目**必须带 Reason**——Reason 是"为什么不能用"的唯一机器可读载体，
// 缺了它，Unavailable 就退化成"缺席不带理由"，后人照样重新踩坑。
func TestInvariant_UnavailableEntriesHaveReason(t *testing.T) {
	for _, e := range List() {
		if e.Available() {
			continue
		}
		if e.Reason == "" {
			t.Errorf("%s/%s 状态为 %s 却没有 Reason", e.Platform, e.Capability, e.Status)
		}
	}
}

// TestInvariant_AvailableEntriesHaveKind：Available 条目必须声明 Kind——下游据此推导
// 内容种类；留空会与 fetcher 实际写入的 Kind 不一致。
func TestInvariant_AvailableEntriesHaveKind(t *testing.T) {
	for _, e := range List() {
		if e.Available() && e.Kind == "" {
			t.Errorf("%s/%s 为 Available 却没有 Kind", e.Platform, e.Capability)
		}
	}
}

func TestLookup(t *testing.T) {
	// 已知可用组合。
	if e, ok := Lookup(types.PlatformXHS, types.CapUserPosts); !ok || !e.Available() {
		t.Errorf("xhs/user_posts 应可用，实际 ok=%v entry=%+v", ok, e)
	}
	// web/contents（页面内容监控，Exa /contents）应可用且产出 page_content
	//（Kind 必须区别于 article，否则 Dedup 的近似去重会吞掉页面变化）。
	if e, ok := Lookup(types.PlatformWeb, types.CapContents); !ok || !e.Available() || e.Kind != types.KindPageContent {
		t.Errorf("web/contents 应可用且 Kind=page_content，实际 ok=%v entry=%+v", ok, e)
	}
	// 已知不可用组合（在表里，但 Unavailable）。
	e, ok := Lookup(types.PlatformX, types.CapSearch)
	if !ok {
		t.Fatal("x/search 应在注册表里（Unavailable 是一等条目，不是缺席）")
	}
	if e.Available() {
		t.Error("x/search 应为 Unavailable")
	}
	// 完全未注册的组合。
	if _, ok := Lookup(types.PlatformXHS, types.CapFeed); ok {
		t.Error("xhs/feed 未注册，Lookup 应返回 ok=false")
	}
}

func TestKindOf(t *testing.T) {
	if k, ok := KindOf(types.PlatformXHS, types.CapUserPosts); !ok || k != types.KindArticle {
		t.Errorf("xhs/user_posts 应产 article，实际 ok=%v kind=%q", ok, k)
	}
	// 不可用能力无 Kind。
	if _, ok := KindOf(types.PlatformX, types.CapSearch); ok {
		t.Error("x/search 不可用，KindOf 应返回 ok=false")
	}
}

// TestList_Stable：List 顺序稳定（按 platform 再 capability），供展示/agent 使用不能每次重排。
func TestList_Stable(t *testing.T) {
	a, b := List(), List()
	if len(a) != len(b) {
		t.Fatalf("两次 List 长度不同：%d vs %d", len(a), len(b))
	}
	for i := range a {
		if a[i] != b[i] {
			t.Errorf("第 %d 项不稳定：%+v vs %+v", i, a[i], b[i])
		}
	}
	// 断言确实有序。
	for i := 1; i < len(a); i++ {
		if less(a[i], a[i-1]) {
			t.Errorf("List 未按 (platform,capability) 升序：%+v 在 %+v 之后", a[i], a[i-1])
		}
	}
}
