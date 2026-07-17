// Package sourcecatalog 是「平台 × 能力」的**机器可读能力注册表**（M6 契约 §2）。
//
// 为什么要有这一层，而不是把可用性散落在各处的 switch 注释里（契约 §2.2）：
// 现状把"某能力为什么不能用"写在 const 注释上（tikhub.go 记 get_video_note_detail
// 串号、general 排序坏），**只对读代码的人生效，对 agent 不生效**。三个月后有人看到
// TikHub 有 fetch_search_timeline 且只要 $0.001，会理直气壮加上去，ship 一个 code=200、
// 报文毫无异常的**教科书级静默失败**源。
//
// 本注册表把 (平台, 能力) → {内容种类, 可用状态, 不可用原因} 变成**数据**，被三处共用：
//   - fetcher/multi.go 分发前的门禁（Unavailable 直接带 Reason 报错，不静默改道）
//   - sourcespec 构造源前的门禁（拒绝为不可用能力建源，把 Reason 回给用户/agent）
//   - agent 工具描述（让模型能主动回答"X 关键词搜索暂不支持，原因…"）
//
// 单一事实来源 + 三处共用 = 新增/下线一个能力只改这一张表，不会与分发实现漂移
// （漂移由 fetcher 的一致性测试与 Multi.Fetch 的兜底分支兜住）。
//
// 本包是**纯数据**，只依赖 types：Provider 实现是 fetcher 的内部细节，不进这里
// （契约 §1：供应商既不进 types.Source，也不进身份，也不进本注册表）。
package sourcecatalog

import "github.com/YouToco/vane/types"

// Status 是一个 (平台, 能力) 条目的可用状态。
//
// Unavailable/Unimplemented **必须留在表里而不是删掉**（契约 §2.2）：缺席不带理由，
// 而理由正是防止后人重新踩坑的唯一载体。二者的区别是"能不能做"与"做没做"：
//   - Unavailable  = 上游端点存在，但作为**追新信源**在结构上不可用（实测结论），
//     加了也是坏源。例：x/search（search_type=Latest 排序乱、无法追新）。
//   - Unimplemented = 合理且计划做，只是还没写。当前表里没有这类条目；留作扩展占位。
type Status string

const (
	StatusAvailable     Status = "available"     // 已实现且可用
	StatusUnavailable   Status = "unavailable"   // 上游能力不适合做信源（实测结论），故意不做
	StatusUnimplemented Status = "unimplemented" // 合理但尚未实现
)

// Entry 是能力注册表的一行。
type Entry struct {
	Platform   types.Platform
	Capability types.Capability
	// Kind 是该能力产出的内容种类。落在这里是为了让"这个能力产出什么"可被下游查到
	// （契约 §1：kind 由 capability 推导）；实际写入 content_items 的 Kind 仍由 fetcher
	// 在构造 item 处显式赋值（契约 §7.2(b)，刻意不从本表反查——见该处注释）。
	// 本字段与 fetcher 实际产出的 Kind 的一致性，由 fetcher/kind_test.go 的
	// TestCatalogKindMatchesFetcherEmittedKind 对每个 article 能力逐条比对锁定
	// （web/page_watch 的 change 由 pagewatch 用例覆盖发射侧、本表 KindOf 覆盖登记侧）；
	// 改了这里的 Kind 而没同步 fetcher，该测试会红。Status != Available 时 Kind 无意义，留空。
	Kind types.Kind
	// Status 见上。
	Status Status
	// Reason 在 Status != Available 时**必填**：它会进到用户/agent 看得见的报错与工具
	// 描述里，是"为什么这个能力不能用"的唯一机器可读载体。Available 时留空。
	Reason string
}

// Available 是 Status == StatusAvailable 的便捷判定。
func (e Entry) Available() bool { return e.Status == StatusAvailable }

// key 是注册表的查表键。不导出：外部一律走 Lookup。
type key struct {
	platform   types.Platform
	capability types.Capability
}

// catalog 是全系统唯一的能力事实来源。新增/下线能力只改这里。
//
// 状态依据（契约 §2.1 / §2.2）：
//   - web/feed、web/search、web/page_watch：迁移自 rss/exa + 页面监控，均已实现。
//   - x/user_posts：追 X 官号，已实现（fetcher/x.go）。
//   - x/search：**Unavailable**——上游 search_type=Latest 排序不可靠，无法追新（见 Reason）。
//   - xhs/search：小红书关键词搜索，已实现（fetcher/tikhub.go）。
//   - xhs/user_posts：订阅小红书博主，本次新增（fetcher/xhs_user.go）。
var catalog = map[key]Entry{
	{types.PlatformWeb, types.CapFeed}: {
		Platform: types.PlatformWeb, Capability: types.CapFeed,
		Kind: types.KindArticle, Status: StatusAvailable,
	},
	{types.PlatformWeb, types.CapSearch}: {
		Platform: types.PlatformWeb, Capability: types.CapSearch,
		Kind: types.KindArticle, Status: StatusAvailable,
	},
	{types.PlatformWeb, types.CapPageWatch}: {
		Platform: types.PlatformWeb, Capability: types.CapPageWatch,
		Kind: types.KindChange, Status: StatusAvailable,
	},
	{types.PlatformX, types.CapUserPosts}: {
		Platform: types.PlatformX, Capability: types.CapUserPosts,
		Kind: types.KindArticle, Status: StatusAvailable,
	},
	{types.PlatformX, types.CapSearch}: {
		Platform: types.PlatformX, Capability: types.CapSearch,
		Status: StatusUnavailable,
		// 契约 §2.2 的实测结论：作为信源不可用，作为 lookup（一次性"X 上在聊什么"）才合理。
		Reason: "X 关键词搜索的上游端点排序不可靠（2026-07-16 实测 search_type=Latest 返回 " +
			"2023–2026 乱序、与 Top 有 18/20 重合），无法用于追新，加了也是静默失败的坏源。" +
			"要追某个 X 账号的新动态，请用 x/user_posts（screen_name）。",
	},
	{types.PlatformXHS, types.CapSearch}: {
		Platform: types.PlatformXHS, Capability: types.CapSearch,
		Kind: types.KindArticle, Status: StatusAvailable,
	},
	{types.PlatformXHS, types.CapUserPosts}: {
		Platform: types.PlatformXHS, Capability: types.CapUserPosts,
		Kind: types.KindArticle, Status: StatusAvailable,
	},
}

// Lookup 返回 (平台, 能力) 对应的条目。ok=false 表示这个组合根本不在注册表里
// （未知能力），与"在表里但 Unavailable/Unimplemented"是两回事——调用方据此区分
// "没这个东西"和"有但不能用（带 Reason）"。
func Lookup(platform types.Platform, capability types.Capability) (Entry, bool) {
	e, ok := catalog[key{platform, capability}]
	return e, ok
}

// KindOf 返回该能力产出的内容种类；未注册或不可用时 ok=false。
func KindOf(platform types.Platform, capability types.Capability) (types.Kind, bool) {
	e, ok := catalog[key{platform, capability}]
	if !ok || !e.Available() {
		return "", false
	}
	return e.Kind, true
}

// List 返回全部条目（含 Unavailable/Unimplemented），顺序稳定（按 platform 再 capability）。
// 供 agent 工具描述与运维视图枚举"系统支持哪些信源、哪些暂不支持及原因"。
func List() []Entry {
	out := make([]Entry, 0, len(catalog))
	for _, e := range catalog {
		out = append(out, e)
	}
	// 稳定排序：注册表是展示给人/agent 的，顺序不能每次进程重启就变。
	sortEntries(out)
	return out
}

// sortEntries 按 (platform, capability) 字典序原地排序。手写插入排序避免为一张
// 个位数长度的静态表引入 sort 包与闭包分配；条目数恒定且极小，O(n²) 无所谓。
func sortEntries(es []Entry) {
	for i := 1; i < len(es); i++ {
		for j := i; j > 0 && less(es[j], es[j-1]); j-- {
			es[j], es[j-1] = es[j-1], es[j]
		}
	}
}

func less(a, b Entry) bool {
	if a.Platform != b.Platform {
		return a.Platform < b.Platform
	}
	return a.Capability < b.Capability
}
