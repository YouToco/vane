package fetcher

import (
	"strings"
	"testing"

	"github.com/YouToco/vane/types"
)

// 生产实测形态的固定样本。用真实 url 而非 https://example.com/x 是刻意的：
// 这两条 url 的形态（xsec_token 临时票据、RSS 的 ?at_medium= 跟踪参数）正是
// "没有单一字段能通吃"的实证来源，换成玩具 url 就测不到它们。
const (
	xhsNoteID  = "6a3d5c46000000000d00bc00"
	xhsURLA    = "https://www.xiaohongshu.com/explore/6a3d5c46000000000d00bc00?xsec_token=AAAfirst==&xsec_source=pc_search"
	xhsURLB    = "https://www.xiaohongshu.com/explore/6a3d5c46000000000d00bc00?xsec_token=BBBsecond==&xsec_source=pc_search"
	bbcURL     = "https://www.bbc.co.uk/news/articles/c9q28dlyxrzo?at_medium=RSS"
	bbcGUIDOld = "https://www.bbc.co.uk/news/articles/c9q28dlyxrzo#0"
	bbcGUIDNew = "https://www.bbc.co.uk/news/articles/c9q28dlyxrzo#1"
)

func rssSource(id int64) types.Source {
	return types.Source{ID: id, Platform: types.PlatformWeb, Capability: types.CapFeed}
}
func exaSource(id int64) types.Source {
	return types.Source{ID: id, Platform: types.PlatformWeb, Capability: types.CapSearch}
}
func xhsSource(id int64) types.Source {
	return types.Source{ID: id, Platform: types.PlatformXHS, Capability: types.CapSearch}
}

// TestCanonicalKey_Golden 固定三类信源的键输出（契约 §5）。这是签名级断言：键一旦变了，
// 全库存量内容的身份就全变了（007 回填出的旧行再也匹配不上新键，重复行立刻重新长出来），
// 故任何改动都必须在此显式过一遍，并同步改 007 的回填 CASE。
//
// 特别注意 want 是**裸值**：加任何前缀/归一化都会与 007 的
// `CASE WHEN s.type='tikhub_xhs' THEN ci.external_id ELSE ci.url END` 漂移。
func TestCanonicalKey_Golden(t *testing.T) {
	cases := []struct {
		name string
		src  types.Source
		item types.ContentItem
		want string
	}{
		{
			name: "rss 认 url（不认 guid）",
			src:  rssSource(1),
			item: types.ContentItem{SourceID: 1, ExternalID: bbcGUIDOld, URL: bbcURL},
			want: bbcURL,
		},
		{
			name: "exa 同属 url 派",
			src:  exaSource(2),
			item: types.ContentItem{SourceID: 2, ExternalID: "exa-abc123", URL: bbcURL},
			want: bbcURL,
		},
		{
			name: "tikhub_xhs 认 note_id（不认 url）",
			src:  xhsSource(3),
			item: types.ContentItem{SourceID: 3, ExternalID: xhsNoteID, URL: xhsURLA},
			want: xhsNoteID,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := CanonicalKey(tc.src, tc.item); got != tc.want {
				t.Errorf("CanonicalKey = %q\n期望 %q", got, tc.want)
			}
		})
	}
}

// TestCanonicalKey_BBCGuidChangesURLStable 固定 219 行里 13 组冗余的成因：
// BBC 给同一篇文章发多个 guid，13 组的 url 全部相同、external_id 各不同。
// 旧的 UNIQUE(source_id, external_id) 挡不住（有 1 篇因此存了 3 份）。
func TestCanonicalKey_BBCGuidChangesURLStable(t *testing.T) {
	src := rssSource(1)
	old := types.ContentItem{SourceID: 1, ExternalID: bbcGUIDOld, URL: bbcURL}
	// 同一篇文章，BBC 换了个 guid 重发——url 逐字节相同。
	renewed := types.ContentItem{SourceID: 1, ExternalID: bbcGUIDNew, URL: bbcURL}

	if old.ExternalID == renewed.ExternalID {
		t.Fatal("用例前提坏了：两条的 guid 必须不同，否则测不到 guid 漂移")
	}
	k1, k2 := CanonicalKey(src, old), CanonicalKey(src, renewed)
	if k1 != k2 {
		t.Errorf("guid 变但 url 不变时 key 必须一致（否则同一篇存 N 份）：\n%q\n%q", k1, k2)
	}
}

// TestCanonicalKey_XHSTokenChangesNoteIDStable 固定与 RSS 相反的那一半实测事实：
// 小红书 url 带 xsec_token（每次搜索新发的临时票据），同一笔记两次搜到 url 不同，
// 只有 note_id 稳定。若照搬 rss 的 url 规则，同一条笔记每轮都是"新内容"。
func TestCanonicalKey_XHSTokenChangesNoteIDStable(t *testing.T) {
	src := xhsSource(3)
	first := types.ContentItem{SourceID: 3, ExternalID: xhsNoteID, URL: xhsURLA}
	second := types.ContentItem{SourceID: 3, ExternalID: xhsNoteID, URL: xhsURLB}

	if first.URL == second.URL {
		t.Fatal("用例前提坏了：两条的 url 必须不同，否则测不到 xsec_token 漂移")
	}
	k1, k2 := CanonicalKey(src, first), CanonicalKey(src, second)
	if k1 != k2 {
		t.Errorf("xsec_token 变但 note_id 不变时 key 必须一致：\n%q\n%q", k1, k2)
	}
	// 反向保险：键里绝不能掺进 token，否则上面的相等只是巧合。
	if strings.Contains(k1, "xsec_token") || strings.Contains(k1, "AAAfirst") {
		t.Errorf("key 不该包含临时票据：%q", k1)
	}
}

// TestCanonicalKey_CrossSourceSameContent 是多用户才暴露的那条：用户 A 订「AI编程」、
// B 订「AI工具」，同一篇内容命中两个不同的源。per-source 唯一挡不住 → 存两份 +
// 详情补全被付两次钱。全局身份必须与 source_id 无关。
func TestCanonicalKey_CrossSourceSameContent(t *testing.T) {
	// 小红书：同一笔记被两个关键词源搜到（url 因 token 不同还不一样）。
	kA := CanonicalKey(xhsSource(101), types.ContentItem{SourceID: 101, ExternalID: xhsNoteID, URL: xhsURLA})
	kB := CanonicalKey(xhsSource(202), types.ContentItem{SourceID: 202, ExternalID: xhsNoteID, URL: xhsURLB})
	if kA != kB {
		t.Errorf("同一笔记跨源必须同键：\n源101=%q\n源202=%q", kA, kB)
	}

	// rss × exa：Exa 搜到用户 RSS 源里的同一篇文章（guid 与 exa id 天然不同）。
	kRSS := CanonicalKey(rssSource(1), types.ContentItem{SourceID: 1, ExternalID: bbcGUIDOld, URL: bbcURL})
	kExa := CanonicalKey(exaSource(2), types.ContentItem{SourceID: 2, ExternalID: "exa-abc123", URL: bbcURL})
	if kRSS != kExa {
		t.Errorf("同一篇文章跨 rss/exa 必须同键：\nrss=%q\nexa=%q", kRSS, kExa)
	}
}

// TestCanonicalKey_EmptyWhenNoIdentityField 固定契约 §5 的判空口径：拿不到身份字段
// 时返回空串（调用方据此丢弃），**不得兜底成别的键**。
//
// 为什么不兜底：含 source_id 的兜底键跨源必然不同 → 归并失效，等于没重构；含
// content_hash 的兜底键正文一改就变 → 同一篇长出 N 份。两者都是"看着有身份、其实
// 没有"，而调用方会照单全收地落库——比直接丢一条更坏，且没有任何告警。
func TestCanonicalKey_EmptyWhenNoIdentityField(t *testing.T) {
	cases := []struct {
		name string
		src  types.Source
		item types.ContentItem
	}{
		{
			name: "rss 无 url（只有 guid）",
			src:  rssSource(9),
			item: types.ContentItem{SourceID: 9, ExternalID: "guid-only"},
		},
		{
			name: "exa 无 url（只有 title）",
			src:  exaSource(9),
			item: types.ContentItem{SourceID: 9, ExternalID: "exa-1", Title: "有标题没链接"},
		},
		{
			name: "xhs 无 note_id",
			src:  xhsSource(9),
			item: types.ContentItem{SourceID: 9, ExternalID: "", URL: xhsURLA},
		},
		{
			name: "url 只有空白等同于无 url",
			src:  rssSource(9),
			item: types.ContentItem{SourceID: 9, ExternalID: "g", URL: "   "},
		},
		{
			name: "note_id 只有空白等同于无 note_id",
			src:  xhsSource(9),
			item: types.ContentItem{SourceID: 9, ExternalID: "  "},
		},
		{
			name: "未知平台不猜身份字段",
			src:  types.Source{ID: 9, Platform: "carrier_pigeon"},
			item: types.ContentItem{SourceID: 9, ExternalID: "x", URL: bbcURL},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := CanonicalKey(tc.src, tc.item); got != "" {
				t.Errorf("无身份字段时必须返回空串让调用方丢弃，实得 %q", got)
			}
		})
	}
}

// TestCanonicalKey_XHSKeyMatchesGate 是防漂移断言：付费闸门（enrichDescs）用 xhsKey
// 构造查询键，落库用 CanonicalKey——两者若漂移，闸门会静默全 miss，表现不是报错
// 而是每轮为整库老笔记重复付费（$0.01/次）且无告警。
func TestCanonicalKey_XHSKeyMatchesGate(t *testing.T) {
	item := types.ContentItem{SourceID: 3, ExternalID: xhsNoteID, URL: xhsURLA}
	if got, want := CanonicalKey(xhsSource(3), item), xhsKey(xhsNoteID); got != want {
		t.Errorf("落库键与闸门键必须逐字相同：落库=%q 闸门=%q", got, want)
	}
}

// TestCanonicalKey_NoNormalization 锁死"键即 url 原文"。这几对 url 在语义上确实指向
// 同一资源，归一化后能多归并几条——但 007 的回填写的是 `ci.url` 裸值，运行时多做一步
// 归一化就会与存量行算出不同的键，全库内容立刻重新长出一份重复。少归并只是留了份冗余，
// 与回填漂移则是整库翻倍：故这里刻意**不**归一化。要改必须连 007 的 CASE 一起改。
func TestCanonicalKey_NoNormalization(t *testing.T) {
	key := func(u string) string {
		return CanonicalKey(rssSource(1), types.ContentItem{SourceID: 1, ExternalID: "g", URL: u})
	}
	cases := []struct {
		name string
		a, b string
	}{
		{"host 大小写", "https://EXAMPLE.COM/a", "https://example.com/a"},
		{"默认端口", "https://example.com:443/a", "https://example.com/a"},
		{"fragment", "https://example.com/a#comments", "https://example.com/a"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if key(tc.a) == key(tc.b) {
				t.Errorf("键必须是 url 原文（与 007 回填的 ci.url 逐字对齐），不得归一化：%q", key(tc.a))
			}
		})
	}
}

// TestFinalize_AssignsCanonicalKey finalize 必须在落库前就把身份定好——
// 留给 store 或调用方补算就会出现"有的行有键、有的行没键"。
func TestFinalize_AssignsCanonicalKey(t *testing.T) {
	src := rssSource(1)
	item := types.ContentItem{SourceID: 1, ExternalID: bbcGUIDOld, URL: bbcURL, Title: "t", Content: "c",
		Kind: types.KindArticle}

	if !finalize(src, &item) {
		t.Fatal("有 url 的 rss 条目不该被丢弃")
	}
	if item.CanonicalKey != bbcURL {
		t.Errorf("CanonicalKey 未被 finalize 赋值或与直算不符：%q", item.CanonicalKey)
	}
	if item.ContentHash == "" || item.Simhash == nil {
		t.Error("finalize 仍须补齐 content_hash / simhash")
	}
}

// TestFinalize_DropsItemWithoutIdentity 固定契约 §5 的"空 key 必须跳过"：
// 空串在 canonical_key 的 UNIQUE 上会让两条各自无身份的内容互撞成同一条
// （把两篇无关内容合并是不可逆的数据损坏），故宁可丢。
func TestFinalize_DropsItemWithoutIdentity(t *testing.T) {
	// rss 无 link：finalize 必须报废这条，而不是拿 content_hash 兜底成假身份。
	src := rssSource(1)
	item := types.ContentItem{SourceID: 1, Title: "无 guid 无 link", Content: "正文"}

	if finalize(src, &item) {
		t.Fatalf("无 url 的 rss 条目必须被丢弃，实得 canonical_key=%q", item.CanonicalKey)
	}
	if item.CanonicalKey != "" {
		t.Errorf("被丢弃的条目不该带上任何身份：%q", item.CanonicalKey)
	}
}

// TestFinalize_CanonicalKeyPrecedesExternalIDFallback 固定 finalize 内的顺序依赖：
// canonical_key 必须在 external_id 兜底（用 content_hash）之前算。顺序反了的话，
// xhs 缺 note_id 的笔记会拿到 content_hash 当身份——正文一改身份就变，同一笔记
// 长出 N 份，且判空分支永远不触发（bug 完全静默）。
func TestFinalize_CanonicalKeyPrecedesExternalIDFallback(t *testing.T) {
	src := xhsSource(1)
	item := types.ContentItem{SourceID: 1, Title: "没有 note_id 的笔记", Content: "正文"}

	if finalize(src, &item) {
		t.Fatalf("缺 note_id 的 xhs 笔记必须被丢弃，实得 canonical_key=%q", item.CanonicalKey)
	}
	// 反向保险：若 external_id 兜底跑在了前面，key 会等于 content_hash 而非空串。
	if item.CanonicalKey != "" {
		t.Errorf("canonical_key 疑似取了 external_id 的 content_hash 兜底值：%q", item.CanonicalKey)
	}
}

// TestFinalize_ExternalIDFallbackStillApplies 兜底本身没被删——它仍是 001 的约束
// （external_id 不得为空串），只是降级为留档字段、不再参与身份。
func TestFinalize_ExternalIDFallbackStillApplies(t *testing.T) {
	src := rssSource(1)
	item := types.ContentItem{SourceID: 1, URL: bbcURL, Title: "有 link 无 guid", Content: "正文",
		Kind: types.KindArticle}

	if !finalize(src, &item) {
		t.Fatal("有 url 的条目不该被丢弃")
	}
	if item.ExternalID != item.ContentHash {
		t.Errorf("external_id 应兜底为 content_hash，实得 %q", item.ExternalID)
	}
	if item.CanonicalKey != bbcURL {
		t.Errorf("身份仍应是 url，不受 external_id 兜底影响：%q", item.CanonicalKey)
	}
}
