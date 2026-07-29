package tikhubcatalog

import (
	"regexp"
	"strings"
	"testing"
)

// fcNameRe 是 FC 工具名约束（DeepSeek/OpenAI 兼容面，与 gen 的硬校验同一正则）。
var fcNameRe = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,64}$`)

// TestInvariant_CatalogWellFormed 锁死嵌入数据的结构不变量：init 里的 panic 分支
// 生产不可达的前提就是本测试在 CI 拦住损坏的 catalog.json。
func TestInvariant_CatalogWellFormed(t *testing.T) {
	if Len() < 850 {
		// 全量收录（排除平台管理类）应有 ~1000 端点：数量骤降说明 re-gen 时
		// 排除清单误伤或上游 spec 大幅缩水，都需要人工确认而不是静默接受。
		t.Fatalf("注册表仅 %d 个端点，疑似生成损坏", Len())
	}
	seen := make(map[string]bool, Len())
	for _, e := range entries {
		if !fcNameRe.MatchString(e.Name) {
			t.Errorf("端点名 %q 不符合 FC 命名约束", e.Name)
		}
		if seen[e.Name] {
			t.Errorf("端点名重复: %q", e.Name)
		}
		seen[e.Name] = true
		if e.Method != "GET" && e.Method != "POST" {
			t.Errorf("%s: 未知 method %q", e.Name, e.Method)
		}
		if !strings.HasPrefix(e.Path, "/") {
			t.Errorf("%s: path %q 不以 / 开头", e.Name, e.Path)
		}
		if e.Summary == "" {
			t.Errorf("%s: summary 为空（检索质量依赖描述，不允许空 summary 端点入表）", e.Name)
		}
		if e.Platform == "" || e.Platform != strings.ToLower(e.Platform) {
			t.Errorf("%s: platform %q 应为非空小写", e.Name, e.Platform)
		}
		for _, p := range e.Params {
			if p.Name == "" {
				t.Errorf("%s: 存在空名参数", e.Name)
			}
			switch p.In {
			case "query", "path", "body":
			default:
				t.Errorf("%s: 参数 %s 的 in=%q 非法", e.Name, p.Name, p.In)
			}
		}
	}
}

// TestInvariant_ExcludedTagsAbsent：平台管理类 tag 不得出现在注册表
// （Boss 拍板 2026-07-18：排除 TikHub 账户/下载器/Demo/临时邮箱/健康检查/iOS 快捷指令）。
func TestInvariant_ExcludedTagsAbsent(t *testing.T) {
	excluded := []string{"TikHub-User-API", "TikHub-Downloader-API", "Demo-API", "Health-Check", "Temp-Mail-API", "iOS-Shortcut"}
	for _, e := range entries {
		for _, x := range excluded {
			if e.Tag == x {
				t.Errorf("%s: 平台管理类 tag %s 不应入表", e.Name, x)
			}
		}
	}
}

// TestInvariant_ExcludedEndpointsAbsent：精确排除的端点不得进只读查询目录。
// 两类（对抗审查 HIGH + Boss 拍板 2026-07-18）：① 改第三方平台状态的写端点（刷播放/
// 刷浏览/注册设备）——lookup 层免确认直调，混进来 = agent 可无确认刷量；② 越界/社工
// 风险端点（生成发私信唤起链接）。
func TestInvariant_ExcludedEndpointsAbsent(t *testing.T) {
	banned := []string{
		"douyin_app_v3_add_video_play_count",
		"tiktok_app_v3_add_video_play_count",
		"pipixia_app_fetch_increase_post_view_count",
		"douyin_app_v3_register_device",
		"tiktok_web_device_register",
		"douyin_app_v3_open_douyin_app_to_send_private_message",
		"tiktok_app_v3_open_tiktok_app_to_send_private_message",
	}
	for _, name := range banned {
		if _, ok := Lookup(name); ok {
			t.Errorf("排除端点 %s 不应入只读查询目录", name)
		}
	}
}

func TestInvariant_CredentialAndRemoteMutationCapabilitiesAbsent(t *testing.T) {
	if AgentLen() < 850 || AgentLen() >= Len() {
		t.Fatalf("Agent 目录数量异常: agent=%d internal=%d", AgentLen(), Len())
	}
	for _, e := range Entries() {
		normalized := strings.ToLower(e.Name)
		for _, marker := range []string{
			"guest_cookie", "generate_real_mstoken",
			"generate_wss_xb_signature", "fetch_sec_token",
			"generate_a_bogus", "generate_x_bogus", "generate_xbogus",
			"generate_xgnarly", "generate_ttwid", "generate_verify_fp",
			"generate_fingerprint", "generate_hashed_id",
			"generate_s_v_web_id", "generate_x_mssdk_info",
			"register_device", "private_message", "login_request",
			"encrypt_decrypt", "ttencrypt", "decrypt_strdata",
			"encrypt_strdata", "add_video_play_count",
			"increase_post_view_count",
		} {
			if strings.Contains(normalized, marker) {
				t.Errorf("%s: forbidden capability marker %s", e.Name, marker)
			}
		}
		if strings.Contains(strings.ToLower(e.Description), "cookie") {
			t.Errorf("%s: cookie-dependent capability must not enter catalog", e.Name)
		}
		for _, param := range e.Params {
			name := strings.ToLower(strings.TrimSpace(param.Name))
			if strings.Contains(name, "cookie") ||
				strings.Contains(name, "secret") ||
				name == "session_token" ||
				name == "xsec_token" ||
				name == "access_token" ||
				name == "auth_token" ||
				name == "refresh_token" ||
				name == "password" ||
				name == "signature" {
				t.Errorf("%s: forbidden credential parameter %s", e.Name, param.Name)
			}
		}
	}
}

func TestAgentDirectoryExcludesSourceBindingOnlyTokenEndpoint(t *testing.T) {
	const name = "xiaohongshu_web_v3_fetch_note_detail"
	if _, ok := Lookup(name); !ok {
		t.Fatalf("%s must remain available to trusted source binding", name)
	}
	if _, ok := AgentLookup(name); ok {
		t.Fatalf("%s must not be model-callable", name)
	}
	for _, hit := range Search("小红书笔记详情", "xiaohongshu", 20) {
		if hit.Entry.Name == name {
			t.Fatalf("%s leaked through Agent search", name)
		}
	}
}

func TestLookup(t *testing.T) {
	// 已知端点（小红书搜索——现有信源 fetcher 用的同一上游接口，spec 里必然存在）。
	e, ok := Lookup("xiaohongshu_app_v2_search_notes")
	if !ok {
		t.Fatal("xiaohongshu_app_v2_search_notes 应存在")
	}
	if e.Method != "GET" || e.Platform != "xiaohongshu" {
		t.Errorf("端点元数据不符: %+v", e)
	}
	hasKeyword := false
	for _, p := range e.Params {
		if p.Name == "keyword" {
			hasKeyword = true
		}
	}
	if !hasKeyword {
		t.Error("search_notes 应有 keyword 参数")
	}
	if _, ok := Lookup("不存在的端点"); ok {
		t.Error("未知端点名不应命中")
	}
}

// TestInvariant_TwitterEndpointsPreserved locks the complete TikHub Twitter
// keep-set. The provider policy forbids direct X/Twitter APIs, but must never
// remove TikHub endpoints merely because their names or tags contain Twitter.
func TestInvariant_TwitterEndpointsPreserved(t *testing.T) {
	want := []string{
		"twitter_web_fetch_latest_post_comments",
		"twitter_web_fetch_post_comments",
		"twitter_web_fetch_retweet_user_list",
		"twitter_web_fetch_search_timeline",
		"twitter_web_fetch_trending",
		"twitter_web_fetch_tweet_detail",
		"twitter_web_fetch_user_followers",
		"twitter_web_fetch_user_followings",
		"twitter_web_fetch_user_media",
		"twitter_web_fetch_user_post_tweet",
		"twitter_web_fetch_user_profile",
		"twitter_web_fetch_user_tweet_replies",
	}
	wantSet := make(map[string]struct{}, len(want))
	for _, name := range want {
		wantSet[name] = struct{}{}
		entry, ok := Lookup(name)
		if !ok {
			t.Errorf("TikHub Twitter endpoint %q is missing", name)
			continue
		}
		wantPath := "/api/v1/twitter/web/" + strings.TrimPrefix(name, "twitter_web_")
		if entry.Method != "GET" ||
			entry.Path != wantPath ||
			entry.Platform != "twitter" ||
			entry.Tag != "Twitter-Web-API" {
			t.Errorf("%s metadata drifted: %+v", name, entry)
		}
	}

	var got []string
	for _, entry := range entries {
		if entry.Platform != "twitter" {
			continue
		}
		got = append(got, entry.Name)
		if _, ok := wantSet[entry.Name]; !ok {
			t.Errorf("unreviewed TikHub Twitter endpoint %q entered the catalog", entry.Name)
		}
	}
	if len(got) != len(want) {
		t.Errorf("TikHub Twitter endpoint count = %d, want %d; got %v",
			len(got), len(want), got)
	}
}

// TestSearch_Golden 用一组代表性查询锁住检索质量下限：这些用例是「agent 会怎么问」
// 的最小样本，重构分词/打分后必须仍然命中。断言只要求目标端点进 top-K，
// 不锁具体排名——BM25 参数微调不应打红测试。
func TestSearch_Golden(t *testing.T) {
	cases := []struct {
		query    string
		platform string // 空 = 不过滤
		want     string // 期望出现在 top-5 的端点名（前缀匹配）
	}{
		{"抖音 热点 榜单", "", "douyin_billboard_"},
		{"douyin hot search board", "", "douyin_"},
		{"小红书 搜索 笔记", "", "xiaohongshu_"},
		{"tiktok user post video list", "tiktok", "tiktok_"},
		{"知乎 热榜", "", "zhihu_"},
		{"微博 用户 主页", "weibo", "weibo_"},
		{"bilibili 视频 详情", "", "bilibili_"},
		{"youtube search video", "youtube", "youtube_"},
	}
	for _, c := range cases {
		hits := Search(c.query, c.platform, 5)
		if len(hits) == 0 {
			t.Errorf("查询 %q（platform=%q）零命中", c.query, c.platform)
			continue
		}
		found := false
		for _, h := range hits {
			if strings.HasPrefix(h.Entry.Name, c.want) {
				found = true
				break
			}
		}
		if !found {
			names := make([]string, 0, len(hits))
			for _, h := range hits {
				names = append(names, h.Entry.Name)
			}
			t.Errorf("查询 %q top-5 未命中前缀 %q，实际: %v", c.query, c.want, names)
		}
	}
}

// TestSearch_PlatformFilter：平台过滤是硬约束——过滤后不得出现其他平台的端点。
func TestSearch_PlatformFilter(t *testing.T) {
	for _, h := range Search("视频 详情", "douyin", 5) {
		if h.Entry.Platform != "douyin" {
			t.Errorf("platform=douyin 的结果混入 %s（%s）", h.Entry.Platform, h.Entry.Name)
		}
	}
	// 过滤大小写不敏感：模型可能回传 "Douyin"。
	if len(Search("视频", "Douyin", 5)) == 0 {
		t.Error("platform 过滤应大小写不敏感")
	}
}

// TestSearch_Determinism：同一查询两次结果必须逐位一致（结果给模型看，
// 顺序抖动会让会话不可复现、缓存前缀失效）。
func TestSearch_Determinism(t *testing.T) {
	a := Search("抖音 用户 视频 列表", "", 5)
	b := Search("抖音 用户 视频 列表", "", 5)
	if len(a) != len(b) {
		t.Fatalf("两次搜索长度不一: %d vs %d", len(a), len(b))
	}
	for i := range a {
		if a[i].Entry.Name != b[i].Entry.Name {
			t.Errorf("第 %d 位不一致: %s vs %s", i, a[i].Entry.Name, b[i].Entry.Name)
		}
	}
}

func TestSearch_EdgeCases(t *testing.T) {
	if got := Search("", "", 5); got != nil {
		t.Errorf("空查询应返回 nil，实际 %d 条", len(got))
	}
	if got := Search("完全不存在的词汇组合 zzzyyyxxx", "", 5); len(got) != 0 {
		// CJK bigram 可能碰撞出低分命中，允许非空但不允许 panic；这里只记录不失败。
		t.Logf("垃圾查询命中 %d 条（低分碰撞，可接受）", len(got))
	}
	if got := Search("视频", "", 0); got != nil {
		t.Errorf("topK=0 应返回 nil")
	}
	if got := Search("视频", "nonexistent_platform", 5); len(got) != 0 {
		t.Errorf("未知平台过滤应零命中，实际 %d 条", len(got))
	}
}

func TestPlatforms(t *testing.T) {
	ps := Platforms()
	if len(ps) < 15 {
		t.Fatalf("平台数 %d 异常（应 ~20）", len(ps))
	}
	for _, want := range []string{"douyin", "tiktok", "xiaohongshu", "weibo", "bilibili", "zhihu"} {
		if PlatformCount(want) == 0 {
			t.Errorf("核心平台 %s 缺失", want)
		}
	}
	for i := 1; i < len(ps); i++ {
		if ps[i-1] >= ps[i] {
			t.Errorf("Platforms() 应严格字典序: %s >= %s", ps[i-1], ps[i])
		}
	}
}

func TestTokenize(t *testing.T) {
	cases := []struct {
		in   string
		want []string
	}{
		{"fetch_post_detail", []string{"fetch", "post", "detail"}},
		{"热榜数据", []string{"热榜", "榜数", "数据"}},
		{"抖音Web端Hot榜", []string{"抖音", "web", "端", "hot", "榜"}},
		{"", nil},
		{"！！！", nil},
	}
	for _, c := range cases {
		got := tokenize(c.in)
		if len(got) != len(c.want) {
			t.Errorf("tokenize(%q) = %v, want %v", c.in, got, c.want)
			continue
		}
		for i := range got {
			if got[i] != c.want[i] {
				t.Errorf("tokenize(%q)[%d] = %q, want %q", c.in, i, got[i], c.want[i])
			}
		}
	}
}
