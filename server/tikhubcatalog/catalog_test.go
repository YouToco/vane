package tikhubcatalog

import (
	"encoding/json"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"testing"

	"github.com/YouToco/vane/server/toolsearch"
)

// fcNameRe 是 FC 工具名约束（DeepSeek/OpenAI 兼容面，与 gen 的硬校验同一正则）。
var fcNameRe = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,64}$`)

// TestInvariant_CatalogWellFormed 锁死嵌入数据的结构不变量：init 里的 panic 分支
// 生产不可达的前提就是本测试在 CI 拦住损坏的 catalog.json。
func TestInvariant_CatalogWellFormed(t *testing.T) {
	// The reviewed upstream snapshot had 995 entries. Commit 1630f82 removed
	// 14 credential/encryption capabilities from even the trusted internal
	// registry, leaving 981. The stricter model directory admits 883.
	if Len() != 981 || AgentLen() != 883 {
		t.Fatalf("catalog cardinality drifted: internal=%d want 981, agent=%d want 883", Len(), AgentLen())
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

func TestAgentCatalogDigestAndOrderAreStable(t *testing.T) {
	modelEntries := make([]toolsearch.Entry, 0, len(agentEntries))
	documents := make([]toolsearch.Document, 0, len(agentEntries))
	for i := len(agentEntries) - 1; i >= 0; i-- {
		entry := agentEntries[i]
		modelEntries = append(modelEntries, modelToolEntry(entry))
		documents = append(documents, toolsearch.Document{ID: entry.Name, Text: docText(entry)})
	}
	rebuilt, err := toolsearch.NewCatalogWithDocuments(modelEntries, documents)
	if err != nil {
		t.Fatal(err)
	}
	if rebuilt.Digest() != AgentCatalogDigest() {
		t.Fatalf("input order changed digest: %s != %s", rebuilt.Digest(), AgentCatalogDigest())
	}
	const wantDigest = "13d787acac3cce2cf30290173747258183004135064476069341f9d103cee31e"
	if AgentCatalogDigest() != wantDigest {
		t.Fatalf("agent catalog digest = %s, want %s", AgentCatalogDigest(), wantDigest)
	}
	for _, query := range []string{"抖音 热点 榜单", "youtube search video", "小红书 搜索 笔记"} {
		got, err := agentCatalog.Search(query, 5)
		if err != nil {
			t.Fatal(err)
		}
		want, err := rebuilt.Search(query, 5)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("reordered catalog changed Search(%q): %#v != %#v", query, got, want)
		}
	}
}

func TestAgentCatalogIndexesOnlyEligibleDirectory(t *testing.T) {
	documents := make([]toolsearch.Document, len(agentEntries))
	for i, entry := range agentEntries {
		documents[i] = toolsearch.Document{ID: entry.Name, Text: docText(entry)}
	}
	authorizedOnly, err := toolsearch.New(documents)
	if err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{"video detail", "用户 主页", "search notes"} {
		want := authorizedOnly.Search(query, 8)
		got, err := agentCatalog.SearchFiltered(query, 8, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != len(want) {
			t.Fatalf("Search(%q) length = %d, want %d", query, len(got), len(want))
		}
		for i := range want {
			if got[i].Entry.Name != want[i].ID || got[i].Score != want[i].Score {
				t.Fatalf("Search(%q)[%d] = %s/%v, want %s/%v", query, i,
					got[i].Entry.Name, got[i].Score, want[i].ID, want[i].Score)
			}
		}
	}
}

func TestAgentDefinitionIsAuthorizedCompleteAndDefensive(t *testing.T) {
	const name = "xiaohongshu_app_v2_search_notes"
	definition, ok := AgentDefinition(name)
	if !ok {
		t.Fatalf("AgentDefinition(%q) not found", name)
	}
	if definition.Name != name || definition.Namespace != "social/xiaohongshu" || definition.Description == "" {
		t.Fatalf("AgentDefinition(%q) = %#v", name, definition)
	}
	var schema struct {
		Type       string                     `json:"type"`
		Properties map[string]json.RawMessage `json:"properties"`
	}
	if err := json.Unmarshal(definition.Parameters, &schema); err != nil {
		t.Fatal(err)
	}
	if schema.Type != "object" || len(schema.Properties) == 0 || schema.Properties["keyword"] == nil {
		t.Fatalf("model schema is incomplete: %s", definition.Parameters)
	}
	definition.Parameters[0] = '!'
	again, ok := AgentDefinition(name)
	if !ok || again.Parameters[0] != '{' {
		t.Fatal("AgentDefinition returned mutable catalog storage")
	}

	for _, excluded := range []string{
		"xiaohongshu_web_v3_fetch_note_detail",
		"douyin_web_fetch_douyin_web_guest_cookie",
	} {
		if _, ok := AgentDefinition(excluded); ok {
			t.Fatalf("excluded tool %q has a model definition", excluded)
		}
	}
}

func TestAgentDefinitionsExcludeProviderTransportMetadata(t *testing.T) {
	for _, entry := range agentEntries {
		definition, ok := AgentDefinition(entry.Name)
		if !ok {
			t.Fatalf("eligible tool %q has no model definition", entry.Name)
		}
		if definition.Namespace != "social/"+entry.Platform || definition.Name != entry.Name {
			t.Fatalf("model identity %q drifted: namespace=%q name=%q", entry.Name, definition.Namespace, definition.Name)
		}
		if !reflect.DeepEqual(definition.Tags, []string{entry.Platform}) {
			t.Fatalf("model definition %q leaked provider tag %q as %#v", entry.Name, entry.Tag, definition.Tags)
		}
		for _, visible := range append([]string{
			definition.Namespace, definition.Name, definition.Description, string(definition.Parameters),
		}, definition.Tags...) {
			if containsProviderTransport(visible) {
				t.Fatalf("model definition %q leaked provider transport metadata in %q", entry.Name, visible)
			}
		}
	}
}

func TestModelDefinitionRecursivelyRemovesProviderPaths(t *testing.T) {
	entry, ok := Lookup("weibo_web_v2_fetch_city_list")
	if !ok {
		t.Fatal("known Weibo city endpoint is missing")
	}
	entry.Description = "Endpoint path: /api/v1/weibo/web/v2/fetch_city_list\nSafe city mapping description"
	entry.Params = append(entry.Params, Param{
		Name: "options", Type: "object",
		Desc: "Request method GET, endpoint path /api/v1/weibo/city",
		Default: map[string]any{
			"safe":          "city",
			"endpoint_path": "/api/v1/weibo/city",
			"nested":        []any{"region", "https://api.tikhub.io/api/v1/weibo/city"},
		},
		Enum: []any{map[string]any{
			"safe":  "province",
			"route": []any{"/api/v1/weibo/province", "district"},
		}},
	})
	definition := modelToolEntry(entry)
	visible := definition.Description + "\n" + strings.Join(definition.Tags, "\n") + "\n" + string(definition.Parameters)
	if containsProviderTransport(visible) || strings.Contains(strings.ToLower(visible), strings.ToLower(entry.Tag)) {
		t.Fatalf("Weibo city definition leaked provider metadata: %s", visible)
	}
	for _, retained := range []string{"Safe city mapping description", `"safe":"city"`, `"safe":"province"`, "district"} {
		if !strings.Contains(visible, retained) {
			t.Fatalf("recursive scrub removed safe business value %q: %s", retained, visible)
		}
	}
}

func TestKnownWeiboSearchDefinitionRemovesCityEndpointPaths(t *testing.T) {
	definition, ok := AgentDefinition("weibo_web_v2_fetch_user_search")
	if !ok {
		t.Fatal("known Weibo user search definition is missing")
	}
	visible := definition.Description + "\n" + strings.Join(definition.Tags, "\n") + "\n" + string(definition.Parameters)
	for _, forbidden := range []string{"/fetch_city_list", "/city_list", "Weibo-Web-V2-API"} {
		if strings.Contains(strings.ToLower(visible), strings.ToLower(forbidden)) {
			t.Fatalf("Weibo user search definition leaked %q: %s", forbidden, visible)
		}
	}
}

func TestModelDefinitionsPreservePublicBusinessURLFormats(t *testing.T) {
	for _, test := range []struct {
		name string
		want string
	}{
		{name: "wechat_mp_v2_fetch_article_detail", want: "https://mp.weixin.qq.com/s/"},
		{name: "youtube_web_get_channel_id_v2", want: "https://www.youtube.com/channel/"},
	} {
		definition, ok := AgentDefinition(test.name)
		if !ok {
			t.Fatalf("AgentDefinition(%q) is missing", test.name)
		}
		visible := definition.Description + "\n" + string(definition.Parameters)
		if !strings.Contains(visible, test.want) {
			t.Fatalf("AgentDefinition(%q) removed public business URL format %q: %s", test.name, test.want, visible)
		}
		if containsProviderTransport(test.want) {
			t.Fatalf("public business URL %q classified as provider transport", test.want)
		}
	}
}

func TestProviderEntryAPIsReturnDeepCopies(t *testing.T) {
	const defaultName = "douyin_billboard_fetch_hot_account_list"
	getters := map[string]func() (Entry, bool){
		"Lookup":      func() (Entry, bool) { return Lookup(defaultName) },
		"AgentLookup": func() (Entry, bool) { return AgentLookup(defaultName) },
		"Entries": func() (Entry, bool) {
			for _, entry := range Entries() {
				if entry.Name == defaultName {
					return entry, true
				}
			}
			return Entry{}, false
		},
		"Search": func() (Entry, bool) {
			for _, hit := range Search("douyin billboard hot account list", "douyin", 50) {
				if hit.Entry.Name == defaultName {
					return hit.Entry, true
				}
			}
			return Entry{}, false
		},
	}
	for name, getter := range getters {
		entry, ok := getter()
		if !ok {
			t.Fatalf("%s did not return %s", name, defaultName)
		}
		if !mutateProviderEntryForTest(&entry) {
			t.Fatalf("fixture %s has no compound default", entry.Name)
		}
		again, ok := getter()
		if !ok {
			t.Fatalf("%s lost %s after caller mutation", name, defaultName)
		}
		assertProviderEntryUnmutated(t, name, again)
	}

	const enumName = "douyin_index_fetch_brand_cycles"
	enumEntry, ok := Lookup(enumName)
	if !ok {
		t.Fatalf("enum fixture %s is unavailable", enumName)
	}
	enumIndex := -1
	for i := range enumEntry.Params {
		if len(enumEntry.Params[i].Enum) > 0 {
			enumIndex = i
			break
		}
	}
	if enumIndex < 0 {
		t.Fatalf("enum fixture %s has no enum", enumName)
	}
	enumEntry.Params[enumIndex].Enum[0] = "mutated"
	again, ok := Lookup(enumName)
	if !ok || again.Params[enumIndex].Enum[0] == "mutated" {
		t.Fatal("Param.Enum mutation polluted the provider catalog")
	}
}

func TestProviderEntryCopiesAreConcurrentMutationSafe(t *testing.T) {
	const name = "douyin_billboard_fetch_hot_account_list"
	var wait sync.WaitGroup
	for i := 0; i < 16; i++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for j := 0; j < 50; j++ {
				entry, ok := Lookup(name)
				if !ok {
					t.Errorf("Lookup(%q) failed", name)
					return
				}
				if !mutateProviderEntryForTest(&entry) {
					t.Errorf("fixture %s has no compound default", entry.Name)
					return
				}
				entry, ok = AgentLookup(name)
				if !ok {
					t.Errorf("AgentLookup(%q) failed", name)
					return
				}
				if !mutateProviderEntryForTest(&entry) {
					t.Errorf("fixture %s has no compound default", entry.Name)
					return
				}
				found := false
				for _, candidate := range Entries() {
					if candidate.Name == name {
						found = mutateProviderEntryForTest(&candidate)
						break
					}
				}
				if !found {
					t.Errorf("Entries() did not return mutable fixture %s", name)
					return
				}
				found = false
				for _, hit := range Search("douyin billboard hot account list", "douyin", 50) {
					if hit.Entry.Name == name {
						found = mutateProviderEntryForTest(&hit.Entry)
						break
					}
				}
				if !found {
					t.Errorf("Search() did not return mutable fixture %s", name)
					return
				}
			}
		}()
	}
	wait.Wait()
	entry, ok := Lookup(name)
	if !ok {
		t.Fatal("provider entry disappeared")
	}
	assertProviderEntryUnmutated(t, "concurrent APIs", entry)
}

func mutateProviderEntryForTest(entry *Entry) bool {
	if len(entry.Params) == 0 {
		return false
	}
	entry.Params[0].Name = "mutated"
	for i := range entry.Params {
		object, ok := entry.Params[i].Default.(map[string]any)
		if ok {
			object["mutated"] = true
			return true
		}
		values, ok := entry.Params[i].Default.([]any)
		if ok && len(values) > 0 {
			values[0] = "mutated"
			return true
		}
	}
	return false
}

func assertProviderEntryUnmutated(t *testing.T, source string, entry Entry) {
	t.Helper()
	if len(entry.Params) == 0 || entry.Params[0].Name == "mutated" {
		t.Fatalf("%s returned polluted Params", source)
	}
	for _, parameter := range entry.Params {
		object, ok := parameter.Default.(map[string]any)
		if ok && object["mutated"] == true {
			t.Fatalf("%s returned polluted compound Default", source)
		}
		values, ok := parameter.Default.([]any)
		if ok && len(values) > 0 && values[0] == "mutated" {
			t.Fatalf("%s returned polluted compound Default", source)
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

func TestSearchToolsReturnsCompleteDefinitionsWithLegacyRanking(t *testing.T) {
	for _, test := range []struct {
		query    string
		platform string
	}{
		{query: "抖音 热点 榜单"},
		{query: "tiktok user post video list", platform: "tiktok"},
		{query: "youtube search video", platform: "youtube"},
	} {
		matches, err := SearchTools(test.query, test.platform, 5)
		if err != nil {
			t.Fatalf("SearchTools(%q): %v", test.query, err)
		}
		legacy := Search(test.query, test.platform, 5)
		if len(matches) != len(legacy) {
			t.Fatalf("SearchTools(%q) returned %d, legacy returned %d", test.query, len(matches), len(legacy))
		}
		for i := range matches {
			if matches[i].Entry.Name != legacy[i].Entry.Name || matches[i].Score != legacy[i].Score {
				t.Fatalf("SearchTools(%q)[%d] = %s/%v, legacy = %s/%v", test.query, i,
					matches[i].Entry.Name, matches[i].Score, legacy[i].Entry.Name, legacy[i].Score)
			}
			if len(matches[i].Entry.Parameters) == 0 {
				t.Fatalf("SearchTools(%q)[%d] omitted schema", test.query, i)
			}
			definition, ok := AgentDefinition(matches[i].Entry.Name)
			if !ok || !reflect.DeepEqual(matches[i].Entry, definition) {
				t.Fatalf("SearchTools(%q)[%d] definition differs from exact activation lookup", test.query, i)
			}
			provider, ok := AgentLookup(matches[i].Entry.Name)
			if !ok || test.platform != "" && provider.Platform != test.platform {
				t.Fatalf("SearchTools(%q)[%d] violated platform policy: %+v", test.query, i, provider)
			}
		}
	}
}

func TestSearchToolsAdvancedAnalyticsPolicy(t *testing.T) {
	for _, match := range mustSearchTools(t, "video creator list", "douyin", 8) {
		entry, ok := AgentLookup(match.Entry.Name)
		if !ok {
			t.Fatalf("unknown match %q", match.Entry.Name)
		}
		if advancedAnalyticsEntry(entry) {
			t.Fatalf("generic query exposed advanced analytics tool %q", entry.Name)
		}
	}
	foundAdvanced := false
	for _, match := range mustSearchTools(t, "douplus 投放 creator analytics", "douyin", 8) {
		entry, ok := AgentLookup(match.Entry.Name)
		if ok && advancedAnalyticsEntry(entry) {
			foundAdvanced = true
			break
		}
	}
	if !foundAdvanced {
		t.Fatal("explicit advanced analytics query did not return an advanced tool")
	}
}

func TestExplicitAdvancedAnalyticsQueryUsesTokenBoundaries(t *testing.T) {
	for _, query := range []string{"threads latest posts", "photoshop creator tutorial"} {
		if explicitAdvancedAnalyticsQuery(query) {
			t.Fatalf("query %q falsely enabled advanced analytics", query)
		}
	}
	for _, query := range []string{
		"ads performance", "shop trends", "creator analytics report", "广告 投放", "douplus", "xingtu",
	} {
		if !explicitAdvancedAnalyticsQuery(query) {
			t.Fatalf("query %q did not enable explicit advanced analytics", query)
		}
	}
}

func mustSearchTools(t *testing.T, query, platform string, limit int) []toolsearch.Match {
	t.Helper()
	matches, err := SearchTools(query, platform, limit)
	if err != nil {
		t.Fatal(err)
	}
	return matches
}

func TestSearchToolsBounds(t *testing.T) {
	for _, test := range []struct {
		query string
		limit int
	}{
		{query: "", limit: 5},
		{query: "video", limit: 0},
		{query: "video", limit: maxModelSearchResults + 1},
	} {
		if _, err := SearchTools(test.query, "", test.limit); err == nil {
			t.Fatalf("SearchTools(%q, %d) succeeded, want error", test.query, test.limit)
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

func TestSearch_RankingCompatibility(t *testing.T) {
	for _, test := range []struct {
		query string
		want  []string
	}{
		{"抖音 热点 榜单", []string{"douyin_app_v3_fetch_hot_search_list", "douyin_creator_fetch_creator_material_center_related", "douyin_creator_fetch_creator_hot_spot_billboard", "douyin_billboard_fetch_hot_category_list", "weibo_web_v2_fetch_social_ranking"}},
		{"douyin hot search board", []string{"douyin_app_v3_fetch_hot_search_list", "pipixia_app_fetch_hot_search_board_detail", "pipixia_app_fetch_hot_search_board_list", "kuaishou_app_fetch_hot_search_person", "kuaishou_web_fetch_kuaishou_hot_list_v2"}},
		{"小红书 搜索 笔记", []string{"xiaohongshu_app_v2_get_image_note_detail", "xiaohongshu_app_v2_search_notes", "xiaohongshu_app_v2_get_video_note_detail", "xiaohongshu_app_v2_get_user_faved_notes", "xiaohongshu_app_v2_get_user_posted_notes"}},
		{"tiktok user post video list", []string{"tiktok_app_v3_fetch_user_post_videos_v2", "tiktok_app_v3_fetch_user_post_videos_v3", "tiktok_web_fetch_post_comment", "tiktok_web_get_all_aweme_id", "tiktok_web_fetch_post_comment_reply"}},
		{"知乎 热榜", []string{"zhihu_web_fetch_hot_list", "douyin_web_fetch_hot_search_result", "kuaishou_web_fetch_kuaishou_hot_list_v1", "kuaishou_web_fetch_kuaishou_hot_list_v2", "kuaishou_app_fetch_hot_board_categories"}},
		{"微博 用户 主页", []string{"weibo_web_v2_fetch_user_recommend_timeline", "weibo_app_fetch_user_timeline", "weibo_web_fetch_user_posts", "toutiao_app_get_user_id", "linkedin_web_v2_get_user_posts"}},
		{"bilibili 视频 详情", []string{"bilibili_web_fetch_one_video_v3", "bilibili_web_fetch_video_detail", "bilibili_web_fetch_one_video", "bilibili_web_fetch_one_video_v2", "bilibili_app_fetch_one_video"}},
		{"youtube search video", []string{"youtube_web_search_video", "youtube_web_v2_get_shorts_search_v2", "youtube_web_get_video_info_v2", "youtube_web_v2_get_general_search", "youtube_web_get_relate_video"}},
	} {
		hits := Search(test.query, "", 5)
		got := make([]string, len(hits))
		for i := range hits {
			got[i] = hits[i].Entry.Name
		}
		if !reflect.DeepEqual(got, test.want) {
			t.Errorf("Search(%q) = %#v, want %#v", test.query, got, test.want)
		}
	}
}

func BenchmarkSearchToolsTop5(b *testing.B) {
	for b.Loop() {
		matches, err := SearchTools("小红书 搜索 笔记", "xiaohongshu", 5)
		if err != nil || len(matches) != 5 {
			b.Fatalf("SearchTools() = %d matches, err=%v", len(matches), err)
		}
	}
}
