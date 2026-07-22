package fetcher

// 绑定引擎测试（endpoint-binding-contract.md §8）：
//   - 模板引用完整性（§8.1，CI 卡注册表 re-gen 破坏绑定）
//   - 六能力 fixture 提取（§8.2，真实响应样本；迁移能力含 §6.2 字节等价对照）
//   - 漂移模拟（§8.4：参数漂移 / 结构漂移 / 身份全灭 / 时间全灭 → 显式失败非空成功）
//   - probe 准入（§8.7：0 条拒 / 时序拒 / 全过出报告）
//   - enrich 行为（计费闸门 / 串号 / 空值保旧 / 记账）

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/YouToco/vane/config"
	"github.com/YouToco/vane/sourcecatalog"
	"github.com/YouToco/vane/tikhubcatalog"
	"github.com/YouToco/vane/tikhubinvoke"
	"github.com/YouToco/vane/types"
)

// ────────── 测试基建 ──────────

// fakeUpstream 按路径路由响应体，并记录全部请求供断言。
type fakeUpstream struct {
	mu     sync.Mutex
	bodies map[string]string // path -> body
	status map[string]int    // path -> 覆盖状态码（缺省 200）
	reqs   []*http.Request
}

func newFakeUpstream() *fakeUpstream {
	return &fakeUpstream{bodies: map[string]string{}, status: map[string]int{}}
}

func (f *fakeUpstream) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.reqs = append(f.reqs, r.Clone(context.Background()))
		body, ok := f.bodies[r.URL.Path]
		st := f.status[r.URL.Path]
		f.mu.Unlock()
		if st != 0 {
			w.WriteHeader(st)
		}
		if ok {
			_, _ = w.Write([]byte(body))
		} else if st == 0 {
			w.WriteHeader(http.StatusNotFound)
		}
	})
}

func (f *fakeUpstream) requests(path string) []*http.Request {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []*http.Request
	for _, r := range f.reqs {
		if r.URL.Path == path {
			out = append(out, r)
		}
	}
	return out
}

type fakeRecorder struct {
	mu   sync.Mutex
	rows []*types.ToolCall
}

func (f *fakeRecorder) RecordBindingCall(_ context.Context, rec *types.ToolCall) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rows = append(f.rows, rec)
}

// fakeSeen 固定返回给定键集合。
type fakeSeen struct{ keys map[string]struct{} }

func (f *fakeSeen) EnrichedCanonicalKeys(_ context.Context, _ []string, _ int) (map[string]struct{}, error) {
	if f.keys == nil {
		return map[string]struct{}{}, nil
	}
	return f.keys, nil
}

type notifyingSeen struct {
	once   sync.Once
	called chan struct{}
}

func (f *notifyingSeen) EnrichedCanonicalKeys(_ context.Context, _ []string, _ int) (map[string]struct{}, error) {
	f.once.Do(func() { close(f.called) })
	return map[string]struct{}{}, nil
}

func newTestBinding(srvURL string, seen SeenChecker, rec BindingCallRecorder) *BindingFetcher {
	b := NewBinding(config.FetchConfig{TikhubAPIKey: "k", TimeoutSeconds: 10, MaxResponseMB: 1},
		seen, rec, tikhubinvoke.WithBaseURL(srvURL))
	b.detailInterval = time.Millisecond // 测试不真等 1.1s
	return b
}

func bindingSrc(c types.Capability, cfg string) types.Source {
	p := types.PlatformXHS
	if c == types.CapUserPosts && strings.Contains(cfg, "screen_name") {
		p = types.PlatformX
	}
	return types.Source{ID: 7, Platform: p, Capability: c, Config: json.RawMessage(cfg)}
}

const (
	pathSearch   = "/api/v1/xiaohongshu/app_v2/search_notes"
	pathDetail   = "/api/v1/xiaohongshu/web_v3/fetch_note_detail"
	pathUserPost = "/api/v1/xiaohongshu/app_v2/get_user_posted_notes"
	pathTwitter  = "/api/v1/twitter/web/fetch_user_post_tweet"
	pathHotList  = "/api/v1/xiaohongshu/web_v3/fetch_hot_list"
	pathTopic    = "/api/v1/xiaohongshu/app_v2/get_topic_feed"
	pathFaved    = "/api/v1/xiaohongshu/app_v2/get_user_faved_notes"
)

// ────────── 模板引用完整性（契约 §8.1）──────────

// TestBindingTemplates_Integrity 是注册表 re-gen 与模板之间的防漂移锁：
// 模板引用的端点必须 Lookup 命中，模板可发送的参数必须都被 Entry 声明，
// Entry 的必填参数必须都在模板参数集里，Kind 必须与 sourcecatalog 登记一致。
// 上游 spec 改名/删端点/改参数时，re-gen 提交在 CI 就红，而不是生产静默断供。
func TestBindingTemplates_Integrity(t *testing.T) {
	for key, spec := range bindingTemplates {
		entry, ok := tikhubcatalog.Lookup(spec.Endpoint)
		if !ok {
			t.Errorf("%s/%s: 端点 %s 不在注册表（re-gen 漂移？）", key.P, key.C, spec.Endpoint)
			continue
		}
		known := map[string]bool{}
		required := map[string]bool{}
		for _, p := range entry.Params {
			known[p.Name] = true
			if p.Required {
				required[p.Name] = true
			}
		}
		sendable := map[string]bool{}
		for _, p := range spec.Params {
			sendable[p.Key] = true
			if !known[p.Key] {
				t.Errorf("%s/%s: 模板参数 %s 不被端点 %s 声明", key.P, key.C, p.Key, spec.Endpoint)
			}
		}
		for name := range required {
			if !sendable[name] {
				t.Errorf("%s/%s: 端点 %s 必填参数 %s 不在模板参数集", key.P, key.C, spec.Endpoint, name)
			}
		}
		if len(spec.Fields.ID) == 0 {
			t.Errorf("%s/%s: 模板缺 ID 字段链", key.P, key.C)
		}
		if len(spec.Fields.Time) > 0 {
			switch spec.Fields.TimeFormat {
			case tfUnixS, tfUnixMS, tfRubyDate:
			default:
				t.Errorf("%s/%s: 未知 TimeFormat %q", key.P, key.C, spec.Fields.TimeFormat)
			}
		}
		if want, ok := sourcecatalog.KindOf(key.P, key.C); !ok || spec.Kind != want {
			t.Errorf("%s/%s: 模板 Kind=%q 与 sourcecatalog 登记 %q 漂移", key.P, key.C, spec.Kind, want)
		}

		if es := spec.Enrich; es != nil {
			dentry, ok := tikhubcatalog.Lookup(es.Endpoint)
			if !ok {
				t.Errorf("%s/%s: 详情端点 %s 不在注册表", key.P, key.C, es.Endpoint)
				continue
			}
			dknown := map[string]bool{}
			dreq := map[string]bool{}
			for _, p := range dentry.Params {
				dknown[p.Name] = true
				if p.Required {
					dreq[p.Name] = true
				}
			}
			dsend := map[string]bool{es.KeyParam: true}
			for k := range es.ItemParams {
				dsend[k] = true
			}
			for k := range dsend {
				if !dknown[k] {
					t.Errorf("%s/%s: enrich 参数 %s 不被端点 %s 声明", key.P, key.C, k, es.Endpoint)
				}
			}
			for name := range dreq {
				if !dsend[name] {
					t.Errorf("%s/%s: 详情端点 %s 必填参数 %s 不在 enrich 参数集", key.P, key.C, es.Endpoint, name)
				}
			}
		}
	}
}

// ────────── 六能力提取（含迁移等价对照，契约 §6.2/§8.2）──────────

func TestBinding_XHSSearch_MapsNotes(t *testing.T) {
	up := newFakeUpstream()
	up.bodies[pathSearch] = sampleTikhubResponse
	srv := httptest.NewServer(up.handler())
	defer srv.Close()

	b := newTestBinding(srv.URL, nil, nil)
	items, err := b.Fetch(context.Background(), bindingSrc(types.CapSearch, `{"keyword":"AI创业"}`))
	if err != nil {
		t.Fatalf("Fetch 失败: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("期望 1 条（广告位与空 id 均剔除），实际 %d", len(items))
	}
	it := items[0]
	// 与被删除的 mapTikhubNotes 字节等价（§6.2）：身份、URL（含 xsec 查询组）、字段。
	if it.ExternalID != "69ca2af0000000001b020a10" || it.CanonicalKey != "69ca2af0000000001b020a10" {
		t.Errorf("身份漂移: external=%q canonical=%q", it.ExternalID, it.CanonicalKey)
	}
	wantURL := "https://www.xiaohongshu.com/explore/69ca2af0000000001b020a10?xsec_token=ABtoken%3D&xsec_source=pc_search"
	if it.URL != wantURL {
		t.Errorf("URL 漂移:\n got %q\nwant %q", it.URL, wantURL)
	}
	if it.Title != "分享几个AI创业方向" || it.Author != "Zimablue" {
		t.Errorf("字段漂移: title=%q author=%q", it.Title, it.Author)
	}
	if it.PublishedAt == nil || it.PublishedAt.Unix() != 1783670775 {
		t.Errorf("时间漂移: %v", it.PublishedAt)
	}
	// 请求参数与旧实现同形：page=1 + sort_type 默认 time_descending。
	reqs := up.requests(pathSearch)
	if len(reqs) != 1 {
		t.Fatalf("期望 1 次搜索请求，实际 %d", len(reqs))
	}
	q := reqs[0].URL.Query()
	if q.Get("page") != "1" || q.Get("sort_type") != "time_descending" || q.Get("keyword") != "AI创业" {
		t.Errorf("请求参数漂移: %v", reqs[0].URL.RawQuery)
	}
	if q.Has("note_type") {
		t.Error("未配置的 note_type 不该发送（OmitEmpty）")
	}
}

func TestBinding_XHSUser_MapsAndTitleFallback(t *testing.T) {
	up := newFakeUpstream()
	up.bodies[pathUserPost] = sampleXHSUserResponse
	srv := httptest.NewServer(up.handler())
	defer srv.Close()

	b := newTestBinding(srv.URL, nil, nil)
	items, err := b.Fetch(context.Background(), bindingSrc(types.CapUserPosts, `{"user_id":"6a5578b3000000000e03cc00"}`))
	if err != nil {
		t.Fatalf("Fetch 失败: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("期望 2 条，实际 %d", len(items))
	}
	if items[1].Title != "零基础AI编程的三个核心步骤" {
		t.Errorf("title 回退 display_title 失效: %q", items[1].Title)
	}
	if strings.Contains(items[0].URL, "?") {
		t.Errorf("user_posts 直链不该带查询串（拿不到 xsec_token 的已知取舍）: %q", items[0].URL)
	}
	if items[0].PublishedAt == nil || items[0].PublishedAt.Unix() != 1784303645 {
		t.Errorf("unix_s 时间漂移: %v", items[0].PublishedAt)
	}
}

func TestBinding_XHSUser_DropsMismatchedAuthor(t *testing.T) {
	// 串号防御（§6.4）：userid 非空且 ≠ 所订 user_id 的笔记必须丢弃；为空则宽容保留。
	body := `{"code":200,"data":{"success":true,"msg":null,"data":{"notes":[
	  {"id":"aaaaaaaaaaaaaaaaaaaaaaaa","title":"别人的","desc":"x","create_time":1,"user":{"userid":"其他人","nickname":"n"}},
	  {"id":"bbbbbbbbbbbbbbbbbbbbbbbb","title":"没作者","desc":"x","create_time":1,"user":{"userid":"","nickname":"n"}},
	  {"id":"cccccccccccccccccccccccc","title":"我们的","desc":"x","create_time":1,"user":{"userid":"u1","nickname":"n"}}
	]}}}`
	up := newFakeUpstream()
	up.bodies[pathUserPost] = body
	srv := httptest.NewServer(up.handler())
	defer srv.Close()

	b := newTestBinding(srv.URL, nil, nil)
	items, err := b.Fetch(context.Background(), bindingSrc(types.CapUserPosts, `{"user_id":"u1"}`))
	if err != nil {
		t.Fatalf("Fetch 失败: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("期望 2 条（串号 1 条被丢），实际 %d", len(items))
	}
	for _, it := range items {
		if it.ExternalID == "aaaaaaaaaaaaaaaaaaaaaaaa" {
			t.Error("串号条目未被丢弃")
		}
	}
}

func TestBinding_Twitter_UnwrapAndAuthorFallback(t *testing.T) {
	up := newFakeUpstream()
	up.bodies[pathTwitter] = sampleTwitterResponse
	srv := httptest.NewServer(up.handler())
	defer srv.Close()

	b := newTestBinding(srv.URL, nil, nil)
	items, err := b.Fetch(context.Background(), types.Source{
		ID: 7, Platform: types.PlatformX, Capability: types.CapUserPosts,
		Config: json.RawMessage(`{"screen_name":"OpenAI"}`),
	})
	if err != nil {
		t.Fatalf("Fetch 失败: %v", err)
	}
	if len(items) != 4 {
		t.Fatalf("期望 4 条（原创/转推拆包/引用/回复），实际 %d", len(items))
	}
	byID := map[string]types.ContentItem{}
	for _, it := range items {
		byID[it.ExternalID] = it
	}
	// 转推拆包：ExternalID 是被转推那条，作者与 URL 也随之（与被删除的 tweetToItem 等价）。
	rt, ok := byID["rt_orig_1"]
	if !ok {
		t.Fatalf("转推未拆包，ids=%v", keysOf(byID))
	}
	if rt.Author != "claudeai" || rt.URL != "https://x.com/claudeai/status/rt_orig_1" {
		t.Errorf("拆包字段漂移: author=%q url=%q", rt.Author, rt.URL)
	}
	if !strings.Contains(rt.Content, "Claude for Teachers") {
		t.Errorf("拆包应取被转推全文: %q", rt.Content)
	}
	if _, hasWrapper := byID["t2"]; hasWrapper {
		t.Error("转推外壳（t2）不该与拆包内容并存")
	}
	t1 := byID["t1"]
	if t1.PublishedAt == nil || !t1.PublishedAt.Equal(time.Date(2026, 7, 15, 17, 30, 0, 0, time.UTC)) {
		t.Errorf("ruby_date 时间漂移: %v", t1.PublishedAt)
	}
}

func keysOf(m map[string]types.ContentItem) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func TestBinding_HotList(t *testing.T) {
	up := newFakeUpstream()
	up.bodies[pathHotList] = sampleHotListResponse
	srv := httptest.NewServer(up.handler())
	defer srv.Close()

	b := newTestBinding(srv.URL, nil, nil)
	items, err := b.Fetch(context.Background(), bindingSrc(types.CapHotList, `{}`))
	if err != nil {
		t.Fatalf("Fetch 失败: %v", err)
	}
	if len(items) != 3 {
		t.Fatalf("期望 3 条，实际 %d", len(items))
	}
	for _, it := range items {
		if it.PublishedAt != nil {
			t.Errorf("热榜条目不该有时间戳（updated_at 是坏值，契约 §7）: %v", it.PublishedAt)
		}
		if !strings.Contains(it.Content, "小红书热榜第") || !strings.Contains(it.Content, "热度") {
			t.Errorf("合成正文缺热度上下文: %q", it.Content)
		}
		if !strings.HasPrefix(it.URL, "https://www.xiaohongshu.com/search_result") {
			t.Errorf("URL 应取上游落地页: %q", it.URL)
		}
		if it.ExternalID == "" || it.CanonicalKey != it.ExternalID {
			t.Errorf("身份漂移: %q/%q", it.ExternalID, it.CanonicalKey)
		}
	}
}

func TestBinding_TopicFeed(t *testing.T) {
	up := newFakeUpstream()
	up.bodies[pathTopic] = sampleTopicFeedResponse
	srv := httptest.NewServer(up.handler())
	defer srv.Close()

	b := newTestBinding(srv.URL, nil, nil)
	items, err := b.Fetch(context.Background(), bindingSrc(types.CapTopicFeed, `{"page_id":"6301c499df9bea0001dc6f47"}`))
	if err != nil {
		t.Fatalf("Fetch 失败: %v", err)
	}
	if len(items) != 3 {
		t.Fatalf("期望 3 条，实际 %d", len(items))
	}
	if items[0].PublishedAt == nil || items[0].PublishedAt.UnixMilli() != 1784102366000 {
		t.Errorf("unix_ms 时间漂移: %v", items[0].PublishedAt)
	}
	reqs := up.requests(pathTopic)
	if len(reqs) != 1 || reqs[0].URL.Query().Get("sort") != "time" {
		t.Fatalf("sort=time 模板常量未发送: %v", reqs[0].URL.RawQuery)
	}
}

func TestBinding_FavedNotes(t *testing.T) {
	up := newFakeUpstream()
	up.bodies[pathFaved] = sampleFavedNotesResponse
	srv := httptest.NewServer(up.handler())
	defer srv.Close()

	b := newTestBinding(srv.URL, nil, nil)
	items, err := b.Fetch(context.Background(), bindingSrc(types.CapFavedNotes, `{"user_id":"69bfda630000000034019ee8"}`))
	if err != nil {
		t.Fatalf("Fetch 失败: %v", err)
	}
	if len(items) != 3 {
		t.Fatalf("期望 3 条，实际 %d", len(items))
	}
	if items[0].PublishedAt == nil || items[0].PublishedAt.Unix() != 1761006989 {
		t.Errorf("unix_s 时间漂移: %v", items[0].PublishedAt)
	}
}

// ────────── 漂移模拟（契约 §8.4：显式失败，绝不空成功）──────────

func TestBinding_Drift_ItemsPathMissing(t *testing.T) {
	up := newFakeUpstream()
	up.bodies[pathHotList] = `{"code":200,"data":{"renamed_items":[]}}`
	srv := httptest.NewServer(up.handler())
	defer srv.Close()

	b := newTestBinding(srv.URL, nil, nil)
	_, err := b.Fetch(context.Background(), bindingSrc(types.CapHotList, `{}`))
	if !errors.Is(err, types.ErrValidation) {
		t.Fatalf("结构漂移应 CodeValidation（走 fail_count 告警链），实际 %v", err)
	}
	if !strings.Contains(err.Error(), "结构漂移") {
		t.Errorf("错误应指明结构漂移: %v", err)
	}
}

func TestBinding_Drift_AllIdentityMissing(t *testing.T) {
	up := newFakeUpstream()
	up.bodies[pathHotList] = `{"code":200,"data":{"items":[{"title":"无 id"},{"title":"也无 id"}]}}`
	srv := httptest.NewServer(up.handler())
	defer srv.Close()

	b := newTestBinding(srv.URL, nil, nil)
	_, err := b.Fetch(context.Background(), bindingSrc(types.CapHotList, `{}`))
	if !errors.Is(err, types.ErrValidation) {
		t.Fatalf("身份全灭应 CodeValidation，实际 %v", err)
	}
}

func TestBinding_Drift_AllTimeUnparseable(t *testing.T) {
	body := `{"code":200,"data":{"success":true,"msg":null,"data":{"notes":[
	  {"id":"aaaaaaaaaaaaaaaaaaaaaaaa","title":"t","desc":"x","create_time":"不是数字","user":{"nickname":"n"}}
	]}}}`
	up := newFakeUpstream()
	up.bodies[pathFaved] = body
	srv := httptest.NewServer(up.handler())
	defer srv.Close()

	b := newTestBinding(srv.URL, nil, nil)
	_, err := b.Fetch(context.Background(), bindingSrc(types.CapFavedNotes, `{"user_id":"u"}`))
	if !errors.Is(err, types.ErrValidation) {
		t.Fatalf("时间全灭应 CodeValidation（类型漂移可检），实际 %v", err)
	}
}

func TestBinding_Drift_ParamValidation(t *testing.T) {
	entry, ok := tikhubcatalog.Lookup("xiaohongshu_app_v2_get_topic_feed")
	if !ok {
		t.Fatal("端点缺失")
	}
	src := types.Source{ID: 1}
	// 未声明参数（模拟 re-gen 改名后我方仍发旧名）→ 显式失败，而不是被 buildRequest
	// 静默丢弃后上游用默认值返回 200 但数据错误。
	if err := validateAgainstEntry(entry, map[string]any{"page_id": "x", "sort_v2": "time"}, src); err == nil {
		t.Error("未声明参数应显式失败")
	}
	// 必填缺失（模拟 re-gen 新增必填）→ 显式失败。
	if err := validateAgainstEntry(entry, map[string]any{"sort": "time"}, src); err == nil {
		t.Error("必填缺失应显式失败")
	}
}

func TestBinding_BusinessFailureCarriesMsg(t *testing.T) {
	up := newFakeUpstream()
	up.bodies[pathTopic] = `{"code":200,"data":{"success":false,"msg":"话题不存在","data":{}}}`
	srv := httptest.NewServer(up.handler())
	defer srv.Close()

	b := newTestBinding(srv.URL, nil, nil)
	_, err := b.Fetch(context.Background(), bindingSrc(types.CapTopicFeed, `{"page_id":"x"}`))
	if err == nil || types.IsRetryable(err) {
		t.Fatalf("业务失败应确定性不可重试，实际 %v", err)
	}
	if !strings.Contains(err.Error(), "话题不存在") {
		t.Errorf("错误应携带上游 msg: %v", err)
	}
}

// ────────── probe 准入（契约 §2.2/§8.7）──────────

func TestBinding_Probe_OK(t *testing.T) {
	up := newFakeUpstream()
	up.bodies[pathTopic] = sampleTopicFeedResponse
	srv := httptest.NewServer(up.handler())
	defer srv.Close()

	b := newTestBinding(srv.URL, nil, nil)
	report, err := b.Probe(context.Background(), bindingSrc(types.CapTopicFeed, `{"page_id":"6301c499df9bea0001dc6f47"}`))
	if err != nil {
		t.Fatalf("probe 应通过: %v", err)
	}
	if report.Extracted != 3 || report.Newest == nil || len(report.SampleTitles) == 0 {
		t.Errorf("报告不完整: %+v", report)
	}
}

func TestBinding_Probe_RejectsEmpty(t *testing.T) {
	up := newFakeUpstream()
	up.bodies[pathFaved] = `{"code":200,"data":{"success":true,"msg":null,"data":{"notes":[]}}}`
	srv := httptest.NewServer(up.handler())
	defer srv.Close()

	b := newTestBinding(srv.URL, nil, nil)
	_, err := b.Probe(context.Background(), bindingSrc(types.CapFavedNotes, `{"user_id":"u"}`))
	if !errors.Is(err, types.ErrValidation) {
		t.Fatalf("0 条应拒绝准入，实际 %v", err)
	}
	if !strings.Contains(err.Error(), "未公开") {
		t.Errorf("拒绝话术应提示收藏可能未公开: %v", err)
	}
}

func TestBinding_Probe_RejectsOutOfOrder(t *testing.T) {
	// OrderCheck 模板（topic_feed）遇到升序 → 拒（x/search 乱序教训的可检面）。
	body := `{"code":200,"data":{"success":true,"msg":null,"data":{"items":[
	  {"id":"aaaaaaaaaaaaaaaaaaaaaaaa","title":"旧","desc":"x","create_time":1000000000000,"user":{"nickname":"n"}},
	  {"id":"bbbbbbbbbbbbbbbbbbbbbbbb","title":"新","desc":"x","create_time":2000000000000,"user":{"nickname":"n"}}
	]}}}`
	up := newFakeUpstream()
	up.bodies[pathTopic] = body
	srv := httptest.NewServer(up.handler())
	defer srv.Close()

	b := newTestBinding(srv.URL, nil, nil)
	_, err := b.Probe(context.Background(), bindingSrc(types.CapTopicFeed, `{"page_id":"x"}`))
	if !errors.Is(err, types.ErrValidation) || !strings.Contains(err.Error(), "降序") {
		t.Fatalf("时序违例应拒绝准入，实际 %v", err)
	}
}

func TestBinding_Probe_FavedNonMonotonicAccepted(t *testing.T) {
	// faved 的 create_time 实测非单调（收藏序≠创建序），OrderCheck 关——不得误拒。
	up := newFakeUpstream()
	up.bodies[pathFaved] = sampleFavedNotesResponse
	srv := httptest.NewServer(up.handler())
	defer srv.Close()

	b := newTestBinding(srv.URL, nil, nil)
	if _, err := b.Probe(context.Background(), bindingSrc(types.CapFavedNotes, `{"user_id":"u"}`)); err != nil {
		t.Fatalf("faved 非单调不该被时序检查误拒: %v", err)
	}
}

// ────────── enrich（计费闸门/串号/空值保旧/记账）──────────

func enrichSearchBody(desc string) string {
	return `{"code":200,"data":{"success":true,"msg":null,"data":{"items":[
	  {"model_type":"note","note":{"id":"e1e1e1e1e1e1e1e1e1e1e1e1","title":"t","desc":"` + desc + `","timestamp":1,"xsec_token":"tok","user":{"nickname":"n"}}}
	]}}}`
}

func enrichDetailBody(noteID, desc string) string {
	return `{"code":200,"data":{"success":true,"msg":null,"data":{"items":[{"note_card":{"note_id":"` + noteID + `","desc":"` + desc + `"}}]}}}`
}

func TestBinding_Enrich_ReplacesTruncatedDesc(t *testing.T) {
	trunc := strings.Repeat("字", 60) // 恰 60 rune = 被截断信号
	up := newFakeUpstream()
	up.bodies[pathSearch] = enrichSearchBody(trunc)
	up.bodies[pathDetail] = enrichDetailBody("e1e1e1e1e1e1e1e1e1e1e1e1", "完整正文"+strings.Repeat("长", 80))
	srv := httptest.NewServer(up.handler())
	defer srv.Close()

	rec := &fakeRecorder{}
	b := newTestBinding(srv.URL, &fakeSeen{}, rec)
	items, err := b.Fetch(context.Background(), bindingSrc(types.CapSearch, `{"keyword":"k"}`))
	if err != nil || len(items) != 1 {
		t.Fatalf("Fetch: %v, %d 条", err, len(items))
	}
	if !strings.HasPrefix(items[0].Content, "完整正文") {
		t.Errorf("desc 未被详情全文替换: %q", items[0].Content[:20])
	}
	dreqs := up.requests(pathDetail)
	if len(dreqs) != 1 {
		t.Fatalf("期望 1 次详情调用，实际 %d", len(dreqs))
	}
	q := dreqs[0].URL.Query()
	if q.Get("note_id") != "e1e1e1e1e1e1e1e1e1e1e1e1" || q.Get("xsec_token") != "tok" {
		t.Errorf("详情参数漂移: %v", dreqs[0].URL.RawQuery)
	}
	// 记账：list + detail 各一行（契约 §5：每次计费调用有记录）。
	if len(rec.rows) != 2 {
		t.Fatalf("期望 2 行记账，实际 %d", len(rec.rows))
	}
	for _, r := range rec.rows {
		if r.ToolKind != types.ToolCallKindBindingFetch || r.HTTPStatus == nil || *r.HTTPStatus != 200 {
			t.Errorf("记账行异常: %+v", r)
		}
	}
}

func TestBinding_Enrich_SeenGateSkipsPaidCall(t *testing.T) {
	trunc := strings.Repeat("字", 60)
	up := newFakeUpstream()
	up.bodies[pathSearch] = enrichSearchBody(trunc)
	srv := httptest.NewServer(up.handler())
	defer srv.Close()

	seen := &fakeSeen{keys: map[string]struct{}{"e1e1e1e1e1e1e1e1e1e1e1e1": {}}}
	b := newTestBinding(srv.URL, seen, nil)
	if _, err := b.Fetch(context.Background(), bindingSrc(types.CapSearch, `{"keyword":"k"}`)); err != nil {
		t.Fatal(err)
	}
	if n := len(up.requests(pathDetail)); n != 0 {
		t.Errorf("已补全的笔记不该再付费调详情，实际 %d 次", n)
	}
}

func TestBinding_Enrich_MismatchedIDKeepsOriginal(t *testing.T) {
	trunc := strings.Repeat("字", 60)
	up := newFakeUpstream()
	up.bodies[pathSearch] = enrichSearchBody(trunc)
	up.bodies[pathDetail] = enrichDetailBody("ffffffffffffffffffffffff", "别人的笔记正文")
	srv := httptest.NewServer(up.handler())
	defer srv.Close()

	b := newTestBinding(srv.URL, &fakeSeen{}, nil)
	items, err := b.Fetch(context.Background(), bindingSrc(types.CapSearch, `{"keyword":"k"}`))
	if err != nil || len(items) != 1 {
		t.Fatal(err)
	}
	// 串号防御：宁可保留截断摘要，绝不把别人的正文安在这条身份上。
	if items[0].Content != trunc {
		t.Errorf("串号详情不该覆盖原文: %q", items[0].Content)
	}
}

func TestBinding_Enrich_EmptyDetailKeepsOriginal(t *testing.T) {
	trunc := strings.Repeat("字", 60)
	up := newFakeUpstream()
	up.bodies[pathSearch] = enrichSearchBody(trunc)
	up.bodies[pathDetail] = enrichDetailBody("e1e1e1e1e1e1e1e1e1e1e1e1", "")
	srv := httptest.NewServer(up.handler())
	defer srv.Close()

	b := newTestBinding(srv.URL, &fakeSeen{}, nil)
	items, _ := b.Fetch(context.Background(), bindingSrc(types.CapSearch, `{"keyword":"k"}`))
	if len(items) != 1 || items[0].Content != trunc {
		t.Error("详情为空（纯图笔记）应保留搜索摘要")
	}
}

func TestBinding_Enrich_ShortDescNotEnriched(t *testing.T) {
	up := newFakeUpstream()
	up.bodies[pathSearch] = enrichSearchBody("只有三十个字的完整正文")
	srv := httptest.NewServer(up.handler())
	defer srv.Close()

	b := newTestBinding(srv.URL, &fakeSeen{}, nil)
	if _, err := b.Fetch(context.Background(), bindingSrc(types.CapSearch, `{"keyword":"k"}`)); err != nil {
		t.Fatal(err)
	}
	if n := len(up.requests(pathDetail)); n != 0 {
		t.Errorf("<60 rune 是完整正文，不该调详情（零内容损失、省一次计费），实际 %d 次", n)
	}
}

func TestBinding_EffectGateRechecksAfterDetailLimiterWait(t *testing.T) {
	trunc := strings.Repeat("字", 60)
	up := newFakeUpstream()
	up.bodies[pathSearch] = enrichSearchBody(trunc)
	up.bodies[pathDetail] = enrichDetailBody(
		"e1e1e1e1e1e1e1e1e1e1e1e1", "不应被调用")
	srv := httptest.NewServer(up.handler())
	defer srv.Close()

	seen := &notifyingSeen{called: make(chan struct{})}
	b := newTestBinding(srv.URL, seen, nil)

	// Hold the shared limiter while the main request completes. The compiled
	// authorization is revoked while the detail call is waiting for this slot.
	b.rateMu.Lock()
	locked := true
	defer func() {
		if locked {
			b.rateMu.Unlock()
		}
	}()

	var authorized atomic.Bool
	authorized.Store(true)
	var gateCalls atomic.Int32
	errRevoked := errors.New("compiled task revoked")
	beforeEffect := func(context.Context) error {
		gateCalls.Add(1)
		if !authorized.Load() {
			return errRevoked
		}
		return nil
	}

	done := make(chan error, 1)
	go func() {
		_, err := b.fetchWithEffectGate(
			t.Context(), bindingSrc(types.CapSearch, `{"keyword":"k"}`), beforeEffect)
		done <- err
	}()

	select {
	case <-seen.called:
	case <-time.After(2 * time.Second):
		t.Fatal("detail enrichment did not reach its limiter")
	}
	authorized.Store(false)
	b.rateMu.Unlock()
	locked = false

	select {
	case err := <-done:
		if !errors.Is(err, errRevoked) {
			t.Fatalf("Fetch error = %v, want revocation", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Fetch did not return after revocation")
	}
	if got := len(up.requests(pathSearch)); got != 1 {
		t.Fatalf("main requests = %d, want 1", got)
	}
	if got := len(up.requests(pathDetail)); got != 0 {
		t.Fatalf("detail requests after revocation = %d, want 0", got)
	}
	if got := gateCalls.Load(); got != 2 {
		t.Fatalf("effect gate calls = %d, want main + detail", got)
	}
}

// ────────── 杂项 ──────────

func TestBinding_MissingKey(t *testing.T) {
	b := NewBinding(config.FetchConfig{}, nil, nil)
	_, err := b.Fetch(context.Background(), bindingSrc(types.CapHotList, `{}`))
	if !errors.Is(err, types.ErrValidation) {
		t.Errorf("缺 key 应 CodeValidation，实际 %v", err)
	}
}

func TestBinding_ConfigBigIntPrecision(t *testing.T) {
	// config 里的雪花级大整数必须原串透传（UseNumber），float64 会丢精度查错对象。
	m, err := decodeConfigMap(json.RawMessage(`{"user_id":6871234567890123456}`))
	if err != nil {
		t.Fatal(err)
	}
	if m["user_id"] != "6871234567890123456" {
		t.Errorf("大整数精度丢失: %q", m["user_id"])
	}
}

func TestBinding_RecorderLogsHTTPError(t *testing.T) {
	up := newFakeUpstream()
	up.status[pathHotList] = http.StatusInternalServerError
	srv := httptest.NewServer(up.handler())
	defer srv.Close()

	rec := &fakeRecorder{}
	b := newTestBinding(srv.URL, nil, rec)
	_, err := b.Fetch(context.Background(), bindingSrc(types.CapHotList, `{}`))
	if err == nil || !types.IsRetryable(err) {
		t.Fatalf("5xx 应可重试，实际 %v", err)
	}
	if len(rec.rows) != 1 || rec.rows[0].ErrorType != types.ToolErrHTTP {
		t.Errorf("HTTP 错误应记账为 http_error: %+v", rec.rows)
	}
}

// ────────── 对抗审查修复的回归锁定（2026-07-18 两怀疑者审查）──────────

func TestBinding_Drift_ItemRootRenamed(t *testing.T) {
	// HIGH-1：过滤通过但 ItemRoot 下钻全部失败（上游改名 note 子对象）必须触发
	// 防线 3，绝不能被并进 filtered 豁免成静默空成功。
	body := `{"code":200,"data":{"success":true,"msg":null,"data":{"items":[
	  {"model_type":"note","note_v2":{"id":"aaaaaaaaaaaaaaaaaaaaaaaa"}},
	  {"model_type":"note","note_v2":{"id":"bbbbbbbbbbbbbbbbbbbbbbbb"}}
	]}}}`
	up := newFakeUpstream()
	up.bodies[pathSearch] = body
	srv := httptest.NewServer(up.handler())
	defer srv.Close()

	b := newTestBinding(srv.URL, nil, nil)
	_, err := b.Fetch(context.Background(), bindingSrc(types.CapSearch, `{"keyword":"k"}`))
	if !errors.Is(err, types.ErrValidation) {
		t.Fatalf("ItemRoot 全灭应 CodeValidation（结构漂移），实际 %v", err)
	}
}

func TestBinding_Fetch_OrderCheckEveryRound(t *testing.T) {
	// HIGH-2：OrderCheck 模板的时序断言必须在 fetch 每轮生效（准入后排序腐坏可检），
	// 不只是 probe 一次性。
	body := `{"code":200,"data":{"success":true,"msg":null,"data":{"items":[
	  {"id":"aaaaaaaaaaaaaaaaaaaaaaaa","title":"旧","desc":"x","create_time":1000000000000,"user":{"nickname":"n"}},
	  {"id":"bbbbbbbbbbbbbbbbbbbbbbbb","title":"新","desc":"x","create_time":2000000000000,"user":{"nickname":"n"}}
	]}}}`
	up := newFakeUpstream()
	up.bodies[pathTopic] = body
	srv := httptest.NewServer(up.handler())
	defer srv.Close()

	b := newTestBinding(srv.URL, nil, nil)
	_, err := b.Fetch(context.Background(), bindingSrc(types.CapTopicFeed, `{"page_id":"x"}`))
	if !errors.Is(err, types.ErrValidation) {
		t.Fatalf("fetch 轮次时序违例应 CodeValidation 走告警链，实际 %v", err)
	}
	// faved（OrderCheck 关）不受影响由 TestBinding_FavedNotes（非单调 fixture）保证。
}

func TestBinding_Twitter_NullTimelineIsQuietRound(t *testing.T) {
	// 分票核实：安静的 X 账号 timeline 可能为 JSON null——是合法空轮（旧 x.go 行为），
	// 不是结构漂移；键整个缺失才是漂移。
	up := newFakeUpstream()
	up.bodies[pathTwitter] = `{"code":200,"data":{"status":"ok","timeline":null}}`
	srv := httptest.NewServer(up.handler())
	defer srv.Close()

	b := newTestBinding(srv.URL, nil, nil)
	items, err := b.Fetch(context.Background(), types.Source{
		ID: 7, Platform: types.PlatformX, Capability: types.CapUserPosts,
		Config: json.RawMessage(`{"screen_name":"quiet"}`),
	})
	if err != nil || len(items) != 0 {
		t.Fatalf("timeline=null 应为空成功，实际 err=%v items=%d", err, len(items))
	}

	up.bodies[pathTwitter] = `{"code":200,"data":{"status":"ok"}}`
	if _, err := b.Fetch(context.Background(), types.Source{
		ID: 7, Platform: types.PlatformX, Capability: types.CapUserPosts,
		Config: json.RawMessage(`{"screen_name":"quiet"}`),
	}); !errors.Is(err, types.ErrValidation) {
		t.Fatalf("timeline 键缺失应判结构漂移，实际 %v", err)
	}
}

func TestBinding_Twitter_LongContentNotTruncated(t *testing.T) {
	// 分票核实（parity）：旧 x.go 存推文全文，长文推不得被 xhs 族的 4000 字节截断。
	long := strings.Repeat("a", 6000)
	body := `{"code":200,"data":{"status":"ok","timeline":[
	  {"tweet_id":"t1","text":"` + long + `","created_at":"Wed Jul 15 17:30:00 +0000 2026","author":{"screen_name":"OpenAI"}}
	]}}`
	up := newFakeUpstream()
	up.bodies[pathTwitter] = body
	srv := httptest.NewServer(up.handler())
	defer srv.Close()

	b := newTestBinding(srv.URL, nil, nil)
	items, err := b.Fetch(context.Background(), types.Source{
		ID: 7, Platform: types.PlatformX, Capability: types.CapUserPosts,
		Config: json.RawMessage(`{"screen_name":"OpenAI"}`),
	})
	if err != nil || len(items) != 1 {
		t.Fatal(err)
	}
	if len(items[0].Content) != 6000 {
		t.Errorf("推文全文被截断: %d 字节", len(items[0].Content))
	}
}

func TestBinding_XHSContentTruncatedAt4000(t *testing.T) {
	long := strings.Repeat("b", 6000)
	body := `{"code":200,"data":{"success":true,"msg":null,"data":{"notes":[
	  {"id":"aaaaaaaaaaaaaaaaaaaaaaaa","title":"t","desc":"` + long + `","create_time":1,"user":{"nickname":"n"}}
	]}}}`
	up := newFakeUpstream()
	up.bodies[pathFaved] = body
	srv := httptest.NewServer(up.handler())
	defer srv.Close()

	b := newTestBinding(srv.URL, nil, nil)
	items, err := b.Fetch(context.Background(), bindingSrc(types.CapFavedNotes, `{"user_id":"u"}`))
	if err != nil || len(items) != 1 {
		t.Fatal(err)
	}
	if len(items[0].Content) != 4000 {
		t.Errorf("xhs 正文应截断到 4000 字节（成本护栏），实际 %d", len(items[0].Content))
	}
}

func TestBinding_ProbeRejectionMarksUserFacing(t *testing.T) {
	// HIGH-3 的机制锁定：准入拒绝带 ProbeRejection 标记（可透出）；
	// 漂移类错误不带标记（调用方必须映射固定话术）。
	up := newFakeUpstream()
	up.bodies[pathFaved] = `{"code":200,"data":{"success":true,"msg":null,"data":{"notes":[]}}}`
	up.bodies[pathHotList] = `{"code":200,"data":{"renamed":[]}}`
	srv := httptest.NewServer(up.handler())
	defer srv.Close()

	b := newTestBinding(srv.URL, nil, nil)
	var pr *ProbeRejection
	_, err := b.Probe(context.Background(), bindingSrc(types.CapFavedNotes, `{"user_id":"u"}`))
	if !errors.As(err, &pr) {
		t.Errorf("0 条拒绝应带 ProbeRejection 标记（用户话术可透出）: %v", err)
	}
	_, err = b.Probe(context.Background(), bindingSrc(types.CapHotList, `{}`))
	if errors.As(err, &pr) {
		t.Errorf("结构漂移不该带 ProbeRejection 标记（含内部端点名，须映射固定话术）: %v", err)
	}
}

// ────────── TikHub 记账：cost_usd + source_id ──────────

func TestBinding_RecorderCostAndSourceID(t *testing.T) {
	up := newFakeUpstream()
	up.bodies[pathHotList] = sampleHotListResponse
	srv := httptest.NewServer(up.handler())
	defer srv.Close()

	rec := &fakeRecorder{}
	b := newTestBinding(srv.URL, nil, rec)
	_, err := b.Fetch(context.Background(), bindingSrc(types.CapHotList, `{}`))
	if err != nil {
		t.Fatalf("Fetch 失败: %v", err)
	}
	if len(rec.rows) != 1 {
		t.Fatalf("期望 1 行记账，实际 %d", len(rec.rows))
	}
	got := rec.rows[0]
	if got.CostUSD == nil || *got.CostUSD != 0.001 {
		t.Errorf("CostUSD: 期望 0.001（hot_list 单价），实际 %v", got.CostUSD)
	}
	if got.SourceID == nil || *got.SourceID != 7 {
		t.Errorf("SourceID: 期望 7（bindingSrc 固定值），实际 %v", got.SourceID)
	}
}

func TestBinding_RecorderCostPerEndpoint(t *testing.T) {
	up := newFakeUpstream()
	up.bodies[pathSearch] = sampleTikhubResponse
	srv := httptest.NewServer(up.handler())
	defer srv.Close()

	rec := &fakeRecorder{}
	b := newTestBinding(srv.URL, nil, rec)
	_, _ = b.Fetch(context.Background(), bindingSrc(types.CapSearch, `{"keyword":"k"}`))
	if len(rec.rows) != 1 {
		t.Fatalf("期望 1 行记账，实际 %d", len(rec.rows))
	}
	got := rec.rows[0]
	if got.CostUSD == nil || *got.CostUSD != 0.010 {
		t.Errorf("CostUSD: 期望 0.010（app_v2 search 单价），实际 %v", got.CostUSD)
	}
}

func TestBinding_RecorderNoCostOnHTTPError(t *testing.T) {
	up := newFakeUpstream()
	up.status[pathHotList] = http.StatusInternalServerError
	srv := httptest.NewServer(up.handler())
	defer srv.Close()

	rec := &fakeRecorder{}
	b := newTestBinding(srv.URL, nil, rec)
	_, _ = b.Fetch(context.Background(), bindingSrc(types.CapHotList, `{}`))
	if len(rec.rows) != 1 {
		t.Fatalf("期望 1 行记账，实际 %d", len(rec.rows))
	}
	got := rec.rows[0]
	if got.CostUSD != nil {
		t.Errorf("非 200 不应计费（TikHub 定价承诺），实际 CostUSD=%v", *got.CostUSD)
	}
	if got.SourceID == nil || *got.SourceID != 7 {
		t.Errorf("即使失败 SourceID 也应归因，实际 %v", got.SourceID)
	}
}
