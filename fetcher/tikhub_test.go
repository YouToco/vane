package fetcher

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/YouToco/vane/config"
	"github.com/YouToco/vane/dedup"
	"github.com/YouToco/vane/types"
)

// sampleTikhubResponse 按 2026-07-14 实测的 search_notes 响应结构构造：
// 外壳 code/data.success，笔记在 data.data.items[].note，混入一个非 note 项验证过滤。
const sampleTikhubResponse = `{
  "code": 200,
  "data": {
    "success": true,
    "msg": null,
    "data": {
      "items": [
        {
          "model_type": "note",
          "note": {
            "id": "69ca2af0000000001b020a10",
            "title": "分享几个AI创业方向",
            "desc": "去年开始有创业的想法…",
            "timestamp": 1783670775,
            "xsec_token": "ABtoken=",
            "user": {"nickname": "Zimablue"}
          }
        },
        {
          "model_type": "recommend_query",
          "note": null
        },
        {
          "model_type": "note",
          "note": {
            "id": "",
            "title": "空 id 应被跳过",
            "desc": "x",
            "timestamp": 0,
            "xsec_token": "",
            "user": {"nickname": ""}
          }
        }
      ]
    }
  }
}`

// newTestTikHub 构造指向 httptest.Server 的 TikHubFetcher。
// detailInterval 压到 1ms：限速间隔本身由 TestWaitDetailSlot_SerialInterval 单独验证，
// 其余用例没必要每条笔记真等 1.1s。
func newTestTikHub(srvURL string, seen SeenChecker) *TikHubFetcher {
	f := NewTikHub(config.FetchConfig{TimeoutSeconds: 10, MaxResponseMB: 1, TikhubAPIKey: "test-key"}, seen)
	f.baseURL = srvURL
	f.detailInterval = time.Millisecond
	return f
}

// xhsSrc 构造一个带 keyword 的小红书信源。
func xhsSrc(id int64, cfg string) types.Source {
	return types.Source{ID: id, Type: types.SourceTypeTikHubXHS, Config: json.RawMessage(cfg)}
}

func TestTikHubFetch_MapsNotes(t *testing.T) {
	var gotPath, gotQuery, gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotQuery = r.URL.RawQuery
		gotAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(sampleTikhubResponse))
	}))
	defer srv.Close()

	f := newTestTikHub(srv.URL, nil)
	items, err := f.Fetch(context.Background(), xhsSrc(11, `{"keyword":"AI 创业"}`))
	if err != nil {
		t.Fatalf("Fetch 意外失败: %v", err)
	}
	// 非 note 项与空 id 项都应被跳过，只剩 1 条。
	if len(items) != 1 {
		t.Fatalf("期望 1 条，实际 %d", len(items))
	}

	got := items[0]
	if got.SourceID != 11 {
		t.Errorf("SourceID: 期望 11，实际 %d", got.SourceID)
	}
	if got.ExternalID != "69ca2af0000000001b020a10" {
		t.Errorf("ExternalID: 期望 note.id，实际 %q", got.ExternalID)
	}
	if got.Title != "分享几个AI创业方向" || got.Author != "Zimablue" {
		t.Errorf("字段映射不符: %+v", got)
	}
	if !strings.HasPrefix(got.URL, "https://www.xiaohongshu.com/explore/69ca2af0000000001b020a10?xsec_token=") {
		t.Errorf("URL 应拼 explore 直链 + xsec_token，实际 %q", got.URL)
	}
	if got.PublishedAt == nil || !got.PublishedAt.Equal(time.Unix(1783670775, 0)) {
		t.Errorf("PublishedAt 应为 Unix 秒 1783670775，实际 %v", got.PublishedAt)
	}
	if got.ContentHash == "" || got.Simhash == nil {
		t.Error("ContentHash/Simhash 应由 finalize 补齐")
	}

	// 请求侧断言：路径、鉴权、关键 query 参数（含默认 sort_type）。
	if gotPath != tikhubSearchPath {
		t.Errorf("请求路径: 期望 %s，实际 %s", tikhubSearchPath, gotPath)
	}
	if gotAuth != "Bearer test-key" {
		t.Errorf("Authorization: 期望 Bearer test-key，实际 %q", gotAuth)
	}
	if !strings.Contains(gotQuery, "sort_type=time_descending") || !strings.Contains(gotQuery, "page=1") {
		t.Errorf("query 参数缺失: %s", gotQuery)
	}
}

func TestTikHubFetch_MissingKey(t *testing.T) {
	f := NewTikHub(config.FetchConfig{TimeoutSeconds: 10, MaxResponseMB: 1}, nil) // 无 key
	_, err := f.Fetch(context.Background(), xhsSrc(1, `{"keyword":"x"}`))
	if !errors.Is(err, types.ErrValidation) {
		t.Errorf("缺 key 应判 ErrValidation，实际 %v", err)
	}
}

func TestTikHubFetch_MissingKeyword(t *testing.T) {
	f := newTestTikHub("http://unused.invalid", nil)
	_, err := f.Fetch(context.Background(), xhsSrc(1, `{}`))
	if !errors.Is(err, types.ErrValidation) {
		t.Errorf("缺 keyword 应判 ErrValidation，实际 %v", err)
	}
}

func TestTikHubFetch_AuthFailed(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer srv.Close()

	f := newTestTikHub(srv.URL, nil)
	_, err := f.Fetch(context.Background(), xhsSrc(1, `{"keyword":"x"}`))
	if !errors.Is(err, types.ErrValidation) {
		t.Errorf("401 应判 ErrValidation（key 问题），实际 %v", err)
	}
	if types.IsRetryable(err) {
		t.Error("鉴权失败不应可重试")
	}
}

func TestTikHubFetch_RateLimited(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()

	f := newTestTikHub(srv.URL, nil)
	_, err := f.Fetch(context.Background(), xhsSrc(1, `{"keyword":"x"}`))
	if types.CodeOf(err) != types.CodeFetchRateLimit {
		t.Errorf("429 期望 CodeFetchRateLimit，实际 %s", types.CodeOf(err))
	}
}

func TestTikHubFetch_BusinessFailure(t *testing.T) {
	// HTTP 200 但业务失败（success=false）：应报错且不可重试。
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"code":200,"data":{"success":false,"msg":"keyword blocked","data":{"items":[]}}}`))
	}))
	defer srv.Close()

	f := newTestTikHub(srv.URL, nil)
	_, err := f.Fetch(context.Background(), xhsSrc(1, `{"keyword":"x"}`))
	if err == nil {
		t.Fatal("业务失败应返回错误")
	}
	if types.IsRetryable(err) {
		t.Error("业务失败按确定性处理，不应可重试")
	}
}

func TestTikHubFetch_SortTypeOverride(t *testing.T) {
	var gotQuery string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.RawQuery
		_, _ = w.Write([]byte(`{"code":200,"data":{"success":true,"data":{"items":[]}}}`))
	}))
	defer srv.Close()

	f := newTestTikHub(srv.URL, nil)
	_, err := f.Fetch(context.Background(), xhsSrc(1, `{"keyword":"x","sort_type":"general"}`))
	if err != nil {
		t.Fatalf("Fetch 意外失败: %v", err)
	}
	if !strings.Contains(gotQuery, "sort_type=general") {
		t.Errorf("config 指定 sort_type 应覆盖默认，实际 query: %s", gotQuery)
	}
}

// ---- 详情补全（M5 缺陷 1：search_notes 的 desc 被硬截断到 60 rune）----

// truncatedDesc 模拟上游截断结果：恰好 60 rune / 180 字节 —— 用 len() 判断会得到
// 180，判不出"到达阈值"，这条断言正是 tikhubDetailMinRunes 必须按 rune 计的理由。
var truncatedDesc = strings.Repeat("好", tikhubDetailMinRunes)

// fullDesc 模拟详情接口返回的完整正文（实测增益约 5.8x~11.7x）。
var fullDesc = truncatedDesc + strings.Repeat("这是搜索接口给不了的正文后半段。", 20)

// fakeSeen 是 SeenChecker 的测试替身，记录调用次数以便断言"未入库判定确实走了"。
type fakeSeen struct {
	existing map[string]struct{} // 视为**已入库且正文已补全**的 canonical_key（xhs 即 note_id）
	err      error               // 非 nil 时模拟查库失败
	calls    int
	lastMin  int      // 最近一次调用收到的 minRunes，供断言口径一致
	lastKeys []string // 最近一次调用收到的键，供断言"查的是全局身份、不掺源号"
}

// EnrichedCanonicalKeys 对齐生产语义：只报"已入库**且**正文长于 minRunes"的。
// 假实现直接用 existing 集合表达该判定结果——测试关心的是 fetcher 拿到集合后
// 的行为，长度判定本身由 store 的 DB 门控测试覆盖。
//
// 注意入参**没有 sourceID**：内容身份全局化后"哪些笔记已补全"是全局事实，
// 这个替身也据此按 canonical_key 索引，与生产一致。
func (f *fakeSeen) EnrichedCanonicalKeys(_ context.Context, keys []string, minRunes int) (map[string]struct{}, error) {
	f.calls++
	f.lastMin = minRunes
	f.lastKeys = append([]string(nil), keys...)
	if f.err != nil {
		return nil, f.err
	}
	out := make(map[string]struct{})
	for _, k := range keys {
		if _, ok := f.existing[k]; ok {
			out[k] = struct{}{}
		}
	}
	return out, nil
}

// tikhubStub 是同时伺候 search + detail 两个路径的假上游，记录详情侧的全部命中，
// 供"零调用""token 编码"等断言使用。
//
// 刻意**不记录命中时刻**：这里是服务端收到请求的时间，而限速闸门管的是客户端何时
// 发出，两者之间隔着一段会抖动的投递延迟（gap_observed = interval + (latency[n] −
// latency[n−1])）——前一次投递慢、这一次快，观测间隔就会低于 interval，闸门明明没坏
// 却报错。间隔断言只能看闸门自己的放行时刻，见 TestWaitDetailSlot_SerialInterval。
type tikhubStub struct {
	searchBody string
	detailCode int                        // 详情 HTTP 状态码；0 视为 200
	detailBody func(noteID string) string // 详情响应体；nil 时返回全文

	mu          sync.Mutex
	detailIDs   []string // 被请求的 note_id，按顺序
	detailToken []string // 收到的 xsec_token（解码后）
}

func (s *tikhubStub) handler(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case tikhubSearchPath:
		_, _ = w.Write([]byte(s.searchBody))
	case tikhubNoteDetailPath:
		id := r.URL.Query().Get("note_id")
		s.mu.Lock()
		s.detailIDs = append(s.detailIDs, id)
		s.detailToken = append(s.detailToken, r.URL.Query().Get("xsec_token"))
		s.mu.Unlock()

		if s.detailCode != 0 && s.detailCode != http.StatusOK {
			w.WriteHeader(s.detailCode)
			return
		}
		if s.detailBody != nil {
			_, _ = w.Write([]byte(s.detailBody(id)))
			return
		}
		_, _ = w.Write([]byte(tikhubDetailJSON(id, fullDesc)))
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func (s *tikhubStub) hits() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.detailIDs...)
}

// tikhubSearchJSON 生成含 n 条笔记的搜索响应；每条 desc 相同、id/token 按序号区分。
func tikhubSearchJSON(n int, desc string) string {
	items := make([]map[string]any, 0, n)
	for i := 0; i < n; i++ {
		items = append(items, map[string]any{
			"model_type": "note",
			"note": map[string]any{
				"id":    fmt.Sprintf("note%d", i),
				"title": fmt.Sprintf("标题%d", i),
				"desc":  desc,
				// 秒级时间戳：详情里的 time 是毫秒，绝不能混用（见 tikhubNoteCard）。
				"timestamp": 1783670775,
				// token 刻意含 + / = ——必须被 url.Values 正确编码。
				"xsec_token": fmt.Sprintf("AB+c/d%d=", i),
				"user":       map[string]any{"nickname": "作者"},
			},
		})
	}
	b, _ := json.Marshal(map[string]any{
		"code": 200,
		"data": map[string]any{"success": true, "msg": nil,
			"data": map[string]any{"items": items}},
	})
	return string(b)
}

// tikhubDetailJSON 生成 web_v3 详情响应。note_card.time 特意给毫秒值，
// 以固定"详情的 time 是毫秒、代码不该解析它"这一实测事实。
func tikhubDetailJSON(noteID, desc string) string {
	b, _ := json.Marshal(map[string]any{
		"code": 200,
		"data": map[string]any{"success": true, "msg": nil,
			"data": map[string]any{"items": []any{
				map[string]any{"note_card": map[string]any{
					"note_id": noteID, "desc": desc, "time": int64(1783670775000),
				}},
			}}},
	})
	return string(b)
}

func TestTikHubFetch_EnrichesNewTruncatedNote(t *testing.T) {
	stub := &tikhubStub{searchBody: tikhubSearchJSON(1, truncatedDesc)}
	srv := httptest.NewServer(http.HandlerFunc(stub.handler))
	defer srv.Close()

	seen := &fakeSeen{} // 空库：note0 是新笔记
	f := newTestTikHub(srv.URL, seen)
	items, err := f.Fetch(context.Background(), xhsSrc(7, `{"keyword":"x"}`))
	if err != nil {
		t.Fatalf("Fetch 意外失败: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("期望 1 条，实际 %d", len(items))
	}
	if got := stub.hits(); len(got) != 1 || got[0] != "note0" {
		t.Fatalf("期望对 note0 调一次详情，实际 %v", got)
	}
	if seen.calls != 1 {
		t.Errorf("期望查一次已入库集合，实际 %d 次", seen.calls)
	}
	// 闸门查的必须是 canonical_key，而 xhs 的 canonical_key **就是裸 note_id**
	// （契约 §5，与 007 回填的 ci.external_id 逐字对齐）。若这里拼了前缀或塞进 url，
	// 就会和 store 里按 canonical_key 索引的行永远对不上：闸门静默全 miss、每轮重复付费。
	if len(seen.lastKeys) != 1 || seen.lastKeys[0] != "note0" {
		t.Errorf("闸门应按裸 note_id（= canonical_key）查，实际收到 %v", seen.lastKeys)
	}
	if seen.lastMin != tikhubDetailMinRunes {
		t.Errorf("minRunes 口径应与截断阈值一致，实际 %d", seen.lastMin)
	}

	// 核心断言：入库的是详情全文，不是 60 字残句。
	if items[0].Content != fullDesc {
		t.Errorf("Content 应被替换为详情全文，实际长度 %d", len([]rune(items[0].Content)))
	}
	// 指纹必须基于全文——否则跨批去重会把同一笔记的不同截断当成新内容。
	wantSim := dedup.Simhash(items[0].Title + " " + fullDesc)
	if items[0].Simhash == nil || *items[0].Simhash != wantSim {
		t.Errorf("Simhash 应基于全文计算，实际 %v 期望 %d", items[0].Simhash, wantSim)
	}
	truncSim := dedup.Simhash(items[0].Title + " " + truncatedDesc)
	if items[0].Simhash != nil && *items[0].Simhash == truncSim {
		t.Error("Simhash 等于截断版指纹，说明 finalize 跑在补全之前")
	}

	// xsec_token 含 + / =，必须被完整编码回原值（裸拼会把 + 解成空格）。
	stub.mu.Lock()
	gotToken := stub.detailToken[0]
	stub.mu.Unlock()
	if gotToken != "AB+c/d0=" {
		t.Errorf("xsec_token 应经 url.Values 编码后原样送达，实际 %q", gotToken)
	}
}

func TestTikHubFetch_SkipsDetailForSeenNotes(t *testing.T) {
	stub := &tikhubStub{searchBody: tikhubSearchJSON(2, truncatedDesc)}
	srv := httptest.NewServer(http.HandlerFunc(stub.handler))
	defer srv.Close()

	// note0 已入库（正文首次入库时已补全），只有 note1 该付费。
	seen := &fakeSeen{existing: map[string]struct{}{"note0": {}}}
	f := newTestTikHub(srv.URL, seen)
	items, err := f.Fetch(context.Background(), xhsSrc(7, `{"keyword":"x"}`))
	if err != nil {
		t.Fatalf("Fetch 意外失败: %v", err)
	}
	if got := stub.hits(); len(got) != 1 || got[0] != "note1" {
		t.Fatalf("已入库笔记不该再调详情；期望只命中 note1，实际 %v", got)
	}
	if items[0].Content != truncatedDesc {
		t.Error("已入库笔记应保留搜索 desc 原样")
	}
	if items[1].Content != fullDesc {
		t.Error("新笔记应被补全为详情全文")
	}
}

// TestTikHubFetch_SkipsDetailForNoteEnrichedByAnotherSource 是本次重构**唯一**
// 的省钱证据，也是多用户才暴露的那条：用户 A 订「AI编程」、用户 B 订「AI工具」，
// 同一篇笔记命中两个不同的源。
//
// 旧闸门按 (source_id, external_id) 查：B 的源号对不上 A 补全时写的行，于是同一篇
// 笔记的详情被付两次钱（$0.01/次），库里还存了两份。新闸门按全局 canonical_key 查，
// A 补全后 B **一次详情都不该调**。
//
// 断言写成"零调用"而非"少调用"：这里没有中间态——命中就是 0 次，漏一点就是全额重付。
func TestTikHubFetch_SkipsDetailForNoteEnrichedByAnotherSource(t *testing.T) {
	stub := &tikhubStub{searchBody: tikhubSearchJSON(2, truncatedDesc)}
	srv := httptest.NewServer(http.HandlerFunc(stub.handler))
	defer srv.Close()

	// 库里的事实：note0/note1 已由**别的源**（用户 A 的「AI编程」，source_id=101）
	// 补全过。注意 existing 里只有全局身份，压根没有源号——这正是重点。
	seen := &fakeSeen{existing: map[string]struct{}{
		"note0": {},
		"note1": {},
	}}

	f := newTestTikHub(srv.URL, seen)
	// 用户 B 的「AI工具」是另一个源（source_id=202），搜到同样两条笔记。
	items, err := f.Fetch(context.Background(), xhsSrc(202, `{"keyword":"AI工具"}`))
	if err != nil {
		t.Fatalf("Fetch 意外失败: %v", err)
	}

	if hits := stub.hits(); len(hits) != 0 {
		t.Errorf("跨源已补全的笔记必须零详情调用（每次 $0.01），实际调了 %d 次: %v", len(hits), hits)
	}
	if seen.calls != 1 {
		t.Errorf("仍应查一次闸门，实际 %d 次", seen.calls)
	}
	// 闸门键必须逐字等于 note_id：掺进源号（"202"）跨源命中就无从谈起，
	// 掺进 url 则会带上每次都不同的 xsec_token、连同源命中都保不住。
	for _, k := range seen.lastKeys {
		if k != "note0" && k != "note1" {
			t.Errorf("闸门键必须是裸 note_id，实得 %q", k)
		}
	}

	// 两条都保留搜索摘要入库：跳过补全是"不额外改善"，不是"倒退"。
	// 真正的全文由 A 那次补全写进了同一 canonical_key 的行（store 侧事实）。
	for i, it := range items {
		if it.Content != truncatedDesc {
			t.Errorf("第 %d 条应保留搜索 desc，实际长度 %d", i, len([]rune(it.Content)))
		}
	}
}

func TestTikHubFetch_SkipsDetailForShortDesc(t *testing.T) {
	// 59 rune：未达截断阈值，说明上游给的就是完整正文，补全纯属白花钱。
	short := strings.Repeat("好", tikhubDetailMinRunes-1)
	stub := &tikhubStub{searchBody: tikhubSearchJSON(1, short)}
	srv := httptest.NewServer(http.HandlerFunc(stub.handler))
	defer srv.Close()

	f := newTestTikHub(srv.URL, &fakeSeen{})
	items, err := f.Fetch(context.Background(), xhsSrc(7, `{"keyword":"x"}`))
	if err != nil {
		t.Fatalf("Fetch 意外失败: %v", err)
	}
	if got := stub.hits(); len(got) != 0 {
		t.Errorf("desc < 60 rune 不该调详情，实际 %v", got)
	}
	if items[0].Content != short {
		t.Error("短 desc 应原样保留")
	}
}

// TestTikHubFetch_DetailFailureDegrades 固定降级铁律：详情的任何失败形态都只能
// 让这条笔记退回 60 字 desc，绝不能让整个 Fetch 失败——补全是纯增益，
// 失败时的行为必须与补全上线前完全一致。
func TestTikHubFetch_DetailFailureDegrades(t *testing.T) {
	cases := []struct {
		name string
		code int
		body func(noteID string) string
	}{
		{"404", http.StatusNotFound, nil},
		{"429限流", http.StatusTooManyRequests, nil},
		{"422缺token", http.StatusUnprocessableEntity, nil},
		{"业务失败success=false", 0, func(string) string {
			return `{"code":200,"data":{"success":false,"msg":"当前笔记暂时无法浏览","data":{"items":[]}}}`
		}},
		{"items为空", 0, func(string) string {
			return `{"code":200,"data":{"success":true,"data":{"items":[]}}}`
		}},
		{"响应不是JSON", 0, func(string) string { return `<html>502 Bad Gateway</html>` }},
		// 上游串号：200 + 正常外壳 + 别人的笔记。不校验 note_id 的话，
		// 别人的正文会被安在这条 external_id 上静默入库。
		{"note_id不匹配", 0, func(string) string {
			return tikhubDetailJSON("别人的笔记id", "别人的正文，绝不能落到 note0 头上")
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stub := &tikhubStub{
				searchBody: tikhubSearchJSON(1, truncatedDesc),
				detailCode: tc.code,
				detailBody: tc.body,
			}
			srv := httptest.NewServer(http.HandlerFunc(stub.handler))
			defer srv.Close()

			f := newTestTikHub(srv.URL, &fakeSeen{})
			items, err := f.Fetch(context.Background(), xhsSrc(7, `{"keyword":"x"}`))
			if err != nil {
				t.Fatalf("详情失败绝不能让 Fetch 失败，实际 %v", err)
			}
			if len(items) != 1 {
				t.Fatalf("期望仍入库 1 条，实际 %d", len(items))
			}
			if items[0].Content != truncatedDesc {
				t.Errorf("应保留搜索 desc，实际 %q", items[0].Content)
			}
			if len(stub.hits()) != 1 {
				t.Errorf("失败不该重试，期望恰好 1 次详情调用，实际 %d", len(stub.hits()))
			}
		})
	}
}

func TestTikHubFetch_NilSeenCheckerSkipsDetail(t *testing.T) {
	stub := &tikhubStub{searchBody: tikhubSearchJSON(1, truncatedDesc)}
	srv := httptest.NewServer(http.HandlerFunc(stub.handler))
	defer srv.Close()

	// seen=nil：无从判断新旧，宁可不补也不能每轮为整库老笔记重复付费。
	f := newTestTikHub(srv.URL, nil)
	items, err := f.Fetch(context.Background(), xhsSrc(7, `{"keyword":"x"}`))
	if err != nil {
		t.Fatalf("Fetch 意外失败: %v", err)
	}
	if got := stub.hits(); len(got) != 0 {
		t.Errorf("SeenChecker 为 nil 时不该调详情，实际 %v", got)
	}
	if items[0].Content != truncatedDesc {
		t.Error("跳过补全时应保留搜索 desc")
	}
}

func TestTikHubFetch_SeenCheckerErrorSkipsDetail(t *testing.T) {
	stub := &tikhubStub{searchBody: tikhubSearchJSON(1, truncatedDesc)}
	srv := httptest.NewServer(http.HandlerFunc(stub.handler))
	defer srv.Close()

	f := newTestTikHub(srv.URL, &fakeSeen{err: errors.New("db down")})
	items, err := f.Fetch(context.Background(), xhsSrc(7, `{"keyword":"x"}`))
	if err != nil {
		t.Fatalf("查库失败不该让 Fetch 失败，实际 %v", err)
	}
	if got := stub.hits(); len(got) != 0 {
		t.Errorf("查不出新旧就不该盲目补全（会为老笔记重复付费），实际 %v", got)
	}
	if items[0].Content != truncatedDesc {
		t.Error("跳过补全时应保留搜索 desc")
	}
}

// TestWaitDetailSlot_SerialInterval 验证限速闸门放行的相邻两个槽位至少相隔
// detailInterval（上游 1 req/s，超速直接 429 且照常计费）。
//
// 断言的是闸门**自己记录的放行时刻** lastDetailAt —— 那正是它控制的量：
// L[n] 在等满 time.After(interval − since(L[n−1])) 之后才写入，而 time.After 只会晚
// 触发不会早触发，故 L[n] − L[n−1] ≥ interval 恒成立，与机器负载无关。
// 任何在闸门之外取的时刻（stub 收到请求、RoundTrip 发出请求）都多叠一段抖动的延迟，
// 拿来断言下界就会假失败。
func TestWaitDetailSlot_SerialInterval(t *testing.T) {
	const interval = 20 * time.Millisecond
	f := &TikHubFetcher{detailInterval: interval}

	granted := make([]time.Time, 0, 3)
	for i := 0; i < cap(granted); i++ {
		if !f.waitDetailSlot(context.Background()) {
			t.Fatalf("第 %d 次取槽位意外失败", i+1)
		}
		f.rateMu.Lock()
		granted = append(granted, f.lastDetailAt)
		f.rateMu.Unlock()
	}

	for i := 1; i < len(granted); i++ {
		if gap := granted[i].Sub(granted[i-1]); gap < interval {
			t.Errorf("第 %d 与第 %d 个槽位相隔 %v < %v，限速失效", i, i+1, gap, interval)
		}
	}
}

// TestTikHubFetch_DetailGoesThroughRateLimiter 固定"补全循环确实过闸门"这一接线事实
// ——闸门本身的间隔语义由 TestWaitDetailSlot_SerialInterval 覆盖，这里只证它没被绕开。
//
// 判据是 Fetch 的墙钟耗时 ≥ (n−1)×interval：等待发生在 Fetch **内部**，任何额外开销
// （调度、投递延迟）只会让耗时变长，故这个下界是单边的——闸门被摘掉或少等一轮必然
// 报错，机器再忙也不会误报。反过来说，别在这里加耗时上界，那才会 flaky。
func TestTikHubFetch_DetailGoesThroughRateLimiter(t *testing.T) {
	const interval = 50 * time.Millisecond

	stub := &tikhubStub{searchBody: tikhubSearchJSON(3, truncatedDesc)}
	srv := httptest.NewServer(http.HandlerFunc(stub.handler))
	defer srv.Close()

	f := newTestTikHub(srv.URL, &fakeSeen{})
	f.detailInterval = interval

	start := time.Now()
	if _, err := f.Fetch(context.Background(), xhsSrc(7, `{"keyword":"x"}`)); err != nil {
		t.Fatalf("Fetch 意外失败: %v", err)
	}
	elapsed := time.Since(start)

	if got := stub.hits(); len(got) != 3 {
		t.Fatalf("期望 3 次详情调用，实际 %d: %v", len(got), got)
	}
	// 3 次调用 = 2 次等待。
	if want := 2 * interval; elapsed < want {
		t.Errorf("3 次详情调用共耗时 %v < %v，限速闸门未接进补全循环", elapsed, want)
	}
}
