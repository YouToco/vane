package feedback

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/YouToco/vane/config"
	"github.com/YouToco/vane/llm"
	"github.com/YouToco/vane/types"
)

// ============================================================
// 内存假实现（风格对齐 agent/loop_test.go 的 fakeStore）：
// feedback 只依赖窄接口，单测因此完全不碰数据库；LLM 用 httptest 仿上游。
// ============================================================

const (
	testUserID     int64 = 7
	testDeliveryID int64 = 42
	testItemID     int64 = 100
	testMsgID            = "om_delivery_42"
	testBodyMD           = "**解读标题**\n一句话摘要。\n[阅读原文](https://example.com/a)"
	testTitle            = "示例内容标题"
	// testContent 必须长于 deepDiveMinRunes（178 rune > 150）：默认 harness 代表
	// 一条"正文健全、值得深度解读"的内容，否则证据闸门会把每条 deep_dive 测试
	// 都挡在生成之前，幂等/落库/送达全都测不到。要测闸门本身请显式改短这个字段
	// （见 TestHandleDeepDive_ShortContentGate），别把默认值改短。
	testContent = "示例正文内容。这是一条用于测试的资讯正文，长度刻意超过深度解读的证据闸门下限 deepDiveMinRunes，" +
		"以便默认装配的这条内容能走完整的生成路径。闸门只看正文 rune 数，所以这里必须是一段足够长的真实段落，" +
		"而不是一句话占位符——否则默认 harness 里每条 deep_dive 测试都会被闸门挡在生成之前，测的就不再是它们各自想测的东西了。"
)

func notFoundErr(msg string) error   { return types.NewAppError(types.CodeNotFound, msg, nil) }
func databaseErr(msg string) error   { return types.NewAppError(types.CodeDatabase, msg, nil) }
func validationErr(msg string) error { return types.NewAppError(types.CodeValidation, msg, nil) }

// fakeStore 是 feedback.Store 窄接口的内存实现，语义逐条对齐 store 的生产实现：
//   - GetDeliveryForUser：归属校验进查询条件，越权与不存在同为 NotFound；
//   - GetDeliveryByFeishuMessageID：空串 Go 侧短路 NotFound（契约 §14 双保险的 Go 侧）；
//   - InsertFeedback：追加式日志，action 白名单外 CodeValidation；
//   - LatestFeedbackAction：**按传入动作集合过滤**后取最新一条（集合语义是 F5 的命门，
//     假实现若退化成"取最新任意态度"就测不出传单值的 bug）；空集合 CodeValidation；
//   - InsertDeepDiveFeedback：模拟 006 部分唯一索引，命中回传既有 id/detail + existed；
//   - GetFeedbackDetail：取该 (delivery, action) 最新一条 detail，无行 NotFound。
type fakeStore struct {
	mu sync.Mutex

	deliveries map[int64]*types.Delivery
	items      map[int64]*types.ContentItem
	profiles   map[int64]*types.Profile

	feedbacks []types.Feedback
	nextID    int64

	// 错误注入（非 nil 时对应方法直接返回该错误）。
	getDeliveryErr error
	byMsgIDErr     error
	insertErr      error
	latestErr      error
	hasErr         error
	detailErr      error
	itemErr        error
	profileErr     error
	auditErr       error
	auditOutcome   types.FreshnessFeedbackAuditOutcome
	canonicalBrief types.BriefV1
	canonicalFound bool
	canonicalErr   error
	canonicalCalls int

	// 调用留痕：断言"不查库""不重复生成"这类负向要求。
	byMsgIDCalls   []string
	getDeliveryFor []int64
	itemCalls      int

	// hookInsertDeepDive 在 InsertDeepDiveFeedback 进入时执行，用来模拟
	// "并发对手在本次落行前抢先落行"（触发 existed=true 分支）。
	hookInsertDeepDive func()
	// deadlineProbe 在 GetDeliveryByFeishuMessageID 里窥探调用方 ctx，
	// 用来断言 WrapQuestion 确实给 DB 调用套了自己的预算（审查 F15）。
	deadlineProbe func(context.Context)
}

func (f *fakeStore) LoadCanonicalBriefForFeedbackV1(
	_ context.Context,
	userID int64,
	deliveryID int64,
	batchID int64,
) (types.BriefV1, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.canonicalCalls++
	if f.canonicalErr != nil {
		return types.BriefV1{}, false, f.canonicalErr
	}
	if !f.canonicalFound ||
		f.canonicalBrief.UserID != userID ||
		f.canonicalBrief.PushBatchID != batchID {
		return types.BriefV1{}, false, nil
	}
	for _, insight := range f.canonicalBrief.Insights {
		if insight.ID == deliveryID {
			return f.canonicalBrief, true, nil
		}
	}
	return types.BriefV1{}, false, nil
}

func newFakeStore() *fakeStore {
	fs := &fakeStore{
		deliveries: make(map[int64]*types.Delivery),
		items:      make(map[int64]*types.ContentItem),
		profiles:   make(map[int64]*types.Profile),
	}
	itemID := testItemID
	fs.deliveries[testDeliveryID] = &types.Delivery{
		ID:              testDeliveryID,
		UserID:          testUserID,
		ContentItemID:   &itemID,
		Score:           88,
		BodyMD:          testBodyMD,
		FeishuMessageID: testMsgID,
		Status:          types.DeliveryStatusSent,
		CreatedAt:       time.Now(),
	}
	fs.items[testItemID] = &types.ContentItem{
		ID:      testItemID,
		Title:   testTitle,
		Content: testContent,
		URL:     "https://example.com/a",
	}
	return fs
}

func (f *fakeStore) GetDeliveryForUser(_ context.Context, id, userID int64) (*types.Delivery, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.getDeliveryFor = append(f.getDeliveryFor, id)
	if f.getDeliveryErr != nil {
		return nil, f.getDeliveryErr
	}
	d, ok := f.deliveries[id]
	if !ok || d.UserID != userID {
		return nil, notFoundErr("fake: 投递不存在或不属于该用户")
	}
	cp := *d
	return &cp, nil
}

func (f *fakeStore) ListDeliveriesByFeishuMessage(_ context.Context, userID int64, msgID string) ([]types.Delivery, error) {
	if msgID == "" {
		return nil, nil
	}
	var out []types.Delivery
	for _, d := range f.deliveries {
		if d.UserID == userID && d.FeishuMessageID == msgID {
			out = append(out, *d)
		}
	}
	// 与真 store 同序（id ASC）：map 遍历序随机，不排序会让条目顺序断言 flaky，
	// 且掩盖"重建序≠首发序"一类真错位。
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func (f *fakeStore) GetDeliveryByFeishuMessageID(ctx context.Context, userID int64, msgID string) (*types.Delivery, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.deadlineProbe != nil {
		f.deadlineProbe(ctx)
	}
	f.byMsgIDCalls = append(f.byMsgIDCalls, msgID)
	// 生产实现的 Go 侧空串短路（契约 §14）：未发送行的 message_id 是 ''，
	// 空串查询会误命中。
	if msgID == "" {
		return nil, notFoundErr("fake: 空 message_id 无法反查")
	}
	if f.byMsgIDErr != nil {
		return nil, f.byMsgIDErr
	}
	for _, d := range f.deliveries {
		if d.UserID == userID && d.FeishuMessageID == msgID {
			cp := *d
			return &cp, nil
		}
	}
	return nil, notFoundErr("fake: 消息无对应投递")
}

var fakeValidActions = map[types.FeedbackAction]bool{
	types.FeedbackActionInterested:    true,
	types.FeedbackActionNotInterested: true,
	types.FeedbackActionMisjudged:     true,
	types.FeedbackActionDeepDive:      true,
	types.FeedbackActionQuestion:      true,
}

func (f *fakeStore) InsertFeedback(_ context.Context, fb *types.Feedback) (int64, error) {
	if !fakeValidActions[fb.Action] {
		return 0, validationErr("fake: 非法反馈动作 " + string(fb.Action))
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.insertErr != nil {
		return 0, f.insertErr
	}
	return f.insertLocked(fb), nil
}

func (f *fakeStore) InsertFeedbackWithSessionCutoff(
	ctx context.Context,
	fb *types.Feedback,
	_ time.Time,
) (int64, error) {
	return f.InsertFeedback(ctx, fb)
}

func (f *fakeStore) AuditOutdatedFeedback(
	_ context.Context, _, _ int64,
) (types.FreshnessFeedbackAuditOutcome, error) {
	if f.auditErr != nil {
		return "", f.auditErr
	}
	if f.auditOutcome == "" {
		return types.FreshnessAuditUnverifiable, nil
	}
	return f.auditOutcome, nil
}

// insertLocked 追加一行并返回新 id（调用方须持锁）。id 单调递增，
// 与生产的 BIGSERIAL + created_at 单调递增等价，故"最新"= 切片末尾方向。
func (f *fakeStore) insertLocked(fb *types.Feedback) int64 {
	f.nextID++
	row := *fb
	row.ID = f.nextID
	row.CreatedAt = time.Now()
	f.feedbacks = append(f.feedbacks, row)
	return row.ID
}

func (f *fakeStore) InsertDeepDiveFeedback(_ context.Context, fb *types.Feedback) (int64, string, bool, error) {
	if f.hookInsertDeepDive != nil {
		f.hookInsertDeepDive()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if fb.Action != types.FeedbackActionDeepDive {
		return 0, "", false, validationErr("fake: InsertDeepDiveFeedback 只接受 deep_dive")
	}
	if f.insertErr != nil {
		return 0, "", false, f.insertErr
	}
	// 模拟 006 的部分唯一索引 uq_feedbacks_delivery_deep_dive。
	for _, row := range f.feedbacks {
		if row.DeliveryID == fb.DeliveryID && row.Action == types.FeedbackActionDeepDive {
			return row.ID, row.Detail, true, nil
		}
	}
	return f.insertLocked(fb), "", false, nil
}

func (f *fakeStore) LatestFeedbackAction(_ context.Context, deliveryID int64, actions []types.FeedbackAction) (types.FeedbackAction, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.latestErr != nil {
		return "", f.latestErr
	}
	// 空集合是调用方 bug（生产同款校验）：静默返回 NotFound 会把误用伪装成"无反馈"。
	if len(actions) == 0 {
		return "", validationErr("fake: LatestFeedbackAction 动作集合为空")
	}
	want := make(map[types.FeedbackAction]bool, len(actions))
	for _, a := range actions {
		want[a] = true
	}
	for i := len(f.feedbacks) - 1; i >= 0; i-- {
		row := f.feedbacks[i]
		if row.DeliveryID == deliveryID && want[row.Action] {
			return row.Action, nil
		}
	}
	return "", notFoundErr("fake: 该投递在动作集合内无反馈")
}

func (f *fakeStore) HasFeedback(_ context.Context, deliveryID int64, action types.FeedbackAction) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.hasErr != nil {
		return false, f.hasErr
	}
	for _, row := range f.feedbacks {
		if row.DeliveryID == deliveryID && row.Action == action {
			return true, nil
		}
	}
	return false, nil
}

func (f *fakeStore) GetFeedbackDetail(_ context.Context, deliveryID int64, action types.FeedbackAction) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.detailErr != nil {
		return "", f.detailErr
	}
	for i := len(f.feedbacks) - 1; i >= 0; i-- {
		row := f.feedbacks[i]
		if row.DeliveryID == deliveryID && row.Action == action {
			return row.Detail, nil
		}
	}
	return "", notFoundErr("fake: 无该动作反馈")
}

func (f *fakeStore) GetContentItem(_ context.Context, id int64) (*types.ContentItem, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.itemCalls++
	if f.itemErr != nil {
		return nil, f.itemErr
	}
	it, ok := f.items[id]
	if !ok {
		return nil, notFoundErr("fake: 内容已清理")
	}
	cp := *it
	return &cp, nil
}

func (f *fakeStore) GetSource(_ context.Context, id int64) (*types.Source, error) {
	return &types.Source{ID: id, Title: "fake-source", Platform: "rss"}, nil
}

func (f *fakeStore) GetProfile(_ context.Context, userID int64) (*types.Profile, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.profileErr != nil {
		return nil, f.profileErr
	}
	p, ok := f.profiles[userID]
	if !ok {
		return nil, notFoundErr("fake: 无画像")
	}
	cp := *p
	return &cp, nil
}

// ---- 断言用的读取器（全部加锁：deep_dive 的生成 goroutine 与测试主协程并发）----

// rows 取 (deliveryID, action) 下的全部行快照。
func (f *fakeStore) rows(deliveryID int64, action types.FeedbackAction) []types.Feedback {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []types.Feedback
	for _, row := range f.feedbacks {
		if row.DeliveryID == deliveryID && row.Action == action {
			out = append(out, row)
		}
	}
	return out
}

// allRows 取全部反馈行快照（断言"零副作用"用）。
func (f *fakeStore) allRows() []types.Feedback {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]types.Feedback(nil), f.feedbacks...)
}

func (f *fakeStore) msgIDQueries() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.byMsgIDCalls...)
}

// ============================================================
// Sender / Notifier / CardBuilder 假实现
// ============================================================

type sentReply struct{ parentID, markdown string }

// fakeSender 记录每次回复；err 非 nil 时仍记录后再报错（要能断言"确实尝试发过"）。
type fakeSender struct {
	mu       sync.Mutex
	replies  []sentReply
	err      error
	gate     chan struct{}
	canceled int
}

func (s *fakeSender) ReplyMarkdown(ctx context.Context, parentMessageID, markdown string) error {
	s.mu.Lock()
	s.replies = append(s.replies, sentReply{parentID: parentMessageID, markdown: markdown})
	gate, err := s.gate, s.err
	s.mu.Unlock()
	if gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
			s.mu.Lock()
			s.canceled++
			s.mu.Unlock()
			return ctx.Err()
		}
	}
	return err
}

func (s *fakeSender) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.replies)
}

func (s *fakeSender) sent() []sentReply {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]sentReply(nil), s.replies...)
}

func (s *fakeSender) setErr(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.err = err
}

func (s *fakeSender) setGate(gate chan struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gate = gate
}

func (s *fakeSender) canceledCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.canceled
}

type notice struct {
	userID         int64
	sourceIdentity string
	text           string
}

type fakeNotifier struct {
	mu      sync.Mutex
	notices []notice
}

func (n *fakeNotifier) NotifyEvent(
	_ context.Context,
	userID int64,
	sourceIdentity string,
	text string,
) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.notices = append(n.notices, notice{
		userID: userID, sourceIdentity: sourceIdentity, text: text,
	})
}

func (n *fakeNotifier) all() []notice {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]notice(nil), n.notices...)
}

type cardCall struct {
	bodyMD     string
	deliveryID int64
	state      CardState
}

// fakeCards 代替 feishu.BuildDeliveryCard（feedback 不得 import feishu）：
// 把入参编码成 JSON 返回，测试即可从 ClickResult.CardJSON 反解出状态行三要素。
type fakeCards struct {
	mu    sync.Mutex
	calls []cardCall
}

func (c *fakeCards) build(input CardInput) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, cardCall{bodyMD: input.BodyMD, deliveryID: input.DeliveryID, state: input.State})
	b, err := json.Marshal(map[string]any{
		"body_md":           input.BodyMD,
		"delivery_id":       input.DeliveryID,
		"pref":              string(input.State.Preference),
		"misjudged":         input.State.Misjudged,
		"bad_feedback_open": input.State.BadFeedbackOpen,
		"deep_dive":         input.State.DeepDiveRequested,
	})
	if err != nil {
		panic(err)
	}
	return string(b)
}

func (c *fakeCards) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.calls)
}

// decodedCard 是 fakeCards.build 产物的反解形态。
type decodedCard struct {
	BodyMD          string `json:"body_md"`
	DeliveryID      int64  `json:"delivery_id"`
	Pref            string `json:"pref"`
	Misjudged       bool   `json:"misjudged"`
	BadFeedbackOpen bool   `json:"bad_feedback_open"`
	DeepDive        bool   `json:"deep_dive"`
}

func decodeCard(t *testing.T, cardJSON string) decodedCard {
	t.Helper()
	if cardJSON == "" {
		t.Fatal("CardJSON 为空：本次点击应返回重建卡")
	}
	var c decodedCard
	if err := json.Unmarshal([]byte(cardJSON), &c); err != nil {
		t.Fatalf("CardJSON 不是合法 JSON: %v (%q)", err, cardJSON)
	}
	return c
}

// ============================================================
// 仿 DeepSeek 上游（httptest）
// ============================================================

// llmRequest 捕获请求体里 deep_dive 关心的参数与提示词。
type llmRequest struct {
	Model       string   `json:"model"`
	MaxTokens   *int     `json:"max_tokens"`
	Temperature *float32 `json:"temperature"`
	Thinking    *struct {
		Type string `json:"type"`
	} `json:"thinking"`
	Messages []struct {
		Role    string `json:"role"`
		Content string `json:"content"`
	} `json:"messages"`
}

// fakeLLM 仿上游：按到达序记录请求；gate 非 nil 时 handler 阻塞在其上
// （用于把生成钉在"进行中"，测 in-flight 幂等层）。
type fakeLLM struct {
	mu       sync.Mutex
	reqs     []llmRequest
	status   int
	content  string
	gate     chan struct{}
	canceled int
}

func newFakeLLM(t *testing.T) (*fakeLLM, *llm.Client) {
	t.Helper()
	f := &fakeLLM{status: http.StatusOK, content: "## 背景脉络\n生成的深度解读正文。"}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req llmRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("解析上游请求体失败: %v", err)
		}
		f.mu.Lock()
		f.reqs = append(f.reqs, req)
		gate, status, content := f.gate, f.status, f.content
		f.mu.Unlock()

		if gate != nil {
			select {
			case <-gate: // 卡住生成，模拟"正在生成"
			case <-r.Context().Done():
				f.mu.Lock()
				f.canceled++
				f.mu.Unlock()
				return
			}
		}
		if status != http.StatusOK {
			w.WriteHeader(status)
			_, _ = w.Write([]byte(`{"error":{"message":"boom"}}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"model": "deepseek-v4-pro",
			"choices": []any{map[string]any{
				"finish_reason": "stop",
				"message":       map[string]any{"role": "assistant", "content": content},
			}},
			"usage": map[string]any{"prompt_tokens": 120, "completion_tokens": 60},
		})
	}))
	t.Cleanup(srv.Close)

	cli := llm.New(config.LLMConfig{
		Provider:      "deepseek",
		BaseURL:       srv.URL,
		APIKey:        "test-key",
		Model:         "deepseek-chat",
		MaxConcurrent: 4,
	})
	return f, cli
}

func (f *fakeLLM) calls() []llmRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]llmRequest(nil), f.reqs...)
}

func (f *fakeLLM) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.reqs)
}

func (f *fakeLLM) canceledCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.canceled
}

func (f *fakeLLM) setStatus(s int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.status = s
}

func (f *fakeLLM) setContent(c string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.content = c
}

func (f *fakeLLM) setGate(g chan struct{}) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.gate = g
}

// gateLLM 把上游钉住（生成停在"进行中"），返回释放函数。
// t.Cleanup 兜底释放是必须的：断言失败提前 Goexit 时若 gate 还开着，
// httptest.Server.Close 会一直等未完成的 handler，整包卡到 10 分钟超时。
func gateLLM(t *testing.T, h *harness) func() {
	t.Helper()
	gate := make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(gate) }) }
	h.llm.setGate(gate)
	t.Cleanup(release)
	return release
}

// ============================================================
// 测试装配 + 同步等待工具
// ============================================================

// harness 是一套装好的 Service 及其全部假依赖。
type harness struct {
	svc      *Service
	st       *fakeStore
	sender   *fakeSender
	notifier *fakeNotifier
	cards    *fakeCards
	llm      *fakeLLM
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	h := &harness{
		st:       newFakeStore(),
		sender:   &fakeSender{},
		notifier: &fakeNotifier{},
		cards:    &fakeCards{},
	}
	fl, cli := newFakeLLM(t)
	h.llm = fl
	// Recorder 传 nil：Record 是 nil 接收者安全的 no-op，记账不需要数据库。
	h.svc = New(Deps{
		Store:           h.st,
		Client:          cli,
		Recorder:        nil,
		Sender:          h.sender,
		Notifier:        h.notifier,
		BuildCard:       h.cards.build,
		DashboardOrigin: "https://vane.example",
		DeepDiveModel:   "deepseek-v4-pro",
	})
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := h.svc.Shutdown(ctx); err != nil {
			t.Errorf("feedback Service 测试清理未排空: %v", err)
		}
	})
	return h
}

// delivery 取默认投递（供测试改字段）。
func (h *harness) delivery() *types.Delivery { return h.st.deliveries[testDeliveryID] }

// click 点一次按钮并要求不报错。
func (h *harness) click(t *testing.T, action types.FeedbackAction) ClickResult {
	t.Helper()
	res, err := h.svc.HandleClick(context.Background(), testUserID, Click{Action: action, DeliveryID: testDeliveryID})
	if err != nil {
		t.Fatalf("HandleClick(%s) 意外报错: %v", action, err)
	}
	return res
}

// submitBadFeedback submits the panel opened by the 👎 / historical misjudged
// action. It is intentionally separate from click: opening/cancelling must
// never be mistaken for a persisted preference event.
func (h *harness) submitBadFeedback(t *testing.T, reason types.FeedbackReason, detail string) ClickResult {
	t.Helper()
	res, err := h.svc.HandleReasonSubmit(context.Background(), testUserID, ReasonSubmit{
		DeliveryID: testDeliveryID,
		ReasonCode: reason,
		Detail:     detail,
	})
	if err != nil {
		t.Fatalf("HandleReasonSubmit(%s) 意外报错: %v", reason, err)
	}
	return res
}

// waitFor 轮询等待条件成立（异步 goroutine 的同步点，模式同 agent 的 waitAppends）。
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("等待超时：%s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// waitReplies 等回复数稳定在 want：先等到达，再多留一拍确认没有多发。
func waitReplies(t *testing.T, s *fakeSender, want int) {
	t.Helper()
	waitFor(t, fmt.Sprintf("回复数达到 %d", want), func() bool { return s.count() == want })
	time.Sleep(20 * time.Millisecond)
	if got := s.count(); got != want {
		t.Fatalf("回复数应稳定在 %d, 实得 %d: %+v", want, got, s.sent())
	}
}

// waitInflightReleased 等 deep_dive 的 in-flight 占位释放（生成 goroutine 的 defer）。
// 释放即"可再点"，是幂等第二层的可观察出口。
func waitInflightReleased(t *testing.T, svc *Service, deliveryID int64) {
	t.Helper()
	waitFor(t, "in-flight 占位释放", func() bool {
		_, ok := svc.inflight.Load(deliveryID)
		return !ok
	})
}
