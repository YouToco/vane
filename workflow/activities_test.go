package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"go.temporal.io/sdk/temporal"

	"github.com/YouToco/vane/feedback"
	"github.com/YouToco/vane/types"
)

// ============================================================
// 消费方接口的测试替身。fake 一律带锁：Temporal 测试环境在独立
// goroutine 上执行 Activity，断言侧读取必须与执行侧写入互斥。
// ============================================================

type fakeFetcher struct{}

func (fakeFetcher) Fetch(context.Context, types.Source) ([]types.ContentItem, error) {
	return nil, nil
}

type fakeScorer struct{}

func (fakeScorer) Score(context.Context, int64, types.ContentItem, string) (float64, error) {
	return 80, nil
}

// fakeCardGen 固定返回 body，模拟 cardgen.Generate 产出的解读正文 markdown。
type fakeCardGen struct{ body string }

func (g fakeCardGen) Generate(context.Context, int64, types.ScoredItem, string) (string, error) {
	return g.body, nil
}

type fakePusher struct {
	mu    sync.Mutex
	msgID string
	sent  []string // 每次 Push 收到的卡片 JSON
}

func (p *fakePusher) Push(_ context.Context, _, cardJSON string) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.sent = append(p.sent, cardJSON)
	if p.msgID == "" {
		return "om_test", nil
	}
	return p.msgID, nil
}

func (p *fakePusher) sentCards() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.sent...)
}

type fakeFeishu struct{}

func (fakeFeishu) OwnerOpenID() string { return "ou_owner" }

type fakeEvolver struct {
	mu         sync.Mutex
	err        error
	calls      int
	gotUserID  int64
	gotTraceID string
}

func (e *fakeEvolver) Evolve(_ context.Context, userID int64, traceID string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.calls++
	e.gotUserID, e.gotTraceID = userID, traceID
	return e.err
}

func (e *fakeEvolver) snapshot() (calls int, userID int64, traceID string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.calls, e.gotUserID, e.gotTraceID
}

// markSentCall 记录 MarkDeliverySent 收到的实参，供"最终卡确实落库"断言。
type markSentCall struct {
	deliveryID int64
	msgID      string
	cardJSON   string
}

// emptyBatchCall 记录 RecordEmptyPushBatch 收到的实参，供空批次闸门断言。
type emptyBatchCall struct {
	userID   int64
	idempKey string
	gate     types.BatchExitGate
	counts   types.PipelineCounts
}

type fakeStore struct {
	mu          sync.Mutex
	unpushed    []types.ContentItem
	nextDelID   int64
	sentAlready bool // true = 模拟重试时该 (batch, content) 已发过
	inserted    []types.Delivery
	marked      []markSentCall
	// 空批次记账侧
	emptyErr error // 非 nil = 模拟记账失败（验"记账失败不阻断正常终态"）
	// emptySkipped = true 模拟 store 侧防覆写护栏拦下（该 traceID 已有真实批次）：
	// 真 store 此时返回 (0, true, nil)，替身必须照此形状返回，否则测的就不是护栏路径。
	emptySkipped bool
	emptyBatchN  int64 // 递增出 batch_id；0 值起步即从 1 开始
	emptyCalls   []emptyBatchCall
}

func (s *fakeStore) RecordEmptyPushBatch(_ context.Context, userID int64, idempKey string,
	gate types.BatchExitGate, counts types.PipelineCounts) (int64, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.emptyCalls = append(s.emptyCalls, emptyBatchCall{userID, idempKey, gate, counts})
	if s.emptyErr != nil {
		return 0, false, s.emptyErr
	}
	if s.emptySkipped {
		return 0, true, nil // 护栏拦下：id=0、skipped=true、err=nil
	}
	s.emptyBatchN++
	return s.emptyBatchN, false, nil
}

func (s *fakeStore) emptyBatchCalls() []emptyBatchCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]emptyBatchCall(nil), s.emptyCalls...)
}

func (s *fakeStore) GetSource(_ context.Context, id int64) (*types.Source, error) {
	return &types.Source{ID: id, Title: "fake-source", Platform: "rss"}, nil
}

func (s *fakeStore) ListDueSourcesByUser(context.Context, int64) ([]types.Source, error) {
	return nil, nil
}

func (s *fakeStore) UpsertContentItem(context.Context, *types.ContentItem) (int64, bool, error) {
	return 0, false, nil
}

func (s *fakeStore) ListRecentSimhashesByUser(context.Context, int64, time.Time, []int64) ([]int64, error) {
	return nil, nil
}

func (s *fakeStore) UpdateSourceFetchState(context.Context, int64, time.Time, time.Time, int) error {
	return nil
}

func (s *fakeStore) ListUnpushedByUser(context.Context, int64, int, int) ([]types.ContentItem, error) {
	return s.unpushed, nil
}

func (s *fakeStore) CreatePushBatchIdempotent(context.Context, int64, string) (int64, error) {
	return 7, nil
}

func (s *fakeStore) InsertDeliveryIdempotent(_ context.Context, d *types.Delivery) (int64, bool, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sentAlready {
		return 99, true, true, nil
	}
	s.nextDelID++
	s.inserted = append(s.inserted, *d)
	return s.nextDelID, false, false, nil
}

func (s *fakeStore) UpdatePushBatchStatus(context.Context, int64, types.BatchStatus) error {
	return nil
}

func (s *fakeStore) MarkDeliverySent(_ context.Context, id int64, msgID string, cardJSON json.RawMessage, _ time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.marked = append(s.marked, markSentCall{deliveryID: id, msgID: msgID, cardJSON: string(cardJSON)})
	return nil
}

func (s *fakeStore) markedCalls() []markSentCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]markSentCall(nil), s.marked...)
}

func (s *fakeStore) insertedRows() []types.Delivery {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]types.Delivery(nil), s.inserted...)
}

// ============================================================
// Activity 级测试
// ============================================================

func TestEvolveProfile_NilEvolverNoop(t *testing.T) {
	a := NewActivities(fakeFetcher{}, fakeScorer{}, fakeCardGen{}, &fakePusher{}, &fakeStore{}, fakeFeishu{}, nil, nil)
	if err := a.EvolveProfile(context.Background(), EvolveIn{UserID: 1, TraceID: "tr"}); err != nil {
		t.Fatalf("evolver 未注入时 EvolveProfile 应 no-op 成功: %v", err)
	}
}

func TestEvolveProfile_DelegatesArgsAndError(t *testing.T) {
	ev := &fakeEvolver{err: errors.New("演化失败")}
	a := NewActivities(fakeFetcher{}, fakeScorer{}, fakeCardGen{}, &fakePusher{}, &fakeStore{}, fakeFeishu{}, ev, nil)

	err := a.EvolveProfile(context.Background(), EvolveIn{UserID: 5, TraceID: "tr-5"})
	if err == nil || err.Error() != "演化失败" {
		t.Fatalf("evolver 错误应原样上抛交给 RetryPolicy，实得: %v", err)
	}
	if calls, userID, traceID := ev.snapshot(); calls != 1 || userID != 5 || traceID != "tr-5" {
		t.Errorf("evolver 实参不符: calls=%d userID=%d traceID=%q", calls, userID, traceID)
	}
}

// TestRecordEmptyBatch_DelegatesArgs 空批次记账把 traceID 当幂等键、
// 闸门与漏斗原样下传 store（009）。
func TestRecordEmptyBatch_DelegatesArgs(t *testing.T) {
	st := &fakeStore{}
	a := NewActivities(fakeFetcher{}, fakeScorer{}, fakeCardGen{}, &fakePusher{}, st, fakeFeishu{}, nil, nil)

	counts := types.PipelineCounts{}.WithFetched(20).WithDeduped(0)
	in := RecordEmptyIn{UserID: 5, TraceID: "tr-empty", Gate: types.BatchExitGateDedup, Counts: counts}
	if err := a.RecordEmptyBatch(context.Background(), in); err != nil {
		t.Fatalf("RecordEmptyBatch 意外报错: %v", err)
	}

	calls := st.emptyBatchCalls()
	if len(calls) != 1 {
		t.Fatalf("应恰好调 store 一次，实得 %d", len(calls))
	}
	got := calls[0]
	// 幂等键必须就是 traceID：Temporal 重试本活动时靠它命中 004 的
	// uq_push_batches_idem 复用同一行，而不是每次重试长一行空批次。
	if got.userID != 5 || got.idempKey != "tr-empty" || got.gate != types.BatchExitGateDedup {
		t.Errorf("store 实参不符: %+v", got)
	}
	if got.counts.Fetched == nil || *got.counts.Fetched != 20 {
		t.Errorf("fetched 应为 20，实得: %v", got.counts.Fetched)
	}
	if got.counts.Deduped == nil || *got.counts.Deduped != 0 {
		t.Errorf("deduped 应为 0（跑了得 0），实得: %v", got.counts.Deduped)
	}
	// 未跑到的阶段必须缺席而非 0——"没跑"与"跑了得 0"是两种不同的事故。
	if got.counts.Scored != nil || got.counts.Selected != nil || got.counts.Cards != nil {
		t.Errorf("dedup 闸门退出时下游阶段应缺席（nil），实得: %+v", got.counts)
	}
}

// TestRecordEmptyBatch_SkipIsNotAnError store 侧护栏拦下（该 traceID 已有真实批次）
// 时返回 (0, true, nil)，活动必须成功返回——护栏拦对了，它不是错误，不该被包成
// ApplicationError 触发重试（重试只会被同一道护栏再拦一次）。
//
// 活动此时打的那条 slog.Info 是刻意的、也是本用例覆盖不到的部分：断言日志需要接管
// slog 全局 handler，而本包的活动都用包级 slog（非注入 logger），拦它会污染并发跑的
// 其他用例。这里钉住的是**控制流**（skipped ⇒ 成功返回、不报错），出声与否由
// 代码审查保证——日志断言的性价比在此低于它引入的耦合。
func TestRecordEmptyBatch_SkipIsNotAnError(t *testing.T) {
	st := &fakeStore{emptySkipped: true} // 模拟真 store 的 (0, true, nil)
	a := NewActivities(fakeFetcher{}, fakeScorer{}, fakeCardGen{}, &fakePusher{}, st, fakeFeishu{}, nil, nil)

	in := RecordEmptyIn{UserID: 1, TraceID: "tr", Gate: types.BatchExitGateFetch}
	if err := a.RecordEmptyBatch(context.Background(), in); err != nil {
		t.Fatalf("护栏跳过不是错误，实得: %v", err)
	}
	// 护栏是 store 侧的判断，活动必须**如实转达**而非自作主张跳过调用。
	if calls := st.emptyBatchCalls(); len(calls) != 1 {
		t.Fatalf("应仍调 store 一次（护栏在 store 侧生效），实得 %d", len(calls))
	}
}

// TestRecordEmptyBatch_ValidationIsNonRetryable 确定性失败（闸门传空是代码 bug）
// 必须包成不可重试，否则 RetryPolicy 会把同一个 bug 重试到上限才罢休。
func TestRecordEmptyBatch_ValidationIsNonRetryable(t *testing.T) {
	st := &fakeStore{emptyErr: types.NewAppError(types.CodeValidation, "空批次必须带退出闸门", nil)}
	a := NewActivities(fakeFetcher{}, fakeScorer{}, fakeCardGen{}, &fakePusher{}, st, fakeFeishu{}, nil, nil)

	err := a.RecordEmptyBatch(context.Background(), RecordEmptyIn{UserID: 1, TraceID: "tr"})
	if err == nil {
		t.Fatal("VALIDATION 应上抛错误")
	}
	var appErr *temporal.ApplicationError
	if !errors.As(err, &appErr) || !appErr.NonRetryable() {
		t.Fatalf("VALIDATION 应包成不可重试的 ApplicationError，实得: %#v", err)
	}
	// 红线 3（错误卫生）：ApplicationError 携带的 Message 只能是 AppError.Message
	// 这句人话，不能是原始 error 链（可能含连接串 / Temporal 服务端原文）。
	if appErr.Message() != "空批次必须带退出闸门" {
		t.Errorf("消息应为 AppError.Message 人话，实得: %q", appErr.Message())
	}
	// Type 必须恰为纯 Code，NonRetryableErrorTypes 才匹配得上（见 nonRetryable 注释）。
	if appErr.Type() != string(types.CodeValidation) {
		t.Errorf("Type 应为纯 Code %q，实得 %q", types.CodeValidation, appErr.Type())
	}
}

// TestRecordEmptyBatch_DBErrorMessageIsClean 红线 3：DB 原始 error 链（连接串等）
// 不得进 ApplicationError 的 Message——那是会被展示/喂进上下文的那一层。
func TestRecordEmptyBatch_DBErrorMessageIsClean(t *testing.T) {
	raw := errors.New("failed to connect to `host=10.0.0.5 user=vane password=hunter2`")
	st := &fakeStore{emptyErr: types.NewAppError(types.CodeDatabase, "记录空批次（user=1, gate=fetch）", raw)}
	a := NewActivities(fakeFetcher{}, fakeScorer{}, fakeCardGen{}, &fakePusher{}, st, fakeFeishu{}, nil, nil)

	err := a.RecordEmptyBatch(context.Background(), RecordEmptyIn{
		UserID: 1, TraceID: "tr", Gate: types.BatchExitGateFetch})
	var appErr *temporal.ApplicationError
	if errors.As(err, &appErr) && strings.Contains(appErr.Message(), "password") {
		t.Errorf("原始 error 链不得进 Message，实得: %q", appErr.Message())
	}
}

// TestRecordEmptyBatch_DBErrorStaysRetryable 库连接抖动是可重试的，
// 不该被误包成不可重试——那会让一次瞬时抖动直接丢掉这行记录。
func TestRecordEmptyBatch_DBErrorStaysRetryable(t *testing.T) {
	st := &fakeStore{emptyErr: types.NewAppError(types.CodeDatabase, "记录空批次", errors.New("conn reset"))}
	a := NewActivities(fakeFetcher{}, fakeScorer{}, fakeCardGen{}, &fakePusher{}, st, fakeFeishu{}, nil, nil)

	err := a.RecordEmptyBatch(context.Background(), RecordEmptyIn{
		UserID: 1, TraceID: "tr", Gate: types.BatchExitGateFetch})
	if err == nil {
		t.Fatal("DB 错误应上抛")
	}
	var appErr *temporal.ApplicationError
	if errors.As(err, &appErr) && appErr.NonRetryable() {
		t.Error("CodeDatabase 应保持可重试，不得被包成不可重试")
	}
}

// TestPush_BuildCardInjectedAndPersisted Push 四步重排（契约 §8.2）：
// 建投递（body_md 入库、card_json 留空）→ 经注入 buildCard 用真实 delivery id
// 构最终卡 → 推送 → MarkDeliverySent 回填同一张最终卡。
func TestPush_BuildCardInjectedAndPersisted(t *testing.T) {
	st := &fakeStore{nextDelID: 41} // 首条投递 id=42
	push := &fakePusher{msgID: "om_42"}

	type buildArgs struct {
		bodyMD     string
		deliveryID int64
		state      feedback.CardState
	}
	var mu sync.Mutex
	var builds []buildArgs
	buildCard := func(input feedback.CardInput) string {
		mu.Lock()
		defer mu.Unlock()
		builds = append(builds, buildArgs{input.BodyMD, input.DeliveryID, input.State})
		return fmt.Sprintf(`{"final_card":true,"delivery_id":%d}`, input.DeliveryID)
	}
	a := NewActivities(fakeFetcher{}, fakeScorer{}, fakeCardGen{}, push, st, fakeFeishu{}, nil, buildCard)

	bodyMD := "**正文**\n\n[阅读原文](https://e.com/1)"
	in := PushIn{UserID: 1, TraceID: "tr-1", Cards: []GeneratedCard{{
		Scored: types.ScoredItem{Item: types.ContentItem{ID: 11}, Score: 80},
		BodyMD: bodyMD,
	}}}
	if err := a.Push(context.Background(), in); err != nil {
		t.Fatalf("Push 意外报错: %v", err)
	}

	if len(builds) != 1 {
		t.Fatalf("buildCard 应恰好调用一次，实得 %d", len(builds))
	}
	if builds[0].bodyMD != bodyMD || builds[0].deliveryID != 42 || builds[0].state != (feedback.CardState{}) {
		t.Errorf("buildCard 实参不符（应为 bodyMD + 真实 delivery id + 零值状态行）: %+v", builds[0])
	}

	rows := st.insertedRows()
	if len(rows) != 1 || rows[0].BodyMD != bodyMD {
		t.Fatalf("投递行应携带 body_md，实得: %+v", rows)
	}
	if len(rows[0].CardJSON) != 0 {
		t.Errorf("建投递时 card_json 应留空待 MarkDeliverySent 回填，实得: %s", rows[0].CardJSON)
	}

	wantCard := `{"final_card":true,"delivery_id":42}`
	if sent := push.sentCards(); len(sent) != 1 || sent[0] != wantCard {
		t.Errorf("推送出去的应是 buildCard 构造的最终卡，实得: %v", sent)
	}
	marked := st.markedCalls()
	if len(marked) != 1 || marked[0] != (markSentCall{deliveryID: 42, msgID: "om_42", cardJSON: wantCard}) {
		t.Errorf("MarkDeliverySent 应收到 (delID, msgID, 最终卡)，实得: %+v", marked)
	}
}

// TestPush_SentAlreadySkipsBuildAndSend 幂等分支不变：重试时已发过的投递
// 不构卡、不重推、不重复回填。
func TestPush_SentAlreadySkipsBuildAndSend(t *testing.T) {
	st := &fakeStore{sentAlready: true}
	push := &fakePusher{}
	buildCalls := 0
	buildCard := func(feedback.CardInput) string {
		buildCalls++
		return "{}"
	}
	a := NewActivities(fakeFetcher{}, fakeScorer{}, fakeCardGen{}, push, st, fakeFeishu{}, nil, buildCard)

	in := PushIn{UserID: 1, TraceID: "tr-2", Cards: []GeneratedCard{{
		Scored: types.ScoredItem{Item: types.ContentItem{ID: 11}, Score: 80},
		BodyMD: "**正文**",
	}}}
	if err := a.Push(context.Background(), in); err != nil {
		t.Fatalf("已发过的投递应算成功，实得: %v", err)
	}
	if buildCalls != 0 || len(push.sentCards()) != 0 || len(st.markedCalls()) != 0 {
		t.Errorf("已发过的投递不得构卡/重推/重复回填: builds=%d sent=%d marked=%d",
			buildCalls, len(push.sentCards()), len(st.markedCalls()))
	}
}

// TestGeneratedCard_ReplayCompatTag BodyMD 的 json tag 必须保持 "card_json"
// （契约 §8.2）：停在 CardGen 之后的 in-flight workflow 重放时，历史 payload
// 里的 card_json 必须还能解进 BodyMD，否则静默推空卡。
func TestGeneratedCard_ReplayCompatTag(t *testing.T) {
	b, err := json.Marshal(GeneratedCard{BodyMD: "md"})
	if err != nil {
		t.Fatalf("序列化失败: %v", err)
	}
	if !strings.Contains(string(b), `"card_json":"md"`) {
		t.Errorf("BodyMD 必须序列化为 card_json 键，实得: %s", b)
	}

	var gc GeneratedCard
	if err := json.Unmarshal([]byte(`{"card_json":"旧历史正文"}`), &gc); err != nil {
		t.Fatalf("反序列化失败: %v", err)
	}
	if gc.BodyMD != "旧历史正文" {
		t.Errorf("旧历史 payload 的 card_json 应解进 BodyMD，实得: %q", gc.BodyMD)
	}
}
