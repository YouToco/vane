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
	// perCallID=true 时每次调用返回 msgID+"#序号"——聚合分块用例靠它断言
	// "各块的投递回填各自块的 msgID"（跨块错记 msgID 的变异体曾全绿存活）。
	perCallID bool
	// failCalls 里列出的调用序号（1 起）返回错误——部分失败矩阵用。
	failCalls map[int]error
	calls     int
}

func (p *fakePusher) Push(_ context.Context, _, cardJSON string) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls++
	if err := p.failCalls[p.calls]; err != nil {
		return "", err
	}
	p.sent = append(p.sent, cardJSON)
	id := p.msgID
	if id == "" {
		id = "om_test"
	}
	if p.perCallID {
		id = fmt.Sprintf("%s#%d", id, p.calls)
	}
	return id, nil
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
	userID     int64
	idempKey   string
	scheduleID string
	gate       types.BatchExitGate
	counts     types.PipelineCounts
}

type fakeStore struct {
	runAuthorized       bool
	runAuthorizationErr error
	tenantGone          bool  // true = 模拟租户已注销
	tenantLiveErr       error // 非 nil = 模拟状态查询失败
	mu                  sync.Mutex
	gotBatchScheduleID  string // Push 建批次时收到的 schedule_id（P1b 穿参断言）
	unpushed            []types.ContentItem

	strictness    types.PushStrictness // GetScheduleStrictness 返回值（零值=未设置）
	strictnessErr error                // 非 nil = 模拟门槛档位查询失败（Select 应降级兜底）

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
	// P1b b3 分流捕获：hasSchedSources 决定走隔离还是用户级；两个 bySchedule 是隔离路径的返回；
	// gotSchedDueID/gotSchedUnpushedID 记录隔离路径收到的 scheduleID，供断言"确实走了本任务隔离"。
	hasSchedSources    bool
	dueBySchedule      []types.Source
	unpushedBySchedule []types.ContentItem
	gotSchedDueID      string
	gotSchedUnpushedID string
	// 抓取失败告警测试用（功能 5.2）：dueSources 供 ListDueSourcesByUser 返回，
	// updateFetchErr 模拟 UpdateSourceFetchState 落库失败（验"状态没落库不告警"）。
	dueSources     []types.Source
	updateFetchErr error
	statusSet      []types.BatchStatus // UpdatePushBatchStatus 调用记录（部分失败矩阵用）
	// 自动停用测试用（功能 5.2）：disableResult 是 DisableSourceIfActive 的返回
	// （true=本次真从 active 翻成 disabled），disableErr 模拟落库失败，disableCalls 记录入参。
	disableResult bool
	disableErr    error
	disableCalls  []int64
	// #8 卡片源归属：schedSrcForContent 映射 content_item_id→本任务命中源 id；
	// 命中即 ScheduleSourceForContent 返回 (id,true)，缺失返回 (0,false) 让调用方回退首发源。
	schedSrcForContent map[int64]int64
}

func (f *fakeStore) AuthorizeScheduledRun(context.Context, string, int64) (bool, error) {
	if f.runAuthorizationErr != nil {
		return false, f.runAuthorizationErr
	}
	return f.runAuthorized, nil
}

func TestAuthorizeRun_FailClosedScheduledAndBypassesAdHoc(t *testing.T) {
	t.Run("scheduled uses exact store decision", func(t *testing.T) {
		st := &fakeStore{runAuthorized: true}
		a := &Activities{store: st}
		params := PushParams{UserID: 7, RunKind: PushRunKindScheduled, ScheduleID: "task-7"}
		ok, err := a.AuthorizeRun(t.Context(), params)
		if err != nil || !ok {
			t.Fatalf("ok=%v err=%v", ok, err)
		}
		st.runAuthorizationErr = errors.New("db unavailable")
		if ok, err := a.AuthorizeRun(t.Context(), params); err == nil || ok {
			t.Fatalf("DB error must fail closed: ok=%v err=%v", ok, err)
		}
	})

	t.Run("ad hoc does not require a schedule row", func(t *testing.T) {
		st := &fakeStore{runAuthorizationErr: errors.New("must not be consulted")}
		a := &Activities{store: st}
		ok, err := a.AuthorizeRun(t.Context(), PushParams{UserID: 7, RunKind: PushRunKindAdHoc})
		if err != nil || !ok {
			t.Fatalf("ok=%v err=%v", ok, err)
		}
	})

	for _, tc := range []struct {
		name   string
		params PushParams
	}{
		{name: "legacy unknown kind cannot impersonate ad hoc", params: PushParams{UserID: 7}},
		{name: "scheduled kind requires schedule id", params: PushParams{UserID: 7, RunKind: PushRunKindScheduled}},
		{name: "ad hoc kind rejects schedule id", params: PushParams{UserID: 7, RunKind: PushRunKindAdHoc, ScheduleID: "task-7"}},
		{name: "user id must be positive", params: PushParams{RunKind: PushRunKindAdHoc}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := &fakeStore{runAuthorizationErr: errors.New("must not be consulted")}
			ok, err := (&Activities{store: st}).AuthorizeRun(t.Context(), tc.params)
			if err != nil || ok {
				t.Fatalf("ok=%v err=%v, want fail-closed without store call", ok, err)
			}
		})
	}
}

func (f *fakeStore) GetScheduleStrictness(context.Context, string) (types.PushStrictness, error) {
	if f.strictnessErr != nil {
		return "", f.strictnessErr
	}
	return f.strictness, nil
}

// tenantGone 让用例模拟「租户已注销」（D9）。零值 false = 租户在服务中，
// 既有用例因此不受影响。
func (f *fakeStore) TenantLiveForUser(context.Context, int64) (bool, error) {
	if f.tenantGone {
		return false, nil
	}
	if f.tenantLiveErr != nil {
		return false, f.tenantLiveErr
	}
	return true, nil
}

func (s *fakeStore) DisableSourceIfActive(_ context.Context, id int64) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.disableCalls = append(s.disableCalls, id)
	return s.disableResult, s.disableErr
}

func (s *fakeStore) RecordEmptyPushBatch(_ context.Context, userID int64, idempKey, scheduleID string,
	gate types.BatchExitGate, counts types.PipelineCounts) (int64, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.emptyCalls = append(s.emptyCalls, emptyBatchCall{userID, idempKey, scheduleID, gate, counts})
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
	// Title 编码 id（src-<id>），便于断言构卡用了哪个源（首发源 vs 任务命中源）。
	return &types.Source{ID: id, Title: fmt.Sprintf("src-%d", id), Platform: "rss"}, nil
}

func (s *fakeStore) ScheduleSourceForContent(_ context.Context, contentItemID int64, _ string) (int64, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if sid, ok := s.schedSrcForContent[contentItemID]; ok {
		return sid, true, nil
	}
	return 0, false, nil
}

func (s *fakeStore) ListDueSourcesByUser(context.Context, int64) ([]types.Source, error) {
	return s.dueSources, nil // 默认 nil：不涉及抓取的既有测试行为不变。
}

func (s *fakeStore) UpsertContentItem(context.Context, *types.ContentItem) (int64, bool, error) {
	return 0, false, nil
}

func (s *fakeStore) ListRecentSimhashesByUser(context.Context, int64, time.Time, []int64) ([]int64, error) {
	return nil, nil
}

func (s *fakeStore) UpdateSourceFetchState(context.Context, int64, time.Time, time.Time, int) error {
	return s.updateFetchErr // 默认 nil；置非 nil 模拟落库失败（功能 5.2 gating 测试）。
}

func (s *fakeStore) ListUnpushedByUser(context.Context, int64, int, int) ([]types.ContentItem, error) {
	return s.unpushed, nil
}

func (s *fakeStore) ScheduleHasSources(_ context.Context, _ string) (bool, error) {
	return s.hasSchedSources, nil
}

func (s *fakeStore) ListDueSourcesBySchedule(_ context.Context, scheduleID string) ([]types.Source, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gotSchedDueID = scheduleID
	return s.dueBySchedule, nil
}

func (s *fakeStore) ListUnpushedBySchedule(_ context.Context, scheduleID string, _, _ int) ([]types.ContentItem, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gotSchedUnpushedID = scheduleID
	return s.unpushedBySchedule, nil
}

func (s *fakeStore) CreatePushBatchIdempotent(_ context.Context, _ int64, _, scheduleID string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gotBatchScheduleID = scheduleID // 供断言 PushIn.ScheduleID 穿到建批次（P1b）
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

func (s *fakeStore) UpdatePushBatchStatus(_ context.Context, _ int64, st types.BatchStatus) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.statusSet = append(s.statusSet, st)
	return nil
}

func (s *fakeStore) statusCalls() []types.BatchStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]types.BatchStatus(nil), s.statusSet...)
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
	a := NewActivities(fakeFetcher{}, fakeScorer{}, fakeCardGen{}, &fakePusher{}, &fakeStore{}, fakeFeishu{}, nil, nil, nil, nil)
	if err := a.EvolveProfile(context.Background(), EvolveIn{UserID: 1, TraceID: "tr"}); err != nil {
		t.Fatalf("evolver 未注入时 EvolveProfile 应 no-op 成功: %v", err)
	}
}

func TestEvolveProfile_DelegatesArgsAndError(t *testing.T) {
	ev := &fakeEvolver{err: errors.New("演化失败")}
	a := NewActivities(fakeFetcher{}, fakeScorer{}, fakeCardGen{}, &fakePusher{}, &fakeStore{}, fakeFeishu{}, ev, nil, nil, nil)

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
	a := NewActivities(fakeFetcher{}, fakeScorer{}, fakeCardGen{}, &fakePusher{}, st, fakeFeishu{}, nil, nil, nil, nil)

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
	a := NewActivities(fakeFetcher{}, fakeScorer{}, fakeCardGen{}, &fakePusher{}, st, fakeFeishu{}, nil, nil, nil, nil)

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
	a := NewActivities(fakeFetcher{}, fakeScorer{}, fakeCardGen{}, &fakePusher{}, st, fakeFeishu{}, nil, nil, nil, nil)

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
	a := NewActivities(fakeFetcher{}, fakeScorer{}, fakeCardGen{}, &fakePusher{}, st, fakeFeishu{}, nil, nil, nil, nil)

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
	a := NewActivities(fakeFetcher{}, fakeScorer{}, fakeCardGen{}, &fakePusher{}, st, fakeFeishu{}, nil, nil, nil, nil)

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

// TestPush_AggregateCardBuiltAndPersisted 聚合卡改版（附录 A）：一批一张聚合卡。
// 建 N 条投递（body_md 入库、card_json 留空）→ buildAggCard 拿全部条目与真实
// delivery id 构一张卡 → 一次推送 → 每条投递 MarkDeliverySent 回填同一 msgID 同一卡。
func TestPush_AggregateCardBuiltAndPersisted(t *testing.T) {
	st := &fakeStore{nextDelID: 41} // 首条投递 id=42
	push := &fakePusher{msgID: "om_agg"}

	var gotAgg []feedback.AggregateCardInput
	buildAgg := func(in feedback.AggregateCardInput) string {
		gotAgg = append(gotAgg, in)
		return `{"agg":true}`
	}
	aggHeader := func(task string, n int) (string, string) {
		return "📮 " + task + " · 今日 2 条", "blue"
	}
	a := NewActivities(fakeFetcher{}, fakeScorer{}, fakeCardGen{}, push, st, fakeFeishu{}, nil, nil, buildAgg, aggHeader)

	in := PushIn{UserID: 1, TraceID: "tr-1", TaskTitle: "Anthropic 动态", Cards: []GeneratedCard{
		{Scored: types.ScoredItem{Item: types.ContentItem{ID: 11, Title: "A"}, Score: 90}, BodyMD: "**甲**"},
		{Scored: types.ScoredItem{Item: types.ContentItem{ID: 12, Title: "B"}, Score: 80}, BodyMD: "**乙**"},
	}}
	if err := a.Push(context.Background(), in); err != nil {
		t.Fatalf("Push 意外报错: %v", err)
	}

	// 一批 → 恰好一张聚合卡、一次推送。
	if len(gotAgg) != 1 {
		t.Fatalf("buildAggCard 应恰好调用一次（一批一卡），实得 %d", len(gotAgg))
	}
	agg := gotAgg[0]
	if agg.HeaderTitle != "📮 Anthropic 动态 · 今日 2 条" || agg.HeaderTemplate != "blue" {
		t.Errorf("header 应由任务名派生，实得 %q/%q", agg.HeaderTitle, agg.HeaderTemplate)
	}
	if len(agg.Items) != 2 || agg.Items[0].DeliveryID != 42 || agg.Items[1].DeliveryID != 43 {
		t.Fatalf("聚合卡应含全部条目且携带真实 delivery id，实得 %+v", agg.Items)
	}
	if agg.Items[0].BodyMD != "**甲**" || agg.Items[1].BodyMD != "**乙**" {
		t.Errorf("各条 BodyMD 应原样入卡，实得 %+v", agg.Items)
	}

	rows := st.insertedRows()
	if len(rows) != 2 {
		t.Fatalf("应建 2 条投递，实得 %d", len(rows))
	}
	for _, r := range rows {
		if len(r.CardJSON) != 0 {
			t.Errorf("建投递时 card_json 应留空待回填，实得 %s", r.CardJSON)
		}
	}
	if sent := push.sentCards(); len(sent) != 1 || sent[0] != `{"agg":true}` {
		t.Errorf("应恰好推一张聚合卡，实得 %v", sent)
	}
	// 每条投递都回填同一 msgID 与同一张卡——重建路径靠共享 message_id 找兄弟条目。
	marked := st.markedCalls()
	if len(marked) != 2 {
		t.Fatalf("两条投递都应 MarkDeliverySent，实得 %d", len(marked))
	}
	for _, m := range marked {
		if m.msgID != "om_agg" || m.cardJSON != `{"agg":true}` {
			t.Errorf("投递应共享同一 msgID 与卡 JSON，实得 %+v", m)
		}
	}
}

// TestPush_ThreadsScheduleID 守 P1b b1：PushIn.ScheduleID 一路穿到建批次
// （push_batches.schedule_id 的写入锚点）；空串（即时/老任务触发）也如实透传。
func TestPush_ThreadsScheduleID(t *testing.T) {
	run := func(schedID string) string {
		st := &fakeStore{nextDelID: 41}
		a := NewActivities(fakeFetcher{}, fakeScorer{}, fakeCardGen{}, &fakePusher{msgID: "om_1"}, st, fakeFeishu{}, nil, nil,
			func(feedback.AggregateCardInput) string { return "{}" }, nil)
		in := PushIn{UserID: 1, ScheduleID: schedID, TraceID: "tr-" + schedID + "-x", Cards: []GeneratedCard{{
			Scored: types.ScoredItem{Item: types.ContentItem{ID: 11}, Score: 80}, BodyMD: "x",
		}}}
		if err := a.Push(context.Background(), in); err != nil {
			t.Fatalf("Push 意外报错: %v", err)
		}
		return st.gotBatchScheduleID
	}
	if got := run("push-7-abc"); got != "push-7-abc" {
		t.Errorf("定时触发应把 schedule_id 穿到建批次，实得 %q", got)
	}
	if got := run(""); got != "" {
		t.Errorf("即时触发应透传空 schedule_id（用户级语义），实得 %q", got)
	}
}

// TestPush_IsolatedCardSourceAttribution 守 #8：隔离任务（ScheduleID 非空）构卡时源归属
// 取 content_sources ∩ schedule_sources 的命中源，而非全局首发源 content_items.source_id；
// 非隔离 / 无交集时回退首发源。
func TestPush_IsolatedCardSourceAttribution(t *testing.T) {
	run := func(schedID string, itemFirstSrc int64, mapping map[int64]int64) string {
		st := &fakeStore{nextDelID: 41, schedSrcForContent: mapping}
		var got []feedback.AggregateCardInput
		buildAgg := func(in feedback.AggregateCardInput) string { got = append(got, in); return "{}" }
		a := NewActivities(fakeFetcher{}, fakeScorer{}, fakeCardGen{}, &fakePusher{msgID: "om_1"}, st, fakeFeishu{}, nil, nil, buildAgg, nil)
		in := PushIn{UserID: 1, ScheduleID: schedID, TraceID: "tr-" + schedID + "-src", Cards: []GeneratedCard{{
			Scored: types.ScoredItem{Item: types.ContentItem{ID: 11, SourceID: itemFirstSrc}, Score: 80}, BodyMD: "x",
		}}}
		if err := a.Push(context.Background(), in); err != nil {
			t.Fatalf("Push 意外报错: %v", err)
		}
		if len(got) != 1 || len(got[0].Items) != 1 {
			t.Fatalf("应构一张含一条目的卡，实得 %+v", got)
		}
		return got[0].Items[0].SourceTitle
	}
	// 隔离任务：内容 11 的首发源是 7，但本任务通过源 99 命中它 → 卡应标任务命中源 src-99。
	if got := run("push-1-x", 7, map[int64]int64{11: 99}); got != "src-99" {
		t.Errorf("隔离任务应标任务命中源 src-99，实得 %q", got)
	}
	// 非隔离（即时/老任务，ScheduleID 空）→ 用首发源 src-7。
	if got := run("", 7, nil); got != "src-7" {
		t.Errorf("非隔离应标首发源 src-7，实得 %q", got)
	}
	// 隔离但无交集（理论上不该发生）→ 回退首发源 src-7。
	if got := run("push-1-x", 7, nil); got != "src-7" {
		t.Errorf("无交集应回退首发源 src-7，实得 %q", got)
	}
}

// TestPush_SentAlreadySkipsBuildAndSend 幂等分支：重试时全部已发过 →
// 不构卡、不重推、不重复回填（聚合版语义与单条版一致）。
func TestPush_SentAlreadySkipsBuildAndSend(t *testing.T) {
	st := &fakeStore{sentAlready: true}
	push := &fakePusher{}
	aggCalls := 0
	buildAgg := func(feedback.AggregateCardInput) string {
		aggCalls++
		return "{}"
	}
	a := NewActivities(fakeFetcher{}, fakeScorer{}, fakeCardGen{}, push, st, fakeFeishu{}, nil, nil, buildAgg, nil)

	in := PushIn{UserID: 1, TraceID: "tr-2", Cards: []GeneratedCard{{
		Scored: types.ScoredItem{Item: types.ContentItem{ID: 11}, Score: 80},
		BodyMD: "**正文**",
	}}}
	if err := a.Push(context.Background(), in); err != nil {
		t.Fatalf("已发过的投递应算成功，实得: %v", err)
	}
	if aggCalls != 0 || len(push.sentCards()) != 0 || len(st.markedCalls()) != 0 {
		t.Errorf("已发过的投递不得构卡/重推/重复回填: builds=%d sent=%d marked=%d",
			aggCalls, len(push.sentCards()), len(st.markedCalls()))
	}
}

// TestPush_ChunkSplitting 体积护栏：条数超 aggMaxItemsPerCard 拆多张卡；
// 字节超上限对半拆。绝不静默截断条目——每条投递都必须被某张卡送达。
func TestPush_ChunkSplitting(t *testing.T) {
	st := &fakeStore{nextDelID: 0}
	push := &fakePusher{msgID: "om_chunk"}
	var sizes []int
	buildAgg := func(in feedback.AggregateCardInput) string {
		sizes = append(sizes, len(in.Items))
		return `{"n":true}`
	}
	a := NewActivities(fakeFetcher{}, fakeScorer{}, fakeCardGen{}, push, st, fakeFeishu{}, nil, nil, buildAgg, nil)

	cards := make([]GeneratedCard, aggMaxItemsPerCard+3) // 8+3=11 → 8 + 3 两张
	for i := range cards {
		cards[i] = GeneratedCard{Scored: types.ScoredItem{Item: types.ContentItem{ID: int64(100 + i)}, Score: 50}, BodyMD: "x"}
	}
	if err := a.Push(context.Background(), PushIn{UserID: 1, TraceID: "tr-3", Cards: cards}); err != nil {
		t.Fatalf("Push 意外报错: %v", err)
	}
	if len(sizes) != 2 || sizes[0] != aggMaxItemsPerCard || sizes[1] != 3 {
		t.Errorf("11 条应拆成 [8,3] 两张卡，实得 %v", sizes)
	}
	if got := len(st.markedCalls()); got != 11 {
		t.Errorf("全部 11 条投递都应送达（无静默截断），实得 %d", got)
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

// ============================================================
// 抓取失败主动告警（功能 5.2）
// ============================================================

// scriptedFetcher 按 source.ID 返回预设错误：map 里非 nil=该源抓取失败，否则抓成返回空内容。
type scriptedFetcher struct{ errByID map[int64]error }

func (f scriptedFetcher) Fetch(_ context.Context, src types.Source) ([]types.ContentItem, error) {
	return nil, f.errByID[src.ID]
}

// noOwnerFeishu 模拟"尚未捕获 owner"（OwnerOpenID 空串）。
type noOwnerFeishu struct{}

func (noOwnerFeishu) OwnerOpenID() string { return "" }

// idNotice 身份构卡器：把 markdown 原样当卡 JSON 返回，便于对推送内容做子串断言。
func idNotice(md string) string { return md }

func fetchSrc(id int64, failCount int, title string) types.Source {
	return types.Source{
		ID: id, Title: title, Platform: "rss", Capability: "feed",
		URL:       fmt.Sprintf("https://example.com/%d", id),
		FailCount: failCount, FetchIntervalSeconds: 1800,
	}
}

// TestFetch_ScheduleScoped 守 P1b b3 的分流：有源手册任务走本任务隔离
// （ListDueSourcesBySchedule + ListUnpushedBySchedule），无源任务/push_now 退回用户级。
func TestFetch_ScheduleScoped(t *testing.T) {
	mk := func(st *fakeStore) *Activities {
		return NewActivities(scriptedFetcher{}, fakeScorer{}, fakeCardGen{}, &fakePusher{}, st, fakeFeishu{}, nil, idNotice, nil, nil)
	}

	t.Run("有源手册任务：只抓/挑本任务的源", func(t *testing.T) {
		st := &fakeStore{
			hasSchedSources:    true,
			dueBySchedule:      []types.Source{fetchSrc(1, 0, "本任务源")},
			unpushedBySchedule: []types.ContentItem{{ID: 77}},
			dueSources:         []types.Source{fetchSrc(2, 0, "用户级源")}, // 走错路径才会用它
			unpushed:           []types.ContentItem{{ID: 88}},
		}
		got, err := mk(st).Fetch(context.Background(), PushParams{UserID: 7, ScheduleID: "push-7-x"})
		if err != nil {
			t.Fatalf("Fetch 报错: %v", err)
		}
		if st.gotSchedDueID != "push-7-x" || st.gotSchedUnpushedID != "push-7-x" {
			t.Errorf("应走本任务隔离路径, 实得 due=%q unpushed=%q", st.gotSchedDueID, st.gotSchedUnpushedID)
		}
		if len(got) != 1 || got[0].ID != 77 {
			t.Errorf("应返回本任务候选(77)而非用户级(88), 实得 %+v", got)
		}
	})

	t.Run("无源手册任务：退回用户级（决策#4 老任务不变）", func(t *testing.T) {
		st := &fakeStore{hasSchedSources: false, unpushed: []types.ContentItem{{ID: 88}}}
		got, err := mk(st).Fetch(context.Background(), PushParams{UserID: 7, ScheduleID: "push-7-x"})
		if err != nil {
			t.Fatalf("Fetch 报错: %v", err)
		}
		if st.gotSchedDueID != "" {
			t.Errorf("无源手册不该走隔离抓取, 实得 %q", st.gotSchedDueID)
		}
		if len(got) != 1 || got[0].ID != 88 {
			t.Errorf("应返回用户级候选(88), 实得 %+v", got)
		}
	})

	t.Run("push_now 空 ScheduleID：连开关都不查、直接用户级", func(t *testing.T) {
		st := &fakeStore{hasSchedSources: true, unpushed: []types.ContentItem{{ID: 88}}}
		got, err := mk(st).Fetch(context.Background(), PushParams{UserID: 7}) // ScheduleID=""
		if err != nil {
			t.Fatalf("Fetch 报错: %v", err)
		}
		if st.gotSchedDueID != "" || st.gotSchedUnpushedID != "" {
			t.Errorf("空 ScheduleID 应完全不碰隔离路径, 实得 due=%q unpushed=%q", st.gotSchedDueID, st.gotSchedUnpushedID)
		}
		if len(got) != 1 || got[0].ID != 88 {
			t.Errorf("应用户级候选(88), 实得 %+v", got)
		}
	})
}

type cancelBlockingFetcher struct {
	mu      sync.Mutex
	started chan struct{}
	calls   int
}

func (f *cancelBlockingFetcher) Fetch(ctx context.Context, _ types.Source) ([]types.ContentItem, error) {
	f.mu.Lock()
	f.calls++
	call := f.calls
	f.mu.Unlock()
	if call == 1 {
		close(f.started)
		<-ctx.Done()
	}
	return nil, ctx.Err()
}

func TestFetchCancellationDoesNotStartAnotherPaidSource(t *testing.T) {
	st := &fakeStore{dueSources: []types.Source{
		fetchSrc(1, 0, "first"), fetchSrc(2, 0, "second"), fetchSrc(3, 0, "third"),
	}}
	fetcher := &cancelBlockingFetcher{started: make(chan struct{})}
	a := NewActivities(fetcher, fakeScorer{}, fakeCardGen{}, &fakePusher{}, st, fakeFeishu{}, nil, idNotice, nil, nil)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		_, err := a.Fetch(ctx, PushParams{UserID: 7})
		done <- err
	}()
	select {
	case <-fetcher.started:
	case <-time.After(time.Second):
		t.Fatal("first source fetch did not start")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Fetch error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Fetch did not stop after cancellation")
	}
	fetcher.mu.Lock()
	defer fetcher.mu.Unlock()
	if fetcher.calls != 1 {
		t.Fatalf("paid source calls after cancellation = %d, want exactly first call", fetcher.calls)
	}
}

// TestFetch_AlertsExactlyOnThresholdCrossing 是 5.2 去重的核心保证：一轮里只有"恰好
// 跨过阈值"的源进告警，未到阈值的、抓成功的都不进；多个跨阈源合并成一次告警。
func TestFetch_AlertsExactlyOnThresholdCrossing(t *testing.T) {
	push := &fakePusher{}
	st := &fakeStore{dueSources: []types.Source{
		fetchSrc(1, alertFetchFailThreshold-1, "将跨阈的源"), // 失败后 = 阈值 → 告警
		fetchSrc(2, 0, "刚开始失败的源"),                       // 失败后 = 1 < 阈值 → 不告警
		fetchSrc(3, alertFetchFailThreshold-1, "抓成功的源"), // 抓成功 → 清零，不告警
	}}
	fetcher := scriptedFetcher{errByID: map[int64]error{
		1: types.NewAppError(types.CodeFetchTimeout, "抓取超时", nil),
		2: types.NewAppError(types.CodeFetchTimeout, "解析失败", nil),
		// 3 不在 map：抓成功
	}}
	a := NewActivities(fetcher, fakeScorer{}, fakeCardGen{}, push, st, fakeFeishu{}, nil, idNotice, nil, nil)

	if _, err := a.Fetch(context.Background(), PushParams{UserID: 1}); err != nil {
		t.Fatalf("Fetch 意外报错: %v", err)
	}

	cards := push.sentCards()
	if len(cards) != 1 {
		t.Fatalf("应恰好发一张汇总告警卡，实得 %d 张", len(cards))
	}
	card := cards[0]
	if !strings.Contains(card, "将跨阈的源") {
		t.Errorf("告警卡应含跨阈源，实得:\n%s", card)
	}
	if !strings.Contains(card, "抓取超时") {
		t.Errorf("告警卡应含失败原因（AppError.Message），实得:\n%s", card)
	}
	if strings.Contains(card, "刚开始失败的源") {
		t.Errorf("未到阈值的源不该进告警卡:\n%s", card)
	}
	if strings.Contains(card, "抓成功的源") {
		t.Errorf("抓成功的源不该进告警卡:\n%s", card)
	}
}

// TestFetch_NoAlertBelowOrAboveThreshold：低于阈值（第一次失败）和已越过阈值（早已坏）
// 两种都不再告警——前者没到、后者已在跨阈那轮发过，避免每轮刷屏。
func TestFetch_NoAlertBelowOrAboveThreshold(t *testing.T) {
	cases := []struct {
		name      string
		failCount int
	}{
		{"低于阈值", 0},                        // 失败后 =1 < 阈值
		{"已越过阈值", alertFetchFailThreshold}, // 失败后 =阈值+1 > 阈值（跨阈那轮早发过）
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			push := &fakePusher{}
			st := &fakeStore{dueSources: []types.Source{fetchSrc(1, c.failCount, "坏源")}}
			fetcher := scriptedFetcher{errByID: map[int64]error{1: types.NewAppError(types.CodeFetchTimeout, "超时", nil)}}
			a := NewActivities(fetcher, fakeScorer{}, fakeCardGen{}, push, st, fakeFeishu{}, nil, idNotice, nil, nil)
			if _, err := a.Fetch(context.Background(), PushParams{UserID: 1}); err != nil {
				t.Fatalf("Fetch 意外报错: %v", err)
			}
			if n := len(push.sentCards()); n != 0 {
				t.Errorf("%s 不应告警，却发了 %d 张卡", c.name, n)
			}
		})
	}
}

// TestFetch_NoAlertWhenStateWriteFails：抓取状态没落库（fail_count 没推进）时不告警，
// 否则下轮会再次算到阈值重复告警——破坏"每个 streak 只告警一次"不变量。
func TestFetch_NoAlertWhenStateWriteFails(t *testing.T) {
	push := &fakePusher{}
	st := &fakeStore{
		dueSources:     []types.Source{fetchSrc(1, alertFetchFailThreshold-1, "将跨阈的源")},
		updateFetchErr: errors.New("db 挂了"),
	}
	fetcher := scriptedFetcher{errByID: map[int64]error{1: types.NewAppError(types.CodeFetchTimeout, "超时", nil)}}
	a := NewActivities(fetcher, fakeScorer{}, fakeCardGen{}, push, st, fakeFeishu{}, nil, idNotice, nil, nil)
	if _, err := a.Fetch(context.Background(), PushParams{UserID: 1}); err != nil {
		t.Fatalf("Fetch 意外报错: %v", err)
	}
	if n := len(push.sentCards()); n != 0 {
		t.Errorf("状态未落库时不应告警，却发了 %d 张卡", n)
	}
}

// TestFetch_SilentWithoutOwnerOrBuilder：无 owner（未捕获）或未注入构卡器时，告警静默
// 降级为 no-op，绝不报错——把"飞书没配好"当正常态（同 Push 对无 owner 的约定）。
func TestFetch_SilentWithoutOwnerOrBuilder(t *testing.T) {
	mkStore := func() *fakeStore {
		return &fakeStore{dueSources: []types.Source{fetchSrc(1, alertFetchFailThreshold-1, "将跨阈的源")}}
	}
	fetcher := scriptedFetcher{errByID: map[int64]error{1: types.NewAppError(types.CodeFetchTimeout, "超时", nil)}}

	t.Run("无owner", func(t *testing.T) {
		push := &fakePusher{}
		a := NewActivities(fetcher, fakeScorer{}, fakeCardGen{}, push, mkStore(), noOwnerFeishu{}, nil, idNotice, nil, nil)
		if _, err := a.Fetch(context.Background(), PushParams{UserID: 1}); err != nil {
			t.Fatalf("Fetch 意外报错: %v", err)
		}
		if n := len(push.sentCards()); n != 0 {
			t.Errorf("无 owner 应静默，却发了 %d 张卡", n)
		}
	})
	t.Run("未注入构卡器", func(t *testing.T) {
		push := &fakePusher{}
		a := NewActivities(fetcher, fakeScorer{}, fakeCardGen{}, push, mkStore(), fakeFeishu{}, nil, nil, nil, nil)
		if _, err := a.Fetch(context.Background(), PushParams{UserID: 1}); err != nil {
			t.Fatalf("Fetch 意外报错: %v", err)
		}
		if n := len(push.sentCards()); n != 0 {
			t.Errorf("未注入 buildNotice 应静默，却发了 %d 张卡", n)
		}
	})
}

// TestFetchFailureReason：红线3——取 AppError.Message 作可读原因，非 AppError 给中性兜底。
func TestFetchFailureReason(t *testing.T) {
	if got := fetchFailureReason(types.NewAppError(types.CodeFetchTimeout, "抓取超时", nil)); got != "抓取超时" {
		t.Errorf("应取 AppError.Message，实得 %q", got)
	}
	if got := fetchFailureReason(errors.New("裸错误链不该外泄")); got != "抓取失败（未知原因）" {
		t.Errorf("非 AppError 应给中性兜底，实得 %q", got)
	}
}

// TestRenderFetchFailureAlert：告警正文含标题/平台/能力/次数/原因/链接；无标题源退回用 URL。
func TestRenderFetchFailureAlert(t *testing.T) {
	md := renderFetchFailureAlert([]fetchFailure{
		{src: types.Source{Title: "某博主", Platform: "xhs", Capability: "user_posts", URL: "https://xhs/u/1"}, failCount: 3, reason: "抓取超时"},
		{src: types.Source{Title: "", Platform: "rss", Capability: "feed", URL: "https://blog/feed"}, failCount: 3, reason: "解析失败"},
	})
	for _, want := range []string{"某博主", "xhs", "user_posts", "连续失败 3 次", "抓取超时", "https://xhs/u/1", "解析失败"} {
		if !strings.Contains(md, want) {
			t.Errorf("告警正文应含 %q，实得:\n%s", want, md)
		}
	}
	if !strings.Contains(md, "**https://blog/feed**") {
		t.Errorf("无标题源应以 URL 作标题，实得:\n%s", md)
	}
}

// TestFetch_AlertDeferredThenSentAfterStateRecovers：落库失败那轮不告警，但 fail_count
// 没推进，下一轮读到同一旧值、重算到阈值、这次落库成功 → 告警被正确补发（deferred 一轮，
// 不丢不重）。钉住"落库失败不告警"gate 之后的完整补发语义。
func TestFetch_AlertDeferredThenSentAfterStateRecovers(t *testing.T) {
	fetcher := scriptedFetcher{errByID: map[int64]error{1: types.NewAppError(types.CodeFetchTimeout, "超时", nil)}}

	// 第一轮：落库失败 → 不告警（fail_count 未推进）。
	push1 := &fakePusher{}
	st1 := &fakeStore{
		dueSources:     []types.Source{fetchSrc(1, alertFetchFailThreshold-1, "将跨阈的源")},
		updateFetchErr: errors.New("db 挂了"),
	}
	a1 := NewActivities(fetcher, fakeScorer{}, fakeCardGen{}, push1, st1, fakeFeishu{}, nil, idNotice, nil, nil)
	if _, err := a1.Fetch(context.Background(), PushParams{UserID: 1}); err != nil {
		t.Fatalf("第一轮 Fetch 意外报错: %v", err)
	}
	if n := len(push1.sentCards()); n != 0 {
		t.Fatalf("第一轮落库失败不应告警，却发了 %d 张", n)
	}

	// 第二轮：上轮未推进，源仍是同一 fail_count；这次落库成功 → 补发一次告警。
	push2 := &fakePusher{}
	st2 := &fakeStore{dueSources: []types.Source{fetchSrc(1, alertFetchFailThreshold-1, "将跨阈的源")}}
	a2 := NewActivities(fetcher, fakeScorer{}, fakeCardGen{}, push2, st2, fakeFeishu{}, nil, idNotice, nil, nil)
	if _, err := a2.Fetch(context.Background(), PushParams{UserID: 1}); err != nil {
		t.Fatalf("第二轮 Fetch 意外报错: %v", err)
	}
	if n := len(push2.sentCards()); n != 1 {
		t.Fatalf("第二轮落库成功应补发告警一次，实得 %d 张", n)
	}
}

// ============================================================
// 连续失败自动停用（功能 5.2，「告警后再宽限」）
// ============================================================

// TestFetch_AutoDisablesAtThreshold：连续失败达停用阈值 → 调 DisableSourceIfActive 停用
// 该源，并发一张「已暂停 + 如何重新启用」卡（措辞与预警卡不同）。
func TestFetch_AutoDisablesAtThreshold(t *testing.T) {
	push := &fakePusher{}
	st := &fakeStore{
		dueSources:    []types.Source{fetchSrc(1, disableFetchFailThreshold-1, "长期失效的源")},
		disableResult: true, // DisableSourceIfActive 真从 active 翻成 disabled。
	}
	fetcher := scriptedFetcher{errByID: map[int64]error{1: types.NewAppError(types.CodeFetchTimeout, "域名解析失败", nil)}}
	a := NewActivities(fetcher, fakeScorer{}, fakeCardGen{}, push, st, fakeFeishu{}, nil, idNotice, nil, nil)

	if _, err := a.Fetch(context.Background(), PushParams{UserID: 1}); err != nil {
		t.Fatalf("Fetch 意外报错: %v", err)
	}
	if len(st.disableCalls) != 1 || st.disableCalls[0] != 1 {
		t.Fatalf("达阈值应对 source 1 调一次 DisableSourceIfActive，实得 %+v", st.disableCalls)
	}
	cards := push.sentCards()
	if len(cards) != 1 {
		t.Fatalf("应恰好发一张停用卡，实得 %d 张", len(cards))
	}
	card := cards[0]
	for _, want := range []string{"已自动暂停", "长期失效的源", "域名解析失败", "重新启用"} {
		if !strings.Contains(card, want) {
			t.Errorf("停用卡应含 %q，实得:\n%s", want, card)
		}
	}
	// 停用卡措辞必须与预警卡区分（不是「建议检查」而是「已停止」）。
	if strings.Contains(card, "建议检查") {
		t.Errorf("停用卡不应复用预警卡措辞「建议检查」:\n%s", card)
	}
}

// TestFetch_NoDisableAlertWhenNotTransitioned：DisableSourceIfActive 返回 false（已是
// disabled，未翻转）时不重复发停用卡——幂等，避免每轮刷屏。
func TestFetch_NoDisableAlertWhenNotTransitioned(t *testing.T) {
	push := &fakePusher{}
	st := &fakeStore{
		dueSources:    []types.Source{fetchSrc(1, disableFetchFailThreshold, "已停用的源")}, // 失败后 > 阈值
		disableResult: false,                                                           // WHERE status='active' 命不中：未翻转。
	}
	fetcher := scriptedFetcher{errByID: map[int64]error{1: types.NewAppError(types.CodeFetchTimeout, "超时", nil)}}
	a := NewActivities(fetcher, fakeScorer{}, fakeCardGen{}, push, st, fakeFeishu{}, nil, idNotice, nil, nil)
	if _, err := a.Fetch(context.Background(), PushParams{UserID: 1}); err != nil {
		t.Fatalf("Fetch 意外报错: %v", err)
	}
	if n := len(push.sentCards()); n != 0 {
		t.Errorf("未翻转（已停用）不应再发停用卡，却发了 %d 张", n)
	}
}

// TestFetch_NoDisableBelowThreshold：告警阈值(3)与停用阈值(10)之间的源继续被抓取、
// 不停用——「告警后再宽限」的核心，短暂宕机的站点在这个窗口内恢复即清零。
func TestFetch_NoDisableBelowThreshold(t *testing.T) {
	push := &fakePusher{}
	st := &fakeStore{dueSources: []types.Source{fetchSrc(1, disableFetchFailThreshold-2, "还在宽限窗口的源")}}
	fetcher := scriptedFetcher{errByID: map[int64]error{1: types.NewAppError(types.CodeFetchTimeout, "超时", nil)}}
	a := NewActivities(fetcher, fakeScorer{}, fakeCardGen{}, push, st, fakeFeishu{}, nil, idNotice, nil, nil)
	if _, err := a.Fetch(context.Background(), PushParams{UserID: 1}); err != nil {
		t.Fatalf("Fetch 意外报错: %v", err)
	}
	if len(st.disableCalls) != 0 {
		t.Errorf("未达停用阈值不该调 DisableSourceIfActive，实得 %+v", st.disableCalls)
	}
	if n := len(push.sentCards()); n != 0 {
		t.Errorf("宽限窗口内不该发停用卡，却发了 %d 张", n)
	}
}

// TestRenderSourcesDisabledAlert：停用卡正文含阈值/源信息/两条恢复路径（信源页 + 对 AI 说）。
func TestRenderSourcesDisabledAlert(t *testing.T) {
	md := renderSourcesDisabledAlert([]fetchFailure{
		{src: types.Source{Title: "某站", Platform: "web", Capability: "feed", URL: "https://s/feed"}, failCount: disableFetchFailThreshold, reason: "连续超时"},
	})
	for _, want := range []string{"已自动暂停", "某站", "连续超时", "信源管理", "重新启用信源"} {
		if !strings.Contains(md, want) {
			t.Errorf("停用卡正文应含 %q，实得:\n%s", want, md)
		}
	}
}

// TestDedup_PageContentExemptFromSimhash 钉死 web/contents 的核心正确性（对抗审查 CRITICAL）：
// KindPageContent 内容豁免 simhash 近似去重。否则同一定价页相邻版本正文几乎相同、simhash
// 距离必 ≤ 阈值，"价格变化"会被当近重复吞掉、永远推不出去——这正是 page_watch 当年的事故。
// 对照子测试证明 simhash 确实会吞掉相同正文的 article，豁免不是空操作。
func TestDedup_PageContentExemptFromSimhash(t *testing.T) {
	a := NewActivities(fakeFetcher{}, fakeScorer{}, fakeCardGen{}, &fakePusher{}, &fakeStore{}, fakeFeishu{}, nil, nil, nil, nil)
	mk := func(kind types.Kind, ck string) types.ContentItem {
		// 两条正文逐字相同 → simhash 距离 0 → 必触发近似去重（除非豁免）。
		return types.ContentItem{Title: "定价页", Content: "gpt-5 价格 30 输出 60 缓存 3", Kind: kind, CanonicalKey: ck}
	}

	t.Run("page_content 豁免近似去重", func(t *testing.T) {
		out, err := a.Dedup(context.Background(), DedupIn{UserID: 1, Items: []types.ContentItem{
			mk(types.KindArticle, "a1"),                  // 首个 article：kept + 进 batchSeen
			mk(types.KindPageContent, "contents://u#v2"), // 与前者 simhash 距离 0，但 page_content 豁免
		}})
		if err != nil {
			t.Fatalf("Dedup 失败: %v", err)
		}
		if len(out) != 2 {
			t.Fatalf("page_content 应豁免、两条都保留，实得 %d 条：%+v", len(out), out)
		}
	})

	t.Run("对照_相同正文的article被吞", func(t *testing.T) {
		out, err := a.Dedup(context.Background(), DedupIn{UserID: 1, Items: []types.ContentItem{
			mk(types.KindArticle, "a1"),
			mk(types.KindArticle, "a2"), // 与前者正文逐字相同 → 被首个吞
		}})
		if err != nil {
			t.Fatalf("Dedup 失败: %v", err)
		}
		if len(out) != 1 {
			t.Fatalf("相同正文的 article 应被近似去重吞掉、只剩 1 条（证明豁免非空操作），实得 %d 条", len(out))
		}
	})
}

// TestPush_ByteSplitting 字节护栏：构出的卡超 aggMaxCardBytes 时对半拆直到放得下；
// 单条仍超限硬发不丢。初版实现在这里死循环（对半结果被循环顶部重算丢弃）——
// 本用例同时是终止性回归锁。
func TestPush_ByteSplitting(t *testing.T) {
	st := &fakeStore{nextDelID: 0}
	push := &fakePusher{msgID: "om_bytes"}
	var chunkSizes []int
	// 假构卡：体积正比于条数——2 条 30KB 超限，1 条 15KB 放得下。
	buildAgg := func(in feedback.AggregateCardInput) string {
		chunkSizes = append(chunkSizes, len(in.Items))
		return strings.Repeat("x", len(in.Items)*15*1024)
	}
	a := NewActivities(fakeFetcher{}, fakeScorer{}, fakeCardGen{}, push, st, fakeFeishu{}, nil, nil, buildAgg, nil)

	cards := []GeneratedCard{
		{Scored: types.ScoredItem{Item: types.ContentItem{ID: 1}, Score: 90}, BodyMD: "a"},
		{Scored: types.ScoredItem{Item: types.ContentItem{ID: 2}, Score: 80}, BodyMD: "b"},
	}
	done := make(chan error, 1)
	go func() { done <- a.Push(context.Background(), PushIn{UserID: 1, TraceID: "tr-b", Cards: cards}) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Push 意外报错: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Push 未在 5s 内返回——拆分循环疑似死循环（初版真实 bug）")
	}

	// 构卡序列：先试 2 条（超限）→ 拆成 1 条（放得下）→ 下一块再 1 条。
	if len(push.sentCards()) != 2 {
		t.Errorf("2 条超限内容应拆成 2 张单条卡发出，实得 %d 张", len(push.sentCards()))
	}
	if got := len(st.markedCalls()); got != 2 {
		t.Errorf("两条投递都应送达（无静默截断），实得 %d", got)
	}
}

// TestPush_各块msgID归属 跨块错记 msgID 的变异体曾存活（对抗审查 #9）：
// 每块独立推送得独立 msgID，块内投递必须回填**本块**的 msgID——重建按 msgID
// 找兄弟，错记会把条目并进错误的卡。
func TestPush_各块msgID归属(t *testing.T) {
	st := &fakeStore{nextDelID: 0}
	push := &fakePusher{msgID: "om", perCallID: true}
	buildAgg := func(in feedback.AggregateCardInput) string { return "{}" }
	a := NewActivities(fakeFetcher{}, fakeScorer{}, fakeCardGen{}, push, st, fakeFeishu{}, nil, nil, buildAgg, nil)

	cards := make([]GeneratedCard, aggMaxItemsPerCard+3) // [8,3] 两块
	for i := range cards {
		cards[i] = GeneratedCard{Scored: types.ScoredItem{Item: types.ContentItem{ID: int64(200 + i)}, Score: 50}, BodyMD: "x"}
	}
	if err := a.Push(context.Background(), PushIn{UserID: 1, TraceID: "tr-m", Cards: cards}); err != nil {
		t.Fatalf("Push 意外报错: %v", err)
	}
	marked := st.markedCalls()
	if len(marked) != 11 {
		t.Fatalf("应回填 11 条，实得 %d", len(marked))
	}
	for i, m := range marked {
		want := "om#1"
		if i >= aggMaxItemsPerCard {
			want = "om#2"
		}
		if m.msgID != want {
			t.Errorf("第 %d 条应回填 %s（本块 msgID），实得 %s", i, want, m.msgID)
		}
	}
}

// TestPush_单条超限硬发不丢 单条构卡即超 28KB 时硬发（可能被飞书拒，但绝不静默
// 丢弃）——静默丢弃条目的变异体曾存活（对抗审查 #11）。
func TestPush_单条超限硬发不丢(t *testing.T) {
	st := &fakeStore{nextDelID: 0}
	push := &fakePusher{msgID: "om_big"}
	buildAgg := func(in feedback.AggregateCardInput) string {
		return strings.Repeat("x", aggMaxCardBytes+1) // 单条也超限
	}
	a := NewActivities(fakeFetcher{}, fakeScorer{}, fakeCardGen{}, push, st, fakeFeishu{}, nil, nil, buildAgg, nil)

	if err := a.Push(context.Background(), PushIn{UserID: 1, TraceID: "tr-h", Cards: []GeneratedCard{
		{Scored: types.ScoredItem{Item: types.ContentItem{ID: 1}, Score: 50}, BodyMD: "x"},
	}}); err != nil {
		t.Fatalf("Push 意外报错: %v", err)
	}
	if len(push.sentCards()) != 1 || len(st.markedCalls()) != 1 {
		t.Errorf("单条超限应硬发送达，实得 sent=%d marked=%d", len(push.sentCards()), len(st.markedCalls()))
	}
}

// TestPush_部分失败可重试 对抗审查 HIGH：块 2 推送失败时不得结算 done——
// 必须返回可重试错误让 Temporal 重试补发，否则失败块的条目永久搁浅 pending
// 且（候选查询按 deliveries 任意状态排除）永不再成为候选。
func TestPush_部分失败可重试(t *testing.T) {
	st := &fakeStore{nextDelID: 0}
	push := &fakePusher{msgID: "om", failCalls: map[int]error{2: errors.New("feishu 拒收")}}
	buildAgg := func(in feedback.AggregateCardInput) string { return "{}" }
	a := NewActivities(fakeFetcher{}, fakeScorer{}, fakeCardGen{}, push, st, fakeFeishu{}, nil, nil, buildAgg, nil)

	cards := make([]GeneratedCard, aggMaxItemsPerCard+3)
	for i := range cards {
		cards[i] = GeneratedCard{Scored: types.ScoredItem{Item: types.ContentItem{ID: int64(300 + i)}, Score: 50}, BodyMD: "x"}
	}
	err := a.Push(context.Background(), PushIn{UserID: 1, TraceID: "tr-p", Cards: cards})
	if err == nil {
		t.Fatal("部分块失败必须返回错误（触发重试补发），不能静默记 done")
	}
	// 成功块正常送达。
	if got := len(st.markedCalls()); got != aggMaxItemsPerCard {
		t.Errorf("成功块 %d 条应送达，实得 %d", aggMaxItemsPerCard, got)
	}
	// 批次不得被结算为 done（终态留待重试收敛）。
	for _, sc := range st.statusCalls() {
		if sc == types.BatchStatusDone {
			t.Error("部分失败时批次不得记 done（失败块条目会永久搁浅）")
		}
	}
}

// TestNotifyEmptyResult 空结果通知（2026-07-18：Boss 点"立即推送"等不到回音来查
// 服务器——空结果与故障在用户侧必须可区分）。
func TestNotifyEmptyResult(t *testing.T) {
	n := func(v int) *int { return &v }

	t.Run("dedup 闸门带抓取数", func(t *testing.T) {
		push := &fakePusher{msgID: "om_n"}
		var noticed []string
		buildNotice := func(md string) string { noticed = append(noticed, md); return `{"notice":true}` }
		a := NewActivities(fakeFetcher{}, fakeScorer{}, fakeCardGen{}, push, &fakeStore{}, fakeFeishu{}, nil, buildNotice, nil, nil)
		if err := a.NotifyEmptyResult(context.Background(), NotifyEmptyIn{
			UserID: 1, Gate: types.BatchExitGateDedup, Counts: types.PipelineCounts{Fetched: n(7)},
		}); err != nil {
			t.Fatalf("意外报错: %v", err)
		}
		if len(noticed) != 1 || !strings.Contains(noticed[0], "7 条") || !strings.Contains(noticed[0], "都已推送过") {
			t.Errorf("dedup 文案应含抓取数与原因，实得 %v", noticed)
		}
		if len(push.sentCards()) != 1 {
			t.Errorf("应发一张通知卡，实得 %d", len(push.sentCards()))
		}
	})

	t.Run("buildNotice 未注入静默跳过", func(t *testing.T) {
		push := &fakePusher{}
		a := NewActivities(fakeFetcher{}, fakeScorer{}, fakeCardGen{}, push, &fakeStore{}, fakeFeishu{}, nil, nil, nil, nil)
		if err := a.NotifyEmptyResult(context.Background(), NotifyEmptyIn{Gate: types.BatchExitGateFetch}); err != nil {
			t.Fatalf("未注入应为 no-op，实得 %v", err)
		}
		if len(push.sentCards()) != 0 {
			t.Error("未注入不该发卡")
		}
	})

	t.Run("发送失败不上抛", func(t *testing.T) {
		push := &fakePusher{failCalls: map[int]error{1: errors.New("feishu down")}}
		a := NewActivities(fakeFetcher{}, fakeScorer{}, fakeCardGen{}, push, &fakeStore{}, fakeFeishu{}, nil,
			func(string) string { return "{}" }, nil, nil)
		if err := a.NotifyEmptyResult(context.Background(), NotifyEmptyIn{Gate: types.BatchExitGateFetch}); err != nil {
			t.Fatalf("通知失败必须吞掉（空批次是正常终态），实得 %v", err)
		}
	})
}

// TestActivities_RefusesSpendingForDeletedTenant 钉住 D9 软删除的**执行面**。
//
// 注销若只落到数据库，「已注销」就是个标记：定时调度照跑、Exa/TikHub/LLM 照花钱、
// 推送照发到对方手机上。这条用例守的是"真的停下来"。
//
// 两个活动分别是两类钱的起点：EvolveProfile 花 LLM，Fetch 花 Exa/TikHub。
func TestActivities_RefusesSpendingForDeletedTenant(t *testing.T) {
	ev := &countingEvolver{}
	fetcher := &countingFetcher{}
	// **必须给假 store 一个到期源**：否则去掉 Fetch 闸门也没有源可抓，calls 照样是 0，
	// 用例看着在测、其实测不出闸门有没有生效（探针实测确认过这一点）。
	st := &fakeStore{
		tenantGone: true,
		dueSources: []types.Source{{ID: 1, Platform: types.PlatformWeb, Capability: types.CapFeed, URL: "https://x.example/rss"}},
	}
	a := NewActivities(fetcher, fakeScorer{}, fakeCardGen{}, &fakePusher{}, st, fakeFeishu{}, ev, nil, nil, nil)

	if err := a.EvolveProfile(t.Context(), EvolveIn{UserID: 1, TraceID: "t"}); err != nil {
		t.Errorf("已注销租户应正常终态返回（报错会触发重试，而重试同样被拒）：%v", err)
	}
	if ev.calls != 0 {
		t.Errorf("已注销租户不得跑画像演化（花 LLM 的钱），实得 %d 次", ev.calls)
	}

	items, err := a.Fetch(t.Context(), PushParams{UserID: 1})
	if err != nil {
		t.Errorf("已注销租户应返回空候选而非报错：%v", err)
	}
	if len(items) != 0 {
		t.Errorf("已注销租户不应产出候选，实得 %d 条", len(items))
	}
	if fetcher.calls != 0 {
		t.Errorf("已注销租户不得抓取（Exa/TikHub 按次计费），实得 %d 次", fetcher.calls)
	}
}

// TestActivities_TenantCheckFailureDoesNotBlock：状态查询失败时**放行**。
//
// 这是一道旁路闸门。数据库抖一下就让全部用户停止推送，比"注销租户多跑一轮"糟糕得多
// ——真正的兜底在登录层（LookupSession 拒绝已注销租户的会话）。
// 失败方向选错的代价是全局停服，而不是多花一点点钱。
func TestActivities_TenantCheckFailureDoesNotBlock(t *testing.T) {
	ev := &countingEvolver{}
	st := &fakeStore{tenantLiveErr: errors.New("数据库抖动（测试构造）")}
	a := NewActivities(fakeFetcher{}, fakeScorer{}, fakeCardGen{}, &fakePusher{}, st, fakeFeishu{}, ev, nil, nil, nil)

	if err := a.EvolveProfile(t.Context(), EvolveIn{UserID: 1, TraceID: "t"}); err != nil {
		t.Errorf("查询失败不应报错：%v", err)
	}
	if ev.calls != 1 {
		t.Errorf("旁路闸门查询失败时应放行（否则 DB 抖动=全局停服），实得 %d 次调用", ev.calls)
	}
}

type countingEvolver struct{ calls int }

func (c *countingEvolver) Evolve(context.Context, int64, string) error { c.calls++; return nil }

type countingFetcher struct{ calls int }

func (c *countingFetcher) Fetch(context.Context, types.Source) ([]types.ContentItem, error) {
	c.calls++
	return nil, nil
}

// countingScorer / countingCardGen：D9 闸门断言用（同 countingEvolver 模式）——
// 闸门生效 = 下游一次都没被调。
type countingScorer struct{ calls int }

func (c *countingScorer) Score(context.Context, int64, types.ContentItem, string) (float64, error) {
	c.calls++
	return 80, nil
}

type countingCardGen struct{ calls int }

func (c *countingCardGen) Generate(context.Context, int64, types.ScoredItem, string) (string, error) {
	c.calls++
	return "md", nil
}

// TestActivities_RefusesSpendingForDeletedTenant_LateStages 补齐 D9 闸门的后三步
// （bug 狩猎 2026-07-19 HIGH：此前只有 EvolveProfile/Fetch 有闸，Fetch 之后软删的
// 租户仍会被 Score 烧 LLM、CardGen 烧 LLM、Push 发卡到已注销用户的手机上）。
// 三步的入参都刻意给真实载荷：没有载荷时去掉闸门下游也不会被调，用例就测不出
// 闸门存在与否（同上方 dueSources 的教训）。
func TestActivities_RefusesSpendingForDeletedTenant_LateStages(t *testing.T) {
	sc := &countingScorer{}
	cg := &countingCardGen{}
	push := &fakePusher{}
	st := &fakeStore{tenantGone: true}
	a := NewActivities(fakeFetcher{}, sc, cg, push, st, fakeFeishu{}, nil, nil, nil, nil)

	items := []types.ContentItem{{ID: 1, Title: "t", Content: "c"}}
	scored, err := a.Score(t.Context(), ScoreIn{UserID: 1, TraceID: "t", Items: items})
	if err != nil {
		t.Errorf("已注销租户 Score 应正常终态返回：%v", err)
	}
	if len(scored) != 0 || sc.calls != 0 {
		t.Errorf("已注销租户不得打分（花 LLM 的钱），产出 %d 条、调用 %d 次", len(scored), sc.calls)
	}

	cards, err := a.CardGen(t.Context(), CardGenIn{UserID: 1, TraceID: "t",
		Items: []types.ScoredItem{{Item: items[0], Score: 80}}})
	if err != nil {
		t.Errorf("已注销租户 CardGen 应正常终态返回：%v", err)
	}
	if len(cards) != 0 || cg.calls != 0 {
		t.Errorf("已注销租户不得生成解读（花 LLM 的钱），产出 %d 张、调用 %d 次", len(cards), cg.calls)
	}

	err = a.Push(t.Context(), PushIn{UserID: 1, TraceID: "t",
		Cards: []GeneratedCard{{Scored: types.ScoredItem{Item: items[0], Score: 80}, BodyMD: "md"}}})
	if err != nil {
		t.Errorf("已注销租户 Push 应正常终态返回：%v", err)
	}
	push.mu.Lock()
	sent := len(push.sent)
	push.mu.Unlock()
	if sent != 0 {
		t.Errorf("已注销租户不得收到推送卡片，实发 %d 张", sent)
	}
}

// TestPush_AllPermanentFailuresNonRetryable：全部块失败且全为确定性拒收
// （SendCard 层 Retryable=false，如 200673 卡片非法）时，Push 必须把整活动包成
// Temporal 不可重试——审查发现原先聚合成新 AppError 时把下层 Retryable=false
// 丢了，卡片非法照样烧满 3 次重试。对照组：可重试失败保持普通 AppError。
func TestPush_AllPermanentFailuresNonRetryable(t *testing.T) {
	perm := types.NewAppError(types.CodePushFailed, "卡片结构非法（测试构造）", nil)
	perm.Retryable = false

	push := &fakePusher{failCalls: map[int]error{1: perm}}
	st := &fakeStore{}
	a := NewActivities(fakeFetcher{}, fakeScorer{}, fakeCardGen{}, push, st, fakeFeishu{}, nil, nil,
		func(feedback.AggregateCardInput) string { return "{}" },
		func(string, int) (string, string) { return "t", "tpl" })

	err := a.Push(t.Context(), PushIn{UserID: 1, TraceID: "t-perm",
		Cards: []GeneratedCard{{Scored: types.ScoredItem{Item: types.ContentItem{ID: 1}, Score: 80}, BodyMD: "md"}}})
	if err == nil {
		t.Fatal("全灭应返回错误")
	}
	var appErr *temporal.ApplicationError
	if !errors.As(err, &appErr) || !appErr.NonRetryable() {
		t.Fatalf("全为确定性拒收时应为不可重试 ApplicationError，实得 %T: %v", err, err)
	}

	// 对照组：可重试失败（瞬态）→ 普通 AppError，交给 RetryPolicy。
	retryable := types.NewAppError(types.CodePushFailed, "飞书 5xx（测试构造）", nil)
	push2 := &fakePusher{failCalls: map[int]error{1: retryable}}
	a2 := NewActivities(fakeFetcher{}, fakeScorer{}, fakeCardGen{}, push2, &fakeStore{}, fakeFeishu{}, nil, nil,
		func(feedback.AggregateCardInput) string { return "{}" },
		func(string, int) (string, string) { return "t", "tpl" })
	err2 := a2.Push(t.Context(), PushIn{UserID: 1, TraceID: "t-transient",
		Cards: []GeneratedCard{{Scored: types.ScoredItem{Item: types.ContentItem{ID: 2}, Score: 80}, BodyMD: "md"}}})
	if err2 == nil {
		t.Fatal("全灭应返回错误")
	}
	var appErr2 *temporal.ApplicationError
	if errors.As(err2, &appErr2) && appErr2.NonRetryable() {
		t.Fatal("瞬态失败不得被包成不可重试（会让飞书抖动直接杀掉批次）")
	}
}
