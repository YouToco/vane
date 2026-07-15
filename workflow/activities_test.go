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

type fakeStore struct {
	mu          sync.Mutex
	unpushed    []types.ContentItem
	nextDelID   int64
	sentAlready bool // true = 模拟重试时该 (batch, content) 已发过
	inserted    []types.Delivery
	marked      []markSentCall
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
	buildCard := func(bodyMD string, deliveryID int64, cs feedback.CardState) string {
		mu.Lock()
		defer mu.Unlock()
		builds = append(builds, buildArgs{bodyMD, deliveryID, cs})
		return fmt.Sprintf(`{"final_card":true,"delivery_id":%d}`, deliveryID)
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
	buildCard := func(string, int64, feedback.CardState) string {
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
