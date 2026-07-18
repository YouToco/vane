package workflow

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/YouToco/vane/types"
)

// ctrlScorer 是可编排的打分假件：按条目 ID 定制延迟与成败，并记录并发峰值。
// 并发峰值是"并发确实发生了"的唯一硬证据——只看耗时会被机器负载搅浑。
type ctrlScorer struct {
	mu          sync.Mutex
	inflight    int
	maxInflight int
	calls       int
	delay       func(id int64) time.Duration
	fail        func(id int64) bool
	// block 非 nil 时，每次调用都阻塞到它被关闭，且**刻意不理会 ctx**——
	// 用来把 goroutine 钉在信号量后面，制造「令牌被占满、其余都在排队」的局面。
	block chan struct{}
}

func (s *ctrlScorer) Score(ctx context.Context, _ int64, item types.ContentItem, _ string) (float64, error) {
	s.mu.Lock()
	s.calls++
	s.inflight++
	if s.inflight > s.maxInflight {
		s.maxInflight = s.inflight
	}
	s.mu.Unlock()

	if s.block != nil {
		<-s.block
	}
	defer func() {
		s.mu.Lock()
		s.inflight--
		s.mu.Unlock()
	}()

	if s.delay != nil {
		select {
		case <-time.After(s.delay(item.ID)):
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	if s.fail != nil && s.fail(item.ID) {
		return 0, errors.New("打分失败（测试构造）")
	}
	return float64(item.ID), nil
}

func (s *ctrlScorer) peakInflight() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.maxInflight
}

func (s *ctrlScorer) snapshot() (inflight, calls int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inflight, s.calls
}

// ctrlCardGen 同上，用于 CardGen 一侧。body 里带上 ID 便于断言顺序。
type ctrlCardGen struct {
	mu          sync.Mutex
	inflight    int
	maxInflight int
	delay       func(id int64) time.Duration
	fail        func(id int64) bool
}

func (g *ctrlCardGen) Generate(ctx context.Context, _ int64, si types.ScoredItem, _ string) (string, error) {
	g.mu.Lock()
	g.inflight++
	if g.inflight > g.maxInflight {
		g.maxInflight = g.inflight
	}
	g.mu.Unlock()
	defer func() {
		g.mu.Lock()
		g.inflight--
		g.mu.Unlock()
	}()

	if g.delay != nil {
		select {
		case <-time.After(g.delay(si.Item.ID)):
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	if g.fail != nil && g.fail(si.Item.ID) {
		return "", errors.New("生成失败（测试构造）")
	}
	return fmt.Sprintf("body-%d", si.Item.ID), nil
}

func (g *ctrlCardGen) peakInflight() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.maxInflight
}

func itemsWithIDs(n int) []types.ContentItem {
	out := make([]types.ContentItem, n)
	for i := range out {
		out[i] = types.ContentItem{ID: int64(i + 1), Title: fmt.Sprintf("条目 %d", i+1)}
	}
	return out
}

func scoredWithIDs(n int) []types.ScoredItem {
	out := make([]types.ScoredItem, n)
	for i, it := range itemsWithIDs(n) {
		out[i] = types.ScoredItem{Item: it, Score: float64(100 - i)}
	}
	return out
}

func newActivitiesWith(sc Scorer, cg CardGenerator) *Activities {
	return NewActivities(fakeFetcher{}, sc, cg, &fakePusher{}, &fakeStore{}, fakeFeishu{}, nil, nil, nil, nil)
}

// TestScore_PreservesInputOrderUnderReversedCompletion 是本次并发化最要紧的一条守卫。
//
// 构造让**完成顺序与输入顺序完全相反**（第 1 条睡最久、最后一条最先完成）。
// 若实现按完成先后 append，产出会是倒序；按下标预留槽位则与串行版逐字节相同。
//
// 为什么这条不能省：并发化最常见的写法就是 `mu.Lock(); out = append(out, v)`，
// 它在功能测试里完全正常（元素一个不少），只是顺序变了——而顺序恰恰是
// CardGen 一侧决定卡面排列的东西。这种缺陷不会报错，只会让每次推送的卡长得不一样。
func TestScore_PreservesInputOrderUnderReversedCompletion(t *testing.T) {
	const n = 12
	items := itemsWithIDs(n)
	sc := &ctrlScorer{
		// ID 越小睡越久 ⇒ 完成顺序与输入顺序相反。
		delay: func(id int64) time.Duration {
			return time.Duration(n-int(id)+1) * 8 * time.Millisecond
		},
	}
	a := newActivitiesWith(sc, fakeCardGen{})

	got, err := a.Score(t.Context(), ScoreIn{UserID: 1, TraceID: "t", Items: items})
	if err != nil {
		t.Fatalf("Score() 意外报错: %v", err)
	}
	if len(got) != n {
		t.Fatalf("应返回 %d 条，实得 %d", n, len(got))
	}
	for i, s := range got {
		if want := items[i].ID; s.Item.ID != want {
			t.Errorf("第 %d 位应是 ID=%d，实得 ID=%d —— 产出按完成先后收集了，"+
				"输入顺序没被保留", i, want, s.Item.ID)
		}
	}
	if peak := sc.peakInflight(); peak < 2 {
		t.Errorf("并发峰值只有 %d —— 说明根本没并发起来，这条用例也就没验到东西", peak)
	}
}

// TestCardGen_PreservesInputOrder：顺序在 CardGen 侧是硬需求——Push 按本切片顺序
// 建 pending、按序分块、按序填进聚合卡 Items，全程无排序（activities.go 已核对）。
// 也就是说这里乱序 = 同一批内容每次推送的卡面排列都不一样。
func TestCardGen_PreservesInputOrder(t *testing.T) {
	const n = 10
	in := scoredWithIDs(n)
	cg := &ctrlCardGen{
		delay: func(id int64) time.Duration {
			return time.Duration(n-int(id)+1) * 8 * time.Millisecond
		},
	}
	a := newActivitiesWith(fakeScorer{}, cg)

	got, err := a.CardGen(t.Context(), CardGenIn{UserID: 1, TraceID: "t", Items: in})
	if err != nil {
		t.Fatalf("CardGen() 意外报错: %v", err)
	}
	if len(got) != n {
		t.Fatalf("应返回 %d 张，实得 %d", n, len(got))
	}
	for i, c := range got {
		wantID := in[i].Item.ID
		if c.Scored.Item.ID != wantID {
			t.Errorf("第 %d 张应是 ID=%d，实得 %d —— 卡面排列会随机变化", i, wantID, c.Scored.Item.ID)
		}
		if wantBody := fmt.Sprintf("body-%d", wantID); c.BodyMD != wantBody {
			t.Errorf("第 %d 张正文错配: %q，期望 %q —— 正文与条目串了行", i, c.BodyMD, wantBody)
		}
	}
	if peak := cg.peakInflight(); peak < 2 {
		t.Errorf("并发峰值只有 %d —— 没并发起来", peak)
	}
}

// TestScore_FanoutIsBounded：扇出上限必须真的封顶。
// 无界扇出在小批次下看不出问题，等到某天批次变成几千条才会一次性拉起几千个 goroutine。
func TestScore_FanoutIsBounded(t *testing.T) {
	items := itemsWithIDs(parBatchFanout * 3)
	sc := &ctrlScorer{delay: func(int64) time.Duration { return 10 * time.Millisecond }}
	a := newActivitiesWith(sc, fakeCardGen{})

	if _, err := a.Score(t.Context(), ScoreIn{UserID: 1, TraceID: "t", Items: items}); err != nil {
		t.Fatalf("Score() 意外报错: %v", err)
	}
	peak := sc.peakInflight()
	if peak > parBatchFanout {
		t.Errorf("并发峰值 %d 超过扇出上限 %d —— 上限没生效", peak, parBatchFanout)
	}
	if peak < 2 {
		t.Errorf("并发峰值 %d，没并发起来", peak)
	}
}

// TestScore_PartialFailureSkipsAndKeepsOrder：单条失败跳过、其余保持相对顺序、不报错。
// 这是串行版就有的语义，并发化不得改动。
func TestScore_PartialFailureSkipsAndKeepsOrder(t *testing.T) {
	items := itemsWithIDs(8)
	sc := &ctrlScorer{
		delay: func(id int64) time.Duration { return time.Duration(9-id) * 5 * time.Millisecond },
		fail:  func(id int64) bool { return id%2 == 0 },
	}
	a := newActivitiesWith(sc, fakeCardGen{})

	got, err := a.Score(t.Context(), ScoreIn{UserID: 1, TraceID: "t", Items: items})
	if err != nil {
		t.Fatalf("部分失败不应报错，实得: %v", err)
	}
	want := []int64{1, 3, 5, 7}
	if len(got) != len(want) {
		t.Fatalf("应返回 %d 条成功项，实得 %d", len(want), len(got))
	}
	for i, s := range got {
		if s.Item.ID != want[i] {
			t.Errorf("第 %d 位应是 ID=%d，实得 %d —— 失败项跳过后剩余顺序被打乱", i, want[i], s.Item.ID)
		}
	}
}

// TestScore_AllFailReturnsLLMUnavailable：整批全失败必须报可重试的 LLM 不可用，
// 而不是安静地返回空切片——空切片会被下游当成"本来就没内容"，推送静默消失。
func TestScore_AllFailReturnsLLMUnavailable(t *testing.T) {
	sc := &ctrlScorer{fail: func(int64) bool { return true }}
	a := newActivitiesWith(sc, fakeCardGen{})

	_, err := a.Score(t.Context(), ScoreIn{UserID: 1, TraceID: "t", Items: itemsWithIDs(5)})
	if err == nil {
		t.Fatal("整批失败必须报错")
	}
	var ae *types.AppError
	if !errors.As(err, &ae) || ae.Code != types.CodeLLMUnavailable {
		t.Errorf("错误码应为 %s，实得 %v", types.CodeLLMUnavailable, err)
	}
}

// TestCardGen_AllFailReturnsLLMUnavailable：CardGen 一侧同一条闸门。
func TestCardGen_AllFailReturnsLLMUnavailable(t *testing.T) {
	cg := &ctrlCardGen{fail: func(int64) bool { return true }}
	a := newActivitiesWith(fakeScorer{}, cg)

	_, err := a.CardGen(t.Context(), CardGenIn{UserID: 1, TraceID: "t", Items: scoredWithIDs(4)})
	if err == nil {
		t.Fatal("整批失败必须报错")
	}
	var ae *types.AppError
	if !errors.As(err, &ae) || ae.Code != types.CodeLLMUnavailable {
		t.Errorf("错误码应为 %s，实得 %v", types.CodeLLMUnavailable, err)
	}
}

// TestScore_EmptyInputReturnsNonNilEmpty：空输入返回**非 nil** 空切片、不报错。
//
// nil 与空切片的差别会穿过进程边界：Temporal 把活动结果序列化成 JSON 交给下一步，
// nil 编成 null、空切片编成 []。串行版用 make([]T, 0, n) 产出后者，并发版必须一致。
func TestScore_EmptyInputReturnsNonNilEmpty(t *testing.T) {
	a := newActivitiesWith(&ctrlScorer{}, fakeCardGen{})

	got, err := a.Score(t.Context(), ScoreIn{UserID: 1, TraceID: "t", Items: nil})
	if err != nil {
		t.Fatalf("空输入不应报错: %v", err)
	}
	if got == nil {
		t.Error("空输入应返回非 nil 空切片：Temporal 会把 nil 编成 null 传给下一个活动")
	}
	if len(got) != 0 {
		t.Errorf("空输入应返回空切片，实得 %d 条", len(got))
	}
}

// TestScore_CancelledContextStopsQueuedItems 验证「等信号量令牌那一步也响应取消」。
//
// 这条最初写成"取消后整批及时返回"，但那个断言**验不到东西**：假件一取消就立刻出错，
// 于是有没有那个 select 都绿。删掉 ctx 分支跑一遍确认了这点——注释声称的保证并不存在。
// 一条声称覆盖了某路径、实际没覆盖的测试，比没有测试更糟：它会让后来的人不再去看那里。
//
// 改成用可观测量：**fn 到底被调用了多少次**。
// 构造 3 倍扇出的批次，假件调用后就阻塞（刻意不理会 ctx），把令牌全占满、其余全部堵在
// 信号量前；此时取消 ctx。
//   - 有 ctx 分支：排队的那些当场退出，fn 只被调用 parBatchFanout 次。
//   - 无 ctx 分支：它们会等到前面放锁后依次拿到令牌、继续调用 fn，最终调满整批。
func TestScore_CancelledContextStopsQueuedItems(t *testing.T) {
	const mult = 3
	items := itemsWithIDs(parBatchFanout * mult)
	release := make(chan struct{})
	sc := &ctrlScorer{block: release}
	a := newActivitiesWith(sc, fakeCardGen{})

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	var got []types.ScoredItem
	var err error
	go func() {
		got, err = a.Score(ctx, ScoreIn{UserID: 1, TraceID: "t", Items: items})
		close(done)
	}()

	// 等令牌被占满：此刻其余 goroutine 都堵在信号量前。
	deadline := time.After(3 * time.Second)
	for {
		if inflight, _ := sc.snapshot(); inflight >= parBatchFanout {
			break
		}
		select {
		case <-deadline:
			inflight, calls := sc.snapshot()
			t.Fatalf("等不到令牌占满：inflight=%d calls=%d", inflight, calls)
		default:
			time.Sleep(2 * time.Millisecond)
		}
	}

	cancel()
	// 给排队者一点时间响应取消，再放行在飞的那批。
	time.Sleep(50 * time.Millisecond)
	close(release)

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("取消后迟迟不返回")
	}

	_, calls := sc.snapshot()
	if calls > parBatchFanout {
		t.Errorf("取消后仍有排队项进入打分：fn 被调用 %d 次，最多应为 %d 次"+
			"——等令牌那一步没有响应取消，整批取消后仍在继续发起 LLM 请求", calls, parBatchFanout)
	}
	if err != nil && len(got) != 0 {
		t.Errorf("既报错又返回了 %d 条结果，语义自相矛盾", len(got))
	}
}

// TestScore_PanicSurfacesAsActivityPanicNotProcessCrash 钉住 panic 的爆炸半径。
//
// 串行版里 fn 的 panic 沿 activity 自己的栈上抛，由 Temporal SDK 接住转成可重试的
// activity 错误。改并发后若让 panic 在 spawn 出来的 goroutine 里飞，**Go 会直接终止
// 整个进程**——同一个空指针，改造前只是这批推送失败重试，改造后会把 vane 打挂。
//
// 本用例断言两件事：panic 仍然从 Score 调用点抛出（Temporal 接得住），
// 且携带原始 goroutine 栈（否则现场只剩重抛的那一行，排查时看不见挂在哪）。
func TestScore_PanicSurfacesAsActivityPanicNotProcessCrash(t *testing.T) {
	// 条数刻意取 3 倍扇出：必须有 goroutine 真的在等令牌，才能测到
	// 「panic 时令牌是否归还」。若不归还，等令牌的那些会永久阻塞、wg.Wait() 挂死，
	// 表现为 activity 卡到 120 秒超时——比崩溃更难查。
	// （defer 是 LIFO：归还令牌那个 defer 注册最晚、跑得最早，先于 recover 与 wg.Done。）
	sc := &panicScorer{panicOn: 3, delay: 5 * time.Millisecond}
	a := newActivitiesWith(sc, fakeCardGen{})

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("单条 panic 应从 Score 调用点重新抛出，让 Temporal 接住转成 activity 失败")
		}
		msg := fmt.Sprint(r)
		if !strings.Contains(msg, "打分炸了") {
			t.Errorf("重抛应保留原始 panic 值，实得: %s", msg)
		}
		if !strings.Contains(msg, "原始 goroutine 栈") {
			t.Errorf("重抛应带上原始栈，否则排查时看不见挂在哪，实得: %s", msg)
		}
	}()

	_, _ = a.Score(t.Context(), ScoreIn{UserID: 1, TraceID: "t", Items: itemsWithIDs(parBatchFanout * 3)})
}

type panicScorer struct {
	panicOn    int64
	panicOnAll bool
	delay      time.Duration
}

func (p *panicScorer) Score(_ context.Context, _ int64, item types.ContentItem, _ string) (float64, error) {
	if p.delay > 0 {
		time.Sleep(p.delay)
	}
	if p.panicOnAll || item.ID == p.panicOn {
		panic("打分炸了")
	}
	return 50, nil
}

// TestScore_PanicDoesNotLeakSemaphoreTokens 单独验"panic 时令牌归还"。
//
// 上一条用例（单条 panic）测不到这个：只漏 1 个令牌，剩下 15 个足够让其余条目跑完，
// 不会死锁——实测把归还从 defer 改成仅正常路径执行，那条用例照样绿。
// 要暴露泄漏必须让**在飞的那一批全部 panic**，把令牌一次漏光：此时后面排队的
// 永远拿不到令牌，wg.Wait() 永久阻塞，activity 卡到 120 秒超时（比崩溃更难查）。
func TestScore_PanicDoesNotLeakSemaphoreTokens(t *testing.T) {
	sc := &panicScorer{panicOnAll: true, delay: 2 * time.Millisecond}
	a := newActivitiesWith(sc, fakeCardGen{})

	done := make(chan struct{})
	go func() {
		defer close(done)
		defer func() { _ = recover() }()
		_, _ = a.Score(t.Context(), ScoreIn{UserID: 1, TraceID: "t", Items: itemsWithIDs(parBatchFanout * 3)})
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("整批 panic 后 Score 不返回——信号量令牌在 panic 路径上泄漏了，" +
			"排队中的 goroutine 永远拿不到令牌，wg.Wait() 永久阻塞")
	}
}
