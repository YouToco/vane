package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/YouToco/vane/auth"
	"github.com/YouToco/vane/feishu"
	"github.com/YouToco/vane/probe"
	"github.com/YouToco/vane/pusher"
	"github.com/YouToco/vane/store"
	"github.com/YouToco/vane/types"
)

func TestNextDailyProbeAt(t *testing.T) {
	tests := []struct {
		name string
		now  time.Time
		want time.Time
	}{
		{
			name: "巡检时刻之前 → 当天",
			now:  time.Date(2026, 7, 19, 0, 15, 0, 0, time.UTC),
			want: time.Date(2026, 7, 19, 1, 30, 0, 0, time.UTC),
		},
		{
			name: "巡检时刻之后 → 次日",
			now:  time.Date(2026, 7, 19, 9, 0, 0, 0, time.UTC),
			want: time.Date(2026, 7, 20, 1, 30, 0, 0, time.UTC),
		},
		{
			name: "恰为巡检时刻 → 次日（本轮刚跑完，同刻不重跑）",
			now:  time.Date(2026, 7, 19, 1, 30, 0, 0, time.UTC),
			want: time.Date(2026, 7, 20, 1, 30, 0, 0, time.UTC),
		},
		{
			name: "跨月边界",
			now:  time.Date(2026, 7, 31, 23, 59, 0, 0, time.UTC),
			want: time.Date(2026, 8, 1, 1, 30, 0, 0, time.UTC),
		},
		{
			name: "跨年边界",
			now:  time.Date(2026, 12, 31, 23, 59, 0, 0, time.UTC),
			want: time.Date(2027, 1, 1, 1, 30, 0, 0, time.UTC),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := nextDailyProbeAt(tt.now); !got.Equal(tt.want) {
				t.Fatalf("nextDailyProbeAt(%v) = %v, want %v", tt.now, got, tt.want)
			}
		})
	}
}

// fakeProbeStore 实现 probe.Store。零值行为让全部探针落绿或黄（无从判定）；
// quality 可注入制造 §16.3 红灯（EmptyNoError>0 是最短的致红路径），
// failListTraces 可注入让 probe.Run 整体报错（第一条查询即失败）。
type fakeProbeStore struct {
	quality        types.ScoreQualityStat
	failListTraces error
}

func (f *fakeProbeStore) ListScoreTraceStats(context.Context, time.Time, int) ([]types.ScoreTraceStat, error) {
	return nil, f.failListTraces
}
func (f *fakeProbeStore) GetScoreQualityStat(context.Context, time.Time) (types.ScoreQualityStat, error) {
	return f.quality, nil
}
func (f *fakeProbeStore) ListScoreDistribution(context.Context, time.Time) ([]types.ScoreBucket, error) {
	return nil, nil
}
func (f *fakeProbeStore) GetScoreLivenessStat(context.Context, time.Time, int) (types.ScoreLivenessStat, error) {
	return types.ScoreLivenessStat{}, nil
}
func (f *fakeProbeStore) GetProfileInjectionStat(context.Context, time.Time) (types.ProfileInjectionStat, error) {
	return types.ProfileInjectionStat{}, nil
}
func (f *fakeProbeStore) GetNegTailStat(context.Context, time.Time, string) (types.NegTailStat, error) {
	return types.NegTailStat{}, nil
}
func (f *fakeProbeStore) ListSpanDayCosts(context.Context, time.Time) ([]types.SpanDayCost, error) {
	return nil, nil
}
func (f *fakeProbeStore) ListModelUsage(context.Context, time.Time) ([]types.ModelUsage, error) {
	return nil, nil
}
func (f *fakeProbeStore) GetEvolveCallStat(context.Context, int64, time.Time) (types.EvolveCallStat, error) {
	return types.EvolveCallStat{}, nil
}
func (f *fakeProbeStore) ListPushBatchSummaries(context.Context, int64, time.Time, int) ([]types.PushBatchSummary, error) {
	return nil, nil
}
func (f *fakeProbeStore) GetProfile(context.Context, int64) (*types.Profile, error) {
	return nil, types.ErrNotFound
}
func (f *fakeProbeStore) CountA2ATasks(context.Context) (int64, error) { return 0, nil }

type fakePrincipal struct{ err error }

func (f *fakePrincipal) FromContext(context.Context) (auth.Principal, error) {
	if f.err != nil {
		return auth.Principal{}, f.err
	}
	return auth.Principal{UserID: 1}, nil
}

type fakeOwner struct{ openID string }

func (f *fakeOwner) OwnerOpenID() string { return f.openID }

// fakeFPStore 是 fingerprintStore 的替身：fp 模拟落盘值（migration 027 的单行表），
// getErr/setErr 注入读写失败验证 best-effort 降级。
type fakeFPStore struct {
	fp       string
	getErr   error
	setErr   error
	setCalls []string // 记录每次写入，断言复位（""）与成功指纹都真的落了盘
}

func (f *fakeFPStore) GetProbewatchFingerprint(context.Context) (string, error) {
	if f.getErr != nil {
		return "", f.getErr
	}
	return f.fp, nil
}

func (f *fakeFPStore) SetProbewatchFingerprint(_ context.Context, fp string) error {
	f.setCalls = append(f.setCalls, fp)
	if f.setErr != nil {
		return f.setErr
	}
	f.fp = fp
	return nil
}

type fakePusher struct {
	cards []string
	err   error
}

func (f *fakePusher) Push(_ context.Context, _, cardJSON string) (string, error) {
	if f.err != nil {
		return "", f.err
	}
	f.cards = append(f.cards, cardJSON)
	return "msg-id", nil
}

// redQuality 让 §16.3（空输出零容忍）落红：10 次成功调用里 1 次空 completion 无报错。
func redQuality() types.ScoreQualityStat {
	return types.ScoreQualityStat{OKTotal: 10, EmptyNoError: 1}
}

func newTestWatcher(st probe.Store, pr principalSource, owner ownerOpenIDProvider, push cardPusher) *probeWatcher {
	return newProbeWatcher(st, &fakeFPStore{}, pr, owner, push, func(md string) string { return md })
}

// newTestWatcherFP 同上但共享外部指纹盘——模拟重启（新 watcher、旧盘）的用例用它。
func newTestWatcherFP(st probe.Store, fps fingerprintStore, push cardPusher) *probeWatcher {
	return newProbeWatcher(st, fps, &fakePrincipal{}, &fakeOwner{openID: "ou_x"}, push,
		func(md string) string { return md })
}

func TestRunOnceRedAlertsOnceUntilChange(t *testing.T) {
	st := &fakeProbeStore{quality: redQuality()}
	push := &fakePusher{}
	pw := newTestWatcher(st, &fakePrincipal{}, &fakeOwner{openID: "ou_x"}, push)

	pw.runOnce(context.Background())
	if len(push.cards) != 1 {
		t.Fatalf("首轮红灯应发 1 张卡，实发 %d", len(push.cards))
	}
	if !strings.Contains(push.cards[0], "空输出零容忍") || !strings.Contains(push.cards[0], "§16.3") {
		t.Fatalf("告警卡应含红灯探针名与契约号，实际：%s", push.cards[0])
	}

	// 红灯集合未变：本进程内不重发。
	pw.runOnce(context.Background())
	if len(push.cards) != 1 {
		t.Fatalf("同一红灯集合不应重发，实发 %d 张", len(push.cards))
	}

	// 红→绿：指纹复位；再红：重新告警。
	st.quality = types.ScoreQualityStat{OKTotal: 10}
	pw.runOnce(context.Background())
	if len(push.cards) != 1 {
		t.Fatalf("转绿不应发卡，实发 %d 张", len(push.cards))
	}
	if pw.lastFingerprint != "" {
		t.Fatalf("转绿后指纹应复位，实际 %q", pw.lastFingerprint)
	}
	st.quality = redQuality()
	pw.runOnce(context.Background())
	if len(push.cards) != 2 {
		t.Fatalf("红→绿→又红应再次告警，实发 %d 张", len(push.cards))
	}
}

func TestRunOnceGreenDoesNotAlert(t *testing.T) {
	push := &fakePusher{}
	pw := newTestWatcher(&fakeProbeStore{quality: types.ScoreQualityStat{OKTotal: 10}},
		&fakePrincipal{}, &fakeOwner{openID: "ou_x"}, push)
	pw.runOnce(context.Background())
	if len(push.cards) != 0 {
		t.Fatalf("无红灯不应发卡，实发 %d 张", len(push.cards))
	}
}

func TestRunOnceProbeFailureAlertsOnce(t *testing.T) {
	st := &fakeProbeStore{failListTraces: errors.New("db down")}
	push := &fakePusher{}
	pw := newTestWatcher(st, &fakePrincipal{}, &fakeOwner{openID: "ou_x"}, push)

	pw.runOnce(context.Background())
	pw.runOnce(context.Background())
	if len(push.cards) != 1 {
		t.Fatalf("持续的探针故障应只告警一次，实发 %d 张", len(push.cards))
	}
	if !strings.Contains(push.cards[0], "探针自身执行失败") {
		t.Fatalf("应为探针自身失败卡，实际：%s", push.cards[0])
	}
	// 错误原文不得进卡（红线 3）。
	if strings.Contains(push.cards[0], "db down") {
		t.Fatalf("原始错误文本泄漏进告警卡：%s", push.cards[0])
	}

	// 故障恢复但出现红灯：指纹不同，应告警。
	st.failListTraces = nil
	st.quality = redQuality()
	pw.runOnce(context.Background())
	if len(push.cards) != 2 {
		t.Fatalf("故障恢复后的红灯是新指纹，应告警，实发 %d 张", len(push.cards))
	}
}

func TestRunOnceOwnerResolveFailureAlerts(t *testing.T) {
	push := &fakePusher{}
	pw := newTestWatcher(&fakeProbeStore{}, &fakePrincipal{err: errors.New("resolve boom")},
		&fakeOwner{openID: "ou_x"}, push)
	pw.runOnce(context.Background())
	if len(push.cards) != 1 {
		t.Fatalf("owner userID 解析失败应发探针失败卡，实发 %d", len(push.cards))
	}
	if strings.Contains(push.cards[0], "resolve boom") {
		t.Fatalf("原始错误文本泄漏进告警卡：%s", push.cards[0])
	}
}

func TestRunOnceNoOwnerSkips(t *testing.T) {
	push := &fakePusher{}
	pw := newTestWatcher(&fakeProbeStore{quality: redQuality()}, &fakePrincipal{},
		&fakeOwner{openID: ""}, push)
	pw.runOnce(context.Background())
	if len(push.cards) != 0 {
		t.Fatalf("无 owner 时应静默跳过，实发 %d 张", len(push.cards))
	}
	if pw.lastFingerprint != "" {
		t.Fatalf("无 owner 跳过不应动指纹，实际 %q", pw.lastFingerprint)
	}
}

func TestRunOncePushFailureRetriesNextRound(t *testing.T) {
	st := &fakeProbeStore{quality: redQuality()}
	push := &fakePusher{err: errors.New("feishu 5xx")}
	pw := newTestWatcher(st, &fakePrincipal{}, &fakeOwner{openID: "ou_x"}, push)

	pw.runOnce(context.Background())
	if pw.lastFingerprint != "" {
		t.Fatalf("发送失败不应记指纹（否则红灯从此静默），实际 %q", pw.lastFingerprint)
	}

	push.err = nil
	pw.runOnce(context.Background())
	if len(push.cards) != 1 {
		t.Fatalf("发送恢复后应补发，实发 %d 张", len(push.cards))
	}
	if pw.lastFingerprint == "" {
		t.Fatal("补发成功后应记录指纹")
	}
}

// ---------- 指纹落盘（migration 027，探针实现债 P2） ----------

// 落盘的核心承诺：同一红灯集合跨重启不重发。2026-07-19 一天 6 次部署对同一
// §16.1 红灯重发 5 张卡——修的就是这个。用两个 watcher 共享同一块"盘"模拟重启。
func TestFingerprintPersistsAcrossRestart(t *testing.T) {
	fps := &fakeFPStore{}
	push := &fakePusher{}
	pw1 := newTestWatcherFP(&fakeProbeStore{quality: redQuality()}, fps, push)
	pw1.runOnce(context.Background())
	if len(push.cards) != 1 {
		t.Fatalf("首轮红灯应发 1 张卡，实发 %d", len(push.cards))
	}
	if fps.fp == "" {
		t.Fatal("发送成功后指纹应已落盘")
	}

	// "重启"：新 watcher（进程内存清零），同一块盘、同一红灯集合。
	pw2 := newTestWatcherFP(&fakeProbeStore{quality: redQuality()}, fps, push)
	pw2.runOnce(context.Background())
	if len(push.cards) != 1 {
		t.Fatalf("重启后同一红灯集合不应重发（落盘去重），实发 %d 张", len(push.cards))
	}

	// 但红灯集合变化时（新故障）照发——「部署后复跑」的告警语义保留。
	pw3 := newTestWatcherFP(&fakeProbeStore{failListTraces: errors.New("db down")}, fps, push)
	pw3.runOnce(context.Background())
	if len(push.cards) != 2 {
		t.Fatalf("重启后新故障指纹不同，应告警，实发 %d 张", len(push.cards))
	}
}

// 转绿必须把复位（空串）也写穿到盘上：只清内存的话，重启后盘上还是旧红灯指纹，
// "红→绿→重启→又红（同一条）"会被误去重成静默。
func TestFingerprintResetPersisted(t *testing.T) {
	fps := &fakeFPStore{}
	push := &fakePusher{}
	st := &fakeProbeStore{quality: redQuality()}
	pw := newTestWatcherFP(st, fps, push)
	pw.runOnce(context.Background())
	if fps.fp == "" {
		t.Fatal("前置：红灯指纹应已落盘")
	}

	st.quality = types.ScoreQualityStat{OKTotal: 10}
	pw.runOnce(context.Background())
	if fps.fp != "" {
		t.Fatalf("转绿后盘上指纹应复位为空串，实际 %q", fps.fp)
	}
	if len(fps.setCalls) != 2 || fps.setCalls[1] != "" {
		t.Fatalf("复位应真的写盘（第二次 Set 为空串），实际写入序列 %v", fps.setCalls)
	}
}

// 读盘失败按"没告警过"降级：宁可对现存红灯多发一张，不能让指纹读不到挡住告警。
func TestFingerprintLoadFailureFailsOpen(t *testing.T) {
	fps := &fakeFPStore{fp: "red:empty_completion", getErr: errors.New("db flaky")}
	push := &fakePusher{}
	pw := newTestWatcherFP(&fakeProbeStore{quality: redQuality()}, fps, push)
	pw.runOnce(context.Background())
	if len(push.cards) != 1 {
		t.Fatalf("读盘失败应按空指纹继续并发卡，实发 %d 张", len(push.cards))
	}
}

// 写盘失败不影响本轮：内存指纹仍生效（本进程内不重发），代价只是重启后可能多发一张。
func TestFingerprintSaveFailureDoesNotBlock(t *testing.T) {
	fps := &fakeFPStore{setErr: errors.New("disk full")}
	push := &fakePusher{}
	pw := newTestWatcherFP(&fakeProbeStore{quality: redQuality()}, fps, push)
	pw.runOnce(context.Background())
	if len(push.cards) != 1 {
		t.Fatalf("写盘失败不应挡发卡，实发 %d 张", len(push.cards))
	}
	if pw.lastFingerprint == "" {
		t.Fatal("写盘失败时内存指纹仍应更新（本进程内去重不受影响）")
	}
	pw.runOnce(context.Background())
	if len(push.cards) != 1 {
		t.Fatalf("内存指纹仍应挡住本进程内重发，实发 %d 张", len(push.cards))
	}
}

// 编译期钉死生产装配的接口匹配（零运行时成本）：*store.Store 满足 probe.Store
// 与 fingerprintStore，*feishu.Manager / *pusher.Pusher / auth resolver 满足本文件
// 的窄接口。装配点在 main.go 的 go 语句里，不匹配要到改 main 时才炸；这里让测试编译先炸。
var (
	_ probe.Store         = (*store.Store)(nil)
	_ fingerprintStore    = (*store.Store)(nil)
	_ ownerOpenIDProvider = (*feishu.Manager)(nil)
	_ cardPusher          = (*pusher.Pusher)(nil)
)
