// probewatch：gate 探针的服务内每日巡检（2026-07-19 Boss 拍板"做进服务内"）。
//
// 为什么存在：probe 包的 8 条判定此前只有两个"要人主动来"的出口——管理后台看板
// 与 cmd/gate CLI。2026-07-19 的 §16.1 RED 在后台挂了半天无人知晓，靠人工 SSH 跑
// gate 才发现。本文件让红灯主动找 owner：每日定时 + 每次启动各跑一轮 probe.Run，
// 有红（或探针自身跑不动）就给 owner 发一张飞书告警卡。
//
// 为什么是进程内 goroutine 而非 Temporal 调度：与 runSessionCleanup 同一先例——
// 只读旁路巡检，丢一轮无害（次日自动再跑），不值得为它引入 workflow/activity/
// 调度管理三件套；且系统调度混进用户调度的 Temporal 命名空间反而要再造"系统 vs
// 用户"的区分。探针判定本身仍是唯一实现 probe.Run（见 probe 包头注释），本文件
// 只是第三个出口。
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/YouToco/vane/auth"
	"github.com/YouToco/vane/probe"
	"github.com/YouToco/vane/types"
)

const (
	// probeDailyHourUTC / probeDailyMinuteUTC：每日巡检时刻 01:30 UTC（北京 09:30）。
	// 选它是因为早报调度在 00:30 UTC（北京 08:30）——巡检窗口 24h 恰好覆盖刚跑完的
	// 早报批次，且 owner 在北京时间早上醒着，告警卡发出去当时就能被看到。
	probeDailyHourUTC   = 1
	probeDailyMinuteUTC = 30
	// probeStartupDelay：启动后首轮巡检的延迟。契约 §16 要求"部署后当天跑探针"，
	// 启动即巡检正是它的自动化；延迟几分钟等 WS 连接建立、owner 缓存加载完毕，
	// 也避开与迁移/reconcile 抢启动窗口。
	probeStartupDelay = 3 * time.Minute
	// probeRunTimeout：单轮巡检总预算，与 cmd/gate 的 runTimeout 同值同理由——
	// 9 条只读聚合正常几百毫秒，库卡死时该明确放弃本轮而不是挂住 goroutine。
	probeRunTimeout = 60 * time.Second
	// probeFailureFingerprint：探针自身执行失败的告警指纹。刻意不含错误文本——
	// 错误串常带时间戳/重试计数，掺进指纹会让同一故障每轮都算"新告警"刷屏。
	probeFailureFingerprint = "probe-failure"
)

// principalSource 是 owner userID 的解析面（生产实现 auth.NewOwnerResolver 的返回值）。
// 后台 goroutine 没有 HTTP 会话，与 cmd/gate 同走 owner 回退路径。
type principalSource interface {
	FromContext(ctx context.Context) (auth.Principal, error)
}

// ownerOpenIDProvider 暴露告警收件人（生产实现 *feishu.Manager）。
type ownerOpenIDProvider interface {
	OwnerOpenID() string
}

// cardPusher 主动发一张卡（生产实现 *pusher.Pusher）。
type cardPusher interface {
	Push(ctx context.Context, ownerOpenID, cardJSON string) (string, error)
}

// fingerprintStore 是告警指纹的持久化面（生产实现 *store.Store，migration 027）。
// 与 probe.Store 分开注入而非合并成大接口：巡检的"读指标"与"记告警状态"是两个
// 独立职责，测试时经常只想替换其中一个。
type fingerprintStore interface {
	GetProbewatchFingerprint(ctx context.Context) (string, error)
	SetProbewatchFingerprint(ctx context.Context, fp string) error
}

// probeWatcher 持有巡检所需的窄依赖。字段全部接口/函数，便于单测注入替身
// （与 workflow.Activities 的依赖收窄同一约定）。
type probeWatcher struct {
	st        probe.Store
	fps       fingerprintStore
	principal principalSource
	owner     ownerOpenIDProvider
	push      cardPusher
	buildCard func(markdown string) string

	// lastFingerprint 是告警去重指纹：相同红灯集合（或持续的探针故障）只在首次
	// 出现时发卡，恢复（非红）时清空，让"红→绿→又红"再次告警。
	//
	// 持久化语义（2026-07-20 修订，探针实现债 P2）：发送成功/复位时写库，首轮
	// 巡检前从库惰性加载——同一红灯集合跨重启不再重发（修订前指纹只在进程内存，
	// 红灯存续期间每次部署重启都重发一张同内容卡，2026-07-19 一天 6 次部署
	// 5 张同卡的生产实锤）。「部署后复跑」保留：启动后 3 分钟那轮照跑，红灯集合
	// **变化**时（新红灯出现、红灯换了一批）照发。读写都是 best-effort：
	// 读失败按空串处理（宁可多发一张也不漏发），写失败只记日志（下轮至多重发一张）。
	lastFingerprint string
	// fpLoaded 标记 lastFingerprint 是否已从库加载过。单 goroutine 顺序消费，无需锁。
	fpLoaded bool
}

func newProbeWatcher(st probe.Store, fps fingerprintStore, principal principalSource,
	owner ownerOpenIDProvider, push cardPusher, buildCard func(string) string) *probeWatcher {
	return &probeWatcher{st: st, fps: fps, principal: principal, owner: owner, push: push, buildCard: buildCard}
}

// run 阻塞循环：启动延迟后跑首轮，此后每天 probeDailyHourUTC:probeDailyMinuteUTC 跑一轮，
// 直到 ctx 取消。所有失败只 slog 不退出——巡检把进程带崩比不巡检更坏（同 runSessionCleanup）。
func (pw *probeWatcher) run(ctx context.Context) {
	timer := time.NewTimer(probeStartupDelay)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			pw.runOnce(ctx)
			timer.Reset(time.Until(nextDailyProbeAt(time.Now().UTC())))
		}
	}
}

// nextDailyProbeAt 返回 now 之后最近的每日巡检时刻（UTC）。纯函数供单测钉死边界：
// now 恰为巡检时刻时返回明天——本轮刚跑完，同刻再跑一次没有意义。
func nextDailyProbeAt(now time.Time) time.Time {
	next := time.Date(now.Year(), now.Month(), now.Day(),
		probeDailyHourUTC, probeDailyMinuteUTC, 0, 0, time.UTC)
	if !next.After(now) {
		next = next.Add(24 * time.Hour)
	}
	return next
}

// runOnce 执行一轮巡检并按需告警。
//
// 告警条件与去重（进程内变化沿，见 lastFingerprint 注释）：
//   - probe.Run 报错、或 owner userID 解析失败 → "探针自身失败"卡（固定指纹，
//     持续故障只发一次）——探针跑不动时线上健康是盲区，比红灯更危险；
//   - Worst 为红 → 红灯卡（指纹 = 红灯 ID 集合，集合不变不重发）；
//   - 非红 → 清空指纹，只记日志。黄不告警：黄的语义是"数据不足/需人工确认"
//     （probe.Status 注释），部署当天几乎必然有黄，按黄告警会把卡训练成噪声。
//
// owner 未捕获（OwnerOpenID 为空）时整轮静默跳过：没有收件人，卡发给谁都不对——
// 与 5.2 抓取告警对无 owner 的处理一致。
func (pw *probeWatcher) runOnce(ctx context.Context) {
	rctx, cancel := context.WithTimeout(ctx, probeRunTimeout)
	defer cancel()

	pw.loadFingerprintOnce(rctx)

	openID := pw.owner.OwnerOpenID()
	if openID == "" {
		slog.Info("probewatch: 尚未捕获 owner，本轮巡检跳过（飞书向导完成后自动恢复）")
		return
	}

	p, err := pw.principal.FromContext(rctx)
	if err != nil {
		slog.Error("probewatch: 解析 owner userID 失败", "err", err)
		pw.alert(ctx, openID, probeFailureFingerprint, renderProbeFailure(err))
		return
	}

	rep, err := probe.Run(rctx, pw.st, p.UserID, time.Now().UTC(), probe.DefaultWindow)
	if err != nil {
		slog.Error("probewatch: 探针执行失败", "err", err)
		pw.alert(ctx, openID, probeFailureFingerprint, renderProbeFailure(err))
		return
	}

	var reds []probe.Result
	for _, res := range rep.Results {
		if res.Status == probe.StatusRed {
			reds = append(reds, res)
		}
	}
	if len(reds) == 0 {
		if pw.lastFingerprint != "" {
			slog.Info("probewatch: 红灯已清除，告警指纹复位", "worst", rep.Worst())
			pw.storeFingerprint(ctx, "")
		}
		pw.lastFingerprint = ""
		slog.Info("probewatch: 巡检完成，无红灯", "worst", rep.Worst())
		return
	}

	ids := make([]string, len(reds))
	for i, r := range reds {
		ids[i] = r.ID
	}
	fp := "red:" + strings.Join(ids, ",")
	if fp == pw.lastFingerprint {
		slog.Info("probewatch: 红灯集合未变化，本进程内已告警过，不重发", "reds", ids)
		return
	}
	pw.alert(ctx, openID, fp, renderProbeAlert(reds))
}

// alert 发卡并在成功后记录指纹；发送失败不记指纹，下一轮（次日或下次部署）重试再发。
// 用外层 ctx 而非 runOnce 的 rctx，是不让 probeRunTimeout 的剩余预算截断发送
// （查询吃掉 55s 时只剩 5s 发卡）——不是送达保证：关停时外层 ctx 同样取消，
// 本轮告警丢弃，与"丢一轮无害"的整体取舍一致（下次启动的首轮会重新告警）。
func (pw *probeWatcher) alert(ctx context.Context, openID, fingerprint, markdown string) {
	if fingerprint == pw.lastFingerprint {
		slog.Info("probewatch: 同一故障本进程内已告警过，不重发")
		return
	}
	if _, err := pw.push.Push(ctx, openID, pw.buildCard(markdown)); err != nil {
		slog.Warn("probewatch: 告警卡发送失败（下轮重试）", "err", err)
		return
	}
	// 成功也要留痕（2026-07-19 部署验证实证）：此前成功路径零日志，"发出去了"与
	// "goroutine 压根没跑"在 journalctl 里不可区分，验证部署只能反向排除法 +
	// 去飞书搜卡。巡检自身必须可观测——这行日志就是首轮巡检的存在证明。
	slog.Info("probewatch: 告警卡已发送", "fingerprint", fingerprint)
	pw.lastFingerprint = fingerprint
	pw.storeFingerprint(ctx, fingerprint)
}

// loadFingerprintOnce 首轮巡检前从库加载指纹（进程生命周期内只加载一次）。
// 读失败按空串继续：空串语义是"没告警过"，最坏结果是对现存红灯多发一张卡——
// 与漏发相比是安全的方向；且此时库多半有更大的问题，探针查询自己会报出来。
func (pw *probeWatcher) loadFingerprintOnce(ctx context.Context) {
	if pw.fpLoaded {
		return
	}
	pw.fpLoaded = true
	fp, err := pw.fps.GetProbewatchFingerprint(ctx)
	if err != nil {
		slog.Warn("probewatch: 读取落盘指纹失败，按未告警过处理", "err", err)
		return
	}
	pw.lastFingerprint = fp
}

// storeFingerprint 把指纹写库（best-effort）。写失败只记日志不影响本轮结果：
// 内存里的指纹仍然有效，代价只是下次重启后可能对同一红灯多发一张卡。
func (pw *probeWatcher) storeFingerprint(ctx context.Context, fp string) {
	if err := pw.fps.SetProbewatchFingerprint(ctx, fp); err != nil {
		slog.Warn("probewatch: 落盘告警指纹失败（重启后可能重发一张卡）", "err", err)
	}
}

// renderProbeAlert 渲染红灯告警卡正文。只放 Summary 不放 Detail：Detail 是排查手册
// （常含整段 SQL），塞进卡里会把"哪几条红了"淹掉——完整报告在管理后台可观测页。
func renderProbeAlert(reds []probe.Result) string {
	var b strings.Builder
	fmt.Fprintf(&b, "**🔴 Gate 探针巡检：%d 条红灯**\n", len(reds))
	for _, r := range reds {
		fmt.Fprintf(&b, "\n**%s**（%s）\n%s\n", r.Name, r.ContractRef, r.Summary)
	}
	b.WriteString("\n红灯按 M5 契约 §16 处置；完整报告见管理后台可观测页，" +
		"或 SSH 跑 /opt/vane/bin/gate -env /opt/vane/.env。")
	return b.String()
}

// renderProbeFailure 渲染"探针自身失败"卡正文。错误原文不进卡（红线 3：store 错误
// 链可能携带连接串等敏感原文，卡片是对外出口）——只透出 AppError.Message 这类
// 已经面向人的文案，原始错误由调用方 slog 落服务日志。
func renderProbeFailure(err error) string {
	msg := "内部错误（详情见服务日志）"
	var ae *types.AppError
	if errors.As(err, &ae) {
		msg = ae.Message
	}
	return "**🔴 Gate 探针自身执行失败**\n\n" +
		"探针跑不动比红灯更危险：当前无法判定线上健康，请尽快排查。\n" +
		"错误概要：" + msg + "\n" +
		"原始错误已写入服务日志：journalctl -u vane | grep probewatch"
}
