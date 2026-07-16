// cmd/gate 是 Gate / CI 的一键探针入口：把 M5 契约 §16 的服务端探针跑一遍，
// 人话表格或 JSON 输出，红灯以退出码 1 阻断流水线。
//
// 为什么走 DB 直连（store.New）而不打 /api/admin/observability：
// post-deploy 在 VPS 上执行，本来就有库权限（与 vane.service 同宿主、同 VANE_DB_URL），
// 走 HTTP 要额外带 Dashboard 密码、还要 Caddy 与 HTTP server 都已就绪——
// 而"刚部署完服务还没起来"恰恰是探针最该说话的时刻，此时 HTTP 出口自己先挂了，
// 探针只会把"连不上"报成一片红，把真正要看的指标盖掉。少一层依赖少一处失败。
// 判定逻辑不因此分叉：两个出口共用同一个 probe.Run（见 probe 包头注释：
// 探针 SQL 依赖 scorer 源码里的字面量，一旦有第二份实现必然漂）。
//
// 用法：
//
//	gate                    # 24h 窗口，人话输出
//	gate -window 48h        # 契约 §16 要求部署当天与次日复跑，跨天时放宽窗口
//	gate -json              # JSON 输出（stdout 只有 JSON，可直接 | jq）
//	gate -user 1            # 显式指定 userID，跳过 owner 解析（全程零写入）
//
// 退出码：0 = 全绿或仅有黄；1 = 有红（按契约回滚排查）；2 = 工具自身没跑起来
// （配置 / 连库 / 查询失败）。2 与 1 刻意分开：红是产品坏了，2 是探针坏了，
// 两者的处置动作完全不同，混成一个码会让 CI 把"探针连不上库"误报成"线上炸了"。
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/YouToco/vane/config"
	"github.com/YouToco/vane/feishu"
	"github.com/YouToco/vane/probe"
	"github.com/YouToco/vane/store"
	"github.com/YouToco/vane/types"
)

// 退出码，见文件头注释。
const (
	exitOK      = 0
	exitRed     = 1
	exitFailure = 2
)

// runTimeout 是整轮探针的总预算：9 条只读聚合查询，正常几百毫秒完事。
// 设上限是为 CI 兜底——库卡住时 gate 应当明确失败退出，而不是把流水线挂到它自己的超时。
const runTimeout = 60 * time.Second

func main() {
	os.Exit(run())
}

func run() int {
	window := flag.Duration("window", probe.DefaultWindow, "统计窗口（如 24h、48h）；<=0 时回退默认 24h")
	asJSON := flag.Bool("json", false, "输出 JSON 而非人话表格（stdout 只含 JSON）")
	userID := flag.Int64("user", 0, "显式指定 userID，跳过 owner 解析；0 表示解析 owner")
	flag.Parse()

	// path 传空：与 cmd/server 同一套来源与优先级（./config.yaml →
	// /opt/vane/config/config.yaml → VANE_ 环境变量覆盖）。刻意不自己 os.Getenv：
	// gate 与 server 必须连同一个库，各读各的迟早漂成"探针在体检另一个环境"。
	cfg, err := config.Load("")
	if err != nil {
		// 原始 error 只进日志（红线 3）：config 的错误链带文件路径与 viper 原文。
		slog.Error("gate: 加载配置失败", "err", err)
		fmt.Fprintln(os.Stderr, "gate: 加载配置失败——请确认 VANE_DB_URL 已设置或 config.yaml 可读")
		return exitFailure
	}
	initLogger(cfg.Log.Level)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, runTimeout)
	defer cancel()

	// 刻意不调 store.Migrate（cmd/server 启动时已跑过）：本工具是只读探针，
	// 给"看指标"附带改 schema 的能力毫无收益——post-deploy 时 vane.service 早已迁移完毕。
	st, err := store.New(ctx, cfg.DB.URL)
	if err != nil {
		slog.Error("gate: 连接数据库失败", "err", err)
		fmt.Fprintln(os.Stderr, "gate: 连接数据库失败——请确认 Postgres 可达且连接串正确")
		return exitFailure
	}
	defer st.Close()

	uid := *userID
	if uid == 0 {
		if uid, err = resolveOwnerUserID(ctx, st); err != nil {
			slog.Error("gate: 解析 owner 失败", "err", err)
			fmt.Fprintln(os.Stderr, "gate: "+userMessage(err, "解析 owner 失败"))
			return exitFailure
		}
	}

	// now 在此注入且取 UTC：DB 是 UTC，探针内部一律 UTC，换算只在前端（红线 6）。
	// 由调用方给"现在"也保证一轮内所有查询共用同一时间原点（见 probe.Run 注释）。
	rep, err := probe.Run(ctx, st, uid, time.Now().UTC(), *window)
	if err != nil {
		slog.Error("gate: 探针执行失败", "err", err)
		fmt.Fprintln(os.Stderr, "gate: "+userMessage(err, "探针查询失败"))
		return exitFailure
	}

	if *asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(rep); err != nil {
			slog.Error("gate: 输出 JSON 失败", "err", err)
			fmt.Fprintln(os.Stderr, "gate: 输出 JSON 失败")
			return exitFailure
		}
	} else {
		printReport(rep)
	}

	// 只有红阻断。黄不 exit 1 是刻意的：黄的语义是"数据不足 / 不适用"而非失败
	// （见 probe.Status 注释）——刚部署完窗口内还没跑过定时批次，几乎必然满屏黄。
	// 让黄阻断等于把 gate 训练成"每次都挂，挂了就加 --force"，红灯从此也没人看了。
	// 但黄也绝不能悄悄算过（那正是 probe 要防的 vacuously green），所以人话输出末尾
	// 留一行汇总把黄的条数摆到脸上，逼人按契约 §16 次日复跑。
	if rep.Worst() == probe.StatusRed {
		return exitRed
	}
	return exitOK
}

// initLogger 与 cmd/server 同构，但输出到 stderr 而非 stdout：
// gate 的 stdout 是数据面（-json 要能直接 | jq），日志混进去会把 JSON 撑坏。
func initLogger(level string) {
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(level)); err != nil {
		lvl = slog.LevelInfo
	}
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: lvl})))
}

// printReport 打人话表格：每条探针一行，红 / 黄再缩进打 Detail。
// 绿灯的 Detail 多是"这条探针管不到什么"的免责说明（如 §16.7 的 tags 超集不可验证），
// 全打出来会把真正要看的红黄淹掉——要看全文用 -json。
func printReport(rep probe.Report) {
	fmt.Printf("见微 Vane Gate 探针 · 窗口 %dh · userID %d · %s UTC\n\n",
		rep.WindowHours, rep.UserID, rep.GeneratedAt.Format("2006-01-02 15:04:05"))

	yellow := 0
	for _, res := range rep.Results {
		fmt.Printf("[%s] %s (%s): %s\n",
			strings.ToUpper(string(res.Status)), res.Name, res.ContractRef, res.Summary)
		if res.Status != probe.StatusGreen && res.Detail != "" {
			fmt.Printf("    %s\n", res.Detail)
		}
		if res.Status == probe.StatusYellow {
			yellow++
		}
	}

	fmt.Println()
	switch {
	case rep.Worst() == probe.StatusRed:
		fmt.Println("结论：有红灯——按契约 §16 回滚排查。")
	case yellow > 0:
		fmt.Printf("结论：无红灯，但 %d 条为黄（数据不足 / 不适用），不算通过，需人工确认——"+
			"契约 §16 要求部署当天与次日各复跑一次。\n", yellow)
	default:
		fmt.Println("结论：全绿。")
	}
}

// ownerRecord 对应 settings.feishu_owner 的 value 结构。
// 与 api/owner.go 的同名类型一样是本地复述而非跨包导出：字段少且稳定，
// 真正的耦合点（key 名）已由 feishu.SettingKeyOwner 收口。
type ownerRecord struct {
	OpenID string `json:"open_id"`
	Name   string `json:"name"`
}

// resolveOwnerUserID 把 settings.feishu_owner 解析成 users 表主键，复述 api.ownerUserID
// 的逻辑（那是 api 包的未导出方法，cmd 拿不到）。
//
// 取舍——本工具是只读探针，这里却写了一次库：store 目前没有"按 open_id 纯读用户"的方法，
// store/users.go 只有 UpsertUserByOpenID。评估后仍然用它：
//  1. 恒为幂等命中，不改数据：owner 记录只在 owner 给机器人发过消息之后才存在
//     （feishu/handler.go:497 捕获），而那条消息路径先做过 UpsertUserByOpenID
//     （handler.go:137）——user 行必已存在。这次 upsert 不新增行、不动 created_at，
//     只 SET name = EXCLUDED.name。
//  2. 不引入新的写入形状：与 api.ownerUserID 逐字一致，Dashboard 每次建调度都在做同一件事，
//     风险面等同既有调用，而非 gate 新造的。
//
// 已知瑕疵（同样继承自 api/owner.go，不是本工具引入）：settings.feishu_owner 只在首次捕获时
// 写入（handler.go:480 已捕获即返回），users.name 却每条消息刷新——owner 事后改昵称会让两者漂移，
// 此时这次 upsert 会把 users.name 写回捕获时的旧昵称。只影响展示字段，不碰任何探针指标。
// 要彻底零写入就用 -user 显式指定 userID。若日后 store 补上只读的按 open_id 查询，此处应立刻换过去。
func resolveOwnerUserID(ctx context.Context, st *store.Store) (int64, error) {
	raw, err := st.GetSetting(ctx, feishu.SettingKeyOwner)
	if err != nil {
		// 尚无 owner 是"流程未走到"而非故障，给出下一步动作而不是报错文案。
		if errors.Is(err, types.ErrNotFound) {
			return 0, types.NewAppError(types.CodeConflict,
				"尚未捕获 owner，请先在飞书给机器人发一条消息，或用 -user 显式指定 userID", nil)
		}
		return 0, err
	}
	var rec ownerRecord
	if err := json.Unmarshal(raw, &rec); err != nil {
		return 0, types.NewAppError(types.CodeInternal, "owner 设置格式异常", err)
	}
	if rec.OpenID == "" {
		return 0, types.NewAppError(types.CodeConflict, "owner 记录缺少 open_id，请重新完成飞书向导", nil)
	}
	// 传 rec.Name（非空串）：与 api.ownerUserID 一致，避免把已有昵称覆盖成空。
	u, err := st.UpsertUserByOpenID(ctx, rec.OpenID, rec.Name)
	if err != nil {
		return 0, err
	}
	return u.ID, nil
}

// userMessage 从错误链里取 AppError.Message 作为给人看的文案（红线 3）：
// AppError.Error() 会把 Cause 原文（可能含连接串、pgx 服务端原文）一并拼进去，
// 直接打到 stderr 就是把连接串印在 CI 日志里。链上没有 AppError 时回退兜底话术，
// 原始 error 一律只走 slog。
func userMessage(err error, fallback string) string {
	var ae *types.AppError
	if errors.As(err, &ae) {
		return ae.Message
	}
	return fallback
}
