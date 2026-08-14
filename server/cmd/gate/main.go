// cmd/gate 是 Gate 探针的命令行入口：把 M5 契约 §16 的服务端探针跑一遍，
// 人话表格或 JSON 输出。定位是**部署验证与红灯后的人工深查工具**——日常告警
// 由服务内 probewatch 承担（每日 01:30 UTC + 每次启动后 3 分钟自动跑，红灯发
// 飞书卡），本工具是人主动来查时用的。
//
// 退出码 1 供脚本判红。deploy CI 的 post-deploy 步骤（ci.yml「Gate probes」）会在
// VPS 上执行本工具（Boss 2026-07-20 拍板接线）：红灯（exit 1）与探针自身失败
// （exit 2）都会把 deploy job 打红——服务此刻已在跑新代码（无自动回滚），流水线红
// 是"部署已生效但体检不过"的强信号，与 probewatch 的飞书告警卡双通道。
//
// 为什么走 DB 直连（store.NewServerRuntime）而不打 /api/admin/observability：
// post-deploy 在 VPS 上执行，本来就有库权限（与 vane.service 同宿主、同 VANE_DB_URL），
// 走 HTTP 要额外带 Dashboard 密码、还要 Caddy 与 HTTP server 都已就绪——
// 而"刚部署完服务还没起来"恰恰是探针最该说话的时刻，此时 HTTP 出口自己先挂了，
// 探针只会把"连不上"报成一片红，把真正要看的指标盖掉。少一层依赖少一处失败。
// 判定逻辑不因此分叉：两个出口共用同一个 probe.Run（见 probe 包头注释：
// 探针 SQL 依赖 scorer 源码里的字面量，一旦有第二份实现必然漂）。
//
// 用法：
//
//	gate                            # 24h 窗口，人话输出
//	gate -env /opt/vane/.env        # 先从 env 文件补齐环境变量（VPS 上手动跑的标配）
//	gate -window 48h                # 契约 §16 要求部署当天与次日复跑，跨天时放宽窗口
//	gate -json                      # JSON 输出（stdout 只有 JSON，可直接 | jq）
//	gate -user 1                    # 显式指定 userID，跳过 owner 解析（全程零写入）
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

	"github.com/YouToco/vane/server/auth"
	"github.com/YouToco/vane/server/config"
	"github.com/YouToco/vane/server/feishu"
	"github.com/YouToco/vane/server/probe"
	"github.com/YouToco/vane/server/store"
	"github.com/YouToco/vane/server/types"
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
	envFile := flag.String("env", "", "先从该 env 文件（KEY=VALUE）补齐缺失的环境变量，如 /opt/vane/.env")
	flag.Parse()

	if *envFile != "" {
		if err := loadEnvFile(*envFile); err != nil {
			// 路径是操作者刚敲的参数，不算敏感，直接说清哪里没读到。
			fmt.Fprintf(os.Stderr, "gate: 读取 env 文件失败（%s）：%v\n", *envFile, err)
			return exitFailure
		}
	}

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
	// Gate receives the same non-owner DSN as vane.service. Enter the exact
	// default vane_app capability and validate the runtime shell; store.New
	// would leave current_user at the inert NOINHERIT login and make every
	// business-table probe fail before the old worker is drained.
	st, err := store.NewServerRuntime(ctx, cfg.DB.URL)
	if err != nil {
		slog.Error("gate: 连接数据库失败", "err", err)
		fmt.Fprintln(os.Stderr, "gate: 连接数据库失败——请确认 Postgres 可达且连接串正确")
		return exitFailure
	}
	defer st.Close()
	if _, err := st.AssertAgentFirstLegacyWriteFence(ctx); err != nil {
		slog.Error("gate: Agent-first legacy 写入冻结验证失败", "err", err)
		fmt.Fprintln(os.Stderr, "gate: Agent-first legacy 写入冻结不完整")
		return exitFailure
	}

	var principal auth.Principal
	if *userID == 0 {
		if principal, err = resolveOwnerPrincipal(ctx, st); err != nil {
			slog.Error("gate: 解析 owner 失败", "err", err)
			fmt.Fprintln(os.Stderr, "gate: "+userMessage(err, "解析 owner 失败"))
			return exitFailure
		}
	} else if principal, err = resolveExplicitPrincipal(ctx, st, *userID); err != nil {
		slog.Error("gate: 解析显式用户租户失败", "err", err)
		fmt.Fprintln(os.Stderr, "gate: "+userMessage(err, "解析显式用户租户失败"))
		return exitFailure
	}

	// now 在此注入且取 UTC：DB 是 UTC，探针内部一律 UTC，换算只在前端（红线 6）。
	// 由调用方给"现在"也保证一轮内所有查询共用同一时间原点（见 probe.Run 注释）。
	rep, err := probe.Run(ctx, st, int64(principal.TenantID), principal.UserID,
		time.Now().UTC(), *window)
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

// loadEnvFile 把 KEY=VALUE 形式的 env 文件补进进程环境（已存在的变量不覆盖——
// 显式 export 的值优先于文件）。存在的意义：VPS 上手动跑 gate 时环境是裸 shell，
// 而 /opt/vane/.env 混有 CRLF（生产实锤），`source` 会把 `\r` 带进值里报
// 莫名其妙的连接错误，systemd 的 EnvironmentFile 容忍它、shell 不容忍——
// 本函数按 systemd 的宽松度解析：剥 CRLF、跳过注释与空行、剥一层对称引号。
func loadEnvFile(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(strings.TrimRight(line, "\r"))
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, val, ok := strings.Cut(line, "=")
		if !ok {
			continue // 与 systemd 一致：解析不出的行跳过，不因一行坏格式拒绝整个文件
		}
		key = strings.TrimSpace(key)
		val = strings.TrimSpace(val)
		if len(val) >= 2 && (val[0] == '"' || val[0] == '\'') && val[len(val)-1] == val[0] {
			val = val[1 : len(val)-1]
		}
		if key == "" {
			continue
		}
		if _, exists := os.LookupEnv(key); !exists {
			if err := os.Setenv(key, val); err != nil {
				return err
			}
		}
	}
	return nil
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

// resolveOwnerPrincipal 把当前 owner 解析成不可猜测的租户/用户二元组。
//
// 逻辑本体已收敛到 auth 包（企业级契约 §1.1，不变量 I-A1）——收敛前这里是
// api.ownerUserID 的第三份逐字副本（因为那是 api 包的未导出方法，cmd 拿不到）。
//
// 本函数保留的唯一职责：把「尚无 owner」的文案替换成 gate 专属版本（提示 -user 参数）。
// auth 包给的是面向 Dashboard 用户的通用文案，对命令行操作者要多给一条出路。
// 其余错误原样透传。
//
// 继承自 auth 包的已知取舍（不是本工具引入）：解析会写一次库（UpsertUserByOpenID），
// 但恒为幂等命中——owner 记录只在 owner 给机器人发过消息后才存在，而那条路径已建好
// user 行。要彻底零写入就用 -user 显式指定 userID。
func resolveOwnerPrincipal(ctx context.Context, st *store.Store) (auth.Principal, error) {
	p, err := auth.NewOwnerResolver(st, feishu.SettingKeyOwner).FromContext(ctx)
	if err != nil {
		var ae *types.AppError
		if errors.As(err, &ae) && ae.Code == types.CodeConflict {
			return auth.Principal{}, types.NewAppError(types.CodeConflict,
				"尚未捕获 owner，请先在飞书给机器人发一条消息，或用 -user 显式指定 userID", err)
		}
		return auth.Principal{}, err
	}
	return p, nil
}

// resolveExplicitPrincipal keeps -user useful without letting the probe infer
// a tenant from row visibility or list order. A user with zero or multiple
// memberships is not an exact authority and therefore fails closed.
func resolveExplicitPrincipal(ctx context.Context, st *store.Store, userID int64) (auth.Principal, error) {
	if userID <= 0 {
		return auth.Principal{}, types.NewAppError(types.CodeValidation, "userID 必须为正整数", nil)
	}
	memberships, err := st.ListMembershipsByUser(ctx, userID)
	if err != nil {
		return auth.Principal{}, err
	}
	return explicitPrincipalFromMemberships(userID, memberships)
}

func explicitPrincipalFromMemberships(userID int64, memberships []types.Membership) (auth.Principal, error) {
	if userID <= 0 {
		return auth.Principal{}, types.NewAppError(types.CodeValidation, "userID 必须为正整数", nil)
	}
	if len(memberships) == 0 {
		return auth.Principal{}, types.NewAppError(types.CodeNotFound, "显式用户没有可用租户 membership", nil)
	}
	if len(memberships) != 1 {
		return auth.Principal{}, types.NewAppError(types.CodeConflict,
			"显式用户属于多个租户，gate 拒绝猜测画像范围", nil)
	}
	return auth.Principal{
		TenantID: types.TenantID(memberships[0].TenantID),
		UserID:   userID,
	}, nil
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
