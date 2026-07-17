// vane server 入口：加载配置 → 初始化日志 → 自动迁移 → 建库连接 →
// LLM 客户端/记账 → 飞书 Manager → HTTP 服务（healthz/readyz + Dashboard API）→ 优雅关停。
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"github.com/YouToco/vane/a2a"
	"github.com/YouToco/vane/agent"
	"github.com/YouToco/vane/api"
	"github.com/YouToco/vane/cardgen"
	"github.com/YouToco/vane/config"
	"github.com/YouToco/vane/evolver"
	"github.com/YouToco/vane/feedback"
	"github.com/YouToco/vane/feishu"
	"github.com/YouToco/vane/fetcher"
	"github.com/YouToco/vane/llm"
	"github.com/YouToco/vane/profilehint"
	"github.com/YouToco/vane/pusher"
	"github.com/YouToco/vane/scheduler"
	"github.com/YouToco/vane/scorer"
	"github.com/YouToco/vane/store"
	"github.com/YouToco/vane/workflow"
)

// vaneVersion 是服务版本串（进 A2A AgentCard.version，a2a-contract §7）。
// 值 = CHANGELOG 最上方已发布版本号，随发版手动同步；不为此新增 ldflags 基建。
const vaneVersion = "0.5.0"

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "vane: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	// 顶层 ctx：SIGINT/SIGTERM 时取消，触发优雅关停。
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// path 传空：按 ./config.yaml → /opt/vane/config/config.yaml 自动探测，缺失则纯环境变量运行。
	cfg, err := config.Load("")
	if err != nil {
		return fmt.Errorf("加载配置: %w", err)
	}

	initLogger(cfg.Log.Level)

	// MVP 决策：启动时自动执行数据库迁移，失败则拒绝启动。
	// 60s 上限覆盖"VPS 开机时 Postgres 容器尚未就绪"的等待窗口；SIGTERM 可提前中断。
	migrateCtx, cancelMigrate := context.WithTimeout(ctx, 60*time.Second)
	if err := store.Migrate(migrateCtx, cfg.DB.URL); err != nil {
		cancelMigrate()
		return fmt.Errorf("数据库迁移: %w", err)
	}
	cancelMigrate()
	slog.Info("数据库迁移完成")

	st, err := store.New(ctx, cfg.DB.URL)
	if err != nil {
		return fmt.Errorf("初始化数据库连接池: %w", err)
	}

	// LLM 客户端 + 调用记账（写库失败只记日志，不影响主流程）。
	llmClient := llm.New(cfg.LLM)
	recorder := llm.NewRecorder(st)

	// 飞书 Manager：凭证存 settings 表而非 config——用户在 Dashboard 向导中填入。
	// 先构造不 Start：推送管道（pusher）要用它做主动发卡的出口；WS 连接推迟到
	// worker/scheduler 就绪后再拉起（B10 装配顺序），保证首个定时触发时出口已备好。
	manager := feishu.NewManager(st, llmClient, recorder)

	// Temporal 客户端：worker 与 HTTP server 同进程。Temporal 是 M3 推送管道的核心，
	// 连不上则拒绝启动（而非降级）——定时/立即推送都依赖它，静默半可用只会掩盖故障。
	temporalClient, err := client.Dial(client.Options{
		HostPort:  cfg.Temporal.Host,
		Namespace: cfg.Temporal.Namespace,
	})
	if err != nil {
		st.Close()
		return fmt.Errorf("连接 Temporal(%s): %w", cfg.Temporal.Host, err)
	}

	// 组装推送管道各步依赖，注入 Activities（所有 I/O 只在 activity 内做，满足确定性约束）。
	// hints 由 scorer 与 cardgen 共享同一实例（M5 契约 §13）：同一 trace 内两者拿到
	// 同一份画像快照——卡片"为什么与你有关"与打分依据必须是同一个画像。
	// st 作为 fetcher.SeenChecker 注入：TikHub 详情补全按次计费，只为未入库的新笔记
	// 付费（见 fetcher.SeenChecker）。传真实 store 而非 nil，否则补全整体被跳过。
	fetch := fetcher.NewMulti(cfg.Fetch, st, st)
	hints := profilehint.NewCache(st)
	score := scorer.New(llmClient, recorder, st, hints)
	cards := cardgen.New(llmClient, recorder, hints)
	push := pusher.New(manager)
	// 构卡函数注入而非 workflow 直接 import feishu：feishu→agent→workflow 依赖链
	// 已存在，直接调用会成环（M5 契约 §8.2）。
	ev := evolver.New(llmClient, recorder, st)
	activities := workflow.NewActivities(fetch, score, cards, push, st, manager, ev, feishu.BuildDeliveryCard)

	// worker：非阻塞 Start，关停时 Stop()（见下方顺序关停）。
	w := worker.New(temporalClient, cfg.Temporal.TaskQueue, worker.Options{})
	w.RegisterWorkflow(workflow.PushPipelineWorkflow)
	// 逐个注册（非整体 Register）：漏注册不会启动失败，而是每批推送在该活动上
	// 重试到超时——EvolveProfile 的错误被 workflow 刻意吞掉，漏注册只会表现为
	// 推送莫名变慢（M5 契约 §13 明示）。
	//
	// 这份清单漏一个的后果在 009 上真实发生过：RecordEmptyBatch 加进 workflow 却
	// 忘了加进这里，全套测试与 go build 照样绿（Temporal 按名查表是运行时行为），
	// 而线上五处闸门的记账**全部静默失败**——整个"空批次可见化"沦为死代码，
	// 库里依旧零行，与没做这个功能逐字一致。由怀疑者审查在合并前抓出。
	// 现已由 workflow/registration_test.go 钉死：它反射 *Activities 的全部
	// Activity 方法并逐字比对本清单，漏一个 CI 就红。**新增 Activity 时改这里即可，
	// 那个测试会告诉你漏没漏。**
	w.RegisterActivity(activities.EvolveProfile)
	w.RegisterActivity(activities.Fetch)
	w.RegisterActivity(activities.Dedup)
	w.RegisterActivity(activities.Score)
	w.RegisterActivity(activities.Select)
	w.RegisterActivity(activities.CardGen)
	w.RegisterActivity(activities.RecordEmptyBatch)
	w.RegisterActivity(activities.Push)
	if err := w.Start(); err != nil {
		temporalClient.Close()
		st.Close()
		return fmt.Errorf("启动 Temporal worker: %w", err)
	}

	// scheduler 是唯一直接碰 SDK client 的调度封装（供 API 建/删/触发调度）。
	sched := scheduler.New(temporalClient, cfg.Temporal.TaskQueue, st)

	// agent loop（M4 契约 §9 装配序：store → llm → scheduler → tools → agent.New →
	// manager 注入）：push_now 工具依赖 scheduler（TriggerPushNow 即 PushTrigger 窄接口），
	// 故装配在 scheduler 之后；注入须在 manager.Start 之前，保证 WS 连接建立时
	// 消息链已能走 agent 而非回退 chat_reply。
	tools := agent.BuildTools(st, sched, sched)
	agentLoop := agent.New(agent.Deps{
		Client:     llmClient,
		Recorder:   recorder,
		Store:      st,
		Profiles:   st,
		Tools:      tools,
		Model:      cfg.LLM.AgentModel,
		MaxTurns:   cfg.Agent.MaxTurns,
		SessionTTL: time.Duration(cfg.Agent.SessionTTLMinutes) * time.Minute,
	})
	manager.SetAgent(agentLoop)

	// 反馈服务（M5 契约 §13）：装在 agent 之后——Notifier 就是 agentLoop
	// （反馈点击要以「[卡片回调]」通告写进当前会话）；同样须在 manager.Start 之前
	// 注入，否则 WS 连上后的首批卡片点击会落到 nil runner 上。
	// deep_dive 走质量档 AgentModel（Boss 拍板③）：用户显式请求、低频、长文质量敏感。
	fbSvc := feedback.New(feedback.Deps{
		Store:         st,
		Client:        llmClient,
		Recorder:      recorder,
		Sender:        manager,
		Notifier:      agentLoop,
		BuildCard:     feishu.BuildDeliveryCard,
		DeepDiveModel: cfg.LLM.AgentModel,
	})
	manager.SetFeedback(fbSvc)

	// 依赖就绪后再拉飞书 WS 连接：无配置静默待命，ctx 取消时断开。
	manager.Start(ctx)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", handleHealthz)
	mux.HandleFunc("GET /readyz", handleReadyz(st))
	api.Mount(mux, api.Deps{
		Store:     st,
		Manager:   manager,
		Scheduler: sched,
		Password:  cfg.Dashboard.Password,
	})

	// A2A server（a2a-contract §7）：enabled=false 时不 Mount——/a2a 与
	// agent-card 路径在 mux 上根本不存在（404），零新增暴露面。
	if cfg.A2A.Enabled {
		if err := a2a.Mount(mux, a2a.Deps{
			Storage: st,
			Content: st,
			Token:   cfg.A2A.Token,
			BaseURL: cfg.A2A.BaseURL,
			Version: vaneVersion,
		}); err != nil {
			return fmt.Errorf("挂载 A2A server: %w", err)
		}
	}

	srv := &http.Server{
		Addr:              cfg.Server.Addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second, // 不设则空闲 keep-alive 连接永不回收
	}

	serveErr := make(chan error, 1)
	go func() {
		slog.Info("HTTP 服务启动", "addr", cfg.Server.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErr <- err
		}
	}()

	select {
	case <-ctx.Done():
		slog.Info("收到关停信号，开始优雅关停")
	case err := <-serveErr:
		// 先取消 ctx 让飞书 Manager 断开 WS、停止使用连接池，再按依赖逆序拆栈。
		stop()
		w.Stop()
		temporalClient.Close()
		st.Close()
		return fmt.Errorf("HTTP 服务异常退出: %w", err)
	}

	// 顺序关停（契约 B10）：HTTP Shutdown → worker.Stop() → Temporal client.Close()
	// → 飞书 Manager（随顶层 ctx 取消自行断 WS）→ DB 连接池。
	// 逆装配序拆栈：先停对外入口，再停后台执行器，最后放连接类资源。
	// 1) HTTP Shutdown（5s 预算）停止接新请求、等在途请求完成；
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Warn("HTTP 关停超时，放弃等待在途请求", "err", err)
	}
	// 2) 停 worker：阻塞等待在跑的 activity 收尾，之后不再拉新任务；
	w.Stop()
	// 3) 关 Temporal 客户端：worker 已停，scheduler 不再被调用，可安全释放连接；
	temporalClient.Close()
	// 4) 关 DB 连接池——此时已无正在执行的 DB 操作。
	// 注意：pgxpool.Close 会阻塞等待借出连接归还——所有走 DB 的 handler
	// 必须自带请求级超时（readyz 是 2s），否则关停可能挂住。
	st.Close()

	// 信号与 ListenAndServe 失败同时发生时 select 可能选中信号分支，
	// 这里补捞一次真实的服务错误，避免启动失败被伪装成正常关停（退出码 0）。
	select {
	case err := <-serveErr:
		return fmt.Errorf("HTTP 服务异常退出: %w", err)
	default:
	}

	slog.Info("关停完成")
	return nil
}

// initLogger 按配置级别初始化全局 slog（JSON 输出，便于日志聚合）。
// 级别解析失败时回退 info，不阻塞启动。
func initLogger(level string) {
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(level)); err != nil {
		lvl = slog.LevelInfo
	}
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl})))
}

// handleHealthz 是存活探针：进程活着即 200，不依赖任何下游。
func handleHealthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, `{"status":"ok"}`)
}

// handleReadyz 是就绪探针：DB Ping 通过返回 200，否则 503。
func handleReadyz(st *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := st.Ping(ctx); err != nil {
			slog.Warn("readyz 数据库探活失败", "err", err)
			writeJSON(w, http.StatusServiceUnavailable, `{"status":"unavailable"}`)
			return
		}
		writeJSON(w, http.StatusOK, `{"status":"ok"}`)
	}
}

func writeJSON(w http.ResponseWriter, status int, body string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	io.WriteString(w, body) //nolint:errcheck // 响应写失败无从补救
}
