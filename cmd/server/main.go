// vane server 入口：加载配置 → 初始化日志 → 自动迁移 → 建库连接 → HTTP 服务 → 优雅关停。
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

	"github.com/YouToco/vane/config"
	"github.com/YouToco/vane/store"
)

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

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", handleHealthz)
	mux.HandleFunc("GET /readyz", handleReadyz(st))

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
		st.Close()
		return fmt.Errorf("HTTP 服务异常退出: %w", err)
	}

	// 顺序关停（Step 4 设计的三步，MVP 尚无 Temporal Worker，简化为两步）：
	// 1) HTTP Shutdown（5s 预算）停止接新请求、等在途请求完成；
	// 2) 关闭 DB 连接池——此时已无正在执行的 DB 操作。
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Warn("HTTP 关停超时，放弃等待在途请求", "err", err)
	}
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
