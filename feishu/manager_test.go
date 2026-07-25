package feishu

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	lark "github.com/larksuite/oapi-sdk-go/v3"

	"github.com/YouToco/vane/store"
	"github.com/YouToco/vane/types"
)

func TestManagerSDKLogLevelDoesNotExposeWebSocketCredentials(t *testing.T) {
	source, err := os.ReadFile("manager.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(source)
	if strings.Contains(code, "larkws.WithLogLevel(larkcore.LogLevelInfo)") {
		t.Fatal("Feishu SDK INFO logs expose WebSocket access_key and ticket")
	}
	if !strings.Contains(code, "larkws.WithLogLevel(larkcore.LogLevelError)") {
		t.Fatal("Feishu SDK must retain credential-free error logging")
	}
}

func TestManagerShutdownRejectsNewWorkAndWaitsForInflight(t *testing.T) {
	m := NewManager(nil, nil, nil)
	started := make(chan struct{})
	release := make(chan struct{})
	finished := make(chan struct{})
	if !m.startAsync(context.Background(), time.Minute, true, "test_inflight", func(context.Context) {
		close(started)
		<-release
		close(finished)
	}) {
		t.Fatal("初始异步工作应被接纳")
	}
	<-started

	shutdownDone := make(chan error, 1)
	go func() {
		shutdownDone <- m.Shutdown(context.Background())
	}()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		m.asyncMu.Lock()
		accepting := m.asyncAccepting
		m.asyncMu.Unlock()
		if !accepting {
			break
		}
		time.Sleep(time.Millisecond)
	}
	m.asyncMu.Lock()
	accepting := m.asyncAccepting
	m.asyncMu.Unlock()
	if accepting {
		t.Fatal("Shutdown 未在 1s 内关闭异步准入")
	}

	var rejectedRan atomic.Bool
	if m.startAsync(context.Background(), time.Second, true, "must_reject", func(context.Context) {
		rejectedRan.Store(true)
	}) {
		t.Fatal("Shutdown 开始后不应接纳新异步工作")
	}
	if rejectedRan.Load() {
		t.Fatal("被拒绝的异步工作不应执行")
	}

	select {
	case err := <-shutdownDone:
		t.Fatalf("在途工作完成前 Shutdown 不应返回，实际 err=%v", err)
	case <-time.After(25 * time.Millisecond):
	}

	close(release)
	select {
	case err := <-shutdownDone:
		if err != nil {
			t.Fatalf("Shutdown() = %v，期望 nil", err)
		}
	case <-time.After(time.Second):
		t.Fatal("在途工作完成后 Shutdown 未返回")
	}
	select {
	case <-finished:
	default:
		t.Fatal("Shutdown 返回前必须已等待在途工作完成")
	}
}

func TestManagerShutdownDeadlineIsBounded(t *testing.T) {
	m := NewManager(nil, nil, nil)
	started := make(chan struct{})
	release := make(chan struct{})
	if !m.startAsync(context.Background(), time.Minute, true, "test_stuck", func(context.Context) {
		close(started)
		// 刻意模拟无视 ctx 的错误下游；Shutdown 仍必须服从调用方 deadline。
		<-release
	}) {
		t.Fatal("初始异步工作应被接纳")
	}
	<-started

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	startedAt := time.Now()
	err := m.Shutdown(ctx)
	elapsed := time.Since(startedAt)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Shutdown() = %v，期望 DeadlineExceeded", err)
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("Shutdown 超时返回耗时 %v，未受调用方 deadline 约束", elapsed)
	}

	// 释放故障夹具并再次等待，避免测试自己留下 goroutine。
	close(release)
	drainCtx, drainCancel := context.WithTimeout(context.Background(), time.Second)
	defer drainCancel()
	if err := m.Shutdown(drainCtx); err != nil {
		t.Fatalf("释放在途工作后的 Shutdown() = %v，期望 nil", err)
	}
}

func TestManagerAsyncPanicDoesNotBlockShutdown(t *testing.T) {
	m := NewManager(nil, nil, nil)
	if !m.startAsync(context.Background(), time.Second, true, "test_panic", func(context.Context) {
		panic("boom")
	}) {
		t.Fatal("异步工作应被接纳")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := m.Shutdown(ctx); err != nil {
		t.Fatalf("panic 兜底后 Shutdown() = %v，期望 nil", err)
	}
}

func TestManagerReconfigureRejectedAfterShutdown(t *testing.T) {
	m := NewManager(nil, nil, nil)
	if err := m.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	err := m.Reconfigure(t.Context())
	if !errors.Is(err, types.ErrConflict) {
		t.Fatalf("Reconfigure after shutdown = %v, want conflict without touching Store", err)
	}
}

func TestManagerShutdownRetainsSenderForDownstreamDrain(t *testing.T) {
	m := NewManager(nil, nil, nil)
	want := lark.NewClient("test-app", "test-secret")
	m.mu.Lock()
	m.apiClient = want
	m.apiAppID = "test-app"
	m.mu.Unlock()

	if err := m.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got := m.api(); got != want {
		t.Fatal("Manager 排空后必须保留发送客户端，供 feedback/worker 后续 drain")
	}
	m.mu.Lock()
	gotAppID := m.apiAppID
	m.mu.Unlock()
	if gotAppID != "test-app" {
		t.Fatalf("Manager 排空后发送身份=%q, want test-app", gotAppID)
	}
}

func TestManagerReloadDisabledRevokesOutboundClient(t *testing.T) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("DATABASE_URL 未设置，跳过飞书权限撤销真库测试")
	}
	ctx := t.Context()
	if err := store.Migrate(ctx, dbURL); err != nil {
		t.Fatal(err)
	}
	st, err := store.New(ctx, dbURL)
	if err != nil {
		t.Fatal(err)
	}
	registerStoreClose(t, st)
	old, oldErr := st.GetSetting(ctx, settingKeyFeishu)
	if oldErr != nil && !errors.Is(oldErr, types.ErrNotFound) {
		t.Fatal(oldErr)
	}
	cleanupCtx, cancelCleanup := cleanupContext()
	t.Cleanup(cancelCleanup)
	t.Cleanup(func() {
		if oldErr == nil {
			cleanupExec(cleanupCtx, t, dbURL, `
				INSERT INTO settings (key, value, updated_at) VALUES ($1, $2, now())
				ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now()`,
				settingKeyFeishu, old)
			return
		}
		cleanupExec(cleanupCtx, t, dbURL, `DELETE FROM settings WHERE key = $1`, settingKeyFeishu)
	})
	raw, err := json.Marshal(feishuSetting{
		AppID: "test-app", AppSecret: "revoked-secret", Enabled: false,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := st.PutSetting(ctx, settingKeyFeishu, raw); err != nil {
		t.Fatal(err)
	}

	m := NewManager(st, nil, nil)
	m.mu.Lock()
	m.baseCtx = context.Background()
	m.apiClient = lark.NewClient("old-app", "old-secret")
	m.apiAppID = "old-app"
	m.mu.Unlock()
	if err := m.reload(ctx); err != nil {
		t.Fatal(err)
	}
	m.mu.Lock()
	client, appID := m.apiClient, m.apiAppID
	m.mu.Unlock()
	if client != nil || appID != "" {
		t.Fatalf("disabled reconfigure retained outbound authority: client=%v app_id=%q", client != nil, appID)
	}
}
