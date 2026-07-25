package feishu

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	lark "github.com/larksuite/oapi-sdk-go/v3"

	"github.com/YouToco/vane/store"
	"github.com/YouToco/vane/types"
)

func TestManagerUsesCredentialSafeSDKLogger(t *testing.T) {
	fset := token.NewFileSet()
	newClientSelectors := 0
	err := filepath.WalkDir("..", func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			if entry.Name() == ".git" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, parseErr := parser.ParseFile(fset, filepath.Clean(path), nil, 0)
		if parseErr != nil {
			return parseErr
		}
		wsAliases := make(map[string]struct{})
		for _, spec := range file.Imports {
			importPath, unquoteErr := strconv.Unquote(spec.Path.Value)
			if unquoteErr != nil || importPath != "github.com/larksuite/oapi-sdk-go/v3/ws" {
				continue
			}
			alias := "ws"
			if spec.Name != nil {
				alias = spec.Name.Name
			}
			if alias == "." {
				t.Fatalf("Feishu WS SDK dot import bypasses constructor guard: %s", path)
			}
			wsAliases[alias] = struct{}{}
		}

		var parents []ast.Node
		ast.Inspect(file, func(node ast.Node) bool {
			if node == nil {
				parents = parents[:len(parents)-1]
				return true
			}
			var parent ast.Node
			if len(parents) > 0 {
				parent = parents[len(parents)-1]
			}
			parents = append(parents, node)

			selector, ok := node.(*ast.SelectorExpr)
			if !ok || selector.Sel.Name != "NewClient" {
				return true
			}
			pkg, ok := selector.X.(*ast.Ident)
			if !ok {
				return true
			}
			if _, imported := wsAliases[pkg.Name]; !imported {
				return true
			}
			newClientSelectors++
			call, direct := parent.(*ast.CallExpr)
			rel, relErr := filepath.Rel("..", path)
			if relErr != nil {
				t.Fatal(relErr)
			}
			if !direct || call.Fun != selector ||
				filepath.ToSlash(rel) != "feishu/manager.go" {
				t.Fatalf("Feishu WS constructor must be one direct manager.go call: %s",
					fset.Position(selector.Pos()))
			}
			assertSafeWSClientOptions(t, call, pkg.Name)
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if newClientSelectors != 1 {
		t.Fatalf("Feishu WS constructor selectors = %d, want 1", newClientSelectors)
	}
}

func assertSafeWSClientOptions(t *testing.T, call *ast.CallExpr, wsAlias string) {
	t.Helper()
	if len(call.Args) != 5 {
		t.Fatalf("Feishu WS constructor args = %d, want app, secret and exactly 3 options",
			len(call.Args))
	}
	counts := map[string]int{}
	for _, expr := range call.Args[2:] {
		option, ok := expr.(*ast.CallExpr)
		if !ok {
			t.Fatal("Feishu WS options must be direct calls")
		}
		selector, ok := option.Fun.(*ast.SelectorExpr)
		if !ok {
			t.Fatal("Feishu WS option must be a package selector")
		}
		pkg, ok := selector.X.(*ast.Ident)
		if !ok || pkg.Name != wsAlias || len(option.Args) != 1 {
			t.Fatal("Feishu WS option must be a single-argument SDK option")
		}
		counts[selector.Sel.Name]++
		switch selector.Sel.Name {
		case "WithEventHandler":
		case "WithLogger":
			if !isDirectZeroArgCall(option.Args[0], "newFeishuSDKLogger") {
				t.Fatal("Feishu WS logger is not the credential-safe logger")
			}
		case "WithLogLevel":
			if !isSelector(option.Args[0], "larkcore", "LogLevelError") {
				t.Fatal("Feishu WS log level is not Error")
			}
		default:
			t.Fatalf("unreviewed Feishu WS option %s", selector.Sel.Name)
		}
	}
	for _, option := range []string{"WithEventHandler", "WithLogger", "WithLogLevel"} {
		if counts[option] != 1 {
			t.Fatalf("Feishu WS option %s count = %d, want 1", option, counts[option])
		}
	}
}

func isDirectZeroArgCall(expr ast.Expr, name string) bool {
	call, ok := expr.(*ast.CallExpr)
	if !ok || len(call.Args) != 0 {
		return false
	}
	ident, ok := call.Fun.(*ast.Ident)
	return ok && ident.Name == name
}

func isSelector(expr ast.Expr, pkgName, selectorName string) bool {
	selector, ok := expr.(*ast.SelectorExpr)
	if !ok || selector.Sel.Name != selectorName {
		return false
	}
	pkg, ok := selector.X.(*ast.Ident)
	return ok && pkg.Name == pkgName
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

func TestCaptureOwnerBindsP2PChatToCurrentApp(t *testing.T) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("DATABASE_URL 未设置，跳过 owner chat 真库测试")
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
	old, oldErr := st.GetSetting(ctx, settingKeyOwner)
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
				settingKeyOwner, old)
			return
		}
		cleanupExec(cleanupCtx, t, dbURL,
			`DELETE FROM settings WHERE key = $1`, settingKeyOwner)
	})

	initial, err := json.Marshal(ownerSetting{
		OpenID: "ou_owner", Name: "owner",
		CapturedAt: "2026-07-24T00:00:00Z",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := st.PutSetting(ctx, settingKeyOwner, initial); err != nil {
		t.Fatal(err)
	}
	m := NewManager(st, nil, nil)
	m.mu.Lock()
	m.apiClient = lark.NewClient("test-app", "test-secret")
	m.apiAppID = "test-app"
	m.mu.Unlock()
	m.setOwner("ou_owner", "owner")
	h := newHandlerForApp(m, context.Background(), "test-app")

	h.captureOwnerIfFirst(
		ctx, "ou_owner", "owner", "oc_group", "group", "test-app")
	if got := m.OwnerChatID(); got != "" {
		t.Fatalf("group message captured owner P2P chat id: %q", got)
	}
	h.captureOwnerIfFirst(
		ctx, "ou_owner", "owner", "oc_owner_chat", "p2p", "test-app")
	if got := m.OwnerChatID(); got != "oc_owner_chat" {
		t.Fatalf("owner chat id = %q, want oc_owner_chat", got)
	}
	raw, err := st.GetSetting(ctx, settingKeyOwner)
	if err != nil {
		t.Fatal(err)
	}
	var persisted ownerSetting
	if err := json.Unmarshal(raw, &persisted); err != nil {
		t.Fatal(err)
	}
	if persisted.OpenID != "ou_owner" ||
		persisted.AppIdentity != "test-app" ||
		persisted.ChatID != "oc_owner_chat" {
		t.Fatalf("persisted owner = %+v", persisted)
	}

	h.captureOwnerIfFirst(
		ctx, "ou_owner", "owner", "oc_group_2", "group", "test-app")
	if got := m.OwnerChatID(); got != "oc_owner_chat" {
		t.Fatalf("group message overwrote owner P2P chat id: %q", got)
	}
	h.captureOwnerIfFirst(
		ctx, "ou_other", "other", "oc_other_chat", "p2p", "test-app")
	if got := m.OwnerChatID(); got != "oc_owner_chat" {
		t.Fatalf("non-owner overwrote owner chat id: %q", got)
	}

	restarted := NewManager(st, nil, nil)
	restarted.mu.Lock()
	restarted.apiClient = lark.NewClient("test-app", "rotated-secret")
	restarted.apiAppID = "test-app"
	restarted.mu.Unlock()
	restarted.loadOwner(ctx)
	if got := restarted.OwnerChatID(); got != "oc_owner_chat" {
		t.Fatalf("same-App secret rotation lost owner chat id: %q", got)
	}

	retiredHandler := newHandlerForApp(
		restarted, context.Background(), "test-app")
	restarted.mu.Lock()
	restarted.apiClient = lark.NewClient("new-app", "new-secret")
	restarted.apiAppID = "new-app"
	restarted.mu.Unlock()
	if got := restarted.OwnerChatID(); got != "" {
		t.Fatalf("cross-App reconfigure exposed old owner chat id: %q", got)
	}
	// An event from the retired App generation cannot restore its chat.
	retiredHandler.captureOwnerIfFirst(
		ctx, "ou_owner", "owner", "oc_retired", "p2p", "test-app")
	if got := restarted.OwnerChatID(); got != "" {
		t.Fatalf("retired App handler restored old owner chat id: %q", got)
	}
	newHandler := newHandlerForApp(restarted, context.Background(), "new-app")
	newHandler.captureOwnerIfFirst(
		ctx, "ou_owner", "owner", "oc_new_group", "group", "new-app")
	if got := restarted.OwnerChatID(); got != "" {
		t.Fatalf("new App group message captured P2P chat id: %q", got)
	}
	newHandler.captureOwnerIfFirst(
		ctx, "ou_owner", "owner", "oc_new_p2p", "p2p", "new-app")
	if got := restarted.OwnerChatID(); got != "oc_new_p2p" {
		t.Fatalf("new App P2P message did not rebind owner chat id: %q", got)
	}
}

func TestCaptureOwnerConcurrentGroupAndP2PConvergesToP2P(t *testing.T) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("DATABASE_URL 未设置，跳过 owner chat 并发真库测试")
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
	old, oldErr := st.GetSetting(ctx, settingKeyOwner)
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
				settingKeyOwner, old)
			return
		}
		cleanupExec(cleanupCtx, t, dbURL,
			`DELETE FROM settings WHERE key = $1`, settingKeyOwner)
	})
	if err := st.PutSetting(ctx, settingKeyOwner, json.RawMessage(
		`{"open_id":"ou_owner","name":"owner","captured_at":"2026-07-24T00:00:00Z"}`,
	)); err != nil {
		t.Fatal(err)
	}

	m := NewManager(st, nil, nil)
	m.mu.Lock()
	m.apiClient = lark.NewClient("test-app", "test-secret")
	m.apiAppID = "test-app"
	m.mu.Unlock()
	m.setOwner("ou_owner", "owner")
	h := newHandlerForApp(m, context.Background(), "test-app")

	var wg sync.WaitGroup
	for i := range 32 {
		wg.Add(2)
		go func() {
			defer wg.Done()
			h.captureOwnerIfFirst(
				ctx, "ou_owner", "owner", "oc_group", "group", "test-app")
		}()
		go func(index int) {
			defer wg.Done()
			h.captureOwnerIfFirst(
				ctx,
				"ou_owner",
				"owner",
				fmt.Sprintf("oc_p2p_%d", index),
				"p2p",
				"test-app",
			)
		}(i)
	}
	wg.Wait()
	if got := m.OwnerChatID(); !strings.HasPrefix(got, "oc_p2p_") {
		t.Fatalf("concurrent capture did not converge to P2P chat: %q", got)
	}
	raw, err := st.GetSetting(ctx, settingKeyOwner)
	if err != nil {
		t.Fatal(err)
	}
	var persisted ownerSetting
	if err := json.Unmarshal(raw, &persisted); err != nil {
		t.Fatal(err)
	}
	if persisted.AppIdentity != "test-app" ||
		!strings.HasPrefix(persisted.ChatID, "oc_p2p_") {
		t.Fatalf("persisted owner = %+v, want current App P2P chat", persisted)
	}
}
