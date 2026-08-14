package a2a

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2aclient"
	"github.com/a2aproject/a2a-go/v2/a2asrv"

	"github.com/YouToco/vane/types"
)

const testToken = "test-token-a2a"

// newTestServer Mount 后的真 mux 起 httptest server（契约 §9.2）。
func newTestServer(t *testing.T, content *fakeContent) (*httptest.Server, *fakeTaskStorage) {
	t.Helper()
	storage := newFakeTaskStorage()
	mux := http.NewServeMux()
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	if _, err := Mount(mux, Deps{
		Storage: storage,
		Content: content,
		Token:   testToken,
		BaseURL: srv.URL + "/a2a",
		Version: "0.5.0-test",
	}); err != nil {
		t.Fatalf("Mount 失败: %v", err)
	}
	return srv, storage
}

// rpc 发一个 JSON-RPC 请求，返回原始响应体。
func rpc(t *testing.T, srv *httptest.Server, token, method string, params any) map[string]any {
	t.Helper()
	body, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0", "id": 1, "method": method, "params": params,
	})
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/a2a", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("请求失败: %v", err)
	}
	defer resp.Body.Close()
	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("响应不是 JSON（status=%d）: %v", resp.StatusCode, err)
	}
	return out
}

func TestCardEndpointPublic(t *testing.T) {
	srv, _ := newTestServer(t, &fakeContent{})
	resp, err := srv.Client().Get(srv.URL + a2asrv.WellKnownAgentCardPath)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("card 端点应公开 200，实际 %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "application/json") {
		t.Errorf("Content-Type 应 application/json，实际 %q", ct)
	}
	var card map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&card); err != nil {
		t.Fatal(err)
	}
	if card["name"] != "见微 Vane" {
		t.Errorf("card name 不符: %v", card["name"])
	}
}

func TestPostA2AUnauthorized(t *testing.T) {
	srv, _ := newTestServer(t, &fakeContent{})
	for _, auth := range []string{"", "Bearer wrong"} {
		req, _ := http.NewRequest(http.MethodPost, srv.URL+"/a2a", strings.NewReader("{}"))
		if auth != "" {
			req.Header.Set("Authorization", auth)
		}
		resp, err := srv.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("auth=%q 应 401，实际 %d", auth, resp.StatusCode)
		}
		if resp.Header.Get("WWW-Authenticate") != "Bearer" {
			t.Error("401 应带 WWW-Authenticate: Bearer")
		}
	}
}

// TestSendMessageCompleted 合法 JSON-RPC SendMessage 走通回 COMPLETED（契约 §9.2）。
func TestSendMessageCompleted(t *testing.T) {
	pub := time.Now().Add(-2 * time.Hour)
	content := &fakeContent{items: []types.ContentItem{
		{Title: "GPT-Red 发布", URL: "https://example.com/x", Content: "正文", PublishedAt: &pub},
	}}
	srv, storage := newTestServer(t, content)

	out := rpc(t, srv, testToken, "SendMessage", map[string]any{
		"message": map[string]any{
			"messageId": "m-1", "role": "ROLE_USER",
			"parts": []map[string]any{{"text": `{"keyword":"GPT","days":7}`}},
		},
	})
	if out["error"] != nil {
		t.Fatalf("SendMessage 报错: %v", out["error"])
	}
	result := out["result"].(map[string]any)
	task, ok := result["task"].(map[string]any)
	if !ok {
		// 结果联合类型的另一形态：直接就是 task 对象。
		task = result
	}
	status := task["status"].(map[string]any)
	if status["state"] != "TASK_STATE_COMPLETED" {
		t.Fatalf("应 COMPLETED，实际 %v（完整: %v）", status["state"], out)
	}
	if arts := task["artifacts"].([]any); len(arts) != 1 {
		t.Fatalf("应有 1 个 Artifact，实际 %v", task["artifacts"])
	}
	// 任务已持久化到 storage（⑧ 的单测面）。
	if len(storage.rows) == 0 {
		t.Fatal("任务应已写入 TaskStorage")
	}
}

// TestTaskstoreErrorSanitized 适配层错误卫生突变测试（契约 §8.1，审查 HIGH 实证）：
// SDK 把 taskstore 错误的 Error() 文本写进 JSON-RPC error.message——注入原始 DB
// 错误链，断言响应逐字不含。
func TestTaskstoreErrorSanitized(t *testing.T) {
	const leak = "pgx: connection refused; host=127.0.0.1 dbname=vane"
	srv, storage := newTestServer(t, &fakeContent{})
	storage.getErr = fmt.Errorf("%s", leak)

	out := rpc(t, srv, testToken, "GetTask", map[string]any{"id": "any-task"})
	raw, _ := json.Marshal(out)
	for _, frag := range []string{"pgx", "dbname", "127.0.0.1"} {
		if strings.Contains(string(raw), frag) {
			t.Fatalf("taskstore 错误泄露内部链（%q）: %s", frag, raw)
		}
	}
	if out["error"] == nil {
		t.Fatalf("应返回错误响应，实际 %v", out)
	}
}

// TestGetTaskNotFound 查不存在 taskId 得 -32001（依赖 §5.9 Get→a2a.ErrTaskNotFound 映射）。
func TestGetTaskNotFound(t *testing.T) {
	srv, _ := newTestServer(t, &fakeContent{})
	out := rpc(t, srv, testToken, "GetTask", map[string]any{"id": "no-such-task"})
	errObj, ok := out["error"].(map[string]any)
	if !ok {
		t.Fatalf("应返回 JSON-RPC error，实际 %v", out)
	}
	if code := errObj["code"].(float64); code != -32001 {
		t.Fatalf("error code 应 -32001（TaskNotFound），实际 %v", code)
	}
}

// TestStreamingRejected streaming 方法应收能力类错误而非 SSE（WithCapabilityChecks 生效钉子；
// 若 SDK 缺省已拒，保留为 SDK 行为钉子，升级 SDK 先跑）。
func TestStreamingRejected(t *testing.T) {
	srv, _ := newTestServer(t, &fakeContent{})
	body, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0", "id": 1, "method": "SendStreamingMessage",
		"params": map[string]any{
			"message": map[string]any{
				"messageId": "m-1", "role": "ROLE_USER",
				"parts": []map[string]any{{"text": "hi"}},
			},
		},
	})
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/a2a", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+testToken)
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	buf := new(bytes.Buffer)
	if _, err := buf.ReadFrom(resp.Body); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), "error") {
		t.Fatalf("streaming 方法应得错误响应，实际: %s", buf.String())
	}
	// 不应回成功的 SSE 事件流。
	if strings.Contains(buf.String(), "TASK_STATE_COMPLETED") {
		t.Fatalf("streaming=false 却走通了流式: %s", buf.String())
	}
}

// TestDisabledIs404 enabled=false 语义（契约 §7/§9.5）：main.go 不 Mount 时
// /a2a 与 card 路径在 mux 上根本不存在。
func TestDisabledIs404(t *testing.T) {
	mux := http.NewServeMux()
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	for _, path := range []string{"/a2a", a2asrv.WellKnownAgentCardPath} {
		req, _ := http.NewRequest(http.MethodPost, srv.URL+path, strings.NewReader("{}"))
		resp, err := srv.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("未 Mount 时 %s 应 404，实际 %d", path, resp.StatusCode)
		}
	}
}

// TestA2AClientSmoke 官方 a2aclient 互通 smoke（契约 §9.4）：NewFromCard 读本进程
// server 的 card → SendMessage → GetTask 轮询终态 → 断言 Artifact。client 侧只配
// 凭证、不手动加 Authorization 头——AuthInterceptor 按卡片 securityRequirements
// 附 Bearer，顺带实证 §5.8 卡片驱动认证成立（缺 securityRequirements 本测试必 401 红）。
func TestA2AClientSmoke(t *testing.T) {
	pub := time.Now().Add(-time.Hour)
	content := &fakeContent{items: []types.ContentItem{
		{Title: "smoke 条目", URL: "https://example.com/s", Content: "正文S", PublishedAt: &pub},
	}}
	srv, _ := newTestServer(t, content)

	// 从 well-known 端点取真实 card（不是进程内对象——走完整发现路径）。
	resp, err := srv.Client().Get(srv.URL + a2asrv.WellKnownAgentCardPath)
	if err != nil {
		t.Fatal(err)
	}
	var card a2a.AgentCard
	if err := json.NewDecoder(resp.Body).Decode(&card); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	creds := a2aclient.NewInMemoryCredentialsStore()
	client, err := a2aclient.NewFromCard(t.Context(), &card,
		a2aclient.WithCallInterceptors(&a2aclient.AuthInterceptor{Service: creds}))
	if err != nil {
		t.Fatalf("NewFromCard 失败: %v", err)
	}
	defer client.Destroy()

	const sid = a2aclient.SessionID("smoke-session")
	creds.Set(sid, bearerScheme, a2aclient.AuthCredential(testToken))
	ctx := a2aclient.AttachSessionID(t.Context(), sid)

	result, err := client.SendMessage(ctx, &a2a.SendMessageRequest{
		Message: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("smoke")),
	})
	if err != nil {
		t.Fatalf("SendMessage 失败: %v", err)
	}
	task, ok := result.(*a2a.Task)
	if !ok {
		t.Fatalf("结果应为 *a2a.Task，实际 %T", result)
	}

	// GetTask 轮询终态（同步实现应立即终态，轮询上限只是防御）。
	deadline := time.Now().Add(5 * time.Second)
	for !task.Status.State.Terminal() && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
		task, err = client.GetTask(ctx, &a2a.GetTaskRequest{ID: task.ID})
		if err != nil {
			t.Fatalf("GetTask 失败: %v", err)
		}
	}
	if task.Status.State != a2a.TaskStateCompleted {
		t.Fatalf("终态应 COMPLETED，实际 %s", task.Status.State)
	}
	if len(task.Artifacts) != 1 || len(task.Artifacts[0].Parts) != 2 {
		t.Fatalf("Artifact 应含 text+data 两 part，实际 %+v", task.Artifacts)
	}
	if !strings.Contains(task.Artifacts[0].Parts[0].Text(), "smoke 条目") {
		t.Errorf("Artifact 文本应含条目标题，实际 %q", task.Artifacts[0].Parts[0].Text())
	}
	_ = fmt.Sprint() // keep fmt import if assertions change
}
