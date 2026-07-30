package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/YouToco/vane/store"
	"github.com/YouToco/vane/taskhealth"
	"github.com/YouToco/vane/types"
)

type briefFollowupDeadlineRecorder struct {
	*httptest.ResponseRecorder
	deadlines []time.Time
	setErr    error
}

type briefFollowupSlowAuthStore struct {
	AuthStore
	delay time.Duration
}

func (s *briefFollowupSlowAuthStore) LookupSession(
	ctx context.Context,
	_ []byte,
) (*types.Session, error) {
	timer := time.NewTimer(s.delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
		return nil, types.NewAppError(
			types.CodeNotFound, "fake: session not found", nil)
	}
}

func (w *briefFollowupDeadlineRecorder) SetWriteDeadline(
	deadline time.Time,
) error {
	w.deadlines = append(w.deadlines, deadline)
	return w.setErr
}

func TestBriefFollowupWriteBudgetCoversBoundedRetryContract(t *testing.T) {
	if groundedBriefFollowupExecutionBudget != 397*time.Second {
		t.Fatalf("grounded retry contract drifted to %s",
			groundedBriefFollowupExecutionBudget)
	}
	if groundedBriefFollowupWriteBudget != 7*time.Minute {
		t.Fatalf("write budget = %s, want exactly 7m", groundedBriefFollowupWriteBudget)
	}
	if groundedBriefFollowupWriteBudget <=
		groundedBriefFollowupExecutionBudget {
		t.Fatalf("write budget %s does not leave response headroom after %s",
			groundedBriefFollowupWriteBudget,
			groundedBriefFollowupExecutionBudget)
	}
}

func TestBriefFollowupExtendsRealServerWriteDeadlineForStructuredLLMError(
	t *testing.T,
) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := prepareBriefFollowupResponseV1(
			w, r.Context(), time.Now(),
		); err != nil {
			t.Errorf("prepare response: %v", err)
			return
		}
		// Exceed the real server WriteTimeout. Without the route-local
		// ResponseController deadline the client observes EOF, not this JSON.
		time.Sleep(100 * time.Millisecond)
		writeAppError(w, types.NewAppError(
			types.CodeLLMRateLimit,
			"模型服务繁忙，请稍后重试",
			errors.New("provider returned 429"),
		))
	})
	testServer := httptest.NewUnstartedServer(handler)
	testServer.Config.WriteTimeout = 20 * time.Millisecond
	testServer.Start()
	t.Cleanup(testServer.Close)

	response, err := testServer.Client().Get(testServer.URL)
	if err != nil {
		t.Fatalf("grounded error response became a transport failure: %v", err)
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read structured response: %v", err)
	}
	if response.StatusCode != http.StatusBadGateway {
		t.Fatalf("status = %d, want %d; body=%s",
			response.StatusCode, http.StatusBadGateway, raw)
	}
	var payload map[string]string
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatalf("response is not JSON: %v; body=%s", err, raw)
	}
	if payload["error"] != "模型服务繁忙，请稍后重试" {
		t.Fatalf("unexpected structured error: %#v", payload)
	}
}

func TestGroundedBriefFollowupDeadlineWrapsRealRouteBeforeSlowAuth(
	t *testing.T,
) {
	mux := http.NewServeMux()
	Mount(mux, Deps{Auth: &briefFollowupSlowAuthStore{
		delay: 100 * time.Millisecond,
	}})
	testServer := httptest.NewUnstartedServer(mux)
	testServer.Config.WriteTimeout = 20 * time.Millisecond
	testServer.Start()
	t.Cleanup(testServer.Close)

	request, err := http.NewRequest(
		http.MethodPost,
		testServer.URL+"/api/schedules/task-v1/reports/3/ask",
		strings.NewReader(`{"question":"依据是什么？"}`),
	)
	if err != nil {
		t.Fatal(err)
	}
	request.AddCookie(&http.Cookie{Name: sessionCookieName, Value: "test"})
	response, err := testServer.Client().Do(request)
	if err != nil {
		t.Fatalf("slow auth became EOF instead of structured response: %v", err)
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d; body=%s",
			response.StatusCode, http.StatusUnauthorized, raw)
	}
	var payload map[string]string
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatalf("slow-auth response is not JSON: %v; body=%s", err, raw)
	}
	if payload["error"] != "未登录或会话已过期" {
		t.Fatalf("unexpected slow-auth response: %#v", payload)
	}
}

func TestGroundedBriefFollowupAuthDeadlineBecomesStructuredUpstreamError(
	t *testing.T,
) {
	s := &server{deps: Deps{Auth: &briefFollowupSlowAuthStore{
		delay: time.Hour,
	}}}
	inner := http.NewServeMux()
	inner.HandleFunc(
		"POST /api/schedules/{id}/reports/{target_id}/ask",
		func(http.ResponseWriter, *http.Request) {
			t.Error("expired authentication must not reach grounded handler")
		},
	)
	handler := groundedBriefFollowupDeadlineWithBudgetV1(
		s.cors(s.requireSession(inner)),
		40*time.Millisecond,
		200*time.Millisecond,
	)
	testServer := httptest.NewUnstartedServer(handler)
	testServer.Config.WriteTimeout = 20 * time.Millisecond
	testServer.Start()
	t.Cleanup(testServer.Close)

	request, err := http.NewRequest(
		http.MethodPost,
		testServer.URL+"/api/schedules/task-v1/reports/3/ask",
		strings.NewReader(`{"question":"依据是什么？"}`),
	)
	if err != nil {
		t.Fatal(err)
	}
	request.AddCookie(&http.Cookie{Name: sessionCookieName, Value: "test"})
	response, err := testServer.Client().Do(request)
	if err != nil {
		t.Fatalf("auth deadline became a transport failure: %v", err)
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusBadGateway {
		t.Fatalf("status = %d, want %d; body=%s",
			response.StatusCode, http.StatusBadGateway, raw)
	}
	var payload map[string]string
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatalf("deadline response is not JSON: %v; body=%s", err, raw)
	}
	if payload["error"] != "简报追问处理超时，请稍后重试" {
		t.Fatalf("auth deadline leaked a 401 response: %#v", payload)
	}
}

func TestGroundedBriefFollowupDeadlineRouteMatchIsExact(t *testing.T) {
	for _, tc := range []struct {
		method string
		path   string
		want   bool
	}{
		{http.MethodPost, "/api/schedules/task/briefs/1/ask", true},
		{http.MethodPost, "/api/schedules/task/reports/1/ask", true},
		{http.MethodGet, "/api/schedules/task/reports/1/ask", false},
		{http.MethodOptions, "/api/schedules/task/reports/1/ask", false},
		{http.MethodPost, "/api/schedules/task/reports/1/deep-dive", false},
		{http.MethodPost, "/api/schedules/task/reports/1/ask/extra", false},
		{http.MethodPost, "/api/schedules//reports/1/ask", false},
		{http.MethodPost, "/api/schedules/task/runs/1/ask", false},
		{http.MethodPost, "/api/schedules/task%2Fchild/reports/1/ask", true},
		{http.MethodPost, "/%61pi/schedules/task/reports/1/%61sk", true},
	} {
		request := httptest.NewRequest(tc.method, tc.path, nil)
		if got := isGroundedBriefFollowupRequestV1(request); got != tc.want {
			t.Fatalf("%s %s matched=%t, want %t",
				tc.method, tc.path, got, tc.want)
		}
	}
}

func TestGroundedBriefFollowupDeadlineDoesNotWrapOtherAPIRoutes(
	t *testing.T,
) {
	response := &briefFollowupDeadlineRecorder{
		ResponseRecorder: httptest.NewRecorder(),
	}
	called := false
	handler := groundedBriefFollowupDeadlineWithBudgetV1(
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			called = true
			writeError(w, http.StatusTeapot, "control")
		}),
		20*time.Millisecond,
		20*time.Millisecond,
	)
	handler.ServeHTTP(
		response,
		httptest.NewRequest(
			http.MethodPost, "/api/schedules/task/run", nil),
	)
	if !called {
		t.Fatal("ordinary API route did not reach original handler")
	}
	if len(response.deadlines) != 0 {
		t.Fatalf("ordinary API route received %d write deadlines, want 0",
			len(response.deadlines))
	}
	if response.Code != http.StatusTeapot {
		t.Fatalf("ordinary API status = %d, want %d",
			response.Code, http.StatusTeapot)
	}
}

func TestBriefFollowupDeadlineIsFirstBoundedStepForDeterministicError(
	t *testing.T,
) {
	response := &briefFollowupDeadlineRecorder{
		ResponseRecorder: httptest.NewRecorder(),
	}
	request := httptest.NewRequest(
		http.MethodPost, "/api/schedules/task/reports/not-a-number/ask", nil)
	request.SetPathValue("target_id", "not-a-number")
	before := time.Now()

	(&server{}).handleBriefFollowup(
		response, request, store.GroundedBriefReport)

	after := time.Now()
	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusBadRequest)
	}
	if len(response.deadlines) != 1 {
		t.Fatalf("SetWriteDeadline calls = %d, want 1", len(response.deadlines))
	}
	deadline := response.deadlines[0]
	if deadline.Before(before.Add(groundedBriefFollowupWriteBudget)) ||
		deadline.After(after.Add(groundedBriefFollowupWriteBudget)) {
		t.Fatalf("deadline %s is outside bounded request-start window [%s, %s]",
			deadline,
			before.Add(groundedBriefFollowupWriteBudget),
			after.Add(groundedBriefFollowupWriteBudget))
	}
}

func TestBriefFollowupDeadlineFailureStopsBeforeRequestWork(t *testing.T) {
	response := &briefFollowupDeadlineRecorder{
		ResponseRecorder: httptest.NewRecorder(),
		setErr:           errors.New("deadline unavailable"),
	}
	request := httptest.NewRequest(
		http.MethodPost, "/api/schedules/task/reports/not-a-number/ask", nil)
	request.SetPathValue("target_id", "not-a-number")

	(&server{}).handleBriefFollowup(
		response, request, store.GroundedBriefReport)

	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d; body=%s",
			response.Code, http.StatusServiceUnavailable, response.Body.String())
	}
	if len(response.deadlines) != 1 {
		t.Fatalf("SetWriteDeadline calls = %d, want 1", len(response.deadlines))
	}
	var payload map[string]string
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
		t.Fatalf("response is not JSON: %v", err)
	}
	if payload["error"] == "" {
		t.Fatalf("deadline failure did not return a structured error: %#v", payload)
	}
}

func TestBriefFollowupExecutionDeadlineReturnsStructuredUpstreamError(
	t *testing.T,
) {
	response := &briefFollowupDeadlineRecorder{
		ResponseRecorder: httptest.NewRecorder(),
	}
	ctx, cancel := context.WithDeadline(
		context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	request := httptest.NewRequest(
		http.MethodPost, "/api/schedules/task/reports/3/ask", nil,
	).WithContext(ctx)
	request.SetPathValue("target_id", "3")

	(&server{}).handleBriefFollowup(
		response, request, store.GroundedBriefReport)

	if response.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want %d; body=%s",
			response.Code, http.StatusBadGateway, response.Body.String())
	}
	var payload map[string]string
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
		t.Fatalf("response is not JSON: %v", err)
	}
	if payload["error"] != "简报追问处理超时，请稍后重试" {
		t.Fatalf("unexpected deadline response: %#v", payload)
	}
}

func TestBriefFollowupHandlerReusesOuterWriteDeadline(t *testing.T) {
	response := &briefFollowupDeadlineRecorder{
		ResponseRecorder: httptest.NewRecorder(),
	}
	want := time.Now().Add(groundedBriefFollowupWriteBudget)
	ctx := context.WithValue(
		context.Background(),
		briefFollowupWriteDeadlineContextKeyV1{},
		want,
	)
	if err := prepareBriefFollowupResponseV1(
		response, ctx, time.Now().Add(time.Minute),
	); err != nil {
		t.Fatal(err)
	}
	if len(response.deadlines) != 1 ||
		!response.deadlines[0].Equal(want) {
		t.Fatalf("handler deadline = %v, want exact outer deadline %s",
			response.deadlines, want)
	}
}

func TestBriefFollowupDeadlinePreservesClientCancellation(t *testing.T) {
	started := make(chan struct{})
	finished := make(chan struct{})
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(finished)
		if err := prepareBriefFollowupResponseV1(
			w, r.Context(), time.Now(),
		); err != nil {
			t.Errorf("prepare response: %v", err)
			return
		}
		close(started)
		<-r.Context().Done()
		// Exercise the disconnected write path too: it must return rather than
		// leave handler work alive until the seven-minute write deadline.
		writeError(w, http.StatusBadGateway, "request canceled")
	})
	testServer := httptest.NewServer(handler)
	t.Cleanup(testServer.Close)

	ctx, cancel := context.WithCancel(context.Background())
	request, err := http.NewRequestWithContext(
		ctx, http.MethodPost, testServer.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	requestDone := make(chan error, 1)
	go func() {
		response, requestErr := testServer.Client().Do(request)
		if response != nil {
			response.Body.Close()
		}
		requestDone <- requestErr
	}()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("handler did not start")
	}
	cancel()
	select {
	case requestErr := <-requestDone:
		if requestErr == nil {
			t.Fatal("canceled client request unexpectedly succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("client request did not observe cancellation")
	}
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("handler leaked after client cancellation/write failure")
	}
}

func TestExecutiveBriefTaskEnabledUsesExactRolloutScope(t *testing.T) {
	s := &server{deps: Deps{
		ExecutiveBriefWebCanaryScheduleID: "task-canary",
	}}
	if !s.executiveBriefTaskEnabled("task-canary") ||
		s.executiveBriefTaskEnabled("task-other") {
		t.Fatal("exact executive Brief rollout scope drifted")
	}
	s.deps.ExecutiveBriefWebCanaryScheduleID = ""
	if s.executiveBriefTaskEnabled("task-other") {
		t.Fatal("disabled executive Brief rollout remained visible")
	}
	s.deps.ExecutiveBriefWebProjectionAllowAll = true
	if s.executiveBriefTaskEnabled("task-other") ||
		!s.executiveBriefProjectionEnabled("task-other") ||
		s.executiveBriefProjectionEnabled("") {
		t.Fatal("projection allow-all widened interactive scope")
	}
}

func TestWebProjectionAllowAllKeepsInteractiveRoutesDark(t *testing.T) {
	s := &server{deps: Deps{
		ExecutiveBriefWebCanaryScheduleID:   "task-canary",
		ExecutiveBriefWebProjectionAllowAll: true,
	}}
	if !s.executiveBriefTaskEnabled("task-canary") ||
		!s.executiveBriefProjectionEnabled("task-canary") {
		t.Fatal("projection allow-all disabled exact interactive canary")
	}
	assertNotFound := func(
		name string,
		handler func(http.ResponseWriter, *http.Request),
		request *http.Request,
	) {
		t.Helper()
		recorder := httptest.NewRecorder()
		handler(recorder, request)
		if recorder.Code != http.StatusNotFound {
			t.Fatalf("%s status=%d body=%s",
				name, recorder.Code, recorder.Body.String())
		}
	}

	settingsRequest := httptest.NewRequest(
		http.MethodPatch, "/api/schedules/task-other/report-settings",
		strings.NewReader(`{"delivery":"web_only"}`))
	settingsRequest.SetPathValue("id", "task-other")
	assertNotFound(
		"settings", s.handlePatchBriefReportSettings, settingsRequest)

	groundingRequest := httptest.NewRequest(
		http.MethodGet, "/api/schedules/task-other/reports/3/grounding", nil)
	groundingRequest.SetPathValue("id", "task-other")
	groundingRequest.SetPathValue("target_id", "3")
	assertNotFound(
		"grounding", s.handlePeriodicBriefGrounding, groundingRequest)

	deepDiveRequest := httptest.NewRequest(
		http.MethodPost, "/api/schedules/task-other/reports/3/deep-dive",
		strings.NewReader(`{"insight_id":7}`))
	deepDiveRequest.SetPathValue("id", "task-other")
	deepDiveRequest.SetPathValue("target_id", "3")
	assertNotFound(
		"deep-dive", s.handlePeriodicBriefDeepDive, deepDiveRequest)

	followupRequest := httptest.NewRequest(
		http.MethodPost, "/api/schedules/task-other/reports/3/ask",
		strings.NewReader(`{"question":"依据是什么？"}`))
	followupRequest.SetPathValue("id", "task-other")
	followupRequest.SetPathValue("target_id", "3")
	followupRecorder := &briefFollowupDeadlineRecorder{
		ResponseRecorder: httptest.NewRecorder(),
	}
	s.handlePeriodicBriefFollowup(followupRecorder, followupRequest)
	if followupRecorder.Code != http.StatusNotFound {
		t.Fatalf("follow-up status=%d body=%s",
			followupRecorder.Code, followupRecorder.Body.String())
	}
}

func TestBriefPublicProjectionsDoNotExposeIntegrityMetadata(t *testing.T) {
	page := store.TaskBriefPageV1{
		Items: []store.TaskBriefItemV1{{
			ID: 1,
			Executive: &types.ExecutiveBriefArtifactV1{
				ID: 99,
				ExecutiveBriefArtifactDraftV1: types.ExecutiveBriefArtifactDraftV1{
					TenantID:      7,
					UserID:        8,
					RunSnapshotID: 9,
					ProfileDigest: strings.Repeat("a", 64),
					InputDigest:   strings.Repeat("b", 64),
					Content: types.ExecutiveBriefContentV1{
						Headline: "public",
					},
				},
			},
		}},
	}
	raw, err := json.Marshal(publicTaskBriefPageV1(page, true))
	if err != nil {
		t.Fatal(err)
	}
	darkRaw, err := json.Marshal(publicTaskBriefPageV1(page, false))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(darkRaw), `"executive"`) {
		t.Fatalf("dark rollout exposed executive Brief: %s", darkRaw)
	}
	text := string(raw)
	if !strings.Contains(text, `"signals":[]`) ||
		!strings.Contains(text, `"next_steps":[]`) ||
		strings.Contains(text, `"signals":null`) ||
		strings.Contains(text, `"next_steps":null`) {
		t.Fatalf("public Brief response exposed nullable arrays: %s", text)
	}
	for _, forbidden := range []string{
		"tenant_id", "user_id", "run_snapshot_id", "run_outcome_id",
		"profile_digest", "input_digest", "request_digest",
		"artifact_digest",
	} {
		if strings.Contains(text, `"`+forbidden+`"`) {
			t.Fatalf("public Brief response leaked %q: %s", forbidden, text)
		}
	}

	reportRaw, err := json.Marshal(publicPeriodicBriefReportPageV1(
		store.PeriodicBriefReportPageV1{
			Items: []types.PeriodicBriefReportV1{{
				ID: 2,
				PeriodicBriefReportDraftV1: types.PeriodicBriefReportDraftV1{
					TenantID:      7,
					UserID:        8,
					ProfileDigest: strings.Repeat("c", 64),
					InputDigest:   strings.Repeat("d", 64),
				},
			}},
		}))
	if err != nil {
		t.Fatal(err)
	}
	text = string(reportRaw)
	if !strings.Contains(text, `"signals":[]`) ||
		!strings.Contains(text, `"next_steps":[]`) ||
		strings.Contains(text, `"signals":null`) ||
		strings.Contains(text, `"next_steps":null`) {
		t.Fatalf("public report response exposed nullable arrays: %s", text)
	}
	for _, forbidden := range []string{
		"tenant_id", "user_id", "profile_epoch", "profile_version",
		"profile_digest", "input_digest", "inputs", "digest",
	} {
		if strings.Contains(text, `"`+forbidden+`"`) {
			t.Fatalf("public report response leaked %q: %s", forbidden, text)
		}
	}
}

func TestTaskHealthProjectionUsesControlledFailureCostAndExactRole(t *testing.T) {
	now := time.Date(2026, 7, 29, 18, 0, 0, 0, time.UTC)
	health := projectTaskHealthV1(
		&store.TaskLatestCheckV1{
			FinalizedAt:    now,
			Result:         types.RunResultFailed,
			SourceCoverage: types.RunCompletenessPartial,
			Processing:     types.RunCompletenessPartial,
			FailureCode:    string(types.CodeLLMRateLimit),
		},
		&store.ScheduleRunCost{
			LLMCostUSD:                 1.25,
			LLMCalls:                   3,
			LLMPricedCalls:             3,
			ToolCostUSD:                0.21,
			ToolCalls:                  3,
			ToolPricedCalls:            3,
			LatestAcquisitionCalls:     3,
			LatestAcquisitionFailures:  1,
			LatestAcquisitionErrorType: types.ToolErrHTTP,
		},
		types.MembershipRoleOwner,
		true,
	)
	if health.State != "attention" ||
		health.Issue != "model_temporarily_unavailable" ||
		health.RecommendedAction != "wait_for_retry" {
		t.Fatalf("health failure projection=%#v", health)
	}
	if health.Usage == nil ||
		health.Usage.Coverage != "llm_and_tools" ||
		health.Usage.KnownCostUSD != 1.46 ||
		health.Usage.ToolCalls == nil ||
		*health.Usage.ToolCalls != 3 {
		t.Fatalf("health usage projection=%#v", health.Usage)
	}
	if health.Acquisition.FailureReason !=
		taskhealth.AcquisitionFailureProviderV1 {
		t.Fatalf("health acquisition projection=%#v",
			health.Acquisition)
	}
	if !health.Permissions.CanRun ||
		!health.Permissions.CanPause ||
		!health.Permissions.CanEdit ||
		!health.Permissions.CanDelete ||
		!health.Permissions.CanViewUsage {
		t.Fatalf("owner health permissions=%#v", health.Permissions)
	}

	member := projectTaskHealthV1(
		nil,
		&store.ScheduleRunCost{LLMCostUSD: 99, LLMCalls: 8},
		types.MembershipRoleMember,
		true,
	)
	if member.Usage == nil ||
		!member.Permissions.CanRun ||
		!member.Permissions.CanEdit ||
		!member.Permissions.CanDelete ||
		!member.Permissions.CanViewUsage {
		t.Fatalf("task-owned authority drifted by membership label=%#v", member)
	}
}

func TestScheduleRunCostJSONNeverExposesInternalAcquisitionError(t *testing.T) {
	raw, err := json.Marshal(store.ScheduleRunCost{
		LatestAcquisitionCalls:     1,
		LatestAcquisitionFailures:  1,
		LatestAcquisitionErrorType: "postgres password=secret",
	})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "postgres") ||
		strings.Contains(string(raw), "error_type") {
		t.Fatalf("schedule detail leaked internal acquisition error: %s",
			raw)
	}
}

func TestTaskHealthMembershipRoleRequiresExactTenantAndUser(t *testing.T) {
	memberships := []types.Membership{
		{
			TenantID: 7,
			UserID:   11,
			Role:     types.MembershipRoleAdmin,
		},
		{
			TenantID: 8,
			UserID:   12,
			Role:     types.MembershipRoleOwner,
		},
	}
	if got := membershipRoleForTenantV1(memberships, 7, 11); got != types.MembershipRoleAdmin {
		t.Fatalf("exact membership role=%q", got)
	}
	if got := membershipRoleForTenantV1(memberships, 7, 12); got != "" {
		t.Fatalf("cross-user membership escaped=%q", got)
	}
	if got := membershipRoleForTenantV1(memberships, 8, 11); got != "" {
		t.Fatalf("cross-tenant membership escaped=%q", got)
	}
}

func TestGroundedContextDeepDiveRequiresExactFrozenReference(t *testing.T) {
	contextValue := store.GroundedBriefContextV1{
		Content: types.ExecutiveBriefContentV1{
			NextSteps: []types.ExecutiveNextStepV1{{
				Kind: types.ExecutiveNextStepDeepDive,
				EvidenceRefs: []types.ExecutiveEvidenceRefV1{{
					InsightID: 41, ClaimIndexes: []int{0},
				}},
			}, {
				Kind: types.ExecutiveNextStepEditTask,
				EvidenceRefs: []types.ExecutiveEvidenceRefV1{{
					InsightID: 42, ClaimIndexes: []int{0},
				}},
			}},
		},
		Evidence: []store.GroundedEvidenceBriefV1{{
			BriefID: 7,
			Insights: []store.TaskBriefInsightV1{
				{ID: 41}, {ID: 42},
			},
		}},
	}
	if !groundedContextAllowsDeepDiveV1(contextValue, 41) {
		t.Fatal("exact frozen deep-dive evidence was rejected")
	}
	for _, denied := range []int64{0, 42, 99} {
		if groundedContextAllowsDeepDiveV1(contextValue, denied) {
			t.Fatalf("non-deep-dive Insight %d was accepted", denied)
		}
	}
	contextValue.Evidence = nil
	if groundedContextAllowsDeepDiveV1(contextValue, 41) {
		t.Fatal("missing immutable evidence accepted deep-dive")
	}
}

func TestBriefFollowupGroundingOmitsInternalProvenance(t *testing.T) {
	published := time.Date(2026, 7, 29, 3, 12, 39, 0, time.UTC)
	contextValue := store.GroundedBriefContextV1{
		Kind:           store.GroundedBriefReport,
		ID:             9001,
		Cadence:        "daily",
		PeriodStart:    "2026-07-28T00:00:00Z",
		PeriodEnd:      "2026-07-29T00:00:00Z",
		SourceCoverage: types.RunCompletenessComplete,
		Processing:     types.RunCompletenessPartial,
		GenerationMode: types.ExecutiveGenerationFallback,
		Content: types.ExecutiveBriefContentV1{
			Headline:         "关注成本变化",
			ExecutiveSummary: "两期内容指向同一变化。",
			DecisionState:    types.ExecutiveDecisionWatch,
			WhyForYou:        "可能影响模型预算。",
			Signals: []types.ExecutiveSignalV1{{
				Kind:      types.ExecutiveSignalTrend,
				Lifecycle: types.ExecutiveSignalPersistent,
				Title:     "成本效率持续改善",
				Summary:   "两期均提到更低估算成本。",
				EvidenceRefs: []types.ExecutiveEvidenceRefV1{{
					BriefID: 77, InsightID: 88,
					ClaimIndexes: []int{0},
				}},
			}},
			NextSteps: []types.ExecutiveNextStepV1{{
				Kind:      types.ExecutiveNextStepDeepDive,
				Label:     "查看成本数据",
				Rationale: "核对是否影响当前选择。",
				EvidenceRefs: []types.ExecutiveEvidenceRefV1{{
					BriefID: 77, InsightID: 88,
					ClaimIndexes: []int{0},
				}},
			}},
		},
		Evidence: []store.GroundedEvidenceBriefV1{{
			BriefID: 77, GeneratedAt: published.Add(2 * time.Hour),
			Insights: []store.TaskBriefInsightV1{{
				ID:           88,
				RankPosition: 1,
				Title:        "GPT-5.6 Luna 一般可用",
				BodyMD:       "Luna 强调 source-1 成本效率。",
				SourceTitle:  "OpenAI",
				SourceURL:    "https://openai.com/index/gpt-5-6/",
				PublishedAt:  &published,
				DiscoveredAt: published.Add(time.Hour),
				Structured: &store.TaskBriefStructuredInsightV1{
					SchemaVersion:    "internal-schema",
					BodyMD:           "Luna 强调成本效率。",
					WhatChanged:      "Luna 已一般可用。",
					WhyItMatters:     "profile_epoch=3",
					ImportanceReason: "属于正式模型更新。",
					Claims: []types.StructuredClaimV1{{
						Text:       "Luna 是成本效率型号。",
						Excerpt:    "raw frozen excerpt",
						SourceRefs: []string{"source-1"},
					}},
				},
			}},
		}},
	}
	grounding, err := renderBriefFollowupGroundingV1(contextValue)
	if err != nil {
		t.Fatal(err)
	}
	for _, wanted := range []string{
		"GPT-5.6 Luna 一般可用", "Luna 是成本效率型号。",
		"raw frozen excerpt", "https://openai.com/index/gpt-5-6/",
		"不得输出数据库编号", `"来源覆盖":"完整"`,
		`"处理覆盖":"不完整"`, `"期次":1`,
		`"情报序号":1`, `"事实序号":[1]`,
		groundedHiddenTextV1,
	} {
		if !strings.Contains(grounding, wanted) {
			t.Fatalf("grounding omitted %q: %s", wanted, grounding)
		}
	}
	for _, forbidden := range []string{
		"9001", "77", "88", "brief_id", "insight_id",
		"claim_indexes", "source-1",
		"internal-schema", "generation_mode", "profile_epoch",
		"deterministic_fallback", "rank_position", "discovered_at",
		"xsec_token", "temporary-secret", "#luna",
	} {
		if strings.Contains(grounding, forbidden) {
			t.Fatalf("grounding leaked %q: %s", forbidden, grounding)
		}
	}
}

func TestBriefFollowupSafeSourceURLV1(t *testing.T) {
	for _, tc := range []struct {
		name string
		url  string
		want string
	}{
		{
			name: "clean exact URL",
			url:  "https://openai.com/index/gpt-5-6/",
			want: "https://openai.com/index/gpt-5-6/",
		},
		{
			name: "query may identify another resource",
			url:  "https://example.com/article?id=123",
		},
		{
			name: "fragment may identify another resource",
			url:  "https://example.com/article#section",
		},
		{
			name: "userinfo must not reach the model",
			url:  "https://secret@example.com/article",
		},
		{
			name: "internal field in URL",
			url:  "https://example.com/brief_id/7",
		},
		{
			name: "unsupported scheme",
			url:  "file:///tmp/evidence",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := briefFollowupSafeSourceURLV1(tc.url); got != tc.want {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestGroundedReplyInternalReferenceGuardFailsClosed(t *testing.T) {
	for _, reply := range []string{
		"依据 brief_id: 7 和 insight_id: 9。",
		"brief IDs 分别是 7 和 9。",
		"详情见 source ref source-1。",
		"证据标签是 source-2。",
		"详情见 source reference。",
		"claim_indexes 为 0。",
		"claim indices 为 0。",
		"report_id 3 对应 request_digest。",
		"profile_epoch 为 3。",
		"tenant_id 与 workflow_id 不应展示。",
		"discovered_at 是 2026-07-29。",
	} {
		if !groundedInternalReferenceV1.MatchString(reply) {
			t.Fatalf("internal reference was not blocked: %q", reply)
		}
	}
	for _, reply := range []string{
		"这份日报依据两期简报：GPT-5.6 正式发布与 Luna 一般可用。",
		"来源覆盖不完整，因此暂不形成跨期趋势。",
		"这是每日 news digest，公开采用 schema version 2 和 workflow ID。",
	} {
		if groundedInternalReferenceV1.MatchString(reply) {
			t.Fatalf("user-facing answer was blocked: %q", reply)
		}
	}
}
