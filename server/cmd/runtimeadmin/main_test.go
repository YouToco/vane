package main

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/YouToco/vane/server/store"
	"github.com/YouToco/vane/server/types"
)

func TestBaselineExitCode(t *testing.T) {
	tests := []struct {
		name   string
		mode   store.TaskDefinitionBaselineMode
		status store.TaskDefinitionBaselineStatus
		want   int
	}{
		{
			name: "verified returns zero",
			mode: store.TaskDefinitionBaselineVerify, status: store.TaskDefinitionBaselineVerified,
			want: exitOK,
		},
		{
			name: "needs apply returns verify failure",
			mode: store.TaskDefinitionBaselineVerify, status: store.TaskDefinitionBaselineNeedsApply,
			want: exitVerifyFailed,
		},
		{
			name: "unsupported returns verify failure",
			mode: store.TaskDefinitionBaselineVerify, status: store.TaskDefinitionBaselineUnsupported,
			want: exitVerifyFailed,
		},
		{
			name: "deleted returns verify failure",
			mode: store.TaskDefinitionBaselineVerify, status: store.TaskDefinitionBaselineDeleted,
			want: exitVerifyFailed,
		},
		{
			name: "dry run does not apply strict verify exit",
			mode: store.TaskDefinitionBaselineDryRun, status: store.TaskDefinitionBaselineWouldApply,
			want: exitOK,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := baselineExitCode(tt.mode, store.TaskDefinitionBaselinePage{
				Items: []store.TaskDefinitionBaselineResult{{
					TaskID: "task", Status: tt.status,
				}},
			})
			if got != tt.want {
				t.Fatalf("baselineExitCode=%d want=%d", got, tt.want)
			}
		})
	}
}

func TestFinishBaselineRunOutputsJSONBeforeStrictVerifyFailure(t *testing.T) {
	page := store.TaskDefinitionBaselinePage{
		Items: []store.TaskDefinitionBaselineResult{{
			TenantID: 1, UserID: 2, TaskID: "task",
			Status: store.TaskDefinitionBaselineNeedsApply,
		}},
	}
	var stdout, stderr bytes.Buffer
	exitCode := finishBaselineRun(
		&stdout, &stderr, store.TaskDefinitionBaselineVerify, page, nil)
	if exitCode != exitVerifyFailed {
		t.Fatalf("exit=%d want=%d", exitCode, exitVerifyFailed)
	}
	if !strings.Contains(stdout.String(), `"status": "needs_apply"`) {
		t.Fatalf("stdout did not retain JSON report: %q", stdout.String())
	}
	if stderr.Len() != 0 {
		t.Fatalf("strict verify wrote stderr: %q", stderr.String())
	}
}

func TestBaselineExitCodeRequiresEveryVerifyPage(t *testing.T) {
	page1 := store.TaskDefinitionBaselinePage{
		Items: []store.TaskDefinitionBaselineResult{{
			TaskID: "first", Status: store.TaskDefinitionBaselineVerified,
		}},
		Next: &store.TaskDefinitionBaselineCursor{
			TenantID: 1, UserID: 2, TaskID: "first",
		},
	}
	page2 := store.TaskDefinitionBaselinePage{
		Items: []store.TaskDefinitionBaselineResult{{
			TaskID: "second", Status: store.TaskDefinitionBaselineNeedsApply,
		}},
	}
	if got := baselineExitCode(
		store.TaskDefinitionBaselineVerify, page1); got != exitVerifyMorePages {
		t.Fatalf("limit=1 first page exit=%d want=%d", got, exitVerifyMorePages)
	}
	if got := baselineExitCode(
		store.TaskDefinitionBaselineVerify, page2); got != exitVerifyFailed {
		t.Fatalf("limit=1 second page exit=%d want=%d", got, exitVerifyFailed)
	}
}

func TestFinishBaselineRunSanitizesStoreError(t *testing.T) {
	const secret = "postgres://user:password@private/database"
	runErr := types.NewAppError(
		types.CodeDatabase, "baseline database unavailable", errors.New(secret))
	var stdout, stderr bytes.Buffer
	exitCode := finishBaselineRun(
		&stdout, &stderr, store.TaskDefinitionBaselineVerify,
		store.TaskDefinitionBaselinePage{}, runErr)
	if exitCode != exitFailure {
		t.Fatalf("exit=%d want=%d", exitCode, exitFailure)
	}
	if stdout.Len() != 0 {
		t.Fatalf("store error wrote stdout: %q", stdout.String())
	}
	if !strings.Contains(stderr.String(), "baseline database unavailable") ||
		strings.Contains(stderr.String(), secret) {
		t.Fatalf("unsafe store error output: %q", stderr.String())
	}
}

func TestFinishBaselineRunSanitizesEncodingError(t *testing.T) {
	const secret = "encoder leaked payload"
	var stderr bytes.Buffer
	exitCode := finishBaselineRun(
		errorWriter{err: errors.New(secret)}, &stderr,
		store.TaskDefinitionBaselineVerify,
		store.TaskDefinitionBaselinePage{}, nil)
	if exitCode != exitFailure {
		t.Fatalf("exit=%d want=%d", exitCode, exitFailure)
	}
	if !strings.Contains(stderr.String(), "输出结果失败") ||
		strings.Contains(stderr.String(), secret) {
		t.Fatalf("unsafe encoding error output: %q", stderr.String())
	}
}

func TestFinishSnapshotShadowRunStrictExit(t *testing.T) {
	next := int64(7)
	tests := []struct {
		name          string
		page          store.TaskRunSnapshotShadowAuditPage
		expectedCount int
		want          int
	}{
		{name: "empty", expectedCount: 1, want: exitVerifyFailed},
		{name: "match", page: store.TaskRunSnapshotShadowAuditPage{
			Items: []store.TaskRunSnapshotShadowAuditItem{{
				SnapshotID: 1, Status: store.TaskRunSnapshotShadowMatch,
				TypedAuditStatus: store.CompiledRunSnapshotV2AuditMatch,
				TypedEqual:       true,
			}},
		}, expectedCount: 1, want: exitOK},
		{name: "missing", page: store.TaskRunSnapshotShadowAuditPage{
			Items: []store.TaskRunSnapshotShadowAuditItem{{
				SnapshotID: 1, Status: "missing",
			}},
		}, expectedCount: 1, want: exitVerifyFailed},
		{name: "headless", page: store.TaskRunSnapshotShadowAuditPage{
			Items: []store.TaskRunSnapshotShadowAuditItem{{
				SnapshotID: 1, Status: store.TaskRunSnapshotShadowHeadless,
			}},
		}, expectedCount: 1, want: exitVerifyFailed},
		{name: "legacy", page: store.TaskRunSnapshotShadowAuditPage{
			Items: []store.TaskRunSnapshotShadowAuditItem{{
				SnapshotID: 1, Status: store.TaskRunSnapshotShadowLegacyCompatible,
			}},
		}, expectedCount: 1, want: exitVerifyFailed},
		{name: "next", page: store.TaskRunSnapshotShadowAuditPage{
			Items: []store.TaskRunSnapshotShadowAuditItem{{
				SnapshotID: 1, Status: store.TaskRunSnapshotShadowMatch,
				TypedAuditStatus: store.CompiledRunSnapshotV2AuditMatch,
				TypedEqual:       true,
			}}, Next: &next,
		}, expectedCount: 1, want: exitVerifyMorePages},
		{name: "typed mismatch", page: store.TaskRunSnapshotShadowAuditPage{
			Items: []store.TaskRunSnapshotShadowAuditItem{{
				SnapshotID: 1, Status: store.TaskRunSnapshotShadowMatch,
				TypedAuditStatus: store.CompiledRunSnapshotV2AuditTypedMismatch,
			}},
		}, expectedCount: 1, want: exitVerifyFailed},
		{name: "count mismatch", page: store.TaskRunSnapshotShadowAuditPage{
			Items: []store.TaskRunSnapshotShadowAuditItem{
				{
					SnapshotID: 1, Status: store.TaskRunSnapshotShadowMatch,
					TypedAuditStatus: store.CompiledRunSnapshotV2AuditMatch,
					TypedEqual:       true,
				},
				{
					SnapshotID: 2, Status: store.TaskRunSnapshotShadowMatch,
					TypedAuditStatus: store.CompiledRunSnapshotV2AuditMatch,
					TypedEqual:       true,
				},
			},
		}, expectedCount: 1, want: exitVerifyFailed},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			if got := finishSnapshotShadowRun(
				&stdout, &stderr, test.page, test.expectedCount, nil); got != test.want {
				t.Fatalf("exit=%d want=%d", got, test.want)
			}
			if stdout.Len() == 0 || stderr.Len() != 0 {
				t.Fatalf("stdout/stderr = %q/%q", stdout.String(), stderr.String())
			}
		})
	}
}

func TestCollectSnapshotShadowAuditScansFrozenPrefix(t *testing.T) {
	firstCursor := int64(1)
	var afters []int64
	load := func(
		_ context.Context,
		_ string,
		_ time.Time,
		afterID int64,
		_ int64,
		limit int,
	) (store.TaskRunSnapshotShadowAuditPage, error) {
		afters = append(afters, afterID)
		if limit != 1 {
			t.Fatalf("limit = %d, want 1", limit)
		}
		if afterID == 0 {
			return store.TaskRunSnapshotShadowAuditPage{
				Items: []store.TaskRunSnapshotShadowAuditItem{{
					SnapshotID: 1, Status: store.TaskRunSnapshotShadowProjectionMismatch,
					TypedAuditStatus: store.CompiledRunSnapshotV2AuditNonMatch,
				}},
				Next: &firstCursor,
			}, nil
		}
		if afterID != firstCursor {
			t.Fatalf("unexpected cursor %d", afterID)
		}
		return store.TaskRunSnapshotShadowAuditPage{
			Items: []store.TaskRunSnapshotShadowAuditItem{{
				SnapshotID: 2, Status: store.TaskRunSnapshotShadowMatch,
				TypedAuditStatus: store.CompiledRunSnapshotV2AuditMatch,
				TypedEqual:       true,
			}},
		}, nil
	}
	page, err := collectSnapshotShadowAudit(
		t.Context(), "task", time.Now(), 2, 1, 2, load)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 2 || len(afters) != 2 ||
		afters[0] != 0 || afters[1] != 1 {
		t.Fatalf("collected page/afters = %+v/%v", page, afters)
	}
	var stdout, stderr bytes.Buffer
	if got := finishSnapshotShadowRun(
		&stdout, &stderr, page, 2, nil); got != exitVerifyFailed {
		t.Fatalf("bad frozen prefix exit = %d, want %d", got, exitVerifyFailed)
	}
}

func TestCollectSnapshotShadowAuditLimitOneExpectedTwoPasses(t *testing.T) {
	cursor := int64(1)
	load := func(
		_ context.Context,
		_ string,
		_ time.Time,
		afterID int64,
		_ int64,
		_ int,
	) (store.TaskRunSnapshotShadowAuditPage, error) {
		item := store.TaskRunSnapshotShadowAuditItem{
			SnapshotID: afterID + 1, Status: store.TaskRunSnapshotShadowMatch,
			TypedAuditStatus: store.CompiledRunSnapshotV2AuditMatch,
			TypedEqual:       true,
		}
		if afterID == 0 {
			return store.TaskRunSnapshotShadowAuditPage{
				Items: []store.TaskRunSnapshotShadowAuditItem{item},
				Next:  &cursor,
			}, nil
		}
		return store.TaskRunSnapshotShadowAuditPage{
			Items: []store.TaskRunSnapshotShadowAuditItem{item},
		}, nil
	}
	page, err := collectSnapshotShadowAudit(
		t.Context(), "task", time.Now(), 2, 1, 2, load)
	if err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if got := finishSnapshotShadowRun(
		&stdout, &stderr, page, 2, nil); got != exitOK {
		t.Fatalf("complete two-page sample exit = %d, want %d", got, exitOK)
	}
}

func TestRunSnapshotShadowRejectsCallerControlledBounds(t *testing.T) {
	for _, flag := range []string{"-after-id", "-through-id"} {
		t.Run(flag, func(t *testing.T) {
			if got := runSnapshotShadow([]string{flag, "1"}); got != exitFailure {
				t.Fatalf("%s escape exit = %d, want %d", flag, got, exitFailure)
			}
		})
	}
}

func TestRunSnapshotCutoverRequiresExactIdentityAndConfirmation(t *testing.T) {
	for name, args := range map[string][]string{
		"missing identity": {"-action", "status"},
		"invalid action": {
			"-tenant", "1", "-user", "2", "-task", "task",
			"-action", "apply",
		},
		"activate unconfirmed": {
			"-tenant", "1", "-user", "2", "-task", "task",
			"-action", "activate",
		},
		"rollback unconfirmed": {
			"-tenant", "1", "-user", "2", "-task", "task",
			"-action", "rollback",
		},
		"positional carrier": {
			"-tenant", "1", "-user", "2", "-task", "task",
			"-action", "status", "carrier",
		},
	} {
		t.Run(name, func(t *testing.T) {
			if got := runSnapshotCutover(args); got != exitFailure {
				t.Fatalf("exit=%d want=%d", got, exitFailure)
			}
		})
	}
}

func TestParseAgentSessionCutoverRequiresExactIdentityAndConfirmation(
	t *testing.T,
) {
	for name, args := range map[string][]string{
		"missing identity": {"-action", "status"},
		"zero session": {
			"-tenant", "1", "-user", "2", "-session", "0",
			"-action", "status",
		},
		"invalid action": {
			"-tenant", "1", "-user", "2", "-session", "3",
			"-action", "apply",
		},
		"activate unconfirmed": {
			"-tenant", "1", "-user", "2", "-session", "3",
			"-action", "activate",
		},
		"rollback unconfirmed": {
			"-tenant", "1", "-user", "2", "-session", "3",
			"-action", "rollback",
		},
		"status with write confirmation": {
			"-tenant", "1", "-user", "2", "-session", "3",
			"-action", "status", "-confirm-cutover",
		},
		"positional carrier": {
			"-tenant", "1", "-user", "2", "-session", "3",
			"-action", "status", "carrier",
		},
	} {
		t.Run(name, func(t *testing.T) {
			var stderr bytes.Buffer
			if _, ok := parseAgentSessionCutoverOptions(
				args, &stderr,
			); ok {
				t.Fatalf("options unexpectedly accepted: %v", args)
			}
			if stderr.Len() == 0 {
				t.Fatal("rejection did not explain safe usage")
			}
		})
	}
	for _, action := range []agentSessionCutoverAction{
		agentSessionCutoverStatus,
		agentSessionCutoverActivate,
		agentSessionCutoverRollback,
	} {
		t.Run("valid "+string(action), func(t *testing.T) {
			args := []string{
				"-tenant", "1", "-user", "2", "-session", "3",
				"-action", string(action),
			}
			if action != agentSessionCutoverStatus {
				args = append(args, "-confirm-cutover")
			}
			var stderr bytes.Buffer
			opts, ok := parseAgentSessionCutoverOptions(args, &stderr)
			if !ok || stderr.Len() != 0 ||
				opts.TenantID != 1 || opts.UserID != 2 ||
				opts.SessionID != 3 || opts.Action != action {
				t.Fatalf("options=%+v ok=%v stderr=%q",
					opts, ok, stderr.String())
			}
		})
	}
}

func TestExecuteAgentSessionCutoverRoutesOnlyDurableAuthorityAPI(
	t *testing.T,
) {
	for _, action := range []agentSessionCutoverAction{
		agentSessionCutoverStatus,
		agentSessionCutoverActivate,
		agentSessionCutoverRollback,
	} {
		t.Run(string(action), func(t *testing.T) {
			fake := &fakeAgentSessionCutoverStore{
				output: store.AgentSessionProjectionAuthorityStatus{
					TenantID: 1, UserID: 2, SessionID: 3,
					Route:      store.AgentSessionProjectionRouteLedger,
					Generation: 4, EventID: 5, LedgerHeadSequence: 6,
				},
			}
			output, err := executeAgentSessionCutover(
				t.Context(), fake, agentSessionCutoverOptions{
					TenantID: 1, UserID: 2, SessionID: 3,
					Action:  action,
					Confirm: action != agentSessionCutoverStatus,
				})
			if err != nil {
				t.Fatal(err)
			}
			wantCall := "control"
			if action == agentSessionCutoverStatus {
				wantCall = "status"
			}
			if fake.call != wantCall || fake.tenantID != 1 ||
				fake.userID != 2 || fake.sessionID != 3 ||
				(action != agentSessionCutoverStatus &&
					string(fake.action) != string(action)) ||
				output.Route != "ledger" ||
				output.LedgerHeadSequence != 6 {
				t.Fatalf("fake/output=%+v/%+v", fake, output)
			}
		})
	}
}

func TestExecuteAgentSessionCutoverRevalidatesScopeAndConfirmation(
	t *testing.T,
) {
	tests := []agentSessionCutoverOptions{
		{
			TenantID: 0, UserID: 2, SessionID: 3,
			Action: agentSessionCutoverStatus,
		},
		{
			TenantID: 1, UserID: 2, SessionID: 3,
			Action: agentSessionCutoverActivate,
		},
		{
			TenantID: 1, UserID: 2, SessionID: 3,
			Action: agentSessionCutoverRollback,
		},
		{
			TenantID: 1, UserID: 2, SessionID: 3,
			Action: agentSessionCutoverStatus, Confirm: true,
		},
		{
			TenantID: 1, UserID: 2, SessionID: 3,
			Action: "unknown", Confirm: true,
		},
	}
	for _, opts := range tests {
		fake := &fakeAgentSessionCutoverStore{}
		if _, err := executeAgentSessionCutover(
			t.Context(), fake, opts,
		); !errors.Is(err, types.ErrValidation) {
			t.Fatalf("opts=%+v error=%v, want validation", opts, err)
		}
		if fake.call != "" {
			t.Fatalf("opts=%+v reached Store via %q", opts, fake.call)
		}
	}
}

type fakeAgentSessionCutoverStore struct {
	call      string
	tenantID  int64
	userID    int64
	sessionID int64
	action    store.AgentSessionProjectionAuthorityAction
	output    store.AgentSessionProjectionAuthorityStatus
	err       error
}

func (f *fakeAgentSessionCutoverStore) GetAgentSessionProjectionAuthorityStatus(
	_ context.Context,
	tenantID int64,
	userID int64,
	sessionID int64,
) (store.AgentSessionProjectionAuthorityStatus, error) {
	f.call, f.tenantID, f.userID, f.sessionID =
		"status", tenantID, userID, sessionID
	return f.output, f.err
}

func (f *fakeAgentSessionCutoverStore) ControlAgentSessionProjectionAuthority(
	_ context.Context,
	tenantID int64,
	userID int64,
	sessionID int64,
	action store.AgentSessionProjectionAuthorityAction,
) (store.AgentSessionProjectionAuthorityStatus, error) {
	f.call, f.tenantID, f.userID, f.sessionID =
		"control", tenantID, userID, sessionID
	f.action = action
	return f.output, f.err
}

func TestFinishAgentSessionCutoverRunOutputsOnlySafeCarrier(t *testing.T) {
	result := agentSessionCutoverOutput{
		TenantID: 1, UserID: 2, SessionID: 3,
		Route: "ledger", Generation: 4, EventID: 5,
		LedgerHeadSequence: 6,
	}
	var stdout, stderr bytes.Buffer
	if got := finishAgentSessionCutoverRun(
		&stdout, &stderr, result, nil,
	); got != exitOK {
		t.Fatalf("exit=%d want=%d", got, exitOK)
	}
	output := stdout.String()
	for _, want := range []string{
		`"session_id": 3`, `"route": "ledger"`, `"generation": 4`,
		`"ledger_head_sequence": 6`,
	} {
		if !strings.Contains(output, want) {
			t.Errorf("JSON missing %q: %s", want, output)
		}
	}
	for _, forbidden := range []string{
		"messages", "payload", "prompt", "content", "tool",
		"activated_tools", "legacy_replica",
	} {
		if strings.Contains(output, forbidden) {
			t.Errorf("JSON leaked forbidden field %q: %s",
				forbidden, output)
		}
	}
	if stderr.Len() != 0 {
		t.Fatalf("stderr=%q", stderr.String())
	}
}

func TestFinishAgentSessionCutoverRunSanitizesStoreError(t *testing.T) {
	const secret = `session body: [{"role":"user","content":"secret"}]`
	runErr := types.NewAppError(
		types.CodeConflict, "agent session cutover rejected",
		errors.New(secret))
	var stdout, stderr bytes.Buffer
	if got := finishAgentSessionCutoverRun(
		&stdout, &stderr, agentSessionCutoverOutput{}, runErr,
	); got != exitFailure {
		t.Fatalf("exit=%d want=%d", got, exitFailure)
	}
	if stdout.Len() != 0 ||
		!strings.Contains(stderr.String(), "agent session cutover rejected") ||
		strings.Contains(stderr.String(), secret) {
		t.Fatalf("unsafe output stdout/stderr=%q/%q",
			stdout.String(), stderr.String())
	}
}

func TestFinishSnapshotCutoverRunOutputsOnlySafeCarrier(t *testing.T) {
	result := store.TaskRunSnapshotCutoverResult{
		TenantID: 1, UserID: 2, TaskID: "task", EventID: 3,
		Generation: 4, Action: store.TaskRunSnapshotCutoverActivate,
		ApprovedDefinitionVersion: 5,
		SnapshotHighWatermark:     6,
		AuditFromSnapshotID:       1,
		AuditCount:                6,
		AuditThroughID:            6,
	}
	var stdout, stderr bytes.Buffer
	if got := finishSnapshotCutoverRun(
		&stdout, &stderr, result, nil); got != exitOK {
		t.Fatalf("exit=%d want=%d", got, exitOK)
	}
	output := stdout.String()
	for _, want := range []string{
		`"task_id": "task"`, `"action": "activate"`,
		`"audit_count": 6`,
	} {
		if !strings.Contains(output, want) {
			t.Errorf("JSON missing %q: %s", want, output)
		}
	}
	for _, forbidden := range []string{
		"payload", "playbook", "prompt", "url", "config",
		"definition_digest",
	} {
		if strings.Contains(output, forbidden) {
			t.Errorf("JSON leaked forbidden field %q: %s", forbidden, output)
		}
	}
	if stderr.Len() != 0 {
		t.Fatalf("stderr=%q", stderr.String())
	}
}

func TestFinishSnapshotCutoverRunSanitizesStoreError(t *testing.T) {
	const secret = "postgres://owner:secret@private/db"
	runErr := types.NewAppError(
		types.CodeDatabase, "snapshot cutover unavailable",
		errors.New(secret))
	var stdout, stderr bytes.Buffer
	if got := finishSnapshotCutoverRun(
		&stdout, &stderr, nil, runErr); got != exitFailure {
		t.Fatalf("exit=%d want=%d", got, exitFailure)
	}
	if stdout.Len() != 0 ||
		!strings.Contains(stderr.String(), "snapshot cutover unavailable") ||
		strings.Contains(stderr.String(), secret) {
		t.Fatalf("unsafe output stdout/stderr=%q/%q",
			stdout.String(), stderr.String())
	}
}

type errorWriter struct {
	err error
}

func (w errorWriter) Write([]byte) (int, error) {
	return 0, w.err
}
