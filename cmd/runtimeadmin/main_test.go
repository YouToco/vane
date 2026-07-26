package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/YouToco/vane/store"
	"github.com/YouToco/vane/types"
)

func TestParseLegacyBatch63RepairOptionsIsPhysicallyPinned(t *testing.T) {
	digest := strings.Repeat("a", 64)
	expiresAt := "2026-07-25T13:00:00Z"
	tests := []struct {
		name string
		args []string
		ok   bool
	}{
		{
			name: "preview",
			args: []string{
				"-action", "preview", "-evidence", "proof.json",
				"-expires-at", expiresAt,
			},
			ok: true,
		},
		{
			name: "apply exact confirmed plan",
			args: []string{
				"-action", "apply", "-evidence", "proof.json",
				"-expires-at", expiresAt, "-plan-digest", digest,
				"-confirm-apply",
			},
			ok: true,
		},
		{name: "verify", args: []string{"-action", "verify"}, ok: true},
		{
			name: "abort exact confirmed plan",
			args: []string{
				"-action", "abort", "-plan-digest", digest, "-confirm-abort",
			},
			ok: true,
		},
		{
			name: "reject batch selector",
			args: []string{"-action", "verify", "-batch", "63"},
		},
		{
			name: "reject positional carrier",
			args: []string{"-action", "verify", "63"},
		},
		{
			name: "reject unconfirmed apply",
			args: []string{
				"-action", "apply", "-evidence", "proof.json",
				"-expires-at", expiresAt, "-plan-digest", digest,
			},
		},
		{
			name: "reject unconfirmed abort",
			args: []string{"-action", "abort", "-plan-digest", digest},
		},
		{
			name: "reject evidence on abort",
			args: []string{
				"-action", "abort", "-evidence", "proof.json",
				"-plan-digest", digest, "-confirm-abort",
			},
		},
		{
			name: "reject digest on preview",
			args: []string{
				"-action", "preview", "-evidence", "proof.json",
				"-expires-at", expiresAt, "-plan-digest", digest,
			},
		},
		{
			name: "reject uppercase digest",
			args: []string{
				"-action", "apply", "-evidence", "proof.json",
				"-expires-at", expiresAt,
				"-plan-digest", strings.Repeat("A", 64), "-confirm-apply",
			},
		},
		{
			name: "reject missing preview expiry",
			args: []string{"-action", "preview", "-evidence", "proof.json"},
		},
		{
			name: "reject malformed expiry",
			args: []string{
				"-action", "preview", "-evidence", "proof.json",
				"-expires-at", "tomorrow",
			},
		},
		{
			name: "reject expiry carrier on verify",
			args: []string{
				"-action", "verify", "-expires-at", expiresAt,
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var stderr bytes.Buffer
			_, ok := parseLegacyBatch63RepairOptions(test.args, &stderr)
			if ok != test.ok {
				t.Fatalf("ok=%t want=%t stderr=%q", ok, test.ok, stderr.String())
			}
		})
	}
}

func TestReadLegacyBatch63EvidenceIsBoundedAndSanitized(t *testing.T) {
	dir := t.TempDir()
	validPath := filepath.Join(dir, "evidence.json")
	if err := os.WriteFile(validPath, []byte(`{"proof":"bound"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	raw, err := readLegacyBatch63Evidence(validPath)
	if err != nil || string(raw) != `{"proof":"bound"}` {
		t.Fatalf("raw/err=%q/%v", raw, err)
	}

	oversizePath := filepath.Join(dir, "oversize.json")
	if err := os.WriteFile(
		oversizePath,
		bytes.Repeat([]byte("x"), maxLegacyBatch63EvidenceBytes+1),
		0o600,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := readLegacyBatch63Evidence(oversizePath); err == nil ||
		strings.Contains(err.Error(), oversizePath) {
		t.Fatalf("oversize error leaked path or was nil: %v", err)
	}
	const secretPath = "operator-secret-proof.json"
	if _, err := readLegacyBatch63Evidence(
		filepath.Join(dir, secretPath)); err == nil ||
		strings.Contains(err.Error(), secretPath) {
		t.Fatalf("missing-file error leaked path or was nil: %v", err)
	}
}

func TestFinishLegacyBatch63RepairRunOutputsOnlySafeCarrier(t *testing.T) {
	enableBy := time.Date(2026, 7, 25, 12, 5, 0, 0, time.UTC)
	expiresAt := time.Date(2026, 7, 25, 13, 0, 0, 0, time.UTC)
	output := legacyBatch63RepairOutput{
		Phase: "finalized", PlanDigest: strings.Repeat("a", 64),
		EnableBy: &enableBy, ExpiresAt: &expiresAt, Remaining: 3300,
	}
	var stdout, stderr bytes.Buffer
	if got := finishLegacyBatch63RepairRun(
		&stdout, &stderr, output, nil); got != exitOK {
		t.Fatalf("exit=%d stderr=%q", got, stderr.String())
	}
	for _, expected := range []string{
		`"phase": "finalized"`,
		`"plan_digest": "`,
		`"enable_by": "2026-07-25T12:05:00Z"`,
		`"expires_at": "2026-07-25T13:00:00Z"`,
		`"remaining": 3300`,
	} {
		if !strings.Contains(stdout.String(), expected) {
			t.Errorf("output missing %q: %s", expected, stdout.String())
		}
	}
	for _, forbidden := range []string{
		"evidence", "journal", "code_excerpt", "card", "target", "app_identity",
		"effect_id", "effect_state", "batch_state", "authority",
	} {
		if strings.Contains(stdout.String(), forbidden) {
			t.Errorf("output leaked forbidden field %q: %s",
				forbidden, stdout.String())
		}
	}
	if stderr.Len() != 0 {
		t.Fatalf("stderr=%q", stderr.String())
	}
}

func TestFinishLegacyBatch63RepairRunSanitizesError(t *testing.T) {
	const secret = "raw journal and postgres://owner:secret@private/db"
	runErr := types.NewAppError(
		types.CodeDatabase, "legacy repair unavailable", errors.New(secret))
	var stdout, stderr bytes.Buffer
	if got := finishLegacyBatch63RepairRun(
		&stdout, &stderr, legacyBatch63RepairOutput{}, runErr,
	); got != exitFailure {
		t.Fatalf("exit=%d", got)
	}
	if stdout.Len() != 0 ||
		!strings.Contains(stderr.String(), "legacy repair unavailable") ||
		strings.Contains(stderr.String(), secret) {
		t.Fatalf("unsafe output stdout/stderr=%q/%q",
			stdout.String(), stderr.String())
	}
}

func TestExecuteLegacyBatch63RepairRoutesExactAction(t *testing.T) {
	expiresAt := time.Date(2026, 7, 25, 13, 0, 0, 0, time.UTC)
	databaseNow := expiresAt.Add(-50 * time.Minute)
	enableBy := databaseNow.Add(5 * time.Minute)
	digest := strings.Repeat("a", 64)
	evidence := []byte(`{"schema_version":"proof"}`)

	t.Run("preview binds evidence expiry and fixed card builder", func(t *testing.T) {
		fake := &fakeLegacyBatch63RepairStore{
			previewPlan: store.LegacyBatch63RepairPlan{
				PlanDigest: digest, DatabaseNow: databaseNow,
				EnableBy: enableBy, ExpiresAt: expiresAt,
			},
		}
		output, err := executeLegacyBatch63Repair(
			t.Context(), fake,
			legacyBatch63RepairOptions{
				Action: legacyBatch63RepairPreview, ExpiresAt: expiresAt,
			},
			evidence, databaseNow,
		)
		if err != nil {
			t.Fatal(err)
		}
		if fake.action != "preview" ||
			!bytes.Equal(fake.evidence.CanonicalBytes, evidence) ||
			!fake.expiresAt.Equal(expiresAt) ||
			output.Phase != "preview" || output.PlanDigest != digest ||
			output.Remaining != int64((50*time.Minute)/time.Second) ||
			!strings.Contains(fake.card, `"effect_id":"effect-fixed"`) ||
			!strings.Contains(fake.card, `"delivery_id":"202"`) {
			t.Fatalf("fake/output=%+v/%+v", fake, output)
		}
	})

	t.Run("apply forwards only exact digest", func(t *testing.T) {
		fake := &fakeLegacyBatch63RepairStore{
			status: store.LegacyBatch63RepairStatus{
				Phase: "finalized", PlanDigest: digest,
				EnableBy: &enableBy, ExpiresAt: &expiresAt,
				DatabaseNow: databaseNow,
			},
		}
		output, err := executeLegacyBatch63Repair(
			t.Context(), fake,
			legacyBatch63RepairOptions{
				Action:     legacyBatch63RepairApply,
				PlanDigest: digest, ExpiresAt: expiresAt,
			},
			evidence, databaseNow,
		)
		if err != nil {
			t.Fatal(err)
		}
		if fake.action != "apply" || fake.planDigest != digest ||
			output.Phase != "finalized" ||
			output.Remaining != int64((50*time.Minute)/time.Second) {
			t.Fatalf("fake/output=%+v/%+v", fake, output)
		}
	})

	t.Run("verify carries no evidence", func(t *testing.T) {
		fake := &fakeLegacyBatch63RepairStore{
			status: store.LegacyBatch63RepairStatus{Phase: "absent"},
		}
		output, err := executeLegacyBatch63Repair(
			t.Context(), fake,
			legacyBatch63RepairOptions{Action: legacyBatch63RepairVerify},
			nil, databaseNow,
		)
		if err != nil || fake.action != "verify" ||
			len(fake.evidence.CanonicalBytes) != 0 ||
			output.Phase != "absent" {
			t.Fatalf("fake/output/err=%+v/%+v/%v", fake, output, err)
		}
	})

	t.Run("abort forwards only exact digest", func(t *testing.T) {
		fake := &fakeLegacyBatch63RepairStore{
			status: store.LegacyBatch63RepairStatus{
				Phase: "blocked", PlanDigest: digest,
			},
		}
		output, err := executeLegacyBatch63Repair(
			t.Context(), fake,
			legacyBatch63RepairOptions{
				Action: legacyBatch63RepairAbort, PlanDigest: digest,
			},
			nil, databaseNow,
		)
		if err != nil || fake.action != "abort" ||
			fake.planDigest != digest || output.Phase != "blocked" {
			t.Fatalf("fake/output/err=%+v/%+v/%v", fake, output, err)
		}
	})
}

type fakeLegacyBatch63RepairStore struct {
	action      string
	planDigest  string
	evidence    store.LegacyBatch63RepairEvidence
	expiresAt   time.Time
	card        string
	previewPlan store.LegacyBatch63RepairPlan
	status      store.LegacyBatch63RepairStatus
	err         error
}

func (f *fakeLegacyBatch63RepairStore) PreviewLegacyBatch63Repair(
	_ context.Context,
	evidence store.LegacyBatch63RepairEvidence,
	expiresAt time.Time,
	buildCard store.LegacyBatch63CardBuilder,
) (store.LegacyBatch63RepairPlan, error) {
	f.action, f.evidence, f.expiresAt = "preview", evidence, expiresAt
	f.card = buildCard(store.LegacyBatch63CardInput{
		EffectID: "effect-fixed",
		Items: []store.LegacyBatch63CardItem{{
			DeliveryID: 202, BodyMD: "body", Title: "title", Score: 85,
		}},
	})
	return f.previewPlan, f.err
}

func (f *fakeLegacyBatch63RepairStore) FinalizeLegacyBatch63Repair(
	_ context.Context,
	planDigest string,
	evidence store.LegacyBatch63RepairEvidence,
	expiresAt time.Time,
	buildCard store.LegacyBatch63CardBuilder,
) (store.LegacyBatch63RepairStatus, error) {
	f.action, f.planDigest = "apply", planDigest
	f.evidence, f.expiresAt = evidence, expiresAt
	f.card = buildCard(store.LegacyBatch63CardInput{EffectID: "effect-fixed"})
	return f.status, f.err
}

func (f *fakeLegacyBatch63RepairStore) VerifyLegacyBatch63Repair(
	context.Context,
) (store.LegacyBatch63RepairStatus, error) {
	f.action = "verify"
	return f.status, f.err
}

func (f *fakeLegacyBatch63RepairStore) AbortLegacyBatch63Repair(
	_ context.Context,
	planDigest string,
) (store.LegacyBatch63RepairStatus, error) {
	f.action, f.planDigest = "abort", planDigest
	return f.status, f.err
}

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

func TestSourceCIExcludesProductionDeployment(t *testing.T) {
	payload, err := os.ReadFile(filepath.Join(
		"..", "..", ".github", "workflows", "ci.yml"))
	if err != nil {
		t.Fatal(err)
	}
	workflow := string(payload)
	for _, required := range []string{
		"runs-on: [self-hosted, Linux, ARM64, vane-test]",
		"permissions:\n  contents: read",
		"persist-credentials: false",
	} {
		if !strings.Contains(workflow, required) {
			t.Errorf("source CI missing isolation guard %q", required)
		}
	}
	for _, forbidden := range []string{
		"vane-deploy",
		"VPS_",
		"appleboy/",
		"workflow_dispatch:",
	} {
		if strings.Contains(workflow, forbidden) {
			t.Errorf("source CI contains production capability %q", forbidden)
		}
	}
}

type errorWriter struct {
	err error
}

func (w errorWriter) Write([]byte) (int, error) {
	return 0, w.err
}
