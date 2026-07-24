package main

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/YouToco/vane/store"
	"github.com/YouToco/vane/types"
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
		name string
		page store.TaskRunSnapshotShadowAuditPage
		want int
	}{
		{name: "empty", want: exitVerifyFailed},
		{name: "match", page: store.TaskRunSnapshotShadowAuditPage{
			Items: []store.TaskRunSnapshotShadowAuditItem{{
				SnapshotID: 1, Status: store.TaskRunSnapshotShadowMatch,
			}},
		}, want: exitOK},
		{name: "missing", page: store.TaskRunSnapshotShadowAuditPage{
			Items: []store.TaskRunSnapshotShadowAuditItem{{
				SnapshotID: 1, Status: "missing",
			}},
		}, want: exitVerifyFailed},
		{name: "headless", page: store.TaskRunSnapshotShadowAuditPage{
			Items: []store.TaskRunSnapshotShadowAuditItem{{
				SnapshotID: 1, Status: store.TaskRunSnapshotShadowHeadless,
			}},
		}, want: exitVerifyFailed},
		{name: "legacy", page: store.TaskRunSnapshotShadowAuditPage{
			Items: []store.TaskRunSnapshotShadowAuditItem{{
				SnapshotID: 1, Status: store.TaskRunSnapshotShadowLegacyCompatible,
			}},
		}, want: exitVerifyFailed},
		{name: "next", page: store.TaskRunSnapshotShadowAuditPage{
			Items: []store.TaskRunSnapshotShadowAuditItem{{
				SnapshotID: 1, Status: store.TaskRunSnapshotShadowMatch,
			}}, Next: &next,
		}, want: exitVerifyMorePages},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			if got := finishSnapshotShadowRun(
				&stdout, &stderr, test.page, nil); got != test.want {
				t.Fatalf("exit=%d want=%d", got, test.want)
			}
			if stdout.Len() == 0 || stderr.Len() != 0 {
				t.Fatalf("stdout/stderr = %q/%q", stdout.String(), stderr.String())
			}
		})
	}
}

func TestRuntimeAdminDeploymentWorkflow(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve test source")
	}
	repoRoot := filepath.Dir(filepath.Dir(filepath.Dir(thisFile)))
	payload, err := os.ReadFile(filepath.Join(
		repoRoot, ".github", "workflows", "ci.yml"))
	if err != nil {
		t.Fatal(err)
	}
	workflow := string(payload)
	for _, fragment := range []string{
		"# 四个二进制都要上生产：",
		"GOOS=linux GOARCH=amd64 go build -o bin/runtimeadmin ./cmd/runtimeadmin",
		`source: "bin/vane,bin/useradmin,bin/gate,bin/runtimeadmin"`,
		"chmod +x /opt/vane/bin/vane /opt/vane/bin/useradmin /opt/vane/bin/gate /opt/vane/bin/runtimeadmin",
		"test -x /opt/vane/bin/runtimeadmin",
	} {
		if !strings.Contains(workflow, fragment) {
			t.Errorf("deployment workflow missing %q", fragment)
		}
	}
}

type errorWriter struct {
	err error
}

func (w errorWriter) Write([]byte) (int, error) {
	return 0, w.err
}
