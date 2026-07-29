package store

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/types"
)

func TestAuthorizeTaskRunSideEffectUsesPinnedReferenceValidation(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller could not locate authorization test")
	}
	sourceFile := strings.TrimSuffix(thisFile, "_test.go") + ".go"
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, sourceFile, nil, 0)
	if err != nil {
		t.Fatalf("parse authorization source: %v", err)
	}
	var targets []*ast.FuncDecl
	for _, declaration := range file.Decls {
		function, isFunction := declaration.(*ast.FuncDecl)
		if isFunction && (function.Name.Name == "AuthorizeTaskRunSideEffect" ||
			function.Name.Name == "authorizeLiveTaskRunSideEffectV1") {
			targets = append(targets, function)
		}
	}
	if len(targets) != 2 || targets[0].Body == nil || targets[1].Body == nil {
		t.Fatal("AuthorizeTaskRunSideEffect v1 authorization chain is incomplete")
	}
	required := map[string]bool{
		"validateTaskRunSnapshotReferenceForExpectedV1": false,
		"loadTaskRunSnapshot":                           false,
		"safeRef":                                       false,
		"authorizeLiveTaskRunSideEffectV1":              false,
		"validateTaskRunExpectedIdentityV1":             false,
	}
	forbiddenMethods := map[string]struct{}{
		"Identity": {}, "Seal": {}, "Valid": {}, "Validate": {},
		"ValidateFor": {}, "ValidateForMode": {}, "ReferenceDigest": {},
	}
	forbiddenIdentifiers := map[string]struct{}{
		"scheduledTaskWorkflowID": {},
	}
	forbiddenTypeSelectors := map[string]struct{}{
		"ExecutionModeCompiled":    {},
		"RunSnapshotKindScheduled": {},
		"RunSnapshotSchemaVersion": {},
	}
	var violations []string
	inspect := func(node ast.Node) bool {
		switch n := node.(type) {
		case *ast.Ident:
			if _, forbidden := forbiddenIdentifiers[n.Name]; forbidden {
				violations = append(violations,
					fset.Position(n.Pos()).String()+": "+n.Name)
			}
			if _, ok := required[n.Name]; ok {
				required[n.Name] = true
			}
		case *ast.CallExpr:
			selector, isSelector := n.Fun.(*ast.SelectorExpr)
			if !isSelector {
				break
			}
			if _, forbidden := forbiddenMethods[selector.Sel.Name]; forbidden {
				violations = append(violations,
					fset.Position(selector.Pos()).String()+": "+selector.Sel.Name)
			}
			if _, ok := required[selector.Sel.Name]; ok {
				required[selector.Sel.Name] = true
			}
		case *ast.SelectorExpr:
			pkg, isPackage := n.X.(*ast.Ident)
			if isPackage && pkg.Name == "types" {
				if _, forbidden := forbiddenTypeSelectors[n.Sel.Name]; forbidden {
					violations = append(violations,
						fset.Position(n.Pos()).String()+": types."+n.Sel.Name)
				}
			}
		}
		return true
	}
	for _, target := range targets {
		ast.Inspect(target.Body, inspect)
	}
	for name, found := range required {
		if !found {
			violations = append(violations, "missing required call: "+name)
		}
	}
	if len(violations) != 0 {
		t.Fatalf("side-effect authorization bypasses pinned v1 validation: %v", violations)
	}
}

type taskRunAuthorizationTestRow func(...any) error

func (r taskRunAuthorizationTestRow) Scan(dest ...any) error {
	return r(dest...)
}

type taskRunAuthorizationTestQueryer struct {
	calls int
	query string
	args  []any
	row   pgx.Row
}

func (q *taskRunAuthorizationTestQueryer) QueryRow(
	_ context.Context,
	query string,
	args ...any,
) pgx.Row {
	q.calls++
	q.query = query
	q.args = append([]any(nil), args...)
	return q.row
}

func TestResolveScheduledRunIdentity_QueryShapeAndFailures(t *testing.T) {
	const (
		taskID     = "push-auth-query"
		userID     = int64(23)
		workflowID = "wf-push-auth-query"
		runID      = "run-auth-query"
		tenantID   = int64(41)
	)

	t.Run("complete live scope", func(t *testing.T) {
		q := &taskRunAuthorizationTestQueryer{
			row: taskRunAuthorizationTestRow(func(dest ...any) error {
				if len(dest) != 1 {
					t.Fatalf("Scan destination count = %d, want 1", len(dest))
				}
				value, ok := dest[0].(*int64)
				if !ok {
					t.Fatalf("Scan destination type = %T, want *int64", dest[0])
				}
				*value = tenantID
				return nil
			}),
		}
		got, err := resolveScheduledRunIdentity(
			t.Context(), q, taskID, userID, workflowID, runID)
		if err != nil {
			t.Fatal(err)
		}
		want := scheduledRunIdentity(taskID, tenantID, userID, runID)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("identity = %+v, want %+v", got, want)
		}
		if q.calls != 1 {
			t.Fatalf("QueryRow calls = %d, want 1", q.calls)
		}
		assertTaskRunAuthorizationSQL(t, q.query,
			"SELECT s.tenant_id FROM schedules s",
			"JOIN tenants t ON t.id = s.tenant_id",
			"JOIN memberships m ON m.tenant_id = s.tenant_id AND m.user_id = s.user_id",
			"WHERE s.id = $1 AND s.user_id = $2",
			"s.status = $3",
			"t.status = $4 AND t.deleted_at IS NULL",
			"FROM task_creation_operations p",
			"p.task_id = s.id",
			"p.tenant_id = s.tenant_id AND p.user_id = s.user_id",
			"p.tool_name = 'create_schedule' AND p.execution_version = 1",
			"NOT (p.status = 'executed' AND p.phase = 'completed')",
		)
		wantArgs := []any{
			taskID, userID, types.ScheduleStatusActive, types.TenantStatusActive,
		}
		if !reflect.DeepEqual(q.args, wantArgs) {
			t.Fatalf("query args = %#v, want %#v", q.args, wantArgs)
		}
	})

	t.Run("scope miss is generic not found", func(t *testing.T) {
		q := &taskRunAuthorizationTestQueryer{
			row: taskRunAuthorizationTestRow(func(...any) error {
				return pgx.ErrNoRows
			}),
		}
		got, err := resolveScheduledRunIdentity(
			t.Context(), q, taskID, userID, workflowID, runID)
		if got != (types.RunIdentity{}) {
			t.Fatalf("identity on miss = %+v, want zero", got)
		}
		assertTaskRunAuthorizationError(t, err, types.CodeNotFound)
		if err.Error() != taskRunNotFound().Error() {
			t.Fatalf("not-found error = %q, want unified %q",
				err, taskRunNotFound())
		}
	})

	t.Run("database error fails closed and strips detail", func(t *testing.T) {
		q := &taskRunAuthorizationTestQueryer{
			row: taskRunAuthorizationTestRow(func(...any) error {
				return errors.New("private-row-detail")
			}),
		}
		got, err := resolveScheduledRunIdentity(
			t.Context(), q, taskID, userID, workflowID, runID)
		if got != (types.RunIdentity{}) {
			t.Fatalf("identity on database failure = %+v, want zero", got)
		}
		assertTaskRunAuthorizationError(t, err, types.CodeDatabase)
		if strings.Contains(err.Error(), "private-row-detail") {
			t.Fatalf("database error leaked driver detail: %v", err)
		}
	})

	t.Run("invalid input never reaches database", func(t *testing.T) {
		tests := []struct {
			name       string
			taskID     string
			userID     int64
			workflowID string
			runID      string
			wantCode   types.ErrCode
		}{
			{name: "empty task", taskID: "", userID: userID,
				workflowID: workflowID, runID: runID, wantCode: types.CodeValidation},
			{name: "non-positive user", taskID: taskID, userID: 0,
				workflowID: workflowID, runID: runID, wantCode: types.CodeValidation},
			{name: "empty run", taskID: taskID, userID: userID,
				workflowID: workflowID, runID: "", wantCode: types.CodeValidation},
			{name: "non-scheduled workflow", taskID: taskID, userID: userID,
				workflowID: "push-agent-elsewhere", runID: runID,
				wantCode: types.CodeNotFound},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				q := &taskRunAuthorizationTestQueryer{}
				got, err := resolveScheduledRunIdentity(
					t.Context(), q, tt.taskID, tt.userID, tt.workflowID, tt.runID)
				if got != (types.RunIdentity{}) {
					t.Fatalf("identity = %+v, want zero", got)
				}
				assertTaskRunAuthorizationError(t, err, tt.wantCode)
				if q.calls != 0 {
					t.Fatalf("QueryRow calls = %d, want 0", q.calls)
				}
			})
		}
	})
}

func TestAuthorizeLiveTaskRunSideEffect_QueryShapeAndFailures(t *testing.T) {
	identity := scheduledRunIdentity("push-auth-gate", 71, 53, "run-auth-gate")

	t.Run("exact live predicates", func(t *testing.T) {
		q := &taskRunAuthorizationTestQueryer{
			row: taskRunAuthorizationTestRow(func(dest ...any) error {
				value, ok := dest[0].(*bool)
				if !ok {
					t.Fatalf("Scan destination type = %T, want *bool", dest[0])
				}
				*value = true
				return nil
			}),
		}
		got, err := authorizeLiveTaskRunSideEffectV1(t.Context(), q, identity)
		if err != nil {
			t.Fatal(err)
		}
		if !got {
			t.Fatal("authorized = false, want true")
		}
		assertTaskRunAuthorizationSQL(t, q.query,
			"FROM schedules s",
			"JOIN tenants t ON t.id = s.tenant_id",
			"JOIN memberships m ON m.tenant_id = s.tenant_id AND m.user_id = s.user_id",
			"WHERE s.id = $1",
			"s.tenant_id = $2",
			"s.user_id = $3",
			"s.status = $4",
			"t.status = $5 AND t.deleted_at IS NULL",
			"FROM task_creation_operations p",
			"p.task_id = s.id",
			"p.tenant_id = s.tenant_id AND p.user_id = s.user_id",
			"p.tool_name = 'create_schedule' AND p.execution_version = 1",
			"NOT (p.status = 'executed' AND p.phase = 'completed')",
		)
		wantArgs := []any{
			identity.TaskID, identity.TenantID, identity.UserID,
			types.ScheduleStatusActive, types.TenantStatusActive,
		}
		if !reflect.DeepEqual(q.args, wantArgs) {
			t.Fatalf("query args = %#v, want %#v", q.args, wantArgs)
		}
	})

	t.Run("revoked live state is false", func(t *testing.T) {
		q := &taskRunAuthorizationTestQueryer{
			row: taskRunAuthorizationTestRow(func(dest ...any) error {
				*(dest[0].(*bool)) = false
				return nil
			}),
		}
		got, err := authorizeLiveTaskRunSideEffectV1(t.Context(), q, identity)
		if err != nil {
			t.Fatal(err)
		}
		if got {
			t.Fatal("authorized = true, want false")
		}
	})

	t.Run("database error fails closed and strips detail", func(t *testing.T) {
		q := &taskRunAuthorizationTestQueryer{
			row: taskRunAuthorizationTestRow(func(...any) error {
				return errors.New("private-authorization-detail")
			}),
		}
		got, err := authorizeLiveTaskRunSideEffectV1(t.Context(), q, identity)
		if got {
			t.Fatal("authorized = true on database failure")
		}
		assertTaskRunAuthorizationError(t, err, types.CodeDatabase)
		if strings.Contains(err.Error(), "private-authorization-detail") {
			t.Fatalf("database error leaked driver detail: %v", err)
		}
	})

	t.Run("invalid identity never reaches database", func(t *testing.T) {
		tests := []struct {
			name   string
			mutate func(*types.RunIdentity)
		}{
			{name: "missing tenant", mutate: func(i *types.RunIdentity) { i.TenantID = 0 }},
			{name: "unsupported kind", mutate: func(i *types.RunIdentity) { i.RunKind = "adhoc" }},
			{name: "workflow task mismatch", mutate: func(i *types.RunIdentity) {
				i.TemporalWorkflowID = "wf-another-task"
			}},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				invalid := identity
				tt.mutate(&invalid)
				q := &taskRunAuthorizationTestQueryer{}
				got, err := authorizeLiveTaskRunSideEffectV1(t.Context(), q, invalid)
				if got {
					t.Fatal("authorized = true for invalid identity")
				}
				assertTaskRunAuthorizationError(t, err, types.CodeValidation)
				if q.calls != 0 {
					t.Fatalf("QueryRow calls = %d, want 0", q.calls)
				}
			})
		}
	})
}

func TestAuthorizeTaskRunSideEffect_RequiresExactPersistedSnapshot(t *testing.T) {
	f := newTaskRunSnapshotFixture(t)
	taskID := f.taskID()
	f.createApprovedTask(t, taskID, 1)
	identity := scheduledRunIdentity(taskID, f.tenantID, f.userID, "run-auth-ref-"+uuid.NewString())
	ref, err := f.st.CreateOrGetCompiledTaskRunSnapshotV1(t.Context(),
		CreateOrGetCompiledTaskRunSnapshotV1Params{
			Identity: identity, Policy: testCompiledRunPolicyV1(t),
		})
	if err != nil {
		t.Fatalf("create exact authorization snapshot: %v", err)
	}

	authorized, err := f.st.AuthorizeTaskRunSideEffect(t.Context(), identity, ref)
	if err != nil || !authorized {
		t.Fatalf("exact live snapshot authorization = %v, err=%v", authorized, err)
	}

	t.Run("well-formed run without snapshot is denied", func(t *testing.T) {
		missing := ref
		missing.TemporalRunID = "run-auth-missing-" + uuid.NewString()
		missing, sealErr := missing.Seal()
		if sealErr != nil {
			t.Fatal(sealErr)
		}
		got, gateErr := f.st.AuthorizeTaskRunSideEffect(t.Context(), missing.Identity(), missing)
		if gateErr != nil || got {
			t.Fatalf("missing snapshot authorization = %v, err=%v", got, gateErr)
		}
	})

	t.Run("wrong snapshot id is an integrity failure", func(t *testing.T) {
		mismatched := ref
		mismatched.SnapshotID++
		mismatched, sealErr := mismatched.Seal()
		if sealErr != nil {
			t.Fatal(sealErr)
		}
		got, gateErr := f.st.AuthorizeTaskRunSideEffect(t.Context(), identity, mismatched)
		if got {
			t.Fatal("mismatched snapshot id was authorized")
		}
		assertTaskRunAuthorizationError(t, gateErr, types.CodeInternal)
	})

	t.Run("unsealed mutation is rejected before database", func(t *testing.T) {
		mutated := ref
		mutated.TemporalRunID = "run-auth-mutated"
		got, gateErr := f.st.AuthorizeTaskRunSideEffect(t.Context(), identity, mutated)
		if got {
			t.Fatal("mutated snapshot ref was authorized")
		}
		assertTaskRunAuthorizationError(t, gateErr, types.CodeValidation)
	})

	t.Run("valid cross-run reference is not a bearer token", func(t *testing.T) {
		other := newTaskRunSnapshotFixture(t)
		otherTaskID := other.taskID()
		other.createApprovedTask(t, otherTaskID, 1)
		otherIdentity := scheduledRunIdentity(
			otherTaskID, other.tenantID, other.userID,
			"run-auth-other-"+uuid.NewString(),
		)
		otherRef, createErr := other.st.CreateOrGetCompiledTaskRunSnapshotV1(
			t.Context(), CreateOrGetCompiledTaskRunSnapshotV1Params{
				Identity: otherIdentity, Policy: testCompiledRunPolicyV1(t),
			})
		if createErr != nil {
			t.Fatalf("create other valid snapshot: %v", createErr)
		}
		got, gateErr := f.st.AuthorizeTaskRunSideEffect(t.Context(), identity, otherRef)
		if got {
			t.Fatal("valid snapshot from another Activity was authorized")
		}
		assertTaskRunAuthorizationError(t, gateErr, types.CodeValidation)
	})

	if _, err := f.st.pool.Exec(t.Context(),
		`DELETE FROM memberships WHERE tenant_id=$1 AND user_id=$2`,
		f.tenantID, f.userID); err != nil {
		t.Fatal(err)
	}
	authorized, err = f.st.AuthorizeTaskRunSideEffect(t.Context(), identity, ref)
	if err != nil || authorized {
		t.Fatalf("revoked member authorization = %v, err=%v", authorized, err)
	}
}

func TestScheduledTaskWorkflowID_MatchesBothSchedulerActionPaths(t *testing.T) {
	if got := scheduledTaskWorkflowID("push-contract"); got != "wf-push-contract" {
		t.Fatalf("scheduledTaskWorkflowID() = %q, want %q", got, "wf-push-contract")
	}
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller could not locate authorization test")
	}
	repoRoot := filepath.Dir(filepath.Dir(thisFile))
	tests := []struct {
		path      string
		fragments []string
	}{
		{
			path: filepath.Join(repoRoot, "scheduler", "scheduler.go"),
			fragments: []string{
				`ID: "wf-" + schedID, Workflow: workflow.PushPipelineWorkflow,`,
			},
		},
		{
			path: filepath.Join(repoRoot, "scheduler", "task_schedule.go"),
			fragments: []string{
				`ActionID: "wf-" + taskID, WorkflowTaskTimeoutNanos:`,
				`prepared.Action.ActionID != "wf-"+prepared.TaskID ||`,
			},
		},
	}
	for _, tt := range tests {
		body, err := os.ReadFile(tt.path)
		if err != nil {
			t.Fatalf("read scheduler action contract %s: %v", tt.path, err)
		}
		normalized := strings.Join(strings.Fields(string(body)), " ")
		for _, fragment := range tt.fragments {
			if !strings.Contains(normalized, fragment) {
				t.Errorf("scheduler action contract %s missing %q", tt.path, fragment)
			}
		}
	}
}

func TestTaskRunAuthorization_LiveScopeAndRevocation(t *testing.T) {
	type liveCase struct {
		name           string
		mutate         func(*testing.T, *taskRunAuthorizationFixture, *string, *int64, *types.RunIdentity)
		wantResolved   bool
		wantAuthorized bool
	}
	tests := []liveCase{
		{name: "active legacy task", wantResolved: true, wantAuthorized: true},
		{
			name: "active Temporal schedule execution",
			mutate: func(_ *testing.T, _ *taskRunAuthorizationFixture, _ *string, _ *int64, identity *types.RunIdentity) {
				identity.TemporalWorkflowID += "-2026-07-24T15:52:40Z"
			},
			wantResolved: true, wantAuthorized: true,
		},
		{
			name: "completed v1 task is mature",
			mutate: func(t *testing.T, f *taskRunAuthorizationFixture, taskID *string, _ *int64, _ *types.RunIdentity) {
				f.createOperation(t, *taskID, "executed", "completed")
			},
			wantResolved: true, wantAuthorized: true,
		},
		{
			name: "missing task",
			mutate: func(_ *testing.T, _ *taskRunAuthorizationFixture, taskID *string, _ *int64, identity *types.RunIdentity) {
				*taskID = "push-auth-missing-" + uuid.NewString()
				identity.TaskID = *taskID
				identity.TemporalWorkflowID = scheduledTaskWorkflowID(*taskID)
			},
		},
		{
			name: "different user even with membership",
			mutate: func(t *testing.T, f *taskRunAuthorizationFixture, _ *string, userID *int64, identity *types.RunIdentity) {
				otherUser := f.addMember(t, f.tenantID)
				*userID = otherUser
				identity.UserID = otherUser
			},
		},
		{
			name: "different tenant",
			mutate: func(t *testing.T, f *taskRunAuthorizationFixture, _ *string, _ *int64, identity *types.RunIdentity) {
				identity.TenantID = f.addTenant(t)
			},
			wantResolved: true,
		},
		{
			name: "paused task",
			mutate: func(t *testing.T, f *taskRunAuthorizationFixture, taskID *string, _ *int64, _ *types.RunIdentity) {
				f.exec(t, `UPDATE schedules SET status = 'paused' WHERE id = $1`, *taskID)
			},
		},
		{
			name: "deleted task",
			mutate: func(t *testing.T, f *taskRunAuthorizationFixture, taskID *string, _ *int64, _ *types.RunIdentity) {
				f.exec(t, `DELETE FROM schedules WHERE id = $1`, *taskID)
			},
		},
		{
			name: "stale membership",
			mutate: func(t *testing.T, f *taskRunAuthorizationFixture, _ *string, _ *int64, _ *types.RunIdentity) {
				f.exec(t, `DELETE FROM memberships WHERE tenant_id = $1 AND user_id = $2`,
					f.tenantID, f.userID)
			},
		},
		{
			name: "suspended tenant",
			mutate: func(t *testing.T, f *taskRunAuthorizationFixture, _ *string, _ *int64, _ *types.RunIdentity) {
				f.exec(t, `UPDATE tenants SET status = 'suspended' WHERE id = $1`, f.tenantID)
			},
		},
		{
			name: "soft-deleted tenant",
			mutate: func(t *testing.T, f *taskRunAuthorizationFixture, _ *string, _ *int64, _ *types.RunIdentity) {
				f.exec(t, `UPDATE tenants SET deleted_at = clock_timestamp() WHERE id = $1`, f.tenantID)
			},
		},
		{
			name: "immature v1 task",
			mutate: func(t *testing.T, f *taskRunAuthorizationFixture, taskID *string, _ *int64, _ *types.RunIdentity) {
				f.createOperation(t, *taskID, "executing", "prepared")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newTaskRunAuthorizationFixture(t)
			taskID := "push-auth-live-" + uuid.NewString()
			f.createSchedule(t, taskID)
			lookupUserID := f.userID
			runID := "run-auth-live-" + uuid.NewString()
			identity := scheduledRunIdentity(taskID, f.tenantID, f.userID, runID)
			if tt.mutate != nil {
				tt.mutate(t, f, &taskID, &lookupUserID, &identity)
			}

			got, err := f.st.ResolveScheduledRunIdentity(
				t.Context(), taskID, lookupUserID,
				identity.TemporalWorkflowID, runID)
			if tt.wantResolved {
				if err != nil {
					t.Fatalf("ResolveScheduledRunIdentity() error: %v", err)
				}
				want := scheduledRunIdentity(taskID, f.tenantID, lookupUserID, runID)
				want.TemporalWorkflowID = identity.TemporalWorkflowID
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("resolved identity = %+v, want %+v", got, want)
				}
			} else {
				if got != (types.RunIdentity{}) {
					t.Fatalf("resolved identity on scope miss = %+v, want zero", got)
				}
				assertTaskRunAuthorizationError(t, err, types.CodeNotFound)
				if err.Error() != taskRunNotFound().Error() {
					t.Fatalf("scope miss error = %q, want unified %q",
						err, taskRunNotFound())
				}
			}

			authorized, err := authorizeLiveTaskRunSideEffectV1(t.Context(), f.st.pool, identity)
			if err != nil {
				t.Fatalf("AuthorizeTaskRunSideEffect() error: %v", err)
			}
			if authorized != tt.wantAuthorized {
				t.Fatalf("authorized = %v, want %v", authorized, tt.wantAuthorized)
			}
		})
	}
}

type taskRunAuthorizationFixture struct {
	st        *Store
	tenantID  int64
	userID    int64
	tenantIDs []int64
	userIDs   []int64
}

func newTaskRunAuthorizationFixture(t *testing.T) *taskRunAuthorizationFixture {
	t.Helper()
	st := tenantTestStore(t)
	userID := testUser(t, st)
	f := &taskRunAuthorizationFixture{st: st, userID: userID, userIDs: []int64{userID}}
	t.Cleanup(func() {
		ctx, cancel := cleanupContext()
		defer cancel()
		for _, tenantID := range f.tenantIDs {
			cleanupExec(ctx, t, st, `DELETE FROM task_creation_operations WHERE tenant_id = $1`, tenantID)
			cleanupExec(ctx, t, st, `DELETE FROM schedules WHERE tenant_id = $1`, tenantID)
			cleanupExec(ctx, t, st, `DELETE FROM memberships WHERE tenant_id = $1`, tenantID)
		}
		for _, userID := range f.userIDs {
			cleanupExec(ctx, t, st, `DELETE FROM users WHERE id = $1`, userID)
		}
		for i := len(f.tenantIDs) - 1; i >= 0; i-- {
			cleanupExec(ctx, t, st, `DELETE FROM tenants WHERE id = $1`, f.tenantIDs[i])
		}
	})
	f.tenantID = f.addTenant(t)
	f.exec(t,
		`INSERT INTO memberships (tenant_id, user_id, role) VALUES ($1, $2, 'owner')`,
		f.tenantID, f.userID)
	return f
}

func (f *taskRunAuthorizationFixture) addTenant(t *testing.T) int64 {
	t.Helper()
	var tenantID int64
	if err := f.st.pool.QueryRow(t.Context(),
		`INSERT INTO tenants (status, plan) VALUES ('active', 'free') RETURNING id`,
	).Scan(&tenantID); err != nil {
		t.Fatalf("create authorization tenant: %v", err)
	}
	f.tenantIDs = append(f.tenantIDs, tenantID)
	return tenantID
}

func (f *taskRunAuthorizationFixture) addMember(t *testing.T, tenantID int64) int64 {
	t.Helper()
	userID := testUser(t, f.st)
	f.userIDs = append(f.userIDs, userID)
	f.exec(t,
		`INSERT INTO memberships (tenant_id, user_id, role) VALUES ($1, $2, 'member')`,
		tenantID, userID)
	return userID
}

func (f *taskRunAuthorizationFixture) createSchedule(t *testing.T, taskID string) {
	t.Helper()
	f.exec(t,
		`INSERT INTO schedules
		    (id, tenant_id, user_id, nl_description, spec_json, scope_json, status)
		 VALUES ($1, $2, $3, 'authorization test', '{}', '{}', 'active')`,
		taskID, f.tenantID, f.userID)
}

func (f *taskRunAuthorizationFixture) createOperation(
	t *testing.T,
	taskID string,
	status string,
	phase string,
) {
	t.Helper()
	f.exec(t,
		`INSERT INTO task_creation_operations
		    (id, tenant_id, user_id, tool_name, args, summary, status, expires_at,
		     execution_version, phase, task_id)
		 VALUES ($1, $2, $3, 'create_schedule', '{}', 'authorization test', $4,
		         clock_timestamp() + interval '1 hour', 1, $5, $6)`,
		uuid.NewString(), f.tenantID, f.userID, status, phase, taskID)
}

func (f *taskRunAuthorizationFixture) exec(t *testing.T, query string, args ...any) {
	t.Helper()
	if _, err := f.st.pool.Exec(t.Context(), query, args...); err != nil {
		t.Fatalf("authorization fixture SQL failed: %v", err)
	}
}

func scheduledRunIdentity(
	taskID string,
	tenantID int64,
	userID int64,
	runID string,
) types.RunIdentity {
	return types.RunIdentity{
		TemporalWorkflowID: scheduledTaskWorkflowID(taskID),
		TemporalRunID:      runID,
		RunKind:            types.RunSnapshotKindScheduled,
		TenantID:           tenantID,
		UserID:             userID,
		TaskID:             taskID,
	}
}

func assertTaskRunAuthorizationSQL(t *testing.T, query string, fragments ...string) {
	t.Helper()
	normalized := strings.Join(strings.Fields(query), " ")
	for _, fragment := range fragments {
		if !strings.Contains(normalized, fragment) {
			t.Errorf("authorization SQL missing %q:\n%s", fragment, normalized)
		}
	}
}

func assertTaskRunAuthorizationError(t *testing.T, err error, code types.ErrCode) {
	t.Helper()
	if err == nil {
		t.Fatalf("error = nil, want %s", code)
	}
	if got := types.CodeOf(err); got != code {
		t.Fatalf("error code = %s, want %s: %v", got, code, err)
	}
}
