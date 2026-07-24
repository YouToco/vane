package store

import (
	"bytes"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/YouToco/vane/runcontext"
	"github.com/YouToco/vane/runtimepolicy"
	"github.com/YouToco/vane/types"
)

func TestAuditCompiledTaskRunSnapshotV2_MatchAndMissing(t *testing.T) {
	tests := []struct {
		name       string
		runID      string
		withShadow bool
		want       CompiledRunSnapshotV2AuditStatus
	}{
		{name: "typed match", runID: "run-v2-audit-match", withShadow: true, want: CompiledRunSnapshotV2AuditMatch},
		{name: "pre-shadow parent is explicit missing", runID: "run-v2-audit-missing", want: CompiledRunSnapshotV2AuditMissing},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := newTaskRunSnapshotFixture(t)
			taskID := f.taskID()
			f.createApprovedTask(t, taskID, 2)
			baseline, err := f.st.reconcileTaskDefinitionBaseline(
				t.Context(), TaskDefinitionBaselineApply, TaskDefinitionBaselineCursor{
					TenantID: f.tenantID, UserID: f.userID, TaskID: taskID,
				})
			if err != nil || baseline.Status != TaskDefinitionBaselineApplied {
				t.Fatalf("apply baseline = %+v, %v", baseline, err)
			}
			identity := types.RunIdentity{
				TemporalWorkflowID: scheduledTaskWorkflowID(taskID),
				TemporalRunID:      test.runID,
				RunKind:            types.RunSnapshotKindScheduled,
				TenantID:           f.tenantID,
				UserID:             f.userID,
				TaskID:             taskID,
			}
			var ref types.RunSnapshotRef
			if test.withShadow {
				ref, err = f.st.CreateOrGetCompiledRunSnapshotShadowV2(
					t.Context(), identity, testCompiledRunPolicyV1(t))
			} else {
				ref, err = f.st.CreateOrGetCompiledRunSnapshotV1(
					t.Context(), identity, testCompiledRunPolicyV1(t))
			}
			if err != nil {
				t.Fatalf("create snapshot: %v", err)
			}

			result, err := f.st.AuditCompiledTaskRunSnapshotV2(
				t.Context(), identity, ref)
			if err != nil {
				t.Fatalf("AuditCompiledTaskRunSnapshotV2() error = %v", err)
			}
			if result.Status != test.want {
				t.Fatalf("audit = %+v, want status %q", result, test.want)
			}
			if test.withShadow {
				if result.ShadowStatus != TaskRunSnapshotShadowMatch ||
					!result.TypedEqual || result.ShadowPayloadDigest == "" {
					t.Fatalf("match audit = %+v", result)
				}
			} else if result.TypedEqual || result.ShadowPayloadDigest != "" {
				t.Fatalf("missing audit leaked match material = %+v", result)
			}
		})
	}
}

func TestAuditCompiledTaskRunSnapshotV2_NonMatchNeverMaterializes(t *testing.T) {
	f := newTaskRunSnapshotFixture(t)
	taskID := f.taskID()
	f.createApprovedTask(t, taskID, 1)
	identity := types.RunIdentity{
		TemporalWorkflowID: scheduledTaskWorkflowID(taskID),
		TemporalRunID:      "run-v2-audit-headless",
		RunKind:            types.RunSnapshotKindScheduled,
		TenantID:           f.tenantID,
		UserID:             f.userID,
		TaskID:             taskID,
	}
	ref, err := f.st.CreateOrGetCompiledRunSnapshotShadowV2(
		t.Context(), identity, testCompiledRunPolicyV1(t))
	if err != nil {
		t.Fatalf("create headless shadow: %v", err)
	}
	result, err := f.st.AuditCompiledTaskRunSnapshotV2(
		t.Context(), identity, ref)
	if err != nil {
		t.Fatalf("audit headless shadow: %v", err)
	}
	if result.Status != CompiledRunSnapshotV2AuditNonMatch ||
		result.ShadowStatus != TaskRunSnapshotShadowHeadless ||
		result.TypedEqual {
		t.Fatalf("headless audit = %+v", result)
	}
}

func TestAuditTaskRunSnapshotShadowsV2Through_FreezesTypedSample(t *testing.T) {
	f := newTaskRunSnapshotFixture(t)
	taskID := f.taskID()
	f.createApprovedTask(t, taskID, 1)
	baseline, err := f.st.reconcileTaskDefinitionBaseline(
		t.Context(), TaskDefinitionBaselineApply, TaskDefinitionBaselineCursor{
			TenantID: f.tenantID, UserID: f.userID, TaskID: taskID,
		})
	if err != nil || baseline.Status != TaskDefinitionBaselineApplied {
		t.Fatalf("apply baseline = %+v, %v", baseline, err)
	}
	create := func(runID string) types.RunSnapshotRef {
		t.Helper()
		ref, err := f.st.CreateOrGetCompiledRunSnapshotShadowV2(
			t.Context(), types.RunIdentity{
				TemporalWorkflowID: scheduledTaskWorkflowID(taskID),
				TemporalRunID:      runID,
				RunKind:            types.RunSnapshotKindScheduled,
				TenantID:           f.tenantID,
				UserID:             f.userID,
				TaskID:             taskID,
			}, testCompiledRunPolicyV1(t))
		if err != nil {
			t.Fatalf("create %s: %v", runID, err)
		}
		return ref
	}
	first := create("run-v2-through-first")
	_ = create("run-v2-through-second")

	page, err := f.st.AuditTaskRunSnapshotShadowsV2Through(
		t.Context(), taskID, time.Now().Add(-time.Minute), 0,
		first.SnapshotID, 1)
	if err != nil {
		t.Fatalf("typed frozen audit: %v", err)
	}
	if len(page.Items) != 1 || page.Next != nil ||
		page.Items[0].SnapshotID != first.SnapshotID ||
		page.Items[0].Status != TaskRunSnapshotShadowMatch ||
		page.Items[0].TypedAuditStatus != CompiledRunSnapshotV2AuditMatch ||
		!page.Items[0].TypedEqual {
		t.Fatalf("typed frozen audit page = %+v", page)
	}
}

func TestCompiledSnapshotV1ExactEqual_IsByteAndOrderSensitive(t *testing.T) {
	base := runcontext.CompiledSnapshotV1{
		Mode: types.ExecutionModeCompiled,
		Definition: runcontext.DefinitionV1{
			TaskID: "task", TenantID: 1, UserID: 2,
			SpecJSON:  []byte(`{"a":1}`),
			ScopeJSON: []byte(`{}`),
			FetchPlan: []byte(`{"sources":[]}`),
			Sources: []runcontext.SourceV1{
				{SourceID: 1, Platform: "web", Capability: "search", Config: []byte(`{}`)},
				{SourceID: 2, Platform: "web", Capability: "feed", Config: []byte(`{}`)},
			},
		},
		Policy: testCompiledRunPolicyV1(t),
	}
	if !compiledSnapshotV1ExactEqual(base, base) {
		t.Fatal("identical snapshot differs")
	}

	nilDrift := base
	nilDrift.Definition.SpecJSON = nil
	emptyBase := base
	emptyBase.Definition.SpecJSON = []byte{}
	if compiledSnapshotV1ExactEqual(nilDrift, emptyBase) {
		t.Fatal("nil and empty RawMessage compared equal")
	}

	byteDrift := base
	byteDrift.Definition.SpecJSON = bytes.Clone(base.Definition.SpecJSON)
	byteDrift.Definition.SpecJSON = append(byteDrift.Definition.SpecJSON, ' ')
	if compiledSnapshotV1ExactEqual(base, byteDrift) {
		t.Fatal("RawMessage byte drift compared equal")
	}

	orderDrift := base
	orderDrift.Definition.Sources = append(
		[]runcontext.SourceV1(nil), base.Definition.Sources...)
	orderDrift.Definition.Sources[0], orderDrift.Definition.Sources[1] =
		orderDrift.Definition.Sources[1], orderDrift.Definition.Sources[0]
	if compiledSnapshotV1ExactEqual(base, orderDrift) {
		t.Fatal("source order drift compared equal")
	}

	policyDrift := base
	policyDrift.Policy = base.Policy
	policyDrift.Policy.ModelPolicy.Calls = append(
		[]runtimepolicy.ModelCallV1(nil), base.Policy.ModelPolicy.Calls...)
	policyDrift.Policy.ModelPolicy.Calls[0].Model += "-other"
	if compiledSnapshotV1ExactEqual(base, policyDrift) {
		t.Fatal("canonical policy drift compared equal")
	}
}

func TestCompiledSourceSideEffectsRemainPinnedToV1(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve source fence guard")
	}
	storeDir := filepath.Dir(thisFile)
	want := map[string]int{
		"UpsertContentItemForTaskRunV1":      1,
		"UpdateSourceFetchStateForTaskRunV1": 1,
		"DisableSourceIfActiveForTaskRunV1":  1,
		"ListUnpushedForTaskRunV1":           1,
	}
	got := make(map[string]int)
	for _, name := range []string{"compiled_run_fetch.go", "compiled_run_sources.go"} {
		file, err := parser.ParseFile(
			token.NewFileSet(), filepath.Join(storeDir, name), nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, declaration := range file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || function.Body == nil {
				continue
			}
			ast.Inspect(function.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				identifier, ok := call.Fun.(*ast.Ident)
				if ok && identifier.Name == "loadFrozenTaskRunSourceV1" {
					got[function.Name.Name]++
				}
				return true
			})
		}
	}
	if len(got) != len(want) {
		t.Fatalf("v1 source fence callers = %v, want %v", got, want)
	}
	for function, calls := range want {
		if got[function] != calls {
			t.Errorf("%s v1 source fence calls = %d, want %d",
				function, got[function], calls)
		}
	}
}
