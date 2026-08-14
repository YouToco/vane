package store

import (
	"bytes"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/YouToco/vane/server/runcontext"
	"github.com/YouToco/vane/server/runtimepolicy"
	"github.com/YouToco/vane/server/types"
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
	empty, err := f.st.FreezeTaskRunSnapshotShadowAuditScope(
		t.Context(), f.taskID(), time.Now().Add(-time.Minute))
	if err != nil || empty != (TaskRunSnapshotShadowAuditScope{}) {
		t.Fatalf("empty frozen scope = %+v, %v", empty, err)
	}
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
	scope, err := f.st.FreezeTaskRunSnapshotShadowAuditScope(
		t.Context(), taskID, time.Now().Add(-time.Minute))
	if err != nil {
		t.Fatalf("freeze typed audit scope: %v", err)
	}
	if scope.ThroughID != first.SnapshotID || scope.Count != 1 {
		t.Fatalf("frozen scope = %+v, want through=%d count=1",
			scope, first.SnapshotID)
	}
	_ = create("run-v2-through-second")

	page, err := f.st.AuditTaskRunSnapshotShadowsV2Through(
		t.Context(), taskID, time.Now().Add(-time.Minute), 0,
		scope.ThroughID, 1)
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

func TestCompiledSourceSideEffectsUseAuthoritativeSnapshot(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve source fence guard")
	}
	storeDir := filepath.Dir(thisFile)
	want := map[string]int{
		"UpsertContentItemForTaskRunV1":          1,
		"UpdateFetchTargetStateForTaskRunV1":     1,
		"DisableFetchTargetIfActiveForTaskRunV1": 1,
		"ListUnpushedForTaskRunV1":               1,
	}
	got := make(map[string]int)
	err := filepath.WalkDir(storeDir, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if entry.Name() == ".git" || entry.Name() == "vendor" {
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			return err
		}
		for _, declaration := range file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || function.Body == nil {
				continue
			}
			ast.Inspect(function.Body, func(node ast.Node) bool {
				if identifier, ok := node.(*ast.Ident); ok &&
					identifier.Name == "loadAuthoritativeTaskRunSource" {
					got[function.Name.Name]++
				}
				return true
			})
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(want) {
		t.Fatalf("authoritative source fence callers = %v, want %v", got, want)
	}
	for function, calls := range want {
		if got[function] != calls {
			t.Errorf("%s authoritative source fence calls = %d, want %d",
				function, got[function], calls)
		}
	}
}
