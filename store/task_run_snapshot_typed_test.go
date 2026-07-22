package store

import (
	"bytes"
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/YouToco/vane/internal/strictjson"
	"github.com/YouToco/vane/runtimepolicy"
	"github.com/YouToco/vane/types"
)

func TestCreateOrGetCompiledTaskRunSnapshotV1UsesPinnedIdentityValidation(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller could not locate typed snapshot test")
	}
	sourceFile := filepath.Join(filepath.Dir(thisFile), "task_run_snapshot_typed.go")
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, sourceFile, nil, 0)
	if err != nil {
		t.Fatalf("parse typed snapshot adapter: %v", err)
	}
	var target *ast.FuncDecl
	for _, declaration := range file.Decls {
		function, isFunction := declaration.(*ast.FuncDecl)
		if isFunction && function.Name.Name == "CreateOrGetCompiledTaskRunSnapshotV1" {
			target = function
			break
		}
	}
	if target == nil || target.Body == nil {
		t.Fatal("CreateOrGetCompiledTaskRunSnapshotV1 is missing")
	}
	required := map[string]bool{
		"validateTaskRunExpectedIdentityV1": false,
		"createOrGetTaskRunSnapshot":        false,
		"loadTaskRunSnapshotBehindFence":    false,
	}
	forbiddenIdentifiers := map[string]struct{}{
		"scheduledTaskWorkflowID": {},
	}
	forbiddenMethods := map[string]struct{}{
		"Identity": {}, "Valid": {}, "Validate": {}, "ValidateFor": {},
	}
	forbiddenTypeSelectors := map[string]struct{}{
		"ExecutionModeCompiled":    {},
		"RunSnapshotKindScheduled": {},
		"RunSnapshotSchemaVersion": {},
	}
	var violations []string
	ast.Inspect(target.Body, func(node ast.Node) bool {
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
	})
	for name, found := range required {
		if !found {
			violations = append(violations, "missing required call: "+name)
		}
	}
	if len(violations) != 0 {
		t.Fatalf("typed v1 snapshot adapter bypasses pinned identity validation: %v", violations)
	}
}

func testCompiledRunPolicyV1(t *testing.T) runtimepolicy.BundleV1 {
	t.Helper()
	bundle, err := runtimepolicy.BuildV1(runtimepolicy.BuildInputV1{
		AllowedCapabilities: []runtimepolicy.CapabilityV1{{
			Platform: "web", Capability: "search", Kind: "article",
			ImplementationVersion: "fetcher.exa/v1",
			CredentialRef: runtimepolicy.CredentialRefV1{
				ID: runtimepolicy.CredentialIDExaPrimaryV1, Generation: 1,
			},
		}},
		ScorePrompt: runtimepolicy.PromptStageV1{
			SystemPrompt: "score prompt", RendererVersion: "scorer.render/v1",
		},
		CardGenPrompt: runtimepolicy.PromptStageV1{
			SystemPrompt: "card prompt", RendererVersion: "cardgen.render/v1",
		},
		ProfileEvolvePrompt: runtimepolicy.PromptStageV1{
			SystemPrompt: "evolve prompt", RendererVersion: "evolver.render/v1",
		},
		TaskInstructionEnabled: true,
		ModelProvider:          "deepseek",
		ModelEndpoint: runtimepolicy.EndpointRefV1{
			ID: runtimepolicy.EndpointIDDeepSeekCompatiblePrimaryV1, Generation: 1,
		},
		ModelCredentialRef: runtimepolicy.CredentialRefV1{
			ID: runtimepolicy.CredentialIDLLMPrimaryV1, Generation: 1,
		},
		ModelCalls: []runtimepolicy.ModelCallV1{
			{Stage: runtimepolicy.ModelStageScore, Model: "model-1", MaxTokens: 16, DisableThinking: true},
			{Stage: runtimepolicy.ModelStageCardGen, Model: "model-1", Temperature: 0.7, MaxTokens: 400, DisableThinking: true},
			{Stage: runtimepolicy.ModelStageProfileEvolve, Model: "model-1", MaxTokens: 800, DisableThinking: true},
		},
		QuotaBuckets: []runtimepolicy.QuotaBucketV1{{
			Name: "llm_tokens", RatePerSecond: 1, Burst: 100,
			Financial: true, EnforcementVersion: "precharge-reconcile/v1",
		}},
	})
	if err != nil {
		t.Fatalf("BuildV1() error = %v", err)
	}
	return bundle
}

func TestCreateOrGetCompiledTaskRunSnapshotV1_UsesTypedCanonicalPolicies(t *testing.T) {
	f := newTaskRunSnapshotFixture(t)
	taskID := f.taskID()
	f.createApprovedTask(t, taskID, 1)
	policy := testCompiledRunPolicyV1(t)
	identity := types.RunIdentity{
		TemporalWorkflowID: scheduledTaskWorkflowID(taskID),
		TemporalRunID:      "run-typed",
		RunKind:            types.RunSnapshotKindScheduled,
		TenantID:           f.tenantID,
		UserID:             f.userID,
		TaskID:             taskID,
	}
	ref, err := f.st.CreateOrGetCompiledTaskRunSnapshotV1(t.Context(),
		CreateOrGetCompiledTaskRunSnapshotV1Params{Identity: identity, Policy: policy})
	if err != nil {
		t.Fatalf("CreateOrGetCompiledTaskRunSnapshotV1() error = %v", err)
	}
	if err := ref.ValidateFor(identity); err != nil {
		t.Fatalf("typed snapshot ref is invalid: %v", err)
	}

	var payloadBytes []byte
	if err := f.st.pool.QueryRow(t.Context(),
		`SELECT payload FROM task_run_snapshots
		  WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3 AND temporal_run_id=$4`,
		f.tenantID, f.userID, taskID, identity.TemporalRunID).Scan(&payloadBytes); err != nil {
		t.Fatal(err)
	}
	var payload taskRunSnapshotPayload
	if err := strictjson.Decode(payloadBytes, &payload); err != nil {
		t.Fatalf("decode persisted payload: %v", err)
	}
	expected := []struct {
		name string
		got  json.RawMessage
		want func() ([]byte, error)
	}{
		{"capability catalog", payload.Policies.CapabilityCatalog, func() ([]byte, error) {
			return runtimepolicy.EncodeCapabilityCatalogV1(policy.CapabilityCatalog)
		}},
		{"tool policy", payload.Policies.ToolPolicy, func() ([]byte, error) {
			return runtimepolicy.EncodeToolPolicyV1(policy.ToolPolicy)
		}},
		{"prompt policy", payload.Policies.PromptPolicy, func() ([]byte, error) {
			return runtimepolicy.EncodePromptPolicyV1(policy.PromptPolicy)
		}},
		{"model policy", payload.Policies.ModelPolicy, func() ([]byte, error) {
			return runtimepolicy.EncodeModelPolicyV1(policy.ModelPolicy)
		}},
		{"quota policy", payload.Policies.QuotaPolicy, func() ([]byte, error) {
			return runtimepolicy.EncodeQuotaPolicyV1(policy.QuotaPolicy)
		}},
	}
	for _, check := range expected {
		want, encodeErr := check.want()
		if encodeErr != nil {
			t.Fatalf("encode %s: %v", check.name, encodeErr)
		}
		want, encodeErr = canonicalTaskRunJSONObject(want)
		if encodeErr != nil {
			t.Fatalf("canonicalize %s: %v", check.name, encodeErr)
		}
		if !bytes.Equal(check.got, want) {
			t.Errorf("%s bytes differ:\n got %s\nwant %s", check.name, check.got, want)
		}
	}
	for _, forbidden := range [][]byte{
		[]byte(`"api_key"`), []byte(`"token"`), []byte(`"password"`),
		[]byte(`"app_secret"`), []byte("CREDENTIAL-CANARY"),
	} {
		if bytes.Contains(payloadBytes, forbidden) {
			t.Fatalf("persisted payload contains forbidden credential material %q", forbidden)
		}
	}
}

func TestCreateOrGetCompiledTaskRunSnapshotV1_RejectsInvalidPolicyBeforeWrite(t *testing.T) {
	f := newTaskRunSnapshotFixture(t)
	taskID := f.taskID()
	f.createApprovedTask(t, taskID, 1)
	policy := testCompiledRunPolicyV1(t)
	policy.ToolPolicy.AllowedTools = []string{"write_schedule"}
	identity := types.RunIdentity{
		TemporalWorkflowID: scheduledTaskWorkflowID(taskID),
		TemporalRunID:      "run-invalid-policy",
		RunKind:            types.RunSnapshotKindScheduled,
		TenantID:           f.tenantID,
		UserID:             f.userID,
		TaskID:             taskID,
	}

	_, err := f.st.CreateOrGetCompiledTaskRunSnapshotV1(t.Context(),
		CreateOrGetCompiledTaskRunSnapshotV1Params{Identity: identity, Policy: policy})
	assertAppCode(t, err, types.CodeValidation)
	assertTaskRunSnapshotCount(t, f, taskID, 0)
}

func TestCreateOrGetCompiledTaskRunSnapshotV1_RejectsMisboundWorkflow(t *testing.T) {
	f := newTaskRunSnapshotFixture(t)
	taskID := f.taskID()
	f.createApprovedTask(t, taskID, 1)
	identity := types.RunIdentity{
		TemporalWorkflowID: "wf-another-task",
		TemporalRunID:      "run-misbound-workflow",
		RunKind:            types.RunSnapshotKindScheduled,
		TenantID:           f.tenantID,
		UserID:             f.userID,
		TaskID:             taskID,
	}

	_, err := f.st.CreateOrGetCompiledTaskRunSnapshotV1(t.Context(),
		CreateOrGetCompiledTaskRunSnapshotV1Params{
			Identity: identity, Policy: testCompiledRunPolicyV1(t),
		})
	assertAppCode(t, err, types.CodeValidation)
	assertTaskRunSnapshotCount(t, f, taskID, 0)
}

func TestCreateOrGetCompiledTaskRunSnapshotV1_ResponseLostRetryPrecedesPolicyAndTask(t *testing.T) {
	f := newTaskRunSnapshotFixture(t)
	taskID := f.taskID()
	f.createApprovedTask(t, taskID, 1)
	identity := types.RunIdentity{
		TemporalWorkflowID: scheduledTaskWorkflowID(taskID),
		TemporalRunID:      "run-response-lost",
		RunKind:            types.RunSnapshotKindScheduled,
		TenantID:           f.tenantID,
		UserID:             f.userID,
		TaskID:             taskID,
	}
	first, err := f.st.CreateOrGetCompiledTaskRunSnapshotV1(t.Context(),
		CreateOrGetCompiledTaskRunSnapshotV1Params{
			Identity: identity, Policy: testCompiledRunPolicyV1(t),
		})
	if err != nil {
		t.Fatalf("first CreateOrGetCompiledTaskRunSnapshotV1() error = %v", err)
	}
	if _, err := f.st.pool.Exec(t.Context(), `DELETE FROM schedules WHERE id=$1`, taskID); err != nil {
		t.Fatalf("delete task after committed response loss: %v", err)
	}

	// The zero bundle is intentionally invalid. A retry must load the immutable
	// winner before inspecting any current policy or current task aggregate.
	retry, err := f.st.CreateOrGetCompiledTaskRunSnapshotV1(t.Context(),
		CreateOrGetCompiledTaskRunSnapshotV1Params{Identity: identity})
	if err != nil {
		t.Fatalf("response-lost retry error = %v", err)
	}
	if retry != first {
		t.Fatalf("response-lost retry returned another ref:\n got %+v\nwant %+v", retry, first)
	}
}

func TestCreateOrGetCompiledTaskRunSnapshotV1_InFlightWinnerPrecedesInvalidRetryPolicy(t *testing.T) {
	f := newTaskRunSnapshotFixture(t)
	taskID := f.taskID()
	f.createApprovedTask(t, taskID, 1)
	identity := types.RunIdentity{
		TemporalWorkflowID: scheduledTaskWorkflowID(taskID),
		TemporalRunID:      "run-in-flight-" + taskID,
		RunKind:            types.RunSnapshotKindScheduled,
		TenantID:           f.tenantID,
		UserID:             f.userID,
		TaskID:             taskID,
	}
	validPolicy := testCompiledRunPolicyV1(t)

	// Hold the snapshot table so the valid writer reaches and retains the
	// per-RunID advisory fence but cannot complete its initial winner lookup.
	// A table lock changes no aggregate row, avoiding a Repeatable Read 40001
	// that would make this synchronization fixture test a different property.
	locker, err := f.st.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer locker.Rollback(t.Context()) //nolint:errcheck -- cleanup fallback
	if _, err := locker.Exec(t.Context(),
		`LOCK TABLE task_run_snapshots IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatal(err)
	}

	type result struct {
		ref types.RunSnapshotRef
		err error
	}
	validResult := make(chan result, 1)
	go func() {
		ref, callErr := f.st.CreateOrGetCompiledTaskRunSnapshotV1(t.Context(),
			CreateOrGetCompiledTaskRunSnapshotV1Params{
				Identity: identity, Policy: validPolicy,
			})
		validResult <- result{ref: ref, err: callErr}
	}()
	waitForTaskRunAdvisoryFence(t, f.st, identity.TemporalRunID)

	retryResult := make(chan result, 1)
	go func() {
		// A new worker cannot construct the old policy. It must wait for and
		// reuse the in-flight immutable winner instead of returning Validation.
		ref, callErr := f.st.CreateOrGetCompiledTaskRunSnapshotV1(t.Context(),
			CreateOrGetCompiledTaskRunSnapshotV1Params{Identity: identity})
		retryResult <- result{ref: ref, err: callErr}
	}()
	select {
	case got := <-retryResult:
		t.Fatalf("invalid retry escaped in-flight fence early: ref=%+v err=%v", got.ref, got.err)
	case <-time.After(100 * time.Millisecond):
	}

	if err := locker.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	first := <-validResult
	if first.err != nil {
		t.Fatalf("valid in-flight writer error: %v", first.err)
	}
	retry := <-retryResult
	if retry.err != nil {
		t.Fatalf("response-lost retry error: %v", retry.err)
	}
	if retry.ref != first.ref {
		t.Fatalf("retry did not reuse in-flight winner:\n got %+v\nwant %+v", retry.ref, first.ref)
	}
}

func waitForTaskRunAdvisoryFence(t *testing.T, st *Store, runID string) {
	t.Helper()
	conn, err := st.pool.Acquire(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	deadline := time.Now().Add(5 * time.Second)
	for {
		var acquired bool
		if err := conn.QueryRow(t.Context(),
			`SELECT pg_try_advisory_lock(hashtextextended($1, $2))`,
			runID, taskRunSnapshotLockSeed).Scan(&acquired); err != nil {
			t.Fatal(err)
		}
		if !acquired {
			return
		}
		var released bool
		if err := conn.QueryRow(t.Context(),
			`SELECT pg_advisory_unlock(hashtextextended($1, $2))`,
			runID, taskRunSnapshotLockSeed).Scan(&released); err != nil || !released {
			t.Fatalf("release probe advisory lock: released=%v err=%v", released, err)
		}
		if time.Now().After(deadline) {
			t.Fatal("valid writer never acquired task run advisory fence")
		}
		time.Sleep(5 * time.Millisecond)
	}
}
