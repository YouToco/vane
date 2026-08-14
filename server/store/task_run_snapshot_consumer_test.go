package store

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/YouToco/vane/server/runcontext"
	"github.com/YouToco/vane/server/runtimepolicy"
	"github.com/YouToco/vane/server/types"
)

func TestLoadCompiledTaskRunSnapshotV1_BindsCallerIdentityAndSealedReference(t *testing.T) {
	f := newTaskRunSnapshotFixture(t)
	taskID := f.taskID()
	f.createApprovedTask(t, taskID, 1)
	identity := types.RunIdentity{
		TemporalWorkflowID: scheduledTaskWorkflowID(taskID),
		TemporalRunID:      "run-consumer-binding",
		RunKind:            types.RunSnapshotKindScheduled,
		TenantID:           f.tenantID,
		UserID:             f.userID,
		TaskID:             taskID,
	}
	ref, err := f.st.CreateOrGetCompiledTaskRunSnapshotV1(t.Context(),
		CreateOrGetCompiledTaskRunSnapshotV1Params{
			Identity: identity, Policy: testCompiledRunPolicyV1(t),
		})
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name   string
		mutate func(*types.RunIdentity)
	}{
		{"workflow id", func(v *types.RunIdentity) { v.TemporalWorkflowID += "-other" }},
		{"run id", func(v *types.RunIdentity) { v.TemporalRunID += "-other" }},
		{"tenant id", func(v *types.RunIdentity) { v.TenantID++ }},
		{"user id", func(v *types.RunIdentity) { v.UserID++ }},
		{"task id", func(v *types.RunIdentity) { v.TaskID += "-other" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			expected := identity
			test.mutate(&expected)
			_, err := f.st.LoadCompiledTaskRunSnapshotV1(t.Context(), expected, ref)
			assertAppCode(t, err, types.CodeValidation)
		})
	}

	tampered := ref
	tampered.PayloadDigest = "0" + tampered.PayloadDigest[1:]
	if tampered.PayloadDigest == ref.PayloadDigest {
		tampered.PayloadDigest = "1" + tampered.PayloadDigest[1:]
	}
	_, err = f.st.LoadCompiledTaskRunSnapshotV1(t.Context(), identity, tampered)
	assertAppCode(t, err, types.CodeValidation)

	// A correctly re-sealed reference is still not a bearer token. The lookup
	// is scoped to the caller-observed run, so an invented run is simply absent.
	missingIdentity := identity
	missingIdentity.TemporalRunID += "-absent"
	missing := ref
	missing.TemporalRunID = missingIdentity.TemporalRunID
	missing, err = missing.Seal()
	if err != nil {
		t.Fatalf("seal absent reference fixture: %v", err)
	}
	_, err = f.st.LoadCompiledTaskRunSnapshotV1(t.Context(), missingIdentity, missing)
	assertAppCode(t, err, types.CodeNotFound)
}

func TestLoadCompiledTaskRunSnapshotV1_FailsClosedOnStoredIntegrityDrift(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(t *testing.T, f *taskRunSnapshotFixture, snapshotID int64)
	}{
		{
			name: "exact payload bytes",
			mutate: func(t *testing.T, f *taskRunSnapshotFixture, snapshotID int64) {
				t.Helper()
				var payload []byte
				if err := f.st.pool.QueryRow(t.Context(),
					`SELECT payload FROM task_run_snapshots WHERE id=$1`, snapshotID,
				).Scan(&payload); err != nil {
					t.Fatal(err)
				}
				payload = append(bytes.Clone(payload), '\n')
				if _, err := f.st.pool.Exec(t.Context(),
					`UPDATE task_run_snapshots SET payload=$2 WHERE id=$1`, snapshotID, payload); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "payload digest",
			mutate: func(t *testing.T, f *taskRunSnapshotFixture, snapshotID int64) {
				t.Helper()
				if _, err := f.st.pool.Exec(t.Context(),
					`UPDATE task_run_snapshots SET payload_digest=repeat('0', 64) WHERE id=$1`, snapshotID); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "reference digest",
			mutate: func(t *testing.T, f *taskRunSnapshotFixture, snapshotID int64) {
				t.Helper()
				if _, err := f.st.pool.Exec(t.Context(),
					`UPDATE task_run_snapshots SET reference_digest=repeat('0', 64) WHERE id=$1`, snapshotID); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "reference schema",
			mutate: func(t *testing.T, f *taskRunSnapshotFixture, snapshotID int64) {
				t.Helper()
				if _, err := f.st.pool.Exec(t.Context(),
					`UPDATE task_run_snapshots SET reference_schema_version='vane.run-snapshot-ref/v2' WHERE id=$1`, snapshotID); err != nil {
					t.Fatal(err)
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := newTaskRunSnapshotFixture(t)
			taskID := f.taskID()
			f.createApprovedTask(t, taskID, 1)
			identity := types.RunIdentity{
				TemporalWorkflowID: scheduledTaskWorkflowID(taskID),
				TemporalRunID:      "run-integrity-" + taskID,
				RunKind:            types.RunSnapshotKindScheduled,
				TenantID:           f.tenantID,
				UserID:             f.userID,
				TaskID:             taskID,
			}
			ref, err := f.st.CreateOrGetCompiledTaskRunSnapshotV1(t.Context(),
				CreateOrGetCompiledTaskRunSnapshotV1Params{
					Identity: identity, Policy: testCompiledRunPolicyV1(t),
				})
			if err != nil {
				t.Fatal(err)
			}
			test.mutate(t, f, ref.SnapshotID)
			_, err = f.st.LoadCompiledTaskRunSnapshotV1(t.Context(), identity, ref)
			assertAppCode(t, err, types.CodeInternal)
		})
	}
}

func TestLoadCompiledTaskRunSnapshotV1_RejectsSemanticallyNonCanonicalPolicy(t *testing.T) {
	f := newTaskRunSnapshotFixture(t)
	taskID := f.taskID()
	f.createApprovedTask(t, taskID, 1)
	identity := types.RunIdentity{
		TemporalWorkflowID: scheduledTaskWorkflowID(taskID),
		TemporalRunID:      "run-consumer-policy-order",
		RunKind:            types.RunSnapshotKindScheduled,
		TenantID:           f.tenantID,
		UserID:             f.userID,
		TaskID:             taskID,
	}
	ref, err := f.st.CreateOrGetCompiledTaskRunSnapshotV1(t.Context(),
		CreateOrGetCompiledTaskRunSnapshotV1Params{
			Identity: identity, Policy: testCompiledRunPolicyV1(t),
		})
	if err != nil {
		t.Fatal(err)
	}

	row, err := scanTaskRunSnapshot(f.st.pool.QueryRow(t.Context(),
		`SELECT `+taskRunSnapshotColumns+` FROM task_run_snapshots WHERE id=$1`, ref.SnapshotID))
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := readTaskRunSnapshotPayload(row.Payload)
	if err != nil {
		t.Fatal(err)
	}
	modelPolicy, err := runtimepolicy.DecodeModelPolicyV1(decoded.Payload.Policies.ModelPolicy)
	if err != nil {
		t.Fatal(err)
	}
	modelPolicy.Calls[0], modelPolicy.Calls[len(modelPolicy.Calls)-1] =
		modelPolicy.Calls[len(modelPolicy.Calls)-1], modelPolicy.Calls[0]
	type rawModelPolicyV1 runtimepolicy.ModelPolicyV1
	nonCanonicalModel, err := json.Marshal(rawModelPolicyV1(modelPolicy))
	if err != nil {
		t.Fatal(err)
	}
	canonicalModel, err := runtimepolicy.EncodeModelPolicyV1(modelPolicy)
	if err != nil {
		t.Fatal(err)
	}
	canonicalModel, err = canonicalTaskRunJSONObjectV1(canonicalModel)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(nonCanonicalModel, canonicalModel) {
		t.Fatal("fixture model policy unexpectedly remained canonical")
	}
	newRef := rewriteSelfConsistentTaskRunSnapshot(t, f, identity, ref,
		func(payload *taskRunSnapshotPayloadV1) {
			payload.Policies.ModelPolicy = nonCanonicalModel
		})

	_, err = f.st.LoadCompiledTaskRunSnapshotV1(t.Context(), identity, newRef)
	if !errors.Is(err, types.ErrInternal) {
		t.Fatalf("semantically non-canonical policy must fail closed, got %v", err)
	}
}

func TestLoadCompiledTaskRunSnapshotV1_RejectsTypedPolicySchemaDrift(t *testing.T) {
	f := newTaskRunSnapshotFixture(t)
	taskID := f.taskID()
	f.createApprovedTask(t, taskID, 1)
	identity := types.RunIdentity{
		TemporalWorkflowID: scheduledTaskWorkflowID(taskID),
		TemporalRunID:      "run-consumer-policy-schema",
		RunKind:            types.RunSnapshotKindScheduled,
		TenantID:           f.tenantID,
		UserID:             f.userID,
		TaskID:             taskID,
	}
	ref, err := f.st.CreateOrGetCompiledTaskRunSnapshotV1(t.Context(),
		CreateOrGetCompiledTaskRunSnapshotV1Params{
			Identity: identity, Policy: testCompiledRunPolicyV1(t),
		})
	if err != nil {
		t.Fatal(err)
	}
	newRef := rewriteSelfConsistentTaskRunSnapshot(t, f, identity, ref,
		func(payload *taskRunSnapshotPayloadV1) {
			payload.Policies.ModelPolicy = bytes.Replace(
				payload.Policies.ModelPolicy,
				[]byte(runtimepolicy.ModelPolicySchemaVersionV1),
				[]byte(strings.TrimSuffix(runtimepolicy.ModelPolicySchemaVersionV1, "/v1")+"/v2"),
				1,
			)
		})
	_, err = f.st.LoadCompiledTaskRunSnapshotV1(t.Context(), identity, newRef)
	assertAppCode(t, err, types.CodeInternal)
}

func rewriteSelfConsistentTaskRunSnapshot(
	t *testing.T,
	f *taskRunSnapshotFixture,
	identity types.RunIdentity,
	ref types.RunSnapshotRef,
	mutate func(*taskRunSnapshotPayloadV1),
) types.RunSnapshotRef {
	t.Helper()
	row, err := scanTaskRunSnapshot(f.st.pool.QueryRow(t.Context(),
		`SELECT `+taskRunSnapshotColumns+` FROM task_run_snapshots WHERE id=$1`, ref.SnapshotID))
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := readTaskRunSnapshotPayload(row.Payload)
	if err != nil {
		t.Fatal(err)
	}
	mutate(decoded.Payload)
	mutatedPayload, err := json.Marshal(decoded.Payload)
	if err != nil {
		t.Fatal(err)
	}
	mutated, err := readTaskRunSnapshotPayload(mutatedPayload)
	if err != nil {
		t.Fatalf("generic payload reader must accept fixture: %v", err)
	}

	row.CapabilityCatalogDigest = mutated.PolicyDigests.CapabilityCatalog
	row.ToolPolicyDigest = mutated.PolicyDigests.ToolPolicy
	row.PromptPolicyDigest = mutated.PolicyDigests.PromptPolicy
	row.ModelPolicyDigest = mutated.PolicyDigests.ModelPolicy
	row.QuotaPolicyDigest = mutated.PolicyDigests.QuotaPolicy
	row.DefinitionDigest = mutated.DefinitionDigest
	row.PlanDigest = mutated.PlanDigest
	row.Payload = mutated.Canonical
	row.PayloadDigest = sha256Hex(mutated.Canonical)
	newRef, err := sealTaskRunSnapshotReferenceV1(row, taskRunBudgetV1{})
	if err != nil {
		t.Fatal(err)
	}
	row.ReferenceDigest = newRef.ReferenceDigest
	lookup := CreateOrGetTaskRunSnapshotParams{
		TenantID: identity.TenantID, UserID: identity.UserID, TaskID: identity.TaskID,
		TemporalWorkflowID: identity.TemporalWorkflowID, TemporalRunID: identity.TemporalRunID,
	}
	if err := validateStoredTaskRunSnapshot(row, lookup); err != nil {
		t.Fatalf("generic snapshot integrity fixture is not self-consistent: %v", err)
	}
	if _, err := f.st.pool.Exec(t.Context(),
		`UPDATE task_run_snapshots
		    SET capability_catalog_digest=$2, tool_policy_digest=$3,
		        prompt_policy_digest=$4, model_policy_digest=$5, quota_policy_digest=$6,
		        definition_digest=$7, plan_digest=$8, payload=$9, payload_digest=$10,
		        reference_digest=$11
		  WHERE id=$1`,
		row.ID, row.CapabilityCatalogDigest, row.ToolPolicyDigest, row.PromptPolicyDigest,
		row.ModelPolicyDigest, row.QuotaPolicyDigest, row.DefinitionDigest, row.PlanDigest,
		row.Payload, row.PayloadDigest, row.ReferenceDigest,
	); err != nil {
		t.Fatal(err)
	}
	return newRef
}

func TestLoadCompiledTaskRunSnapshotV1_ReturnsFrozenTypedBody(t *testing.T) {
	f := newTaskRunSnapshotFixture(t)
	taskID := f.taskID()
	sourceIDs := f.createApprovedTask(t, taskID, 2)
	policy := testCompiledRunPolicyV1(t)
	identity := types.RunIdentity{
		TemporalWorkflowID: scheduledTaskWorkflowID(taskID),
		TemporalRunID:      "run-consumer-frozen",
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

	got, err := f.st.LoadCompiledTaskRunSnapshotV1(t.Context(), identity, ref)
	if err != nil {
		t.Fatalf("LoadCompiledTaskRunSnapshotV1() error = %v", err)
	}
	assertCompiledSnapshotConsumerBody(t, got, ref, f, taskID, sourceIDs, policy)

	// Current task rows are adaptive/live state after run start. The consumer
	// must not reinterpret them as the already-approved definition.
	if _, err := f.st.pool.Exec(t.Context(),
		`UPDATE schedules
		    SET nl_description='MUTATED CURRENT TASK',
		        spec_json='{"cron":"0 1 * * *"}', scope_json='{"max_items":99}',
		        push_strictness='strict'
		  WHERE id=$1`, taskID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.st.pool.Exec(t.Context(),
		`UPDATE schedule_playbooks
		    SET content='MUTATED CURRENT PLAYBOOK', fetch_plan='{}'
		  WHERE schedule_id=$1`, taskID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.st.pool.Exec(t.Context(),
		`UPDATE fetch_targets
		    SET title='MUTATED CURRENT SOURCE', config='{"query":"mutated"}'
		  WHERE id = ANY($1)`, sourceIDs); err != nil {
		t.Fatal(err)
	}

	// Mutating caller-owned slices must not affect a later load either.
	got.Definition.SpecJSON[0] = '!'
	got.Definition.ScopeJSON[0] = '!'
	got.Definition.FetchPlan[0] = '!'
	got.Definition.Sources[0].Config[0] = '!'
	got.Policy.CapabilityCatalog.Allowed[0].Platform = "mutated"
	got.Policy.ModelPolicy.Calls[0].Model = "mutated"

	reloaded, err := f.st.LoadCompiledTaskRunSnapshotV1(t.Context(), identity, ref)
	if err != nil {
		t.Fatalf("second LoadCompiledTaskRunSnapshotV1() error = %v", err)
	}
	assertCompiledSnapshotConsumerBody(t, reloaded, ref, f, taskID, sourceIDs, policy)
}

func assertCompiledSnapshotConsumerBody(
	t *testing.T,
	got runcontext.CompiledSnapshotV1,
	ref types.RunSnapshotRef,
	f *taskRunSnapshotFixture,
	taskID string,
	sourceIDs []int64,
	policy runtimepolicy.BundleV1,
) {
	t.Helper()
	if got.Ref != ref {
		t.Errorf("Ref = %+v, want %+v", got.Ref, ref)
	}
	if got.Mode != types.ExecutionModeCompiled || got.AdaptiveVersion != 0 ||
		got.Budget != (types.PlannerBudget{}) {
		t.Errorf("run controls = mode %q adaptive %d budget %+v",
			got.Mode, got.AdaptiveVersion, got.Budget)
	}
	definition := got.Definition
	if definition.TaskID != taskID || definition.TenantID != f.tenantID ||
		definition.UserID != f.userID {
		t.Errorf("definition identity = task %q tenant %d user %d",
			definition.TaskID, definition.TenantID, definition.UserID)
	}
	if definition.NLDescription != "monitor approved sources" ||
		string(definition.SpecJSON) != `{"cron":"0 8 * * *","tz":"Asia/Shanghai"}` ||
		string(definition.ScopeJSON) != `{"max_items":5}` ||
		definition.PlaybookContent != "only trusted sources" ||
		definition.Strictness != types.StrictnessNormal ||
		definition.SourceScope != runcontext.SourceScopeApprovedPlan {
		t.Errorf("definition body was not frozen: %+v", definition)
	}
	if len(definition.Sources) != len(sourceIDs) {
		t.Fatalf("sources len = %d, want %d", len(definition.Sources), len(sourceIDs))
	}
	for i, source := range definition.Sources {
		wantURL := fmt.Sprintf("%s/approved/%s/%d", f.urlPrefix, taskID, i)
		wantConfig := fmt.Sprintf(`{"num_results":%d,"query":"topic-%d"}`, i+3, i)
		if source.SourceID != sourceIDs[i] || source.Platform != types.PlatformWeb ||
			source.Capability != types.CapSearch ||
			source.Title != fmt.Sprintf("approved %d", i) || source.URL != wantURL ||
			string(source.Config) != wantConfig {
			t.Errorf("source[%d] = %+v config=%s", i, source, source.Config)
		}
	}
	// task-run-snapshot/v1 freezes the deployed historical wire. Current
	// schedule_playbooks use targets, but this reader must decode sources.
	var plan taskRunFetchPlanV1
	if err := json.Unmarshal(definition.FetchPlan, &plan); err != nil {
		t.Fatalf("decode frozen fetch plan: %v", err)
	}
	if len(plan.Sources) != len(sourceIDs) {
		t.Errorf("frozen plan sources len = %d, want %d", len(plan.Sources), len(sourceIDs))
	}
	if !reflect.DeepEqual(got.Policy, policy) {
		t.Errorf("typed policy differs:\n got %#v\nwant %#v", got.Policy, policy)
	}
}
