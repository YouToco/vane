package store

import (
	"errors"
	"testing"

	"github.com/YouToco/vane/server/runtimepolicy"
	"github.com/YouToco/vane/server/types"
)

func TestAuthorizeAndConsumeTaskRunLLMQuotaV1_IsAtomicAndExactTenant(t *testing.T) {
	f := newTaskRunSnapshotFixture(t)
	taskID := f.taskID()
	f.createApprovedTask(t, taskID, 1)
	identity := types.RunIdentity{
		TemporalWorkflowID: scheduledTaskWorkflowID(taskID),
		TemporalRunID:      "run-atomic-quota",
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
	setBucket(t, f.st, f.tenantID, QuotaLLMTokens, 100, 0.000001, 100)
	rule := runtimepolicy.QuotaBucketV1{
		Name: string(QuotaLLMTokens), Financial: true,
		EnforcementVersion: runtimepolicy.QuotaEnforcementLLMPrechargeV1,
	}

	if err := f.st.AuthorizeAndConsumeTaskRunLLMQuotaV1(
		t.Context(), identity, ref, rule, 20); err != nil {
		t.Fatalf("authorized reserve failed: %v", err)
	}
	if got := runtimeQuotaTokens(t, f.st, f.tenantID, QuotaLLMTokens); got < 79.9 || got > 80.1 {
		t.Fatalf("tokens after reserve = %.6f, want about 80", got)
	}

	if _, err := f.st.pool.Exec(t.Context(),
		`UPDATE schedules SET status='paused' WHERE id=$1 AND tenant_id=$2`,
		taskID, f.tenantID); err != nil {
		t.Fatal(err)
	}
	before := runtimeQuotaTokens(t, f.st, f.tenantID, QuotaLLMTokens)
	if err := f.st.AuthorizeAndConsumeTaskRunLLMQuotaV1(
		t.Context(), identity, ref, rule, 1); !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("paused task reserve = %v, want ErrQuotaExceeded", err)
	}
	after := runtimeQuotaTokens(t, f.st, f.tenantID, QuotaLLMTokens)
	if after < before || after-before > 0.01 {
		t.Fatalf("denied reserve mutated tokens: before %.6f after %.6f", before, after)
	}
}

func TestAuthorizeAndConsumeTaskRunLLMQuotaV1_RejectsForgedScopeBeforeSpend(t *testing.T) {
	f := newTaskRunSnapshotFixture(t)
	taskID := f.taskID()
	f.createApprovedTask(t, taskID, 1)
	identity := types.RunIdentity{
		TemporalWorkflowID: scheduledTaskWorkflowID(taskID), TemporalRunID: "run-forged-quota",
		RunKind: types.RunSnapshotKindScheduled, TenantID: f.tenantID,
		UserID: f.userID, TaskID: taskID,
	}
	ref, err := f.st.CreateOrGetCompiledTaskRunSnapshotV1(t.Context(),
		CreateOrGetCompiledTaskRunSnapshotV1Params{Identity: identity, Policy: testCompiledRunPolicyV1(t)})
	if err != nil {
		t.Fatal(err)
	}
	setBucket(t, f.st, f.tenantID, QuotaLLMTokens, 100, 0.000001, 100)
	rule := runtimepolicy.QuotaBucketV1{
		Name: string(QuotaLLMTokens), Financial: true,
		EnforcementVersion: runtimepolicy.QuotaEnforcementLLMPrechargeV1,
	}
	forged := identity
	forged.UserID++
	if err := f.st.AuthorizeAndConsumeTaskRunLLMQuotaV1(
		t.Context(), forged, ref, rule, 1); types.CodeOf(err) != types.CodeValidation {
		t.Fatalf("forged scope error = %v, want CodeValidation", err)
	}
	if got := runtimeQuotaTokens(t, f.st, f.tenantID, QuotaLLMTokens); got != 100 {
		t.Fatalf("forged reserve changed balance to %.6f", got)
	}
}
