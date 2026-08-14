package task

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/YouToco/vane/server/taskstate"
	"github.com/YouToco/vane/server/types"
)

func testResearchV3CreationInput() ResearchV3CreationProposalInput {
	return ResearchV3CreationProposalInput{
		ActionID: "native-v3-create-1", UserID: 42,
		TaskName:   "Kimi 套餐可购买状态",
		TaskManual: "每周一 9:00 检查 Kimi 官方套餐页，交叉核验并和历史结论比较；没有重大更新不推送。",
		SpecJSON:   json.RawMessage(`{"tz":"Asia/Shanghai","cron":"0 9 * * 1"}`),
		Notification: taskstate.NotificationPolicyV3{
			MinimumSignificance: taskstate.NotificationThresholdMajorV3,
			SuppressEmpty:       true,
		},
		Output: taskstate.OutputPreferenceV3{
			Language:             taskstate.OutputLanguageZhCNV3,
			Format:               taskstate.OutputFormatExecutiveBriefV3,
			Instructions:         "先写结论，再列官方依据和变化。",
			IncludeEvidenceLinks: true,
		},
		ExpiresAt: time.Now().Add(time.Hour),
	}
}

func testResearchV3CreationPolicy() CreationV3ServerPolicy {
	return CreationV3ServerPolicy{PlannerBudget: types.PlannerBudget{
		MaxPlannerRounds: 8, MaxToolCalls: 16, MaxTokens: 32768,
		MaxCostMicroUSD: 1_000_000, DurationMs: 300_000,
	}}
}

func TestCreationCoordinatorPrepareResearchV3InjectsServerPolicy(t *testing.T) {
	store := newCreationSagaFakeStore()
	coordinator := NewCreationCoordinator(store, &creationSagaFakeScheduler{}, nil,
		WithResearchV3CreationPolicy(testResearchV3CreationPolicy()))

	proposal, err := coordinator.PrepareResearchV3(t.Context(), testResearchV3CreationInput())
	if err != nil {
		t.Fatal(err)
	}
	if proposal.ID != "native-v3-create-1" || proposal.Summary != "Kimi 套餐可购买状态" ||
		store.op.ExecutionVersion != types.TaskCreationExecutionVersionV2 {
		t.Fatalf("proposal=%+v operation=%+v", proposal, store.op)
	}
	definition, err := taskstate.DecodeApprovedDefinitionV3(store.op.Args)
	if err != nil {
		t.Fatal(err)
	}
	if definition.ExecutionMode != types.ExecutionModeDiscoverAtRun ||
		definition.DeliveryPolicy != taskstate.DeliveryPolicyOwnerFeishu ||
		definition.TenantBudgetPolicy != taskstate.BudgetPolicyInheritTenantQuota ||
		definition.PlannerBudget != testResearchV3CreationPolicy().PlannerBudget {
		t.Fatalf("server policy not frozen: %+v", definition)
	}
	for _, forbidden := range []string{"ToolCalls", "Sources", "FetchTargets", "fetch_targets"} {
		if _, ok := reflect.TypeOf(definition).FieldByName(forbidden); ok {
			t.Fatalf("V3 native definition exposes retired field %q", forbidden)
		}
	}
}

func TestCreationCoordinatorPrepareResearchV3FailsClosedWithoutServerPolicy(t *testing.T) {
	coordinator := NewCreationCoordinator(
		newCreationSagaFakeStore(), &creationSagaFakeScheduler{}, nil)
	if _, err := coordinator.PrepareResearchV3(
		t.Context(), testResearchV3CreationInput()); err == nil {
		t.Fatal("native V3 creation was enabled without trusted server policy")
	}
}

func TestCreationCoordinatorPrepareResearchV3RejectsNonOwner(t *testing.T) {
	for _, role := range []types.MembershipRole{
		types.MembershipRoleAdmin, types.MembershipRoleMember,
	} {
		t.Run(string(role), func(t *testing.T) {
			store := newCreationSagaFakeStore()
			store.membershipRole = role
			coordinator := NewCreationCoordinator(store, &creationSagaFakeScheduler{}, nil,
				WithResearchV3CreationPolicy(testResearchV3CreationPolicy()))
			_, err := coordinator.PrepareResearchV3(
				t.Context(), testResearchV3CreationInput())
			if !errors.Is(err, types.ErrValidation) || store.createCalls != 0 {
				t.Fatalf("role=%s err=%v create_calls=%d", role, err, store.createCalls)
			}
		})
	}
}
