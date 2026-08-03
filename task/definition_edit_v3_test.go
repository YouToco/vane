package task

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/YouToco/vane/taskstate"
	"github.com/YouToco/vane/types"
)

func TestBuildResearchV3DefinitionEditTargetReplacesCompleteOwnerSurfaceOnly(
	t *testing.T,
) {
	base, err := taskstate.BuildApprovedDefinitionV3(
		taskstate.ApprovedDefinitionInputV3{
			TenantID: 7, UserID: 42, TaskID: "task-v1-edit-v3",
			TaskName: "旧任务", TaskManual: "监控旧目标",
			SpecJSON:      json.RawMessage(`{"cron":"0 9 * * *","tz":"Asia/Shanghai"}`),
			ExecutionMode: types.ExecutionModeDiscoverAtRun,
			Notification: taskstate.NotificationPolicyV3{
				MinimumSignificance: taskstate.NotificationThresholdMajorV3,
				SuppressEmpty:       true,
			},
			Output: taskstate.OutputPreferenceV3{
				Language:     taskstate.OutputLanguageZhCNV3,
				Format:       taskstate.OutputFormatExecutiveBriefV3,
				Instructions: "旧格式", IncludeEvidenceLinks: true,
			},
			PlannerBudget: types.PlannerBudget{
				MaxPlannerRounds: 4, MaxToolCalls: 8, MaxTokens: 4096,
				MaxCostMicroUSD: 10000, DurationMs: 60000,
			},
			DeliveryPolicy:     taskstate.DeliveryPolicyOwnerFeishu,
			TenantBudgetPolicy: taskstate.BudgetPolicyInheritTenantQuota,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	target, err := BuildResearchV3DefinitionEditTarget(base,
		ResearchV3DefinitionEditInput{
			TenantID: 7, UserID: 42, TaskID: "task-v1-edit-v3",
			TaskName:   "Kimi 套餐监控",
			TaskManual: "检查官方定价页，与历史结果比较；无重大更新不推送。",
			SpecJSON:   json.RawMessage(`{"every_seconds":7200,"tz":"Asia/Shanghai"}`),
			Notification: taskstate.NotificationPolicyV3{
				MinimumSignificance: taskstate.NotificationThresholdQualifiedV3,
				SuppressEmpty:       true,
			},
			Output: taskstate.OutputPreferenceV3{
				Language:     taskstate.OutputLanguageAutoV3,
				Format:       taskstate.OutputFormatConciseBriefV3,
				Instructions: "先结论后证据", IncludeEvidenceLinks: true,
			},
		})
	if err != nil {
		t.Fatal(err)
	}
	if target.TaskName != "Kimi 套餐监控" ||
		target.TaskManual == base.TaskManual || bytesEqual(target.SpecJSON, base.SpecJSON) {
		t.Fatalf("target owner surface was not replaced: %+v", target)
	}
	if target.TenantID != base.TenantID || target.UserID != base.UserID ||
		target.TaskID != base.TaskID || target.ExecutionMode != base.ExecutionMode ||
		!reflect.DeepEqual(target.PlannerBudget, base.PlannerBudget) ||
		target.DeliveryPolicy != base.DeliveryPolicy ||
		target.TenantBudgetPolicy != base.TenantBudgetPolicy {
		t.Fatalf("trusted V3 policy changed: base=%+v target=%+v", base, target)
	}
	raw, err := taskstate.EncodeApprovedDefinitionV3(target)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{
		`"tool_calls":`, `"fetch_target":`, `"schedule_sources":`, `"source_catalog":`,
	} {
		if strings.Contains(string(raw), forbidden) {
			t.Fatalf("target definition contains retired state %q: %s", forbidden, raw)
		}
	}
}

func TestBuildResearchV3DefinitionEditTargetFailsClosedOnScopeOrPartialTarget(
	t *testing.T,
) {
	base := validResearchV3DefinitionForEditTest(t)
	valid := ResearchV3DefinitionEditInput{
		TenantID: base.TenantID, UserID: base.UserID, TaskID: base.TaskID,
		TaskName: "新任务", TaskManual: "完整新手册",
		SpecJSON:     json.RawMessage(`{"cron":"15 9 * * *","tz":"Asia/Shanghai"}`),
		Notification: base.Notification, Output: base.Output,
	}
	wrongOwner := valid
	wrongOwner.UserID++
	if _, err := BuildResearchV3DefinitionEditTarget(base, wrongOwner); err == nil {
		t.Fatal("cross-user native V3 edit was accepted")
	}
	partial := valid
	partial.TaskManual = ""
	if _, err := BuildResearchV3DefinitionEditTarget(base, partial); err == nil {
		t.Fatal("partial native V3 edit inherited an omitted manual")
	}
}

func validResearchV3DefinitionForEditTest(t *testing.T) taskstate.ApprovedDefinitionV3 {
	t.Helper()
	definition, err := taskstate.BuildApprovedDefinitionV3(
		taskstate.ApprovedDefinitionInputV3{
			TenantID: 7, UserID: 42, TaskID: "task-v1-edit-v3",
			TaskName: "旧任务", TaskManual: "旧手册",
			SpecJSON:      json.RawMessage(`{"cron":"0 9 * * *","tz":"Asia/Shanghai"}`),
			ExecutionMode: types.ExecutionModeDiscoverAtRun,
			Notification: taskstate.NotificationPolicyV3{
				MinimumSignificance: taskstate.NotificationThresholdMajorV3,
				SuppressEmpty:       true,
			},
			Output: taskstate.OutputPreferenceV3{
				Language:             taskstate.OutputLanguageZhCNV3,
				Format:               taskstate.OutputFormatExecutiveBriefV3,
				IncludeEvidenceLinks: true,
			},
			PlannerBudget: types.PlannerBudget{
				MaxPlannerRounds: 4, MaxToolCalls: 8, MaxTokens: 4096,
				MaxCostMicroUSD: 10000, DurationMs: 60000,
			},
			DeliveryPolicy:     taskstate.DeliveryPolicyOwnerFeishu,
			TenantBudgetPolicy: taskstate.BudgetPolicyInheritTenantQuota,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	return definition
}

func bytesEqual(left, right []byte) bool {
	return string(left) == string(right)
}
