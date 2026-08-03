package scheduler

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/YouToco/vane/taskstate"
	"github.com/YouToco/vane/types"
)

func TestResearchTaskDefinitionEditV3ConvergesAfterEveryTemporalResponseLoss(
	t *testing.T,
) {
	fake := newTaskScheduleFakeClient()
	s := newTaskScheduleTestScheduler(fake)
	request := validTaskScheduleRequest()
	base, err := s.Scheduler.PrepareResearchTaskScheduleV3(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	ensured, err := s.Scheduler.EnsurePausedResearchTaskV3(t.Context(), base)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Scheduler.ActivateResearchTaskV3(
		t.Context(), base, ensured.Snapshot); err != nil {
		t.Fatal(err)
	}

	target, targetDigest := researchV3EditTargetDefinitionForTest(
		t, base, `{"cron":"30 9 * * *","tz":"Asia/Shanghai"}`)
	prepared, baseSnapshot, err := s.Scheduler.PrepareResearchDefinitionEditV3(
		t.Context(), "manage-tasks-edit-0001", base,
		1, request.PreparedDigest, 2, target,
	)
	if err != nil {
		t.Fatal(err)
	}
	if prepared.OriginalState != ResearchTaskDefinitionEditOriginalActiveV3 ||
		baseSnapshot.Phase != "base_original" || baseSnapshot.Paused {
		t.Fatalf("prepared=%+v base snapshot=%+v", prepared, baseSnapshot)
	}
	if prepared.Base.Input.ActionAuthorizationToken !=
		prepared.Target.Input.ActionAuthorizationToken ||
		prepared.Base.ActionAuthorizationDigest !=
			prepared.Target.ActionAuthorizationDigest {
		t.Fatal("V3 edit rotated the formal Action authorization")
	}
	if prepared.Base.TargetActionDigest == prepared.Target.TargetActionDigest {
		t.Fatal("target Action digest did not bind the new definition fingerprint")
	}

	fake.unpauseCommitErr = context.DeadlineExceeded
	paused, err := s.Scheduler.PauseResearchDefinitionEditV3(
		t.Context(), prepared)
	fake.unpauseCommitErr = nil
	if err != nil || paused.Phase != "base_paused" || !paused.Paused ||
		paused.DefinitionDigest != request.PreparedDigest {
		t.Fatalf("pause after response loss: snapshot=%+v err=%v", paused, err)
	}

	fake.unpauseCommitErr = context.DeadlineExceeded
	applied, err := s.Scheduler.ApplyResearchDefinitionEditV3(
		t.Context(), prepared)
	fake.unpauseCommitErr = nil
	if err != nil || applied.Phase != "target_paused" || !applied.Paused ||
		applied.DefinitionDigest != targetDigest {
		t.Fatalf("apply after response loss: snapshot=%+v err=%v", applied, err)
	}

	fake.unpauseCommitErr = context.DeadlineExceeded
	restored, err := s.Scheduler.RestoreResearchDefinitionEditV3(
		t.Context(), prepared)
	fake.unpauseCommitErr = nil
	if err != nil || restored.Phase != "target_final" || restored.Paused ||
		restored.DefinitionDigest != targetDigest {
		t.Fatalf("restore after response loss: snapshot=%+v err=%v", restored, err)
	}
	final, err := s.Scheduler.DescribeResearchTaskV3(t.Context(), prepared.Target)
	if err != nil || (final.State != TaskScheduleActiveVirginExact &&
		final.State != TaskScheduleActiveUsedExact) {
		t.Fatalf("target final description=%+v err=%v", final, err)
	}
	if got := fake.counts().unpause; got != 4 { // create activation + three edit phases
		t.Fatalf("UpdateSchedule calls=%d, want 4", got)
	}
}

func TestPreparedResearchTaskDefinitionEditV3StrictWireRejectsAuthorizationDrift(
	t *testing.T,
) {
	fake := newTaskScheduleFakeClient()
	s := newTaskScheduleTestScheduler(fake)
	request := validTaskScheduleRequest()
	base, err := s.Scheduler.PrepareResearchTaskScheduleV3(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	ensured, err := s.Scheduler.EnsurePausedResearchTaskV3(t.Context(), base)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Scheduler.ActivateResearchTaskV3(
		t.Context(), base, ensured.Snapshot); err != nil {
		t.Fatal(err)
	}
	target, _ := researchV3EditTargetDefinitionForTest(
		t, base, `{"every_seconds":7200,"tz":"Asia/Shanghai"}`)
	prepared, _, err := s.Scheduler.PrepareResearchDefinitionEditV3(
		t.Context(), "manage-tasks-edit-0002", base, 1, request.PreparedDigest,
		2, target,
	)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := EncodePreparedResearchTaskDefinitionEditV3(prepared)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodePreparedResearchTaskDefinitionEditV3(raw)
	if err != nil || decoded.RequestDigest != prepared.RequestDigest {
		t.Fatalf("strict roundtrip: decoded=%+v err=%v", decoded, err)
	}
	for _, forbidden := range [][]byte{
		[]byte(`"tool_calls"`), []byte(`"fetch_target"`),
		[]byte(`"schedule_sources"`), []byte(`"source_catalog"`),
	} {
		if bytes.Contains(raw, forbidden) {
			t.Fatalf("prepared V3 edit contains retired state %s", forbidden)
		}
	}

	var object map[string]any
	if err := json.Unmarshal(raw, &object); err != nil {
		t.Fatal(err)
	}
	targetWire := object["target"].(map[string]any)
	input := targetWire["input"].(map[string]any)
	input["action_authorization_token"] = strings.Repeat("0", 64)
	tampered, err := json.Marshal(object)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodePreparedResearchTaskDefinitionEditV3(tampered); err == nil {
		t.Fatal("strict V3 edit wire accepted a rotated authorization token")
	}
}

func TestResearchTaskDefinitionEditV3PreservesOriginallyPausedStateWithoutPauseRPC(
	t *testing.T,
) {
	fake := newTaskScheduleFakeClient()
	s := newTaskScheduleTestScheduler(fake)
	request := validTaskScheduleRequest()
	base, err := s.Scheduler.PrepareResearchTaskScheduleV3(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	ensured, err := s.Scheduler.EnsurePausedResearchTaskV3(t.Context(), base)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Scheduler.ActivateResearchTaskV3(
		t.Context(), base, ensured.Snapshot); err != nil {
		t.Fatal(err)
	}
	firstTarget, _ := researchV3EditTargetDefinitionForTest(
		t, base, `{"every_seconds":7200,"tz":"Asia/Shanghai"}`)
	first, _, err := s.Scheduler.PrepareResearchDefinitionEditV3(
		t.Context(), "manage-tasks-edit-pause-base", base, 1,
		request.PreparedDigest, 2, firstTarget)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Scheduler.PauseResearchDefinitionEditV3(
		t.Context(), first); err != nil {
		t.Fatal(err)
	}
	secondTarget, _ := researchV3EditTargetDefinitionForTest(
		t, base, `{"every_seconds":10800,"tz":"Asia/Shanghai"}`)
	second, snapshot, err := s.Scheduler.PrepareResearchDefinitionEditV3(
		t.Context(), "manage-tasks-edit-already-paused", base, 1,
		request.PreparedDigest, 2, secondTarget)
	if err != nil || second.OriginalState != ResearchTaskDefinitionEditOriginalPausedV3 ||
		!snapshot.Paused {
		t.Fatalf("prepare paused: prepared=%+v snapshot=%+v err=%v", second, snapshot, err)
	}
	before := fake.counts().unpause
	paused, err := s.Scheduler.PauseResearchDefinitionEditV3(
		t.Context(), second)
	after := fake.counts().unpause
	if err != nil || !paused.Paused || after != before {
		t.Fatalf("paused edit performed RPC: before=%d after=%d snapshot=%+v err=%v",
			before, after, paused, err)
	}
}

func researchV3EditTargetDefinitionForTest(
	t *testing.T,
	base PreparedResearchTaskScheduleV3,
	spec string,
) (taskstate.ApprovedDefinitionV3, string) {
	t.Helper()
	definition, err := taskstate.BuildApprovedDefinitionV3(
		taskstate.ApprovedDefinitionInputV3{
			TenantID: base.Schedule.TenantID, UserID: base.Schedule.UserID,
			TaskID: base.Schedule.TaskID, TaskName: "edited task",
			TaskManual: "monitor the exact owner target",
			SpecJSON:   []byte(spec), ExecutionMode: types.ExecutionModeDiscoverAtRun,
			Notification: taskstate.NotificationPolicyV3{
				MinimumSignificance: taskstate.NotificationThresholdMajorV3,
				SuppressEmpty:       true,
			},
			Output: taskstate.OutputPreferenceV3{
				Language:             taskstate.OutputLanguageAutoV3,
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
	digest, err := taskstate.DigestApprovedDefinitionV3(definition)
	if err != nil {
		t.Fatal(err)
	}
	return definition, digest
}
