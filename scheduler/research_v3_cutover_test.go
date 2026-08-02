package scheduler

import (
	"context"
	"errors"
	"testing"

	commonpb "go.temporal.io/api/common/v1"
	enums "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/proto"

	"github.com/YouToco/vane/types"
	"github.com/YouToco/vane/workflow"
)

type researchV3CutoverJournalFake struct {
	schedule         *types.Schedule
	head             types.ResearchV3DefinitionHead
	op               types.ResearchV3CutoverOperation
	recheckErr       error
	preflightErr     error
	revoked          bool
	advanceLostPhase types.ResearchV3CutoverPhase
	advanceLost      bool
	advanceConflict  types.ResearchV3CutoverPhase
	onBegin          func()
	events           []string
}

func (f *researchV3CutoverJournalFake) GetSchedule(
	_ context.Context, id string, userID int64,
) (*types.Schedule, error) {
	if f.schedule == nil || f.schedule.ID != id || f.schedule.UserID != userID {
		return nil, types.ErrNotFound
	}
	copy := *f.schedule
	return &copy, nil
}

func (f *researchV3CutoverJournalFake) LoadCurrentResearchApprovedDefinitionV3Head(
	context.Context, int64, int64, string,
) (types.ResearchV3DefinitionHead, error) {
	return f.head, nil
}

func (f *researchV3CutoverJournalFake) RequireSuccessfulResearchV3ShadowPreflight(
	context.Context, int64, int64, string, types.ResearchV3DefinitionHead,
) error {
	f.events = append(f.events, "shadow-preflight")
	return f.preflightErr
}

func (f *researchV3CutoverJournalFake) BeginResearchV3Cutover(
	_ context.Context, p types.BeginResearchV3CutoverParams,
) (types.ResearchV3CutoverOperation, error) {
	if f.op.ID != 0 {
		return f.op, nil
	}
	f.op = types.ResearchV3CutoverOperation{
		ID: 1, TenantID: p.TenantID, UserID: p.UserID, TaskID: p.TaskID,
		IdempotencyKey: p.IdempotencyKey, Generation: 1,
		Definition: p.Definition, FrozenSchedule: append([]byte(nil), p.FrozenSchedule...),
		FrozenScheduleDigest:      p.FrozenScheduleDigest,
		FrozenConflictToken:       append([]byte(nil), p.FrozenConflictToken...),
		ConflictTokenDigest:       p.ConflictTokenDigest,
		TargetAction:              append([]byte(nil), p.TargetAction...),
		TargetActionDigest:        p.TargetActionDigest,
		ActionAuthorizationDigest: p.ActionAuthorizationDigest,
		OriginalPaused:            p.OriginalPaused,
		Phase:                     types.ResearchV3CutoverPrepared,
	}
	f.events = append(f.events, "begin")
	if f.onBegin != nil {
		f.onBegin()
	}
	return f.op, nil
}

func (f *researchV3CutoverJournalFake) LoadResearchV3Cutover(
	context.Context, int64, int64, string, string,
) (types.ResearchV3CutoverOperation, bool, error) {
	return f.op, f.op.ID != 0, nil
}

func (f *researchV3CutoverJournalFake) RecheckResearchV3CutoverDefinition(
	context.Context, types.ResearchV3CutoverOperation,
) error {
	f.events = append(f.events, "recheck")
	return f.recheckErr
}

func (f *researchV3CutoverJournalFake) BeginResearchV3RollbackPause(
	_ context.Context, op types.ResearchV3CutoverOperation, token []byte, digest string,
) (types.ResearchV3CutoverOperation, error) {
	if f.op.ID != op.ID ||
		(f.op.Phase != types.ResearchV3CutoverActive &&
			f.op.Phase != types.ResearchV3CutoverActionSwapped) || !f.revoked {
		return types.ResearchV3CutoverOperation{}, types.ErrConflict
	}
	f.op.RollbackConflictToken = append([]byte(nil), token...)
	f.op.RollbackTokenDigest = digest
	f.op.Phase = types.ResearchV3CutoverRollbackPauseRequested
	f.events = append(f.events, "phase:"+string(f.op.Phase))
	return f.op, nil
}

func (f *researchV3CutoverJournalFake) AdvanceResearchV3Cutover(
	_ context.Context, op types.ResearchV3CutoverOperation,
	expected, next types.ResearchV3CutoverPhase,
) (types.ResearchV3CutoverOperation, error) {
	if f.op.ID != op.ID || f.op.Phase != expected {
		return types.ResearchV3CutoverOperation{}, types.ErrConflict
	}
	if next == f.advanceConflict {
		return types.ResearchV3CutoverOperation{}, types.NewAppError(
			types.CodeConflict, "definition head drifted", types.ErrConflict)
	}
	f.op.Phase = next
	f.events = append(f.events, "phase:"+string(next))
	if next == f.advanceLostPhase && !f.advanceLost {
		f.advanceLost = true
		return types.ResearchV3CutoverOperation{}, errors.New("checkpoint response lost")
	}
	return f.op, nil
}

func (f *researchV3CutoverJournalFake) RevokeResearchV3DeliveryAuthority(
	context.Context, types.ResearchV3CutoverOperation,
) error {
	f.revoked = true
	f.events = append(f.events, "revoke")
	return nil
}

type researchV3ScheduleRemoteFake struct {
	schedule       *schedulepb.Schedule
	token          int
	updates        []*schedulepb.Schedule
	lostAfterApply map[int]bool
	requireRevoked *bool
	requestReceipt map[string]bool
	describeCalls  int
}

func (f *researchV3ScheduleRemoteFake) Describe(
	context.Context, string,
) (*workflowservice.DescribeScheduleResponse, error) {
	f.describeCalls++
	return &workflowservice.DescribeScheduleResponse{
		Schedule:      proto.Clone(f.schedule).(*schedulepb.Schedule),
		ConflictToken: []byte{byte(f.token)},
	}, nil
}

func (f *researchV3ScheduleRemoteFake) CompareAndSwap(
	_ context.Context, _ string, schedule *schedulepb.Schedule,
	token []byte, requestID string,
) error {
	if f.requireRevoked != nil && !*f.requireRevoked {
		return errors.New("remote rollback update preceded authority revocation")
	}
	if f.requestReceipt != nil && f.requestReceipt[requestID] {
		return nil
	}
	if len(token) != 1 || int(token[0]) != f.token {
		return types.ErrConflict
	}
	f.schedule = proto.Clone(schedule).(*schedulepb.Schedule)
	f.updates = append(f.updates, proto.Clone(schedule).(*schedulepb.Schedule))
	f.token++
	if f.requestReceipt == nil {
		f.requestReceipt = make(map[string]bool)
	}
	f.requestReceipt[requestID] = true
	if f.lostAfterApply[len(f.updates)] {
		return errors.New("response lost after apply")
	}
	return nil
}

func TestResearchV3CutoverPreservesMondayNineScheduleAndRecoversLostResponses(t *testing.T) {
	for _, checkpointLoss := range []types.ResearchV3CutoverPhase{
		"", types.ResearchV3CutoverPaused,
		types.ResearchV3CutoverActionSwapped, types.ResearchV3CutoverActive,
	} {
		t.Run(string(checkpointLoss), func(t *testing.T) {
			frozen := researchV3MondaySchedule(t)
			original := proto.Clone(frozen).(*schedulepb.Schedule)
			journal := researchV3CutoverJournalForTest()
			journal.advanceLostPhase = checkpointLoss
			remote := &researchV3ScheduleRemoteFake{
				schedule: frozen, lostAfterApply: map[int]bool{1: true, 2: true, 3: true},
			}
			coordinator := researchV3CutoverCoordinatorForTest(t, journal, remote)
			op, err := coordinator.Cutover(t.Context(), researchV3CutoverRequest{
				TaskID: "task-kimi", UserID: 42, IdempotencyKey: "boss-gate-1",
			})
			if err != nil {
				t.Fatalf("cutover recovery: %v", err)
			}
			if op.Phase != types.ResearchV3CutoverActive || len(remote.updates) != 3 {
				t.Fatalf("phase=%s updates=%d", op.Phase, len(remote.updates))
			}
			got := remote.schedule
			if !proto.Equal(got.GetSpec(), original.GetSpec()) ||
				!proto.Equal(got.GetPolicies(), original.GetPolicies()) ||
				!proto.Equal(got.GetState(), original.GetState()) {
				t.Fatal("cutover changed Schedule Spec/Policy/State")
			}
			before := original.GetAction().GetStartWorkflow()
			after := got.GetAction().GetStartWorkflow()
			if before.GetWorkflowId() != after.GetWorkflowId() ||
				after.GetWorkflowType().GetName() != workflow.ResearchScheduledWorkflowV3Name ||
				before.GetTaskQueue().GetName() != after.GetTaskQueue().GetName() {
				t.Fatal("cutover did not preserve workflow ID/task queue or select the V3 workflow")
			}
			if got.GetSpec().GetCronString()[0] != "0 9 * * 1" ||
				got.GetSpec().GetTimezoneName() != "Asia/Shanghai" {
				t.Fatal("Monday 09:00 cron/tz changed")
			}
			if proto.Equal(got.GetAction(), original.GetAction()) {
				t.Fatal("cutover did not replace Action input")
			}
			var input workflow.ResearchScheduledInputV3
			payloads := got.GetAction().GetStartWorkflow().GetInput().GetPayloads()
			if len(payloads) != 1 || converter.GetDefaultDataConverter().FromPayload(
				payloads[0], &input) != nil || input.TenantID != 7 || input.UserID != 42 ||
				input.TaskID != "task-kimi" || len(input.ActionAuthorizationToken) != 64 {
				t.Fatalf("invalid formal V3 Action input: %+v", input)
			}
			if _, err := coordinator.Cutover(t.Context(), researchV3CutoverRequest{
				TaskID: "task-kimi", UserID: 42, IdempotencyKey: "boss-gate-1",
			}); err != nil || len(remote.updates) != 3 {
				t.Fatalf("idempotent cutover err=%v updates=%d", err, len(remote.updates))
			}
		})
	}
}

func TestResearchV3CutoverRollbackRevokesFirstAndRestoresOnlyAction(t *testing.T) {
	original := researchV3MondaySchedule(t)
	journal := researchV3CutoverJournalForTest()
	remote := &researchV3ScheduleRemoteFake{schedule: proto.Clone(original).(*schedulepb.Schedule)}
	coordinator := researchV3CutoverCoordinatorForTest(t, journal, remote)
	req := researchV3CutoverRequest{TaskID: "task-kimi", UserID: 42, IdempotencyKey: "rollback"}
	if _, err := coordinator.Cutover(t.Context(), req); err != nil {
		t.Fatal(err)
	}
	beforeRollbackUpdates := len(remote.updates)
	remote.requireRevoked = &journal.revoked
	op, err := coordinator.Rollback(t.Context(), 7, req)
	if err != nil {
		t.Fatal(err)
	}
	if op.Phase != types.ResearchV3CutoverRolledBack || !journal.revoked {
		t.Fatalf("phase=%s revoked=%t", op.Phase, journal.revoked)
	}
	if len(remote.updates)-beforeRollbackUpdates != 3 {
		t.Fatalf("rollback updates=%d", len(remote.updates)-beforeRollbackUpdates)
	}
	if !proto.Equal(remote.schedule, original) {
		t.Fatal("rollback did not restore byte-equivalent original Schedule")
	}
}

func TestResearchV3CutoverRollbackPreservesExternalEmergencyPause(t *testing.T) {
	original := researchV3MondaySchedule(t)
	journal := researchV3CutoverJournalForTest()
	remote := &researchV3ScheduleRemoteFake{schedule: proto.Clone(original).(*schedulepb.Schedule)}
	coordinator := researchV3CutoverCoordinatorForTest(t, journal, remote)
	req := researchV3CutoverRequest{TaskID: "task-kimi", UserID: 42, IdempotencyKey: "emergency-pause"}
	if _, err := coordinator.Cutover(t.Context(), req); err != nil {
		t.Fatal(err)
	}
	beforeRollbackUpdates := len(remote.updates)
	remote.schedule.State.Paused = true
	remote.token++
	_, err := coordinator.Rollback(t.Context(), 7, req)
	if types.CodeOf(err) != types.CodeConflict || !journal.revoked ||
		journal.op.Phase != types.ResearchV3CutoverManualIntervention ||
		!remote.schedule.GetState().GetPaused() || len(remote.updates) != beforeRollbackUpdates {
		t.Fatalf("err=%v revoked=%t phase=%s paused=%t rollback_updates=%d", err,
			journal.revoked, journal.op.Phase, remote.schedule.GetState().GetPaused(),
			len(remote.updates)-beforeRollbackUpdates)
	}
}

func TestResearchV3CutoverRollbackRecoversLostPauseResponseByRequestReceipt(t *testing.T) {
	original := researchV3MondaySchedule(t)
	journal := researchV3CutoverJournalForTest()
	remote := &researchV3ScheduleRemoteFake{
		schedule:       proto.Clone(original).(*schedulepb.Schedule),
		lostAfterApply: map[int]bool{4: true},
	}
	coordinator := researchV3CutoverCoordinatorForTest(t, journal, remote)
	req := researchV3CutoverRequest{TaskID: "task-kimi", UserID: 42, IdempotencyKey: "lost-rollback-pause"}
	if _, err := coordinator.Cutover(t.Context(), req); err != nil {
		t.Fatal(err)
	}
	op, err := coordinator.Rollback(t.Context(), 7, req)
	if err != nil || op.Phase != types.ResearchV3CutoverRolledBack ||
		!proto.Equal(remote.schedule, original) || len(remote.updates) != 6 {
		t.Fatalf("err=%v phase=%s updates=%d restored=%t", err, op.Phase,
			len(remote.updates), proto.Equal(remote.schedule, original))
	}
}

func TestResearchV3CutoverRejectsNonExactTaskWithoutEffects(t *testing.T) {
	journal := researchV3CutoverJournalForTest()
	remote := &researchV3ScheduleRemoteFake{schedule: researchV3MondaySchedule(t)}
	coordinator := researchV3CutoverCoordinatorForTest(t, journal, remote)
	_, err := coordinator.Cutover(t.Context(), researchV3CutoverRequest{
		TaskID: "task-other", UserID: 42, IdempotencyKey: "nope",
	})
	if types.CodeOf(err) != types.CodeNotFound || journal.op.ID != 0 || len(remote.updates) != 0 {
		t.Fatalf("err=%v operation=%d updates=%d", err, journal.op.ID, len(remote.updates))
	}
}

func TestResearchV3CutoverRequiresDurableSuccessfulShadowBeforeRemoteRead(t *testing.T) {
	journal := researchV3CutoverJournalForTest()
	journal.preflightErr = types.NewAppError(
		types.CodeConflict, "successful V3 shadow is unavailable", types.ErrConflict)
	remote := &researchV3ScheduleRemoteFake{schedule: researchV3MondaySchedule(t)}
	coordinator := researchV3CutoverCoordinatorForTest(t, journal, remote)
	_, err := coordinator.Cutover(t.Context(), researchV3CutoverRequest{
		TaskID: "task-kimi", UserID: 42, IdempotencyKey: "no-shadow",
	})
	if types.CodeOf(err) != types.CodeConflict || journal.op.ID != 0 ||
		remote.describeCalls != 0 || len(remote.updates) != 0 {
		t.Fatalf("err=%v operation=%d describes=%d updates=%d", err,
			journal.op.ID, remote.describeCalls, len(remote.updates))
	}
}

func TestResearchV3CutoverDoesNotCancelIndependentPause(t *testing.T) {
	journal := researchV3CutoverJournalForTest()
	remote := &researchV3ScheduleRemoteFake{schedule: researchV3MondaySchedule(t)}
	journal.onBegin = func() {
		remote.schedule.State.Paused = true
		remote.token++
	}
	coordinator := researchV3CutoverCoordinatorForTest(t, journal, remote)
	_, err := coordinator.Cutover(t.Context(), researchV3CutoverRequest{
		TaskID: "task-kimi", UserID: 42, IdempotencyKey: "external-maintenance",
	})
	if types.CodeOf(err) != types.CodeConflict || !remote.schedule.GetState().GetPaused() ||
		len(remote.updates) != 0 || !journal.revoked ||
		journal.op.Phase != types.ResearchV3CutoverAborted {
		t.Fatalf("err=%v paused=%t updates=%d revoked=%t phase=%s", err,
			remote.schedule.GetState().GetPaused(), len(remote.updates), journal.revoked,
			journal.op.Phase)
	}
}

func TestResearchV3CutoverDefinitionDriftStopsBeforeActionSwap(t *testing.T) {
	journal := researchV3CutoverJournalForTest()
	journal.recheckErr = types.NewAppError(
		types.CodeConflict, "definition head drifted", types.ErrConflict)
	original := researchV3MondaySchedule(t)
	remote := &researchV3ScheduleRemoteFake{schedule: researchV3MondaySchedule(t)}
	coordinator := researchV3CutoverCoordinatorForTest(t, journal, remote)
	_, err := coordinator.Cutover(t.Context(), researchV3CutoverRequest{
		TaskID: "task-kimi", UserID: 42, IdempotencyKey: "drift",
	})
	if types.CodeOf(err) != types.CodeConflict || len(remote.updates) != 2 ||
		!proto.Equal(remote.schedule, original) || !journal.revoked ||
		journal.op.Phase != types.ResearchV3CutoverRolledBack {
		t.Fatalf("err=%v updates=%d revoked=%t phase=%s", err, len(remote.updates),
			journal.revoked, journal.op.Phase)
	}
}

func TestResearchV3CutoverDefinitionDriftAfterTemporalCASRollsBack(t *testing.T) {
	for _, tc := range []struct {
		name        string
		phase       types.ResearchV3CutoverPhase
		wantUpdates int
	}{
		{name: "after_action_cas", phase: types.ResearchV3CutoverActionSwapped, wantUpdates: 4},
		{name: "before_authority_enable", phase: types.ResearchV3CutoverActive, wantUpdates: 6},
	} {
		t.Run(tc.name, func(t *testing.T) {
			journal := researchV3CutoverJournalForTest()
			journal.advanceConflict = tc.phase
			original := researchV3MondaySchedule(t)
			remote := &researchV3ScheduleRemoteFake{
				schedule: proto.Clone(original).(*schedulepb.Schedule),
			}
			coordinator := researchV3CutoverCoordinatorForTest(t, journal, remote)
			_, err := coordinator.Cutover(t.Context(), researchV3CutoverRequest{
				TaskID: "task-kimi", UserID: 42, IdempotencyKey: "definition-race-" + tc.name,
			})
			if types.CodeOf(err) != types.CodeConflict || !journal.revoked ||
				journal.op.Phase != types.ResearchV3CutoverRolledBack ||
				len(remote.updates) != tc.wantUpdates || !proto.Equal(remote.schedule, original) {
				t.Fatalf("err=%v revoked=%t phase=%s updates=%d", err, journal.revoked,
					journal.op.Phase, len(remote.updates))
			}
		})
	}
}

func researchV3CutoverJournalForTest() *researchV3CutoverJournalFake {
	return &researchV3CutoverJournalFake{
		schedule: &types.Schedule{
			ID: "task-kimi", TenantID: 7, UserID: 42, Status: types.ScheduleStatusActive,
		},
		head: types.ResearchV3DefinitionHead{Version: 3, Digest: string(make([]byte, 64))},
	}
}

func researchV3CutoverCoordinatorForTest(
	t *testing.T, journal *researchV3CutoverJournalFake,
	remote *researchV3ScheduleRemoteFake,
) *researchV3CutoverCoordinator {
	t.Helper()
	coordinator, err := newResearchV3CutoverCoordinator(
		"task-kimi", journal, remote, func(params any) (*commonpb.Payload, error) {
			return converter.GetDefaultDataConverter().ToPayload(params)
		})
	if err != nil {
		t.Fatal(err)
	}
	return coordinator
}

func researchV3MondaySchedule(t *testing.T) *schedulepb.Schedule {
	t.Helper()
	legacy, err := converter.GetDefaultDataConverter().ToPayload(map[string]any{"legacy": true})
	if err != nil {
		t.Fatal(err)
	}
	return &schedulepb.Schedule{
		Spec: &schedulepb.ScheduleSpec{
			CronString: []string{"0 9 * * 1"}, TimezoneName: "Asia/Shanghai",
		},
		Action: &schedulepb.ScheduleAction{Action: &schedulepb.ScheduleAction_StartWorkflow{
			StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
				WorkflowId: "wf-task-kimi", WorkflowType: &commonpb.WorkflowType{Name: "PushPipelineWorkflow"},
				TaskQueue: &taskqueuepb.TaskQueue{Name: "vane", Kind: enums.TASK_QUEUE_KIND_NORMAL},
				Input:     &commonpb.Payloads{Payloads: []*commonpb.Payload{legacy}},
			},
		}},
		Policies: &schedulepb.SchedulePolicies{OverlapPolicy: enums.SCHEDULE_OVERLAP_POLICY_SKIP},
		State:    &schedulepb.ScheduleState{Paused: false, Notes: "monday production"},
	}
}
