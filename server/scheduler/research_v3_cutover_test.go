package scheduler

import (
	"context"
	"errors"
	"strings"
	"testing"

	commonpb "go.temporal.io/api/common/v1"
	enums "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/proto"

	"github.com/YouToco/vane/server/types"
	"github.com/YouToco/vane/server/workflow"
)

func TestResearchV3CutoverParamsEncoderUsesReservedProtocolNotTaskID(t *testing.T) {
	s := &Scheduler{}
	encode, err := s.researchV3CutoverParamsEncoder()
	if err != nil {
		t.Fatalf("build cutover encoder: %v", err)
	}
	want := workflow.ResearchScheduledInputV3{
		TenantID: 7, UserID: 9, TaskID: "task-kimi",
		ActionAuthorizationToken: "authorization-token",
	}
	payload, err := encode(want)
	if err != nil || payload == nil {
		t.Fatalf("encode cutover payload: payload=%v err=%v", payload, err)
	}
	var got workflow.ResearchScheduledInputV3
	if err := converter.GetDefaultDataConverter().FromPayload(payload, &got); err != nil {
		t.Fatalf("decode cutover payload: %v", err)
	}
	if got != want {
		t.Fatalf("cutover payload = %#v, want %#v", got, want)
	}
}

func TestResearchV3CutoverParamsEncoderFailsClosedWithoutReservedDecoder(t *testing.T) {
	s := New(nil, "", nil,
		WithTaskScheduleDataConverter("custom-json-v1", converter.GetDefaultDataConverter()))
	if _, err := s.researchV3CutoverParamsEncoder(); err == nil {
		t.Fatal("cutover encoder unexpectedly accepted a scheduler without the reserved decoder")
	}
}

type researchV3CutoverJournalFake struct {
	schedule          *types.Schedule
	head              types.ResearchV3DefinitionHead
	op                types.ResearchV3CutoverOperation
	recheckErr        error
	preflightErr      error
	revoked           bool
	advanceLostPhase  types.ResearchV3CutoverPhase
	advanceLost       bool
	advanceConflict   types.ResearchV3CutoverPhase
	advanceConcurrent types.ResearchV3CutoverPhase
	promoteConcurrent bool
	promoteErr        error
	onBegin           func()
	events            []string
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
		OriginalScheduleStatus:    p.OriginalScheduleStatus,
		PreflightDigest:           p.PreflightDigest,
		Phase:                     types.ResearchV3CutoverPrepared,
	}
	f.events = append(f.events, "begin")
	if f.onBegin != nil {
		f.onBegin()
	}
	return f.op, nil
}

func (f *researchV3CutoverJournalFake) LoadResearchV3CutoverAuthorityStatus(
	context.Context, types.ResearchV3CutoverOperation,
) (string, error) {
	if f.revoked {
		return "revoked", nil
	}
	if f.op.Phase == types.ResearchV3CutoverActive {
		return "enabled", nil
	}
	return "pending", nil
}

func (f *researchV3CutoverJournalFake) VerifyResearchV3CutoverDatabaseState(
	context.Context, types.ResearchV3CutoverOperation,
) error {
	return nil
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

func (f *researchV3CutoverJournalFake) PromoteResearchV3PreparedDefinition(
	_ context.Context, op types.ResearchV3CutoverOperation,
) (types.ResearchV3CutoverOperation, error) {
	if f.op.ID != op.ID || f.op.Phase != types.ResearchV3CutoverPaused {
		return types.ResearchV3CutoverOperation{}, types.ErrConflict
	}
	if f.promoteErr != nil {
		return types.ResearchV3CutoverOperation{}, f.promoteErr
	}
	f.op.Phase = types.ResearchV3CutoverDefinitionPromoted
	f.events = append(f.events, "phase:"+string(f.op.Phase))
	if f.promoteConcurrent {
		f.promoteConcurrent = false
		return types.ResearchV3CutoverOperation{}, types.ErrConflict
	}
	return f.op, nil
}

func (f *researchV3CutoverJournalFake) RestoreResearchV3OriginalDefinition(
	_ context.Context, op types.ResearchV3CutoverOperation,
) (types.ResearchV3CutoverOperation, error) {
	if f.op.ID != op.ID || f.op.Phase != types.ResearchV3CutoverRollbackPaused {
		return types.ResearchV3CutoverOperation{}, types.ErrConflict
	}
	f.op.Phase = types.ResearchV3CutoverDefinitionRestored
	f.events = append(f.events, "phase:"+string(f.op.Phase))
	return f.op, nil
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
			types.CodeConflict, "definition head drifted", types.ErrResearchV3CutoverDrift)
	}
	f.op.Phase = next
	f.events = append(f.events, "phase:"+string(next))
	if next == f.advanceConcurrent {
		f.advanceConcurrent = ""
		return types.ResearchV3CutoverOperation{}, types.ErrConflict
	}
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
	schedule         *schedulepb.Schedule
	token            int
	updates          []*schedulepb.Schedule
	lostAfterApply   map[int]bool
	requireRevoked   *bool
	requestReceipt   map[string]bool
	runningWorkflows []*commonpb.WorkflowExecution
	describeCalls    int
}

func (f *researchV3ScheduleRemoteFake) Describe(
	context.Context, string,
) (*workflowservice.DescribeScheduleResponse, error) {
	f.describeCalls++
	return &workflowservice.DescribeScheduleResponse{
		Schedule:      proto.Clone(f.schedule).(*schedulepb.Schedule),
		ConflictToken: []byte{byte(f.token)},
		Info: &schedulepb.ScheduleInfo{
			RunningWorkflows: append([]*commonpb.WorkflowExecution(nil),
				f.runningWorkflows...),
		},
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

func TestResearchV3CutoverPreservesPausedMondayTask(t *testing.T) {
	original := researchV3MondaySchedule(t)
	original.State.Paused = true
	original.State.Notes = "paused by owner"
	journal := researchV3CutoverJournalForTest()
	journal.schedule.Status = types.ScheduleStatusPaused
	journal.schedule.SpecJSON = []byte(`{"cron":"0 9 * * 1","tz":"Asia/Shanghai"}`)
	remote := &researchV3ScheduleRemoteFake{
		schedule: proto.Clone(original).(*schedulepb.Schedule),
	}
	coordinator := researchV3CutoverCoordinatorForTest(t, journal, remote)
	op, err := coordinator.Cutover(t.Context(), researchV3CutoverRequest{
		TaskID: "task-kimi", UserID: 42, IdempotencyKey: "paused-cutover",
	})
	if err != nil {
		t.Fatal(err)
	}
	if op.Phase != types.ResearchV3CutoverActive ||
		op.OriginalScheduleStatus != types.ScheduleStatusPaused ||
		!op.OriginalPaused || !remote.schedule.GetState().GetPaused() {
		t.Fatalf("operation=%+v temporal_paused=%t", op,
			remote.schedule.GetState().GetPaused())
	}
	if !proto.Equal(remote.schedule.GetSpec(), original.GetSpec()) ||
		!proto.Equal(remote.schedule.GetPolicies(), original.GetPolicies()) ||
		remote.schedule.GetAction().GetStartWorkflow().GetWorkflowId() !=
			original.GetAction().GetStartWorkflow().GetWorkflowId() {
		t.Fatal("paused cutover changed schedule identity, spec, or policy")
	}
	if remote.schedule.GetAction().GetStartWorkflow().GetWorkflowType().GetName() !=
		workflow.ResearchScheduledWorkflowV3Name {
		t.Fatal("paused cutover did not install V3 action")
	}
}

func TestResearchV3CutoverBindsPlanDigestAndReplaysSameKey(t *testing.T) {
	journal := researchV3CutoverJournalForTest()
	remote := &researchV3ScheduleRemoteFake{schedule: researchV3MondaySchedule(t)}
	coordinator := researchV3CutoverCoordinatorForTest(t, journal, remote)
	req := researchV3CutoverRequest{
		TaskID: "task-kimi", UserID: 42, IdempotencyKey: "digest-bound",
	}
	scope := types.ResearchV3OperatorScope{
		TenantID: journal.schedule.TenantID, UserID: journal.schedule.UserID,
		TaskID: journal.schedule.ID, Status: journal.schedule.Status,
		ExecutionMode: journal.schedule.ExecutionMode,
		SpecJSON:      append([]byte(nil), journal.schedule.SpecJSON...),
	}
	inspection, err := coordinator.Preflight(t.Context(), scope, req)
	if err != nil || len(inspection.PlanDigest) != 64 || !inspection.Ready {
		t.Fatalf("preflight=%+v err=%v", inspection, err)
	}
	if _, err := coordinator.CutoverWithPreflight(
		t.Context(), scope, req, strings.Repeat("f", 64)); types.CodeOf(err) != types.CodeConflict || journal.op.ID != 0 || len(remote.updates) != 0 {
		t.Fatalf("wrong digest err=%v operation=%d updates=%d",
			err, journal.op.ID, len(remote.updates))
	}
	otherKey := req
	otherKey.IdempotencyKey = "digest-bound-other"
	otherInspection, err := coordinator.Preflight(t.Context(), scope, otherKey)
	if err != nil || otherInspection.PlanDigest == inspection.PlanDigest {
		t.Fatalf("different-key preflight=%+v err=%v", otherInspection, err)
	}
	if _, err := coordinator.CutoverWithPreflight(
		t.Context(), scope, otherKey, inspection.PlanDigest); types.CodeOf(err) != types.CodeConflict || journal.op.ID != 0 || len(remote.updates) != 0 {
		t.Fatalf("cross-key digest replay err=%v operation=%d updates=%d",
			err, journal.op.ID, len(remote.updates))
	}
	op, err := coordinator.CutoverWithPreflight(
		t.Context(), scope, req, inspection.PlanDigest)
	if err != nil || op.Phase != types.ResearchV3CutoverActive {
		t.Fatalf("cutover=%+v err=%v", op, err)
	}
	updates := len(remote.updates)
	if _, err := coordinator.CutoverWithPreflight(
		t.Context(), scope, req, inspection.PlanDigest); err != nil ||
		len(remote.updates) != updates {
		t.Fatalf("same-key replay err=%v updates=%d/%d", err, len(remote.updates), updates)
	}
	if _, err := coordinator.CutoverWithPreflight(
		t.Context(), scope, req, strings.Repeat("e", 64)); types.CodeOf(err) != types.CodeConflict || len(remote.updates) != updates {
		t.Fatalf("replay digest drift err=%v updates=%d/%d", err, len(remote.updates), updates)
	}
}

func TestResearchV3CutoverRevalidatesExternalPreflightBeforeJournal(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*researchV3ScheduleRemoteFake)
	}{
		{
			name: "schedule drift",
			mutate: func(remote *researchV3ScheduleRemoteFake) {
				remote.schedule.Spec.CronString[0] = "0 10 * * 1"
			},
		},
		{
			name: "pause state drift",
			mutate: func(remote *researchV3ScheduleRemoteFake) {
				remote.schedule.State.Paused = true
			},
		},
		{
			name: "running workflow appeared",
			mutate: func(remote *researchV3ScheduleRemoteFake) {
				remote.runningWorkflows = []*commonpb.WorkflowExecution{{}}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			journal := researchV3CutoverJournalForTest()
			remote := &researchV3ScheduleRemoteFake{
				schedule: researchV3MondaySchedule(t),
			}
			coordinator := researchV3CutoverCoordinatorForTest(t, journal, remote)
			req := researchV3CutoverRequest{
				TaskID: "task-kimi", UserID: 42,
				IdempotencyKey: "adversarial-revalidation-" + test.name,
			}
			scope := types.ResearchV3OperatorScope{
				TenantID: journal.schedule.TenantID, UserID: journal.schedule.UserID,
				TaskID: journal.schedule.ID, Status: journal.schedule.Status,
				ExecutionMode: journal.schedule.ExecutionMode,
				SpecJSON:      append([]byte(nil), journal.schedule.SpecJSON...),
			}
			inspection, err := coordinator.Preflight(t.Context(), scope, req)
			if err != nil || !inspection.Ready || remote.describeCalls != 1 {
				t.Fatalf("initial preflight=%+v describes=%d err=%v",
					inspection, remote.describeCalls, err)
			}
			test.mutate(remote)
			if _, err := coordinator.CutoverWithPreflight(
				t.Context(), scope, req, inspection.PlanDigest,
			); types.CodeOf(err) != types.CodeConflict {
				t.Fatalf("drifted cutover error=%v, want conflict", err)
			}
			if journal.op.ID != 0 || len(remote.updates) != 0 || remote.describeCalls != 2 {
				t.Fatalf("drift crossed preflight fence: operation=%d CAS=%d describes=%d",
					journal.op.ID, len(remote.updates), remote.describeCalls)
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

func TestResearchV3CutoverConcurrentResumeAdoptsAdvancedCheckpoint(t *testing.T) {
	tests := []struct {
		name       string
		concurrent types.ResearchV3CutoverPhase
		promote    bool
	}{
		{name: "stale_prepared", concurrent: types.ResearchV3CutoverPauseRequested},
		{name: "stale_pause_requested_after_pause_cas", concurrent: types.ResearchV3CutoverPaused},
		{name: "stale_paused_promote", promote: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			journal := researchV3CutoverJournalForTest()
			journal.advanceConcurrent = tc.concurrent
			journal.promoteConcurrent = tc.promote
			original := researchV3MondaySchedule(t)
			remote := &researchV3ScheduleRemoteFake{
				schedule: proto.Clone(original).(*schedulepb.Schedule),
			}
			coordinator := researchV3CutoverCoordinatorForTest(t, journal, remote)
			op, err := coordinator.Cutover(t.Context(), researchV3CutoverRequest{
				TaskID: "task-kimi", UserID: 42, IdempotencyKey: "concurrent-" + tc.name,
			})
			if err != nil || op.Phase != types.ResearchV3CutoverActive || journal.revoked {
				t.Fatalf("err=%v phase=%s revoked=%t", err, op.Phase, journal.revoked)
			}
			if remote.schedule.GetState().GetPaused() ||
				remote.schedule.GetAction().GetStartWorkflow().GetWorkflowType().GetName() !=
					workflow.ResearchScheduledWorkflowV3Name {
				t.Fatalf("coordinator failed to converge after stale checkpoint: paused=%t workflow=%q",
					remote.schedule.GetState().GetPaused(),
					remote.schedule.GetAction().GetStartWorkflow().GetWorkflowType().GetName())
			}
		})
	}
}

func TestResearchV3CutoverReturnsDefinitionPromotionFailureWithoutLosingTaskIdentity(t *testing.T) {
	want := errors.New("deferred definition integrity rejected promotion")
	journal := researchV3CutoverJournalForTest()
	journal.promoteErr = want
	remote := &researchV3ScheduleRemoteFake{schedule: researchV3MondaySchedule(t)}
	coordinator := researchV3CutoverCoordinatorForTest(t, journal, remote)

	_, err := coordinator.Cutover(t.Context(), researchV3CutoverRequest{
		TaskID: "task-kimi", UserID: 42, IdempotencyKey: "promotion-failure",
	})
	if !errors.Is(err, want) {
		t.Fatalf("cutover error=%v, want promotion failure", err)
	}
	if remote.describeCalls != 4 {
		t.Fatalf("describe calls=%d, want no empty-ID recovery read", remote.describeCalls)
	}
	if journal.op.TaskID != "task-kimi" || journal.op.Phase != types.ResearchV3CutoverPaused {
		t.Fatalf("journal identity/phase changed: %+v", journal.op)
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
