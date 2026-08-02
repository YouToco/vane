package scheduler

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"

	"github.com/YouToco/vane/types"
	"github.com/YouToco/vane/workflow"
)

func TestResearchRuntimeV3AuthorityCanary_IsHardDisabled(t *testing.T) {
	legacy := makePushParams(
		7, 42, "task-v3", workflow.PushScope{SourceIDs: []int64{11}, TopN: 3},
		"Kimi availability",
	)

	for name, configure := range map[string]func(*Scheduler){
		"default hard dark": nil,
		"empty hard dark": func(s *Scheduler) {
			WithResearchRuntimeV3AuthorityCanary("")(s)
		},
		"blank direct option fails closed": func(s *Scheduler) {
			WithResearchRuntimeV3AuthorityCanary("   ")(s)
		},
		"different task remains legacy": func(s *Scheduler) {
			WithResearchRuntimeV3AuthorityCanary("task-other")(s)
		},
		"matching task still remains legacy": func(s *Scheduler) {
			WithResearchRuntimeV3AuthorityCanary("task-v3")(s)
		},
	} {
		t.Run(name, func(t *testing.T) {
			s := &Scheduler{}
			if configure != nil {
				configure(s)
			}
			got := s.actionParamsFor(legacy)
			if !reflect.DeepEqual(got, legacy) {
				t.Fatalf("unselected Action changed:\nwant=%+v\n got=%+v", legacy, got)
			}
		})
	}
}

func TestResearchRuntimeV3AuthorityCannotRewriteMondayNineSchedule(t *testing.T) {
	const taskID = "task-v3-monday-nine"
	initial := makePushParams(
		7, 42, taskID, workflow.PushScope{SourceIDs: []int64{21}, TopN: 4},
		"每周一九点 Kimi 情报",
	)
	h := &fakeScheduleHandle{current: reconcileSchedule(
		"wf-"+taskID, []interface{}{payloadArg(t, initial)},
	)}
	h.current.Spec.CronExpressions = []string{"0 9 * * 1"}
	h.current.Spec.TimeZoneName = "Asia/Shanghai"
	fc := &fakeTemporalClient{sched: &fakeScheduleClient{
		handles: map[string]*fakeScheduleHandle{taskID: h},
	}}
	st := &fakeScheduleStore{active: []types.Schedule{{
		ID: taskID, TenantID: 7, UserID: 42,
		NLDescription: "每周一九点 Kimi 情报",
		ScopeJSON:     json.RawMessage(`{"source_ids":[21],"top_n":4}`),
		Status:        types.ScheduleStatusActive, ExecutionMode: types.ExecutionModeCompiled,
	}}}
	s := New(fc, "tq", st, WithResearchRuntimeV3AuthorityCanary(taskID))

	if err := s.ReconcileActions(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(h.history) != 0 {
		t.Fatalf("hard-disabled authority rewrote Action %d times", len(h.history))
	}
	got, found, err := decodeScheduleActionPushParams(h.current.Action)
	if err != nil || !found {
		t.Fatalf("decode retained Action: found=%v err=%v", found, err)
	}
	if got.ExecutionMode != types.ExecutionModeCompiled || got.RuntimeVersion != "" ||
		got.NLDesc != "每周一九点 Kimi 情报" || got.Scope.TopN != 4 ||
		!reflect.DeepEqual(got.Scope.SourceIDs, []int64{21}) {
		t.Fatalf("authority changed retained Action: %+v", got)
	}
	assertMondayNineSpec(t, h)
}

func assertMondayNineSpec(t *testing.T, h *fakeScheduleHandle) {
	t.Helper()
	if h.current.Spec == nil ||
		!reflect.DeepEqual(h.current.Spec.CronExpressions, []string{"0 9 * * 1"}) ||
		h.current.Spec.TimeZoneName != "Asia/Shanghai" {
		t.Fatalf("Monday 09:00 ScheduleSpec changed: %+v", h.current.Spec)
	}
}

func TestResearchRuntimeV3AuthorityCannotRewriteRename(t *testing.T) {
	const taskID = "task-v3-rename"
	h := &fakeScheduleHandle{current: reconcileSchedule(
		"wf-"+taskID,
		[]interface{}{payloadArg(t, makePushParams(
			7, 42, taskID, workflow.PushScope{TopN: 5}, "old name",
		))},
	)}
	fc := &fakeTemporalClient{sched: &fakeScheduleClient{handle: h}}
	s := New(fc, "tq", &fakeScheduleStore{}, WithResearchRuntimeV3AuthorityCanary(taskID))
	name := "new name"
	if err := s.UpdatePush(
		t.Context(), taskID, 42,
		ScheduleSpec{Cron: "0 9 * * 1", TZ: "Asia/Shanghai"}, &name,
	); err != nil {
		t.Fatal(err)
	}
	got := h.current.Action.(*client.ScheduleWorkflowAction).Args[0].(workflow.PushParams)
	if got.RuntimeVersion != "" || got.ExecutionMode != types.ExecutionModeCompiled ||
		got.NLDesc != "new name" {
		t.Fatalf("authority rewrote renamed Action: %+v", got)
	}
}

func TestPrepareTaskSchedule_ResearchV3AuthorityCannotRewritePreparedAction(t *testing.T) {
	req := validTaskScheduleRequest()
	req.Spec = ScheduleSpec{Cron: "0 9 * * 1", TZ: "Asia/Shanghai"}
	taskID, err := TaskIDForOperation(req.TenantID, req.UserID, req.OperationID)
	if err != nil {
		t.Fatal(err)
	}

	legacyScheduler := newTaskScheduleTestScheduler(newTaskScheduleFakeClient())
	legacy := preparedTaskSchedule(t, legacyScheduler, req)
	v3Scheduler := newTaskScheduleTestScheduler(newTaskScheduleFakeClient())
	WithResearchRuntimeV3AuthorityCanary(taskID)(v3Scheduler.Scheduler)
	v3 := preparedTaskSchedule(t, v3Scheduler, req)

	if !reflect.DeepEqual(v3.Timing, legacy.Timing) {
		t.Fatalf("V3 rollout changed Monday 09:00 timing:\nlegacy=%+v\n    v3=%+v", legacy.Timing, v3.Timing)
	}
	params := v3.Action.Params
	if v3.FingerprintVersion != taskScheduleFingerprintVersionV1 ||
		params.TenantID != 0 || params.ExecutionMode != "" || params.RuntimeVersion != "" ||
		params.NLDesc != legacy.Action.Params.NLDesc ||
		!reflect.DeepEqual(params.Scope, legacy.Action.Params.Scope) || params.Snapshot != nil {
		t.Fatalf("authority rewrote prepared Action = %+v, fingerprint=%q", params, v3.FingerprintVersion)
	}
	if err := ValidatePreparedTaskScheduleRequest(v3, req); err != nil {
		t.Fatalf("validate prepared V3 checkpoint: %v", err)
	}
}

type researchShadowStore struct {
	scheduleStore
	schedule            *types.Schedule
	definitionAvailable bool
	definitionErr       error
	getCalls            int
	definitionCalls     int
}

func (s *researchShadowStore) GetSchedule(
	_ context.Context, taskID string, userID int64,
) (*types.Schedule, error) {
	s.getCalls++
	if s.schedule == nil || s.schedule.ID != taskID || s.schedule.UserID != userID {
		return nil, types.NewAppError(types.CodeNotFound, "not found", types.ErrNotFound)
	}
	copy := *s.schedule
	return &copy, nil
}

func (s *researchShadowStore) HasCurrentResearchApprovedDefinitionV3(
	_ context.Context, tenantID, userID int64, taskID string,
) (bool, error) {
	s.definitionCalls++
	if s.schedule == nil || s.schedule.TenantID != tenantID ||
		s.schedule.UserID != userID || s.schedule.ID != taskID {
		return false, types.ErrNotFound
	}
	return s.definitionAvailable, s.definitionErr
}

type researchShadowTemporalClient struct {
	client.Client
	executeCalls        int
	createdExecutions   int
	scheduleClientCalls int
	seenIDs             map[string]struct{}
	options             client.StartWorkflowOptions
	workflowType        interface{}
	args                []interface{}
}

func (c *researchShadowTemporalClient) ScheduleClient() client.ScheduleClient {
	c.scheduleClientCalls++
	return nil
}

func (c *researchShadowTemporalClient) ExecuteWorkflow(
	_ context.Context,
	options client.StartWorkflowOptions,
	workflowType interface{},
	args ...interface{},
) (client.WorkflowRun, error) {
	c.executeCalls++
	c.options = options
	c.workflowType = workflowType
	c.args = append([]interface{}(nil), args...)
	if c.seenIDs == nil {
		c.seenIDs = make(map[string]struct{})
	}
	if _, exists := c.seenIDs[options.ID]; exists {
		return nil, serviceerror.NewWorkflowExecutionAlreadyStarted(
			"already started", options.ID, "run-existing")
	}
	c.seenIDs[options.ID] = struct{}{}
	c.createdExecutions++
	return nil, nil
}

func TestTriggerResearchShadowNow_FailsClosedBeforeTemporal(t *testing.T) {
	const taskID = "task-v3-shadow"
	active := &types.Schedule{
		ID: taskID, TenantID: 7, UserID: 42,
		Status: types.ScheduleStatusActive,
	}
	tests := []struct {
		name       string
		configured string
		userID     int64
		schedule   *types.Schedule
		definition bool
	}{
		{name: "not configured", userID: 42, schedule: active, definition: true},
		{name: "different exact shadow", configured: "task-other", userID: 42, schedule: active, definition: true},
		{name: "cross owner", configured: taskID, userID: 99, schedule: active, definition: true},
		{name: "inactive", configured: taskID, userID: 42, schedule: &types.Schedule{
			ID: taskID, TenantID: 7, UserID: 42, Status: types.ScheduleStatusPaused,
		}, definition: true},
		{name: "no V3 definition", configured: taskID, userID: 42, schedule: active},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := &researchShadowStore{
				schedule: tc.schedule, definitionAvailable: tc.definition,
			}
			temporal := &researchShadowTemporalClient{}
			s := New(temporal, "vane-shadow", store,
				WithResearchRuntimeV3ShadowCanary(tc.configured))
			err := s.TriggerResearchShadowNow(
				t.Context(), taskID, tc.userID, "ops-attempt-1")
			if err == nil {
				t.Fatal("fail-closed shadow request succeeded")
			}
			if temporal.executeCalls != 0 || temporal.scheduleClientCalls != 0 {
				t.Fatalf("rejected request touched Temporal: execute=%d schedule=%d",
					temporal.executeCalls, temporal.scheduleClientCalls)
			}
		})
	}
}

func TestTriggerResearchShadowNow_IsNoDeliveryIdempotentAndScheduleIndependent(t *testing.T) {
	const taskID = "task-v3-shadow"
	store := &researchShadowStore{
		schedule: &types.Schedule{
			ID: taskID, TenantID: 7, UserID: 42,
			Status: types.ScheduleStatusActive,
		},
		definitionAvailable: true,
	}
	temporal := &researchShadowTemporalClient{}
	s := New(temporal, "vane-shadow", store,
		WithResearchRuntimeV3ShadowCanary(taskID))

	for i := 0; i < 2; i++ {
		if err := s.TriggerResearchShadowNow(
			t.Context(), taskID, 42, "ops-attempt-1"); err != nil {
			t.Fatalf("shadow trigger %d: %v", i+1, err)
		}
	}
	if temporal.executeCalls != 2 || temporal.createdExecutions != 1 ||
		len(temporal.seenIDs) != 1 {
		t.Fatalf("idempotency calls=%d created=%d IDs=%d",
			temporal.executeCalls, temporal.createdExecutions, len(temporal.seenIDs))
	}
	if temporal.scheduleClientCalls != 0 {
		t.Fatalf("shadow trigger read or modified Schedule service %d times",
			temporal.scheduleClientCalls)
	}
	if temporal.options.TaskQueue != "vane-shadow" ||
		!temporal.options.WorkflowExecutionErrorWhenAlreadyStarted ||
		len(temporal.args) != 1 {
		t.Fatalf("shadow start options/args = %+v / %+v", temporal.options, temporal.args)
	}
	input, ok := temporal.args[0].(workflow.ResearchShadowInputV3)
	if !ok || input.TenantID != 7 || input.UserID != 42 || input.TaskID != taskID {
		t.Fatalf("shadow input = %#v", temporal.args[0])
	}
	if reflect.ValueOf(temporal.workflowType).Pointer() !=
		reflect.ValueOf(workflow.ResearchShadowWorkflowV3).Pointer() {
		t.Fatalf("wrong shadow workflow type %T", temporal.workflowType)
	}
}
