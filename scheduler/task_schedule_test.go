package scheduler

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enums "go.temporal.io/api/enums/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	sdkpb "go.temporal.io/api/sdk/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/YouToco/vane/types"
	"github.com/YouToco/vane/workflow"
)

// taskScheduleFakeClient 是 A3 专用的有状态 Temporal 替身。它刻意模拟服务端而非
// 只返回预制值：Create 真正占用 ID，handle 的 Describe/Unpause/Delete 读写同一份
// schedule，因此并发、响应丢失和重试收敛测试不会因 fake 自己无状态而假绿。
type taskScheduleFakeClient struct {
	client.ScheduleClient

	mu             sync.Mutex
	schedules      map[string]*taskScheduleFakeRecord
	dc             converter.DataConverter
	namespaceID    string
	namespaceCalls int

	createCalls   int
	handleCalls   int
	describeCalls int
	unpauseCalls  int
	deleteCalls   int

	createErr           error
	createCommitErr     error
	createCollision     bool
	createReplaySuccess bool
	alreadyExistsError  error
	describeErr         error
	describeErrors      []error
	describeBlockAt     int
	describeEntered     chan struct{}
	describeRelease     chan struct{}
	rawMutate           func(*workflowservice.DescribeScheduleResponse)
	unpauseErr          error
	unpauseCommitErr    error
	unpauseNoCommit     bool
	unpauseDelete       bool
	deleteErr           error
	deleteCommitErr     error
	deleteNoCommit      bool
	afterCommit         func()
	rawRequests         taskScheduleRawRequests
}

type taskScheduleRawRequests struct {
	createNamespace     string
	createRequestID     string
	createIdentity      string
	updateNamespace     string
	updateRequestID     string
	updateIdentity      string
	updateNote          string
	updatePaused        bool
	updateConflictToken []byte
	deleteNamespace     string
	deleteIdentity      string
}

type taskScheduleFakeRecord struct {
	description         client.ScheduleDescription
	conflictToken       []byte
	createRequestID     string
	createConflictToken []byte
}

func newTaskScheduleFakeClient() *taskScheduleFakeClient {
	return &taskScheduleFakeClient{
		schedules:   make(map[string]*taskScheduleFakeRecord),
		dc:          converter.GetDefaultDataConverter(),
		namespaceID: "test-namespace-id",
		alreadyExistsError: serviceerror.NewWorkflowExecutionAlreadyStarted(
			"schedule already exists", "existing-request", "",
		),
	}
}

func (f *taskScheduleFakeClient) Create(
	ctx context.Context,
	opts client.ScheduleOptions,
) (client.ScheduleHandle, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	f.createCalls++
	if f.createErr != nil {
		return nil, f.createErr
	}
	// Simulate another process winning the ID between our preflight Describe
	// and Create. The winning process commits the exact same request.
	if f.createCollision {
		f.createCollision = false
		f.schedules[opts.ID] = &taskScheduleFakeRecord{
			description: descriptionFromOptions(opts, f.dc), conflictToken: []byte{1},
		}
	}
	if _, exists := f.schedules[opts.ID]; exists {
		return nil, f.alreadyExistsError
	}

	record := &taskScheduleFakeRecord{
		description: descriptionFromOptions(opts, f.dc), conflictToken: []byte{1},
	}
	f.schedules[opts.ID] = record
	if f.afterCommit != nil {
		f.afterCommit()
	}
	if f.createCommitErr != nil {
		return nil, f.createCommitErr
	}
	return &taskScheduleFakeHandle{client: f, id: opts.ID}, nil
}

func (f *taskScheduleFakeClient) GetHandle(
	_ context.Context,
	id string,
) client.ScheduleHandle {
	f.mu.Lock()
	f.handleCalls++
	f.mu.Unlock()
	return &taskScheduleFakeHandle{client: f, id: id}
}

func (f *taskScheduleFakeClient) counts() taskScheduleFakeCounts {
	f.mu.Lock()
	defer f.mu.Unlock()
	return taskScheduleFakeCounts{
		create:   f.createCalls,
		handle:   f.handleCalls,
		describe: f.describeCalls,
		unpause:  f.unpauseCalls,
		delete:   f.deleteCalls,
	}
}

func (f *taskScheduleFakeClient) setNamespaceID(id string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.namespaceID = id
}

func (f *taskScheduleFakeClient) namespaceDescribeCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.namespaceCalls
}

func (f *taskScheduleFakeClient) rawRequestSnapshot() taskScheduleRawRequests {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.rawRequests
}

type taskScheduleFakeCounts struct {
	create   int
	handle   int
	describe int
	unpause  int
	delete   int
}

// retainedV1PushParamsWire is the PushParams shape compiled into the binary
// immediately before C1b added its tenant/mode/runtime envelope.
type retainedV1PushParamsWire struct {
	UserID     int64                `json:"user_id"`
	RunKind    workflow.PushRunKind `json:"run_kind,omitempty"`
	ScheduleID string               `json:"schedule_id,omitempty"`
	Scope      workflow.PushScope   `json:"scope"`
	NLDesc     string               `json:"nl_desc,omitempty"`
}

func (f *taskScheduleFakeClient) snapshot(id string) (client.ScheduleDescription, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	record, ok := f.schedules[id]
	if !ok {
		return client.ScheduleDescription{}, false
	}
	return cloneTaskScheduleDescription(record.description), true
}

func (f *taskScheduleFakeClient) mutate(
	id string,
	fn func(*client.ScheduleDescription),
) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	record, ok := f.schedules[id]
	if !ok {
		return false
	}
	fn(&record.description)
	record.conflictToken = nextTaskScheduleConflictToken(record.conflictToken)
	return true
}

func (f *taskScheduleFakeClient) seed(req *workflowservice.CreateScheduleRequest) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.schedules[req.GetScheduleId()] = &taskScheduleFakeRecord{
		description:         clientDescriptionFromRawCreate(req),
		conflictToken:       []byte{1},
		createRequestID:     req.GetRequestId(),
		createConflictToken: []byte{1},
	}
}

func nextTaskScheduleConflictToken(current []byte) []byte {
	if len(current) == 0 {
		return []byte{1}
	}
	next := slices.Clone(current)
	next[len(next)-1]++
	return next
}

type taskScheduleFakeHandle struct {
	client.ScheduleHandle
	client *taskScheduleFakeClient
	id     string
}

func (h *taskScheduleFakeHandle) GetID() string { return h.id }

func (h *taskScheduleFakeHandle) Describe(ctx context.Context) (*client.ScheduleDescription, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	h.client.mu.Lock()
	h.client.describeCalls++
	describeCall := h.client.describeCalls
	block := h.client.describeBlockAt == describeCall
	entered := h.client.describeEntered
	release := h.client.describeRelease
	if block {
		h.client.mu.Unlock()
		if entered != nil {
			close(entered)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-release:
		}
		h.client.mu.Lock()
	}
	defer h.client.mu.Unlock()
	if len(h.client.describeErrors) > 0 {
		err := h.client.describeErrors[0]
		h.client.describeErrors = h.client.describeErrors[1:]
		if err != nil {
			return nil, err
		}
	}
	if h.client.describeErr != nil {
		return nil, h.client.describeErr
	}
	record, ok := h.client.schedules[h.id]
	if !ok {
		return nil, serviceerror.NewNotFound("schedule not found")
	}
	description := cloneTaskScheduleDescription(record.description)
	return &description, nil
}

func (h *taskScheduleFakeHandle) Unpause(
	ctx context.Context,
	opts client.ScheduleUnpauseOptions,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	h.client.mu.Lock()
	defer h.client.mu.Unlock()
	h.client.unpauseCalls++
	if h.client.unpauseErr != nil {
		return h.client.unpauseErr
	}
	record, ok := h.client.schedules[h.id]
	if !ok {
		return serviceerror.NewNotFound("schedule not found")
	}
	if h.client.unpauseDelete {
		delete(h.client.schedules, h.id)
		return nil
	}
	if h.client.unpauseNoCommit {
		return nil
	}
	if record.description.Schedule.State == nil {
		record.description.Schedule.State = &client.ScheduleState{}
	}
	record.description.Schedule.State.Paused = false
	record.description.Schedule.State.Note = opts.Note
	if h.client.afterCommit != nil {
		h.client.afterCommit()
	}
	if h.client.unpauseCommitErr != nil {
		return h.client.unpauseCommitErr
	}
	return nil
}

func (h *taskScheduleFakeHandle) Delete(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	h.client.mu.Lock()
	defer h.client.mu.Unlock()
	h.client.deleteCalls++
	if h.client.deleteErr != nil {
		return h.client.deleteErr
	}
	if _, ok := h.client.schedules[h.id]; !ok {
		return serviceerror.NewNotFound("schedule not found")
	}
	if h.client.deleteNoCommit {
		return nil
	}
	delete(h.client.schedules, h.id)
	if h.client.afterCommit != nil {
		h.client.afterCommit()
	}
	if h.client.deleteCommitErr != nil {
		return h.client.deleteCommitErr
	}
	return nil
}

func (f *taskScheduleFakeClient) createRaw(
	ctx context.Context,
	req *workflowservice.CreateScheduleRequest,
) (*workflowservice.CreateScheduleResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.createCalls++
	f.rawRequests.createNamespace = req.GetNamespace()
	f.rawRequests.createRequestID = req.GetRequestId()
	f.rawRequests.createIdentity = req.GetIdentity()
	if f.createErr != nil {
		return nil, f.createErr
	}
	if f.createCollision {
		f.createCollision = false
		creationRequestID := "other-request"
		if f.createReplaySuccess {
			creationRequestID = req.GetRequestId()
		}
		f.schedules[req.GetScheduleId()] = &taskScheduleFakeRecord{
			description:         clientDescriptionFromRawCreate(req),
			conflictToken:       []byte{1},
			createRequestID:     creationRequestID,
			createConflictToken: []byte{1},
		}
	}
	if record, exists := f.schedules[req.GetScheduleId()]; exists {
		if req.GetRequestId() == record.createRequestID {
			return &workflowservice.CreateScheduleResponse{
				ConflictToken: slices.Clone(record.createConflictToken),
			}, nil
		}
		return nil, f.alreadyExistsError
	}
	f.schedules[req.GetScheduleId()] = &taskScheduleFakeRecord{
		description:         clientDescriptionFromRawCreate(req),
		conflictToken:       []byte{1},
		createRequestID:     req.GetRequestId(),
		createConflictToken: []byte{1},
	}
	if f.afterCommit != nil {
		f.afterCommit()
	}
	if f.createCommitErr != nil {
		err := f.createCommitErr
		f.createCommitErr = nil
		return nil, err
	}
	return &workflowservice.CreateScheduleResponse{ConflictToken: []byte{1}}, nil
}

func (f *taskScheduleFakeClient) describeRaw(
	ctx context.Context,
	id string,
) (*workflowservice.DescribeScheduleResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	f.describeCalls++
	describeCall := f.describeCalls
	block := f.describeBlockAt == describeCall
	entered := f.describeEntered
	release := f.describeRelease
	if block {
		f.mu.Unlock()
		if entered != nil {
			close(entered)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-release:
		}
		f.mu.Lock()
	}
	defer f.mu.Unlock()
	if len(f.describeErrors) > 0 {
		err := f.describeErrors[0]
		f.describeErrors = f.describeErrors[1:]
		if err != nil {
			return nil, err
		}
	}
	if f.describeErr != nil {
		return nil, f.describeErr
	}
	record, ok := f.schedules[id]
	if !ok {
		return nil, serviceerror.NewNotFound("schedule not found")
	}
	response := rawDescriptionFromClient(record.description, f.dc)
	response.ConflictToken = slices.Clone(record.conflictToken)
	if f.rawMutate != nil {
		f.rawMutate(response)
	}
	return response, nil
}

func (f *taskScheduleFakeClient) updateRaw(
	ctx context.Context,
	req *workflowservice.UpdateScheduleRequest,
) (*workflowservice.UpdateScheduleResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.unpauseCalls++
	f.rawRequests.updateNamespace = req.GetNamespace()
	f.rawRequests.updateRequestID = req.GetRequestId()
	f.rawRequests.updateIdentity = req.GetIdentity()
	f.rawRequests.updateConflictToken = slices.Clone(req.GetConflictToken())
	f.rawRequests.updatePaused = req.GetSchedule().GetState().GetPaused()
	f.rawRequests.updateNote = req.GetSchedule().GetState().GetNotes()
	if f.unpauseErr != nil {
		return nil, f.unpauseErr
	}
	record, ok := f.schedules[req.GetScheduleId()]
	if !ok {
		return nil, serviceerror.NewNotFound("schedule not found")
	}
	if !bytes.Equal(req.GetConflictToken(), record.conflictToken) {
		return nil, serviceerror.NewFailedPrecondition("schedule conflict token is stale")
	}
	if f.unpauseDelete {
		delete(f.schedules, req.GetScheduleId())
		return &workflowservice.UpdateScheduleResponse{}, nil
	}
	if f.unpauseNoCommit {
		return &workflowservice.UpdateScheduleResponse{}, nil
	}
	updated := clientDescriptionFromRawCreate(&workflowservice.CreateScheduleRequest{
		Schedule:         req.GetSchedule(),
		Memo:             record.description.Memo,
		SearchAttributes: record.description.SearchAttributes,
	})
	updated.Info = record.description.Info
	record.description = updated
	record.conflictToken = nextTaskScheduleConflictToken(record.conflictToken)
	if f.afterCommit != nil {
		f.afterCommit()
	}
	if f.unpauseCommitErr != nil {
		return nil, f.unpauseCommitErr
	}
	return &workflowservice.UpdateScheduleResponse{}, nil
}

func (f *taskScheduleFakeClient) deleteRaw(
	ctx context.Context,
	req *workflowservice.DeleteScheduleRequest,
) (*workflowservice.DeleteScheduleResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleteCalls++
	f.rawRequests.deleteNamespace = req.GetNamespace()
	f.rawRequests.deleteIdentity = req.GetIdentity()
	if f.deleteErr != nil {
		return nil, f.deleteErr
	}
	if _, ok := f.schedules[req.GetScheduleId()]; !ok {
		return nil, serviceerror.NewNotFound("schedule not found")
	}
	if f.deleteNoCommit {
		return &workflowservice.DeleteScheduleResponse{}, nil
	}
	delete(f.schedules, req.GetScheduleId())
	if f.afterCommit != nil {
		f.afterCommit()
	}
	if f.deleteCommitErr != nil {
		return nil, f.deleteCommitErr
	}
	return &workflowservice.DeleteScheduleResponse{}, nil
}

func rawDescriptionFromClient(
	desc client.ScheduleDescription,
	dc converter.DataConverter,
) *workflowservice.DescribeScheduleResponse {
	return &workflowservice.DescribeScheduleResponse{
		Schedule: &schedulepb.Schedule{
			Spec:     rawScheduleSpecFromClient(desc.Schedule.Spec),
			Action:   rawScheduleActionFromClient(desc.Schedule.Action, dc),
			Policies: rawSchedulePoliciesFromClient(desc.Schedule.Policy),
			State:    rawScheduleStateFromClient(desc.Schedule.State),
		},
		Info:             rawScheduleInfoFromClient(desc.Info),
		Memo:             desc.Memo,
		SearchAttributes: desc.SearchAttributes,
	}
}

func rawScheduleInfoFromClient(info client.ScheduleInfo) *schedulepb.ScheduleInfo {
	raw := &schedulepb.ScheduleInfo{
		ActionCount:         int64(info.NumActions),
		MissedCatchupWindow: int64(info.NumActionsMissedCatchupWindow),
		OverlapSkipped:      int64(info.NumActionsSkippedOverlap),
		RunningWorkflows:    rawRunningWorkflows(info.RunningWorkflows),
		RecentActions:       rawRecentActions(info.RecentActions),
	}
	if !info.CreatedAt.IsZero() {
		raw.CreateTime = timestamppb.New(info.CreatedAt)
	}
	if !info.LastUpdateAt.IsZero() {
		raw.UpdateTime = timestamppb.New(info.LastUpdateAt)
	}
	return raw
}

func rawSchedulePoliciesFromClient(policies *client.SchedulePolicies) *schedulepb.SchedulePolicies {
	if policies == nil {
		return nil
	}
	return &schedulepb.SchedulePolicies{
		OverlapPolicy:  policies.Overlap,
		CatchupWindow:  durationpb.New(policies.CatchupWindow),
		PauseOnFailure: policies.PauseOnFailure,
	}
}

func rawScheduleStateFromClient(state *client.ScheduleState) *schedulepb.ScheduleState {
	if state == nil {
		return nil
	}
	return &schedulepb.ScheduleState{
		Notes:            state.Note,
		Paused:           state.Paused,
		LimitedActions:   state.LimitedActions,
		RemainingActions: int64(state.RemainingActions),
	}
}

func rawScheduleSpecFromClient(spec *client.ScheduleSpec) *schedulepb.ScheduleSpec {
	if spec == nil {
		return nil
	}
	raw := &schedulepb.ScheduleSpec{
		StructuredCalendar:        rawCalendarsFromClient(spec.Calendars),
		CronString:                append([]string(nil), spec.CronExpressions...),
		ExcludeStructuredCalendar: rawCalendarsFromClient(spec.Skip),
		Jitter:                    durationpb.New(spec.Jitter),
		TimezoneName:              spec.TimeZoneName,
	}
	for _, interval := range spec.Intervals {
		raw.Interval = append(raw.Interval, &schedulepb.IntervalSpec{
			Interval: durationpb.New(interval.Every),
			Phase:    durationpb.New(interval.Offset),
		})
	}
	if !spec.StartAt.IsZero() {
		raw.StartTime = timestamppb.New(spec.StartAt)
	}
	if !spec.EndAt.IsZero() {
		raw.EndTime = timestamppb.New(spec.EndAt)
	}
	return raw
}

func rawCalendarsFromClient(calendars []client.ScheduleCalendarSpec) []*schedulepb.StructuredCalendarSpec {
	raw := make([]*schedulepb.StructuredCalendarSpec, len(calendars))
	for i, calendar := range calendars {
		raw[i] = &schedulepb.StructuredCalendarSpec{
			Second:     rawRangesFromClient(calendar.Second),
			Minute:     rawRangesFromClient(calendar.Minute),
			Hour:       rawRangesFromClient(calendar.Hour),
			DayOfMonth: rawRangesFromClient(calendar.DayOfMonth),
			Month:      rawRangesFromClient(calendar.Month),
			Year:       rawRangesFromClient(calendar.Year),
			DayOfWeek:  rawRangesFromClient(calendar.DayOfWeek),
			Comment:    calendar.Comment,
		}
	}
	return raw
}

func rawRangesFromClient(ranges []client.ScheduleRange) []*schedulepb.Range {
	raw := make([]*schedulepb.Range, len(ranges))
	for i, item := range ranges {
		raw[i] = &schedulepb.Range{Start: int32(item.Start), End: int32(item.End), Step: int32(item.Step)}
	}
	return raw
}

func rawScheduleActionFromClient(
	action client.ScheduleAction,
	dc converter.DataConverter,
) *schedulepb.ScheduleAction {
	wf, ok := action.(*client.ScheduleWorkflowAction)
	if !ok || wf == nil {
		return &schedulepb.ScheduleAction{}
	}
	payloads := make([]*commonpb.Payload, len(wf.Args))
	for i, arg := range wf.Args {
		payload, encoded := arg.(*commonpb.Payload)
		if !encoded {
			var err error
			payload, err = dc.ToPayload(arg)
			if err != nil {
				panic(err)
			}
		}
		payloads[i] = payload
	}
	start := &workflowpb.NewWorkflowExecutionInfo{
		WorkflowId:               wf.ID,
		WorkflowType:             &commonpb.WorkflowType{Name: fmt.Sprint(wf.Workflow)},
		TaskQueue:                &taskqueuepb.TaskQueue{Name: wf.TaskQueue, Kind: enums.TASK_QUEUE_KIND_NORMAL},
		Input:                    &commonpb.Payloads{Payloads: payloads},
		WorkflowExecutionTimeout: durationpb.New(wf.WorkflowExecutionTimeout),
		WorkflowRunTimeout:       durationpb.New(wf.WorkflowRunTimeout),
		WorkflowTaskTimeout:      durationpb.New(wf.WorkflowTaskTimeout),
		WorkflowIdReusePolicy:    enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
		Memo:                     rawMemoFromClient(wf.Memo, dc),
		SearchAttributes:         rawSearchAttributesFromClient(wf),
		Priority: &commonpb.Priority{
			PriorityKey:    int32(wf.Priority.PriorityKey),
			FairnessKey:    wf.Priority.FairnessKey,
			FairnessWeight: float32(wf.Priority.FairnessWeight),
		},
	}
	if wf.RetryPolicy != nil {
		start.RetryPolicy = &commonpb.RetryPolicy{}
	}
	if wf.VersioningOverride != nil {
		start.VersioningOverride = &workflowpb.VersioningOverride{}
	}
	if wf.StaticSummary != "" || wf.StaticDetails != "" {
		start.UserMetadata = &sdkpb.UserMetadata{}
		if wf.StaticSummary != "" {
			start.UserMetadata.Summary = mustPayload(dc, wf.StaticSummary)
		}
		if wf.StaticDetails != "" {
			start.UserMetadata.Details = mustPayload(dc, wf.StaticDetails)
		}
	}
	return &schedulepb.ScheduleAction{Action: &schedulepb.ScheduleAction_StartWorkflow{StartWorkflow: start}}
}

func rawMemoFromClient(values map[string]interface{}, dc converter.DataConverter) *commonpb.Memo {
	if len(values) == 0 {
		return nil
	}
	fields := make(map[string]*commonpb.Payload, len(values))
	for key, value := range values {
		if payload, ok := value.(*commonpb.Payload); ok {
			fields[key] = payload
		} else {
			fields[key] = mustPayload(dc, value)
		}
	}
	return &commonpb.Memo{Fields: fields}
}

func rawSearchAttributesFromClient(wf *client.ScheduleWorkflowAction) *commonpb.SearchAttributes {
	fields := make(map[string]*commonpb.Payload, len(wf.UntypedSearchAttributes)+wf.TypedSearchAttributes.Size())
	for key, payload := range wf.UntypedSearchAttributes {
		fields[key] = payload
	}
	if wf.TypedSearchAttributes.Size() != 0 {
		fields["typed-placeholder"] = &commonpb.Payload{}
	}
	if len(fields) == 0 {
		return nil
	}
	return &commonpb.SearchAttributes{IndexedFields: fields}
}

func mustPayload(dc converter.DataConverter, value interface{}) *commonpb.Payload {
	payload, err := dc.ToPayload(value)
	if err != nil {
		panic(err)
	}
	return payload
}

func rawRunningWorkflows(in []client.ScheduleWorkflowExecution) []*commonpb.WorkflowExecution {
	out := make([]*commonpb.WorkflowExecution, len(in))
	for i, execution := range in {
		out[i] = &commonpb.WorkflowExecution{
			WorkflowId: execution.WorkflowID,
			RunId:      execution.FirstExecutionRunID,
		}
	}
	return out
}

func rawRecentActions(in []client.ScheduleActionResult) []*schedulepb.ScheduleActionResult {
	out := make([]*schedulepb.ScheduleActionResult, len(in))
	for i := range in {
		out[i] = &schedulepb.ScheduleActionResult{}
	}
	return out
}

func clientDescriptionFromRawCreate(req *workflowservice.CreateScheduleRequest) client.ScheduleDescription {
	schedule := req.GetSchedule()
	if schedule == nil {
		return client.ScheduleDescription{Memo: req.GetMemo(), SearchAttributes: req.GetSearchAttributes()}
	}
	return client.ScheduleDescription{
		Schedule: client.Schedule{
			Spec:   clientScheduleSpecFromRaw(schedule.GetSpec()),
			Action: clientScheduleActionFromRaw(schedule.GetAction()),
			Policy: clientSchedulePoliciesFromRaw(schedule.GetPolicies()),
			State:  clientScheduleStateFromRaw(schedule.GetState()),
		},
		Memo:             req.GetMemo(),
		SearchAttributes: req.GetSearchAttributes(),
	}
}

func clientScheduleSpecFromRaw(spec *schedulepb.ScheduleSpec) *client.ScheduleSpec {
	if spec == nil {
		return nil
	}
	out := &client.ScheduleSpec{
		Calendars:       clientCalendarsFromRaw(spec.GetStructuredCalendar()),
		CronExpressions: append([]string(nil), spec.GetCronString()...),
		Skip:            clientCalendarsFromRaw(spec.GetExcludeStructuredCalendar()),
		Jitter:          spec.GetJitter().AsDuration(),
		TimeZoneName:    spec.GetTimezoneName(),
	}
	for _, interval := range spec.GetInterval() {
		out.Intervals = append(out.Intervals, client.ScheduleIntervalSpec{
			Every: interval.GetInterval().AsDuration(), Offset: interval.GetPhase().AsDuration(),
		})
	}
	if spec.GetStartTime() != nil {
		out.StartAt = spec.GetStartTime().AsTime()
	}
	if spec.GetEndTime() != nil {
		out.EndAt = spec.GetEndTime().AsTime()
	}
	return out
}

func clientCalendarsFromRaw(raw []*schedulepb.StructuredCalendarSpec) []client.ScheduleCalendarSpec {
	out := make([]client.ScheduleCalendarSpec, len(raw))
	for i, calendar := range raw {
		out[i] = client.ScheduleCalendarSpec{
			Second: clientRangesFromRaw(calendar.GetSecond()), Minute: clientRangesFromRaw(calendar.GetMinute()),
			Hour: clientRangesFromRaw(calendar.GetHour()), DayOfMonth: clientRangesFromRaw(calendar.GetDayOfMonth()),
			Month: clientRangesFromRaw(calendar.GetMonth()), Year: clientRangesFromRaw(calendar.GetYear()),
			DayOfWeek: clientRangesFromRaw(calendar.GetDayOfWeek()), Comment: calendar.GetComment(),
		}
	}
	return out
}

func clientRangesFromRaw(raw []*schedulepb.Range) []client.ScheduleRange {
	out := make([]client.ScheduleRange, len(raw))
	for i, item := range raw {
		out[i] = client.ScheduleRange{Start: int(item.GetStart()), End: int(item.GetEnd()), Step: int(item.GetStep())}
	}
	return out
}

func clientScheduleActionFromRaw(action *schedulepb.ScheduleAction) client.ScheduleAction {
	start := action.GetStartWorkflow()
	if start == nil {
		return nil
	}
	args := make([]interface{}, len(start.GetInput().GetPayloads()))
	for i, payload := range start.GetInput().GetPayloads() {
		args[i] = payload
	}
	memo := make(map[string]interface{}, len(start.GetMemo().GetFields()))
	for key, payload := range start.GetMemo().GetFields() {
		memo[key] = payload
	}
	return &client.ScheduleWorkflowAction{
		ID:                       start.GetWorkflowId(),
		Workflow:                 start.GetWorkflowType().GetName(),
		Args:                     args,
		TaskQueue:                start.GetTaskQueue().GetName(),
		WorkflowExecutionTimeout: start.GetWorkflowExecutionTimeout().AsDuration(),
		WorkflowRunTimeout:       start.GetWorkflowRunTimeout().AsDuration(),
		WorkflowTaskTimeout:      start.GetWorkflowTaskTimeout().AsDuration(),
		Memo:                     memo,
	}
}

func clientSchedulePoliciesFromRaw(policies *schedulepb.SchedulePolicies) *client.SchedulePolicies {
	if policies == nil {
		return nil
	}
	return &client.SchedulePolicies{
		Overlap: policies.GetOverlapPolicy(), CatchupWindow: policies.GetCatchupWindow().AsDuration(),
		PauseOnFailure: policies.GetPauseOnFailure(),
	}
}

func clientScheduleStateFromRaw(state *schedulepb.ScheduleState) *client.ScheduleState {
	if state == nil {
		return nil
	}
	return &client.ScheduleState{
		Note: state.GetNotes(), Paused: state.GetPaused(), LimitedActions: state.GetLimitedActions(),
		RemainingActions: int(state.GetRemainingActions()),
	}
}

func descriptionFromOptions(
	opts client.ScheduleOptions,
	dc converter.DataConverter,
) client.ScheduleDescription {
	memo := &commonpb.Memo{Fields: make(map[string]*commonpb.Payload, len(opts.Memo))}
	for key, value := range opts.Memo {
		payload, ok := value.(*commonpb.Payload)
		if !ok {
			var err error
			payload, err = dc.ToPayload(value)
			if err != nil {
				panic(err)
			}
		}
		memo.Fields[key] = payload
	}
	return client.ScheduleDescription{
		Schedule: client.Schedule{
			Action: serverTaskScheduleAction(opts.Action, dc),
			Spec:   cloneTaskScheduleSpec(&opts.Spec),
			Policy: &client.SchedulePolicies{
				Overlap:        opts.Overlap,
				CatchupWindow:  opts.CatchupWindow,
				PauseOnFailure: opts.PauseOnFailure,
			},
			State: &client.ScheduleState{Note: opts.Note, Paused: opts.Paused},
		},
		Memo: memo,
	}
}

// serverTaskScheduleAction models the important Create -> Describe boundary:
// Temporal returns workflow arguments as encoded Payload values, not the
// original Go values passed to ScheduleOptions.
func serverTaskScheduleAction(
	action client.ScheduleAction,
	dc converter.DataConverter,
) client.ScheduleAction {
	wf, ok := action.(*client.ScheduleWorkflowAction)
	if !ok || wf == nil {
		return action
	}
	copy := *wf
	copy.Args = make([]interface{}, len(wf.Args))
	for i, arg := range wf.Args {
		payload, ok := arg.(*commonpb.Payload)
		if !ok {
			var err error
			payload, err = dc.ToPayload(arg)
			if err != nil {
				panic(err)
			}
		}
		copy.Args[i] = payload
	}
	return &copy
}

func cloneTaskScheduleDescription(in client.ScheduleDescription) client.ScheduleDescription {
	out := in
	out.Schedule.Action = cloneTaskScheduleAction(in.Schedule.Action)
	out.Schedule.Spec = cloneTaskScheduleSpec(in.Schedule.Spec)
	if in.Schedule.Policy != nil {
		policy := *in.Schedule.Policy
		out.Schedule.Policy = &policy
	}
	if in.Schedule.State != nil {
		state := *in.Schedule.State
		out.Schedule.State = &state
	}
	if in.Memo != nil {
		out.Memo = &commonpb.Memo{Fields: make(map[string]*commonpb.Payload, len(in.Memo.Fields))}
		for key, payload := range in.Memo.Fields {
			out.Memo.Fields[key] = payload
		}
	}
	out.Info.RunningWorkflows = append([]client.ScheduleWorkflowExecution(nil), in.Info.RunningWorkflows...)
	out.Info.RecentActions = append([]client.ScheduleActionResult(nil), in.Info.RecentActions...)
	out.Info.NextActionTimes = append([]time.Time(nil), in.Info.NextActionTimes...)
	return out
}

func cloneTaskScheduleAction(action client.ScheduleAction) client.ScheduleAction {
	wf, ok := action.(*client.ScheduleWorkflowAction)
	if !ok || wf == nil {
		return action
	}
	copy := *wf
	copy.Args = append([]interface{}(nil), wf.Args...)
	return &copy
}

func cloneTaskScheduleSpec(spec *client.ScheduleSpec) *client.ScheduleSpec {
	if spec == nil {
		return nil
	}
	copy := *spec
	copy.Calendars = append([]client.ScheduleCalendarSpec(nil), spec.Calendars...)
	copy.Intervals = append([]client.ScheduleIntervalSpec(nil), spec.Intervals...)
	copy.CronExpressions = append([]string(nil), spec.CronExpressions...)
	copy.Skip = append([]client.ScheduleCalendarSpec(nil), spec.Skip...)
	return &copy
}

type taskScheduleTemporalClient struct {
	client.Client
	schedules *taskScheduleFakeClient
}

func (c *taskScheduleTemporalClient) ScheduleClient() client.ScheduleClient { return c.schedules }

func (c *taskScheduleTemporalClient) WorkflowService() workflowservice.WorkflowServiceClient {
	return &taskScheduleFakeWorkflowService{client: c.schedules}
}

type taskScheduleFakeWorkflowService struct {
	workflowservice.WorkflowServiceClient
	client *taskScheduleFakeClient
}

func (s *taskScheduleFakeWorkflowService) DescribeSchedule(
	ctx context.Context,
	req *workflowservice.DescribeScheduleRequest,
	_ ...grpc.CallOption,
) (*workflowservice.DescribeScheduleResponse, error) {
	return s.client.describeRaw(ctx, req.GetScheduleId())
}

func (s *taskScheduleFakeWorkflowService) DescribeNamespace(
	ctx context.Context,
	_ *workflowservice.DescribeNamespaceRequest,
	_ ...grpc.CallOption,
) (*workflowservice.DescribeNamespaceResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.client.mu.Lock()
	defer s.client.mu.Unlock()
	s.client.namespaceCalls++
	return &workflowservice.DescribeNamespaceResponse{
		NamespaceInfo: &namespacepb.NamespaceInfo{Id: s.client.namespaceID},
	}, nil
}

func (s *taskScheduleFakeWorkflowService) CreateSchedule(
	ctx context.Context,
	req *workflowservice.CreateScheduleRequest,
	_ ...grpc.CallOption,
) (*workflowservice.CreateScheduleResponse, error) {
	return s.client.createRaw(ctx, req)
}

func (s *taskScheduleFakeWorkflowService) UpdateSchedule(
	ctx context.Context,
	req *workflowservice.UpdateScheduleRequest,
	_ ...grpc.CallOption,
) (*workflowservice.UpdateScheduleResponse, error) {
	return s.client.updateRaw(ctx, req)
}

func (s *taskScheduleFakeWorkflowService) DeleteSchedule(
	ctx context.Context,
	req *workflowservice.DeleteScheduleRequest,
	_ ...grpc.CallOption,
) (*workflowservice.DeleteScheduleResponse, error) {
	return s.client.deleteRaw(ctx, req)
}

// taskScheduleTestScheduler keeps the pre-Prepared tests compact while still
// traversing the public compiler boundary before every lifecycle call. Tests
// for freezing and environment drift call the embedded Scheduler directly.
type taskScheduleTestScheduler struct {
	*Scheduler
	fake *taskScheduleFakeClient
}

type toolRuntimeCapabilityScheduleStore struct {
	scheduleStore
	available bool
	err       error
}

func (s *toolRuntimeCapabilityScheduleStore) HasCurrentToolApprovedDefinition(
	context.Context,
	int64, int64,
	string,
) (bool, error) {
	return s.available, s.err
}

type taskScheduleConverterRecorder struct {
	mu      sync.Mutex
	encoded []converter.WorkflowSerializationContext
	decoded []converter.WorkflowSerializationContext
}

type taskScheduleXORDataConverter struct {
	converter.DataConverter
	recorder *taskScheduleConverterRecorder
	context  converter.WorkflowSerializationContext
}

type taskScheduleRequestAwareDataConverter struct{ converter.DataConverter }

type taskScheduleNilContextDataConverter struct{ converter.DataConverter }

func (c taskScheduleRequestAwareDataConverter) WithContext(context.Context) converter.DataConverter {
	return c
}

func (taskScheduleNilContextDataConverter) WithSerializationContext(
	converter.SerializationContext,
) converter.DataConverter {
	return nil
}

func (c taskScheduleXORDataConverter) WithSerializationContext(
	serializationContext converter.SerializationContext,
) converter.DataConverter {
	copy := c
	switch typed := serializationContext.(type) {
	case converter.WorkflowSerializationContext:
		copy.context = typed
	case *converter.WorkflowSerializationContext:
		if typed != nil {
			copy.context = *typed
		}
	}
	return copy
}

func (c taskScheduleXORDataConverter) ToPayload(value interface{}) (*commonpb.Payload, error) {
	payload, err := c.DataConverter.ToPayload(value)
	if err != nil {
		return nil, err
	}
	if c.recorder != nil {
		c.recorder.mu.Lock()
		c.recorder.encoded = append(c.recorder.encoded, c.context)
		c.recorder.mu.Unlock()
	}
	marker := sha256.Sum256([]byte(c.context.Namespace + "\x00" + c.context.WorkflowID))
	encoded := commonpb.Payload{
		Metadata: maps.Clone(payload.Metadata),
		Data:     append(append([]byte(nil), marker[:]...), payload.Data...),
	}
	for i := range encoded.Data {
		encoded.Data[i] ^= 0x5a
	}
	return &encoded, nil
}

func (c taskScheduleXORDataConverter) FromPayload(
	payload *commonpb.Payload,
	valuePtr interface{},
) error {
	if c.recorder != nil {
		c.recorder.mu.Lock()
		c.recorder.decoded = append(c.recorder.decoded, c.context)
		c.recorder.mu.Unlock()
	}
	decoded := commonpb.Payload{
		Metadata: maps.Clone(payload.Metadata),
		Data:     append([]byte(nil), payload.Data...),
	}
	for i := range decoded.Data {
		decoded.Data[i] ^= 0x5a
	}
	if len(decoded.Data) < sha256.Size {
		return errors.New("contextual payload is missing its context marker")
	}
	wantMarker := sha256.Sum256([]byte(c.context.Namespace + "\x00" + c.context.WorkflowID))
	if !reflect.DeepEqual(decoded.Data[:sha256.Size], wantMarker[:]) {
		return fmt.Errorf("serialization context mismatch: namespace=%q workflow_id=%q", c.context.Namespace, c.context.WorkflowID)
	}
	decoded.Data = decoded.Data[sha256.Size:]
	return c.DataConverter.FromPayload(&decoded, valuePtr)
}

func newTaskScheduleTestScheduler(fake *taskScheduleFakeClient) *taskScheduleTestScheduler {
	return newTaskScheduleTestSchedulerWithTaskQueue(fake, "vane-task-test")
}

func newTaskScheduleTestSchedulerWithTaskQueue(
	fake *taskScheduleFakeClient,
	taskQueue string,
) *taskScheduleTestScheduler {
	s := New(
		&taskScheduleTemporalClient{schedules: fake},
		taskQueue,
		nil,
		WithTaskScheduleNamespace("test-namespace"),
	)
	s.taskScheduleEnv.namespaceIDOverride = "test-namespace-id"
	return &taskScheduleTestScheduler{Scheduler: s, fake: fake}
}

func (s *taskScheduleTestScheduler) prepare(
	ctx context.Context,
	req TaskScheduleRequest,
) (PreparedTaskSchedule, error) {
	return s.Scheduler.PrepareTaskSchedule(ctx, req)
}

func (s *taskScheduleTestScheduler) EnsurePausedTask(
	ctx context.Context,
	req TaskScheduleRequest,
) (EnsurePausedTaskResult, error) {
	prepared, err := s.prepare(ctx, req)
	if err != nil {
		return EnsurePausedTaskResult{}, err
	}
	return s.Scheduler.EnsurePausedTask(ctx, prepared)
}

func (s *taskScheduleTestScheduler) DescribeTask(
	ctx context.Context,
	req TaskScheduleRequest,
) (TaskScheduleSnapshot, error) {
	prepared, err := s.prepare(ctx, req)
	if err != nil {
		return TaskScheduleSnapshot{}, err
	}
	return s.Scheduler.DescribeTask(ctx, prepared)
}

func (s *taskScheduleTestScheduler) ActivateTask(
	ctx context.Context,
	req TaskScheduleRequest,
) (TaskScheduleSnapshot, error) {
	prepared, err := s.prepare(ctx, req)
	if err != nil {
		return TaskScheduleSnapshot{}, err
	}
	expected, err := s.Scheduler.buildTaskScheduleExpected(ctx, prepared, "test_receipt", false)
	if err != nil {
		return TaskScheduleSnapshot{}, err
	}
	return s.Scheduler.ActivateTask(ctx, prepared, taskScheduleFakeActivationReceipt(s.fake, expected))
}

func (s *taskScheduleTestScheduler) DeleteTask(
	ctx context.Context,
	req TaskScheduleRequest,
) error {
	prepared, err := s.prepare(ctx, req)
	if err != nil {
		return err
	}
	return s.Scheduler.DeleteTask(ctx, prepared)
}

func validTaskScheduleRequest() TaskScheduleRequest {
	return TaskScheduleRequest{
		TenantID:       7,
		UserID:         42,
		OperationID:    "operation-0001",
		Spec:           ScheduleSpec{Cron: "15 8 * * *", TZ: "Asia/Shanghai"},
		Scope:          workflow.PushScope{SourceIDs: []int64{11, 22}, TopN: 3},
		NLDescription:  "每日 AI 情报",
		PreparedDigest: strings.Repeat("a", 64),
	}
}

func preparedTaskSchedule(
	t *testing.T,
	scheduler *taskScheduleTestScheduler,
	req TaskScheduleRequest,
) PreparedTaskSchedule {
	t.Helper()
	prepared, err := scheduler.Scheduler.PrepareTaskSchedule(context.Background(), req)
	if err != nil {
		t.Fatalf("PrepareTaskSchedule: %v", err)
	}
	return prepared
}

func retainedV1PreparedTaskSchedule(
	t *testing.T,
	scheduler *taskScheduleTestScheduler,
	req TaskScheduleRequest,
) PreparedTaskSchedule {
	t.Helper()
	prepared := preparedTaskSchedule(t, scheduler, req)
	prepared.FingerprintVersion = taskScheduleFingerprintVersionV1
	prepared.Action.Params.TenantID = 0
	prepared.Action.Params.ExecutionMode = ""
	prepared.Action.Params.RuntimeVersion = ""
	requestDigest, err := digestPreparedTaskSchedule(prepared)
	if err != nil {
		t.Fatalf("digest retained v1 prepared schedule: %v", err)
	}
	prepared.RequestDigest = requestDigest
	return prepared
}

func expectedTaskSchedule(
	t *testing.T,
	scheduler *taskScheduleTestScheduler,
	req TaskScheduleRequest,
) taskScheduleExpected {
	t.Helper()
	prepared := preparedTaskSchedule(t, scheduler, req)
	var err error
	expected, err := scheduler.Scheduler.buildTaskScheduleExpected(
		context.Background(), prepared, "test", false,
	)
	if err != nil {
		t.Fatalf("build expected: %v", err)
	}
	return expected
}

func taskScheduleFakeActivationReceipt(
	fake *taskScheduleFakeClient,
	expected taskScheduleExpected,
) TaskScheduleSnapshot {
	conflictToken := []byte{1}
	if fake != nil {
		fake.mu.Lock()
		if record := fake.schedules[expected.taskID]; record != nil && len(record.conflictToken) != 0 {
			conflictToken = slices.Clone(record.conflictToken)
		}
		fake.mu.Unlock()
	}
	return TaskScheduleSnapshot{
		TaskID:         expected.taskID,
		RequestDigest:  expected.fingerprint.RequestDigest,
		PreparedDigest: expected.fingerprint.PreparedDigest,
		Revision:       taskScheduleRevision(conflictToken),
		State:          TaskSchedulePausedVirginExact,
	}
}

func taskScheduleCreateRequest(
	t *testing.T,
	expected taskScheduleExpected,
) *workflowservice.CreateScheduleRequest {
	t.Helper()
	req, err := expected.createRequest()
	if err != nil {
		t.Fatalf("create request: %v", err)
	}
	return req
}

func payloadForTaskScheduleTest(t *testing.T, value any) *commonpb.Payload {
	t.Helper()
	payload, err := converter.GetDefaultDataConverter().ToPayload(value)
	if err != nil {
		t.Fatalf("encode payload: %v", err)
	}
	return payload
}

func setTaskScheduleLifecyclePhaseForTest(
	t *testing.T,
	description *client.ScheduleDescription,
	phase string,
) {
	t.Helper()
	action := description.Schedule.Action.(*client.ScheduleWorkflowAction)
	payload := action.Memo[taskScheduleMemoKey].(*commonpb.Payload)
	var fingerprint taskScheduleFingerprint
	if err := converter.GetDefaultDataConverter().FromPayload(payload, &fingerprint); err != nil {
		t.Fatalf("decode lifecycle fingerprint: %v", err)
	}
	fingerprint.LifecyclePhase = phase
	action.Memo[taskScheduleMemoKey] = payloadForTaskScheduleTest(t, fingerprint)
}

func requireTaskScheduleKind(t *testing.T, err error, sentinel error, kind TaskScheduleErrorKind) *TaskScheduleError {
	t.Helper()
	if !errors.Is(err, sentinel) {
		t.Fatalf("errors.Is(%v) = false; err=%v", sentinel, err)
	}
	var typed *TaskScheduleError
	if !errors.As(err, &typed) {
		t.Fatalf("errors.As(*TaskScheduleError) = false; err=%v", err)
	}
	if typed.Kind != kind {
		t.Fatalf("kind=%s, want %s", typed.Kind, kind)
	}
	return typed
}

func requireNoTaskScheduleIO(t *testing.T, fake *taskScheduleFakeClient) {
	t.Helper()
	if got := fake.counts(); got != (taskScheduleFakeCounts{}) {
		t.Fatalf("Temporal I/O occurred: %+v", got)
	}
}

func TestTaskIDForOperation_DeterministicAndIsolating(t *testing.T) {
	id, err := TaskIDForOperation(7, 42, "operation-0001")
	if err != nil {
		t.Fatal(err)
	}
	again, err := TaskIDForOperation(7, 42, "operation-0001")
	if err != nil || again != id {
		t.Fatalf("non-deterministic id: %q, %q, err=%v", id, again, err)
	}
	if !strings.HasPrefix(id, "task-v1-") || len(id) != len("task-v1-")+64 {
		t.Fatalf("unexpected task ID shape: %q", id)
	}
	for _, input := range []struct {
		tenant int64
		user   int64
		op     string
	}{{8, 42, "operation-0001"}, {7, 43, "operation-0001"}, {7, 42, "operation-0002"}} {
		other, otherErr := TaskIDForOperation(input.tenant, input.user, input.op)
		if otherErr != nil || other == id {
			t.Fatalf("identity collision for %+v: id=%q err=%v", input, other, otherErr)
		}
	}

	invalid := []struct {
		name   string
		tenant int64
		user   int64
		op     string
	}{
		{"tenant", 0, 42, "x"}, {"user", 7, 0, "x"}, {"empty", 7, 42, ""},
		{"whitespace", 7, 42, " x"}, {"too_long", 7, 42, strings.Repeat("x", maxTaskOperationIDBytes+1)},
		{"invalid_utf8", 7, 42, string([]byte{0xff})},
	}
	for _, tc := range invalid {
		t.Run(tc.name, func(t *testing.T) {
			_, gotErr := TaskIDForOperation(tc.tenant, tc.user, tc.op)
			requireTaskScheduleKind(t, gotErr, ErrTaskScheduleInvalid, TaskScheduleErrorInvalid)
			var appErr *types.AppError
			if !errors.As(gotErr, &appErr) || appErr.Code != types.CodeValidation {
				t.Fatalf("AppError=%+v, want validation", appErr)
			}
		})
	}
}

func TestTaskIDForOperationVersionV1_RetainsHistoricalByteLimit(t *testing.T) {
	t.Parallel()
	if _, err := taskIDForOperationVersion(
		taskScheduleIDSchemeVersionV1, 7, 42,
		strings.Repeat("x", maxTaskOperationIDBytesV1),
	); err != nil {
		t.Fatalf("v1 exact historical operation ID limit rejected: %v", err)
	}
	if _, err := taskIDForOperationVersion(
		taskScheduleIDSchemeVersionV1, 7, 42,
		strings.Repeat("x", maxTaskOperationIDBytesV1+1),
	); !errors.Is(err, ErrTaskScheduleInvalid) {
		t.Fatalf("v1 historical operation ID limit+1 error = %v, want invalid", err)
	}
}

func TestPrepareTaskSchedule_JSONRoundTripFreezesVersionedDefinition(t *testing.T) {
	fake := newTaskScheduleFakeClient()
	s := newTaskScheduleTestScheduler(fake)
	WithCompiledRuntimeRollout(true, "", true)(s.Scheduler)
	prepared := preparedTaskSchedule(t, s, validTaskScheduleRequest())
	if prepared.IDSchemeVersion != taskScheduleIDSchemeVersion ||
		prepared.FingerprintVersion != taskScheduleFingerprintVersion {
		t.Fatalf("versions: ID=%q fingerprint=%q", prepared.IDSchemeVersion, prepared.FingerprintVersion)
	}
	if !strings.HasPrefix(prepared.TaskID, "task-"+prepared.IDSchemeVersion+"-") {
		t.Fatalf("TaskID %q does not carry ID scheme %q", prepared.TaskID, prepared.IDSchemeVersion)
	}
	encoded, err := json.Marshal(prepared)
	if err != nil {
		t.Fatal(err)
	}
	var decoded PreparedTaskSchedule
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded, prepared) {
		t.Fatalf("JSON round-trip changed prepared definition:\nwant=%+v\n got=%+v", prepared, decoded)
	}
	result, err := s.Scheduler.EnsurePausedTask(context.Background(), decoded)
	if err != nil || result.Disposition != TaskScheduleEnsured {
		t.Fatalf("round-tripped Ensure: result=%+v err=%v", result, err)
	}
	desc, ok := fake.snapshot(prepared.TaskID)
	if !ok {
		t.Fatal("created schedule missing")
	}
	var fingerprint taskScheduleFingerprint
	action := desc.Schedule.Action.(*client.ScheduleWorkflowAction)
	if err := converter.GetDefaultDataConverter().FromPayload(
		action.Memo[taskScheduleMemoKey].(*commonpb.Payload), &fingerprint,
	); err != nil {
		t.Fatal(err)
	}
	if fingerprint.IDSchemeVersion != prepared.IDSchemeVersion ||
		fingerprint.FingerprintVersion != prepared.FingerprintVersion {
		t.Fatalf("memo fingerprint versions=%+v", fingerprint)
	}
}

func TestPrepareTaskSchedule_FingerprintVersionFollowsCompiledRuntimeRollout(t *testing.T) {
	req := validTaskScheduleRequest()
	taskID, err := TaskIDForOperation(req.TenantID, req.UserID, req.OperationID)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name        string
		configure   func(*Scheduler)
		wantVersion string
		wantRuntime string
		wantTenant  int64
		wantMode    types.ExecutionMode
	}{
		{
			name:        "dark rollout stays v1 for old-binary recovery",
			wantVersion: taskScheduleFingerprintVersionV1,
		},
		{
			name: "matching canary writes v2",
			configure: func(s *Scheduler) {
				WithCompiledRuntimeRollout(true, taskID, false)(s)
			},
			wantVersion: taskScheduleFingerprintVersion,
			wantRuntime: workflow.CompiledRuntimeSnapshotV1,
			wantTenant:  req.TenantID,
			wantMode:    types.ExecutionModeCompiled,
		},
		{
			name: "nested run outcome canary writes combined runtime",
			configure: func(s *Scheduler) {
				WithCompiledRuntimeRollout(true, taskID, false)(s)
				WithRunOutcomeRollout(true, taskID, false)(s)
			},
			wantVersion: taskScheduleFingerprintVersion,
			wantRuntime: workflow.CompiledRuntimeRunOutcomeV1,
			wantTenant:  req.TenantID,
			wantMode:    types.ExecutionModeCompiled,
		},
		{
			name: "matching Tool canary writes Source-free runtime",
			configure: func(s *Scheduler) {
				WithCompiledRuntimeRollout(true, taskID, false)(s)
				WithCompiledToolRuntimeCanary(taskID)(s)
			},
			wantVersion: taskScheduleFingerprintVersion,
			wantRuntime: workflow.CompiledRuntimeToolSnapshotV2,
			wantTenant:  req.TenantID,
			wantMode:    types.ExecutionModeCompiled,
		},
		{
			name: "nonmatching canary stays v1",
			configure: func(s *Scheduler) {
				WithCompiledRuntimeRollout(true, "task-v1-other", false)(s)
			},
			wantVersion: taskScheduleFingerprintVersionV1,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := newTaskScheduleTestScheduler(newTaskScheduleFakeClient())
			if tc.configure != nil {
				tc.configure(s.Scheduler)
			}
			prepared := preparedTaskSchedule(t, s, req)
			params := prepared.Action.Params
			if prepared.FingerprintVersion != tc.wantVersion ||
				params.RuntimeVersion != tc.wantRuntime ||
				params.TenantID != tc.wantTenant || params.ExecutionMode != tc.wantMode {
				t.Fatalf("prepared version/envelope = %q/%+v", prepared.FingerprintVersion, params)
			}
			if err := ValidatePreparedTaskScheduleRequest(prepared, req); err != nil {
				t.Fatalf("validate prepared rollout checkpoint: %v", err)
			}
		})
	}
}

func TestActivateTask_ToolRuntimeRequiresCommittedToolDefinition(t *testing.T) {
	req := validTaskScheduleRequest()
	taskID, err := TaskIDForOperation(
		req.TenantID, req.UserID, req.OperationID)
	if err != nil {
		t.Fatal(err)
	}
	fake := newTaskScheduleFakeClient()
	s := newTaskScheduleTestScheduler(fake)
	capabilities := &toolRuntimeCapabilityScheduleStore{}
	s.st = capabilities
	WithCompiledRuntimeRollout(true, taskID, false)(s.Scheduler)
	WithCompiledToolRuntimeCanary(taskID)(s.Scheduler)
	prepared := preparedTaskSchedule(t, s, req)
	ensured, err := s.Scheduler.EnsurePausedTask(t.Context(), prepared)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Scheduler.ActivateTask(
		t.Context(), prepared, ensured.Snapshot,
	); err == nil {
		t.Fatal("Tool schedule activated before Tool definition commit")
	}
	capabilities.available = true
	if _, err := s.Scheduler.ActivateTask(
		t.Context(), prepared, ensured.Snapshot,
	); err != nil {
		t.Fatalf("activate committed Tool task: %v", err)
	}
}

func TestPreparedTaskSchedule_DarkV1RemoteActionRemainsOldWireReadableAcrossResponseLoss(t *testing.T) {
	req := validTaskScheduleRequest()
	fake := newTaskScheduleFakeClient()
	fake.createCommitErr = context.DeadlineExceeded
	s := newTaskScheduleTestScheduler(fake)
	prepared := preparedTaskSchedule(t, s, req)
	if prepared.FingerprintVersion != taskScheduleFingerprintVersionV1 {
		t.Fatalf("dark prepared fingerprint = %q, want retained v1", prepared.FingerprintVersion)
	}

	assertOldVerifierView := func(stage string) {
		t.Helper()
		desc, ok := fake.snapshot(prepared.TaskID)
		if !ok {
			t.Fatalf("%s schedule disappeared", stage)
		}
		action := desc.Schedule.Action.(*client.ScheduleWorkflowAction)
		payload := action.Args[0].(*commonpb.Payload)
		var current workflow.PushParams
		if err := converter.GetDefaultDataConverter().FromPayload(payload, &current); err != nil {
			t.Fatalf("%s decode current Action: %v", stage, err)
		}
		if current.TenantID != req.TenantID || current.ExecutionMode != types.ExecutionModeCompiled ||
			current.RuntimeVersion != "" {
			t.Fatalf("%s current Action envelope = %+v", stage, current)
		}
		var old retainedV1PushParamsWire
		if err := converter.GetDefaultDataConverter().FromPayload(payload, &old); err != nil {
			t.Fatalf("%s retained old-wire decode: %v", stage, err)
		}
		want := retainedV1PushParamsWire{
			UserID: prepared.Action.Params.UserID, RunKind: prepared.Action.Params.RunKind,
			ScheduleID: prepared.Action.Params.ScheduleID, Scope: prepared.Action.Params.Scope,
			NLDesc: prepared.Action.Params.NLDesc,
		}
		if !reflect.DeepEqual(old, want) {
			t.Fatalf("%s old verifier view = %+v, want %+v", stage, old, want)
		}
	}

	ensured, err := s.Scheduler.EnsurePausedTask(t.Context(), prepared)
	if err != nil {
		t.Fatalf("paused Create response-loss recovery: %v", err)
	}
	assertOldVerifierView("paused create response-loss")

	fake.unpauseCommitErr = context.DeadlineExceeded
	active, err := s.Scheduler.ActivateTask(t.Context(), prepared, ensured.Snapshot)
	if err != nil || active.State != TaskScheduleActiveVirginExact {
		t.Fatalf("activation response-loss recovery: snapshot=%+v err=%v", active, err)
	}
	assertOldVerifierView("activated checkpoint response-loss")
}

// A5 创建的持久 Schedule Action 只冻结可信归属；每轮运行的
// snapshot 引用必须由 PrepareRun 产生，不能预写进 Action 或 Temporal history。
func TestPreparedTaskSchedule_ActionCarriesTenantWithoutRunSnapshot(t *testing.T) {
	req := validTaskScheduleRequest()
	fake := newTaskScheduleFakeClient()
	s := newTaskScheduleTestScheduler(fake)
	WithCompiledRuntimeRollout(true, "", true)(s.Scheduler)
	prepared := preparedTaskSchedule(t, s, req)

	if prepared.FingerprintVersion != taskScheduleFingerprintVersion {
		t.Fatalf("prepared fingerprint version = %q, want %q",
			prepared.FingerprintVersion, taskScheduleFingerprintVersion)
	}
	if got := prepared.Action.Params.TenantID; got != req.TenantID {
		t.Fatalf("prepared Action tenant_id = %d，期望 %d", got, req.TenantID)
	}
	if got := prepared.Action.Params.ExecutionMode; got != types.ExecutionModeCompiled {
		t.Fatalf("prepared Action execution_mode = %q, want %q", got, types.ExecutionModeCompiled)
	}
	if prepared.Action.Params.Snapshot != nil {
		t.Fatalf("prepared Action snapshot = %+v，期望 nil", prepared.Action.Params.Snapshot)
	}
	if _, err := s.Scheduler.EnsurePausedTask(t.Context(), prepared); err != nil {
		t.Fatalf("EnsurePausedTask: %v", err)
	}
	desc, ok := fake.snapshot(prepared.TaskID)
	if !ok {
		t.Fatal("created schedule missing")
	}
	action, ok := desc.Schedule.Action.(*client.ScheduleWorkflowAction)
	if !ok || len(action.Args) != 1 {
		t.Fatalf("created Action = %#v，期望单个 workflow 入参", desc.Schedule.Action)
	}
	var got workflow.PushParams
	if err := converter.GetDefaultDataConverter().FromPayload(
		action.Args[0].(*commonpb.Payload), &got,
	); err != nil {
		t.Fatalf("解码持久 Action 入参: %v", err)
	}
	if got.TenantID != req.TenantID {
		t.Fatalf("持久 Action tenant_id = %d，期望 %d", got.TenantID, req.TenantID)
	}
	if got.ExecutionMode != types.ExecutionModeCompiled {
		t.Fatalf("持久 Action execution_mode = %q, want %q", got.ExecutionMode, types.ExecutionModeCompiled)
	}
	if got.Snapshot != nil {
		t.Fatalf("持久 Action snapshot = %+v，期望 nil", got.Snapshot)
	}
}

func TestPreparedTaskSchedule_RetainedV1CheckpointValidatesAndUpgradesAction(t *testing.T) {
	req := validTaskScheduleRequest()
	fake := newTaskScheduleFakeClient()
	s := newTaskScheduleTestScheduler(fake)
	prepared := retainedV1PreparedTaskSchedule(t, s, req)

	if err := ValidatePreparedTaskScheduleRequest(prepared, req); err != nil {
		t.Fatalf("retained v1 checkpoint validation: %v", err)
	}
	expected, err := s.Scheduler.buildTaskScheduleExpected(t.Context(), prepared, "test", false)
	if err != nil {
		t.Fatalf("build retained v1 expected schedule: %v", err)
	}
	create := taskScheduleCreateRequest(t, expected)
	create.GetSchedule().GetAction().GetStartWorkflow().GetInput().Payloads[0] =
		payloadForTaskScheduleTest(t, prepared.Action.Params)
	fake.seed(create)

	ensured, err := s.Scheduler.EnsurePausedTask(t.Context(), prepared)
	if err != nil {
		t.Fatalf("recover retained v1 paused schedule: %v", err)
	}
	active, err := s.Scheduler.ActivateTask(t.Context(), prepared, ensured.Snapshot)
	if err != nil || active.State != TaskScheduleActiveVirginExact {
		t.Fatalf("activate retained v1 schedule: snapshot=%+v err=%v", active, err)
	}
	desc, ok := fake.snapshot(prepared.TaskID)
	if !ok {
		t.Fatal("activated retained v1 schedule disappeared")
	}
	action := desc.Schedule.Action.(*client.ScheduleWorkflowAction)
	var params workflow.PushParams
	if err := converter.GetDefaultDataConverter().FromPayload(
		action.Args[0].(*commonpb.Payload), &params,
	); err != nil {
		t.Fatalf("decode upgraded retained v1 Action: %v", err)
	}
	if params.TenantID != req.TenantID || params.ExecutionMode != types.ExecutionModeCompiled ||
		params.RuntimeVersion != "" || params.Snapshot != nil {
		t.Fatalf("upgraded retained v1 Action params = %+v", params)
	}
}

func TestPreparedTaskSchedule_V1AcceptsOnlyExactMissingEnvelope(t *testing.T) {
	req := validTaskScheduleRequest()
	s := newTaskScheduleTestScheduler(newTaskScheduleFakeClient())
	legacy := retainedV1PreparedTaskSchedule(t, s, req)

	tests := []struct {
		name   string
		mutate func(*PreparedTaskSchedule)
	}{
		{
			name: "explicit unknown mode is not the missing legacy field",
			mutate: func(p *PreparedTaskSchedule) {
				p.Action.Params.ExecutionMode = types.ExecutionModeUnknown
			},
		},
		{
			name: "tenant field is not legacy",
			mutate: func(p *PreparedTaskSchedule) {
				p.Action.Params.TenantID = p.TenantID
			},
		},
		{
			name: "runtime version field is not legacy",
			mutate: func(p *PreparedTaskSchedule) {
				p.Action.Params.RuntimeVersion = workflow.CompiledRuntimeSnapshotV1
			},
		},
		{
			name: "unknown fingerprint version",
			mutate: func(p *PreparedTaskSchedule) {
				p.FingerprintVersion = "v999"
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			prepared := clonePreparedTaskSchedule(legacy)
			tc.mutate(&prepared)
			requestDigest, err := digestPreparedTaskSchedule(prepared)
			if err != nil {
				t.Fatal(err)
			}
			prepared.RequestDigest = requestDigest
			if err := ValidatePreparedTaskScheduleRequest(prepared, req); err == nil {
				t.Fatal("mutated retained checkpoint unexpectedly validated")
			}
		})
	}
}

func TestPreparedTaskSchedule_CustomDataConverterFullLifecycle(t *testing.T) {
	recorder := &taskScheduleConverterRecorder{}
	dc := taskScheduleXORDataConverter{
		DataConverter: converter.GetDefaultDataConverter(),
		recorder:      recorder,
	}
	fake := newTaskScheduleFakeClient()
	fake.dc = dc
	base := New(
		&taskScheduleTemporalClient{schedules: fake},
		"vane-task-test",
		nil,
		WithTaskScheduleNamespace("test-namespace"),
		WithTaskScheduleDataConverter("xor-json-v1", dc),
	)
	base.taskScheduleEnv.namespaceIDOverride = "test-namespace-id"
	s := &taskScheduleTestScheduler{Scheduler: base, fake: fake}
	prepared := preparedTaskSchedule(t, s, validTaskScheduleRequest())
	if prepared.ConverterID != "xor-json-v1" {
		t.Fatalf("ConverterID=%q", prepared.ConverterID)
	}
	result, err := s.Scheduler.EnsurePausedTask(context.Background(), prepared)
	if err != nil || result.Disposition != TaskScheduleEnsured {
		t.Fatalf("Ensure: result=%+v err=%v", result, err)
	}
	paused, err := s.Scheduler.DescribeTask(context.Background(), prepared)
	if err != nil || paused.State != TaskSchedulePausedProvisioningExact {
		t.Fatalf("Describe paused: snapshot=%+v err=%v", paused, err)
	}
	active, err := s.Scheduler.ActivateTask(context.Background(), prepared, result.Snapshot)
	if err != nil || active.State != TaskScheduleActiveVirginExact {
		t.Fatalf("Activate: snapshot=%+v err=%v", active, err)
	}
	if err := s.Scheduler.DeleteTask(context.Background(), prepared); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, ok := fake.snapshot(prepared.TaskID); ok {
		t.Fatal("schedule remained after custom-converter Delete")
	}
	wantContext := converter.WorkflowSerializationContext{
		Namespace: prepared.Namespace, WorkflowID: prepared.Action.ActionID,
	}
	recorder.mu.Lock()
	encoded := append([]converter.WorkflowSerializationContext(nil), recorder.encoded...)
	decoded := append([]converter.WorkflowSerializationContext(nil), recorder.decoded...)
	recorder.mu.Unlock()
	if len(encoded) < 2 || len(decoded) < 2 {
		t.Fatalf("context-aware converter calls: encoded=%v decoded=%v", encoded, decoded)
	}
	for _, got := range append(encoded, decoded...) {
		if got != wantContext {
			t.Fatalf("serialization context=%+v, want %+v", got, wantContext)
		}
	}
}

func TestPreparedTaskSchedule_LifecycleUsesFrozenRawNamespaceAndIdempotencyKeys(t *testing.T) {
	fake := newTaskScheduleFakeClient()
	s := newTaskScheduleTestScheduler(fake)
	prepared := preparedTaskSchedule(t, s, validTaskScheduleRequest())
	ensured, err := s.Scheduler.EnsurePausedTask(context.Background(), prepared)
	if err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	if _, err := s.Scheduler.ActivateTask(context.Background(), prepared, ensured.Snapshot); err != nil {
		t.Fatalf("Activate: %v", err)
	}
	if err := s.Scheduler.DeleteTask(context.Background(), prepared); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	got := fake.rawRequestSnapshot()
	wantIdentity := taskScheduleIdentity(prepared.FingerprintVersion)
	if got.createNamespace != prepared.Namespace || got.updateNamespace != prepared.Namespace ||
		got.deleteNamespace != prepared.Namespace {
		t.Fatalf("raw lifecycle namespaces = %+v, want %q", got, prepared.Namespace)
	}
	if got.createRequestID != prepared.RequestDigest ||
		got.updateRequestID != taskScheduleRequestID("activate", prepared.RequestDigest) {
		t.Fatalf("raw lifecycle request IDs = %+v", got)
	}
	if got.createIdentity != wantIdentity || got.updateIdentity != wantIdentity || got.deleteIdentity != wantIdentity {
		t.Fatalf("raw lifecycle identities = %+v, want %q", got, wantIdentity)
	}
	if got.updatePaused || got.updateNote != prepared.Action.ActivationNote {
		t.Fatalf("raw activation state = paused:%v note:%q, want active note %q",
			got.updatePaused, got.updateNote, prepared.Action.ActivationNote)
	}
	if len(got.updateConflictToken) == 0 {
		t.Fatal("raw activation omitted Describe conflict token")
	}
	if calls := fake.counts(); calls.handle != 0 {
		t.Fatalf("lifecycle used SDK handle with implicit namespace: %+v", calls)
	}
}

func TestPrepareTaskSchedule_RejectsRequestContextAwareConverter(t *testing.T) {
	fake := newTaskScheduleFakeClient()
	dc := taskScheduleRequestAwareDataConverter{DataConverter: converter.GetDefaultDataConverter()}
	base := New(
		&taskScheduleTemporalClient{schedules: fake},
		"vane-task-test",
		nil,
		WithTaskScheduleNamespace("test-namespace"),
		WithTaskScheduleDataConverter("request-aware", dc),
	)
	base.taskScheduleEnv.namespaceIDOverride = "test-namespace-id"
	_, err := base.PrepareTaskSchedule(context.Background(), validTaskScheduleRequest())
	requireTaskScheduleKind(t, err, ErrTaskScheduleInvalid, TaskScheduleErrorInvalid)
	requireNoTaskScheduleIO(t, fake)
}

func TestPrepareTaskSchedule_RejectsNilSerializationContextConverterWithoutPanic(t *testing.T) {
	fake := newTaskScheduleFakeClient()
	dc := taskScheduleNilContextDataConverter{DataConverter: converter.GetDefaultDataConverter()}
	base := New(
		&taskScheduleTemporalClient{schedules: fake},
		"vane-task-test",
		nil,
		WithTaskScheduleNamespace("test-namespace"),
		WithTaskScheduleDataConverter("nil-context", dc),
	)
	base.taskScheduleEnv.namespaceIDOverride = "test-namespace-id"
	_, err := base.PrepareTaskSchedule(context.Background(), validTaskScheduleRequest())
	requireTaskScheduleKind(t, err, ErrTaskScheduleInvalid, TaskScheduleErrorInvalid)
	requireNoTaskScheduleIO(t, fake)
}

func TestPreparedTaskSchedule_TamperAndVersionChangesFailBeforeScheduleIO(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*PreparedTaskSchedule)
	}{
		{"request_digest", func(p *PreparedTaskSchedule) { p.RequestDigest = strings.Repeat("b", 64) }},
		{"prepared_digest", func(p *PreparedTaskSchedule) { p.PreparedDigest = strings.Repeat("b", 64) }},
		{"task_id", func(p *PreparedTaskSchedule) { p.TaskID = "task-v1-tampered" }},
		{"id_scheme_version", func(p *PreparedTaskSchedule) { p.IDSchemeVersion = "v2" }},
		{"fingerprint_version", func(p *PreparedTaskSchedule) { p.FingerprintVersion = "v999" }},
		{"workflow_params", func(p *PreparedTaskSchedule) {
			p.Action.Params.Scope.SourceIDs = append(p.Action.Params.Scope.SourceIDs, 33)
		}},
		{"workflow_tenant", func(p *PreparedTaskSchedule) { p.Action.Params.TenantID++ }},
		{"workflow_snapshot", func(p *PreparedTaskSchedule) {
			p.Action.Params.Snapshot = &workflow.RunSnapshotRef{}
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fake := newTaskScheduleFakeClient()
			s := newTaskScheduleTestScheduler(fake)
			prepared := preparedTaskSchedule(t, s, validTaskScheduleRequest())
			tc.mutate(&prepared)
			_, err := s.Scheduler.DescribeTask(context.Background(), prepared)
			requireTaskScheduleKind(t, err, ErrTaskScheduleInvalid, TaskScheduleErrorInvalid)
			requireNoTaskScheduleIO(t, fake)
		})
	}
}

func TestPreparedTaskSchedule_EnvironmentMismatchFailsBeforeScheduleIO(t *testing.T) {
	tests := []struct {
		name     string
		mutate   func(*Scheduler)
		sentinel error
		kind     TaskScheduleErrorKind
	}{
		{"namespace_name", func(s *Scheduler) { s.taskScheduleEnv.namespace = "other-namespace" }, ErrTaskScheduleConflict, TaskScheduleErrorConflict},
		{"namespace_id", func(s *Scheduler) { s.taskScheduleEnv.namespaceIDOverride = "other-namespace-id" }, ErrTaskScheduleConflict, TaskScheduleErrorConflict},
		{"converter", func(s *Scheduler) { s.taskScheduleEnv.converterID = "other-converter" }, ErrTaskScheduleBlocked, TaskScheduleErrorBlocked},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fake := newTaskScheduleFakeClient()
			s := newTaskScheduleTestScheduler(fake)
			prepared := preparedTaskSchedule(t, s, validTaskScheduleRequest())
			s.taskScheduleEnv.mu.Lock()
			tc.mutate(s.Scheduler)
			s.taskScheduleEnv.mu.Unlock()
			_, err := s.Scheduler.EnsurePausedTask(context.Background(), prepared)
			requireTaskScheduleKind(t, err, tc.sentinel, tc.kind)
			requireNoTaskScheduleIO(t, fake)
		})
	}
}

func TestPreparedTaskSchedule_RefreshesNamespaceIdentityOnEveryOperation(t *testing.T) {
	fake := newTaskScheduleFakeClient()
	base := New(
		&taskScheduleTemporalClient{schedules: fake},
		"vane-task-test",
		nil,
		WithTaskScheduleNamespace("test-namespace"),
	)
	prepared, err := base.PrepareTaskSchedule(context.Background(), validTaskScheduleRequest())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := base.EnsurePausedTask(context.Background(), prepared); err != nil {
		t.Fatalf("Ensure with original namespace identity: %v", err)
	}
	if got := fake.namespaceDescribeCount(); got != 2 {
		t.Fatalf("namespace Describe calls after Prepare+Ensure = %d, want 2", got)
	}

	fake.setNamespaceID("replacement-namespace-id")
	beforeScheduleIO := fake.counts()
	_, err = base.DescribeTask(context.Background(), prepared)
	requireTaskScheduleKind(t, err, ErrTaskScheduleConflict, TaskScheduleErrorConflict)
	if got := fake.namespaceDescribeCount(); got != 3 {
		t.Fatalf("namespace identity was cached across operations; Describe calls = %d, want 3", got)
	}
	if afterScheduleIO := fake.counts(); afterScheduleIO != beforeScheduleIO {
		t.Fatalf("namespace replacement reached schedule I/O: before=%+v after=%+v", beforeScheduleIO, afterScheduleIO)
	}
}

func TestPreparedTaskSchedule_TaskQueueDriftFailsClosedForWritesButAllowsCleanup(t *testing.T) {
	fake := newTaskScheduleFakeClient()
	s := newTaskScheduleTestScheduler(fake)
	prepared := preparedTaskSchedule(t, s, validTaskScheduleRequest())
	expected, err := s.Scheduler.buildTaskScheduleExpected(
		context.Background(), prepared, "test", false,
	)
	if err != nil {
		t.Fatal(err)
	}
	fake.seed(taskScheduleCreateRequest(t, expected))
	s.tq = "new-task-queue"

	_, err = s.Scheduler.EnsurePausedTask(context.Background(), prepared)
	requireTaskScheduleKind(t, err, ErrTaskScheduleConflict, TaskScheduleErrorConflict)
	_, err = s.Scheduler.ActivateTask(context.Background(), prepared, TaskScheduleSnapshot{})
	requireTaskScheduleKind(t, err, ErrTaskScheduleConflict, TaskScheduleErrorConflict)
	requireNoTaskScheduleIO(t, fake)

	snapshot, err := s.Scheduler.DescribeTask(context.Background(), prepared)
	if err != nil || snapshot.State != TaskSchedulePausedProvisioningExact {
		t.Fatalf("Describe frozen task after queue drift: snapshot=%+v err=%v", snapshot, err)
	}
	if err := s.Scheduler.DeleteTask(context.Background(), prepared); err != nil {
		t.Fatalf("Delete frozen task after queue drift: %v", err)
	}
	if _, ok := fake.snapshot(prepared.TaskID); ok {
		t.Fatal("Delete did not remove frozen task")
	}
	if got := fake.counts(); got.create != 0 || got.unpause != 0 || got.delete != 1 {
		t.Fatalf("calls=%+v", got)
	}
}

func TestPreparedTaskSchedule_ConverterRotationUsesRetainedDecoderOnlyForInspectionAndCleanup(t *testing.T) {
	fake := newTaskScheduleFakeClient()
	dc := converter.GetDefaultDataConverter()
	oldScheduler := New(
		&taskScheduleTemporalClient{schedules: fake},
		"vane-task-test",
		nil,
		WithTaskScheduleNamespace("test-namespace"),
		WithTaskScheduleDataConverter("json-v1", dc),
	)
	oldScheduler.taskScheduleEnv.namespaceIDOverride = "test-namespace-id"
	prepared, err := oldScheduler.PrepareTaskSchedule(context.Background(), validTaskScheduleRequest())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := oldScheduler.EnsurePausedTask(context.Background(), prepared); err != nil {
		t.Fatalf("create with old converter: %v", err)
	}

	rotated := New(
		&taskScheduleTemporalClient{schedules: fake},
		"vane-task-test",
		nil,
		WithTaskScheduleNamespace("test-namespace"),
		WithTaskScheduleDataConverter("json-v2", dc),
		WithTaskScheduleDecoder("json-v1", dc),
	)
	rotated.taskScheduleEnv.namespaceIDOverride = "test-namespace-id"
	if _, err := rotated.DescribeTask(context.Background(), prepared); err != nil {
		t.Fatalf("Describe with retained old decoder: %v", err)
	}
	beforeWrites := fake.counts()
	if _, err := rotated.EnsurePausedTask(context.Background(), prepared); !errors.Is(err, ErrTaskScheduleConflict) {
		t.Fatalf("Ensure after converter rotation error = %v, want conflict", err)
	}
	if _, err := rotated.ActivateTask(context.Background(), prepared, TaskScheduleSnapshot{}); !errors.Is(err, ErrTaskScheduleConflict) {
		t.Fatalf("Activate after converter rotation error = %v, want conflict", err)
	}
	afterWrites := fake.counts()
	if afterWrites != beforeWrites {
		t.Fatalf("rotated execution converter performed Temporal I/O: before=%+v after=%+v", beforeWrites, afterWrites)
	}
	if err := rotated.DeleteTask(context.Background(), prepared); err != nil {
		t.Fatalf("Delete with retained old decoder: %v", err)
	}
}

func TestPreparedTaskSchedule_CurrentConverterCannotBeShadowedByRetainedDecoder(t *testing.T) {
	fake := newTaskScheduleFakeClient()
	shadowRecorder := &taskScheduleConverterRecorder{}
	shadow := taskScheduleXORDataConverter{
		DataConverter: converter.GetDefaultDataConverter(),
		recorder:      shadowRecorder,
	}
	base := New(
		&taskScheduleTemporalClient{schedules: fake},
		"vane-task-test",
		nil,
		WithTaskScheduleNamespace("test-namespace"),
		WithTaskScheduleDataConverter("json-v1", converter.GetDefaultDataConverter()),
		WithTaskScheduleDecoder("json-v1", shadow),
	)
	base.taskScheduleEnv.namespaceIDOverride = "test-namespace-id"
	prepared, err := base.PrepareTaskSchedule(context.Background(), validTaskScheduleRequest())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := base.EnsurePausedTask(context.Background(), prepared); err != nil {
		t.Fatalf("Ensure with unshadowed current converter: %v", err)
	}
	shadowRecorder.mu.Lock()
	encoded, decoded := len(shadowRecorder.encoded), len(shadowRecorder.decoded)
	shadowRecorder.mu.Unlock()
	if encoded != 0 || decoded != 0 {
		t.Fatalf("retained decoder shadowed current converter: encoded=%d decoded=%d", encoded, decoded)
	}
}

func TestPreparedTaskSchedule_ImplicitDefaultConverterCannotBeShadowed(t *testing.T) {
	fake := newTaskScheduleFakeClient()
	shadowRecorder := &taskScheduleConverterRecorder{}
	shadow := taskScheduleXORDataConverter{
		DataConverter: converter.GetDefaultDataConverter(),
		recorder:      shadowRecorder,
	}
	base := New(
		&taskScheduleTemporalClient{schedules: fake},
		"vane-task-test",
		nil,
		WithTaskScheduleNamespace("test-namespace"),
		WithTaskScheduleDecoder(taskScheduleDefaultConverterID, shadow),
	)
	base.taskScheduleEnv.namespaceIDOverride = "test-namespace-id"
	prepared, err := base.PrepareTaskSchedule(context.Background(), validTaskScheduleRequest())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := base.EnsurePausedTask(context.Background(), prepared); err != nil {
		t.Fatalf("Ensure with unshadowed implicit default converter: %v", err)
	}
	shadowRecorder.mu.Lock()
	encoded, decoded := len(shadowRecorder.encoded), len(shadowRecorder.decoded)
	shadowRecorder.mu.Unlock()
	if encoded != 0 || decoded != 0 {
		t.Fatalf("retained decoder shadowed implicit default: encoded=%d decoded=%d", encoded, decoded)
	}
}

func TestPreparedTaskSchedule_NilCurrentConverterCannotFallBackToRetainedDecoder(t *testing.T) {
	fake := newTaskScheduleFakeClient()
	good := New(
		&taskScheduleTemporalClient{schedules: fake},
		"vane-task-test",
		nil,
		WithTaskScheduleNamespace("test-namespace"),
		WithTaskScheduleDataConverter("json-v1", converter.GetDefaultDataConverter()),
	)
	good.taskScheduleEnv.namespaceIDOverride = "test-namespace-id"
	prepared, err := good.PrepareTaskSchedule(context.Background(), validTaskScheduleRequest())
	if err != nil {
		t.Fatal(err)
	}

	misconfigured := New(
		&taskScheduleTemporalClient{schedules: fake},
		"vane-task-test",
		nil,
		WithTaskScheduleNamespace("test-namespace"),
		WithTaskScheduleDataConverter("json-v1", nil),
		WithTaskScheduleDecoder("json-v1", converter.GetDefaultDataConverter()),
	)
	misconfigured.taskScheduleEnv.namespaceIDOverride = "test-namespace-id"
	_, err = misconfigured.EnsurePausedTask(context.Background(), prepared)
	requireTaskScheduleKind(t, err, ErrTaskScheduleBlocked, TaskScheduleErrorBlocked)
	requireNoTaskScheduleIO(t, fake)
}

func TestPrepareTaskSchedule_InputEdges(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*TaskScheduleRequest)
	}{
		{"invalid_utf8", func(r *TaskScheduleRequest) { r.NLDescription = string([]byte{0xff}) }},
		{"negative_every_seconds", func(r *TaskScheduleRequest) {
			r.Spec = ScheduleSpec{EverySeconds: -1, TZ: "Asia/Shanghai"}
		}},
		{"fractional_anchor", func(r *TaskScheduleRequest) {
			r.Spec = ScheduleSpec{
				EverySeconds: 7200,
				AnchorAt:     "2026-07-21T08:00:00.500+08:00",
				TZ:           "Asia/Shanghai",
			}
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fake := newTaskScheduleFakeClient()
			s := newTaskScheduleTestScheduler(fake)
			req := validTaskScheduleRequest()
			tc.mutate(&req)
			_, err := s.Scheduler.PrepareTaskSchedule(context.Background(), req)
			requireTaskScheduleKind(t, err, ErrTaskScheduleInvalid, TaskScheduleErrorInvalid)
			requireNoTaskScheduleIO(t, fake)
		})
	}

	for _, tc := range []struct {
		name string
		dow  string
		want uint64
	}{
		{"cron_sunday_7", "7", 1 << 0},
		{"cron_monday", "1", 1 << 1},
		{"cron_monday_step_two", "1/2", 1<<1 | 1<<3 | 1<<5},
		{"cron_full_day_names", "Monday-Wednesday,Friday", 1<<1 | 1<<2 | 1<<3 | 1<<5},
		{"cron_full_sunday_step_two", "Sunday/2", 1<<0 | 1<<2 | 1<<4 | 1<<6},
		{"cron_every_day", "*", 0x7f},
		{"cron_max_int_step_does_not_overflow", "1/" + strconv.Itoa(int(^uint(0)>>1)), 1 << 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := newTaskScheduleFakeClient()
			s := newTaskScheduleTestScheduler(fake)
			req := validTaskScheduleRequest()
			req.Spec.Cron = "0 8 * * " + tc.dow
			prepared := preparedTaskSchedule(t, s, req)
			if prepared.Timing.Calendar == nil || prepared.Timing.Calendar.DayOfWeek != tc.want {
				t.Fatalf("DOW %q bits=%#x, want %#x", tc.dow, prepared.Timing.Calendar.DayOfWeek, tc.want)
			}
			requireNoTaskScheduleIO(t, fake)
		})
	}

	t.Run("cron_full_month_name", func(t *testing.T) {
		fake := newTaskScheduleFakeClient()
		s := newTaskScheduleTestScheduler(fake)
		req := validTaskScheduleRequest()
		req.Spec.Cron = "0 8 1 September *"
		prepared := preparedTaskSchedule(t, s, req)
		if prepared.Timing.Calendar == nil {
			t.Fatal("September cron compiled without calendar timing")
		}
		if prepared.Timing.Calendar.Month != 1<<9 {
			t.Fatalf("September month bits=%#x, want %#x", prepared.Timing.Calendar.Month, uint64(1<<9))
		}
		requireNoTaskScheduleIO(t, fake)
	})
}

func TestEnsurePausedTask_ValidationStopsBeforeTemporal(t *testing.T) {
	tests := []struct {
		name      string
		mutateReq func(*TaskScheduleRequest)
		taskQueue string
	}{
		{"tenant", func(r *TaskScheduleRequest) { r.TenantID = 0 }, "vane-task-test"},
		{"user", func(r *TaskScheduleRequest) { r.UserID = 0 }, "vane-task-test"},
		{"operation", func(r *TaskScheduleRequest) { r.OperationID = " bad" }, "vane-task-test"},
		{"digest_short", func(r *TaskScheduleRequest) { r.PreparedDigest = "abc" }, "vane-task-test"},
		{"digest_upper", func(r *TaskScheduleRequest) { r.PreparedDigest = strings.Repeat("A", 64) }, "vane-task-test"},
		{"description", func(r *TaskScheduleRequest) { r.NLDescription = " " }, "vane-task-test"},
		{"top_n", func(r *TaskScheduleRequest) { r.Scope.TopN = -1 }, "vane-task-test"},
		{"source_zero", func(r *TaskScheduleRequest) { r.Scope.SourceIDs = []int64{0} }, "vane-task-test"},
		{"source_duplicate", func(r *TaskScheduleRequest) { r.Scope.SourceIDs = []int64{11, 11} }, "vane-task-test"},
		{"spec", func(r *TaskScheduleRequest) { r.Spec = ScheduleSpec{} }, "vane-task-test"},
		{"timezone", func(r *TaskScheduleRequest) { r.Spec.TZ = "Mars/Olympus" }, "vane-task-test"},
		{"task_queue", func(*TaskScheduleRequest) {}, " "},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fake := newTaskScheduleFakeClient()
			s := newTaskScheduleTestSchedulerWithTaskQueue(fake, tc.taskQueue)
			req := validTaskScheduleRequest()
			tc.mutateReq(&req)
			_, err := s.EnsurePausedTask(context.Background(), req)
			requireTaskScheduleKind(t, err, ErrTaskScheduleInvalid, TaskScheduleErrorInvalid)
			requireNoTaskScheduleIO(t, fake)
		})
	}
}

func TestEnsurePausedTask_CreateSuccessStillDescribes(t *testing.T) {
	fake := newTaskScheduleFakeClient()
	s := newTaskScheduleTestScheduler(fake)
	req := validTaskScheduleRequest()
	result, err := s.EnsurePausedTask(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if result.Disposition != TaskScheduleEnsured || result.Snapshot.State != TaskSchedulePausedVirginExact {
		t.Fatalf("result=%+v", result)
	}
	wantID, _ := TaskIDForOperation(req.TenantID, req.UserID, req.OperationID)
	if result.Snapshot.TaskID != wantID || result.Snapshot.PreparedDigest != req.PreparedDigest {
		t.Fatalf("snapshot=%+v", result.Snapshot)
	}
	if got := fake.counts(); got.create != 1 || got.describe != 2 || got.unpause != 0 || got.delete != 0 {
		t.Fatalf("calls=%+v", got)
	}
}

func TestEnsurePausedTask_RejectsExactCollisionWithoutMatchingRequestID(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{"sdk_sentinel", temporal.ErrScheduleAlreadyRunning},
		{"service_error", serviceerror.NewAlreadyExists("schedule exists")},
		{"raw_create_error", serviceerror.NewWorkflowExecutionAlreadyStarted(
			"schedule exists", "existing-request", "",
		)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fake := newTaskScheduleFakeClient()
			fake.alreadyExistsError = tc.err
			fake.createCollision = true
			s := newTaskScheduleTestScheduler(fake)
			_, err := s.EnsurePausedTask(context.Background(), validTaskScheduleRequest())
			requireTaskScheduleKind(t, err, ErrTaskScheduleConflict, TaskScheduleErrorConflict)
			if got := fake.counts(); got.create != 1 || got.describe != 2 || got.unpause != 0 || got.delete != 0 {
				t.Fatalf("calls=%+v", got)
			}
		})
	}
}

func TestEnsurePausedTask_RequestIDReplayIsEnsuredWithoutClaimingUniqueCreation(t *testing.T) {
	fake := newTaskScheduleFakeClient()
	// Simulate a second process winning after our NotFound preflight. Temporal
	// may replay success to this caller because both used the same RequestID.
	fake.createCollision = true
	fake.createReplaySuccess = true
	s := newTaskScheduleTestScheduler(fake)
	result, err := s.EnsurePausedTask(context.Background(), validTaskScheduleRequest())
	if err != nil {
		t.Fatal(err)
	}
	if result.Disposition != TaskScheduleEnsured || result.Snapshot.State != TaskSchedulePausedVirginExact {
		t.Fatalf("request-ID replay result=%+v", result)
	}
}

func TestEnsurePausedTask_CommitThenErrorRequiresRequestIDReplay(t *testing.T) {
	fake := newTaskScheduleFakeClient()
	fake.createCommitErr = context.DeadlineExceeded
	s := newTaskScheduleTestScheduler(fake)
	result, err := s.EnsurePausedTask(context.Background(), validTaskScheduleRequest())
	if err != nil || result.Disposition != TaskScheduleEnsured ||
		result.Snapshot.State != TaskSchedulePausedVirginExact {
		t.Fatalf("replay result=%+v err=%v", result, err)
	}
	if got := fake.counts(); got.create != 2 || got.describe != 2 {
		t.Fatalf("calls=%+v", got)
	}
}

func TestEnsurePausedTask_PreflightTransientStopsBeforeCreate(t *testing.T) {
	fake := newTaskScheduleFakeClient()
	underlying := serviceerror.NewUnavailable("Temporal unavailable")
	fake.describeErr = underlying
	s := newTaskScheduleTestScheduler(fake)
	_, err := s.EnsurePausedTask(context.Background(), validTaskScheduleRequest())
	requireTaskScheduleKind(t, err, ErrTaskScheduleTransient, TaskScheduleErrorTransient)
	var unavailable *serviceerror.Unavailable
	if !errors.As(err, &unavailable) {
		t.Fatalf("underlying Unavailable was lost: %v", err)
	}
	var appErr *types.AppError
	if !errors.As(err, &appErr) || appErr.Code != types.CodeInternal || !appErr.Retryable {
		t.Fatalf("AppError=%+v", appErr)
	}
	if got := fake.counts(); got.describe != 1 || got.create != 0 || got.unpause != 0 || got.delete != 0 {
		t.Fatalf("preflight error crossed the mutation boundary: %+v", got)
	}
}

func TestEnsurePausedTask_CreateDeterministicErrorsWithPostNotFoundAreClassified(t *testing.T) {
	tests := []struct {
		name     string
		cause    error
		sentinel error
		kind     TaskScheduleErrorKind
		code     types.ErrCode
		assertAs func(error) bool
	}{
		{"invalid_argument", serviceerror.NewInvalidArgument("invalid action"), ErrTaskScheduleInvalid, TaskScheduleErrorInvalid, types.CodeValidation, func(err error) bool {
			var target *serviceerror.InvalidArgument
			return errors.As(err, &target)
		}},
		{"permission_denied", serviceerror.NewPermissionDenied("namespace denied", "policy"), ErrTaskScheduleBlocked, TaskScheduleErrorBlocked, types.CodeInternal, func(err error) bool {
			var target *serviceerror.PermissionDenied
			return errors.As(err, &target)
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fake := newTaskScheduleFakeClient()
			fake.createErr = tc.cause
			s := newTaskScheduleTestScheduler(fake)
			_, err := s.EnsurePausedTask(context.Background(), validTaskScheduleRequest())
			requireTaskScheduleKind(t, err, tc.sentinel, tc.kind)
			if !tc.assertAs(err) {
				t.Fatalf("underlying %T was lost: %v", tc.cause, err)
			}
			var appErr *types.AppError
			if !errors.As(err, &appErr) || appErr.Code != tc.code || appErr.Retryable {
				t.Fatalf("AppError=%+v", appErr)
			}
			if got := fake.counts(); got.describe != 2 || got.create != 1 || got.unpause != 0 || got.delete != 0 {
				t.Fatalf("calls=%+v", got)
			}
		})
	}
}

func TestEnsurePausedTask_RejectsDescriptionMismatchesWithoutWrites(t *testing.T) {
	replaceParams := func(t *testing.T, desc *client.ScheduleDescription, mutate func(*workflow.PushParams)) {
		action := desc.Schedule.Action.(*client.ScheduleWorkflowAction)
		var params workflow.PushParams
		if err := converter.GetDefaultDataConverter().FromPayload(action.Args[0].(*commonpb.Payload), &params); err != nil {
			t.Fatal(err)
		}
		mutate(&params)
		action.Args[0] = payloadForTaskScheduleTest(t, params)
	}
	tests := []struct {
		name   string
		mutate func(*testing.T, *client.ScheduleDescription)
	}{
		{"fingerprint", func(t *testing.T, d *client.ScheduleDescription) {
			var fp taskScheduleFingerprint
			action := d.Schedule.Action.(*client.ScheduleWorkflowAction)
			payload := action.Memo[taskScheduleMemoKey].(*commonpb.Payload)
			if err := converter.GetDefaultDataConverter().FromPayload(payload, &fp); err != nil {
				t.Fatalf("decode fingerprint fixture: %v", err)
			}
			fp.PreparedDigest = strings.Repeat("b", 64)
			action.Memo[taskScheduleMemoKey] = payloadForTaskScheduleTest(t, fp)
		}},
		{"lifecycle_phase", func(t *testing.T, d *client.ScheduleDescription) {
			setTaskScheduleLifecyclePhaseForTest(t, d, "unsupported")
		}},
		{"action_memo_nil", func(_ *testing.T, d *client.ScheduleDescription) {
			d.Schedule.Action.(*client.ScheduleWorkflowAction).Memo = nil
		}},
		{"action_memo_extra", func(t *testing.T, d *client.ScheduleDescription) {
			d.Schedule.Action.(*client.ScheduleWorkflowAction).Memo["other"] = payloadForTaskScheduleTest(t, "other")
		}},
		{"spec_nil", func(_ *testing.T, d *client.ScheduleDescription) { d.Schedule.Spec = nil }},
		{"spec", func(_ *testing.T, d *client.ScheduleDescription) { d.Schedule.Spec.TimeZoneName = "UTC" }},
		{"spec_start", func(_ *testing.T, d *client.ScheduleDescription) { d.Schedule.Spec.StartAt = time.Unix(1, 0) }},
		{"spec_end", func(_ *testing.T, d *client.ScheduleDescription) { d.Schedule.Spec.EndAt = time.Unix(2, 0) }},
		{"spec_jitter", func(_ *testing.T, d *client.ScheduleDescription) { d.Schedule.Spec.Jitter = time.Second }},
		{"spec_skip", func(_ *testing.T, d *client.ScheduleDescription) {
			d.Schedule.Spec.Skip = []client.ScheduleCalendarSpec{{}}
		}},
		{"spec_calendar_second", func(_ *testing.T, d *client.ScheduleDescription) {
			d.Schedule.Spec.Calendars[0].Second[0].Start++
		}},
		{"spec_calendar_minute", func(_ *testing.T, d *client.ScheduleDescription) {
			d.Schedule.Spec.Calendars[0].Minute[0].Start++
		}},
		{"spec_calendar_hour", func(_ *testing.T, d *client.ScheduleDescription) {
			d.Schedule.Spec.Calendars[0].Hour[0].Start++
		}},
		{"spec_calendar_day_of_month", func(_ *testing.T, d *client.ScheduleDescription) {
			d.Schedule.Spec.Calendars[0].DayOfMonth[0].Start++
		}},
		{"spec_calendar_month", func(_ *testing.T, d *client.ScheduleDescription) {
			d.Schedule.Spec.Calendars[0].Month[0].Start++
		}},
		{"spec_calendar_day_of_week", func(_ *testing.T, d *client.ScheduleDescription) {
			d.Schedule.Spec.Calendars[0].DayOfWeek[0].Start++
		}},
		{"policy_nil", func(_ *testing.T, d *client.ScheduleDescription) { d.Schedule.Policy = nil }},
		{"policy_overlap", func(_ *testing.T, d *client.ScheduleDescription) {
			d.Schedule.Policy.Overlap = enums.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL
		}},
		{"policy_catchup", func(_ *testing.T, d *client.ScheduleDescription) { d.Schedule.Policy.CatchupWindow++ }},
		{"policy_pause_failure", func(_ *testing.T, d *client.ScheduleDescription) { d.Schedule.Policy.PauseOnFailure = true }},
		{"state_nil", func(_ *testing.T, d *client.ScheduleDescription) { d.Schedule.State = nil }},
		{"limited_actions", func(_ *testing.T, d *client.ScheduleDescription) { d.Schedule.State.LimitedActions = true }},
		{"remaining_actions", func(_ *testing.T, d *client.ScheduleDescription) { d.Schedule.State.RemainingActions = 1 }},
		{"action_type", func(_ *testing.T, d *client.ScheduleDescription) { d.Schedule.Action = nil }},
		{"action_id", func(_ *testing.T, d *client.ScheduleDescription) {
			d.Schedule.Action.(*client.ScheduleWorkflowAction).ID = "other"
		}},
		{"workflow", func(_ *testing.T, d *client.ScheduleDescription) {
			d.Schedule.Action.(*client.ScheduleWorkflowAction).Workflow = "OtherWorkflow"
		}},
		{"task_queue", func(_ *testing.T, d *client.ScheduleDescription) {
			d.Schedule.Action.(*client.ScheduleWorkflowAction).TaskQueue = "other"
		}},
		{"execution_timeout", func(_ *testing.T, d *client.ScheduleDescription) {
			d.Schedule.Action.(*client.ScheduleWorkflowAction).WorkflowExecutionTimeout = time.Second
		}},
		{"run_timeout", func(_ *testing.T, d *client.ScheduleDescription) {
			d.Schedule.Action.(*client.ScheduleWorkflowAction).WorkflowRunTimeout = time.Second
		}},
		{"task_timeout", func(_ *testing.T, d *client.ScheduleDescription) {
			d.Schedule.Action.(*client.ScheduleWorkflowAction).WorkflowTaskTimeout++
		}},
		{"retry_policy", func(_ *testing.T, d *client.ScheduleDescription) {
			d.Schedule.Action.(*client.ScheduleWorkflowAction).RetryPolicy = &temporal.RetryPolicy{MaximumAttempts: 2}
		}},
		{"args_count", func(_ *testing.T, d *client.ScheduleDescription) {
			d.Schedule.Action.(*client.ScheduleWorkflowAction).Args = nil
		}},
		{"args_extra", func(t *testing.T, d *client.ScheduleDescription) {
			action := d.Schedule.Action.(*client.ScheduleWorkflowAction)
			action.Args = append(action.Args, payloadForTaskScheduleTest(t, "unexpected"))
		}},
		{"arg_type", func(_ *testing.T, d *client.ScheduleDescription) {
			d.Schedule.Action.(*client.ScheduleWorkflowAction).Args[0] = workflow.PushParams{}
		}},
		{"arg_payload_invalid", func(_ *testing.T, d *client.ScheduleDescription) {
			d.Schedule.Action.(*client.ScheduleWorkflowAction).Args[0] = &commonpb.Payload{
				Metadata: map[string][]byte{"encoding": []byte("json/plain")}, Data: []byte("{")}
		}},
		{"arg_user", func(t *testing.T, d *client.ScheduleDescription) {
			replaceParams(t, d, func(p *workflow.PushParams) { p.UserID++ })
		}},
		{"arg_run_kind", func(t *testing.T, d *client.ScheduleDescription) {
			replaceParams(t, d, func(p *workflow.PushParams) { p.RunKind = workflow.PushRunKindAdHoc })
		}},
		{"arg_schedule", func(t *testing.T, d *client.ScheduleDescription) {
			replaceParams(t, d, func(p *workflow.PushParams) { p.ScheduleID = "other" })
		}},
		{"arg_scope", func(t *testing.T, d *client.ScheduleDescription) {
			replaceParams(t, d, func(p *workflow.PushParams) { p.Scope.TopN++ })
		}},
		{"arg_source_ids", func(t *testing.T, d *client.ScheduleDescription) {
			replaceParams(t, d, func(p *workflow.PushParams) { p.Scope.SourceIDs = append(p.Scope.SourceIDs, 33) })
		}},
		{"arg_description", func(t *testing.T, d *client.ScheduleDescription) {
			replaceParams(t, d, func(p *workflow.PushParams) { p.NLDesc = "other" })
		}},
		{"workflow_memo", func(_ *testing.T, d *client.ScheduleDescription) {
			d.Schedule.Action.(*client.ScheduleWorkflowAction).Memo = map[string]interface{}{"x": "y"}
		}},
		{"untyped_search", func(t *testing.T, d *client.ScheduleDescription) {
			d.Schedule.Action.(*client.ScheduleWorkflowAction).UntypedSearchAttributes = map[string]*commonpb.Payload{
				"x": payloadForTaskScheduleTest(t, "y"),
			}
		}},
		{"typed_search", func(_ *testing.T, d *client.ScheduleDescription) {
			d.Schedule.Action.(*client.ScheduleWorkflowAction).TypedSearchAttributes = temporal.NewSearchAttributes(
				temporal.NewSearchAttributeKeyKeyword("x").ValueSet("y"),
			)
		}},
		{"version_override", func(_ *testing.T, d *client.ScheduleDescription) {
			d.Schedule.Action.(*client.ScheduleWorkflowAction).VersioningOverride = &client.AutoUpgradeVersioningOverride{}
		}},
		{"static_summary", func(_ *testing.T, d *client.ScheduleDescription) {
			d.Schedule.Action.(*client.ScheduleWorkflowAction).StaticSummary = "summary"
		}},
		{"static_details", func(_ *testing.T, d *client.ScheduleDescription) {
			d.Schedule.Action.(*client.ScheduleWorkflowAction).StaticDetails = "details"
		}},
		{"priority", func(_ *testing.T, d *client.ScheduleDescription) {
			d.Schedule.Action.(*client.ScheduleWorkflowAction).Priority.FairnessKey = "tenant"
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fake := newTaskScheduleFakeClient()
			s := newTaskScheduleTestScheduler(fake)
			expected := expectedTaskSchedule(t, s, validTaskScheduleRequest())
			fake.seed(taskScheduleCreateRequest(t, expected))
			fake.mutate(expected.taskID, func(d *client.ScheduleDescription) { tc.mutate(t, d) })
			_, err := s.EnsurePausedTask(context.Background(), validTaskScheduleRequest())
			requireTaskScheduleKind(t, err, ErrTaskScheduleConflict, TaskScheduleErrorConflict)
			if got := fake.counts(); got.create != 0 || got.unpause != 0 || got.delete != 0 {
				t.Fatalf("mutating calls=%+v", got)
			}
		})
	}
}

func TestEnsurePausedTask_RejectsIntervalFieldMismatchWithoutWrites(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*client.ScheduleIntervalSpec)
	}{
		{"every", func(interval *client.ScheduleIntervalSpec) { interval.Every++ }},
		{"phase", func(interval *client.ScheduleIntervalSpec) { interval.Offset++ }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fake := newTaskScheduleFakeClient()
			s := newTaskScheduleTestScheduler(fake)
			req := validTaskScheduleRequest()
			req.Spec = ScheduleSpec{
				EverySeconds: 7200,
				AnchorAt:     "2026-07-21T08:00:00+08:00",
				TZ:           "Asia/Shanghai",
			}
			expected := expectedTaskSchedule(t, s, req)
			fake.seed(taskScheduleCreateRequest(t, expected))
			fake.mutate(expected.taskID, func(d *client.ScheduleDescription) {
				tc.mutate(&d.Schedule.Spec.Intervals[0])
			})
			_, err := s.EnsurePausedTask(context.Background(), req)
			requireTaskScheduleKind(t, err, ErrTaskScheduleConflict, TaskScheduleErrorConflict)
			if got := fake.counts(); got.create != 0 || got.unpause != 0 || got.delete != 0 {
				t.Fatalf("mismatch caused a write: %+v", got)
			}
		})
	}
}

func TestEnsurePausedTask_RejectsRawHiddenFieldMismatchesWithoutWrites(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*workflowservice.DescribeScheduleResponse)
	}{
		{"keep_original_workflow_id", func(d *workflowservice.DescribeScheduleResponse) {
			d.Schedule.Policies.KeepOriginalWorkflowId = true
		}},
		{"workflow_header", func(d *workflowservice.DescribeScheduleResponse) {
			d.Schedule.Action.GetStartWorkflow().Header = &commonpb.Header{
				Fields: map[string]*commonpb.Payload{"trace": {}},
			}
		}},
		{"workflow_task_queue_normal_name", func(d *workflowservice.DescribeScheduleResponse) {
			d.Schedule.Action.GetStartWorkflow().TaskQueue.NormalName = "unexpected-sticky-parent"
		}},
		{"workflow_task_queue_kind", func(d *workflowservice.DescribeScheduleResponse) {
			d.Schedule.Action.GetStartWorkflow().TaskQueue.Kind = enums.TASK_QUEUE_KIND_STICKY
		}},
		{"workflow_cron_schedule", func(d *workflowservice.DescribeScheduleResponse) {
			d.Schedule.Action.GetStartWorkflow().CronSchedule = "0 9 * * *"
		}},
		{"workflow_id_reuse_policy", func(d *workflowservice.DescribeScheduleResponse) {
			d.Schedule.Action.GetStartWorkflow().WorkflowIdReusePolicy =
				enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE
		}},
		{"timezone_data", func(d *workflowservice.DescribeScheduleResponse) {
			d.Schedule.Spec.TimezoneData = []byte("hidden-tzif")
		}},
		{"cron_string", func(d *workflowservice.DescribeScheduleResponse) {
			d.Schedule.Spec.CronString = []string{"0 9 * * *"}
		}},
		{"legacy_calendar", func(d *workflowservice.DescribeScheduleResponse) {
			d.Schedule.Spec.Calendar = []*schedulepb.CalendarSpec{{Minute: "1"}}
		}},
		{"legacy_exclude_calendar", func(d *workflowservice.DescribeScheduleResponse) {
			d.Schedule.Spec.ExcludeCalendar = []*schedulepb.CalendarSpec{{Minute: "1"}}
		}},
		{"calendar_year", func(d *workflowservice.DescribeScheduleResponse) {
			d.Schedule.Spec.StructuredCalendar[0].Year = []*schedulepb.Range{{Start: 2026, End: 2026, Step: 1}}
		}},
		{"calendar_comment", func(d *workflowservice.DescribeScheduleResponse) {
			d.Schedule.Spec.StructuredCalendar[0].Comment = "unexpected"
		}},
		{"exclude_structured_calendar", func(d *workflowservice.DescribeScheduleResponse) {
			d.Schedule.Spec.ExcludeStructuredCalendar = []*schedulepb.StructuredCalendarSpec{{}}
		}},
		{"extra_structured_calendar", func(d *workflowservice.DescribeScheduleResponse) {
			d.Schedule.Spec.StructuredCalendar = append(
				d.Schedule.Spec.StructuredCalendar, &schedulepb.StructuredCalendarSpec{},
			)
		}},
		{"extra_interval", func(d *workflowservice.DescribeScheduleResponse) {
			d.Schedule.Spec.Interval = append(d.Schedule.Spec.Interval, &schedulepb.IntervalSpec{
				Interval: durationpb.New(time.Hour),
			})
		}},
		{"priority_key", func(d *workflowservice.DescribeScheduleResponse) {
			d.Schedule.Action.GetStartWorkflow().Priority.PriorityKey = 1
		}},
		{"priority_fairness_weight", func(d *workflowservice.DescribeScheduleResponse) {
			d.Schedule.Action.GetStartWorkflow().Priority.FairnessWeight = 1
		}},
		{"top_level_memo", func(d *workflowservice.DescribeScheduleResponse) {
			d.Memo = &commonpb.Memo{Fields: map[string]*commonpb.Payload{"unexpected": {}}}
		}},
		{"top_level_search_attributes", func(d *workflowservice.DescribeScheduleResponse) {
			d.SearchAttributes = &commonpb.SearchAttributes{
				IndexedFields: map[string]*commonpb.Payload{"unexpected": {}},
			}
		}},
		{"invalid_schedule_error", func(d *workflowservice.DescribeScheduleResponse) {
			d.Info.InvalidScheduleError = "invalid"
		}},
		{"empty_conflict_token", func(d *workflowservice.DescribeScheduleResponse) {
			d.ConflictToken = nil
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fake := newTaskScheduleFakeClient()
			fake.rawMutate = tc.mutate
			s := newTaskScheduleTestScheduler(fake)
			expected := expectedTaskSchedule(t, s, validTaskScheduleRequest())
			fake.seed(taskScheduleCreateRequest(t, expected))
			_, err := s.EnsurePausedTask(context.Background(), validTaskScheduleRequest())
			requireTaskScheduleKind(t, err, ErrTaskScheduleConflict, TaskScheduleErrorConflict)
			if got := fake.counts(); got.create != 0 || got.unpause != 0 || got.delete != 0 {
				t.Fatalf("hidden mismatch caused a write: %+v", got)
			}
		})
	}
}

func TestEnsurePausedTask_RejectsWrongProvisioningNoteWithoutWrites(t *testing.T) {
	fake := newTaskScheduleFakeClient()
	fake.rawMutate = func(description *workflowservice.DescribeScheduleResponse) {
		description.Schedule.State.Notes = "unexpected"
	}
	s := newTaskScheduleTestScheduler(fake)
	expected := expectedTaskSchedule(t, s, validTaskScheduleRequest())
	fake.seed(taskScheduleCreateRequest(t, expected))
	_, err := s.EnsurePausedTask(context.Background(), validTaskScheduleRequest())
	requireTaskScheduleKind(t, err, ErrTaskScheduleUnsafeState, TaskScheduleErrorUnsafeState)
	if got := fake.counts(); got.create != 0 || got.unpause != 0 || got.delete != 0 {
		t.Fatalf("wrong provisioning note caused a write: %+v", got)
	}
}

func TestEnsurePausedTask_RejectsUsedOrActiveSchedule(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*client.ScheduleDescription)
	}{
		{"used", func(d *client.ScheduleDescription) { d.Info.NumActions = 1 }},
		{"active", func(d *client.ScheduleDescription) { d.Schedule.State.Paused = false }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fake := newTaskScheduleFakeClient()
			s := newTaskScheduleTestScheduler(fake)
			expected := expectedTaskSchedule(t, s, validTaskScheduleRequest())
			fake.seed(taskScheduleCreateRequest(t, expected))
			fake.mutate(expected.taskID, tc.mutate)
			_, err := s.EnsurePausedTask(context.Background(), validTaskScheduleRequest())
			requireTaskScheduleKind(t, err, ErrTaskScheduleUnsafeState, TaskScheduleErrorUnsafeState)
			if got := fake.counts(); got.unpause != 0 || got.delete != 0 {
				t.Fatalf("mutating calls=%+v", got)
			}
		})
	}
}

func TestEnsurePausedTask_RejectsManuallyRepausedActivatedSchedule(t *testing.T) {
	fake := newTaskScheduleFakeClient()
	s := newTaskScheduleTestScheduler(fake)
	req := validTaskScheduleRequest()
	prepared := preparedTaskSchedule(t, s, req)
	created, err := s.Scheduler.EnsurePausedTask(context.Background(), prepared)
	if err != nil || created.Disposition != TaskScheduleEnsured {
		t.Fatalf("Ensure: result=%+v err=%v", created, err)
	}
	if _, err := s.Scheduler.ActivateTask(context.Background(), prepared, created.Snapshot); err != nil {
		t.Fatalf("Activate: %v", err)
	}
	fake.mutate(prepared.TaskID, func(d *client.ScheduleDescription) {
		d.Schedule.State.Paused = true
		// Deliberately forge the public provisioning marker. The lifecycle
		// phase written atomically by Activate must still prove this schedule
		// is no longer in Vane's provisioning state.
		d.Schedule.State.Note = prepared.Creation.Note
	})
	_, err = s.Scheduler.EnsurePausedTask(context.Background(), prepared)
	requireTaskScheduleKind(t, err, ErrTaskScheduleUnsafeState, TaskScheduleErrorUnsafeState)
	if got := fake.counts(); got.create != 1 || got.unpause != 1 || got.delete != 0 {
		t.Fatalf("manual re-pause caused an unintended write: %+v", got)
	}
}

func TestActivateTask_RejectsStateReplayAfterEnsureByRevision(t *testing.T) {
	fake := newTaskScheduleFakeClient()
	s := newTaskScheduleTestScheduler(fake)
	prepared := preparedTaskSchedule(t, s, validTaskScheduleRequest())
	ensured, err := s.Scheduler.EnsurePausedTask(context.Background(), prepared)
	if err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	if !fake.mutate(prepared.TaskID, func(*client.ScheduleDescription) {
		// Simulate an out-of-band Unpause followed by a perfect Pause replay:
		// the visible definition is unchanged, but Temporal's revision advances.
	}) {
		t.Fatal("seeded schedule disappeared")
	}
	_, err = s.Scheduler.ActivateTask(context.Background(), prepared, ensured.Snapshot)
	requireTaskScheduleKind(t, err, ErrTaskScheduleUnsafeState, TaskScheduleErrorUnsafeState)
	if got := fake.counts(); got.unpause != 0 {
		t.Fatalf("revision mismatch reached UpdateSchedule: %+v", got)
	}
}

func TestActivateTask_RejectsInvalidEnsureReceiptBeforeScheduleIO(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*TaskScheduleSnapshot)
	}{
		{"task_id", func(snapshot *TaskScheduleSnapshot) { snapshot.TaskID = "other" }},
		{"request_digest", func(snapshot *TaskScheduleSnapshot) { snapshot.RequestDigest = strings.Repeat("b", 64) }},
		{"prepared_digest", func(snapshot *TaskScheduleSnapshot) { snapshot.PreparedDigest = strings.Repeat("b", 64) }},
		{"state", func(snapshot *TaskScheduleSnapshot) { snapshot.State = TaskScheduleStateUnknown }},
		{"actions", func(snapshot *TaskScheduleSnapshot) { snapshot.NumActions = 1 }},
		{"empty_revision", func(snapshot *TaskScheduleSnapshot) { snapshot.Revision = "" }},
		{"invalid_revision", func(snapshot *TaskScheduleSnapshot) { snapshot.Revision = "!" }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fake := newTaskScheduleFakeClient()
			s := newTaskScheduleTestScheduler(fake)
			prepared := preparedTaskSchedule(t, s, validTaskScheduleRequest())
			ensured, err := s.Scheduler.EnsurePausedTask(context.Background(), prepared)
			if err != nil {
				t.Fatalf("Ensure: %v", err)
			}
			receipt := ensured.Snapshot
			tc.mutate(&receipt)
			before := fake.counts()
			_, err = s.Scheduler.ActivateTask(context.Background(), prepared, receipt)
			requireTaskScheduleKind(t, err, ErrTaskScheduleInvalid, TaskScheduleErrorInvalid)
			if after := fake.counts(); after != before {
				t.Fatalf("invalid receipt reached schedule I/O: before=%+v after=%+v", before, after)
			}
		})
	}
}

func TestActiveTaskWithWrongNoteIsUnknownAndUnsafeToActivate(t *testing.T) {
	fake := newTaskScheduleFakeClient()
	s := newTaskScheduleTestScheduler(fake)
	prepared := preparedTaskSchedule(t, s, validTaskScheduleRequest())
	expected, err := s.Scheduler.buildTaskScheduleExpected(context.Background(), prepared, "test", false)
	if err != nil {
		t.Fatal(err)
	}
	fake.seed(taskScheduleCreateRequest(t, expected))
	fake.mutate(prepared.TaskID, func(d *client.ScheduleDescription) {
		d.Schedule.State.Paused = false
		d.Schedule.State.Note = "unknown activation source"
	})
	snapshot, err := s.Scheduler.DescribeTask(context.Background(), prepared)
	if err != nil || snapshot.State != TaskScheduleStateUnknown {
		t.Fatalf("Describe: snapshot=%+v err=%v", snapshot, err)
	}
	_, err = s.Scheduler.ActivateTask(
		context.Background(), prepared, taskScheduleFakeActivationReceipt(fake, expected),
	)
	requireTaskScheduleKind(t, err, ErrTaskScheduleUnsafeState, TaskScheduleErrorUnsafeState)
	if got := fake.counts(); got.unpause != 0 || got.delete != 0 || got.create != 0 {
		t.Fatalf("unknown active state caused a write: %+v", got)
	}
}

func TestTaskSchedulePrimitives_PreCanceledContextStopsBeforeIO(t *testing.T) {
	tests := []struct {
		name string
		call func(*Scheduler, context.Context, PreparedTaskSchedule) error
	}{
		{"ensure", func(s *Scheduler, ctx context.Context, prepared PreparedTaskSchedule) error {
			_, err := s.EnsurePausedTask(ctx, prepared)
			return err
		}},
		{"describe", func(s *Scheduler, ctx context.Context, prepared PreparedTaskSchedule) error {
			_, err := s.DescribeTask(ctx, prepared)
			return err
		}},
		{"activate", func(s *Scheduler, ctx context.Context, prepared PreparedTaskSchedule) error {
			_, err := s.ActivateTask(ctx, prepared, TaskScheduleSnapshot{})
			return err
		}},
		{"delete", func(s *Scheduler, ctx context.Context, prepared PreparedTaskSchedule) error {
			return s.DeleteTask(ctx, prepared)
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fake := newTaskScheduleFakeClient()
			s := newTaskScheduleTestScheduler(fake)
			prepared := preparedTaskSchedule(t, s, validTaskScheduleRequest())
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			err := tc.call(s.Scheduler, ctx, prepared)
			requireTaskScheduleKind(t, err, ErrTaskScheduleTransient, TaskScheduleErrorTransient)
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("error chain lost context.Canceled: %v", err)
			}
			requireNoTaskScheduleIO(t, fake)
		})
	}

	t.Run("prepare", func(t *testing.T) {
		fake := newTaskScheduleFakeClient()
		s := newTaskScheduleTestScheduler(fake)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := s.Scheduler.PrepareTaskSchedule(ctx, validTaskScheduleRequest())
		requireTaskScheduleKind(t, err, ErrTaskScheduleTransient, TaskScheduleErrorTransient)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error chain lost context.Canceled: %v", err)
		}
		requireNoTaskScheduleIO(t, fake)
	})
}

func TestDescribeTask_ReportsVirginUsedPausedAndActiveStates(t *testing.T) {
	tests := []struct {
		name   string
		state  TaskScheduleState
		mutate func(*client.ScheduleDescription)
	}{
		{"paused_provisioning", TaskSchedulePausedProvisioningExact, func(*client.ScheduleDescription) {}},
		{"paused_used_actions", TaskSchedulePausedUsedExact, func(d *client.ScheduleDescription) { d.Info.NumActions = 2 }},
		{"paused_used_missed", TaskSchedulePausedUsedExact, func(d *client.ScheduleDescription) { d.Info.NumActionsMissedCatchupWindow = 1 }},
		{"paused_used_skipped", TaskSchedulePausedUsedExact, func(d *client.ScheduleDescription) { d.Info.NumActionsSkippedOverlap = 1 }},
		{"paused_used_running", TaskSchedulePausedUsedExact, func(d *client.ScheduleDescription) {
			d.Info.RunningWorkflows = []client.ScheduleWorkflowExecution{{WorkflowID: "wf-running"}}
		}},
		{"paused_used_recent", TaskSchedulePausedUsedExact, func(d *client.ScheduleDescription) {
			d.Info.RecentActions = []client.ScheduleActionResult{{ActualTime: time.Now()}}
		}},
		{"active_virgin", TaskScheduleActiveVirginExact, func(d *client.ScheduleDescription) {
			setTaskScheduleLifecyclePhaseForTest(t, d, taskScheduleV1PhaseActive)
			d.Schedule.State.Paused = false
			d.Schedule.State.Note = taskScheduleV1ActivationNote
		}},
		{"active_used", TaskScheduleActiveUsedExact, func(d *client.ScheduleDescription) {
			setTaskScheduleLifecyclePhaseForTest(t, d, taskScheduleV1PhaseActive)
			d.Schedule.State.Paused = false
			d.Schedule.State.Note = taskScheduleV1ActivationNote
			d.Info.NumActions = 2
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fake := newTaskScheduleFakeClient()
			s := newTaskScheduleTestScheduler(fake)
			expected := expectedTaskSchedule(t, s, validTaskScheduleRequest())
			fake.seed(taskScheduleCreateRequest(t, expected))
			fake.mutate(expected.taskID, tc.mutate)
			snapshot, err := s.DescribeTask(context.Background(), validTaskScheduleRequest())
			if err != nil {
				t.Fatal(err)
			}
			if snapshot.State != tc.state {
				t.Fatalf("state=%s, want %s", snapshot.State, tc.state)
			}
			if got := fake.counts(); got.describe != 1 || got.create != 0 || got.unpause != 0 || got.delete != 0 {
				t.Fatalf("calls=%+v", got)
			}
		})
	}
}

func TestDescribeTask_RawBufferCountersMarkPausedScheduleUsed(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*schedulepb.ScheduleInfo)
	}{
		{"buffer_dropped", func(info *schedulepb.ScheduleInfo) { info.BufferDropped = 1 }},
		{"buffer_size", func(info *schedulepb.ScheduleInfo) { info.BufferSize = 1 }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fake := newTaskScheduleFakeClient()
			fake.rawMutate = func(description *workflowservice.DescribeScheduleResponse) {
				tc.mutate(description.Info)
			}
			s := newTaskScheduleTestScheduler(fake)
			expected := expectedTaskSchedule(t, s, validTaskScheduleRequest())
			fake.seed(taskScheduleCreateRequest(t, expected))
			snapshot, err := s.DescribeTask(context.Background(), validTaskScheduleRequest())
			if err != nil || snapshot.State != TaskSchedulePausedUsedExact {
				t.Fatalf("snapshot=%+v err=%v, want paused used", snapshot, err)
			}
		})
	}
}

func TestDescribeTask_NotFoundPreservesTypedErrorChain(t *testing.T) {
	fake := newTaskScheduleFakeClient()
	s := newTaskScheduleTestScheduler(fake)
	req := validTaskScheduleRequest()
	_, err := s.DescribeTask(context.Background(), req)
	typed := requireTaskScheduleKind(t, err, ErrTaskScheduleNotFound, TaskScheduleErrorNotFound)
	wantID, _ := TaskIDForOperation(req.TenantID, req.UserID, req.OperationID)
	if typed.Operation != "describe" || typed.TaskID != wantID {
		t.Fatalf("typed error=%+v", typed)
	}
	var appErr *types.AppError
	if !errors.As(err, &appErr) || appErr.Code != types.CodeNotFound || appErr.Retryable {
		t.Fatalf("AppError=%+v", appErr)
	}
	var notFound *serviceerror.NotFound
	if !errors.As(err, &notFound) {
		t.Fatalf("underlying serviceerror.NotFound was lost: %v", err)
	}
	if got := fake.counts(); got.describe != 1 || got.create != 0 || got.unpause != 0 || got.delete != 0 {
		t.Fatalf("calls=%+v", got)
	}
}

func TestDescribeTask_ClassifiesTemporalReadErrors(t *testing.T) {
	ordinary := errors.New("connection reset")
	tests := []struct {
		name      string
		cause     error
		sentinel  error
		kind      TaskScheduleErrorKind
		code      types.ErrCode
		retryable bool
		assertAs  func(error) bool
	}{
		{"invalid_argument", serviceerror.NewInvalidArgument("invalid request"), ErrTaskScheduleInvalid, TaskScheduleErrorInvalid, types.CodeValidation, false, func(err error) bool {
			var target *serviceerror.InvalidArgument
			return errors.As(err, &target)
		}},
		{"permission_denied", serviceerror.NewPermissionDenied("denied", "policy"), ErrTaskScheduleBlocked, TaskScheduleErrorBlocked, types.CodeInternal, false, func(err error) bool {
			var target *serviceerror.PermissionDenied
			return errors.As(err, &target)
		}},
		{"namespace_not_found", serviceerror.NewNamespaceNotFound("missing"), ErrTaskScheduleBlocked, TaskScheduleErrorBlocked, types.CodeInternal, false, func(err error) bool {
			var target *serviceerror.NamespaceNotFound
			return errors.As(err, &target)
		}},
		{"client_version_not_supported", serviceerror.NewClientVersionNotSupported("1", "test", ">=2"), ErrTaskScheduleBlocked, TaskScheduleErrorBlocked, types.CodeInternal, false, func(err error) bool {
			var target *serviceerror.ClientVersionNotSupported
			return errors.As(err, &target)
		}},
		{"server_version_not_supported", serviceerror.NewServerVersionNotSupported("1", ">=2"), ErrTaskScheduleBlocked, TaskScheduleErrorBlocked, types.CodeInternal, false, func(err error) bool {
			var target *serviceerror.ServerVersionNotSupported
			return errors.As(err, &target)
		}},
		{"unimplemented", serviceerror.NewUnimplemented("DescribeSchedule unsupported"), ErrTaskScheduleBlocked, TaskScheduleErrorBlocked, types.CodeInternal, false, func(err error) bool {
			var target *serviceerror.Unimplemented
			return errors.As(err, &target)
		}},
		{"failed_precondition", serviceerror.NewFailedPrecondition("bad state"), ErrTaskScheduleConflict, TaskScheduleErrorConflict, types.CodeConflict, false, func(err error) bool {
			var target *serviceerror.FailedPrecondition
			return errors.As(err, &target)
		}},
		{"unavailable", serviceerror.NewUnavailable("unavailable"), ErrTaskScheduleTransient, TaskScheduleErrorTransient, types.CodeInternal, true, func(err error) bool {
			var target *serviceerror.Unavailable
			return errors.As(err, &target)
		}},
		{"ordinary_transient", ordinary, ErrTaskScheduleTransient, TaskScheduleErrorTransient, types.CodeInternal, true, func(err error) bool {
			return errors.Is(err, ordinary)
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fake := newTaskScheduleFakeClient()
			fake.describeErr = tc.cause
			s := newTaskScheduleTestScheduler(fake)
			_, err := s.DescribeTask(context.Background(), validTaskScheduleRequest())
			requireTaskScheduleKind(t, err, tc.sentinel, tc.kind)
			if !tc.assertAs(err) {
				t.Fatalf("underlying %T was lost: %v", tc.cause, err)
			}
			var appErr *types.AppError
			if !errors.As(err, &appErr) || appErr.Code != tc.code || appErr.Retryable != tc.retryable {
				t.Fatalf("AppError=%+v", appErr)
			}
			if got := fake.counts(); got.describe != 1 || got.create != 0 || got.unpause != 0 || got.delete != 0 {
				t.Fatalf("calls=%+v", got)
			}
		})
	}
}

func TestActivateTask_ConvergesAfterSuccessOrCommitThenError(t *testing.T) {
	tests := []struct {
		name      string
		commitErr error
	}{
		{"success", nil},
		{"response_lost", context.DeadlineExceeded},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fake := newTaskScheduleFakeClient()
			fake.unpauseCommitErr = tc.commitErr
			s := newTaskScheduleTestScheduler(fake)
			expected := expectedTaskSchedule(t, s, validTaskScheduleRequest())
			fake.seed(taskScheduleCreateRequest(t, expected))
			snapshot, err := s.ActivateTask(context.Background(), validTaskScheduleRequest())
			if err != nil {
				t.Fatal(err)
			}
			if snapshot.State != TaskScheduleActiveVirginExact {
				t.Fatalf("snapshot=%+v", snapshot)
			}
			if got := fake.counts(); got.describe != 2 || got.unpause != 1 || got.create != 0 || got.delete != 0 {
				t.Fatalf("calls=%+v", got)
			}
		})
	}
}

func TestActivateTask_IdempotenceAndFailureBoundaries(t *testing.T) {
	tests := []struct {
		name       string
		seed       bool
		mutate     func(*client.ScheduleDescription)
		unpauseErr error
		wantState  TaskScheduleState
		wantErr    error
		wantKind   TaskScheduleErrorKind
		wantCalls  int
	}{
		{"already_active", true, func(d *client.ScheduleDescription) {
			setTaskScheduleLifecyclePhaseForTest(t, d, taskScheduleV1PhaseActive)
			d.Schedule.State.Paused = false
			d.Schedule.State.Note = taskScheduleV1ActivationNote
		}, nil, TaskScheduleActiveVirginExact, nil, "", 0},
		{"already_active_used", true, func(d *client.ScheduleDescription) {
			setTaskScheduleLifecyclePhaseForTest(t, d, taskScheduleV1PhaseActive)
			d.Schedule.State.Paused = false
			d.Schedule.State.Note = taskScheduleV1ActivationNote
			d.Info.NumActions = 1
		}, nil, TaskScheduleActiveUsedExact, nil, "", 0},
		{"paused_used", true, func(d *client.ScheduleDescription) { d.Info.NumActions = 1 }, nil, "", ErrTaskScheduleUnsafeState, TaskScheduleErrorUnsafeState, 0},
		{"unpause_pre_error", true, func(*client.ScheduleDescription) {}, errors.New("unpause unavailable"), "", ErrTaskScheduleTransient, TaskScheduleErrorTransient, 1},
		{"stale_update_token", true, func(*client.ScheduleDescription) {}, serviceerror.NewFailedPrecondition("stale token"), "", ErrTaskScheduleConflict, TaskScheduleErrorConflict, 1},
		{"missing", false, func(*client.ScheduleDescription) {}, nil, "", ErrTaskScheduleNotFound, TaskScheduleErrorNotFound, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fake := newTaskScheduleFakeClient()
			fake.unpauseErr = tc.unpauseErr
			s := newTaskScheduleTestScheduler(fake)
			expected := expectedTaskSchedule(t, s, validTaskScheduleRequest())
			if tc.seed {
				fake.seed(taskScheduleCreateRequest(t, expected))
				fake.mutate(expected.taskID, tc.mutate)
			}
			snapshot, err := s.ActivateTask(context.Background(), validTaskScheduleRequest())
			if tc.wantErr == nil {
				if err != nil || snapshot.State != tc.wantState {
					t.Fatalf("snapshot=%+v err=%v", snapshot, err)
				}
			} else {
				requireTaskScheduleKind(t, err, tc.wantErr, tc.wantKind)
			}
			if got := fake.counts(); got.unpause != tc.wantCalls || got.create != 0 || got.delete != 0 {
				t.Fatalf("calls=%+v", got)
			}
			if tc.name == "unpause_pre_error" {
				desc, ok := fake.snapshot(expected.taskID)
				if !ok || !desc.Schedule.State.Paused {
					t.Fatal("pre-commit Unpause error changed state")
				}
			}
		})
	}
}

func TestActivateTask_PostDescribeOutcomeBoundaries(t *testing.T) {
	tests := []struct {
		name          string
		configure     func(*taskScheduleFakeClient)
		wantSentinel  error
		wantKind      TaskScheduleErrorKind
		wantRemaining bool
	}{
		{"post_transport_failure", func(f *taskScheduleFakeClient) {
			f.describeErrors = []error{nil, errors.New("describe unavailable")}
		}, ErrTaskScheduleOutcomeUnknown, TaskScheduleErrorOutcomeUnknown, true},
		{"nil_but_state_unchanged", func(f *taskScheduleFakeClient) {
			f.unpauseNoCommit = true
		}, ErrTaskScheduleTransient, TaskScheduleErrorTransient, true},
		{"post_not_found", func(f *taskScheduleFakeClient) {
			f.unpauseDelete = true
		}, ErrTaskScheduleNotFound, TaskScheduleErrorNotFound, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fake := newTaskScheduleFakeClient()
			tc.configure(fake)
			s := newTaskScheduleTestScheduler(fake)
			expected := expectedTaskSchedule(t, s, validTaskScheduleRequest())
			fake.seed(taskScheduleCreateRequest(t, expected))
			_, err := s.ActivateTask(context.Background(), validTaskScheduleRequest())
			requireTaskScheduleKind(t, err, tc.wantSentinel, tc.wantKind)
			_, remaining := fake.snapshot(expected.taskID)
			if remaining != tc.wantRemaining {
				t.Fatalf("schedule remaining=%v, want %v", remaining, tc.wantRemaining)
			}
			if got := fake.counts(); got.describe != 2 || got.unpause != 1 || got.delete != 0 {
				t.Fatalf("calls=%+v", got)
			}
		})
	}
}

func TestDeleteTask_ConvergesAfterSuccessOrCommitThenError(t *testing.T) {
	tests := []struct {
		name      string
		commitErr error
	}{
		{"success", nil},
		{"response_lost", context.DeadlineExceeded},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fake := newTaskScheduleFakeClient()
			fake.deleteCommitErr = tc.commitErr
			s := newTaskScheduleTestScheduler(fake)
			expected := expectedTaskSchedule(t, s, validTaskScheduleRequest())
			fake.seed(taskScheduleCreateRequest(t, expected))
			if err := s.DeleteTask(context.Background(), validTaskScheduleRequest()); err != nil {
				t.Fatal(err)
			}
			if _, ok := fake.snapshot(expected.taskID); ok {
				t.Fatal("schedule still exists after converged delete")
			}
			if got := fake.counts(); got.describe != 2 || got.delete != 1 || got.create != 0 || got.unpause != 0 {
				t.Fatalf("calls=%+v", got)
			}
		})
	}
}

func TestDeleteTask_IdempotenceAndFailureBoundaries(t *testing.T) {
	t.Run("already_missing", func(t *testing.T) {
		fake := newTaskScheduleFakeClient()
		s := newTaskScheduleTestScheduler(fake)
		if err := s.DeleteTask(context.Background(), validTaskScheduleRequest()); err != nil {
			t.Fatal(err)
		}
		if got := fake.counts(); got.describe != 1 || got.delete != 0 {
			t.Fatalf("calls=%+v", got)
		}
	})

	t.Run("delete_pre_error", func(t *testing.T) {
		fake := newTaskScheduleFakeClient()
		fake.deleteErr = errors.New("delete unavailable")
		s := newTaskScheduleTestScheduler(fake)
		expected := expectedTaskSchedule(t, s, validTaskScheduleRequest())
		fake.seed(taskScheduleCreateRequest(t, expected))
		err := s.DeleteTask(context.Background(), validTaskScheduleRequest())
		requireTaskScheduleKind(t, err, ErrTaskScheduleTransient, TaskScheduleErrorTransient)
		if _, ok := fake.snapshot(expected.taskID); !ok {
			t.Fatal("pre-commit Delete error removed schedule")
		}
		if got := fake.counts(); got.describe != 2 || got.delete != 1 {
			t.Fatalf("calls=%+v", got)
		}
	})

	t.Run("mismatch_blocks_delete", func(t *testing.T) {
		fake := newTaskScheduleFakeClient()
		s := newTaskScheduleTestScheduler(fake)
		expected := expectedTaskSchedule(t, s, validTaskScheduleRequest())
		fake.seed(taskScheduleCreateRequest(t, expected))
		fake.mutate(expected.taskID, func(d *client.ScheduleDescription) { d.Schedule.Policy.PauseOnFailure = true })
		err := s.DeleteTask(context.Background(), validTaskScheduleRequest())
		requireTaskScheduleKind(t, err, ErrTaskScheduleConflict, TaskScheduleErrorConflict)
		if got := fake.counts(); got.delete != 0 || got.describe != 1 {
			t.Fatalf("calls=%+v", got)
		}
	})
}

func TestDeleteTask_PostDescribeOutcomeBoundaries(t *testing.T) {
	t.Run("post_transport_failure", func(t *testing.T) {
		fake := newTaskScheduleFakeClient()
		fake.describeErrors = []error{nil, errors.New("describe unavailable")}
		s := newTaskScheduleTestScheduler(fake)
		expected := expectedTaskSchedule(t, s, validTaskScheduleRequest())
		fake.seed(taskScheduleCreateRequest(t, expected))
		err := s.DeleteTask(context.Background(), validTaskScheduleRequest())
		requireTaskScheduleKind(t, err, ErrTaskScheduleOutcomeUnknown, TaskScheduleErrorOutcomeUnknown)
		if _, ok := fake.snapshot(expected.taskID); ok {
			t.Fatal("Delete committed before its response became uncertain")
		}
		if got := fake.counts(); got.describe != 2 || got.delete != 1 {
			t.Fatalf("calls=%+v", got)
		}
	})

	t.Run("nil_but_state_unchanged", func(t *testing.T) {
		fake := newTaskScheduleFakeClient()
		fake.deleteNoCommit = true
		s := newTaskScheduleTestScheduler(fake)
		expected := expectedTaskSchedule(t, s, validTaskScheduleRequest())
		fake.seed(taskScheduleCreateRequest(t, expected))
		err := s.DeleteTask(context.Background(), validTaskScheduleRequest())
		requireTaskScheduleKind(t, err, ErrTaskScheduleTransient, TaskScheduleErrorTransient)
		if _, ok := fake.snapshot(expected.taskID); !ok {
			t.Fatal("no-op Delete unexpectedly removed schedule")
		}
		if got := fake.counts(); got.describe != 2 || got.delete != 1 {
			t.Fatalf("calls=%+v", got)
		}
	})
}

func TestTaskScheduleMutations_CallerCanceledAfterCommitStillConverge(t *testing.T) {
	t.Run("create", func(t *testing.T) {
		fake := newTaskScheduleFakeClient()
		s := newTaskScheduleTestScheduler(fake)
		prepared := preparedTaskSchedule(t, s, validTaskScheduleRequest())
		ctx, cancel := context.WithCancel(context.Background())
		fake.afterCommit = cancel
		fake.createCommitErr = context.Canceled
		result, err := s.Scheduler.EnsurePausedTask(ctx, prepared)
		if ctx.Err() != context.Canceled {
			t.Fatalf("commit hook did not cancel caller: %v", ctx.Err())
		}
		if err != nil || result.Disposition != TaskScheduleEnsured ||
			result.Snapshot.State != TaskSchedulePausedVirginExact {
			t.Fatalf("create replay: result=%+v err=%v", result, err)
		}
		if got := fake.counts(); got.create != 2 || got.describe != 2 {
			t.Fatalf("calls=%+v", got)
		}
	})

	t.Run("unpause", func(t *testing.T) {
		fake := newTaskScheduleFakeClient()
		s := newTaskScheduleTestScheduler(fake)
		prepared := preparedTaskSchedule(t, s, validTaskScheduleRequest())
		expected, err := s.Scheduler.buildTaskScheduleExpected(
			context.Background(), prepared, "test", false,
		)
		if err != nil {
			t.Fatal(err)
		}
		fake.seed(taskScheduleCreateRequest(t, expected))
		ctx, cancel := context.WithCancel(context.Background())
		fake.afterCommit = cancel
		fake.unpauseCommitErr = context.Canceled
		receipt := taskScheduleFakeActivationReceipt(fake, expected)
		snapshot, err := s.Scheduler.ActivateTask(ctx, prepared, receipt)
		if err != nil || snapshot.State != TaskScheduleActiveVirginExact {
			t.Fatalf("Unpause recovery: snapshot=%+v err=%v", snapshot, err)
		}
		if ctx.Err() != context.Canceled {
			t.Fatalf("commit hook did not cancel caller: %v", ctx.Err())
		}
		if got := fake.counts(); got.unpause != 1 || got.describe != 2 {
			t.Fatalf("calls=%+v", got)
		}
	})

	t.Run("delete", func(t *testing.T) {
		fake := newTaskScheduleFakeClient()
		s := newTaskScheduleTestScheduler(fake)
		prepared := preparedTaskSchedule(t, s, validTaskScheduleRequest())
		expected, err := s.Scheduler.buildTaskScheduleExpected(
			context.Background(), prepared, "test", false,
		)
		if err != nil {
			t.Fatal(err)
		}
		fake.seed(taskScheduleCreateRequest(t, expected))
		ctx, cancel := context.WithCancel(context.Background())
		fake.afterCommit = cancel
		fake.deleteCommitErr = context.Canceled
		if err := s.Scheduler.DeleteTask(ctx, prepared); err != nil {
			t.Fatalf("Delete recovery: %v", err)
		}
		if ctx.Err() != context.Canceled {
			t.Fatalf("commit hook did not cancel caller: %v", ctx.Err())
		}
		if _, ok := fake.snapshot(prepared.TaskID); ok {
			t.Fatal("schedule remained after committed Delete")
		}
		if got := fake.counts(); got.delete != 1 || got.describe != 2 {
			t.Fatalf("calls=%+v", got)
		}
	})
}

func TestEnsurePausedTask_OutcomeUnknownWithCanceledCauseRemainsRetryable(t *testing.T) {
	fake := newTaskScheduleFakeClient()
	fake.describeErrors = []error{nil, errors.New("detached recovery read failed")}
	fake.createErr = context.Canceled
	s := newTaskScheduleTestScheduler(fake)
	prepared := preparedTaskSchedule(t, s, validTaskScheduleRequest())
	_, err := s.Scheduler.EnsurePausedTask(context.Background(), prepared)
	requireTaskScheduleKind(t, err, ErrTaskScheduleOutcomeUnknown, TaskScheduleErrorOutcomeUnknown)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("OutcomeUnknown lost canceled write cause: %v", err)
	}
	var appErr *types.AppError
	if !errors.As(err, &appErr) || appErr.Code != types.CodeInternal || !appErr.Retryable {
		t.Fatalf("OutcomeUnknown must remain retryable: %+v", appErr)
	}
}

func TestEnsurePausedTask_DeterministicWriteRejectionWinsOverRecoveryReadError(t *testing.T) {
	tests := []struct {
		name     string
		writeErr error
		sentinel error
		kind     TaskScheduleErrorKind
	}{
		{"invalid_argument", serviceerror.NewInvalidArgument("invalid raw schedule"), ErrTaskScheduleInvalid, TaskScheduleErrorInvalid},
		{"permission_denied", serviceerror.NewPermissionDenied("denied", "policy"), ErrTaskScheduleBlocked, TaskScheduleErrorBlocked},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fake := newTaskScheduleFakeClient()
			fake.createErr = tc.writeErr
			fake.describeErrors = []error{nil, serviceerror.NewUnavailable("recovery unavailable")}
			s := newTaskScheduleTestScheduler(fake)
			_, err := s.EnsurePausedTask(context.Background(), validTaskScheduleRequest())
			requireTaskScheduleKind(t, err, tc.sentinel, tc.kind)
			if errors.Is(err, ErrTaskScheduleOutcomeUnknown) {
				t.Fatalf("deterministically rejected write was mislabeled OutcomeUnknown: %v", err)
			}
			if got := fake.counts(); got.create != 1 || got.describe != 2 {
				t.Fatalf("calls=%+v", got)
			}
		})
	}
}

func TestEnsurePausedTask_CreateCollisionRequiresRecoveryEvidence(t *testing.T) {
	collision := serviceerror.NewWorkflowExecutionAlreadyStarted(
		"schedule exists", "existing-request", "",
	)
	t.Run("recovery_unavailable", func(t *testing.T) {
		fake := newTaskScheduleFakeClient()
		fake.createErr = collision
		fake.describeErrors = []error{nil, serviceerror.NewUnavailable("recovery unavailable")}
		s := newTaskScheduleTestScheduler(fake)
		_, err := s.EnsurePausedTask(context.Background(), validTaskScheduleRequest())
		requireTaskScheduleKind(t, err, ErrTaskScheduleOutcomeUnknown, TaskScheduleErrorOutcomeUnknown)
		if !errors.Is(err, collision) {
			t.Fatalf("collision cause was lost: %v", err)
		}
	})

	t.Run("object_disappeared", func(t *testing.T) {
		fake := newTaskScheduleFakeClient()
		fake.createErr = collision
		s := newTaskScheduleTestScheduler(fake)
		_, err := s.EnsurePausedTask(context.Background(), validTaskScheduleRequest())
		requireTaskScheduleKind(t, err, ErrTaskScheduleTransient, TaskScheduleErrorTransient)
		if !errors.Is(err, collision) {
			t.Fatalf("collision cause was lost: %v", err)
		}
	})
}

func TestEnsurePausedTask_ConcurrentSameRequestConvergesThroughRequestIDReplay(t *testing.T) {
	fake := newTaskScheduleFakeClient()
	s := newTaskScheduleTestScheduler(fake)
	req := validTaskScheduleRequest()

	const callers = 20
	start := make(chan struct{})
	results := make(chan EnsurePausedTaskResult, callers)
	errs := make(chan error, callers)
	var wg sync.WaitGroup
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			result, err := s.EnsurePausedTask(context.Background(), req)
			results <- result
			errs <- err
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	close(errs)

	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent EnsurePausedTask: %v", err)
		}
	}
	ensured := 0
	for result := range results {
		if result.Snapshot.State != TaskSchedulePausedVirginExact {
			t.Fatalf("result=%+v", result)
		}
		if result.Disposition == TaskScheduleEnsured {
			ensured++
		}
	}
	if ensured != callers {
		t.Fatalf("ensured dispositions=%d, want %d", ensured, callers)
	}
	if got := fake.counts(); got.create != callers {
		t.Fatalf("Create calls=%d, want one create/replay per caller; all calls=%+v", got.create, got)
	}
}

func TestEnsurePausedTask_CanceledGateWaiterDoesNoIOAndDoesNotLeak(t *testing.T) {
	fake := newTaskScheduleFakeClient()
	fake.describeBlockAt = 1
	fake.describeEntered = make(chan struct{})
	fake.describeRelease = make(chan struct{})
	s := newTaskScheduleTestScheduler(fake)
	req := validTaskScheduleRequest()
	taskID, _ := TaskIDForOperation(req.TenantID, req.UserID, req.OperationID)

	type ensureOutcome struct {
		result EnsurePausedTaskResult
		err    error
	}
	holderDone := make(chan ensureOutcome, 1)
	go func() {
		result, err := s.EnsurePausedTask(context.Background(), req)
		holderDone <- ensureOutcome{result: result, err: err}
	}()
	<-fake.describeEntered

	waiterCtx, cancelWaiter := context.WithCancel(context.Background())
	waiterDone := make(chan error, 1)
	go func() {
		_, err := s.EnsurePausedTask(waiterCtx, req)
		waiterDone <- err
	}()

	deadline := time.Now().Add(5 * time.Second)
	for {
		s.taskScheduleGates.mu.Lock()
		gate := s.taskScheduleGates.byID[taskID]
		refs := 0
		if gate != nil {
			refs = gate.refs
		}
		s.taskScheduleGates.mu.Unlock()
		if refs == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("waiter did not enter gate; refs=%d", refs)
		}
		runtime.Gosched()
	}

	beforeCancel := fake.counts()
	cancelWaiter()
	waiterErr := <-waiterDone
	requireTaskScheduleKind(t, waiterErr, ErrTaskScheduleTransient, TaskScheduleErrorTransient)
	if !errors.Is(waiterErr, context.Canceled) {
		t.Fatalf("waiter lost context.Canceled: %v", waiterErr)
	}
	if afterCancel := fake.counts(); afterCancel != beforeCancel {
		t.Fatalf("canceled gate waiter performed Temporal I/O: before=%+v after=%+v", beforeCancel, afterCancel)
	}

	s.taskScheduleGates.mu.Lock()
	gate := s.taskScheduleGates.byID[taskID]
	remainingRefs := 0
	if gate != nil {
		remainingRefs = gate.refs
	}
	s.taskScheduleGates.mu.Unlock()
	if remainingRefs != 1 {
		t.Fatalf("canceled waiter leaked gate reference: refs=%d, want holder only", remainingRefs)
	}

	close(fake.describeRelease)
	holder := <-holderDone
	if holder.err != nil || holder.result.Disposition != TaskScheduleEnsured {
		t.Fatalf("holder result=%+v err=%v", holder.result, holder.err)
	}
	s.taskScheduleGates.mu.Lock()
	leakedEntries := len(s.taskScheduleGates.byID)
	s.taskScheduleGates.mu.Unlock()
	if leakedEntries != 0 {
		t.Fatalf("gate map leaked %d entries after holder release", leakedEntries)
	}

	after, err := s.EnsurePausedTask(context.Background(), req)
	if err != nil || after.Disposition != TaskScheduleEnsured {
		t.Fatalf("request after canceled waiter: result=%+v err=%v", after, err)
	}
	s.taskScheduleGates.mu.Lock()
	leakedEntries = len(s.taskScheduleGates.byID)
	s.taskScheduleGates.mu.Unlock()
	if leakedEntries != 0 {
		t.Fatalf("gate map leaked %d entries after subsequent request", leakedEntries)
	}
}

func TestEnsurePausedTask_SameTaskIDDifferentFingerprintIsStableConflict(t *testing.T) {
	fake := newTaskScheduleFakeClient()
	s := newTaskScheduleTestScheduler(fake)
	original := validTaskScheduleRequest()
	first, err := s.EnsurePausedTask(context.Background(), original)
	if err != nil || first.Disposition != TaskScheduleEnsured {
		t.Fatalf("initial ensure: result=%+v err=%v", first, err)
	}

	changed := original
	changed.PreparedDigest = strings.Repeat("b", 64)
	for attempt := 0; attempt < 5; attempt++ {
		_, err := s.EnsurePausedTask(context.Background(), changed)
		requireTaskScheduleKind(t, err, ErrTaskScheduleConflict, TaskScheduleErrorConflict)
	}
	desc, ok := fake.snapshot(first.Snapshot.TaskID)
	if !ok {
		t.Fatal("original schedule disappeared")
	}
	var gotFingerprint taskScheduleFingerprint
	action := desc.Schedule.Action.(*client.ScheduleWorkflowAction)
	if err := converter.GetDefaultDataConverter().FromPayload(
		action.Memo[taskScheduleMemoKey].(*commonpb.Payload), &gotFingerprint,
	); err != nil {
		t.Fatal(err)
	}
	if gotFingerprint.PreparedDigest != original.PreparedDigest || gotFingerprint.RequestDigest != first.Snapshot.RequestDigest {
		t.Fatalf("colliding retries mutated original fingerprint: %+v", gotFingerprint)
	}
	if got := fake.counts(); got.unpause != 0 || got.delete != 0 {
		t.Fatalf("collision caused mutating lifecycle calls: %+v", got)
	}
}

func TestEnsurePausedTask_ConcurrentDifferentFingerprintsHaveOneWinner(t *testing.T) {
	fake := newTaskScheduleFakeClient()
	s := newTaskScheduleTestScheduler(fake)
	first := validTaskScheduleRequest()
	second := first
	second.PreparedDigest = strings.Repeat("b", 64)

	type outcome struct {
		preparedDigest string
		result         EnsurePausedTaskResult
		err            error
	}
	start := make(chan struct{})
	outcomes := make(chan outcome, 2)
	var ready sync.WaitGroup
	ready.Add(2)
	for _, req := range []TaskScheduleRequest{first, second} {
		go func() {
			ready.Done()
			<-start
			result, err := s.EnsurePausedTask(context.Background(), req)
			outcomes <- outcome{preparedDigest: req.PreparedDigest, result: result, err: err}
		}()
	}
	ready.Wait()
	close(start)

	var winner string
	created, conflicted := 0, 0
	for range 2 {
		got := <-outcomes
		switch {
		case got.err == nil:
			if got.result.Disposition != TaskScheduleEnsured {
				t.Fatalf("winner disposition=%s, want Ensured", got.result.Disposition)
			}
			created++
			winner = got.preparedDigest
		case errors.Is(got.err, ErrTaskScheduleConflict):
			requireTaskScheduleKind(t, got.err, ErrTaskScheduleConflict, TaskScheduleErrorConflict)
			conflicted++
		default:
			t.Fatalf("unexpected race outcome: %+v", got)
		}
	}
	if created != 1 || conflicted != 1 {
		t.Fatalf("created=%d conflicted=%d", created, conflicted)
	}
	if got := fake.counts(); got.create != 1 || got.unpause != 0 || got.delete != 0 {
		t.Fatalf("calls=%+v", got)
	}
	taskID, _ := TaskIDForOperation(first.TenantID, first.UserID, first.OperationID)
	desc, ok := fake.snapshot(taskID)
	if !ok {
		t.Fatal("winning schedule is missing")
	}
	var fingerprint taskScheduleFingerprint
	action := desc.Schedule.Action.(*client.ScheduleWorkflowAction)
	if err := converter.GetDefaultDataConverter().FromPayload(
		action.Memo[taskScheduleMemoKey].(*commonpb.Payload), &fingerprint,
	); err != nil {
		t.Fatal(err)
	}
	if fingerprint.PreparedDigest != winner {
		t.Fatalf("stored prepared digest=%s, winner=%s", fingerprint.PreparedDigest, winner)
	}
}

func TestTaskScheduleError_AllKindsSupportIsAndAs(t *testing.T) {
	tests := []struct {
		kind     TaskScheduleErrorKind
		sentinel error
		code     types.ErrCode
		retry    bool
	}{
		{TaskScheduleErrorInvalid, ErrTaskScheduleInvalid, types.CodeValidation, false},
		{TaskScheduleErrorNotFound, ErrTaskScheduleNotFound, types.CodeNotFound, false},
		{TaskScheduleErrorConflict, ErrTaskScheduleConflict, types.CodeConflict, false},
		{TaskScheduleErrorUnsafeState, ErrTaskScheduleUnsafeState, types.CodeConflict, false},
		{TaskScheduleErrorBlocked, ErrTaskScheduleBlocked, types.CodeInternal, false},
		{TaskScheduleErrorOutcomeUnknown, ErrTaskScheduleOutcomeUnknown, types.CodeInternal, true},
		{TaskScheduleErrorTransient, ErrTaskScheduleTransient, types.CodeInternal, true},
	}
	for _, tc := range tests {
		t.Run(string(tc.kind), func(t *testing.T) {
			cause := context.DeadlineExceeded
			err := newTaskScheduleError(tc.kind, "test", "task-v1-test", cause)
			typed := requireTaskScheduleKind(t, err, tc.sentinel, tc.kind)
			if typed.Operation != "test" || typed.TaskID != "task-v1-test" || !errors.Is(err, cause) {
				t.Fatalf("typed chain=%+v err=%v", typed, err)
			}
			var appErr *types.AppError
			if !errors.As(err, &appErr) || appErr.Code != tc.code || appErr.Retryable != tc.retry {
				t.Fatalf("AppError=%+v", appErr)
			}
		})
	}
}

// TestTaskSchedulePrimitives_AreOnlyUsedByCreationSaga 是 A5 的接线守卫：
// 所有生产入口必须经 task.CreationCoordinator 驱动创建 Saga，不能绕过它直接
// 调用 Temporal 原语。方法值、函数值和 selector 引用也算接线，不能只盯
// CallExpr；provider 文件内部为了响应丢失收敛而互调不算外部接线。
func TestTaskSchedulePrimitives_AreOnlyUsedByCreationSaga(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller 无法定位测试文件")
	}
	schedulerDir := filepath.Dir(thisFile)
	repoRoot := filepath.Dir(schedulerDir)
	provider := filepath.Join(schedulerDir, "task_schedule.go")
	creationSaga := filepath.Join(repoRoot, "task", "creation_saga.go")
	watched := map[string]struct{}{
		"TaskIDForOperation":  {},
		"PrepareTaskSchedule": {},
		"EnsurePausedTask":    {},
		"DescribeTask":        {},
		"ActivateTask":        {},
		"DeleteTask":          {},
	}

	fset := token.NewFileSet()
	references := make(map[string]struct{})
	err := filepath.WalkDir(repoRoot, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			if entry.Name() == ".git" || entry.Name() == "vendor" {
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") ||
			filepath.Clean(path) == filepath.Clean(provider) ||
			filepath.Clean(path) == filepath.Clean(creationSaga) {
			return nil
		}
		file, parseErr := parser.ParseFile(fset, path, nil, 0)
		if parseErr != nil {
			return parseErr
		}
		ast.Inspect(file, func(node ast.Node) bool {
			identifier, ok := node.(*ast.Ident)
			if ok {
				if _, watched := watched[identifier.Name]; watched {
					position := fset.Position(identifier.Pos())
					references[fmt.Sprintf("%s:%d:%s", position.Filename, position.Line, identifier.Name)] = struct{}{}
				}
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("扫描任务调度原语生产调用点: %v", err)
	}
	if len(references) != 0 {
		t.Fatalf("任务调度原语必须只由创建 Saga 调用，发现旁路引用 %v", references)
	}
}

var (
	_ client.ScheduleClient = (*taskScheduleFakeClient)(nil)
	_ client.ScheduleHandle = (*taskScheduleFakeHandle)(nil)
)
