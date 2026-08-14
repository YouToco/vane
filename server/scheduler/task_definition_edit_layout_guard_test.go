package scheduler

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"testing"

	"go.temporal.io/sdk/converter"

	"github.com/YouToco/vane/server/types"
	"github.com/YouToco/vane/server/workflow"
)

// These mirrors freeze the current structs that definition-edit/v1 embeds.
// If one changes, do not update this test to make it green: introduce a new
// edit wire (and retain the v1 DTO/reader) before evolving the shared type.
type taskDefinitionEditScheduleSpecLayoutV1 struct {
	Cron         string `json:"cron,omitempty"`
	EverySeconds int    `json:"every_seconds,omitempty"`
	AnchorAt     string `json:"anchor_at,omitempty"`
	TZ           string `json:"tz,omitempty"`
}

type taskDefinitionEditPushScopeLayoutV1 struct {
	SourceIDs []int64 `json:"source_ids,omitempty"`
	TopN      int     `json:"top_n,omitempty"`
}

type taskDefinitionEditPushParamsLayoutV1 struct {
	TenantID       int64                    `json:"tenant_id,omitempty"`
	UserID         int64                    `json:"user_id"`
	RunKind        workflow.PushRunKind     `json:"run_kind,omitempty"`
	ExecutionMode  types.ExecutionMode      `json:"execution_mode,omitempty"`
	RuntimeVersion string                   `json:"runtime_version,omitempty"`
	ScheduleID     string                   `json:"schedule_id,omitempty"`
	Scope          workflow.PushScope       `json:"scope"`
	NLDesc         string                   `json:"nl_desc,omitempty"`
	Snapshot       *workflow.RunSnapshotRef `json:"run_snapshot,omitempty"`
}

type taskDefinitionEditPreparedScheduleLayoutV1 struct {
	IDSchemeVersion    string                       `json:"id_scheme_version"`
	FingerprintVersion string                       `json:"fingerprint_version"`
	Namespace          string                       `json:"namespace"`
	NamespaceID        string                       `json:"namespace_id"`
	ConverterID        string                       `json:"converter_id"`
	TaskID             string                       `json:"task_id"`
	TenantID           int64                        `json:"tenant_id"`
	UserID             int64                        `json:"user_id"`
	OperationID        string                       `json:"operation_id"`
	PreparedDigest     string                       `json:"prepared_digest"`
	RequestDigest      string                       `json:"request_digest"`
	Timing             PreparedTaskScheduleTiming   `json:"timing"`
	Action             PreparedTaskScheduleAction   `json:"action"`
	Policy             PreparedTaskSchedulePolicy   `json:"policy"`
	Creation           PreparedTaskScheduleCreation `json:"creation"`
}

type taskDefinitionEditPreparedTimingLayoutV1 struct {
	Calendar     *PreparedTaskScheduleCalendar `json:"calendar,omitempty"`
	EveryNanos   int64                         `json:"every_nanos,omitempty"`
	OffsetNanos  int64                         `json:"offset_nanos,omitempty"`
	TimeZoneName string                        `json:"time_zone_name"`
}

type taskDefinitionEditPreparedCalendarLayoutV1 struct {
	Second     uint64 `json:"second"`
	Minute     uint64 `json:"minute"`
	Hour       uint64 `json:"hour"`
	DayOfMonth uint64 `json:"day_of_month"`
	Month      uint64 `json:"month"`
	DayOfWeek  uint64 `json:"day_of_week"`
}

type taskDefinitionEditPreparedActionLayoutV1 struct {
	Params                        workflow.PushParams `json:"params"`
	TaskQueue                     string              `json:"task_queue"`
	WorkflowType                  string              `json:"workflow_type"`
	ActionID                      string              `json:"action_id"`
	WorkflowExecutionTimeoutNanos int64               `json:"workflow_execution_timeout_nanos"`
	WorkflowRunTimeoutNanos       int64               `json:"workflow_run_timeout_nanos"`
	WorkflowTaskTimeoutNanos      int64               `json:"workflow_task_timeout_nanos"`
	HasRetryPolicy                bool                `json:"has_retry_policy"`
	ActivationNote                string              `json:"activation_note"`
}

type taskDefinitionEditPreparedPolicyLayoutV1 struct {
	Overlap        int32 `json:"overlap"`
	CatchupNanos   int64 `json:"catchup_nanos"`
	PauseOnFailure bool  `json:"pause_on_failure"`
}

type taskDefinitionEditPreparedCreationLayoutV1 struct {
	Paused             bool   `json:"paused"`
	RemainingActions   int    `json:"remaining_actions"`
	TriggerImmediately bool   `json:"trigger_immediately"`
	BackfillCount      int    `json:"backfill_count"`
	Note               string `json:"note"`
}

type taskDefinitionEditDefinitionLayoutV1 struct {
	Spec          ScheduleSpec       `json:"spec"`
	Scope         workflow.PushScope `json:"scope"`
	NLDescription string             `json:"nl_description"`
}

type taskDefinitionEditHeadLayoutV1 struct {
	Version int64  `json:"version"`
	Digest  string `json:"digest"`
}

type taskDefinitionEditFingerprintLayoutV1 struct {
	IDSchemeVersion        string `json:"id_scheme_version"`
	FingerprintVersion     string `json:"fingerprint_version"`
	Namespace              string `json:"namespace"`
	NamespaceID            string `json:"namespace_id"`
	ConverterID            string `json:"converter_id"`
	TenantID               int64  `json:"tenant_id"`
	UserID                 int64  `json:"user_id"`
	TaskID                 string `json:"task_id"`
	CreationOperationID    string `json:"operation_id"`
	CreationPreparedDigest string `json:"prepared_digest"`
	CreationRequestDigest  string `json:"request_digest"`
	LifecyclePhase         string `json:"lifecycle_phase"`
	DefinitionVersion      int64  `json:"definition_version,omitempty"`
	DefinitionDigest       string `json:"definition_digest,omitempty"`
	EditOperationDigest    string `json:"edit_operation_digest,omitempty"`
	EditPhase              string `json:"edit_phase,omitempty"`
}

type taskDefinitionEditScheduleStateLayoutV1 struct {
	Paused           bool   `json:"paused"`
	Note             string `json:"note"`
	LimitedActions   bool   `json:"limited_actions"`
	RemainingActions int64  `json:"remaining_actions"`
}

type taskDefinitionEditPreparedRepresentationLayoutV1 struct {
	Digest                string                          `json:"digest"`
	Timing                PreparedTaskScheduleTiming      `json:"timing"`
	Action                PreparedTaskScheduleAction      `json:"action"`
	Policy                PreparedTaskSchedulePolicy      `json:"policy"`
	WorkflowIDReusePolicy int32                           `json:"workflow_id_reuse_policy"`
	State                 TaskDefinitionEditScheduleState `json:"state"`
	Fingerprint           TaskDefinitionEditFingerprint   `json:"fingerprint"`
}

type taskDefinitionEditPreparedLayoutV1 struct {
	WireVersion            string                             `json:"wire_version"`
	OperationID            string                             `json:"operation_id"`
	OperationDigest        string                             `json:"operation_digest"`
	RequestDigest          string                             `json:"request_digest"`
	BaseProjectionDigest   string                             `json:"base_projection_digest"`
	TargetProjectionDigest string                             `json:"target_projection_digest"`
	Creation               PreparedTaskSchedule               `json:"creation"`
	BaseHead               TaskDefinitionEditHead             `json:"base_head"`
	TargetHead             TaskDefinitionEditHead             `json:"target_head"`
	OriginalState          TaskDefinitionEditOriginalState    `json:"original_state"`
	BaseRevision           string                             `json:"base_revision"`
	BaseOriginal           PreparedTaskDefinitionEditSchedule `json:"base_original"`
	BasePaused             PreparedTaskDefinitionEditSchedule `json:"base_paused"`
	TargetPaused           PreparedTaskDefinitionEditSchedule `json:"target_paused"`
	TargetFinal            PreparedTaskDefinitionEditSchedule `json:"target_final"`
}

type taskDefinitionEditSnapshotLayoutV1 struct {
	TaskID               string                  `json:"task_id"`
	RequestDigest        string                  `json:"request_digest"`
	Phase                TaskDefinitionEditPhase `json:"phase"`
	RepresentationDigest string                  `json:"representation_digest"`
	Revision             string                  `json:"revision"`
}

type taskDefinitionEditOperationSeedLayoutV1 struct {
	WireVersion            string                          `json:"wire_version"`
	OperationID            string                          `json:"operation_id"`
	CreationRequestDigest  string                          `json:"creation_request_digest"`
	TenantID               int64                           `json:"tenant_id"`
	UserID                 int64                           `json:"user_id"`
	TaskID                 string                          `json:"task_id"`
	BaseHead               TaskDefinitionEditHead          `json:"base_head"`
	TargetHead             TaskDefinitionEditHead          `json:"target_head"`
	OriginalState          TaskDefinitionEditOriginalState `json:"original_state"`
	BaseProjectionDigest   string                          `json:"base_projection_digest"`
	TargetProjectionDigest string                          `json:"target_projection_digest"`
	BaseTiming             PreparedTaskScheduleTiming      `json:"base_timing"`
	BaseAction             PreparedTaskScheduleAction      `json:"base_action"`
	BasePolicy             PreparedTaskSchedulePolicy      `json:"base_policy"`
	BaseReusePolicy        int32                           `json:"base_reuse_policy"`
	BaseState              TaskDefinitionEditScheduleState `json:"base_state"`
	TargetTiming           PreparedTaskScheduleTiming      `json:"target_timing"`
	TargetAction           PreparedTaskScheduleAction      `json:"target_action"`
	TargetPolicy           PreparedTaskSchedulePolicy      `json:"target_policy"`
	TargetReusePolicy      int32                           `json:"target_reuse_policy"`
}

type taskDefinitionEditProjectionLayoutV1 struct {
	Spec          ScheduleSpec       `json:"spec"`
	Scope         workflow.PushScope `json:"scope"`
	NLDescription string             `json:"nl_description"`
}

func TestTaskDefinitionEditV1SharedLayoutsAreFrozen(t *testing.T) {
	t.Parallel()
	assertTaskDefinitionEditLayoutV1[
		ScheduleSpec, taskDefinitionEditScheduleSpecLayoutV1,
	](t, "ScheduleSpec")
	assertTaskDefinitionEditLayoutV1[
		workflow.PushScope, taskDefinitionEditPushScopeLayoutV1,
	](t, "workflow.PushScope")
	assertTaskDefinitionEditLayoutV1[
		workflow.PushParams, taskDefinitionEditPushParamsLayoutV1,
	](t, "workflow.PushParams")
	assertTaskDefinitionEditLayoutV1[
		PreparedTaskSchedule, taskDefinitionEditPreparedScheduleLayoutV1,
	](t, "PreparedTaskSchedule")
	assertTaskDefinitionEditLayoutV1[
		PreparedTaskScheduleTiming, taskDefinitionEditPreparedTimingLayoutV1,
	](t, "PreparedTaskScheduleTiming")
	assertTaskDefinitionEditLayoutV1[
		PreparedTaskScheduleCalendar, taskDefinitionEditPreparedCalendarLayoutV1,
	](t, "PreparedTaskScheduleCalendar")
	assertTaskDefinitionEditLayoutV1[
		PreparedTaskScheduleAction, taskDefinitionEditPreparedActionLayoutV1,
	](t, "PreparedTaskScheduleAction")
	assertTaskDefinitionEditLayoutV1[
		PreparedTaskSchedulePolicy, taskDefinitionEditPreparedPolicyLayoutV1,
	](t, "PreparedTaskSchedulePolicy")
	assertTaskDefinitionEditLayoutV1[
		PreparedTaskScheduleCreation, taskDefinitionEditPreparedCreationLayoutV1,
	](t, "PreparedTaskScheduleCreation")
	assertTaskDefinitionEditLayoutV1[
		TaskDefinitionEditDefinition, taskDefinitionEditDefinitionLayoutV1,
	](t, "TaskDefinitionEditDefinition")
	assertTaskDefinitionEditLayoutV1[
		TaskDefinitionEditHead, taskDefinitionEditHeadLayoutV1,
	](t, "TaskDefinitionEditHead")
	assertTaskDefinitionEditLayoutV1[
		TaskDefinitionEditFingerprint, taskDefinitionEditFingerprintLayoutV1,
	](t, "TaskDefinitionEditFingerprint")
	assertTaskDefinitionEditLayoutV1[
		TaskDefinitionEditScheduleState, taskDefinitionEditScheduleStateLayoutV1,
	](t, "TaskDefinitionEditScheduleState")
	assertTaskDefinitionEditLayoutV1[
		PreparedTaskDefinitionEditSchedule, taskDefinitionEditPreparedRepresentationLayoutV1,
	](t, "PreparedTaskDefinitionEditSchedule")
	assertTaskDefinitionEditLayoutV1[
		PreparedTaskDefinitionEdit, taskDefinitionEditPreparedLayoutV1,
	](t, "PreparedTaskDefinitionEdit")
	assertTaskDefinitionEditLayoutV1[
		TaskDefinitionEditSnapshot, taskDefinitionEditSnapshotLayoutV1,
	](t, "TaskDefinitionEditSnapshot")
	assertTaskDefinitionEditLayoutV1[
		taskDefinitionEditOperationSeed, taskDefinitionEditOperationSeedLayoutV1,
	](t, "taskDefinitionEditOperationSeed")
	assertTaskDefinitionEditLayoutV1[
		taskDefinitionEditProjectionV1, taskDefinitionEditProjectionLayoutV1,
	](t, "taskDefinitionEditProjectionV1")
}

func TestTaskScheduleDefaultConverterV1PayloadIsFrozen(t *testing.T) {
	t.Parallel()
	params := workflow.PushParams{
		TenantID: 7, UserID: 42, RunKind: workflow.PushRunKindScheduled,
		ExecutionMode:  types.ExecutionModeCompiled,
		RuntimeVersion: workflow.CompiledRuntimeSnapshotV1,
		ScheduleID:     "task-v1-fixture",
		Scope:          workflow.PushScope{SourceIDs: []int64{11, 22}, TopN: 3},
		NLDesc:         "daily intelligence",
	}
	payload, err := converter.GetDefaultDataConverter().ToPayload(params)
	if err != nil {
		t.Fatalf("encode default converter fixture: %v", err)
	}
	want := []byte(`{"tenant_id":7,"user_id":42,"run_kind":"scheduled","execution_mode":"compiled","runtime_version":"compiled-snapshot/v1","schedule_id":"task-v1-fixture","scope":{"source_ids":[11,22],"top_n":3},"nl_desc":"daily intelligence"}`)
	if len(payload.GetMetadata()) != 1 ||
		!bytes.Equal(payload.GetMetadata()["encoding"], []byte("json/plain")) ||
		!bytes.Equal(payload.GetData(), want) {
		t.Fatalf(
			"%s drifted with the Temporal SDK; bump the converter ID and retain the old decoder before upgrading: metadata=%q data=%s",
			taskScheduleDefaultConverterID, payload.GetMetadata(), payload.GetData(),
		)
	}
	var decoded workflow.PushParams
	if err := converter.GetDefaultDataConverter().FromPayload(payload, &decoded); err != nil ||
		!reflect.DeepEqual(decoded, params) {
		t.Fatalf("default converter v1 round trip = %+v, %v", decoded, err)
	}
}

func TestTaskScheduleDefaultConverterIDCannotBeRebound(t *testing.T) {
	fake := newTaskScheduleFakeClient()
	s := New(
		&taskScheduleTemporalClient{schedules: fake}, "vane-task-test", nil,
		WithTaskScheduleNamespace("test-namespace"),
		WithTaskScheduleDataConverter(
			taskScheduleDefaultConverterID, converter.GetDefaultDataConverter(),
		),
	)
	s.taskScheduleEnv.namespaceIDOverride = "test-namespace-id"
	if _, err := s.PrepareTaskSchedule(context.Background(), validTaskScheduleRequest()); !errors.Is(err, ErrTaskScheduleInvalid) {
		t.Fatalf("reserved default converter ID was rebound: %v", err)
	}
}

func assertTaskDefinitionEditLayoutV1[Actual, Frozen any](t *testing.T, name string) {
	t.Helper()
	actual := reflect.TypeFor[Actual]()
	frozen := reflect.TypeFor[Frozen]()
	if actual.Kind() != reflect.Struct || frozen.Kind() != reflect.Struct {
		t.Fatalf("%s layout guard requires structs", name)
	}
	if actual.NumField() != frozen.NumField() {
		t.Fatalf(
			"%s changed from definition-edit/v1 (%d fields, want %d); add a new wire and retain v1 instead of updating this guard",
			name, actual.NumField(), frozen.NumField(),
		)
	}
	for index := range actual.NumField() {
		got := actual.Field(index)
		want := frozen.Field(index)
		if got.Name != want.Name || got.Type != want.Type ||
			got.Tag.Get("json") != want.Tag.Get("json") || got.Anonymous != want.Anonymous {
			t.Fatalf(
				"%s field %d changed from definition-edit/v1: got %s %s %q, want %s %s %q; add a new wire and retain v1",
				name, index, got.Name, got.Type, got.Tag.Get("json"),
				want.Name, want.Type, want.Tag.Get("json"),
			)
		}
	}
}
