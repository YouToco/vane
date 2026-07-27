package scheduler

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/robfig/cron"
	commonpb "go.temporal.io/api/common/v1"
	enums "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/YouToco/vane/types"
	"github.com/YouToco/vane/workflow"
)

const (
	taskScheduleIDSchemeVersionV1 = "v1"
	// taskScheduleIDSchemeVersion is the current writer alias. Retained
	// readers must compare against explicit version constants so a future ID
	// scheme cannot strand existing ownership checkpoints.
	taskScheduleIDSchemeVersion      = taskScheduleIDSchemeVersionV1
	taskScheduleFingerprintVersionV1 = "v1"
	taskScheduleFingerprintVersionV2 = "v2"
	// taskScheduleFingerprintVersion is the current writer alias. Retained
	// readers must compare against explicit V1/V2 constants so advancing this
	// alias cannot strand already checkpointed schedules or definition edits.
	taskScheduleFingerprintVersion  = taskScheduleFingerprintVersionV2
	taskScheduleMemoKey             = "vane.task-schedule"
	taskScheduleDefaultConverterID  = "temporal-default-json-v1"
	taskScheduleV1WorkflowType      = "PushPipelineWorkflow"
	taskScheduleV1CatchupWindow     = time.Minute
	taskScheduleV1WorkflowTaskTime  = 10 * time.Second
	taskScheduleV1ProvisioningNote  = "vane/task-schedule/v1:provisioning-paused"
	taskScheduleV1ActivationNote    = "vane/task-schedule/v1:definition-committed"
	taskScheduleV1PhaseProvisioning = "provisioning"
	taskScheduleV1PhaseActive       = "active"
	taskScheduleRecoveryTimeout     = 5 * time.Second
	maxTaskOperationIDBytesV1       = 512
	// maxTaskOperationIDBytes is the current writer alias. Retained v1 readers
	// use maxTaskOperationIDBytesV1 directly.
	maxTaskOperationIDBytes = maxTaskOperationIDBytesV1
)

// SchedulerOption configures the A3 task-schedule control plane without
// changing the legacy Scheduler behavior. Existing CreatePush callers do not
// need an option; PrepareTaskSchedule requires an explicit namespace.
type SchedulerOption func(*Scheduler)

// WithTaskScheduleNamespace binds prepared definitions to one Temporal
// namespace. PrepareTaskSchedule also records the namespace's stable server ID,
// so recreating the same name on another cluster fails closed.
func WithTaskScheduleNamespace(namespace string) SchedulerOption {
	return func(s *Scheduler) {
		s.taskScheduleEnv.namespace = namespace
	}
}

// WithTaskScheduleDataConverter installs one stable, versioned, local converter
// from the application's decoder registry. It must not depend on request
// context or remote services: A4 must be able to decode an old checkpoint after
// a process restart. Serialization-context-aware converters are supported.
func WithTaskScheduleDataConverter(id string, dc converter.DataConverter) SchedulerOption {
	return func(s *Scheduler) {
		s.taskScheduleEnv.converterID = id
		s.taskScheduleEnv.dc = dc
		if s.taskScheduleEnv.decoders == nil {
			s.taskScheduleEnv.decoders = make(map[string]converter.DataConverter)
		}
		s.taskScheduleEnv.decoders[id] = dc
	}
}

// WithTaskScheduleDecoder retains a previous versioned decoder for Describe
// and Delete after the current execution converter rotates. Ensure and Activate
// still fail closed unless the Prepared converter is the current worker format.
func WithTaskScheduleDecoder(id string, dc converter.DataConverter) SchedulerOption {
	return func(s *Scheduler) {
		if s.taskScheduleEnv.decoders == nil {
			s.taskScheduleEnv.decoders = make(map[string]converter.DataConverter)
		}
		s.taskScheduleEnv.decoders[id] = dc
	}
}

type taskScheduleEnvironment struct {
	mu        sync.Mutex
	namespace string
	// namespaceIDOverride is only for hermetic tests. Production deliberately
	// resolves the ID on every operation so deleting and recreating a namespace
	// with the same name cannot reuse a stale in-process cache.
	namespaceIDOverride string
	converterID         string
	dc                  converter.DataConverter
	decoders            map[string]converter.DataConverter
}

type taskScheduleRequestContextAwareConverter interface {
	WithContext(context.Context) converter.DataConverter
}

// TaskScheduleRequest is the immutable Temporal part of a prepared task.
// PreparedDigest is the lowercase SHA-256 digest of the definition frozen by
// the creation saga. A3 deliberately does not know or persist that definition.
type TaskScheduleRequest struct {
	TenantID       int64
	UserID         int64
	OperationID    string
	Spec           ScheduleSpec
	Scope          workflow.PushScope
	NLDescription  string
	PreparedDigest string
}

// PreparedTaskSchedule is the immutable, JSON-serializable result that A4 must
// checkpoint before issuing any Temporal mutation. Recovery never re-translates
// the user's request or substitutes the process's current task queue, time zone,
// workflow name, policy, or converter.
//
// OperationID is a generation: A4 must allocate it once, never reuse it for a
// different definition, and retain a tombstone after deletion. That durable
// invariant closes Temporal's unavoidable cross-process Describe-then-write
// race, because a deleted TaskID can never be replaced by a different owner.
type PreparedTaskSchedule struct {
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

// PreparedTaskScheduleTiming is a server-normalization-independent schedule
// specification. Exactly one of Calendar and EveryNanos is populated. A named
// time zone intentionally means "current civil-time rules for this IANA name";
// Temporal's tzdata may evolve without changing this logical definition.
type PreparedTaskScheduleTiming struct {
	Calendar     *PreparedTaskScheduleCalendar `json:"calendar,omitempty"`
	EveryNanos   int64                         `json:"every_nanos,omitempty"`
	OffsetNanos  int64                         `json:"offset_nanos,omitempty"`
	TimeZoneName string                        `json:"time_zone_name"`
}

// PreparedTaskScheduleCalendar stores semantic bit sets, not Temporal range
// formatting, so a real server may normalize ranges without changing identity.
type PreparedTaskScheduleCalendar struct {
	Second     uint64 `json:"second"`
	Minute     uint64 `json:"minute"`
	Hour       uint64 `json:"hour"`
	DayOfMonth uint64 `json:"day_of_month"`
	Month      uint64 `json:"month"`
	DayOfWeek  uint64 `json:"day_of_week"`
}

type PreparedTaskScheduleAction struct {
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

type PreparedTaskSchedulePolicy struct {
	Overlap        int32 `json:"overlap"`
	CatchupNanos   int64 `json:"catchup_nanos"`
	PauseOnFailure bool  `json:"pause_on_failure"`
}

type PreparedTaskScheduleCreation struct {
	Paused             bool   `json:"paused"`
	RemainingActions   int    `json:"remaining_actions"`
	TriggerImmediately bool   `json:"trigger_immediately"`
	BackfillCount      int    `json:"backfill_count"`
	Note               string `json:"note"`
}

// TaskScheduleDisposition reports that CreateSchedule either created the
// object or replayed the original deterministic RequestID. Temporal does not
// expose which caller uniquely created it, so A3 deliberately makes no such
// claim.
type TaskScheduleDisposition string

const (
	TaskScheduleEnsured TaskScheduleDisposition = "ensured"
)

// TaskScheduleState is derived from Temporal state, execution counters, and
// Vane's durable lifecycle phase. Paused alone is not a provisioning state: a
// schedule can be paused after it has already run or was activated by Vane.
// A4 must also preserve Snapshot.Revision across its database transaction;
// ActivateTask uses it to reject any intervening out-of-band mutation.
type TaskScheduleState string

const (
	TaskScheduleStateUnknown TaskScheduleState = "unknown"
	// PausedProvisioningExact describes only the current representation: Vane's
	// provisioning phase, paused, exact definition, and no recorded action. A
	// raw Describe cannot prove that no prior patch was visually replayed.
	TaskSchedulePausedProvisioningExact TaskScheduleState = "paused_provisioning_exact"
	// PausedVirginExact is returned only by EnsurePausedTask after Temporal's
	// immutable Create-request token matches the current revision.
	TaskSchedulePausedVirginExact TaskScheduleState = "paused_virgin_exact"
	TaskSchedulePausedUsedExact   TaskScheduleState = "paused_used_exact"
	TaskScheduleActiveVirginExact TaskScheduleState = "active_virgin_exact"
	TaskScheduleActiveUsedExact   TaskScheduleState = "active_used_exact"
)

// TaskScheduleSnapshot is the verified receipt returned to the future A4 saga.
// Revision is an opaque, cloned Temporal conflict token encoded for durable
// JSON checkpointing; callers must compare it only through ActivateTask and
// must not interpret its bytes.
type TaskScheduleSnapshot struct {
	TaskID         string
	RequestDigest  string
	PreparedDigest string
	Revision       string
	State          TaskScheduleState
	NumActions     int
}

// EnsurePausedTaskResult is stable input for the A4 checkpoint decision.
type EnsurePausedTaskResult struct {
	Disposition TaskScheduleDisposition
	Snapshot    TaskScheduleSnapshot
}

// TaskScheduleErrorKind tells the A4 saga whether it may retry, must isolate a
// collision, or needs a later reconciliation pass because the outcome is not
// currently provable.
type TaskScheduleErrorKind string

const (
	TaskScheduleErrorInvalid        TaskScheduleErrorKind = "invalid"
	TaskScheduleErrorNotFound       TaskScheduleErrorKind = "not_found"
	TaskScheduleErrorConflict       TaskScheduleErrorKind = "conflict"
	TaskScheduleErrorBlocked        TaskScheduleErrorKind = "blocked"
	TaskScheduleErrorUnsafeState    TaskScheduleErrorKind = "unsafe_state"
	TaskScheduleErrorOutcomeUnknown TaskScheduleErrorKind = "outcome_unknown"
	TaskScheduleErrorTransient      TaskScheduleErrorKind = "transient"
)

var (
	ErrTaskScheduleInvalid        = errors.New("scheduler: invalid task schedule request")
	ErrTaskScheduleNotFound       = errors.New("scheduler: task schedule not found")
	ErrTaskScheduleConflict       = errors.New("scheduler: task schedule conflict")
	ErrTaskScheduleBlocked        = errors.New("scheduler: task schedule operation blocked")
	ErrTaskScheduleUnsafeState    = errors.New("scheduler: unsafe task schedule state")
	ErrTaskScheduleOutcomeUnknown = errors.New("scheduler: task schedule outcome unknown")
	ErrTaskScheduleTransient      = errors.New("scheduler: transient task schedule failure")
)

// TaskScheduleError preserves both a stable kind and the original Temporal or
// context error. Callers can therefore use errors.Is on the sentinels above and
// errors.As for serviceerror types without parsing text.
type TaskScheduleError struct {
	Kind      TaskScheduleErrorKind
	Operation string
	TaskID    string
	Cause     error
}

func (e *TaskScheduleError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("task schedule %s %s (%s): %v", e.TaskID, e.Operation, e.Kind, e.Cause)
	}
	return fmt.Sprintf("task schedule %s %s (%s)", e.TaskID, e.Operation, e.Kind)
}

func (e *TaskScheduleError) Unwrap() error { return e.Cause }

func (e *TaskScheduleError) Is(target error) bool {
	return target == taskScheduleKindSentinel(e.Kind)
}

func taskScheduleKindSentinel(kind TaskScheduleErrorKind) error {
	switch kind {
	case TaskScheduleErrorInvalid:
		return ErrTaskScheduleInvalid
	case TaskScheduleErrorNotFound:
		return ErrTaskScheduleNotFound
	case TaskScheduleErrorConflict:
		return ErrTaskScheduleConflict
	case TaskScheduleErrorBlocked:
		return ErrTaskScheduleBlocked
	case TaskScheduleErrorUnsafeState:
		return ErrTaskScheduleUnsafeState
	case TaskScheduleErrorOutcomeUnknown:
		return ErrTaskScheduleOutcomeUnknown
	case TaskScheduleErrorTransient:
		return ErrTaskScheduleTransient
	default:
		return nil
	}
}

// TaskIDForOperation deterministically assigns one Temporal schedule ID to one
// tenant/user operation. Mutable definition fields are intentionally excluded:
// the same operation with a changed definition must collide and be isolated.
func TaskIDForOperation(tenantID, userID int64, operationID string) (string, error) {
	return taskIDForOperationVersion(taskScheduleIDSchemeVersion, tenantID, userID, operationID)
}

func taskIDForOperationVersion(version string, tenantID, userID int64, operationID string) (string, error) {
	if version != taskScheduleIDSchemeVersionV1 {
		return "", newTaskScheduleError(
			TaskScheduleErrorInvalid, "derive_id", "",
			fmt.Errorf("unsupported task ID scheme version %q", version),
		)
	}
	if tenantID <= 0 || userID <= 0 {
		return "", newTaskScheduleError(
			TaskScheduleErrorInvalid, "derive_id", "",
			errors.New("tenant_id and user_id must be positive"),
		)
	}
	if operationID == "" || strings.TrimSpace(operationID) != operationID {
		return "", newTaskScheduleError(
			TaskScheduleErrorInvalid, "derive_id", "",
			errors.New("operation_id must be non-empty and have no surrounding whitespace"),
		)
	}
	if !utf8.ValidString(operationID) {
		return "", newTaskScheduleError(
			TaskScheduleErrorInvalid, "derive_id", "",
			errors.New("operation_id must be valid UTF-8"),
		)
	}
	if len(operationID) > maxTaskOperationIDBytesV1 {
		return "", newTaskScheduleError(
			TaskScheduleErrorInvalid, "derive_id", "",
			fmt.Errorf("operation_id exceeds %d bytes", maxTaskOperationIDBytesV1),
		)
	}

	identity := struct {
		Version     string `json:"version"`
		TenantID    int64  `json:"tenant_id"`
		UserID      int64  `json:"user_id"`
		OperationID string `json:"operation_id"`
	}{version, tenantID, userID, operationID}
	b, err := json.Marshal(identity)
	if err != nil {
		return "", newTaskScheduleError(TaskScheduleErrorInvalid, "derive_id", "", err)
	}
	sum := sha256.Sum256(b)
	return "task-" + version + "-" + hex.EncodeToString(sum[:]), nil
}

// EnsurePausedTask creates a schedule paused or replays the original Create by
// deterministic RequestID. Temporal returns the original creation conflict
// token for that replay; comparing it with the current Describe token proves
// that the object has not changed since creation. Every result is
// Describe-checked, including when Create returned nil.
func (s *Scheduler) EnsurePausedTask(
	ctx context.Context,
	prepared PreparedTaskSchedule,
) (EnsurePausedTaskResult, error) {
	if err := taskScheduleContextError(ctx, "ensure_paused", ""); err != nil {
		return EnsurePausedTaskResult{}, err
	}
	expected, err := s.buildTaskScheduleExpected(ctx, prepared, "ensure_paused", true)
	if err != nil {
		return EnsurePausedTaskResult{}, err
	}
	release, err := s.acquireTaskScheduleGate(ctx, "ensure_paused", expected.taskID)
	if err != nil {
		return EnsurePausedTaskResult{}, err
	}
	defer release()
	if err := taskScheduleContextError(ctx, "ensure_paused", expected.taskID); err != nil {
		return EnsurePausedTaskResult{}, err
	}

	// A preflight rejects observable collisions without issuing a write RPC.
	// An exact provisioning object must still continue through CreateSchedule:
	// only deterministic RequestID replay can recover its immutable creation
	// token and prove that the object was not changed and visually replayed.
	existing, existingErr := s.describeTaskSchedule(ctx, expected)
	if existingErr == nil {
		snapshot, err := verifyTaskScheduleDescription(expected, existing, "ensure_paused")
		if err != nil {
			return EnsurePausedTaskResult{}, err
		}
		if snapshot.State != TaskSchedulePausedProvisioningExact {
			return EnsurePausedTaskResult{}, newTaskScheduleError(
				TaskScheduleErrorUnsafeState, "ensure_paused", expected.taskID,
				fmt.Errorf("expected %s, got %s", TaskSchedulePausedProvisioningExact, snapshot.State),
			)
		}
	} else if !isTaskScheduleNotFound(existingErr) {
		return EnsurePausedTaskResult{}, classifyTaskScheduleReadError(
			"ensure_paused", expected.taskID, existingErr,
		)
	}

	createRequest, err := expected.createRequest()
	if err != nil {
		return EnsurePausedTaskResult{}, newTaskScheduleError(
			TaskScheduleErrorBlocked, "ensure_paused", expected.taskID, err,
		)
	}
	createResponse, createErr := s.c.WorkflowService().CreateSchedule(ctx, createRequest)
	if createErr != nil && !taskScheduleMutationDefinitelyRejected(createErr) &&
		!isTaskScheduleAlreadyExistsError(createErr) {
		// The first response may have been lost after commit. Replaying the
		// exact deterministic request is part of converging that one mutation,
		// not permission to advance the saga after caller cancellation.
		replayedResponse, replayErr := s.createTaskScheduleForRecovery(ctx, createRequest)
		if replayErr == nil {
			createResponse = replayedResponse
			createErr = nil
		} else {
			createErr = errors.Join(createErr, replayErr)
		}
	}
	desc, describeErr := s.describeTaskScheduleForRecovery(ctx, expected)
	if describeErr != nil {
		if isTaskScheduleNotFound(describeErr) {
			if isTaskScheduleAlreadyExistsError(createErr) {
				return EnsurePausedTaskResult{}, newTaskScheduleError(
					TaskScheduleErrorTransient, "ensure_paused", expected.taskID,
					errors.Join(createErr, describeErr),
				)
			}
			cause := createErr
			if cause == nil {
				cause = describeErr
			}
			return EnsurePausedTaskResult{}, classifyTaskScheduleMutationError(
				"ensure_paused", expected.taskID, cause,
			)
		}
		if isTaskScheduleAlreadyExistsError(createErr) {
			return EnsurePausedTaskResult{}, newTaskScheduleError(
				TaskScheduleErrorOutcomeUnknown, "ensure_paused", expected.taskID,
				errors.Join(createErr, describeErr),
			)
		}
		if taskScheduleMutationDefinitelyRejected(createErr) {
			return EnsurePausedTaskResult{}, classifyTaskScheduleMutationError(
				"ensure_paused", expected.taskID, createErr,
			)
		}
		return EnsurePausedTaskResult{}, newTaskScheduleError(
			TaskScheduleErrorOutcomeUnknown, "ensure_paused", expected.taskID,
			errors.Join(createErr, describeErr),
		)
	}

	snapshot, err := verifyTaskScheduleDescription(expected, desc, "ensure_paused")
	if err != nil {
		return EnsurePausedTaskResult{}, err
	}
	if snapshot.State != TaskSchedulePausedProvisioningExact {
		return EnsurePausedTaskResult{}, newTaskScheduleError(TaskScheduleErrorUnsafeState, "ensure_paused", expected.taskID,
			fmt.Errorf("expected %s, got %s", TaskSchedulePausedProvisioningExact, snapshot.State))
	}
	if createErr != nil {
		if !taskScheduleMutationDefinitelyRejected(createErr) &&
			!isTaskScheduleAlreadyExistsError(createErr) {
			return EnsurePausedTaskResult{}, newTaskScheduleError(
				TaskScheduleErrorOutcomeUnknown, "ensure_paused", expected.taskID, createErr,
			)
		}
		return EnsurePausedTaskResult{}, classifyTaskScheduleMutationError(
			"ensure_paused", expected.taskID, createErr,
		)
	}
	if createResponse == nil || len(createResponse.GetConflictToken()) == 0 {
		return EnsurePausedTaskResult{}, newTaskScheduleError(
			TaskScheduleErrorOutcomeUnknown, "ensure_paused", expected.taskID,
			errors.New("CreateSchedule returned no creation conflict token"),
		)
	}
	if snapshot.Revision != taskScheduleRevision(createResponse.GetConflictToken()) {
		return EnsurePausedTaskResult{}, newTaskScheduleError(
			TaskScheduleErrorUnsafeState, "ensure_paused", expected.taskID,
			errors.New("schedule changed after its original CreateSchedule request"),
		)
	}
	snapshot.State = TaskSchedulePausedVirginExact
	return EnsurePausedTaskResult{Disposition: TaskScheduleEnsured, Snapshot: snapshot}, nil
}

// DescribeTask verifies ownership and the full execution configuration before
// returning state. A handle alone does not prove existence: GetHandle makes no
// RPC, so this method always calls Describe.
func (s *Scheduler) DescribeTask(
	ctx context.Context,
	prepared PreparedTaskSchedule,
) (TaskScheduleSnapshot, error) {
	if err := taskScheduleContextError(ctx, "describe", ""); err != nil {
		return TaskScheduleSnapshot{}, err
	}
	expected, err := s.buildTaskScheduleExpected(ctx, prepared, "describe", false)
	if err != nil {
		return TaskScheduleSnapshot{}, err
	}
	desc, err := s.describeTaskSchedule(ctx, expected)
	if err != nil {
		return TaskScheduleSnapshot{}, classifyTaskScheduleReadError("describe", expected.taskID, err)
	}
	return verifyTaskScheduleDescription(expected, desc, "describe")
}

// ActivateTask idempotently activates an exact task schedule. ensured is the
// immutable snapshot returned by EnsurePausedTask before the caller commits
// InsertPausedCompiledTaskDefinition. Its revision closes the otherwise
// invisible out-of-band Unpause-then-Pause window across that database step.
// A3 cannot observe or prove the database precondition itself; A4 supplies the
// durable lease, fence, and checkpoint and must make A3 the exclusive lifecycle
// writer for this TaskID.
func (s *Scheduler) ActivateTask(
	ctx context.Context,
	prepared PreparedTaskSchedule,
	ensured TaskScheduleSnapshot,
) (TaskScheduleSnapshot, error) {
	if err := taskScheduleContextError(ctx, "activate", ""); err != nil {
		return TaskScheduleSnapshot{}, err
	}
	expected, err := s.buildTaskScheduleExpected(ctx, prepared, "activate", true)
	if err != nil {
		return TaskScheduleSnapshot{}, err
	}
	if err := validateTaskScheduleActivationReceipt(expected, ensured); err != nil {
		return TaskScheduleSnapshot{}, newTaskScheduleError(
			TaskScheduleErrorInvalid, "activate", expected.taskID, err,
		)
	}
	release, err := s.acquireTaskScheduleGate(ctx, "activate", expected.taskID)
	if err != nil {
		return TaskScheduleSnapshot{}, err
	}
	defer release()
	if err := taskScheduleContextError(ctx, "activate", expected.taskID); err != nil {
		return TaskScheduleSnapshot{}, err
	}

	desc, err := s.describeTaskSchedule(ctx, expected)
	if err != nil {
		return TaskScheduleSnapshot{}, classifyTaskScheduleReadError("activate", expected.taskID, err)
	}
	snapshot, err := verifyTaskScheduleDescription(expected, desc, "activate")
	if err != nil {
		return TaskScheduleSnapshot{}, err
	}
	switch snapshot.State {
	case TaskScheduleActiveVirginExact, TaskScheduleActiveUsedExact:
		return snapshot, nil
	case TaskSchedulePausedUsedExact:
		return TaskScheduleSnapshot{}, newTaskScheduleError(TaskScheduleErrorUnsafeState, "activate", expected.taskID,
			fmt.Errorf("used schedule cannot be activated from provisioning state"))
	case TaskSchedulePausedProvisioningExact:
		if snapshot.Revision != ensured.Revision {
			return TaskScheduleSnapshot{}, newTaskScheduleError(
				TaskScheduleErrorUnsafeState, "activate", expected.taskID,
				errors.New("schedule changed after the paused definition was checkpointed"),
			)
		}
		// Safe to issue the one intended transition below. The raw update also
		// carries the just-described conflict token to close the final RPC race.
	default:
		return TaskScheduleSnapshot{}, newTaskScheduleError(TaskScheduleErrorUnsafeState, "activate", expected.taskID,
			fmt.Errorf("unexpected state %s", snapshot.State))
	}

	updateRequest, err := expected.activationRequest(desc.GetConflictToken())
	if err != nil {
		return TaskScheduleSnapshot{}, newTaskScheduleError(
			TaskScheduleErrorBlocked, "activate", expected.taskID, err,
		)
	}
	_, unpauseErr := s.c.WorkflowService().UpdateSchedule(ctx, updateRequest)
	post, describeErr := s.describeTaskScheduleForRecovery(ctx, expected)
	if describeErr != nil {
		if isTaskScheduleNotFound(describeErr) {
			return TaskScheduleSnapshot{}, newTaskScheduleError(
				TaskScheduleErrorNotFound, "activate", expected.taskID, describeErr,
			)
		}
		if taskScheduleMutationDefinitelyRejected(unpauseErr) {
			return TaskScheduleSnapshot{}, classifyTaskScheduleMutationError(
				"activate", expected.taskID, unpauseErr,
			)
		}
		return TaskScheduleSnapshot{}, newTaskScheduleError(
			TaskScheduleErrorOutcomeUnknown, "activate", expected.taskID,
			errors.Join(unpauseErr, describeErr),
		)
	}
	postSnapshot, err := verifyTaskScheduleDescription(expected, post, "activate")
	if err != nil {
		return TaskScheduleSnapshot{}, err
	}
	if postSnapshot.State == TaskScheduleActiveVirginExact || postSnapshot.State == TaskScheduleActiveUsedExact {
		return postSnapshot, nil
	}
	cause := unpauseErr
	if cause == nil {
		cause = errors.New("Unpause returned success but schedule remained paused")
	}
	return TaskScheduleSnapshot{}, classifyTaskScheduleMutationError("activate", expected.taskID, cause)
}

// DeleteTask deletes only an exact schedule. NotFound, including a second
// delete, is success. Every Delete result is followed by Describe; a transport
// timeout is therefore harmless when the object is in fact gone.
func (s *Scheduler) DeleteTask(ctx context.Context, prepared PreparedTaskSchedule) error {
	if err := taskScheduleContextError(ctx, "delete", ""); err != nil {
		return err
	}
	expected, err := s.buildTaskScheduleExpected(ctx, prepared, "delete", false)
	if err != nil {
		return err
	}
	release, err := s.acquireTaskScheduleGate(ctx, "delete", expected.taskID)
	if err != nil {
		return err
	}
	defer release()
	if err := taskScheduleContextError(ctx, "delete", expected.taskID); err != nil {
		return err
	}

	desc, err := s.describeTaskSchedule(ctx, expected)
	if err != nil {
		if isTaskScheduleNotFound(err) {
			return nil
		}
		return classifyTaskScheduleReadError("delete", expected.taskID, err)
	}
	if _, err := verifyTaskScheduleDescription(expected, desc, "delete"); err != nil {
		return err
	}

	_, deleteErr := s.c.WorkflowService().DeleteSchedule(ctx, &workflowservice.DeleteScheduleRequest{
		Namespace:  expected.prepared.Namespace,
		ScheduleId: expected.taskID,
		Identity:   taskScheduleIdentity(expected.prepared.FingerprintVersion),
	})
	post, describeErr := s.describeTaskScheduleForRecovery(ctx, expected)
	if isTaskScheduleNotFound(describeErr) {
		return nil
	}
	if describeErr != nil {
		if taskScheduleMutationDefinitelyRejected(deleteErr) {
			return classifyTaskScheduleMutationError("delete", expected.taskID, deleteErr)
		}
		return newTaskScheduleError(
			TaskScheduleErrorOutcomeUnknown, "delete", expected.taskID,
			errors.Join(deleteErr, describeErr),
		)
	}
	if _, err := verifyTaskScheduleDescription(expected, post, "delete"); err != nil {
		return err
	}
	if deleteErr == nil {
		deleteErr = errors.New("Delete returned success but schedule still exists")
	}
	return classifyTaskScheduleMutationError("delete", expected.taskID, deleteErr)
}

type taskScheduleFingerprint struct {
	IDSchemeVersion    string `json:"id_scheme_version"`
	FingerprintVersion string `json:"fingerprint_version"`
	Namespace          string `json:"namespace"`
	NamespaceID        string `json:"namespace_id"`
	ConverterID        string `json:"converter_id"`
	TenantID           int64  `json:"tenant_id"`
	UserID             int64  `json:"user_id"`
	TaskID             string `json:"task_id"`
	OperationID        string `json:"operation_id"`
	PreparedDigest     string `json:"prepared_digest"`
	RequestDigest      string `json:"request_digest"`
	LifecyclePhase     string `json:"lifecycle_phase"`
}

type taskScheduleExpected struct {
	taskID      string
	fingerprint taskScheduleFingerprint
	params      workflow.PushParams
	taskQueue   string
	prepared    PreparedTaskSchedule
	dc          converter.DataConverter
}

// taskScheduleGateSet serializes mutations for one TaskID inside this process.
// It is a context-aware keyed semaphore rather than a forever-growing mutex map:
// entries are reference-counted and removed after the last holder or waiter.
// A4's durable lease/fence remains necessary across processes.
type taskScheduleGateSet struct {
	mu   sync.Mutex
	byID map[string]*taskScheduleGate
}

type taskScheduleGate struct {
	token chan struct{}
	refs  int
}

func (s *Scheduler) acquireTaskScheduleGate(ctx context.Context, operation, taskID string) (func(), error) {
	gate := s.taskScheduleGates.reference(taskID)
	select {
	case <-ctx.Done():
		s.taskScheduleGates.unreference(taskID, gate)
		return nil, newTaskScheduleError(TaskScheduleErrorTransient, operation, taskID, ctx.Err())
	case <-gate.token:
		return func() {
			gate.token <- struct{}{}
			s.taskScheduleGates.unreference(taskID, gate)
		}, nil
	}
}

func (g *taskScheduleGateSet) reference(taskID string) *taskScheduleGate {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.byID == nil {
		g.byID = make(map[string]*taskScheduleGate)
	}
	gate := g.byID[taskID]
	if gate == nil {
		gate = &taskScheduleGate{token: make(chan struct{}, 1)}
		gate.token <- struct{}{}
		g.byID[taskID] = gate
	}
	gate.refs++
	return gate
}

func (g *taskScheduleGateSet) unreference(taskID string, gate *taskScheduleGate) {
	g.mu.Lock()
	defer g.mu.Unlock()
	gate.refs--
	if gate.refs == 0 && g.byID[taskID] == gate {
		delete(g.byID, taskID)
	}
}

func (e taskScheduleExpected) createRequest() (*workflowservice.CreateScheduleRequest, error) {
	schedule, err := e.protoSchedule(
		e.fingerprint,
		e.prepared.Creation.Paused,
		e.prepared.Creation.Note,
	)
	if err != nil {
		return nil, err
	}
	// Build the raw request instead of SDK ScheduleOptions. This makes hidden
	// proto fields explicit and prevents client ContextPropagators from freezing
	// retry-specific headers into a durable scheduled workflow action.
	return &workflowservice.CreateScheduleRequest{
		Namespace:  e.prepared.Namespace,
		ScheduleId: e.taskID,
		RequestId:  e.prepared.RequestDigest,
		Identity:   taskScheduleIdentity(e.prepared.FingerprintVersion),
		Schedule:   schedule,
	}, nil
}

func (e taskScheduleExpected) activationRequest(
	conflictToken []byte,
) (*workflowservice.UpdateScheduleRequest, error) {
	if len(conflictToken) == 0 {
		return nil, errors.New("Describe returned no schedule conflict token")
	}
	activeFingerprint := e.fingerprint
	activeFingerprint.LifecyclePhase = taskScheduleV1PhaseActive
	schedule, err := e.protoSchedule(activeFingerprint, false, e.prepared.Action.ActivationNote)
	if err != nil {
		return nil, err
	}
	return &workflowservice.UpdateScheduleRequest{
		Namespace:     e.prepared.Namespace,
		ScheduleId:    e.taskID,
		Schedule:      schedule,
		ConflictToken: slices.Clone(conflictToken),
		Identity:      taskScheduleIdentity(e.prepared.FingerprintVersion),
		RequestId:     taskScheduleRequestID("activate", e.prepared.RequestDigest),
	}, nil
}

func (e taskScheduleExpected) protoSchedule(
	fingerprint taskScheduleFingerprint,
	paused bool,
	note string,
) (*schedulepb.Schedule, error) {
	actionDC, err := taskScheduleActionDataConverter(e.prepared, e.dc)
	if err != nil {
		return nil, err
	}
	fingerprintPayload, err := actionDC.ToPayload(fingerprint)
	if err != nil {
		return nil, fmt.Errorf("encode task schedule fingerprint: %w", err)
	}
	if fingerprintPayload == nil {
		return nil, errors.New("encode task schedule fingerprint: converter returned nil payload")
	}
	paramsPayload, err := actionDC.ToPayload(e.params)
	if err != nil {
		return nil, fmt.Errorf("encode task schedule workflow params: %w", err)
	}
	if paramsPayload == nil {
		return nil, errors.New("encode task schedule workflow params: converter returned nil payload")
	}
	return &schedulepb.Schedule{
		Spec: taskScheduleProtoSpec(e.prepared.Timing),
		Action: &schedulepb.ScheduleAction{Action: &schedulepb.ScheduleAction_StartWorkflow{
			StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
				WorkflowId:   e.prepared.Action.ActionID,
				WorkflowType: &commonpb.WorkflowType{Name: e.prepared.Action.WorkflowType},
				TaskQueue: &taskqueuepb.TaskQueue{
					Name: e.taskQueue, Kind: enums.TASK_QUEUE_KIND_NORMAL,
				},
				Input:                    &commonpb.Payloads{Payloads: []*commonpb.Payload{paramsPayload}},
				WorkflowExecutionTimeout: durationpb.New(time.Duration(e.prepared.Action.WorkflowExecutionTimeoutNanos)),
				WorkflowRunTimeout:       durationpb.New(time.Duration(e.prepared.Action.WorkflowRunTimeoutNanos)),
				WorkflowTaskTimeout:      durationpb.New(time.Duration(e.prepared.Action.WorkflowTaskTimeoutNanos)),
				Memo: &commonpb.Memo{Fields: map[string]*commonpb.Payload{
					taskScheduleMemoKey: fingerprintPayload,
				}},
			},
		}},
		Policies: &schedulepb.SchedulePolicies{
			OverlapPolicy:  enums.ScheduleOverlapPolicy(e.prepared.Policy.Overlap),
			CatchupWindow:  durationpb.New(time.Duration(e.prepared.Policy.CatchupNanos)),
			PauseOnFailure: e.prepared.Policy.PauseOnFailure,
		},
		State: &schedulepb.ScheduleState{
			Notes:            note,
			Paused:           paused,
			LimitedActions:   e.prepared.Creation.RemainingActions != 0,
			RemainingActions: int64(e.prepared.Creation.RemainingActions),
		},
	}, nil
}

func taskScheduleIdentity(fingerprintVersion string) string {
	return "vane-task-schedule/" + fingerprintVersion
}

func taskScheduleRequestID(operation, requestDigest string) string {
	sum := sha256.Sum256([]byte(operation + ":" + requestDigest))
	return hex.EncodeToString(sum[:])
}

func taskScheduleProtoSpec(timing PreparedTaskScheduleTiming) *schedulepb.ScheduleSpec {
	spec := &schedulepb.ScheduleSpec{TimezoneName: timing.TimeZoneName}
	if timing.Calendar != nil {
		calendar := timing.Calendar
		spec.StructuredCalendar = []*schedulepb.StructuredCalendarSpec{{
			Second:     taskScheduleProtoRanges(calendar.Second, 0, 59),
			Minute:     taskScheduleProtoRanges(calendar.Minute, 0, 59),
			Hour:       taskScheduleProtoRanges(calendar.Hour, 0, 23),
			DayOfMonth: taskScheduleProtoRanges(calendar.DayOfMonth, 1, 31),
			Month:      taskScheduleProtoRanges(calendar.Month, 1, 12),
			DayOfWeek:  taskScheduleProtoRanges(calendar.DayOfWeek, 0, 6),
		}}
		return spec
	}
	spec.Interval = []*schedulepb.IntervalSpec{{
		Interval: durationpb.New(time.Duration(timing.EveryNanos)),
		Phase:    durationpb.New(time.Duration(timing.OffsetNanos)),
	}}
	return spec
}

func taskScheduleProtoRanges(bits uint64, minValue, maxValue int) []*schedulepb.Range {
	ranges := make([]*schedulepb.Range, 0, maxValue-minValue+1)
	for value := minValue; value <= maxValue; {
		if bits&(uint64(1)<<value) == 0 {
			value++
			continue
		}
		end := value
		for end+1 <= maxValue && bits&(uint64(1)<<(end+1)) != 0 {
			end++
		}
		ranges = append(ranges, &schedulepb.Range{Start: int32(value), End: int32(end), Step: 1})
		value = end + 1
	}
	return ranges
}

func taskScheduleActionDataConverter(
	prepared PreparedTaskSchedule,
	dc converter.DataConverter,
) (converter.DataConverter, error) {
	if dc == nil {
		return nil, errors.New("task schedule data converter is unavailable")
	}
	contextual, ok := dc.(converter.DataConverterWithSerializationContext)
	if !ok {
		return dc, nil
	}
	wrapped := contextual.WithSerializationContext(converter.WorkflowSerializationContext{
		Namespace: prepared.Namespace, WorkflowID: prepared.Action.ActionID,
	})
	if wrapped == nil {
		return nil, errors.New("task schedule data converter returned nil for workflow serialization context")
	}
	return wrapped, nil
}

// PrepareTaskSchedule compiles a request once and binds it to the current
// Temporal namespace identity. A4 checkpoints the returned value verbatim;
// retries and recovery call the lifecycle primitives with that value instead of
// running this compiler again.
func (s *Scheduler) PrepareTaskSchedule(
	ctx context.Context,
	req TaskScheduleRequest,
) (PreparedTaskSchedule, error) {
	if err := taskScheduleContextError(ctx, "prepare", ""); err != nil {
		return PreparedTaskSchedule{}, err
	}
	prepared, err := s.buildPreparedTaskSchedule(req)
	if err != nil {
		return PreparedTaskSchedule{}, err
	}
	if _, err := taskScheduleActionDataConverter(prepared, s.taskScheduleDecoder(prepared.ConverterID)); err != nil {
		return PreparedTaskSchedule{}, newTaskScheduleError(
			TaskScheduleErrorInvalid, "prepare", prepared.TaskID, err,
		)
	}
	namespaceID, err := s.resolveTaskScheduleNamespaceID(ctx, prepared.TaskID)
	if err != nil {
		return PreparedTaskSchedule{}, err
	}
	prepared.NamespaceID = namespaceID
	prepared.RequestDigest, err = digestPreparedTaskSchedule(prepared)
	if err != nil {
		return PreparedTaskSchedule{}, newTaskScheduleError(
			TaskScheduleErrorInvalid, "prepare", prepared.TaskID, err,
		)
	}
	if err := validatePreparedTaskSchedule(prepared); err != nil {
		return PreparedTaskSchedule{}, newTaskScheduleError(
			TaskScheduleErrorInvalid, "prepare", prepared.TaskID, err,
		)
	}
	return prepared, nil
}

func (s *Scheduler) buildPreparedTaskSchedule(req TaskScheduleRequest) (PreparedTaskSchedule, error) {
	taskID, err := TaskIDForOperation(req.TenantID, req.UserID, req.OperationID)
	if err != nil {
		return PreparedTaskSchedule{}, err
	}
	if s == nil || s.c == nil {
		return PreparedTaskSchedule{}, newTaskScheduleError(
			TaskScheduleErrorInvalid, "build", taskID,
			errors.New("Temporal client is required"),
		)
	}
	namespace, converterID, dc := s.taskScheduleEnvironment()
	if err := validateTaskScheduleString("namespace", namespace, true); err != nil {
		return PreparedTaskSchedule{}, newTaskScheduleError(TaskScheduleErrorInvalid, "build", taskID, err)
	}
	if err := validateTaskScheduleString("task queue", s.tq, true); err != nil {
		return PreparedTaskSchedule{}, newTaskScheduleError(
			TaskScheduleErrorInvalid, "build", taskID,
			err,
		)
	}
	if err := validateTaskScheduleString("converter ID", converterID, true); err != nil || dc == nil {
		if err == nil {
			err = errors.New("data converter is required")
		}
		return PreparedTaskSchedule{}, newTaskScheduleError(TaskScheduleErrorInvalid, "build", taskID, err)
	}
	if _, requestAware := dc.(taskScheduleRequestContextAwareConverter); requestAware {
		return PreparedTaskSchedule{}, newTaskScheduleError(
			TaskScheduleErrorInvalid, "build", taskID,
			errors.New("request-context-aware data converters are not durable for task schedules"),
		)
	}
	if err := validateTaskScheduleDigest("prepared_digest", req.PreparedDigest); err != nil {
		return PreparedTaskSchedule{}, newTaskScheduleError(TaskScheduleErrorInvalid, "build", taskID, err)
	}
	name := strings.TrimSpace(req.NLDescription)
	if name == "" {
		return PreparedTaskSchedule{}, newTaskScheduleError(
			TaskScheduleErrorInvalid, "build", taskID,
			errors.New("nl_description must be non-empty"),
		)
	}
	if !utf8.ValidString(name) {
		return PreparedTaskSchedule{}, newTaskScheduleError(
			TaskScheduleErrorInvalid, "build", taskID,
			errors.New("nl_description must be valid UTF-8"),
		)
	}
	if req.Scope.TopN < 0 {
		return PreparedTaskSchedule{}, newTaskScheduleError(
			TaskScheduleErrorInvalid, "build", taskID,
			errors.New("scope.top_n must not be negative"),
		)
	}
	seenSourceIDs := make(map[int64]struct{}, len(req.Scope.SourceIDs))
	for _, sourceID := range req.Scope.SourceIDs {
		if sourceID <= 0 {
			return PreparedTaskSchedule{}, newTaskScheduleError(
				TaskScheduleErrorInvalid, "build", taskID,
				errors.New("scope source IDs must be positive"),
			)
		}
		if _, exists := seenSourceIDs[sourceID]; exists {
			return PreparedTaskSchedule{}, newTaskScheduleError(
				TaskScheduleErrorInvalid, "build", taskID,
				fmt.Errorf("duplicate scope source ID %d", sourceID),
			)
		}
		seenSourceIDs[sourceID] = struct{}{}
	}

	_, timing, err := buildTaskScheduleSpec(req.Spec)
	if err != nil {
		return PreparedTaskSchedule{}, newTaskScheduleError(TaskScheduleErrorInvalid, "build", taskID, err)
	}
	scope := workflow.PushScope{TopN: req.Scope.TopN}
	if len(req.Scope.SourceIDs) > 0 {
		scope.SourceIDs = slices.Clone(req.Scope.SourceIDs)
	}
	params := makePushParams(req.TenantID, req.UserID, taskID, scope, name)
	runtimeVersion := s.compiledRuntime.runtimeVersionFor(taskID)
	fingerprintVersion := taskScheduleFingerprintVersionV1
	if runtimeVersion == workflow.CompiledRuntimeSnapshotV1 {
		// A v2 checkpoint is written only for an explicitly selected C1b
		// canary/all-task rollout. This keeps dark deployment expansion-safe:
		// an older binary can still resume every checkpoint written before C1b
		// is deliberately activated.
		fingerprintVersion = taskScheduleFingerprintVersion
		params.RuntimeVersion = s.runOutcome.runtimeVersionFor(
			taskID, runtimeVersion)
	} else {
		// Preserve the exact semantic v1 Action envelope. buildTaskScheduleExpected
		// upgrades it deterministically before Temporal I/O, while the durable
		// checkpoint itself remains readable by the previous binary.
		params.TenantID = 0
		params.ExecutionMode = ""
		params.RuntimeVersion = ""
	}
	return PreparedTaskSchedule{
		IDSchemeVersion:    taskScheduleIDSchemeVersion,
		FingerprintVersion: fingerprintVersion,
		Namespace:          namespace,
		ConverterID:        converterID,
		TaskID:             taskID,
		TenantID:           req.TenantID,
		UserID:             req.UserID,
		OperationID:        req.OperationID,
		PreparedDigest:     req.PreparedDigest,
		Timing:             timing,
		Action: PreparedTaskScheduleAction{
			Params:                   params,
			TaskQueue:                s.tq,
			WorkflowType:             taskScheduleV1WorkflowType,
			ActionID:                 "wf-" + taskID,
			WorkflowTaskTimeoutNanos: int64(taskScheduleV1WorkflowTaskTime),
			ActivationNote:           taskScheduleV1ActivationNote,
		},
		Policy: PreparedTaskSchedulePolicy{
			Overlap:      int32(enums.SCHEDULE_OVERLAP_POLICY_SKIP),
			CatchupNanos: int64(taskScheduleV1CatchupWindow),
		},
		Creation: PreparedTaskScheduleCreation{
			Paused: true,
			Note:   taskScheduleV1ProvisioningNote,
		},
	}, nil
}

func (s *Scheduler) taskScheduleEnvironment() (string, string, converter.DataConverter) {
	s.taskScheduleEnv.mu.Lock()
	defer s.taskScheduleEnv.mu.Unlock()
	converterID := s.taskScheduleEnv.converterID
	dc := s.taskScheduleEnv.dc
	if converterID == "" && dc == nil {
		converterID = taskScheduleDefaultConverterID
		dc = converter.GetDefaultDataConverter()
	} else if converterID == taskScheduleDefaultConverterID {
		// This ID denotes the SDK default converter guarded by an exact payload
		// golden. Allowing callers to bind arbitrary bytes to the same ID would
		// make retained checkpoints ambiguous across process restarts.
		dc = nil
	}
	return s.taskScheduleEnv.namespace, converterID, dc
}

func (s *Scheduler) taskScheduleDecoder(id string) converter.DataConverter {
	s.taskScheduleEnv.mu.Lock()
	defer s.taskScheduleEnv.mu.Unlock()
	if id == s.taskScheduleEnv.converterID {
		if id == taskScheduleDefaultConverterID {
			return nil
		}
		return s.taskScheduleEnv.dc
	}
	if id == taskScheduleDefaultConverterID &&
		s.taskScheduleEnv.converterID == "" && s.taskScheduleEnv.dc == nil {
		return converter.GetDefaultDataConverter()
	}
	if dc := s.taskScheduleEnv.decoders[id]; dc != nil {
		return dc
	}
	return nil
}

func (s *Scheduler) resolveTaskScheduleNamespaceID(ctx context.Context, taskID string) (string, error) {
	s.taskScheduleEnv.mu.Lock()
	namespace := s.taskScheduleEnv.namespace
	override := s.taskScheduleEnv.namespaceIDOverride
	s.taskScheduleEnv.mu.Unlock()
	if override != "" {
		return override, nil
	}
	resp, err := s.c.WorkflowService().DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{
		Namespace: namespace,
	})
	if err != nil {
		// A missing namespace is an environment/configuration blocker, whereas a
		// missing schedule is a normal lifecycle state. Some transports preserve
		// NamespaceNotFound and others expose only the gRPC NotFound code.
		if status.Code(err) == codes.NotFound {
			return "", newTaskScheduleError(TaskScheduleErrorBlocked, "resolve_namespace", taskID, err)
		}
		return "", classifyTaskScheduleReadError("resolve_namespace", taskID, err)
	}
	namespaceID := resp.GetNamespaceInfo().GetId()
	if err := validateTaskScheduleString("Temporal namespace ID", namespaceID, true); err != nil {
		return "", newTaskScheduleError(
			TaskScheduleErrorConflict, "resolve_namespace", taskID,
			err,
		)
	}
	return namespaceID, nil
}

func digestPreparedTaskSchedule(prepared PreparedTaskSchedule) (string, error) {
	prepared.RequestDigest = ""
	b, err := json.Marshal(prepared)
	if err != nil {
		return "", fmt.Errorf("marshal prepared task schedule: %w", err)
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

// ValidateTaskScheduleSpec validates the complete deterministic A3 timing
// contract without consulting Temporal or any process configuration. Task
// preparation uses it before a paid compiler call, so an invalid cron, time
// zone, anchor, or interval cannot consume budget first and fail afterwards.
func ValidateTaskScheduleSpec(spec ScheduleSpec) error {
	_, _, err := buildTaskScheduleSpec(spec)
	return err
}

// ValidatePreparedTaskScheduleRequest proves that a serialized A3 result is
// both internally valid and derived from req. It is deliberately pure: A4 can
// validate an immutable checkpoint after a restart without reaching Temporal
// or rebuilding it from the current Scheduler configuration.
func ValidatePreparedTaskScheduleRequest(
	prepared PreparedTaskSchedule,
	req TaskScheduleRequest,
) error {
	if err := validatePreparedTaskSchedule(prepared); err != nil {
		return err
	}
	if prepared.TenantID != req.TenantID || prepared.UserID != req.UserID ||
		prepared.OperationID != req.OperationID || prepared.PreparedDigest != req.PreparedDigest {
		return errors.New("prepared schedule does not belong to the requested definition")
	}
	taskID, err := taskIDForOperationVersion(
		prepared.IDSchemeVersion,
		req.TenantID,
		req.UserID,
		req.OperationID,
	)
	if err != nil {
		return fmt.Errorf("derive requested task ID: %w", err)
	}
	if prepared.TaskID != taskID {
		return errors.New("prepared schedule task ID differs from the request")
	}
	_, timing, err := buildTaskScheduleSpec(req.Spec)
	if err != nil {
		return fmt.Errorf("validate requested schedule spec: %w", err)
	}
	if !preparedTaskScheduleTimingEqual(prepared.Timing, timing) {
		return errors.New("prepared schedule timing differs from the request")
	}
	params := prepared.Action.Params
	if params.UserID != req.UserID || params.RunKind != workflow.PushRunKindScheduled ||
		params.ScheduleID != taskID ||
		params.NLDesc != strings.TrimSpace(req.NLDescription) ||
		params.Scope.TopN != req.Scope.TopN ||
		!slices.Equal(params.Scope.SourceIDs, req.Scope.SourceIDs) {
		return errors.New("prepared workflow parameters differ from the request")
	}
	return nil
}

func preparedTaskScheduleTimingEqual(a, b PreparedTaskScheduleTiming) bool {
	if a.EveryNanos != b.EveryNanos || a.OffsetNanos != b.OffsetNanos ||
		a.TimeZoneName != b.TimeZoneName || (a.Calendar == nil) != (b.Calendar == nil) {
		return false
	}
	return a.Calendar == nil || *a.Calendar == *b.Calendar
}

func validateTaskScheduleDigest(field, digest string) error {
	if len(digest) != sha256.Size*2 || strings.ToLower(digest) != digest {
		return fmt.Errorf("%s must be a lowercase SHA-256 hex digest", field)
	}
	if _, err := hex.DecodeString(digest); err != nil {
		return fmt.Errorf("%s must be hex: %w", field, err)
	}
	return nil
}

func validateTaskScheduleString(field, value string, required bool) error {
	if required && value == "" {
		return fmt.Errorf("%s must be non-empty", field)
	}
	if !utf8.ValidString(value) {
		return fmt.Errorf("%s must be valid UTF-8", field)
	}
	if strings.TrimSpace(value) != value {
		return fmt.Errorf("%s must not have surrounding whitespace", field)
	}
	return nil
}

func clonePreparedTaskSchedule(prepared PreparedTaskSchedule) PreparedTaskSchedule {
	prepared.Action.Params.Scope.SourceIDs = slices.Clone(prepared.Action.Params.Scope.SourceIDs)
	if prepared.Timing.Calendar != nil {
		calendar := *prepared.Timing.Calendar
		prepared.Timing.Calendar = &calendar
	}
	return prepared
}

func validatePreparedTaskSchedule(prepared PreparedTaskSchedule) error {
	if prepared.IDSchemeVersion != taskScheduleIDSchemeVersionV1 {
		return fmt.Errorf("unsupported task ID scheme version %q", prepared.IDSchemeVersion)
	}
	if prepared.FingerprintVersion != taskScheduleFingerprintVersionV1 &&
		prepared.FingerprintVersion != taskScheduleFingerprintVersionV2 {
		return fmt.Errorf("unsupported fingerprint version %q", prepared.FingerprintVersion)
	}
	for _, field := range []struct {
		name  string
		value string
	}{
		{"namespace", prepared.Namespace},
		{"namespace_id", prepared.NamespaceID},
		{"converter_id", prepared.ConverterID},
		{"task_id", prepared.TaskID},
		{"operation_id", prepared.OperationID},
		{"task_queue", prepared.Action.TaskQueue},
		{"workflow_type", prepared.Action.WorkflowType},
		{"action_id", prepared.Action.ActionID},
		{"activation_note", prepared.Action.ActivationNote},
		{"nl_description", prepared.Action.Params.NLDesc},
		{"creation.note", prepared.Creation.Note},
	} {
		if err := validateTaskScheduleString(field.name, field.value, true); err != nil {
			return err
		}
	}
	if err := validateTaskScheduleDigest("prepared_digest", prepared.PreparedDigest); err != nil {
		return err
	}
	if err := validateTaskScheduleDigest("request_digest", prepared.RequestDigest); err != nil {
		return err
	}
	taskID, err := taskIDForOperationVersion(
		prepared.IDSchemeVersion,
		prepared.TenantID,
		prepared.UserID,
		prepared.OperationID,
	)
	if err != nil {
		return fmt.Errorf("derive prepared task ID: %w", err)
	}
	if prepared.TaskID != taskID {
		return errors.New("task_id does not match its tenant, user, operation, and ID scheme")
	}
	params := prepared.Action.Params
	if params.UserID != prepared.UserID || params.RunKind != workflow.PushRunKindScheduled ||
		params.ScheduleID != prepared.TaskID {
		return errors.New("workflow params do not match the prepared owner and task ID")
	}
	if params.Snapshot != nil {
		return errors.New("prepared Schedule Action must not carry a run snapshot")
	}
	if prepared.FingerprintVersion == taskScheduleFingerprintVersionV1 {
		// Retained v1 checkpoints predate these JSON fields. Their exact wire
		// shape therefore decodes to the language zero values, not to the
		// explicit "unknown" sentinel introduced later. Accept only that old
		// shape so a forged partially-upgraded v1 checkpoint still fails closed.
		if params.TenantID != 0 || params.ExecutionMode != "" ||
			params.RuntimeVersion != "" {
			return errors.New("v1 workflow params are not in the retained legacy shape")
		}
	} else {
		if params.TenantID != prepared.TenantID || params.ExecutionMode != types.ExecutionModeCompiled {
			return errors.New("v2 workflow params do not match the prepared tenant and compiled mode")
		}
		if params.RuntimeVersion != "" && !workflow.IsCompiledRuntimeV1(params.RuntimeVersion) {
			return errors.New("prepared Schedule Action runtime version is unsupported")
		}
	}
	if params.Scope.TopN < 0 {
		return errors.New("scope.top_n must not be negative")
	}
	seenSourceIDs := make(map[int64]struct{}, len(params.Scope.SourceIDs))
	for _, sourceID := range params.Scope.SourceIDs {
		if sourceID <= 0 {
			return errors.New("scope source IDs must be positive")
		}
		if _, exists := seenSourceIDs[sourceID]; exists {
			return fmt.Errorf("duplicate scope source ID %d", sourceID)
		}
		seenSourceIDs[sourceID] = struct{}{}
	}
	if _, err := scheduleSpecFromPreparedTimingV1(prepared.Timing); err != nil {
		return err
	}
	if prepared.Action.WorkflowType != taskScheduleV1WorkflowType ||
		prepared.Action.ActionID != "wf-"+prepared.TaskID ||
		prepared.Action.WorkflowExecutionTimeoutNanos != 0 ||
		prepared.Action.WorkflowRunTimeoutNanos != 0 ||
		prepared.Action.WorkflowTaskTimeoutNanos != int64(taskScheduleV1WorkflowTaskTime) ||
		prepared.Action.HasRetryPolicy || prepared.Action.ActivationNote != taskScheduleV1ActivationNote {
		return errors.New("workflow action is not a supported v1 task action")
	}
	if prepared.Policy.Overlap != int32(enums.SCHEDULE_OVERLAP_POLICY_SKIP) ||
		prepared.Policy.CatchupNanos != int64(taskScheduleV1CatchupWindow) ||
		prepared.Policy.PauseOnFailure {
		return errors.New("schedule policy is not the supported v1 policy")
	}
	if !prepared.Creation.Paused || prepared.Creation.RemainingActions != 0 ||
		prepared.Creation.TriggerImmediately || prepared.Creation.BackfillCount != 0 ||
		prepared.Creation.Note != taskScheduleV1ProvisioningNote {
		return errors.New("schedule creation settings are not the supported paused v1 settings")
	}
	digest, err := digestPreparedTaskSchedule(prepared)
	if err != nil {
		return err
	}
	if digest != prepared.RequestDigest {
		return errors.New("request_digest does not match the immutable prepared task schedule")
	}
	return nil
}

func (s *Scheduler) buildTaskScheduleExpected(
	ctx context.Context,
	prepared PreparedTaskSchedule,
	operation string,
	requireCurrentTaskQueue bool,
) (taskScheduleExpected, error) {
	prepared = clonePreparedTaskSchedule(prepared)
	if err := validatePreparedTaskSchedule(prepared); err != nil {
		return taskScheduleExpected{}, newTaskScheduleError(
			TaskScheduleErrorInvalid, operation, prepared.TaskID, err,
		)
	}
	if s == nil || s.c == nil {
		return taskScheduleExpected{}, newTaskScheduleError(
			TaskScheduleErrorInvalid, operation, prepared.TaskID,
			errors.New("Temporal client is required"),
		)
	}
	namespace, currentConverterID, _ := s.taskScheduleEnvironment()
	if namespace != prepared.Namespace {
		return taskScheduleExpected{}, newTaskScheduleError(
			TaskScheduleErrorConflict, operation, prepared.TaskID,
			fmt.Errorf("prepared namespace %q does not match current namespace %q", prepared.Namespace, namespace),
		)
	}
	dc := s.taskScheduleDecoder(prepared.ConverterID)
	if dc == nil {
		return taskScheduleExpected{}, newTaskScheduleError(
			TaskScheduleErrorBlocked, operation, prepared.TaskID,
			fmt.Errorf("prepared converter %q is unavailable in the current process", prepared.ConverterID),
		)
	}
	if _, requestAware := dc.(taskScheduleRequestContextAwareConverter); requestAware {
		return taskScheduleExpected{}, newTaskScheduleError(
			TaskScheduleErrorBlocked, operation, prepared.TaskID,
			errors.New("prepared converter is request-context-aware and cannot be recovered durably"),
		)
	}
	namespaceID, err := s.resolveTaskScheduleNamespaceID(ctx, prepared.TaskID)
	if err != nil {
		return taskScheduleExpected{}, err
	}
	if namespaceID != prepared.NamespaceID {
		return taskScheduleExpected{}, newTaskScheduleError(
			TaskScheduleErrorConflict, operation, prepared.TaskID,
			fmt.Errorf("prepared namespace ID %q does not match current namespace ID %q", prepared.NamespaceID, namespaceID),
		)
	}
	if requireCurrentTaskQueue &&
		(s.tq != prepared.Action.TaskQueue || currentConverterID != prepared.ConverterID) {
		return taskScheduleExpected{}, newTaskScheduleError(
			TaskScheduleErrorConflict, operation, prepared.TaskID,
			fmt.Errorf(
				"prepared execution environment (task queue %q, converter %q) is not current (%q, %q)",
				prepared.Action.TaskQueue, prepared.ConverterID, s.tq, currentConverterID,
			),
		)
	}
	_, err = scheduleSpecFromPreparedTimingV1(prepared.Timing)
	if err != nil {
		return taskScheduleExpected{}, newTaskScheduleError(
			TaskScheduleErrorInvalid, operation, prepared.TaskID, err,
		)
	}
	params := prepared.Action.Params
	if prepared.FingerprintVersion == taskScheduleFingerprintVersionV1 {
		// Fixed retained-reader upgrade. It never consults rollout config: v1
		// remains the legacy Compiled implementation, but gains the explicit
		// tenant/mode envelope before it can be activated.
		params.TenantID = prepared.TenantID
		params.ExecutionMode = types.ExecutionModeCompiled
		params.RuntimeVersion = ""
	}
	params.Scope.SourceIDs = slices.Clone(params.Scope.SourceIDs)
	return taskScheduleExpected{
		taskID: prepared.TaskID,
		fingerprint: taskScheduleFingerprint{
			IDSchemeVersion:    prepared.IDSchemeVersion,
			FingerprintVersion: prepared.FingerprintVersion,
			Namespace:          prepared.Namespace,
			NamespaceID:        prepared.NamespaceID,
			ConverterID:        prepared.ConverterID,
			TenantID:           prepared.TenantID,
			UserID:             prepared.UserID,
			TaskID:             prepared.TaskID,
			OperationID:        prepared.OperationID,
			PreparedDigest:     prepared.PreparedDigest,
			RequestDigest:      prepared.RequestDigest,
			LifecyclePhase:     taskScheduleV1PhaseProvisioning,
		},
		params:    params,
		taskQueue: prepared.Action.TaskQueue,
		prepared:  prepared,
		dc:        dc,
	}, nil
}

func buildTaskScheduleSpec(spec ScheduleSpec) (client.ScheduleSpec, PreparedTaskScheduleTiming, error) {
	if spec.EverySeconds < 0 {
		return client.ScheduleSpec{}, PreparedTaskScheduleTiming{}, errors.New("every_seconds must not be negative")
	}
	if err := validateSpec(spec); err != nil {
		return client.ScheduleSpec{}, PreparedTaskScheduleTiming{}, err
	}
	return buildTaskScheduleSpecV1(spec, defaultTZ, parseAnchor)
}

// buildTaskDefinitionEditScheduleSpecV1 reconstructs timing from an already
// authenticated definition-edit/v1 checkpoint. Its validator, default zone,
// and anchor parser are retained literals; current writer policy is applied
// separately only when sealing a new target.
func buildTaskDefinitionEditScheduleSpecV1(
	spec ScheduleSpec,
) (client.ScheduleSpec, PreparedTaskScheduleTiming, error) {
	if err := validateTaskDefinitionEditScheduleSpecV1(spec); err != nil {
		return client.ScheduleSpec{}, PreparedTaskScheduleTiming{}, err
	}
	return buildTaskScheduleSpecV1(
		spec, taskDefinitionEditV1DefaultTimeZone, parseTaskDefinitionEditAnchorV1,
	)
}

// buildTaskScheduleSpecV1 is the retained v1 spec-to-timing translation. New
// schedule wire versions must add a new translator instead of changing these
// semantics, because durable definition edits bind their Approved spec to the
// exact timing produced here.
func buildTaskScheduleSpecV1(
	spec ScheduleSpec,
	defaultTimeZone string,
	parseV1Anchor func(string) (time.Time, error),
) (client.ScheduleSpec, PreparedTaskScheduleTiming, error) {
	tz := strings.TrimSpace(spec.TZ)
	if tz == "" {
		tz = defaultTimeZone
	}
	if _, err := time.LoadLocation(tz); err != nil {
		return client.ScheduleSpec{}, PreparedTaskScheduleTiming{}, fmt.Errorf("invalid time zone %q: %w", tz, err)
	}
	if spec.EverySeconds > 0 {
		if int64(spec.EverySeconds) > int64((time.Duration(1<<63-1))/time.Second) {
			return client.ScheduleSpec{}, PreparedTaskScheduleTiming{}, errors.New("every_seconds exceeds time.Duration")
		}
		anchor, err := parseV1Anchor(spec.AnchorAt)
		if err != nil {
			return client.ScheduleSpec{}, PreparedTaskScheduleTiming{}, err
		}
		if anchor.Nanosecond() != 0 {
			return client.ScheduleSpec{}, PreparedTaskScheduleTiming{},
				errors.New("anchor_at must use whole-second precision")
		}
		every := time.Duration(spec.EverySeconds) * time.Second
		offset := taskScheduleIntervalOffsetV1(anchor, spec.EverySeconds)
		timing := PreparedTaskScheduleTiming{
			EveryNanos: int64(every), OffsetNanos: int64(offset), TimeZoneName: tz,
		}
		sdkSpec, err := scheduleSpecFromPreparedTimingV1(timing)
		return sdkSpec, timing, err
	}

	fields := strings.Fields(spec.Cron)
	if len(fields) != 5 {
		return client.ScheduleSpec{}, PreparedTaskScheduleTiming{}, errors.New("cron must contain five fields")
	}
	dayOfWeek, err := parseTaskScheduleCronDayOfWeekV1(fields[4])
	if err != nil {
		return client.ScheduleSpec{}, PreparedTaskScheduleTiming{}, fmt.Errorf("invalid cron day-of-week: %w", err)
	}
	// robfig/cron rejects the standard Sunday alias 7. Parse the other four
	// fields with its proven parser and canonicalize DOW ourselves.
	fields[4] = "*"
	fields[3] = normalizeTaskScheduleCronMonthNamesV1(fields[3])
	parsed, err := cron.ParseStandard(strings.Join(fields, " "))
	if err != nil {
		return client.ScheduleSpec{}, PreparedTaskScheduleTiming{}, fmt.Errorf("invalid cron: %w", err)
	}
	parsedSpec, ok := parsed.(*cron.SpecSchedule)
	if !ok {
		return client.ScheduleSpec{}, PreparedTaskScheduleTiming{}, errors.New("cron did not compile to a calendar schedule")
	}
	calendarBits := PreparedTaskScheduleCalendar{
		Second:     trimTaskScheduleCronBitsV1(parsedSpec.Second, 0, 59),
		Minute:     trimTaskScheduleCronBitsV1(parsedSpec.Minute, 0, 59),
		Hour:       trimTaskScheduleCronBitsV1(parsedSpec.Hour, 0, 23),
		DayOfMonth: trimTaskScheduleCronBitsV1(parsedSpec.Dom, 1, 31),
		Month:      trimTaskScheduleCronBitsV1(parsedSpec.Month, 1, 12),
		DayOfWeek:  dayOfWeek,
	}
	timing := PreparedTaskScheduleTiming{
		Calendar: &calendarBits, TimeZoneName: tz,
	}
	sdkSpec, err := scheduleSpecFromPreparedTimingV1(timing)
	return sdkSpec, timing, err
}

// scheduleSpecFromPreparedTimingV1 is the retained semantic reader for every
// persisted task-schedule/v1 timing. Current policy belongs before a new v1
// checkpoint is written; never add current floors or allowlists here.
func scheduleSpecFromPreparedTimingV1(timing PreparedTaskScheduleTiming) (client.ScheduleSpec, error) {
	if err := validateTaskScheduleString("time_zone_name", timing.TimeZoneName, true); err != nil {
		return client.ScheduleSpec{}, err
	}
	if _, err := time.LoadLocation(timing.TimeZoneName); err != nil {
		return client.ScheduleSpec{}, fmt.Errorf("invalid time zone %q: %w", timing.TimeZoneName, err)
	}
	if timing.Calendar != nil {
		if timing.EveryNanos != 0 || timing.OffsetNanos != 0 {
			return client.ScheduleSpec{}, errors.New("calendar and interval timing are mutually exclusive")
		}
		calendar := timing.Calendar
		fields := []struct {
			name     string
			bits     uint64
			min, max int
		}{
			{"second", calendar.Second, 0, 59},
			{"minute", calendar.Minute, 0, 59},
			{"hour", calendar.Hour, 0, 23},
			{"day_of_month", calendar.DayOfMonth, 1, 31},
			{"month", calendar.Month, 1, 12},
			{"day_of_week", calendar.DayOfWeek, 0, 6},
		}
		for _, field := range fields {
			allowed := scheduleBitMask(field.min, field.max)
			if field.bits == 0 || field.bits&^allowed != 0 {
				return client.ScheduleSpec{}, fmt.Errorf("calendar %s bit set is empty or out of range", field.name)
			}
		}
		sdkCalendar := client.ScheduleCalendarSpec{
			Second:     cronBitsToRanges(calendar.Second, 0, 59),
			Minute:     cronBitsToRanges(calendar.Minute, 0, 59),
			Hour:       cronBitsToRanges(calendar.Hour, 0, 23),
			DayOfMonth: cronBitsToRanges(calendar.DayOfMonth, 1, 31),
			Month:      cronBitsToRanges(calendar.Month, 1, 12),
			DayOfWeek:  cronBitsToRanges(calendar.DayOfWeek, 0, 6),
		}
		return client.ScheduleSpec{
			Calendars: []client.ScheduleCalendarSpec{sdkCalendar}, TimeZoneName: timing.TimeZoneName,
		}, nil
	}
	if timing.EveryNanos <= 0 {
		return client.ScheduleSpec{}, errors.New("prepared timing must contain one positive interval or calendar")
	}
	if timing.OffsetNanos < 0 || timing.OffsetNanos >= timing.EveryNanos {
		return client.ScheduleSpec{}, errors.New("interval offset must be non-negative and smaller than every")
	}
	return client.ScheduleSpec{
		Intervals: []client.ScheduleIntervalSpec{{
			Every: time.Duration(timing.EveryNanos), Offset: time.Duration(timing.OffsetNanos),
		}},
		TimeZoneName: timing.TimeZoneName,
	}, nil
}

func scheduleBitMask(minValue, maxValue int) uint64 {
	var mask uint64
	for value := minValue; value <= maxValue; value++ {
		mask |= uint64(1) << value
	}
	return mask
}

func parseTaskScheduleCronDayOfWeekV1(field string) (uint64, error) {
	field = strings.TrimSpace(strings.ToLower(field))
	if field == "" {
		return 0, errors.New("field is empty")
	}
	aliases := map[string]int{
		"sun": 0, "mon": 1, "tue": 2, "wed": 3,
		"thu": 4, "fri": 5, "sat": 6,
		"sunday": 0, "monday": 1, "tuesday": 2, "wednesday": 3,
		"thursday": 4, "friday": 5, "saturday": 6,
	}
	parseValue := func(value string) (int, error) {
		if alias, ok := aliases[value]; ok {
			return alias, nil
		}
		parsed, err := strconv.Atoi(value)
		if err != nil {
			return 0, fmt.Errorf("invalid value %q", value)
		}
		if parsed < 0 || parsed > 7 {
			return 0, fmt.Errorf("value %d is outside 0..7", parsed)
		}
		return parsed, nil
	}
	var bits uint64
	for token := range strings.SplitSeq(field, ",") {
		if token == "" {
			return 0, errors.New("empty list item")
		}
		parts := strings.Split(token, "/")
		if len(parts) > 2 {
			return 0, fmt.Errorf("invalid step expression %q", token)
		}
		step := 1
		if len(parts) == 2 {
			parsedStep, err := strconv.Atoi(parts[1])
			if err != nil || parsedStep <= 0 {
				return 0, fmt.Errorf("invalid step in %q", token)
			}
			step = parsedStep
		}
		base := parts[0]
		start, end := 0, 6
		switch {
		case base == "*" || base == "?":
		case strings.Contains(base, "-"):
			rangeParts := strings.Split(base, "-")
			if len(rangeParts) != 2 {
				return 0, fmt.Errorf("invalid range %q", base)
			}
			var err error
			start, err = parseValue(rangeParts[0])
			if err != nil {
				return 0, err
			}
			end, err = parseValue(rangeParts[1])
			if err != nil {
				return 0, err
			}
			if start > end {
				return 0, fmt.Errorf("range %q descends", base)
			}
		default:
			value, err := parseValue(base)
			if err != nil {
				return 0, err
			}
			start, end = value, value
			if len(parts) == 2 && value < 7 {
				end = 6
			}
		}
		for value := start; ; value += step {
			bits |= uint64(1) << (value % 7)
			if step > end-value {
				break
			}
		}
	}
	if bits == 0 {
		return 0, errors.New("field selects no days")
	}
	return bits, nil
}

func normalizeTaskScheduleCronMonthNamesV1(field string) string {
	replacer := strings.NewReplacer(
		"january", "jan", "february", "feb", "march", "mar", "april", "apr",
		"june", "jun", "july", "jul", "august", "aug", "september", "sep",
		"october", "oct", "november", "nov", "december", "dec",
	)
	return replacer.Replace(strings.ToLower(field))
}

func trimTaskScheduleCronBitsV1(bits uint64, minValue, maxValue int) uint64 {
	var trimmed uint64
	for value := minValue; value <= maxValue; value++ {
		if bits&(uint64(1)<<value) != 0 {
			trimmed |= uint64(1) << value
		}
	}
	return trimmed
}

func cronBitsToRanges(bits uint64, minValue, maxValue int) []client.ScheduleRange {
	ranges := make([]client.ScheduleRange, 0, maxValue-minValue+1)
	for value := minValue; value <= maxValue; {
		if bits&(uint64(1)<<value) == 0 {
			value++
			continue
		}
		end := value
		for end+1 <= maxValue && bits&(uint64(1)<<(end+1)) != 0 {
			end++
		}
		ranges = append(ranges, client.ScheduleRange{Start: value, End: end, Step: 1})
		value = end + 1
	}
	return ranges
}

func verifyTaskScheduleDescription(
	expected taskScheduleExpected,
	desc *workflowservice.DescribeScheduleResponse,
	operation string,
) (TaskScheduleSnapshot, error) {
	conflict := func(cause error) (TaskScheduleSnapshot, error) {
		return TaskScheduleSnapshot{}, newTaskScheduleError(TaskScheduleErrorConflict, operation, expected.taskID, cause)
	}
	if desc == nil {
		return conflict(errors.New("Describe returned nil description"))
	}
	if len(desc.GetConflictToken()) == 0 {
		return conflict(errors.New("Describe returned no schedule conflict token"))
	}
	schedule := desc.GetSchedule()
	if schedule == nil {
		return conflict(errors.New("Describe returned no schedule"))
	}
	if len(desc.GetMemo().GetFields()) != 0 || len(desc.GetSearchAttributes().GetIndexedFields()) != 0 {
		return conflict(errors.New("top-level schedule metadata does not match request"))
	}
	if !taskScheduleProtoSpecMatches(schedule.GetSpec(), expected.prepared.Timing) {
		return conflict(errors.New("schedule spec does not match request"))
	}
	policies := schedule.GetPolicies()
	if policies == nil ||
		policies.GetOverlapPolicy() != enums.ScheduleOverlapPolicy(expected.prepared.Policy.Overlap) ||
		!protoDurationMatches(policies.GetCatchupWindow(), time.Duration(expected.prepared.Policy.CatchupNanos)) ||
		policies.GetPauseOnFailure() != expected.prepared.Policy.PauseOnFailure ||
		policies.GetKeepOriginalWorkflowId() {
		return conflict(errors.New("schedule policy does not match request"))
	}
	stateDetails := schedule.GetState()
	if stateDetails == nil || stateDetails.GetLimitedActions() || stateDetails.GetRemainingActions() != 0 {
		return conflict(errors.New("schedule state limits do not match request"))
	}
	gotFingerprint, err := verifyTaskScheduleProtoAction(schedule.GetAction(), expected)
	if err != nil {
		return conflict(err)
	}
	lifecyclePhase := gotFingerprint.LifecyclePhase
	if lifecyclePhase != taskScheduleV1PhaseProvisioning && lifecyclePhase != taskScheduleV1PhaseActive {
		return conflict(fmt.Errorf("unsupported schedule lifecycle phase %q", lifecyclePhase))
	}
	gotFingerprint.LifecyclePhase = expected.fingerprint.LifecyclePhase
	if gotFingerprint != expected.fingerprint {
		return conflict(errors.New("schedule fingerprint does not match request"))
	}
	info := desc.GetInfo()
	if info == nil || info.GetInvalidScheduleError() != "" {
		return conflict(errors.New("schedule info is missing or reports an invalid schedule"))
	}

	used := info.GetActionCount() != 0 ||
		info.GetMissedCatchupWindow() != 0 ||
		info.GetOverlapSkipped() != 0 ||
		info.GetBufferDropped() != 0 || info.GetBufferSize() != 0 ||
		len(info.GetRunningWorkflows()) != 0 || len(info.GetRecentActions()) != 0
	state := TaskScheduleStateUnknown
	if stateDetails.GetPaused() {
		state = TaskSchedulePausedUsedExact
		if !used && lifecyclePhase == taskScheduleV1PhaseProvisioning &&
			stateDetails.GetNotes() == expected.prepared.Creation.Note {
			state = TaskSchedulePausedProvisioningExact
		}
	} else if lifecyclePhase == taskScheduleV1PhaseActive &&
		stateDetails.GetNotes() == expected.prepared.Action.ActivationNote {
		state = TaskScheduleActiveVirginExact
		if used {
			state = TaskScheduleActiveUsedExact
		}
	}
	return TaskScheduleSnapshot{
		TaskID:         expected.taskID,
		RequestDigest:  expected.fingerprint.RequestDigest,
		PreparedDigest: expected.fingerprint.PreparedDigest,
		Revision:       taskScheduleRevision(desc.GetConflictToken()),
		State:          state,
		NumActions:     int(info.GetActionCount()),
	}, nil
}

func taskScheduleRevision(conflictToken []byte) string {
	return base64.RawURLEncoding.EncodeToString(conflictToken)
}

func validateTaskScheduleActivationReceipt(
	expected taskScheduleExpected,
	receipt TaskScheduleSnapshot,
) error {
	if receipt.TaskID != expected.taskID ||
		receipt.RequestDigest != expected.fingerprint.RequestDigest ||
		receipt.PreparedDigest != expected.fingerprint.PreparedDigest {
		return errors.New("activation receipt does not belong to the prepared task")
	}
	if receipt.State != TaskSchedulePausedVirginExact || receipt.NumActions != 0 {
		return errors.New("activation receipt is not a paused virgin task")
	}
	if receipt.Revision == "" {
		return errors.New("activation receipt revision is empty")
	}
	if _, err := base64.RawURLEncoding.DecodeString(receipt.Revision); err != nil {
		return fmt.Errorf("activation receipt revision is invalid: %w", err)
	}
	return nil
}

func decodeTaskScheduleFingerprint(
	memo *commonpb.Memo,
	dc converter.DataConverter,
) (taskScheduleFingerprint, error) {
	if memo == nil || len(memo.Fields) != 1 {
		return taskScheduleFingerprint{}, errors.New("schedule memo must contain only the Vane fingerprint")
	}
	payload, ok := memo.Fields[taskScheduleMemoKey]
	if !ok || payload == nil {
		return taskScheduleFingerprint{}, errors.New("schedule fingerprint memo is missing")
	}
	var fingerprint taskScheduleFingerprint
	if dc == nil {
		return taskScheduleFingerprint{}, errors.New("task schedule data converter is unavailable")
	}
	if err := dc.FromPayload(payload, &fingerprint); err != nil {
		return taskScheduleFingerprint{}, fmt.Errorf("decode schedule fingerprint: %w", err)
	}
	return fingerprint, nil
}

func verifyTaskScheduleProtoAction(
	action *schedulepb.ScheduleAction,
	expected taskScheduleExpected,
) (taskScheduleFingerprint, error) {
	workflowAction := action.GetStartWorkflow()
	if workflowAction == nil {
		return taskScheduleFingerprint{}, errors.New("schedule action is not a workflow action")
	}
	taskQueue := workflowAction.GetTaskQueue()
	if workflowAction.GetWorkflowId() != expected.prepared.Action.ActionID ||
		workflowAction.GetWorkflowType().GetName() != expected.prepared.Action.WorkflowType ||
		taskQueue.GetName() != expected.taskQueue || taskQueue.GetNormalName() != "" ||
		(taskQueue.GetKind() != enums.TASK_QUEUE_KIND_UNSPECIFIED &&
			taskQueue.GetKind() != enums.TASK_QUEUE_KIND_NORMAL) {
		return taskScheduleFingerprint{}, errors.New("workflow identity or task queue does not match request")
	}
	if !protoDurationMatches(
		workflowAction.GetWorkflowExecutionTimeout(),
		time.Duration(expected.prepared.Action.WorkflowExecutionTimeoutNanos),
	) || !protoDurationMatches(
		workflowAction.GetWorkflowRunTimeout(),
		time.Duration(expected.prepared.Action.WorkflowRunTimeoutNanos),
	) || !protoDurationMatches(
		workflowAction.GetWorkflowTaskTimeout(),
		time.Duration(expected.prepared.Action.WorkflowTaskTimeoutNanos),
	) || (workflowAction.GetRetryPolicy() != nil) != expected.prepared.Action.HasRetryPolicy {
		return taskScheduleFingerprint{}, errors.New("workflow timeout or retry policy does not match request")
	}
	reusePolicy := workflowAction.GetWorkflowIdReusePolicy()
	if reusePolicy != enums.WORKFLOW_ID_REUSE_POLICY_UNSPECIFIED &&
		reusePolicy != enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE {
		return taskScheduleFingerprint{}, errors.New("workflow ID reuse policy does not match request")
	}
	priority := workflowAction.GetPriority()
	metadataMismatch := workflowAction.GetCronSchedule() != "" ||
		len(workflowAction.GetHeader().GetFields()) != 0 ||
		len(workflowAction.GetSearchAttributes().GetIndexedFields()) != 0 ||
		workflowAction.GetVersioningOverride() != nil ||
		workflowAction.GetUserMetadata().GetSummary() != nil ||
		workflowAction.GetUserMetadata().GetDetails() != nil ||
		priority.GetPriorityKey() != 0 || priority.GetFairnessKey() != "" || priority.GetFairnessWeight() != 0
	if metadataMismatch {
		return taskScheduleFingerprint{}, errors.New("workflow metadata does not match request")
	}
	actionDC, err := taskScheduleActionDataConverter(expected.prepared, expected.dc)
	if err != nil {
		return taskScheduleFingerprint{}, err
	}
	fingerprint, err := decodeTaskScheduleFingerprint(workflowAction.GetMemo(), actionDC)
	if err != nil {
		return taskScheduleFingerprint{}, err
	}
	inputs := workflowAction.GetInput().GetPayloads()
	if len(inputs) != 1 || inputs[0] == nil {
		return taskScheduleFingerprint{}, fmt.Errorf("workflow args count is %d, want one payload", len(inputs))
	}
	var got workflow.PushParams
	if err := actionDC.FromPayload(inputs[0], &got); err != nil {
		return taskScheduleFingerprint{}, fmt.Errorf("decode workflow args: %w", err)
	}
	matches := taskScheduleParamsEqual(got, expected.params)
	if !matches && expected.prepared.FingerprintVersion == taskScheduleFingerprintVersionV1 {
		// A response-lost v1 Create may have committed the old tenant-less
		// Action. It is still owned by the sealed v1 memo/request digest, and the
		// next activation update deterministically rewrites it to expected.params.
		matches = taskScheduleParamsEqual(got, expected.prepared.Action.Params)
	}
	if !matches {
		return taskScheduleFingerprint{}, errors.New("workflow args do not match request")
	}
	return fingerprint, nil
}

func taskScheduleParamsEqual(got, want workflow.PushParams) bool {
	return got.TenantID == want.TenantID && got.UserID == want.UserID &&
		got.RunKind == want.RunKind && got.ExecutionMode == want.ExecutionMode &&
		got.RuntimeVersion == want.RuntimeVersion && got.ScheduleID == want.ScheduleID &&
		got.NLDesc == want.NLDesc && got.Scope.TopN == want.Scope.TopN &&
		got.Snapshot == nil && want.Snapshot == nil &&
		slices.Equal(got.Scope.SourceIDs, want.Scope.SourceIDs)
}

func taskScheduleProtoSpecMatches(
	got *schedulepb.ScheduleSpec,
	expected PreparedTaskScheduleTiming,
) bool {
	if got == nil || got.GetTimezoneName() != expected.TimeZoneName || len(got.GetTimezoneData()) != 0 ||
		got.GetStartTime() != nil || got.GetEndTime() != nil ||
		(got.GetJitter() != nil && !protoDurationMatches(got.GetJitter(), 0)) ||
		len(got.GetCronString()) != 0 || len(got.GetCalendar()) != 0 ||
		len(got.GetExcludeCalendar()) != 0 || len(got.GetExcludeStructuredCalendar()) != 0 {
		return false
	}
	if expected.Calendar != nil {
		if len(got.GetStructuredCalendar()) != 1 || len(got.GetInterval()) != 0 {
			return false
		}
		calendar := got.GetStructuredCalendar()[0]
		if calendar == nil || calendar.GetComment() != "" || len(calendar.GetYear()) != 0 {
			return false
		}
		fields := []struct {
			got          []*schedulepb.Range
			expectedBits uint64
			min, max     int
		}{
			{calendar.GetSecond(), expected.Calendar.Second, 0, 59},
			{calendar.GetMinute(), expected.Calendar.Minute, 0, 59},
			{calendar.GetHour(), expected.Calendar.Hour, 0, 23},
			{calendar.GetDayOfMonth(), expected.Calendar.DayOfMonth, 1, 31},
			{calendar.GetMonth(), expected.Calendar.Month, 1, 12},
			{calendar.GetDayOfWeek(), expected.Calendar.DayOfWeek, 0, 6},
		}
		for _, field := range fields {
			bits, ok := protoScheduleRangesToBits(field.got, field.min, field.max)
			if !ok || bits != field.expectedBits {
				return false
			}
		}
		return true
	}
	if len(got.GetStructuredCalendar()) != 0 || len(got.GetInterval()) != 1 {
		return false
	}
	interval := got.GetInterval()[0]
	return interval != nil &&
		protoDurationMatches(interval.GetInterval(), time.Duration(expected.EveryNanos)) &&
		protoDurationMatches(interval.GetPhase(), time.Duration(expected.OffsetNanos))
}

func protoDurationMatches(got *durationpb.Duration, expected time.Duration) bool {
	if got == nil {
		return expected == 0
	}
	return got.CheckValid() == nil && got.AsDuration() == expected
}

func protoScheduleRangesToBits(ranges []*schedulepb.Range, minValue, maxValue int) (uint64, bool) {
	if len(ranges) == 0 {
		return 0, false
	}
	var bits uint64
	for _, scheduleRange := range ranges {
		if scheduleRange == nil {
			return 0, false
		}
		start := int(scheduleRange.GetStart())
		end := int(scheduleRange.GetEnd())
		if end < start {
			end = start
		}
		step := int(scheduleRange.GetStep())
		if step == 0 {
			step = 1
		}
		if step < 1 || start < minValue || end > maxValue {
			return 0, false
		}
		for value := start; ; value += step {
			bits |= uint64(1) << value
			if step > end-value {
				break
			}
		}
	}
	return bits, true
}

func (s *Scheduler) describeTaskScheduleForRecovery(
	ctx context.Context,
	expected taskScheduleExpected,
) (*workflowservice.DescribeScheduleResponse, error) {
	recoveryCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), taskScheduleRecoveryTimeout)
	defer cancel()
	return s.describeTaskSchedule(recoveryCtx, expected)
}

func (s *Scheduler) createTaskScheduleForRecovery(
	ctx context.Context,
	request *workflowservice.CreateScheduleRequest,
) (*workflowservice.CreateScheduleResponse, error) {
	recoveryCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), taskScheduleRecoveryTimeout)
	defer cancel()
	return s.c.WorkflowService().CreateSchedule(recoveryCtx, request)
}

func (s *Scheduler) describeTaskSchedule(
	ctx context.Context,
	expected taskScheduleExpected,
) (*workflowservice.DescribeScheduleResponse, error) {
	return s.c.WorkflowService().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
		Namespace:  expected.prepared.Namespace,
		ScheduleId: expected.taskID,
	})
}

func taskScheduleContextError(ctx context.Context, operation, taskID string) error {
	if ctx == nil {
		return newTaskScheduleError(TaskScheduleErrorInvalid, operation, taskID, errors.New("context is nil"))
	}
	if err := ctx.Err(); err != nil {
		return newTaskScheduleError(TaskScheduleErrorTransient, operation, taskID, err)
	}
	return nil
}

func classifyTaskScheduleReadError(operation, taskID string, err error) error {
	if isTaskScheduleNotFound(err) {
		return newTaskScheduleError(TaskScheduleErrorNotFound, operation, taskID, err)
	}
	return classifyTaskScheduleOperationError(operation, taskID, err)
}

func classifyTaskScheduleMutationError(operation, taskID string, err error) error {
	return classifyTaskScheduleOperationError(operation, taskID, err)
}

func classifyTaskScheduleOperationError(operation, taskID string, err error) error {
	if _, invalid := errors.AsType[*serviceerror.InvalidArgument](err); invalid || status.Code(err) == codes.InvalidArgument {
		return newTaskScheduleError(TaskScheduleErrorInvalid, operation, taskID, err)
	}
	if isTaskScheduleBlockedError(err) {
		return newTaskScheduleError(TaskScheduleErrorBlocked, operation, taskID, err)
	}
	if _, failed := errors.AsType[*serviceerror.FailedPrecondition](err); failed ||
		status.Code(err) == codes.FailedPrecondition {
		return newTaskScheduleError(TaskScheduleErrorConflict, operation, taskID, err)
	}
	if isTaskScheduleAlreadyExistsError(err) {
		return newTaskScheduleError(TaskScheduleErrorConflict, operation, taskID, err)
	}
	return newTaskScheduleError(TaskScheduleErrorTransient, operation, taskID, err)
}

func isTaskScheduleBlockedError(err error) bool {
	_, denied := errors.AsType[*serviceerror.PermissionDenied](err)
	_, namespaceMissing := errors.AsType[*serviceerror.NamespaceNotFound](err)
	_, namespaceInactive := errors.AsType[*serviceerror.NamespaceNotActive](err)
	_, namespaceInvalid := errors.AsType[*serviceerror.NamespaceInvalidState](err)
	_, clientUnsupported := errors.AsType[*serviceerror.ClientVersionNotSupported](err)
	_, serverUnsupported := errors.AsType[*serviceerror.ServerVersionNotSupported](err)
	_, unimplemented := errors.AsType[*serviceerror.Unimplemented](err)
	grpcCode := status.Code(err)
	return denied || namespaceMissing || namespaceInactive || namespaceInvalid ||
		clientUnsupported || serverUnsupported || unimplemented ||
		grpcCode == codes.Unauthenticated || grpcCode == codes.PermissionDenied ||
		grpcCode == codes.Unimplemented
}

func taskScheduleMutationDefinitelyRejected(err error) bool {
	if err == nil {
		return false
	}
	if _, invalid := errors.AsType[*serviceerror.InvalidArgument](err); invalid {
		return true
	}
	if _, failed := errors.AsType[*serviceerror.FailedPrecondition](err); failed {
		return true
	}
	grpcCode := status.Code(err)
	return isTaskScheduleBlockedError(err) || grpcCode == codes.InvalidArgument ||
		grpcCode == codes.FailedPrecondition
}

func isTaskScheduleAlreadyExistsError(err error) bool {
	_, alreadyExists := errors.AsType[*serviceerror.AlreadyExists](err)
	_, workflowStarted := errors.AsType[*serviceerror.WorkflowExecutionAlreadyStarted](err)
	return alreadyExists || workflowStarted || status.Code(err) == codes.AlreadyExists ||
		errors.Is(err, temporal.ErrScheduleAlreadyRunning)
}

func isTaskScheduleNotFound(err error) bool {
	_, ok := errors.AsType[*serviceerror.NotFound](err)
	return ok || status.Code(err) == codes.NotFound
}

func newTaskScheduleError(kind TaskScheduleErrorKind, operation, taskID string, cause error) error {
	inner := &TaskScheduleError{Kind: kind, Operation: operation, TaskID: taskID, Cause: cause}
	code := types.CodeInternal
	message := "Temporal task schedule operation failed"
	switch kind {
	case TaskScheduleErrorInvalid:
		code = types.CodeValidation
		message = "任务调度定义无效"
	case TaskScheduleErrorNotFound:
		code = types.CodeNotFound
		message = "任务调度不存在"
	case TaskScheduleErrorConflict, TaskScheduleErrorUnsafeState:
		code = types.CodeConflict
		message = "任务调度状态冲突"
	case TaskScheduleErrorBlocked:
		message = "Temporal 任务调度被环境或权限配置阻止"
	case TaskScheduleErrorOutcomeUnknown:
		message = "任务调度操作结果尚无法确认"
	case TaskScheduleErrorTransient:
		message = "Temporal 任务调度暂时不可用"
	}
	appErr := types.NewAppError(code, message, inner)
	if kind == TaskScheduleErrorOutcomeUnknown ||
		(kind == TaskScheduleErrorTransient && !errors.Is(cause, context.Canceled)) {
		appErr.Retryable = true
	}
	return appErr
}
