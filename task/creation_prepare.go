package task

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/YouToco/vane/internal/strictjson"
	"github.com/YouToco/vane/scheduler"
	"github.com/YouToco/vane/sourcespec"
	"github.com/YouToco/vane/types"
	"github.com/YouToco/vane/workflow"
)

const (
	creationCommandVersion      = "vane.create-schedule-command/v1"
	compiledDefinitionVersion   = "vane.compiled-task-definition/v1"
	maxCreationCommandBytes     = 64 << 10
	maxCreationOperationBytes   = 512
	maxCreationPlaybookRunes    = 4000
	maxCreationDescriptionRunes = 256
	maxCompiledFetchPlanBytes   = 256 << 10
	maxCompiledSources          = 64
	maxCompiledSourceURLBytes   = 4096
	maxCompiledSourceRunes      = 256
	defaultCreationTimeZone     = "Asia/Shanghai"
)

var (
	// ErrTranslationOutcomeAmbiguous marks the fail-closed state where a paid
	// compiler call may have happened but no immutable result was checkpointed.
	ErrTranslationOutcomeAmbiguous = errors.New("task: translation outcome is ambiguous")
	// ErrCreationCheckpointInvalid marks a corrupt, cross-scope, or internally
	// inconsistent durable checkpoint. Such bytes are never repaired in place.
	ErrCreationCheckpointInvalid = errors.New("task: creation checkpoint is invalid")
)

// CreationPrepareStore is the smallest durable operation surface needed by
// A4 preparation. The caller acquires the lease; this service never reads a
// lease token from storage and acts as its owner.
type CreationPrepareStore interface {
	LoadTaskCreationOperation(ctx context.Context, id string, tenantID, userID int64) (*types.TaskCreationOperation, error)
	SealTaskCreationCommand(ctx context.Context, lease types.TaskCreationLease, command []byte) error
	BeginTaskCreationTranslation(ctx context.Context, lease types.TaskCreationLease) (started bool, err error)
	CheckpointTaskCreationDefinition(ctx context.Context, lease types.TaskCreationLease, definition []byte, digest string) error
	CheckpointTaskCreationSchedule(ctx context.Context, lease types.TaskCreationLease, prepared []byte) error
	BlockTaskCreationOperation(ctx context.Context, lease types.TaskCreationLease, errorCode, errorMessage string) error
	FailTaskCreationOperation(ctx context.Context, lease types.TaskCreationLease, errorCode, errorMessage string) error
}

// TaskDefinitionCompileRequest is explicit about the scope of the one paid
// compiler call. Implementations must not infer identity from context values.
type TaskDefinitionCompileRequest struct {
	TenantID        int64
	UserID          int64
	OperationID     string
	PlaybookContent string
}

// TaskDefinitionCompiler translates one bounded playbook into an already
// materializable fetch_plan. It is intentionally independent from the legacy
// agent translator so A4 can enforce a single-authorization, fail-closed
// checkpoint protocol around a provider that has no idempotency key.
type TaskDefinitionCompiler interface {
	CompileTaskDefinition(ctx context.Context, req TaskDefinitionCompileRequest) (json.RawMessage, error)
}

// TaskSchedulePreparer is an unconnected adapter seam over A3. Its deliberately
// generic method name keeps the A3 zero-production-call-point guard intact;
// A5 owns the concrete scheduler adapter and production wiring.
type TaskSchedulePreparer interface {
	DeriveID(tenantID, userID int64, operationID string) (string, error)
	Prepare(ctx context.Context, req scheduler.TaskScheduleRequest) (scheduler.PreparedTaskSchedule, error)
}

// CreationPreparer performs the paid translation and the two immutable A4
// preparation checkpoints. It does not create a Temporal schedule and does not
// call InsertPausedCompiledTaskDefinition.
type CreationPreparer struct {
	store     CreationPrepareStore
	compiler  TaskDefinitionCompiler
	schedules TaskSchedulePreparer
}

// NewCreationPreparer constructs the zero-production-call-point A4 service.
func NewCreationPreparer(
	store CreationPrepareStore,
	compiler TaskDefinitionCompiler,
	schedules TaskSchedulePreparer,
) *CreationPreparer {
	return &CreationPreparer{store: store, compiler: compiler, schedules: schedules}
}

// CreationPrepareInput binds the exact leased operation. The user-approved
// args are deliberately absent: Prepare reads only the durable operation Args,
// so a caller cannot pair an unapproved command with a valid lease.
type CreationPrepareInput struct {
	TenantID    int64
	UserID      int64
	OperationID string
	Lease       types.TaskCreationLease
}

// CreationPrepareResult is the complete handoff from A4 to A5. Definition is
// the A2 aggregate with the pre-translation deterministic TaskID; A3's result
// must prove it derived the same ID before this value is returned.
type CreationPrepareResult struct {
	Definition       types.PausedCompiledTaskDefinition
	DefinitionDigest string
	Schedule         scheduler.PreparedTaskSchedule
}

type createScheduleCommandArgs struct {
	Spec          *createScheduleCommandSpec `json:"spec"`
	Intent        string                     `json:"intent"`
	NLDescription string                     `json:"nl_description"`
	Strictness    types.PushStrictness       `json:"strictness"`
}

type createScheduleCommandSpec struct {
	Cron         string `json:"cron"`
	EverySeconds int    `json:"every_seconds"`
	AnchorAt     string `json:"anchor_at"`
	TZ           string `json:"tz"`
}

type normalizedCreateScheduleCommand struct {
	Version       string                 `json:"version"`
	Spec          scheduler.ScheduleSpec `json:"spec"`
	Intent        string                 `json:"intent"`
	NLDescription string                 `json:"nl_description"`
	Strictness    types.PushStrictness   `json:"strictness,omitempty"`
}

type compiledTaskDefinitionCheckpoint struct {
	Version         string                 `json:"version"`
	TaskID          string                 `json:"task_id"`
	TenantID        int64                  `json:"tenant_id"`
	UserID          int64                  `json:"user_id"`
	OperationID     string                 `json:"operation_id"`
	CommandDigest   string                 `json:"command_digest"`
	Spec            scheduler.ScheduleSpec `json:"spec"`
	Scope           workflow.PushScope     `json:"scope"`
	NLDescription   string                 `json:"nl_description"`
	PlaybookContent string                 `json:"playbook_content"`
	FetchPlan       json.RawMessage        `json:"fetch_plan"`
	Strictness      types.PushStrictness   `json:"strictness,omitempty"`
}

type compiledFetchPlan struct {
	Sources []compiledFetchSource `json:"sources"`
}

type compiledFetchSource struct {
	Platform   string          `json:"platform"`
	Capability string          `json:"capability"`
	Title      string          `json:"title,omitempty"`
	URL        string          `json:"url"`
	Config     json.RawMessage `json:"config"`
}

// Prepare seals the canonical command, invokes the compiler at most once, and
// checkpoints both the compiled definition and A3 prepared schedule. Every
// recovery path validates exact scope, digest, canonical bytes, and TaskID.
func (p *CreationPreparer) Prepare(
	ctx context.Context,
	in CreationPrepareInput,
) (CreationPrepareResult, error) {
	if err := ctx.Err(); err != nil {
		return CreationPrepareResult{}, err
	}
	if p == nil || p.store == nil || p.compiler == nil || p.schedules == nil {
		return CreationPrepareResult{}, errors.New("task: creation preparer dependencies are incomplete")
	}
	if err := validateCreationPrepareIdentity(in); err != nil {
		return CreationPrepareResult{}, err
	}

	loaded, err := p.loadScopedOperation(ctx, in)
	if err != nil {
		return CreationPrepareResult{}, err
	}
	command, commandBytes, err := normalizeCreateScheduleCommand(loaded.Args)
	if err != nil {
		cause := fmt.Errorf("normalize durable create_schedule command: %w", err)
		if len(loaded.NormalizedCommand) != 0 ||
			creationPhaseAtLeast(loaded.Phase, types.TaskCreationPhaseCommandSealed) {
			return CreationPrepareResult{}, p.blockInvalidCheckpoint(
				ctx, in.Lease, loaded.Phase, cause,
			)
		}
		return CreationPrepareResult{}, p.failKnownFailure(
			ctx, in.Lease, "command_invalid",
			"已确认的任务参数无效，未创建任务", cause,
		)
	}
	if err := p.store.SealTaskCreationCommand(ctx, in.Lease, commandBytes); err != nil {
		if errors.Is(err, types.ErrConflict) {
			return CreationPrepareResult{}, p.blockInvalidCheckpoint(
				ctx, in.Lease, loaded.Phase,
				fmt.Errorf("sealed command conflicts with approved command: %w", err),
			)
		}
		return CreationPrepareResult{}, fmt.Errorf("seal task creation command: %w", err)
	}

	op, err := p.loadOperation(ctx, in, commandBytes)
	if err != nil {
		if errors.Is(err, ErrCreationCheckpointInvalid) {
			return CreationPrepareResult{}, p.blockInvalidCheckpoint(
				ctx, in.Lease, loaded.Phase, err,
			)
		}
		return CreationPrepareResult{}, err
	}
	if len(op.PreparedSchedule) != 0 || len(op.CompiledDefinition) != 0 || op.CompiledDigest != "" {
		return p.resumeFromCheckpoints(ctx, in, command, commandBytes, op)
	}
	if op.Phase == types.TaskCreationPhaseTranslationStarted {
		if op.Attempt <= 1 {
			return CreationPrepareResult{}, fmt.Errorf(
				"%w: task definition translation may still be running",
				types.ErrTaskCreationBusy,
			)
		}
		return CreationPrepareResult{}, p.blockAmbiguous(ctx, in.Lease, nil)
	}
	if creationPhaseAtLeast(op.Phase, types.TaskCreationPhaseDefinitionCompiled) {
		return CreationPrepareResult{}, p.blockInvalidCheckpoint(
			ctx, in.Lease, op.Phase,
			errors.New("operation phase requires a compiled checkpoint"),
		)
	}
	taskID, err := p.schedules.DeriveID(in.TenantID, in.UserID, in.OperationID)
	if err != nil {
		return CreationPrepareResult{}, p.failKnownFailure(
			ctx, in.Lease, "task_id_invalid", "任务标识生成失败，未创建任务",
			fmt.Errorf("derive deterministic task ID: %w", err),
		)
	}
	if strings.TrimSpace(taskID) == "" || strings.TrimSpace(taskID) != taskID {
		return CreationPrepareResult{}, p.failKnownFailure(
			ctx, in.Lease, "task_id_invalid", "任务标识生成失败，未创建任务",
			errors.New("derived task ID is invalid"),
		)
	}

	started, err := p.store.BeginTaskCreationTranslation(ctx, in.Lease)
	if err != nil {
		// A commit error has an unknowable outcome. Calling the paid compiler
		// here would make an applied-but-response-lost Begin charge twice.
		return CreationPrepareResult{}, fmt.Errorf("begin task definition translation: %w", err)
	}
	if !started {
		op, loadErr := p.loadOperation(ctx, in, commandBytes)
		if loadErr != nil {
			return CreationPrepareResult{}, loadErr
		}
		if len(op.CompiledDefinition) != 0 || len(op.PreparedSchedule) != 0 {
			return p.resumeFromCheckpoints(ctx, in, command, commandBytes, op)
		}
		if op.Phase == types.TaskCreationPhaseTranslationStarted && op.Attempt <= 1 {
			return CreationPrepareResult{}, fmt.Errorf(
				"%w: task definition translation may still be running",
				types.ErrTaskCreationBusy,
			)
		}
		return CreationPrepareResult{}, p.blockAmbiguous(ctx, in.Lease, nil)
	}

	playbook := command.Intent
	plan, err := p.compiler.CompileTaskDefinition(ctx, TaskDefinitionCompileRequest{
		TenantID: in.TenantID, UserID: in.UserID, OperationID: in.OperationID,
		PlaybookContent: playbook,
	})
	if err != nil {
		// The compiler interface has no proof that an error happened before the
		// provider accepted and charged the request. Treat every missing result
		// as ambiguous; the same OperationID is never automatically translated
		// again, even for a timeout or cancellation.
		return CreationPrepareResult{}, p.blockAmbiguous(
			ctx, in.Lease, fmt.Errorf("compile task definition: %w", err),
		)
	}
	canonicalPlan, err := canonicalizeFetchPlan(plan)
	if err != nil {
		cause := fmt.Errorf("validate compiled fetch plan: %w", err)
		return CreationPrepareResult{}, p.failKnownFailure(
			ctx, in.Lease, "compiled_definition_invalid", "编译结果未通过安全校验，未创建任务", cause,
		)
	}

	compiled := compiledTaskDefinitionCheckpoint{
		Version:         compiledDefinitionVersion,
		TaskID:          taskID,
		TenantID:        in.TenantID,
		UserID:          in.UserID,
		OperationID:     in.OperationID,
		CommandDigest:   sha256Hex(commandBytes),
		Spec:            command.Spec,
		Scope:           workflow.PushScope{},
		NLDescription:   command.NLDescription,
		PlaybookContent: playbook,
		FetchPlan:       canonicalPlan,
		Strictness:      command.Strictness,
	}
	compiledBytes, err := json.Marshal(compiled)
	if err != nil {
		return CreationPrepareResult{}, fmt.Errorf("marshal compiled task definition: %w", err)
	}
	digest := sha256Hex(compiledBytes)
	if err := p.store.CheckpointTaskCreationDefinition(ctx, in.Lease, compiledBytes, digest); err != nil {
		return CreationPrepareResult{}, fmt.Errorf("checkpoint compiled task definition: %w", err)
	}

	return p.prepareSchedule(ctx, in, compiled, compiledBytes, digest)
}

func (p *CreationPreparer) resumeFromCheckpoints(
	ctx context.Context,
	in CreationPrepareInput,
	command normalizedCreateScheduleCommand,
	commandBytes []byte,
	op *types.TaskCreationOperation,
) (CreationPrepareResult, error) {
	if len(op.CompiledDefinition) == 0 || op.CompiledDigest == "" {
		return CreationPrepareResult{}, p.blockInvalidCheckpoint(
			ctx, in.Lease, op.Phase, errors.New("compiled definition checkpoint is incomplete"),
		)
	}
	compiled, err := validateCompiledCheckpoint(in, command, commandBytes, op)
	if err != nil {
		return CreationPrepareResult{}, p.blockInvalidCheckpoint(ctx, in.Lease, op.Phase, err)
	}
	if len(op.PreparedSchedule) == 0 {
		if creationPhaseAtLeast(op.Phase, types.TaskCreationPhaseSchedulePrepared) {
			return CreationPrepareResult{}, p.blockInvalidCheckpoint(
				ctx, in.Lease, op.Phase, errors.New("prepared schedule checkpoint is missing"),
			)
		}
		return p.prepareSchedule(
			ctx, in, compiled, op.CompiledDefinition, op.CompiledDigest,
		)
	}
	if !creationPhaseAtLeast(op.Phase, types.TaskCreationPhaseSchedulePrepared) {
		return CreationPrepareResult{}, p.blockInvalidCheckpoint(
			ctx, in.Lease, op.Phase,
			errors.New("prepared schedule checkpoint precedes schedule_prepared phase"),
		)
	}

	prepared, err := decodeAndValidatePreparedSchedule(
		op.PreparedSchedule, in, compiled, op.CompiledDigest,
	)
	if err != nil {
		return CreationPrepareResult{}, p.blockInvalidCheckpoint(ctx, in.Lease, op.Phase, err)
	}
	if op.TaskID != "" && op.TaskID != prepared.TaskID {
		return CreationPrepareResult{}, p.blockInvalidCheckpoint(
			ctx, in.Lease, op.Phase, errors.New("operation task ID differs from prepared schedule"),
		)
	}
	return makeCreationPrepareResult(compiled, op.CompiledDigest, prepared)
}

func (p *CreationPreparer) prepareSchedule(
	ctx context.Context,
	in CreationPrepareInput,
	compiled compiledTaskDefinitionCheckpoint,
	compiledBytes []byte,
	digest string,
) (CreationPrepareResult, error) {
	if sha256Hex(compiledBytes) != digest {
		return CreationPrepareResult{}, p.blockInvalidCheckpoint(
			ctx, in.Lease, types.TaskCreationPhaseDefinitionCompiled,
			errors.New("compiled definition digest differs before schedule preparation"),
		)
	}
	prepared, err := p.schedules.Prepare(ctx, scheduler.TaskScheduleRequest{
		TenantID: in.TenantID, UserID: in.UserID, OperationID: in.OperationID,
		Spec: compiled.Spec, Scope: workflow.PushScope{},
		NLDescription: compiled.NLDescription, PreparedDigest: digest,
	})
	if err != nil {
		switch {
		case errors.Is(err, scheduler.ErrTaskScheduleInvalid):
			return CreationPrepareResult{}, p.failKnownFailure(
				ctx, in.Lease, "schedule_prepare_invalid",
				"任务调度定义未通过校验，未创建任务",
				fmt.Errorf("prepare task schedule: %w", err),
			)
		case errors.Is(err, scheduler.ErrTaskScheduleBlocked),
			errors.Is(err, scheduler.ErrTaskScheduleNotFound):
			return CreationPrepareResult{}, p.blockKnownFailure(
				ctx, in.Lease, "schedule_prepare_blocked",
				"任务调度环境未就绪，已安全停止创建",
				fmt.Errorf("prepare task schedule: %w", err),
			)
		case errors.Is(err, scheduler.ErrTaskScheduleConflict),
			errors.Is(err, scheduler.ErrTaskScheduleUnsafeState):
			return CreationPrepareResult{}, p.blockInvalidCheckpoint(
				ctx, in.Lease, types.TaskCreationPhaseDefinitionCompiled,
				fmt.Errorf("prepare task schedule returned an impossible state: %w", err),
			)
		}
		return CreationPrepareResult{}, fmt.Errorf("prepare task schedule: %w", err)
	}
	preparedBytes, err := json.Marshal(prepared)
	if err != nil {
		return CreationPrepareResult{}, fmt.Errorf("marshal prepared task schedule: %w", err)
	}
	prepared, err = decodeAndValidatePreparedSchedule(preparedBytes, in, compiled, digest)
	if err != nil {
		return CreationPrepareResult{}, p.blockInvalidCheckpoint(
			ctx, in.Lease, types.TaskCreationPhaseDefinitionCompiled, err,
		)
	}
	if err := p.store.CheckpointTaskCreationSchedule(ctx, in.Lease, preparedBytes); err != nil {
		return CreationPrepareResult{}, fmt.Errorf("checkpoint prepared task schedule: %w", err)
	}
	return makeCreationPrepareResult(compiled, digest, prepared)
}

func (p *CreationPreparer) loadOperation(
	ctx context.Context,
	in CreationPrepareInput,
	command []byte,
) (*types.TaskCreationOperation, error) {
	op, err := p.loadScopedOperation(ctx, in)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(op.NormalizedCommand, command) {
		return nil, fmt.Errorf("%w: sealed command differs", ErrCreationCheckpointInvalid)
	}
	return op, nil
}

func (p *CreationPreparer) loadScopedOperation(
	ctx context.Context,
	in CreationPrepareInput,
) (*types.TaskCreationOperation, error) {
	op, err := p.store.LoadTaskCreationOperation(ctx, in.OperationID, in.TenantID, in.UserID)
	if err != nil {
		return nil, fmt.Errorf("load task creation operation: %w", err)
	}
	if op == nil {
		return nil, fmt.Errorf("%w: operation load returned nil", ErrCreationCheckpointInvalid)
	}
	if op.ID != in.OperationID || op.TenantID != in.TenantID || op.UserID != in.UserID {
		return nil, fmt.Errorf("%w: operation scope differs", ErrCreationCheckpointInvalid)
	}
	if op.Lease() != in.Lease {
		return nil, fmt.Errorf("%w: loaded operation lease differs", types.ErrTaskCreationLeaseLost)
	}
	if op.ExecutionVersion != types.TaskCreationExecutionVersionV1 || op.ToolName != "create_schedule" {
		return nil, fmt.Errorf("%w: operation protocol differs", ErrCreationCheckpointInvalid)
	}
	if creationPhaseRank(op.Phase) == 0 {
		return nil, fmt.Errorf("%w: operation phase is unknown", ErrCreationCheckpointInvalid)
	}
	if op.Attempt <= 0 || op.Fence <= 0 {
		return nil, fmt.Errorf("%w: operation lease counters are invalid", ErrCreationCheckpointInvalid)
	}
	if op.Status != types.PendingActionStatusExecuting || creationPhaseTerminal(op.Phase) {
		return nil, fmt.Errorf("%w: operation is terminal", types.ErrTaskCreationTerminal)
	}
	return op, nil
}

func validateCreationPrepareIdentity(in CreationPrepareInput) error {
	if in.TenantID <= 0 || in.UserID <= 0 {
		return errors.New("task: tenant_id and user_id must be positive")
	}
	if in.OperationID == "" || strings.TrimSpace(in.OperationID) != in.OperationID ||
		!utf8.ValidString(in.OperationID) || len(in.OperationID) > maxCreationOperationBytes {
		return errors.New("task: operation_id is invalid")
	}
	if in.Lease.ID != in.OperationID || in.Lease.TenantID != in.TenantID ||
		in.Lease.UserID != in.UserID || strings.TrimSpace(in.Lease.LeaseOwner) == "" ||
		in.Lease.Fence <= 0 {
		return fmt.Errorf("%w: input identity differs from lease", types.ErrTaskCreationLeaseLost)
	}
	return nil
}

func normalizeCreateScheduleCommand(
	raw json.RawMessage,
) (normalizedCreateScheduleCommand, []byte, error) {
	if len(raw) == 0 || len(raw) > maxCreationCommandBytes || !utf8.Valid(raw) {
		return normalizedCreateScheduleCommand{}, nil, errors.New("task: create_schedule args are invalid")
	}
	var args *createScheduleCommandArgs
	if err := decodeStrictJSON(raw, &args); err != nil {
		return normalizedCreateScheduleCommand{}, nil,
			fmt.Errorf("task: decode create_schedule args: %w", err)
	}
	if args == nil || args.Spec == nil {
		return normalizedCreateScheduleCommand{}, nil, errors.New("task: create_schedule spec is required")
	}
	intent := strings.TrimSpace(args.Intent)
	if intent == "" {
		return normalizedCreateScheduleCommand{}, nil,
			errors.New("task: approved intent must be non-empty")
	}
	if utf8.RuneCountInString(intent) > maxCreationPlaybookRunes {
		return normalizedCreateScheduleCommand{}, nil,
			fmt.Errorf("task: approved intent exceeds %d characters", maxCreationPlaybookRunes)
	}
	if args.Strictness != "" && !args.Strictness.Valid() {
		return normalizedCreateScheduleCommand{}, nil,
			errors.New("task: strictness must be empty, loose, normal, or strict")
	}
	spec, err := normalizeCreationScheduleSpec(*args.Spec)
	if err != nil {
		return normalizedCreateScheduleCommand{}, nil, err
	}
	description := strings.TrimSpace(args.NLDescription)
	if description == "" {
		description = truncateCreationRunes(intent, maxCreationDescriptionRunes)
	}
	if utf8.RuneCountInString(description) > maxCreationDescriptionRunes {
		return normalizedCreateScheduleCommand{}, nil,
			fmt.Errorf("task: nl_description exceeds %d characters", maxCreationDescriptionRunes)
	}
	command := normalizedCreateScheduleCommand{
		Version: creationCommandVersion, Spec: spec,
		Intent: intent, NLDescription: description, Strictness: args.Strictness,
	}
	canonical, err := json.Marshal(command)
	if err != nil {
		return normalizedCreateScheduleCommand{}, nil,
			fmt.Errorf("task: marshal normalized create_schedule command: %w", err)
	}
	return command, canonical, nil
}

func normalizeCreationScheduleSpec(in createScheduleCommandSpec) (scheduler.ScheduleSpec, error) {
	cronExpression := strings.Join(strings.Fields(strings.ToLower(in.Cron)), " ")
	anchor := strings.TrimSpace(in.AnchorAt)
	tz := strings.TrimSpace(in.TZ)
	if tz == "" {
		tz = defaultCreationTimeZone
	}
	if anchor != "" {
		parsed, err := time.Parse(time.RFC3339, anchor)
		if err != nil {
			return scheduler.ScheduleSpec{}, fmt.Errorf("task: anchor_at must be RFC3339: %w", err)
		}
		if parsed.Nanosecond() != 0 {
			return scheduler.ScheduleSpec{}, errors.New("task: anchor_at must use whole-second precision")
		}
		anchor = parsed.Format(time.RFC3339)
	}
	spec := scheduler.ScheduleSpec{
		Cron:         cronExpression,
		EverySeconds: in.EverySeconds, AnchorAt: anchor, TZ: tz,
	}
	if err := scheduler.ValidateTaskScheduleSpec(spec); err != nil {
		return scheduler.ScheduleSpec{}, fmt.Errorf("task: invalid schedule spec: %w", err)
	}
	return spec, nil
}

func canonicalizeFetchPlan(raw json.RawMessage) (json.RawMessage, error) {
	if len(raw) == 0 || len(raw) > maxCompiledFetchPlanBytes || !utf8.Valid(raw) {
		return nil, errors.New("fetch_plan size or encoding is invalid")
	}
	var plan *compiledFetchPlan
	if err := decodeStrictJSON(raw, &plan); err != nil {
		return nil, err
	}
	if plan == nil || len(plan.Sources) == 0 {
		return nil, errors.New("fetch_plan.sources must be non-empty")
	}
	if len(plan.Sources) > maxCompiledSources {
		return nil, fmt.Errorf("fetch_plan.sources exceeds %d entries", maxCompiledSources)
	}
	seenURLs := make(map[string]struct{}, len(plan.Sources))
	for i := range plan.Sources {
		source := &plan.Sources[i]
		if strings.TrimSpace(source.Platform) == "" || strings.TrimSpace(source.Platform) != source.Platform ||
			utf8.RuneCountInString(source.Platform) > maxCompiledSourceRunes {
			return nil, fmt.Errorf("fetch_plan.sources[%d].platform is invalid", i)
		}
		if strings.TrimSpace(source.Capability) == "" || strings.TrimSpace(source.Capability) != source.Capability ||
			utf8.RuneCountInString(source.Capability) > maxCompiledSourceRunes {
			return nil, fmt.Errorf("fetch_plan.sources[%d].capability is invalid", i)
		}
		if strings.TrimSpace(source.Title) != source.Title ||
			utf8.RuneCountInString(source.Title) > maxCompiledSourceRunes {
			return nil, fmt.Errorf("fetch_plan.sources[%d].title is invalid", i)
		}
		if strings.TrimSpace(source.URL) == "" || strings.TrimSpace(source.URL) != source.URL ||
			len(source.URL) > maxCompiledSourceURLBytes {
			return nil, fmt.Errorf("fetch_plan.sources[%d].url is invalid", i)
		}
		if _, duplicate := seenURLs[source.URL]; duplicate {
			return nil, fmt.Errorf("fetch_plan.sources[%d].url is duplicated", i)
		}
		seenURLs[source.URL] = struct{}{}
		config := source.Config
		if len(bytes.TrimSpace(config)) == 0 {
			config = json.RawMessage(`{}`)
		}
		canonical, err := canonicalJSONObject(config)
		if err != nil {
			return nil, fmt.Errorf("fetch_plan.sources[%d].config: %w", i, err)
		}
		source.Config = canonical
		candidate := &types.Source{
			Platform: types.Platform(source.Platform), Capability: types.Capability(source.Capability),
			Title: source.Title, URL: source.URL, Config: source.Config,
			Status: types.SourceStatusActive,
		}
		if message := sourcespec.ValidateMaterialized(candidate); message != "" {
			return nil, fmt.Errorf("fetch_plan.sources[%d] is not materializable: %s", i, message)
		}
	}
	canonical, err := json.Marshal(plan)
	if err != nil {
		return nil, fmt.Errorf("marshal canonical fetch_plan: %w", err)
	}
	if len(canonical) > maxCompiledFetchPlanBytes {
		return nil, errors.New("canonical fetch_plan exceeds the size limit")
	}
	return canonical, nil
}

func canonicalJSONObject(raw json.RawMessage) (json.RawMessage, error) {
	var object map[string]any
	if err := strictjson.Decode(raw, &object); err != nil {
		return nil, err
	}
	if object == nil {
		return nil, errors.New("must be a non-null JSON object")
	}
	canonical, err := json.Marshal(object)
	if err != nil {
		return nil, err
	}
	return canonical, nil
}

func validateCompiledCheckpoint(
	in CreationPrepareInput,
	command normalizedCreateScheduleCommand,
	commandBytes []byte,
	op *types.TaskCreationOperation,
) (compiledTaskDefinitionCheckpoint, error) {
	if !creationPhaseAtLeast(op.Phase, types.TaskCreationPhaseDefinitionCompiled) {
		return compiledTaskDefinitionCheckpoint{}, errors.New("compiled bytes precede definition_compiled phase")
	}
	if !validLowerSHA256(op.CompiledDigest) || sha256Hex(op.CompiledDefinition) != op.CompiledDigest {
		return compiledTaskDefinitionCheckpoint{}, errors.New("compiled definition digest differs")
	}
	var compiled *compiledTaskDefinitionCheckpoint
	if err := decodeStrictJSON(op.CompiledDefinition, &compiled); err != nil {
		return compiledTaskDefinitionCheckpoint{}, fmt.Errorf("decode compiled definition: %w", err)
	}
	if compiled == nil {
		return compiledTaskDefinitionCheckpoint{}, errors.New("compiled definition is null")
	}
	canonical, err := json.Marshal(compiled)
	if err != nil || !bytes.Equal(canonical, op.CompiledDefinition) {
		return compiledTaskDefinitionCheckpoint{}, errors.New("compiled definition bytes are not canonical")
	}
	if compiled.Version != compiledDefinitionVersion || compiled.TenantID != in.TenantID ||
		compiled.UserID != in.UserID || compiled.OperationID != in.OperationID {
		return compiledTaskDefinitionCheckpoint{}, errors.New("compiled definition scope differs")
	}
	if strings.TrimSpace(compiled.TaskID) == "" || strings.TrimSpace(compiled.TaskID) != compiled.TaskID {
		return compiledTaskDefinitionCheckpoint{}, errors.New("compiled definition task ID is invalid")
	}
	if compiled.CommandDigest != sha256Hex(commandBytes) || compiled.Spec != command.Spec ||
		compiled.NLDescription != command.NLDescription || compiled.Strictness != command.Strictness ||
		compiled.PlaybookContent != command.Intent {
		return compiledTaskDefinitionCheckpoint{}, errors.New("compiled definition differs from sealed command")
	}
	if compiled.Scope.TopN != 0 || len(compiled.Scope.SourceIDs) != 0 {
		return compiledTaskDefinitionCheckpoint{}, errors.New("compiled definition scope must be explicitly empty")
	}
	plan, err := canonicalizeFetchPlan(compiled.FetchPlan)
	if err != nil || !bytes.Equal(plan, compiled.FetchPlan) {
		return compiledTaskDefinitionCheckpoint{}, errors.New("compiled fetch_plan is not canonical and valid")
	}
	return *compiled, nil
}

func decodeAndValidatePreparedSchedule(
	raw []byte,
	in CreationPrepareInput,
	compiled compiledTaskDefinitionCheckpoint,
	digest string,
) (scheduler.PreparedTaskSchedule, error) {
	var prepared *scheduler.PreparedTaskSchedule
	if err := decodeStrictJSON(raw, &prepared); err != nil {
		return scheduler.PreparedTaskSchedule{}, fmt.Errorf("decode prepared schedule: %w", err)
	}
	if prepared == nil {
		return scheduler.PreparedTaskSchedule{}, errors.New("prepared schedule is null")
	}
	canonical, err := json.Marshal(prepared)
	if err != nil || !bytes.Equal(canonical, raw) {
		return scheduler.PreparedTaskSchedule{}, errors.New("prepared schedule bytes are not canonical")
	}
	request := scheduler.TaskScheduleRequest{
		TenantID: in.TenantID, UserID: in.UserID, OperationID: in.OperationID,
		Spec: compiled.Spec, Scope: workflow.PushScope{},
		NLDescription: compiled.NLDescription, PreparedDigest: digest,
	}
	if err := scheduler.ValidatePreparedTaskScheduleRequest(*prepared, request); err != nil {
		return scheduler.PreparedTaskSchedule{}, fmt.Errorf("validate prepared schedule: %w", err)
	}
	if prepared.TaskID != compiled.TaskID {
		return scheduler.PreparedTaskSchedule{}, errors.New("prepared schedule task ID differs from frozen definition")
	}
	return *prepared, nil
}

func makeCreationPrepareResult(
	compiled compiledTaskDefinitionCheckpoint,
	digest string,
	prepared scheduler.PreparedTaskSchedule,
) (CreationPrepareResult, error) {
	specJSON, err := json.Marshal(compiled.Spec)
	if err != nil {
		return CreationPrepareResult{}, fmt.Errorf("marshal compiled schedule spec: %w", err)
	}
	scopeJSON, err := json.Marshal(workflow.PushScope{})
	if err != nil {
		return CreationPrepareResult{}, fmt.Errorf("marshal compiled task scope: %w", err)
	}
	return CreationPrepareResult{
		Definition: types.PausedCompiledTaskDefinition{
			TaskID: compiled.TaskID, TenantID: compiled.TenantID, UserID: compiled.UserID,
			NLDescription:   compiled.NLDescription,
			SpecJSON:        append(json.RawMessage(nil), specJSON...),
			ScopeJSON:       append(json.RawMessage(nil), scopeJSON...),
			PlaybookContent: compiled.PlaybookContent,
			FetchPlan:       append(json.RawMessage(nil), compiled.FetchPlan...),
			Strictness:      compiled.Strictness,
		},
		DefinitionDigest: digest,
		Schedule:         prepared,
	}, nil
}

func (p *CreationPreparer) blockAmbiguous(
	ctx context.Context,
	lease types.TaskCreationLease,
	cause error,
) error {
	if cause == nil {
		cause = ErrTranslationOutcomeAmbiguous
	} else {
		cause = errors.Join(ErrTranslationOutcomeAmbiguous, cause)
	}
	return p.blockKnownFailure(
		ctx, lease, "translation_outcome_ambiguous",
		"任务定义翻译结果不确定，已停止创建以避免重复扣费",
		cause,
	)
}

func (p *CreationPreparer) blockInvalidCheckpoint(
	ctx context.Context,
	lease types.TaskCreationLease,
	phase types.TaskCreationPhase,
	cause error,
) error {
	wrapped := fmt.Errorf("%w: %w", ErrCreationCheckpointInvalid, cause)
	if creationPhaseAtLeast(phase, types.TaskCreationPhaseScheduleEnsured) {
		// A5 may already own a paused Temporal object. Only its cleanup path may
		// tombstone this operation; blocking here would strand that object.
		return wrapped
	}
	return p.blockKnownFailure(
		ctx, lease, "checkpoint_invalid", "任务创建检查点校验失败，已安全停止",
		wrapped,
	)
}

func (p *CreationPreparer) blockKnownFailure(
	ctx context.Context,
	lease types.TaskCreationLease,
	code string,
	message string,
	cause error,
) error {
	if err := p.store.BlockTaskCreationOperation(ctx, lease, code, message); err != nil {
		return errors.Join(cause, fmt.Errorf("block task creation operation: %w", err))
	}
	return cause
}

func (p *CreationPreparer) failKnownFailure(
	ctx context.Context,
	lease types.TaskCreationLease,
	code string,
	message string,
	cause error,
) error {
	if err := p.store.FailTaskCreationOperation(ctx, lease, code, message); err != nil {
		return errors.Join(cause, fmt.Errorf("fail task creation operation: %w", err))
	}
	return cause
}

func decodeStrictJSON(raw []byte, dst any) error {
	return strictjson.Decode(raw, dst)
}

func truncateCreationRunes(value string, maximum int) string {
	runes := []rune(value)
	if len(runes) <= maximum {
		return value
	}
	return string(runes[:maximum])
}

func sha256Hex(value []byte) string {
	digest := sha256.Sum256(value)
	return hex.EncodeToString(digest[:])
}

func validLowerSHA256(value string) bool {
	if len(value) != sha256.Size*2 || value != strings.ToLower(value) {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size
}

func creationPhaseTerminal(phase types.TaskCreationPhase) bool {
	switch phase {
	case types.TaskCreationPhaseCompleted,
		types.TaskCreationPhaseBlocked,
		types.TaskCreationPhaseFailed:
		return true
	default:
		return false
	}
}

func creationPhaseAtLeast(current, target types.TaskCreationPhase) bool {
	return creationPhaseRank(current) >= creationPhaseRank(target)
}

func creationPhaseRank(phase types.TaskCreationPhase) int {
	switch phase {
	case types.TaskCreationPhaseClaimed:
		return 1
	case types.TaskCreationPhaseCommandSealed:
		return 2
	case types.TaskCreationPhaseTranslationStarted:
		return 3
	case types.TaskCreationPhaseDefinitionCompiled:
		return 4
	case types.TaskCreationPhaseSchedulePrepared:
		return 5
	case types.TaskCreationPhaseScheduleEnsured:
		return 6
	case types.TaskCreationPhaseDefinitionCommitted:
		return 7
	case types.TaskCreationPhaseActivationStarted:
		return 8
	case types.TaskCreationPhaseActivated:
		return 9
	case types.TaskCreationPhaseCleanupPending:
		return 10
	case types.TaskCreationPhaseCompleted,
		types.TaskCreationPhaseBlocked,
		types.TaskCreationPhaseFailed:
		return 11
	default:
		return 0
	}
}
