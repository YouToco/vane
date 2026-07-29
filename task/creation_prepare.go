package task

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/YouToco/vane/acquisitiontool"
	"github.com/YouToco/vane/internal/strictjson"
	"github.com/YouToco/vane/observation"
	"github.com/YouToco/vane/scheduler"
	"github.com/YouToco/vane/types"
	"github.com/YouToco/vane/workflow"
)

const (
	creationCommandVersion      = "vane.create-schedule-command/v1"
	compiledDefinitionVersionV1 = "vane.compiled-task-definition/v1"
	compiledDefinitionVersion   = "vane.compiled-task-definition/v2"
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
	// ErrCreationCheckpointInvalid marks a corrupt, cross-scope, or internally
	// inconsistent durable checkpoint. Such bytes are never repaired in place.
	ErrCreationCheckpointInvalid = errors.New("task: creation checkpoint is invalid")
)

// CreationPrepareStore is the smallest durable operation surface needed by
// task-creation preparation. The caller acquires the lease; this service never reads a
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

// TaskSchedulePreparer is the narrow adapter seam over the schedule control
// plane. CreationCoordinator owns its sole production wiring.
type TaskSchedulePreparer interface {
	DeriveID(tenantID, userID int64, operationID string) (string, error)
	Prepare(ctx context.Context, req scheduler.TaskScheduleRequest) (scheduler.PreparedTaskSchedule, error)
}

// CreationPreparer deterministically validates and freezes the user-approved
// fetch plan into two immutable preparation checkpoints. It never calls
// an LLM, discovers a source, creates a Temporal schedule, or writes the A2
// aggregate. Discovery must happen before the durable operation is created.
type CreationPreparer struct {
	store     CreationPrepareStore
	schedules TaskSchedulePreparer
}

// NewCreationPreparer constructs the deterministic preparation service.
func NewCreationPreparer(
	store CreationPrepareStore,
	schedules TaskSchedulePreparer,
) *CreationPreparer {
	return &CreationPreparer{store: store, schedules: schedules}
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

// CreationPrepareResult is the complete handoff to the creation saga. Definition is
// the A2 aggregate with the deterministic TaskID; the schedule-control result
// must prove it derived the same ID before this value is returned.
type CreationPrepareResult struct {
	Definition       types.PausedCompiledTaskDefinition
	DefinitionDigest string
	Schedule         scheduler.PreparedTaskSchedule
}

type createScheduleCommandArgs struct {
	Spec              *createScheduleCommandSpec `json:"spec"`
	Intent            string                     `json:"intent"`
	NLDescription     string                     `json:"nl_description"`
	Strictness        types.PushStrictness       `json:"strictness"`
	LegacyToolPlanV1  json.RawMessage            `json:"approved_fetch_plan"`
	ObservationPolicy *observation.PolicySpecV1  `json:"observation_policy,omitempty"`
}

type createScheduleCommandSpec struct {
	Cron         string `json:"cron"`
	EverySeconds int    `json:"every_seconds"`
	AnchorAt     string `json:"anchor_at"`
	TZ           string `json:"tz"`
}

type normalizedCreateScheduleCommand struct {
	Version           string                    `json:"version"`
	Spec              scheduler.ScheduleSpec    `json:"spec"`
	Intent            string                    `json:"intent"`
	NLDescription     string                    `json:"nl_description"`
	Strictness        types.PushStrictness      `json:"strictness,omitempty"`
	LegacyToolPlanV1  json.RawMessage           `json:"approved_fetch_plan"`
	ObservationPolicy *observation.PolicySpecV1 `json:"observation_policy,omitempty"`
}

type compiledTaskDefinitionCheckpoint struct {
	Version          string                 `json:"version"`
	DefinitionDigest string                 `json:"definition_digest"`
	TaskID           string                 `json:"task_id"`
	TenantID         int64                  `json:"tenant_id"`
	UserID           int64                  `json:"user_id"`
	OperationID      string                 `json:"operation_id"`
	CommandDigest    string                 `json:"command_digest"`
	Spec             scheduler.ScheduleSpec `json:"spec"`
	Scope            workflow.PushScope     `json:"scope"`
	Observation      *observation.PolicyV1  `json:"observation,omitempty"`
	NLDescription    string                 `json:"nl_description"`
	PlaybookContent  string                 `json:"playbook_content"`
	FetchPlan        json.RawMessage        `json:"fetch_plan"`
	Strictness       types.PushStrictness   `json:"strictness,omitempty"`
}

type compiledFetchPlan struct {
	Targets []compiledFetchTarget `json:"targets"`
}

type compiledFetchTarget struct {
	Platform   string          `json:"platform"`
	Capability string          `json:"capability"`
	Title      string          `json:"title,omitempty"`
	URL        string          `json:"url"`
	Config     json.RawMessage `json:"config"`
	ToolName   string          `json:"tool_name,omitempty"`
	ToolArgs   json.RawMessage `json:"tool_arguments,omitempty"`
}

// Prepare seals the canonical command, materializes only the user-approved
// plan, and checkpoints both the definition and prepared schedule. Every
// recovery path validates exact scope, digest, canonical bytes, and TaskID.
func (p *CreationPreparer) Prepare(
	ctx context.Context,
	in CreationPrepareInput,
) (CreationPrepareResult, error) {
	if err := ctx.Err(); err != nil {
		return CreationPrepareResult{}, err
	}
	if p == nil || p.store == nil || p.schedules == nil {
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
			"任务参数无效，未创建任务", cause,
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
		// This checkpoint has no external effect. A response loss is retried by
		// recovery, which replays the same approved bytes under a fresh fence.
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
		if op.Phase != types.TaskCreationPhaseTranslationStarted {
			return CreationPrepareResult{}, p.blockInvalidCheckpoint(
				ctx, in.Lease, op.Phase,
				errors.New("definition checkpoint is missing after translation phase"),
			)
		}
	}

	playbook := command.Intent
	canonicalPlan := bytes.Clone(command.LegacyToolPlanV1)
	var compiledObservation *observation.PolicyV1
	if command.ObservationPolicy != nil {
		policy, policyErr := observation.Compile(*command.ObservationPolicy, loaded.CreatedAt)
		if policyErr != nil {
			return CreationPrepareResult{}, p.failKnownFailure(
				ctx, in.Lease, "observation_policy_invalid",
				"已批准的新鲜度策略无法生效，未创建任务", policyErr,
			)
		}
		compiledObservation = &policy
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
		Observation:     compiledObservation,
		NLDescription:   command.NLDescription,
		PlaybookContent: playbook,
		FetchPlan:       canonicalPlan,
		Strictness:      command.Strictness,
	}
	materialized, err := pausedDefinitionFromCompiled(compiled)
	if err != nil {
		return CreationPrepareResult{}, p.failKnownFailure(
			ctx, in.Lease, "compiled_definition_invalid",
			"已批准任务定义无法规范化，未创建任务", err,
		)
	}
	compiled.DefinitionDigest, err = types.DigestPausedCompiledTaskDefinition(materialized)
	if err != nil {
		return CreationPrepareResult{}, p.failKnownFailure(
			ctx, in.Lease, "compiled_definition_invalid",
			"已批准任务定义无法生成完整性摘要，未创建任务", err,
		)
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
		Spec: compiled.Spec, Scope: compiled.Scope,
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
	if op.Status != types.TaskOperationStatusExecuting || creationPhaseTerminal(op.Phase) {
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
	command, err := normalizeCreateScheduleEnvelope(args)
	if err != nil {
		return normalizedCreateScheduleCommand{}, nil, err
	}
	approvedPlan, err := canonicalizeFetchPlan(args.LegacyToolPlanV1)
	if err != nil {
		return normalizedCreateScheduleCommand{}, nil,
			fmt.Errorf("task: retained v1 tool plan is invalid: %w", err)
	}
	command.LegacyToolPlanV1 = approvedPlan
	canonical, err := json.Marshal(command)
	if err != nil {
		return normalizedCreateScheduleCommand{}, nil,
			fmt.Errorf("task: marshal normalized create_schedule command: %w", err)
	}
	return command, canonical, nil
}

func normalizeCreateScheduleEnvelope(
	args *createScheduleCommandArgs,
) (normalizedCreateScheduleCommand, error) {
	if args == nil || args.Spec == nil {
		return normalizedCreateScheduleCommand{},
			errors.New("task: create_schedule spec is required")
	}
	intent := strings.TrimSpace(args.Intent)
	if intent == "" {
		return normalizedCreateScheduleCommand{},
			errors.New("task: approved intent must be non-empty")
	}
	if utf8.RuneCountInString(intent) > maxCreationPlaybookRunes {
		return normalizedCreateScheduleCommand{},
			fmt.Errorf("task: approved intent exceeds %d characters", maxCreationPlaybookRunes)
	}
	if args.Strictness != "" && !args.Strictness.Valid() {
		return normalizedCreateScheduleCommand{},
			errors.New("task: strictness must be empty, loose, normal, or strict")
	}
	if args.ObservationPolicy != nil {
		if err := args.ObservationPolicy.Validate(); err != nil {
			return normalizedCreateScheduleCommand{},
				fmt.Errorf("task: observation_policy is invalid: %w", err)
		}
	}
	spec, err := normalizeCreationScheduleSpec(*args.Spec)
	if err != nil {
		return normalizedCreateScheduleCommand{}, err
	}
	description := strings.TrimSpace(args.NLDescription)
	if description == "" {
		description = truncateCreationRunes(intent, maxCreationDescriptionRunes)
	}
	if utf8.RuneCountInString(description) > maxCreationDescriptionRunes {
		return normalizedCreateScheduleCommand{},
			fmt.Errorf("task: nl_description exceeds %d characters", maxCreationDescriptionRunes)
	}
	return normalizedCreateScheduleCommand{
		Version: creationCommandVersion, Spec: spec,
		Intent: intent, NLDescription: description, Strictness: args.Strictness,
		ObservationPolicy: args.ObservationPolicy,
	}, nil
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
	if plan == nil || len(plan.Targets) == 0 {
		return nil, errors.New("fetch_plan.targets must be non-empty")
	}
	if len(plan.Targets) > maxCompiledSources {
		return nil, fmt.Errorf("fetch_plan.targets exceeds %d entries", maxCompiledSources)
	}
	seenURLs := make(map[string]struct{}, len(plan.Targets))
	for i := range plan.Targets {
		target := &plan.Targets[i]
		if strings.TrimSpace(target.Platform) == "" || strings.TrimSpace(target.Platform) != target.Platform ||
			utf8.RuneCountInString(target.Platform) > maxCompiledSourceRunes {
			return nil, fmt.Errorf("fetch_plan.targets[%d].platform is invalid", i)
		}
		if strings.TrimSpace(target.Capability) == "" || strings.TrimSpace(target.Capability) != target.Capability ||
			utf8.RuneCountInString(target.Capability) > maxCompiledSourceRunes {
			return nil, fmt.Errorf("fetch_plan.targets[%d].capability is invalid", i)
		}
		if strings.TrimSpace(target.Title) != target.Title ||
			utf8.RuneCountInString(target.Title) > maxCompiledSourceRunes {
			return nil, fmt.Errorf("fetch_plan.targets[%d].title is invalid", i)
		}
		if strings.TrimSpace(target.URL) == "" || strings.TrimSpace(target.URL) != target.URL ||
			len(target.URL) > maxCompiledSourceURLBytes {
			return nil, fmt.Errorf("fetch_plan.targets[%d].url is invalid", i)
		}
		if _, duplicate := seenURLs[target.URL]; duplicate {
			return nil, fmt.Errorf("fetch_plan.targets[%d].url is duplicated", i)
		}
		seenURLs[target.URL] = struct{}{}
		config := target.Config
		if len(bytes.TrimSpace(config)) == 0 {
			config = json.RawMessage(`{}`)
		}
		canonical, err := canonicalJSONObject(config)
		if err != nil {
			return nil, fmt.Errorf("fetch_plan.targets[%d].config: %w", i, err)
		}
		target.Config = canonical
		candidate := &types.FetchTarget{
			Platform: types.Platform(target.Platform), Capability: types.Capability(target.Capability),
			Title: target.Title, URL: target.URL, Config: target.Config,
			Status: types.FetchTargetStatusActive,
		}
		if message := acquisitiontool.ValidateMaterialized(candidate); message != "" {
			return nil, fmt.Errorf("fetch_plan.targets[%d] is not materializable: %s", i, message)
		}
		if err := validateApprovedSourceNetworkBoundary(candidate); err != nil {
			return nil, fmt.Errorf("fetch_plan.targets[%d] violates network policy: %w", i, err)
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

func validateApprovedSourceNetworkBoundary(source *types.FetchTarget) error {
	var rawURL string
	switch {
	case source.Platform == types.PlatformWeb && source.Capability == types.CapFeed:
		rawURL = source.URL
	case source.Platform == types.PlatformWeb && source.Capability == types.CapContents:
		var config struct {
			URL string `json:"url"`
		}
		if err := decodeStrictJSON(source.Config, &config); err != nil {
			return errors.New("contents URL config is invalid")
		}
		rawURL = config.URL
	default:
		return nil
	}
	parsed, err := url.Parse(rawURL)
	if err != nil || parsed.Hostname() == "" {
		return errors.New("external URL is invalid")
	}
	host := strings.TrimSuffix(strings.ToLower(parsed.Hostname()), ".")
	if host == "localhost" || strings.HasSuffix(host, ".localhost") ||
		strings.HasSuffix(host, ".local") {
		return errors.New("local hostnames are forbidden")
	}
	if ip := net.ParseIP(host); ip != nil {
		if approvedSourceIPBlocked(ip) {
			return errors.New("private or special-use IP addresses are forbidden")
		}
	} else if acquisitiontool.IsIPAddressLike(host) {
		return errors.New("non-canonical numeric IP addresses are forbidden")
	}
	return nil
}

func approvedSourceIPBlocked(ip net.IP) bool {
	if ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast() ||
		ip.IsLinkLocalMulticast() || ip.IsUnspecified() || ip.IsMulticast() {
		return true
	}
	for _, cidr := range []string{
		"100.64.0.0/10",
		"192.0.0.0/24",
		"198.18.0.0/15",
		"192.0.2.0/24",
		"198.51.100.0/24",
		"203.0.113.0/24",
	} {
		_, network, err := net.ParseCIDR(cidr)
		if err == nil && network.Contains(ip) {
			return true
		}
	}
	return false
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
	if (compiled.Version != compiledDefinitionVersionV1 &&
		compiled.Version != compiledDefinitionVersion) ||
		compiled.TenantID != in.TenantID ||
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
		return compiledTaskDefinitionCheckpoint{}, errors.New("compiled definition source scope must be explicitly empty")
	}
	if command.ObservationPolicy == nil {
		if compiled.Observation != nil {
			return compiledTaskDefinitionCheckpoint{}, errors.New("compiled definition has an unapproved observation policy")
		}
	} else {
		expected, compileErr := observation.Compile(*command.ObservationPolicy, op.CreatedAt)
		if compileErr != nil || compiled.Observation == nil ||
			!observationPoliciesEqual(expected, *compiled.Observation) {
			return compiledTaskDefinitionCheckpoint{}, errors.New("compiled observation policy differs from approved command")
		}
	}
	plan, err := canonicalizeFetchPlan(compiled.FetchPlan)
	if err != nil || !bytes.Equal(plan, compiled.FetchPlan) {
		return compiledTaskDefinitionCheckpoint{}, errors.New("compiled fetch_plan is not canonical and valid")
	}
	if compiled.Version == compiledDefinitionVersion {
		var protocolPlan compiledFetchPlan
		if err := decodeStrictJSON(plan, &protocolPlan); err != nil {
			return compiledTaskDefinitionCheckpoint{},
				errors.New("Source-free compiled definition Tool plan is invalid")
		}
		for _, target := range protocolPlan.Targets {
			if target.ToolName == "" || len(bytes.TrimSpace(target.ToolArgs)) == 0 {
				return compiledTaskDefinitionCheckpoint{},
					errors.New("Source-free compiled definition is missing a Tool call")
			}
		}
	}
	if !bytes.Equal(compiled.FetchPlan, command.LegacyToolPlanV1) {
		return compiledTaskDefinitionCheckpoint{}, errors.New("compiled fetch_plan differs from approved command")
	}
	materialized, err := pausedDefinitionFromCompiled(*compiled)
	if err != nil {
		return compiledTaskDefinitionCheckpoint{}, errors.New("compiled materialized definition is invalid")
	}
	digest, err := types.DigestPausedCompiledTaskDefinition(materialized)
	if err != nil || !validLowerSHA256(compiled.DefinitionDigest) || digest != compiled.DefinitionDigest {
		return compiledTaskDefinitionCheckpoint{}, errors.New("compiled materialized definition digest differs")
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
	definition, err := pausedDefinitionFromCompiled(compiled)
	if err != nil {
		return CreationPrepareResult{}, err
	}
	return CreationPrepareResult{
		Definition: definition, DefinitionDigest: digest, Schedule: prepared,
	}, nil
}

func pausedDefinitionFromCompiled(
	compiled compiledTaskDefinitionCheckpoint,
) (types.PausedCompiledTaskDefinition, error) {
	specJSON, err := json.Marshal(compiled.Spec)
	if err != nil {
		return types.PausedCompiledTaskDefinition{}, fmt.Errorf("marshal compiled schedule spec: %w", err)
	}
	scopeJSON, err := json.Marshal(compiledTaskScopeV1{
		Observation: compiled.Observation,
	})
	if err != nil {
		return types.PausedCompiledTaskDefinition{}, fmt.Errorf("marshal compiled task scope: %w", err)
	}
	return types.PausedCompiledTaskDefinition{
		TaskID: compiled.TaskID, TenantID: compiled.TenantID, UserID: compiled.UserID,
		NLDescription:   compiled.NLDescription,
		SpecJSON:        append(json.RawMessage(nil), specJSON...),
		ScopeJSON:       append(json.RawMessage(nil), scopeJSON...),
		PlaybookContent: compiled.PlaybookContent,
		FetchPlan:       append(json.RawMessage(nil), compiled.FetchPlan...),
		Strictness:      compiled.Strictness,
	}, nil
}

type compiledTaskScopeV1 struct {
	Observation *observation.PolicyV1 `json:"observation,omitempty"`
}

func observationPoliciesEqual(left, right observation.PolicyV1) bool {
	leftJSON, leftErr := json.Marshal(left)
	rightJSON, rightErr := json.Marshal(right)
	return leftErr == nil && rightErr == nil && bytes.Equal(leftJSON, rightJSON)
}

func (p *CreationPreparer) blockInvalidCheckpoint(
	ctx context.Context,
	lease types.TaskCreationLease,
	phase types.TaskCreationPhase,
	cause error,
) error {
	wrapped := fmt.Errorf("%w: %w", ErrCreationCheckpointInvalid, cause)
	if creationPhaseAtLeast(phase, types.TaskCreationPhaseSchedulePrepared) {
		// Once the immutable schedule has been prepared, a prior Ensure RPC may
		// have committed while its response/checkpoint was lost. Only the A5
		// cleanup/quarantine path may tombstone the operation from this boundary;
		// blocking here could strand an unaccounted Temporal object.
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
