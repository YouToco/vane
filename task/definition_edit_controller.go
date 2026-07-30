package task

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/YouToco/vane/internal/strictjson"
	"github.com/YouToco/vane/observation"
	"github.com/YouToco/vane/scheduler"
	"github.com/YouToco/vane/taskstate"
	"github.com/YouToco/vane/types"
)

const taskDefinitionEditOperationRefPrefix = "definition-edit:"

// ErrDefinitionEditOperationNotFound is the only result which permits the
// Agent callback router to continue to another protocol generation.
var ErrDefinitionEditOperationNotFound = errors.New(
	"task: definition edit operation not found",
)

// DefinitionEditProposalInput is the authenticated Agent handoff. RawArgs are
// still model output and are strictly decoded before any Store or Temporal
// operation.
type DefinitionEditProposalInput struct {
	ActionID  string
	UserID    int64
	SessionID *int64
	RawArgs   json.RawMessage
	ExpiresAt time.Time
}

type DefinitionEditProposal struct {
	ID      string
	Summary string
}

type definitionEditControllerStore interface {
	LoadTaskDefinitionEditProposalBasis(
		context.Context,
		int64,
		string,
	) (
		int64,
		types.ScheduleStatus,
		int64,
		string,
		taskstate.ApprovedDefinitionV1,
		[]byte,
		error,
	)
	LoadTaskDefinitionEditOperationByActor(
		context.Context,
		string,
		int64,
	) (*types.TaskDefinitionEditOperation, error)
}

// definitionEditCoordinatorBoundary deliberately exposes only the two
// authenticated ingress methods. Recovery, expiration, phase execution and
// raw Store/Temporal APIs are not reachable from the Agent dependency graph.
type definitionEditCoordinatorBoundary interface {
	PrepareAndSealProposal(
		context.Context,
		PrepareTaskDefinitionEditProposalInput,
	) (*types.TaskDefinitionEditOperation, error)
	Execute(
		context.Context,
		types.TaskDefinitionEditScope,
		TaskDefinitionEditReceiptTarget,
	) (TaskDefinitionEditOutcome, error)
}

// DefinitionEditController is the only Agent-facing definition writer. It
// builds a complete target definition from the exact current immutable head,
// then delegates every mutation to TaskDefinitionEditCoordinator.
type DefinitionEditController struct {
	store       definitionEditControllerStore
	coordinator definitionEditCoordinatorBoundary
}

func NewDefinitionEditController(
	store definitionEditControllerStore,
	coordinator definitionEditCoordinatorBoundary,
) *DefinitionEditController {
	return &DefinitionEditController{store: store, coordinator: coordinator}
}

func (c *DefinitionEditController) Prepare(
	ctx context.Context,
	in DefinitionEditProposalInput,
) (DefinitionEditProposal, error) {
	if err := ctx.Err(); err != nil {
		return DefinitionEditProposal{}, err
	}
	if c == nil || c.store == nil || c.coordinator == nil {
		return DefinitionEditProposal{}, errors.New(
			"task: definition edit controller dependencies are incomplete",
		)
	}
	if strings.TrimSpace(in.ActionID) == "" ||
		in.ActionID != strings.TrimSpace(in.ActionID) ||
		in.UserID <= 0 || in.SessionID == nil || *in.SessionID <= 0 ||
		in.ExpiresAt.IsZero() {
		return DefinitionEditProposal{}, definitionEditControllerValidation(
			"任务编辑请求不完整",
		)
	}
	command, err := decodeDefinitionEditCommand(in.RawArgs)
	if err != nil {
		return DefinitionEditProposal{}, err
	}
	tenantID, status, version, digest, base, creationBytes, err :=
		c.store.LoadTaskDefinitionEditProposalBasis(
			ctx, in.UserID, command.TaskID,
		)
	if err != nil {
		return DefinitionEditProposal{}, err
	}
	if tenantID <= 0 || version <= 0 || digest == "" ||
		base.TenantID != tenantID || base.UserID != in.UserID ||
		base.TaskID != command.TaskID {
		return DefinitionEditProposal{}, errors.New(
			"task: definition edit basis identity is invalid",
		)
	}
	if status != types.ScheduleStatusActive &&
		status != types.ScheduleStatusPaused {
		return DefinitionEditProposal{}, definitionEditControllerValidation(
			"任务当前状态不支持编辑",
		)
	}
	baseDigest, err := taskstate.DigestApprovedDefinitionV1(base)
	if err != nil || baseDigest != digest {
		return DefinitionEditProposal{}, errors.New(
			"task: definition edit basis digest differs",
		)
	}
	target, summary, err := applyDefinitionEditCommand(
		base, command, time.Now().UTC().Truncate(time.Second))
	if err != nil {
		return DefinitionEditProposal{}, err
	}
	baseBytes, err := taskstate.EncodeApprovedDefinitionV1(base)
	if err != nil {
		return DefinitionEditProposal{}, definitionEditControllerValidation(
			"当前任务定义无效",
		)
	}
	targetBytes, err := taskstate.EncodeApprovedDefinitionV1(target)
	if err != nil {
		return DefinitionEditProposal{}, definitionEditControllerValidation(
			"目标任务定义无效",
		)
	}
	if bytes.Equal(baseBytes, targetBytes) {
		return DefinitionEditProposal{}, definitionEditControllerValidation(
			"没有检测到任务变更",
		)
	}
	var creation scheduler.PreparedTaskSchedule
	if strictjson.DecodeExact(creationBytes, &creation) != nil {
		return DefinitionEditProposal{}, errors.New(
			"task: definition edit creation provenance is invalid",
		)
	}
	canonicalCreation, err := json.Marshal(creation)
	if err != nil || !bytes.Equal(canonicalCreation, creationBytes) ||
		creation.TenantID != tenantID || creation.UserID != in.UserID ||
		creation.TaskID != command.TaskID {
		return DefinitionEditProposal{}, errors.New(
			"task: definition edit creation provenance differs",
		)
	}
	targetDigest, err := taskstate.DigestApprovedDefinitionV1(target)
	if err != nil {
		return DefinitionEditProposal{}, definitionEditControllerValidation(
			"目标任务定义无效",
		)
	}
	op, err := c.coordinator.PrepareAndSealProposal(
		ctx,
		PrepareTaskDefinitionEditProposalInput{
			OperationID:   in.ActionID,
			OperationRef:  taskDefinitionEditOperationRefPrefix + in.ActionID,
			ActorTenantID: tenantID, ActorUserID: in.UserID,
			TargetTenantID: tenantID, TargetUserID: in.UserID,
			TaskID: command.TaskID, SessionID: *in.SessionID,
			ExpiresAt: in.ExpiresAt, OriginalStatus: status,
			BaseHead: scheduler.TaskDefinitionEditHead{
				Version: version, Digest: digest,
			},
			TargetHead: scheduler.TaskDefinitionEditHead{
				Version: version + 1, Digest: targetDigest,
			},
			BaseDefinition: base, TargetDefinition: target,
			Creation: creation,
		},
	)
	if err != nil {
		return DefinitionEditProposal{}, err
	}
	if op == nil || op.ID != in.ActionID || op.TenantID != tenantID ||
		op.UserID != in.UserID || op.TargetTenantID != tenantID ||
		op.TargetUserID != in.UserID || op.TaskID != command.TaskID {
		return DefinitionEditProposal{}, errors.New(
			"task: definition edit coordinator returned another operation",
		)
	}
	return DefinitionEditProposal{ID: op.ID, Summary: summary}, nil
}

func (c *DefinitionEditController) Execute(
	ctx context.Context,
	userID int64,
	actionID string,
	receipt TaskDefinitionEditReceiptTarget,
) (TaskDefinitionEditOutcome, error) {
	scope, err := c.loadScope(ctx, userID, actionID)
	if err != nil {
		return TaskDefinitionEditOutcome{}, err
	}
	return c.coordinator.Execute(ctx, scope, receipt)
}

func (c *DefinitionEditController) loadScope(
	ctx context.Context,
	userID int64,
	actionID string,
) (types.TaskDefinitionEditScope, error) {
	if c == nil || c.store == nil || c.coordinator == nil {
		return types.TaskDefinitionEditScope{}, errors.New(
			"task: definition edit controller dependencies are incomplete",
		)
	}
	if userID <= 0 || strings.TrimSpace(actionID) == "" ||
		actionID != strings.TrimSpace(actionID) {
		return types.TaskDefinitionEditScope{}, definitionEditControllerValidation(
			"任务编辑动作身份无效",
		)
	}
	op, err := c.store.LoadTaskDefinitionEditOperationByActor(
		ctx, actionID, userID,
	)
	if err != nil {
		if errors.Is(err, types.ErrNotFound) {
			return types.TaskDefinitionEditScope{}, fmt.Errorf(
				"%w: %s", ErrDefinitionEditOperationNotFound, actionID,
			)
		}
		return types.TaskDefinitionEditScope{}, err
	}
	if op == nil || op.ID != actionID || op.UserID != userID {
		return types.TaskDefinitionEditScope{}, errors.New(
			"task: loaded definition edit operation identity differs",
		)
	}
	return op.Scope(), nil
}

type definitionEditCommand struct {
	TaskID            string
	Spec              *scheduler.ScheduleSpec
	Intent            *string
	NLDescription     *string
	Strictness        *types.PushStrictness
	ObservationPolicy *observation.PolicySpecV1
}

func decodeDefinitionEditCommand(raw json.RawMessage) (definitionEditCommand, error) {
	var wire struct {
		TaskID            json.RawMessage `json:"task_id"`
		Spec              json.RawMessage `json:"spec,omitempty"`
		Intent            json.RawMessage `json:"intent,omitempty"`
		NLDescription     json.RawMessage `json:"nl_description,omitempty"`
		Strictness        json.RawMessage `json:"strictness,omitempty"`
		ObservationPolicy json.RawMessage `json:"observation_policy,omitempty"`
	}
	if strictjson.DecodeExact(raw, &wire) != nil {
		return definitionEditCommand{}, definitionEditControllerValidation(
			"任务编辑参数必须严格匹配工具 schema",
		)
	}
	var command definitionEditCommand
	if strictjson.DecodeExact(wire.TaskID, &command.TaskID) != nil ||
		strings.TrimSpace(command.TaskID) == "" ||
		command.TaskID != strings.TrimSpace(command.TaskID) {
		return definitionEditCommand{}, definitionEditControllerValidation(
			"task_id 必须是非空字符串",
		)
	}
	if len(wire.Spec) != 0 {
		var spec scheduler.ScheduleSpec
		if strictjson.DecodeExact(wire.Spec, &spec) != nil {
			return definitionEditCommand{}, definitionEditControllerValidation(
				"spec 必须是严格合法的调度对象",
			)
		}
		command.Spec = &spec
	}
	if len(wire.Intent) != 0 {
		var intent string
		if strictjson.DecodeExact(wire.Intent, &intent) != nil ||
			strings.TrimSpace(intent) == "" {
			return definitionEditCommand{}, definitionEditControllerValidation(
				"intent 不能为空",
			)
		}
		command.Intent = &intent
	}
	if len(wire.NLDescription) != 0 {
		var description string
		if strictjson.DecodeExact(wire.NLDescription, &description) != nil {
			return definitionEditCommand{}, definitionEditControllerValidation(
				"nl_description 必须是字符串",
			)
		}
		command.NLDescription = &description
	}
	if len(wire.Strictness) != 0 {
		var strictness types.PushStrictness
		if strictjson.DecodeExact(wire.Strictness, &strictness) != nil ||
			!strictness.Valid() {
			return definitionEditCommand{}, definitionEditControllerValidation(
				"strictness 只能是 loose / normal / strict",
			)
		}
		command.Strictness = &strictness
	}
	if len(wire.ObservationPolicy) != 0 {
		var policy observation.PolicySpecV1
		if strictjson.DecodeExact(wire.ObservationPolicy, &policy) != nil ||
			policy.Validate() != nil {
			return definitionEditCommand{}, definitionEditControllerValidation(
				"observation_policy 无效",
			)
		}
		command.ObservationPolicy = &policy
	}
	if command.Spec == nil && command.Intent == nil &&
		command.NLDescription == nil && command.Strictness == nil &&
		command.ObservationPolicy == nil {
		return definitionEditCommand{}, definitionEditControllerValidation(
			"请至少提供一项要修改的任务定义",
		)
	}
	return command, nil
}

func applyDefinitionEditCommand(
	base taskstate.ApprovedDefinitionV1,
	command definitionEditCommand,
	effectiveTimes ...time.Time,
) (taskstate.ApprovedDefinitionV1, string, error) {
	target := base
	changes := make([]string, 0, 4)
	effectiveAt := time.Now().UTC().Truncate(time.Second)
	if len(effectiveTimes) > 0 {
		effectiveAt = effectiveTimes[0]
	}
	if command.Spec != nil {
		canonical, err := json.Marshal(command.Spec)
		if err != nil {
			return taskstate.ApprovedDefinitionV1{}, "",
				definitionEditControllerValidation("spec 无法规范化")
		}
		if !bytes.Equal(target.SpecJSON, canonical) {
			target.SpecJSON = canonical
			changes = append(changes,
				"触发频率改为 "+definitionEditSpecSummary(*command.Spec))
		}
	}
	if command.Intent != nil {
		if target.Intent != *command.Intent ||
			target.PlaybookContent != *command.Intent {
			target.Intent = *command.Intent
			target.PlaybookContent = *command.Intent
			changes = append(changes, "监控意图改为「"+*command.Intent+"」")
		}
	}
	if command.NLDescription != nil {
		if target.NLDescription != *command.NLDescription {
			target.NLDescription = *command.NLDescription
			if strings.TrimSpace(*command.NLDescription) == "" {
				changes = append(changes, "清空任务描述")
			} else {
				changes = append(changes,
					"任务描述改为「"+*command.NLDescription+"」")
			}
		}
	}
	if command.Strictness != nil && target.Strictness != *command.Strictness {
		target.Strictness = *command.Strictness
		changes = append(changes,
			"推送门槛改为"+definitionEditStrictnessLabel(*command.Strictness))
	}
	if command.ObservationPolicy != nil {
		policy, err := observation.Compile(*command.ObservationPolicy, effectiveAt)
		if err != nil {
			return taskstate.ApprovedDefinitionV1{}, "",
				definitionEditControllerValidation("observation_policy 无法生效")
		}
		current, err := decodeDefinitionEditApprovedScope(target.ScopeJSON)
		if err != nil {
			return taskstate.ApprovedDefinitionV1{}, "",
				definitionEditControllerValidation("当前任务范围无效")
		}
		current.Observation = &policy
		canonical, err := json.Marshal(current)
		if err != nil {
			return taskstate.ApprovedDefinitionV1{}, "",
				definitionEditControllerValidation("新鲜度策略无法规范化")
		}
		target.ScopeJSON = canonical
		changes = append(changes, "新鲜度策略已更新（生效时间 "+
			policy.EffectiveAt.Format(time.RFC3339)+"）")
	}
	if err := taskstate.ValidateApprovedDefinitionV1ForWrite(target); err != nil {
		return taskstate.ApprovedDefinitionV1{}, "",
			definitionEditControllerValidation("目标任务定义未通过安全校验")
	}
	return target, "编辑定时推送任务（id=" + command.TaskID + "）：\n" +
		strings.Join(changes, "\n"), nil
}

func definitionEditSpecSummary(spec scheduler.ScheduleSpec) string {
	tz := spec.TZ
	if tz == "" {
		tz = "Asia/Shanghai"
	}
	if spec.EverySeconds > 0 {
		if spec.AnchorAt != "" {
			return fmt.Sprintf("从 %s 起每 %d 秒（时区 %s）",
				spec.AnchorAt, spec.EverySeconds, tz)
		}
		return fmt.Sprintf("每 %d 秒（时区 %s，按 epoch 对齐）",
			spec.EverySeconds, tz)
	}
	return fmt.Sprintf("cron「%s」（时区 %s）", spec.Cron, tz)
}

func definitionEditStrictnessLabel(value types.PushStrictness) string {
	switch value {
	case types.StrictnessStrict:
		return "严格"
	case types.StrictnessNormal:
		return "标准"
	default:
		return "宽松"
	}
}

func definitionEditControllerValidation(message string) error {
	return types.NewAppError(types.CodeValidation, message, types.ErrValidation)
}
