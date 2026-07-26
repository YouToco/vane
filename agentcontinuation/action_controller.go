package agentcontinuation

import (
	"context"
	"errors"
	"fmt"

	"github.com/YouToco/vane/store"
)

// ErrNotRouted is the only result that permits the Agent callback router to
// continue to an older protocol generation. All other lookup, authority and
// integrity failures are ambiguous and therefore fail closed.
var ErrNotRouted = errors.New(
	"agentcontinuation: action is not routed",
)

type actionStore interface {
	ConfirmAgentActionContinuation(
		context.Context,
		int64,
		string,
	) (store.AgentActionConfirmation, error)
	CancelAgentActionContinuation(
		context.Context,
		int64,
		string,
	) (store.AgentActionConfirmation, error)
}

// ActionOutcome is the provider-neutral result of one durable card decision.
// The durable continuation owns business execution and session projection;
// the callback path only renders this deterministic acknowledgement.
type ActionOutcome struct {
	Text     string
	Status   string
	Replayed bool
}

// ActionController is the only Agent-facing decision surface for v2 durable
// actions. It deliberately exposes neither raw Store mutation entrypoints nor
// the generic Tool execution registry.
type ActionController struct {
	store actionStore
}

func NewActionController(st actionStore) (*ActionController, error) {
	if st == nil {
		return nil, errors.New(
			"agentcontinuation: action controller Store is required")
	}
	return &ActionController{store: st}, nil
}

func (c *ActionController) Confirm(
	ctx context.Context,
	userID int64,
	actionID string,
) (ActionOutcome, error) {
	if c == nil || c.store == nil {
		return ActionOutcome{}, errors.New(
			"agentcontinuation: action controller Store is required")
	}
	result, err := c.store.ConfirmAgentActionContinuation(
		ctx, userID, actionID)
	if err != nil {
		return ActionOutcome{}, mapActionDecisionError("confirm", err)
	}
	return renderActionOutcome(result, false)
}

func (c *ActionController) Cancel(
	ctx context.Context,
	userID int64,
	actionID string,
) (ActionOutcome, error) {
	if c == nil || c.store == nil {
		return ActionOutcome{}, errors.New(
			"agentcontinuation: action controller Store is required")
	}
	result, err := c.store.CancelAgentActionContinuation(
		ctx, userID, actionID)
	if err != nil {
		return ActionOutcome{}, mapActionDecisionError("cancel", err)
	}
	return renderActionOutcome(result, true)
}

func mapActionDecisionError(operation string, err error) error {
	if errors.Is(err, store.ErrAgentActionNotRouted) {
		return ErrNotRouted
	}
	return fmt.Errorf("%s durable Agent action: %w", operation, err)
}

func renderActionOutcome(
	result store.AgentActionConfirmation,
	cancel bool,
) (ActionOutcome, error) {
	if !result.Handled {
		return ActionOutcome{}, errors.New(
			"agentcontinuation: handled decision proof is missing")
	}
	text, ok := actionDecisionText(result.Status, cancel)
	if !ok {
		return ActionOutcome{}, fmt.Errorf(
			"agentcontinuation: unsupported action status %q",
			result.Status)
	}
	return ActionOutcome{
		Text: text, Status: result.Status, Replayed: result.Replayed,
	}, nil
}

func actionDecisionText(status string, cancel bool) (string, bool) {
	if cancel {
		switch status {
		case store.AgentActionStatusCancelled:
			return "已取消，本次操作不会执行。", true
		case store.AgentActionStatusConfirmed:
			return "该操作已经确认，系统将可靠继续执行，无法再取消。", true
		case store.AgentActionStatusCompleted:
			return "该操作此前已经完成，无法再取消。", true
		case store.AgentActionStatusBlocked:
			return "该操作已因安全检查停止，未产生额外执行。", true
		case store.AgentActionStatusExpired:
			return "该操作已经过期，本次不会执行。", true
		default:
			return "", false
		}
	}
	switch status {
	case store.AgentActionStatusConfirmed:
		return "已确认，系统将可靠继续执行，无需重复点击。", true
	case store.AgentActionStatusCompleted:
		return "该操作此前已经完成，无需重复确认。", true
	case store.AgentActionStatusBlocked:
		return "该操作已因安全检查停止，未继续执行。", true
	case store.AgentActionStatusCancelled:
		return "该操作已经取消，不能再次确认。", true
	case store.AgentActionStatusExpired:
		return "该操作已经过期，本次不会执行。", true
	default:
		return "", false
	}
}
