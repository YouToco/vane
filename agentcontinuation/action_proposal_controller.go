package agentcontinuation

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/YouToco/vane/types"
)

const DurableActionToolName = "enable_source"

const actionProposalConvergenceTimeout = 5 * time.Second

type actionProposalStore interface {
	ProposeAgentActionContinuation(
		context.Context,
		*types.PendingAction,
	) error
}

type ActionProposalInput struct {
	ActionID  string
	UserID    int64
	SessionID int64
	ToolName  string
	RawArgs   json.RawMessage
	Summary   string
	ExpiresAt time.Time
}

type ActionProposal struct {
	ID      string
	Summary string
}

// ActionProposalController is the only Agent-facing producer for a newly
// issued v2 durable action. Store owns canonicalization, tenant derivation,
// frozen payload construction and the three-row atomic commit.
type ActionProposalController struct {
	actionStore actionProposalStore
}

func NewActionProposalController(
	st actionProposalStore,
) (*ActionProposalController, error) {
	if st == nil {
		return nil, errors.New(
			"agentcontinuation: action proposal Store is required")
	}
	return &ActionProposalController{actionStore: st}, nil
}

func (c *ActionProposalController) Propose(
	ctx context.Context,
	in ActionProposalInput,
) (ActionProposal, error) {
	if c == nil || c.actionStore == nil {
		return ActionProposal{}, errors.New(
			"agentcontinuation: action proposal Store is required")
	}
	if in.ActionID == "" || in.UserID <= 0 || in.SessionID <= 0 ||
		in.ToolName != DurableActionToolName ||
		strings.TrimSpace(in.Summary) == "" ||
		in.ExpiresAt.IsZero() {
		return ActionProposal{}, errors.New(
			"agentcontinuation: action proposal input is invalid")
	}
	sessionID := in.SessionID
	action := &types.PendingAction{
		ID: in.ActionID, UserID: in.UserID, SessionID: &sessionID,
		ToolName: in.ToolName, Args: in.RawArgs, Summary: in.Summary,
		Status:    types.PendingActionStatusPending,
		ExpiresAt: time.UnixMicro(in.ExpiresAt.UnixMicro()),
	}
	if err := c.actionStore.ProposeAgentActionContinuation(
		ctx, action,
	); err != nil {
		originalErr := err
		var appErr *types.AppError
		if !errors.As(err, &appErr) ||
			appErr.Code != types.CodeDatabase {
			return ActionProposal{}, err
		}
		// The commit response is ambiguous. Re-enter the same atomic Store API
		// with the same action identity and bytes; its replay path adopts only
		// complete, exact evidence and never repairs a partial transaction.
		replayCtx, cancel := context.WithTimeout(
			context.WithoutCancel(ctx),
			actionProposalConvergenceTimeout,
		)
		err = c.actionStore.ProposeAgentActionContinuation(
			replayCtx, action,
		)
		cancel()
		if err != nil {
			return ActionProposal{}, originalErr
		}
	}
	return ActionProposal{ID: action.ID, Summary: action.Summary}, nil
}
