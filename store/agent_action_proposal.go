package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/types"
)

const agentActionProposalEvidence = "agent-durable-proposal/v1"

// ProposeAgentActionContinuation creates one newly-issued enable_source card
// directly in the v2 durable lane. The pending root, frozen continuation and
// generation-1 authority are one transaction; callers must never compose
// CreatePendingAction with a later activation.
//
// Repeating the exact proposal after a lost commit response is read-only and
// succeeds only when every persisted byte and authority field still matches.
func (s *Store) ProposeAgentActionContinuation(
	ctx context.Context,
	action *types.PendingAction,
) error {
	if action == nil || action.ID == "" || len(action.ID) > 255 ||
		action.UserID <= 0 || action.SessionID == nil ||
		*action.SessionID <= 0 || action.ToolName != agentActionToolName ||
		action.ExpiresAt.IsZero() ||
		strings.TrimSpace(action.Summary) != action.Summary ||
		action.Summary == "" || len(action.Summary) > 4096 ||
		(action.Status != "" &&
			action.Status != types.PendingActionStatusPending) {
		return agentEventValidationError(
			"Agent action durable proposal is invalid")
	}
	sourceID, canonicalArgs, err := canonicalEnableSourceArgs(action.Args)
	if err != nil {
		return err
	}
	frozen, err := freezeEnableSourceAction(sourceID)
	if err != nil {
		return err
	}
	expiresAt := time.UnixMicro(action.ExpiresAt.UnixMicro())

	tx, err := s.beginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return agentEventDatabaseError(
			"begin Agent action durable proposal", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if _, err := tx.Exec(ctx,
		`SELECT pg_advisory_xact_lock($1,$2)`,
		agentActionAdmissionClass, agentActionAdmissionKey,
	); err != nil {
		return agentEventDatabaseError(
			"lock Agent action proposal admission", err)
	}

	// Tenant is derived from the same canonical membership relation as legacy
	// Agent writes. It is not accepted from the model or callback payload.
	var tenantID int64
	if err := tx.QueryRow(ctx,
		`SELECT tenant_id FROM memberships WHERE user_id=$1`,
		action.UserID,
	).Scan(&tenantID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return agentEventNotFound()
		}
		return agentEventDatabaseError(
			"resolve Agent action proposal tenant", err)
	}
	if err := setAgentActionProposerContext(ctx, tx, tenantID); err != nil {
		return err
	}
	var databaseNow time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).
		Scan(&databaseNow); err != nil {
		return agentEventDatabaseError(
			"read Agent action proposal clock", err)
	}
	if !databaseNow.Before(expiresAt) {
		return agentEventValidationError(
			"Agent action durable proposal is expired")
	}

	var (
		rootSessionID        *int64
		rootToolName         string
		rootArgs             []byte
		rootSummary          string
		rootStatus           string
		rootExpiresAt        time.Time
		rootExecutionVersion int
	)
	err = tx.QueryRow(ctx,
		`SELECT session_id,tool_name,args,summary,status,expires_at,
		        execution_version
		   FROM pending_actions
		  WHERE id=$1 AND tenant_id=$2 AND user_id=$3`,
		action.ID, tenantID, action.UserID,
	).Scan(
		&rootSessionID, &rootToolName, &rootArgs, &rootSummary,
		&rootStatus, &rootExpiresAt, &rootExecutionVersion,
	)
	if err == nil {
		if err := replayAgentActionProposal(
			ctx, tx, action, tenantID, sourceID, canonicalArgs, frozen,
			expiresAt, rootSessionID, rootToolName, rootArgs, rootSummary,
			rootStatus, rootExpiresAt, rootExecutionVersion,
		); err != nil {
			return err
		}
		if err := tx.Commit(ctx); err != nil {
			return agentEventDatabaseError(
				"commit Agent action proposal replay", err)
		}
		return nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return agentEventDatabaseError(
			"lock Agent action proposal root", err)
	}

	if _, err := tx.Exec(ctx,
		`INSERT INTO pending_actions (
		     id,tenant_id,user_id,session_id,tool_name,args,summary,status,
		     expires_at,execution_version
		 ) VALUES ($1,$2,$3,$4,$5,$6,$7,'pending',$8,$9)`,
		action.ID, tenantID, action.UserID, *action.SessionID,
		agentActionToolName, canonicalArgs, action.Summary, expiresAt,
		AgentActionExecutionVersion,
	); err != nil {
		return agentEventDatabaseError(
			"insert Agent action durable root", err)
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO agent_action_continuations (
		     action_id,tenant_id,user_id,session_id,tool_name,source_id,
		     canonical_args,args_digest,tool_spec_version,tool_spec,
		     tool_spec_digest,tool_policy_version,tool_policy,
		     tool_policy_digest,adapter_version,success_messages,
		     success_digest,not_found_messages,not_found_digest
		 ) VALUES (
		     $1,$2,$3,$4,'enable_source',$5,$6,$7,$8,$9,$10,$11,$12,
		     $13,$14,$15,$16,$17,$18
		 )`,
		action.ID, tenantID, action.UserID, *action.SessionID, sourceID,
		canonicalArgs, AgentActionPayloadDigest(canonicalArgs),
		agentActionToolSpecVersion, frozen.toolSpec,
		AgentActionPayloadDigest(frozen.toolSpec),
		agentActionToolPolicyVersion, frozen.toolPolicy,
		AgentActionPayloadDigest(frozen.toolPolicy),
		agentActionAdapterVersion, frozen.successMessages,
		AgentActionPayloadDigest(frozen.successMessages),
		frozen.notFoundMessages,
		AgentActionPayloadDigest(frozen.notFoundMessages),
	); err != nil {
		return agentEventDatabaseError(
			"freeze Agent action durable proposal", err)
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO agent_action_continuation_authority_events (
		     tenant_id,user_id,action_id,generation,mode,evidence
		 ) VALUES ($1,$2,$3,1,'durable',$4)`,
		tenantID, action.UserID, action.ID,
		agentActionProposalEvidence,
	); err != nil {
		return agentEventDatabaseError(
			"authorize Agent action durable proposal", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return agentEventDatabaseError(
			"commit Agent action durable proposal", err)
	}
	return nil
}

func replayAgentActionProposal(
	ctx context.Context,
	tx pgx.Tx,
	want *types.PendingAction,
	tenantID, sourceID int64,
	canonicalArgs []byte,
	frozen frozenEnableSourceAction,
	expiresAt time.Time,
	rootSessionID *int64,
	rootToolName string,
	rootArgs []byte,
	rootSummary, rootStatus string,
	rootExpiresAt time.Time,
	rootExecutionVersion int,
) error {
	rootSourceID, rootCanonicalArgs, err :=
		canonicalEnableSourceArgs(rootArgs)
	if err != nil || rootSessionID == nil ||
		*rootSessionID != *want.SessionID ||
		rootToolName != agentActionToolName ||
		rootSourceID != sourceID ||
		string(rootCanonicalArgs) != string(canonicalArgs) ||
		rootSummary != want.Summary ||
		rootStatus != string(types.PendingActionStatusPending) ||
		!rootExpiresAt.Equal(expiresAt) ||
		rootExecutionVersion != AgentActionExecutionVersion {
		return agentEventIntegrityError()
	}
	action, err := scanAgentActionContinuation(tx.QueryRow(ctx,
		`SELECT `+agentActionContinuationColumns+`
		   FROM agent_action_continuations
		  WHERE action_id=$1 AND tenant_id=$2 AND user_id=$3`,
		want.ID, tenantID, want.UserID,
	))
	if err != nil {
		return agentEventDatabaseError(
			"lock Agent action proposal continuation", err)
	}
	if action.Status != AgentActionStatusPending ||
		action.SessionID != *want.SessionID ||
		action.SourceID != sourceID ||
		string(action.CanonicalArgs) != string(canonicalArgs) ||
		string(action.ToolSpec) != string(frozen.toolSpec) ||
		string(action.ToolPolicy) != string(frozen.toolPolicy) ||
		string(action.SuccessMessages) != string(frozen.successMessages) ||
		string(action.NotFoundMessages) != string(frozen.notFoundMessages) {
		return agentEventIntegrityError()
	}
	if err := validateFrozenAgentAction(action); err != nil {
		return err
	}
	rows, err := tx.Query(ctx,
		`SELECT generation,mode,evidence
		   FROM agent_action_continuation_authority_events
		  WHERE action_id=$1 ORDER BY generation`,
		want.ID,
	)
	if err != nil {
		return agentEventDatabaseError(
			"read Agent action proposal authority", err)
	}
	defer rows.Close()
	var generation int64
	var mode, evidence string
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return agentEventDatabaseError(
				"iterate Agent action proposal authority", err)
		}
		return agentEventIntegrityError()
	}
	if err := rows.Scan(&generation, &mode, &evidence); err != nil {
		return agentEventDatabaseError(
			"scan Agent action proposal authority", err)
	}
	if rows.Next() {
		return agentEventIntegrityError()
	}
	if err := rows.Err(); err != nil {
		return agentEventDatabaseError(
			"iterate Agent action proposal authority", err)
	}
	if generation != 1 || mode != AgentActionAuthorityDurable ||
		evidence != agentActionProposalEvidence {
		return agentEventIntegrityError()
	}
	return nil
}

func setAgentActionProposerContext(
	ctx context.Context,
	tx pgx.Tx,
	tenantID int64,
) error {
	if err := validateAgentActionProposer(ctx, tx); err != nil {
		return err
	}
	if tenantID <= 0 {
		return agentEventValidationError(
			"Agent action proposal tenant is invalid")
	}
	if _, err := tx.Exec(ctx,
		`SELECT set_config('app.tenant_id',$1,true)`,
		fmt.Sprintf("%d", tenantID),
	); err != nil {
		return agentEventDatabaseError(
			"set Agent action proposer tenant", err)
	}
	if _, err := tx.Exec(
		ctx, `SET LOCAL ROLE vane_agent_action_proposer`,
	); err != nil {
		return agentEventDatabaseError(
			"enter Agent action proposer role", err)
	}
	return nil
}
