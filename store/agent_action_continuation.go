package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/internal/strictjson"
	"github.com/YouToco/vane/types"
)

const (
	AgentActionExecutionVersion = 2

	AgentActionStatusPending    = "pending"
	AgentActionStatusConfirmed  = "confirmed"
	AgentActionStatusCompleted  = "completed"
	AgentActionStatusCancelled  = "cancelled"
	AgentActionStatusExpired    = "expired"
	AgentActionStatusRolledBack = "rolled_back"
	AgentActionStatusBlocked    = "blocked"

	AgentActionAuthorityDurable = "durable"
	AgentActionAuthorityLegacy  = "legacy"

	agentActionToolName          = "enable_source"
	agentActionToolSpecVersion   = "vane.agent-tool-spec/v1"
	agentActionToolPolicyVersion = "vane.agent-tool-policy/v1"
	agentActionAdapterVersion    = "vane.enable-source/postgres/v1"
	agentActionTerminalEnabled   = "enabled"
	agentActionTerminalNotFound  = "not_found"
	agentActionAdmissionClass    = 1447120453
	agentActionAdmissionKey      = 1095976528
	maxAgentActionLease          = 24 * time.Hour
	maxAgentActionPage           = 1000
)

var (
	ErrAgentActionNotRouted = errors.New("agent action continuation is not routed")
	ErrAgentActionBusy      = errors.New("agent action continuation is busy")
	ErrAgentActionTerminal  = errors.New("agent action continuation is terminal")
)

type AgentActionContinuation struct {
	ActionID          string
	TenantID          int64
	UserID            int64
	SessionID         int64
	SourceID          int64
	CanonicalArgs     []byte
	ArgsDigest        string
	ToolSpecVersion   string
	ToolSpec          []byte
	ToolSpecDigest    string
	ToolPolicyVersion string
	ToolPolicy        []byte
	ToolPolicyDigest  string
	AdapterVersion    string
	SuccessMessages   []byte
	SuccessDigest     string
	NotFoundMessages  []byte
	NotFoundDigest    string
	Status            string
	TerminalCode      *string
	LeaseOwner        *string
	LeaseFence        int64
	LeaseExpiresAt    *time.Time
	AttemptCount      int
	NextAttemptAt     time.Time
	ConfirmedAt       *time.Time
	CompletedAt       *time.Time
	BlockedReason     *string
}

type AgentActionContinuationLease struct {
	ActionID  string
	TenantID  int64
	UserID    int64
	SessionID int64
	SourceID  int64
	Owner     string
	Fence     int64
}

type AgentActionConfirmation struct {
	Handled  bool
	Accepted bool
	Replayed bool
	Status   string
}

// AgentActionContinuationStatus is the exact, read-only operator view of one
// pending action and its durable continuation authority. Eligibility is
// derived only after the root, frozen payload, and complete authority history
// have been verified in one repeatable-read transaction.
type AgentActionContinuationStatus struct {
	TenantID           int64      `json:"tenant_id"`
	UserID             int64      `json:"user_id"`
	ActionID           string     `json:"action_id"`
	SessionID          int64      `json:"session_id"`
	SourceID           int64      `json:"source_id"`
	ExecutionVersion   int        `json:"execution_version"`
	Route              string     `json:"route"`
	Generation         int64      `json:"generation"`
	Status             string     `json:"status"`
	TerminalCode       *string    `json:"terminal_code,omitempty"`
	AttemptCount       int        `json:"attempt_count"`
	LeaseOwner         *string    `json:"lease_owner,omitempty"`
	LeaseFence         int64      `json:"lease_fence"`
	LeaseExpiresAt     *time.Time `json:"lease_expires_at,omitempty"`
	NextAttemptAt      *time.Time `json:"next_attempt_at,omitempty"`
	ConfirmedAt        *time.Time `json:"confirmed_at,omitempty"`
	CompletedAt        *time.Time `json:"completed_at,omitempty"`
	BlockedReason      *string    `json:"blocked_reason,omitempty"`
	ActivationEligible bool       `json:"activation_eligible"`
	RollbackEligible   bool       `json:"rollback_eligible"`
}

const agentActionContinuationColumns = `action_id,tenant_id,user_id,
	session_id,source_id,canonical_args,args_digest,tool_spec_version,
	tool_spec,tool_spec_digest,tool_policy_version,tool_policy,
	tool_policy_digest,adapter_version,success_messages,success_digest,
	not_found_messages,not_found_digest,status,terminal_code,lease_owner,
	lease_fence,lease_expires_at,attempt_count,next_attempt_at,confirmed_at,
	completed_at,blocked_reason`

type agentActionScanner interface {
	Scan(...any) error
}

func scanAgentActionContinuation(
	row agentActionScanner,
) (AgentActionContinuation, error) {
	var action AgentActionContinuation
	err := row.Scan(
		&action.ActionID, &action.TenantID, &action.UserID,
		&action.SessionID, &action.SourceID, &action.CanonicalArgs,
		&action.ArgsDigest, &action.ToolSpecVersion, &action.ToolSpec,
		&action.ToolSpecDigest, &action.ToolPolicyVersion,
		&action.ToolPolicy, &action.ToolPolicyDigest,
		&action.AdapterVersion, &action.SuccessMessages,
		&action.SuccessDigest, &action.NotFoundMessages,
		&action.NotFoundDigest, &action.Status, &action.TerminalCode,
		&action.LeaseOwner, &action.LeaseFence, &action.LeaseExpiresAt,
		&action.AttemptCount, &action.NextAttemptAt, &action.ConfirmedAt,
		&action.CompletedAt, &action.BlockedReason,
	)
	return action, err
}

func AgentActionPayloadDigest(payload []byte) string {
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

// GetAgentActionContinuationStatus returns a fail-closed operator view for one
// exact action. A legacy action with no continuation is returned only when it
// is a valid enable_source activation candidate; a rolled-back action retains
// its verified generation-2 legacy history.
func (s *Store) GetAgentActionContinuationStatus(
	ctx context.Context,
	tenantID, userID int64,
	actionID string,
) (AgentActionContinuationStatus, error) {
	if tenantID <= 0 || userID <= 0 || actionID == "" {
		return AgentActionContinuationStatus{},
			agentEventValidationError(
				"Agent action status scope is invalid")
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{
		IsoLevel: pgx.RepeatableRead,
	})
	if err != nil {
		return AgentActionContinuationStatus{},
			agentEventDatabaseError(
				"begin Agent action status", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := setAgentActionOperatorContext(ctx, tx, tenantID); err != nil {
		return AgentActionContinuationStatus{}, err
	}

	var (
		rootSessionID    *int64
		rootToolName     string
		rootArgs         []byte
		rootStatus       string
		rootExpiresAt    time.Time
		executionVersion int
		databaseNow      time.Time
	)
	if err := tx.QueryRow(ctx,
		`SELECT session_id,tool_name,args,status,expires_at,
		        execution_version,clock_timestamp()
		   FROM pending_actions
		  WHERE id=$1 AND tenant_id=$2 AND user_id=$3`,
		actionID, tenantID, userID,
	).Scan(
		&rootSessionID, &rootToolName, &rootArgs, &rootStatus,
		&rootExpiresAt, &executionVersion, &databaseNow,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return AgentActionContinuationStatus{}, agentEventNotFound()
		}
		return AgentActionContinuationStatus{},
			agentEventDatabaseError(
				"read Agent action status root", err)
	}
	if rootSessionID == nil || *rootSessionID <= 0 ||
		rootToolName != agentActionToolName {
		return AgentActionContinuationStatus{}, agentEventIntegrityError()
	}
	sourceID, canonicalArgs, err := canonicalEnableSourceArgs(rootArgs)
	if err != nil {
		return AgentActionContinuationStatus{}, agentEventIntegrityError()
	}
	status := AgentActionContinuationStatus{
		TenantID: tenantID, UserID: userID, ActionID: actionID,
		SessionID: *rootSessionID, SourceID: sourceID,
		ExecutionVersion: executionVersion,
		Route:            AgentActionAuthorityLegacy,
		Status:           rootStatus,
	}

	action, continuationExists, err :=
		loadAgentActionContinuationForStatus(
			ctx, tx, tenantID, userID, actionID,
		)
	if err != nil {
		return AgentActionContinuationStatus{}, err
	}
	generation, route, authorityEvidence, err := loadAgentActionAuthorityStatus(
		ctx, tx, actionID,
	)
	if err != nil {
		return AgentActionContinuationStatus{}, err
	}
	status.Generation = generation
	status.Route = route

	switch executionVersion {
	case 0:
		if !continuationExists {
			if generation != 0 || route != AgentActionAuthorityLegacy {
				return AgentActionContinuationStatus{},
					agentEventIntegrityError()
			}
			switch types.PendingActionStatus(rootStatus) {
			case types.PendingActionStatusPending,
				types.PendingActionStatusExecuted,
				types.PendingActionStatusCancelled,
				types.PendingActionStatusExpired:
			default:
				return AgentActionContinuationStatus{},
					agentEventIntegrityError()
			}
			status.ActivationEligible =
				rootStatus == string(types.PendingActionStatusPending) &&
					databaseNow.Before(rootExpiresAt)
			if err := tx.Commit(ctx); err != nil {
				return AgentActionContinuationStatus{},
					agentEventDatabaseError(
						"commit Agent action status", err)
			}
			return status, nil
		}
		if generation != 2 || route != AgentActionAuthorityLegacy ||
			action.Status != AgentActionStatusRolledBack ||
			action.ConfirmedAt != nil || action.AttemptCount != 0 ||
			action.LeaseOwner != nil || action.LeaseFence != 0 ||
			action.LeaseExpiresAt != nil || action.TerminalCode != nil ||
			action.CompletedAt != nil || action.BlockedReason != nil ||
			!validRolledBackAgentActionRoot(rootStatus) {
			return AgentActionContinuationStatus{},
				agentEventIntegrityError()
		}
	case AgentActionExecutionVersion:
		if !continuationExists || generation != 1 ||
			route != AgentActionAuthorityDurable {
			return AgentActionContinuationStatus{},
				agentEventIntegrityError()
		}
		if action.Status == AgentActionStatusPending {
			if rootStatus != string(types.PendingActionStatusPending) {
				return AgentActionContinuationStatus{},
					agentEventIntegrityError()
			}
		} else if err := validateAgentActionTerminalRoot(
			action.Status, rootStatus,
		); err != nil {
			return AgentActionContinuationStatus{}, err
		}
	default:
		return AgentActionContinuationStatus{}, agentEventIntegrityError()
	}
	if string(canonicalArgs) != string(action.CanonicalArgs) ||
		action.SessionID != *rootSessionID ||
		action.SourceID != sourceID {
		return AgentActionContinuationStatus{}, agentEventIntegrityError()
	}
	if err := validateFrozenAgentAction(action); err != nil {
		return AgentActionContinuationStatus{}, err
	}
	status.SessionID = action.SessionID
	status.SourceID = action.SourceID
	status.Status = action.Status
	status.TerminalCode = action.TerminalCode
	status.AttemptCount = action.AttemptCount
	status.LeaseOwner = action.LeaseOwner
	status.LeaseFence = action.LeaseFence
	status.LeaseExpiresAt = action.LeaseExpiresAt
	status.NextAttemptAt = &action.NextAttemptAt
	status.ConfirmedAt = action.ConfirmedAt
	status.CompletedAt = action.CompletedAt
	status.BlockedReason = action.BlockedReason
	status.RollbackEligible =
		executionVersion == AgentActionExecutionVersion &&
			authorityEvidence != agentActionProposalEvidence &&
			action.Status == AgentActionStatusPending &&
			action.ConfirmedAt == nil && action.AttemptCount == 0 &&
			action.LeaseFence == 0 && action.TerminalCode == nil
	if err := tx.Commit(ctx); err != nil {
		return AgentActionContinuationStatus{},
			agentEventDatabaseError(
				"commit Agent action status", err)
	}
	return status, nil
}

func loadAgentActionContinuationForStatus(
	ctx context.Context,
	tx pgx.Tx,
	tenantID, userID int64,
	actionID string,
) (AgentActionContinuation, bool, error) {
	action, err := scanAgentActionContinuation(tx.QueryRow(ctx,
		`SELECT `+agentActionContinuationColumns+`
		   FROM agent_action_continuations
		  WHERE action_id=$1 AND tenant_id=$2 AND user_id=$3`,
		actionID, tenantID, userID,
	))
	if errors.Is(err, pgx.ErrNoRows) {
		return AgentActionContinuation{}, false, nil
	}
	if err != nil {
		return AgentActionContinuation{}, false,
			agentEventDatabaseError(
				"read Agent action status continuation", err)
	}
	return action, true, nil
}

func loadAgentActionAuthorityStatus(
	ctx context.Context,
	tx pgx.Tx,
	actionID string,
) (int64, string, string, error) {
	rows, err := tx.Query(ctx,
		`SELECT generation,mode,evidence
		   FROM agent_action_continuation_authority_events
		  WHERE action_id=$1 ORDER BY generation`,
		actionID,
	)
	if err != nil {
		return 0, "", "", agentEventDatabaseError(
			"read Agent action status authority", err)
	}
	defer rows.Close()
	generation := int64(0)
	route := AgentActionAuthorityLegacy
	authorityEvidence := ""
	for rows.Next() {
		var gotGeneration int64
		var mode, evidence string
		if err := rows.Scan(
			&gotGeneration, &mode, &evidence,
		); err != nil {
			return 0, "", "", agentEventDatabaseError(
				"scan Agent action status authority", err)
		}
		wantMode := AgentActionAuthorityDurable
		if gotGeneration == 2 {
			wantMode = AgentActionAuthorityLegacy
		}
		if gotGeneration != generation+1 || gotGeneration > 2 ||
			mode != wantMode ||
			strings.TrimSpace(evidence) != evidence ||
			evidence == "" || len(evidence) > 512 {
			return 0, "", "", agentEventIntegrityError()
		}
		generation = gotGeneration
		route = mode
		if gotGeneration == 1 {
			authorityEvidence = evidence
		}
	}
	if err := rows.Err(); err != nil {
		return 0, "", "", agentEventDatabaseError(
			"iterate Agent action status authority", err)
	}
	return generation, route, authorityEvidence, nil
}

// ActivateAgentActionContinuation atomically promotes one exact, pristine
// legacy enable_source action into the v2 durable lane. Ordinary v0 actions
// remain untouched and continue through the historical Claim path.
func (s *Store) ActivateAgentActionContinuation(
	ctx context.Context,
	tenantID, userID int64,
	actionID, evidence string,
) (int64, error) {
	if tenantID <= 0 || userID <= 0 || actionID == "" ||
		strings.TrimSpace(evidence) != evidence || evidence == "" ||
		len(evidence) > 512 {
		return 0, agentEventValidationError(
			"Agent action activation is invalid")
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return 0, agentEventDatabaseError(
			"begin Agent action activation", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if _, err := tx.Exec(ctx,
		`SELECT pg_advisory_xact_lock($1,$2)`,
		agentActionAdmissionClass, agentActionAdmissionKey,
	); err != nil {
		return 0, agentEventDatabaseError(
			"lock Agent action activation admission", err)
	}
	if err := setAgentActionOperatorContext(ctx, tx, tenantID); err != nil {
		return 0, err
	}
	var sessionID *int64
	var toolName, status string
	var args []byte
	var expiresAt time.Time
	var executionVersion int
	if err := tx.QueryRow(ctx,
		`SELECT session_id,tool_name,args,status,expires_at,execution_version
		   FROM pending_actions
		  WHERE id=$1 AND tenant_id=$2 AND user_id=$3
		  FOR UPDATE`,
		actionID, tenantID, userID,
	).Scan(
		&sessionID, &toolName, &args, &status, &expiresAt,
		&executionVersion,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, agentEventNotFound()
		}
		return 0, agentEventDatabaseError(
			"lock Agent action authority root", err)
	}
	if executionVersion == AgentActionExecutionVersion {
		return replayAgentActionActivation(
			ctx, tx, tenantID, userID, actionID, evidence,
			sessionID, toolName, args, status,
		)
	}
	if executionVersion != 0 || sessionID == nil || *sessionID <= 0 ||
		toolName != agentActionToolName ||
		status != string(types.PendingActionStatusPending) {
		return 0, agentEventIntegrityError()
	}
	var prior int
	if err := tx.QueryRow(ctx,
		`SELECT count(*) FROM agent_action_continuations
		  WHERE action_id=$1`,
		actionID,
	).Scan(&prior); err != nil {
		return 0, agentEventDatabaseError(
			"check prior Agent action activation", err)
	}
	if prior != 0 {
		return 0, ErrAgentActionTerminal
	}
	var databaseNow time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).
		Scan(&databaseNow); err != nil {
		return 0, agentEventDatabaseError(
			"read Agent action activation clock", err)
	}
	if !databaseNow.Before(expiresAt) {
		return 0, ErrAgentActionTerminal
	}
	sourceID, canonicalArgs, err := canonicalEnableSourceArgs(args)
	if err != nil {
		return 0, err
	}
	frozen, err := freezeEnableSourceAction(sourceID)
	if err != nil {
		return 0, err
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
		actionID, tenantID, userID, *sessionID, sourceID,
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
		return 0, agentEventDatabaseError(
			"freeze Agent action continuation", err)
	}
	tag, err := tx.Exec(ctx,
		`UPDATE pending_actions SET execution_version=$4
		  WHERE id=$1 AND tenant_id=$2 AND user_id=$3
		    AND execution_version=0 AND status='pending'`,
		actionID, tenantID, userID, AgentActionExecutionVersion,
	)
	if err != nil {
		return 0, agentEventDatabaseError(
			"promote Agent action root", err)
	}
	if tag.RowsAffected() != 1 {
		return 0, agentEventIntegrityError()
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO agent_action_continuation_authority_events (
		     tenant_id,user_id,action_id,generation,mode,evidence
		 ) VALUES ($1,$2,$3,1,'durable',$4)`,
		tenantID, userID, actionID, evidence,
	); err != nil {
		return 0, agentEventDatabaseError(
			"append Agent action authority", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, agentEventDatabaseError(
			"commit Agent action activation", err)
	}
	return 1, nil
}

func replayAgentActionActivation(
	ctx context.Context,
	tx pgx.Tx,
	tenantID, userID int64,
	actionID, evidence string,
	rootSessionID *int64,
	rootToolName string,
	rootArgs []byte,
	rootStatus string,
) (int64, error) {
	action, err := scanAgentActionContinuation(tx.QueryRow(ctx,
		`SELECT `+agentActionContinuationColumns+`
		   FROM agent_action_continuations
		  WHERE action_id=$1 AND tenant_id=$2 AND user_id=$3
		  FOR UPDATE`,
		actionID, tenantID, userID,
	))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, agentEventIntegrityError()
		}
		return 0, agentEventDatabaseError(
			"lock Agent action activation continuation", err)
	}
	if action.Status != AgentActionStatusPending {
		switch action.Status {
		case AgentActionStatusPending, AgentActionStatusConfirmed,
			AgentActionStatusCompleted, AgentActionStatusBlocked,
			AgentActionStatusCancelled, AgentActionStatusExpired:
		default:
			return 0, agentEventIntegrityError()
		}
	}
	if err := validateFrozenAgentAction(action); err != nil {
		return 0, err
	}
	sourceID, canonicalArgs, err := canonicalEnableSourceArgs(rootArgs)
	if err != nil || rootSessionID == nil ||
		*rootSessionID != action.SessionID ||
		rootToolName != agentActionToolName ||
		sourceID != action.SourceID ||
		string(canonicalArgs) != string(action.CanonicalArgs) {
		return 0, agentEventIntegrityError()
	}
	if action.Status != AgentActionStatusPending {
		if err := validateAgentActionTerminalRoot(
			action.Status, rootStatus,
		); err != nil {
			return 0, err
		}
	} else if rootStatus != string(types.PendingActionStatusPending) {
		return 0, agentEventIntegrityError()
	}
	rows, err := tx.Query(ctx,
		`SELECT generation,mode,evidence
		   FROM agent_action_continuation_authority_events
		  WHERE action_id=$1 ORDER BY generation`,
		actionID,
	)
	if err != nil {
		return 0, agentEventDatabaseError(
			"read Agent action activation history", err)
	}
	defer rows.Close()
	var generation int64
	var mode, gotEvidence string
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return 0, agentEventDatabaseError(
				"iterate Agent action activation history", err)
		}
		return 0, agentEventIntegrityError()
	}
	if err := rows.Scan(
		&generation, &mode, &gotEvidence,
	); err != nil {
		return 0, agentEventDatabaseError(
			"scan Agent action activation history", err)
	}
	if rows.Next() {
		return 0, agentEventIntegrityError()
	}
	if err := rows.Err(); err != nil {
		return 0, agentEventDatabaseError(
			"iterate Agent action activation history", err)
	}
	if generation != 1 || mode != AgentActionAuthorityDurable ||
		gotEvidence != evidence {
		return 0, agentEventIntegrityError()
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, agentEventDatabaseError(
			"commit Agent action activation replay", err)
	}
	return generation, nil
}

// RollbackAgentActionContinuation demotes only a pristine, never-confirmed
// exact canary. Once confirmation or any effect history exists, rollback is
// forbidden and the durable recovery path remains the sole authority.
func (s *Store) RollbackAgentActionContinuation(
	ctx context.Context,
	tenantID, userID int64,
	actionID, evidence string,
) (int64, error) {
	if tenantID <= 0 || userID <= 0 || actionID == "" ||
		strings.TrimSpace(evidence) != evidence || evidence == "" ||
		len(evidence) > 512 {
		return 0, agentEventValidationError(
			"Agent action rollback is invalid")
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return 0, agentEventDatabaseError(
			"begin Agent action rollback", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if _, err := tx.Exec(ctx,
		`SELECT pg_advisory_xact_lock($1,$2)`,
		agentActionAdmissionClass, agentActionAdmissionKey,
	); err != nil {
		return 0, agentEventDatabaseError(
			"lock Agent action rollback admission", err)
	}
	if err := setAgentActionOperatorContext(ctx, tx, tenantID); err != nil {
		return 0, err
	}
	var executionVersion int
	var pendingStatus string
	var sessionID *int64
	var toolName string
	var args []byte
	if err := tx.QueryRow(ctx,
		`SELECT execution_version,status,session_id,tool_name,args
		   FROM pending_actions
		  WHERE id=$1 AND tenant_id=$2 AND user_id=$3
		  FOR UPDATE`,
		actionID, tenantID, userID,
	).Scan(
		&executionVersion, &pendingStatus, &sessionID, &toolName, &args,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, agentEventNotFound()
		}
		return 0, agentEventDatabaseError(
			"lock Agent action rollback root", err)
	}
	action, err := scanAgentActionContinuation(tx.QueryRow(ctx,
		`SELECT `+agentActionContinuationColumns+`
		   FROM agent_action_continuations
		  WHERE action_id=$1 AND tenant_id=$2 AND user_id=$3
		  FOR UPDATE`,
		actionID, tenantID, userID,
	))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, agentEventIntegrityError()
		}
		return 0, agentEventDatabaseError(
			"lock Agent action rollback continuation", err)
	}
	if err := validateAgentActionRootBinding(
		action, sessionID, toolName, args,
	); err != nil {
		return 0, err
	}
	if executionVersion == 0 {
		return replayAgentActionRollback(
			ctx, tx, action, pendingStatus, evidence,
		)
	}
	if executionVersion != AgentActionExecutionVersion ||
		pendingStatus != string(types.PendingActionStatusPending) ||
		action.Status != AgentActionStatusPending ||
		action.ConfirmedAt != nil || action.AttemptCount != 0 ||
		action.LeaseFence != 0 || action.TerminalCode != nil {
		return 0, ErrAgentActionTerminal
	}
	var generation int64
	var mode, activationEvidence string
	rows, err := tx.Query(ctx,
		`SELECT generation,mode,evidence
		   FROM agent_action_continuation_authority_events
		  WHERE action_id=$1 ORDER BY generation`,
		actionID,
	)
	if err != nil {
		return 0, agentEventDatabaseError(
			"read Agent action rollback history", err)
	}
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			rows.Close()
			return 0, agentEventDatabaseError(
				"iterate Agent action rollback history", err)
		}
		rows.Close()
		return 0, agentEventIntegrityError()
	}
	if err := rows.Scan(
		&generation, &mode, &activationEvidence,
	); err != nil {
		rows.Close()
		return 0, agentEventDatabaseError(
			"scan Agent action rollback history", err)
	}
	if rows.Next() {
		rows.Close()
		return 0, agentEventIntegrityError()
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, agentEventDatabaseError(
			"iterate Agent action rollback history", err)
	}
	if generation != 1 ||
		mode != AgentActionAuthorityDurable ||
		strings.TrimSpace(activationEvidence) != activationEvidence ||
		activationEvidence == "" {
		rows.Close()
		return 0, agentEventIntegrityError()
	}
	rows.Close()
	if activationEvidence == agentActionProposalEvidence {
		return 0, ErrAgentActionTerminal
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO agent_action_continuation_authority_events (
		     tenant_id,user_id,action_id,generation,mode,evidence
		 ) VALUES ($1,$2,$3,2,'legacy',$4)`,
		tenantID, userID, actionID, evidence,
	); err != nil {
		return 0, agentEventDatabaseError(
			"append Agent action rollback", err)
	}
	tag, err := tx.Exec(ctx,
		`UPDATE agent_action_continuations
		    SET status='rolled_back',updated_at=clock_timestamp()
		  WHERE action_id=$1 AND tenant_id=$2 AND user_id=$3
		    AND status='pending' AND confirmed_at IS NULL
		    AND attempt_count=0 AND lease_fence=0`,
		actionID, tenantID, userID,
	)
	if err != nil || tag.RowsAffected() != 1 {
		if err == nil {
			return 0, agentEventIntegrityError()
		}
		return 0, agentEventDatabaseError(
			"terminalize Agent action rollback", err)
	}
	tag, err = tx.Exec(ctx,
		`UPDATE pending_actions SET execution_version=0
		  WHERE id=$1 AND tenant_id=$2 AND user_id=$3
		    AND execution_version=$4 AND status='pending'`,
		actionID, tenantID, userID, AgentActionExecutionVersion,
	)
	if err != nil || tag.RowsAffected() != 1 {
		if err == nil {
			return 0, agentEventIntegrityError()
		}
		return 0, agentEventDatabaseError(
			"demote Agent action root", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, agentEventDatabaseError(
			"commit Agent action rollback", err)
	}
	return 2, nil
}

func replayAgentActionRollback(
	ctx context.Context,
	tx pgx.Tx,
	action AgentActionContinuation,
	rootStatus string,
	evidence string,
) (int64, error) {
	if action.Status != AgentActionStatusRolledBack ||
		action.ConfirmedAt != nil || action.AttemptCount != 0 ||
		action.LeaseOwner != nil || action.LeaseFence != 0 ||
		action.LeaseExpiresAt != nil || action.TerminalCode != nil ||
		action.CompletedAt != nil || action.BlockedReason != nil ||
		!validRolledBackAgentActionRoot(rootStatus) {
		return 0, agentEventIntegrityError()
	}
	rows, err := tx.Query(ctx,
		`SELECT generation,mode,evidence
		   FROM agent_action_continuation_authority_events
		  WHERE action_id=$1 ORDER BY generation`,
		action.ActionID,
	)
	if err != nil {
		return 0, agentEventDatabaseError(
			"read Agent action rollback history", err)
	}
	defer rows.Close()
	var got [2]struct {
		generation int64
		mode       string
		evidence   string
	}
	i := 0
	for rows.Next() {
		if i >= len(got) {
			return 0, agentEventIntegrityError()
		}
		if err := rows.Scan(
			&got[i].generation, &got[i].mode, &got[i].evidence,
		); err != nil {
			return 0, agentEventDatabaseError(
				"scan Agent action rollback history", err)
		}
		i++
	}
	if err := rows.Err(); err != nil {
		return 0, agentEventDatabaseError(
			"iterate Agent action rollback history", err)
	}
	if i != 2 ||
		got[0].generation != 1 ||
		got[0].mode != AgentActionAuthorityDurable ||
		got[1].generation != 2 ||
		got[1].mode != AgentActionAuthorityLegacy ||
		strings.TrimSpace(got[0].evidence) != got[0].evidence ||
		got[0].evidence == "" || len(got[0].evidence) > 512 ||
		got[1].evidence != evidence {
		return 0, agentEventIntegrityError()
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, agentEventDatabaseError(
			"commit Agent action rollback replay", err)
	}
	return 2, nil
}

func validRolledBackAgentActionRoot(rootStatus string) bool {
	switch types.PendingActionStatus(rootStatus) {
	case types.PendingActionStatusPending,
		types.PendingActionStatusExecuted,
		types.PendingActionStatusCancelled,
		types.PendingActionStatusExpired:
		return true
	default:
		return false
	}
}

func validateAgentActionRootBinding(
	action AgentActionContinuation,
	rootSessionID *int64,
	rootToolName string,
	rootArgs []byte,
) error {
	if err := validateFrozenAgentAction(action); err != nil {
		return err
	}
	sourceID, canonicalArgs, err := canonicalEnableSourceArgs(rootArgs)
	if err != nil || rootSessionID == nil ||
		*rootSessionID != action.SessionID ||
		rootToolName != agentActionToolName ||
		sourceID != action.SourceID ||
		string(canonicalArgs) != string(action.CanonicalArgs) {
		return agentEventIntegrityError()
	}
	return nil
}

type frozenEnableSourceAction struct {
	toolSpec         []byte
	toolPolicy       []byte
	successMessages  []byte
	notFoundMessages []byte
}

func canonicalEnableSourceArgs(raw []byte) (int64, []byte, error) {
	var args struct {
		SourceID int64 `json:"source_id"`
	}
	if err := strictjson.DecodeExact(raw, &args); err != nil ||
		args.SourceID <= 0 {
		return 0, nil, agentEventValidationError(
			"enable_source arguments are invalid")
	}
	canonical := []byte(fmt.Sprintf(`{"source_id":%d}`, args.SourceID))
	return args.SourceID, canonical, nil
}

func freezeEnableSourceAction(
	sourceID int64,
) (frozenEnableSourceAction, error) {
	toolSpec := []byte(
		`{"description":"重新启用一个因连续抓取失败被自动暂停的信源：置回正常、清零失败计数、立即恢复抓取。source_id 可先用 list_sources 查看状态。","name":"enable_source","parameters":{"properties":{"source_id":{"description":"要重新启用的信源 id（连续抓取失败被自动暂停的源，可先用 list_sources 查看状态）","type":"integer"}},"required":["source_id"],"type":"object"},"version":"vane.agent-tool-spec/v1"}`,
	)
	toolPolicy := []byte(
		`{"authorization":"owner","budget":"none","concurrency":"sequential","confirmation":"required","effects":["state_write"],"retry":"none","version":"vane.agent-tool-policy/v1"}`,
	)
	message := func(content string) ([]byte, error) {
		return json.Marshal([]struct {
			Role    string `json:"role"`
			Content string `json:"content"`
		}{{Role: "user", Content: content}})
	}
	success, err := message(fmt.Sprintf(
		"[卡片回调] 用户已确认重新启用信源（id=%d）；信源已启用，失败计数已清零。",
		sourceID,
	))
	if err != nil {
		return frozenEnableSourceAction{}, agentEventValidationError(
			"encode enable_source success fact")
	}
	notFound, err := message(fmt.Sprintf(
		"[卡片回调] 用户已确认重新启用信源（id=%d）；未找到属于该用户的可启用信源，未产生变更。",
		sourceID,
	))
	if err != nil {
		return frozenEnableSourceAction{}, agentEventValidationError(
			"encode enable_source not-found fact")
	}
	return frozenEnableSourceAction{
		toolSpec: toolSpec, toolPolicy: toolPolicy,
		successMessages: success, notFoundMessages: notFound,
	}, nil
}

func (s *Store) ConfirmAgentActionContinuation(
	ctx context.Context,
	userID int64,
	actionID string,
) (AgentActionConfirmation, error) {
	return s.decideAgentActionContinuation(
		ctx, userID, actionID, false,
	)
}

func (s *Store) CancelAgentActionContinuation(
	ctx context.Context,
	userID int64,
	actionID string,
) (AgentActionConfirmation, error) {
	return s.decideAgentActionContinuation(
		ctx, userID, actionID, true,
	)
}

func (s *Store) decideAgentActionContinuation(
	ctx context.Context,
	userID int64,
	actionID string,
	cancel bool,
) (AgentActionConfirmation, error) {
	if userID <= 0 || actionID == "" {
		return AgentActionConfirmation{}, agentEventValidationError(
			"Agent action confirmation scope is invalid")
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return AgentActionConfirmation{}, agentEventDatabaseError(
			"begin Agent action confirmation", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	var peekTenantID int64
	var peekSessionID *int64
	var peekExecutionVersion int
	err = tx.QueryRow(ctx,
		`SELECT tenant_id,session_id,execution_version
		   FROM pending_actions
		  WHERE id=$1 AND user_id=$2`,
		actionID, userID,
	).Scan(
		&peekTenantID, &peekSessionID, &peekExecutionVersion,
	)
	if errors.Is(err, pgx.ErrNoRows) ||
		(err == nil &&
			peekExecutionVersion != AgentActionExecutionVersion) {
		return AgentActionConfirmation{}, ErrAgentActionNotRouted
	}
	if err != nil {
		return AgentActionConfirmation{}, agentEventDatabaseError(
			"inspect Agent action confirmation root", err)
	}
	if peekSessionID == nil || *peekSessionID <= 0 {
		return AgentActionConfirmation{}, agentEventIntegrityError()
	}
	exists, err := lockTenantAdmissionRoot(ctx, tx, peekTenantID)
	if err != nil {
		return AgentActionConfirmation{}, agentEventDatabaseError(
			"lock Agent action confirmation tenant admission", err)
	}
	if !exists {
		return AgentActionConfirmation{}, agentEventNotFound()
	}
	var tenantID int64
	var expiresAt time.Time
	var executionVersion int
	var pendingStatus string
	var rootSessionID *int64
	var rootToolName string
	var rootArgs []byte
	err = tx.QueryRow(ctx,
		`SELECT tenant_id,expires_at,execution_version,status,
		        session_id,tool_name,args
		   FROM pending_actions
		  WHERE id=$1 AND user_id=$2
		  FOR UPDATE`,
		actionID, userID,
	).Scan(
		&tenantID, &expiresAt, &executionVersion, &pendingStatus,
		&rootSessionID, &rootToolName, &rootArgs,
	)
	if errors.Is(err, pgx.ErrNoRows) ||
		(err == nil && executionVersion != AgentActionExecutionVersion) {
		return AgentActionConfirmation{}, ErrAgentActionNotRouted
	}
	if err != nil {
		return AgentActionConfirmation{}, agentEventDatabaseError(
			"lock Agent action confirmation root", err)
	}
	if tenantID != peekTenantID || rootSessionID == nil ||
		*rootSessionID != *peekSessionID {
		return AgentActionConfirmation{}, agentEventIntegrityError()
	}
	if !cancel {
		if err := lockLiveAgentActionSession(
			ctx, tx, tenantID, userID, *rootSessionID,
		); err != nil {
			return AgentActionConfirmation{}, err
		}
	}
	if err := setAgentActionContinuatorContext(ctx, tx, tenantID); err != nil {
		return AgentActionConfirmation{}, err
	}
	action, err := scanAgentActionContinuation(tx.QueryRow(ctx,
		`SELECT `+agentActionContinuationColumns+`
		   FROM agent_action_continuations
		  WHERE action_id=$1 AND tenant_id=$2 AND user_id=$3
		  FOR UPDATE`,
		actionID, tenantID, userID,
	))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return AgentActionConfirmation{}, agentEventIntegrityError()
		}
		return AgentActionConfirmation{}, agentEventDatabaseError(
			"lock Agent action confirmation continuation", err)
	}
	if err := validateAgentActionRootBinding(
		action, rootSessionID, rootToolName, rootArgs,
	); err != nil {
		return AgentActionConfirmation{}, err
	}
	if err := validateFrozenAgentAction(action); err != nil {
		return AgentActionConfirmation{}, err
	}
	projectionIntegrityBlock :=
		action.Status == AgentActionStatusBlocked &&
			action.BlockedReason != nil &&
			*action.BlockedReason == "projection_integrity"
	if projectionIntegrityBlock {
		if err := validateAgentActionTerminalRoot(
			action.Status, pendingStatus,
		); err != nil {
			return AgentActionConfirmation{}, err
		}
		if err := tx.Commit(ctx); err != nil {
			return AgentActionConfirmation{}, agentEventDatabaseError(
				"commit Agent action projection-integrity replay", err)
		}
		return AgentActionConfirmation{
			Handled: true, Replayed: true, Status: action.Status,
		}, nil
	}
	if err := validateActiveAgentActionAuthority(
		ctx, tx, actionID,
	); err != nil {
		return AgentActionConfirmation{}, err
	}
	if action.Status != AgentActionStatusPending {
		if err := validateAgentActionTerminalRoot(
			action.Status, pendingStatus,
		); err != nil {
			return AgentActionConfirmation{}, err
		}
		if action.Status != AgentActionStatusConfirmed {
			if err := commitAgentActionTerminalSessionTx(
				ctx, tx, action, action.Status, true,
			); err != nil {
				return AgentActionConfirmation{}, err
			}
		}
		if err := tx.Commit(ctx); err != nil {
			return AgentActionConfirmation{}, agentEventDatabaseError(
				"commit Agent action terminal replay", err)
		}
		return AgentActionConfirmation{
			Handled: true, Accepted: action.Status == AgentActionStatusConfirmed ||
				action.Status == AgentActionStatusCompleted,
			Replayed: true, Status: action.Status,
		}, nil
	}
	if pendingStatus != string(types.PendingActionStatusPending) {
		return AgentActionConfirmation{}, agentEventIntegrityError()
	}
	if cancel {
		tag, err := tx.Exec(ctx,
			`UPDATE pending_actions
			    SET status='cancelled',updated_at=clock_timestamp()
			  WHERE id=$1 AND tenant_id=$4 AND user_id=$2
			    AND execution_version=$3 AND status='pending'`,
			actionID, userID, AgentActionExecutionVersion, tenantID,
		)
		if err != nil {
			return AgentActionConfirmation{}, agentEventDatabaseError(
				"cancel Agent action root", err)
		}
		if tag.RowsAffected() != 1 {
			return AgentActionConfirmation{}, agentEventIntegrityError()
		}
		tag, err = tx.Exec(ctx,
			`UPDATE agent_action_continuations
			    SET status='cancelled',updated_at=clock_timestamp()
			  WHERE action_id=$1 AND tenant_id=$3 AND user_id=$2
			    AND status='pending'`,
			actionID, userID, tenantID,
		)
		if err != nil {
			return AgentActionConfirmation{}, agentEventDatabaseError(
				"cancel Agent action continuation", err)
		}
		if tag.RowsAffected() != 1 {
			return AgentActionConfirmation{}, agentEventIntegrityError()
		}
		action.Status = AgentActionStatusCancelled
		if err := commitAgentActionTerminalSessionTx(
			ctx, tx, action, action.Status, false,
		); err != nil {
			return AgentActionConfirmation{}, err
		}
		if err := tx.Commit(ctx); err != nil {
			return AgentActionConfirmation{}, agentEventDatabaseError(
				"commit Agent action cancellation", err)
		}
		return AgentActionConfirmation{
			Handled: true, Status: AgentActionStatusCancelled,
		}, nil
	}
	var databaseNow time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).
		Scan(&databaseNow); err != nil {
		return AgentActionConfirmation{}, agentEventDatabaseError(
			"read Agent action confirmation clock", err)
	}
	if !databaseNow.Before(expiresAt) {
		tag, err := tx.Exec(ctx,
			`UPDATE pending_actions
			    SET status='expired',updated_at=clock_timestamp()
			  WHERE id=$1 AND tenant_id=$4 AND user_id=$2
			    AND execution_version=$3 AND status='pending'`,
			actionID, userID, AgentActionExecutionVersion, tenantID,
		)
		if err != nil {
			return AgentActionConfirmation{}, agentEventDatabaseError(
				"expire Agent action root", err)
		}
		if tag.RowsAffected() != 1 {
			return AgentActionConfirmation{}, agentEventIntegrityError()
		}
		tag, err = tx.Exec(ctx,
			`UPDATE agent_action_continuations
			    SET status='expired',updated_at=clock_timestamp()
			  WHERE action_id=$1 AND tenant_id=$3 AND user_id=$2
			    AND status='pending'`,
			actionID, userID, tenantID,
		)
		if err != nil {
			return AgentActionConfirmation{}, agentEventDatabaseError(
				"expire Agent action continuation", err)
		}
		if tag.RowsAffected() != 1 {
			return AgentActionConfirmation{}, agentEventIntegrityError()
		}
		action.Status = AgentActionStatusExpired
		if err := commitAgentActionTerminalSessionTx(
			ctx, tx, action, action.Status, false,
		); err != nil {
			return AgentActionConfirmation{}, err
		}
		if err := tx.Commit(ctx); err != nil {
			return AgentActionConfirmation{}, agentEventDatabaseError(
				"commit Agent action expiry", err)
		}
		return AgentActionConfirmation{
			Handled: true, Status: AgentActionStatusExpired,
		}, nil
	}
	tag, err := tx.Exec(ctx,
		`UPDATE pending_actions
		    SET status='executed',executed_at=clock_timestamp(),
		        updated_at=clock_timestamp()
		  WHERE id=$1 AND tenant_id=$4 AND user_id=$2
		    AND execution_version=$3 AND status='pending'`,
		actionID, userID, AgentActionExecutionVersion, tenantID,
	)
	if err != nil {
		return AgentActionConfirmation{}, agentEventDatabaseError(
			"confirm Agent action root", err)
	}
	if tag.RowsAffected() != 1 {
		return AgentActionConfirmation{}, agentEventIntegrityError()
	}
	tag, err = tx.Exec(ctx,
		`UPDATE agent_action_continuations
		    SET status='confirmed',confirmed_at=clock_timestamp(),
		        next_attempt_at=clock_timestamp(),
		        updated_at=clock_timestamp()
		  WHERE action_id=$1 AND tenant_id=$3 AND user_id=$2
		    AND status='pending'`,
		actionID, userID, tenantID,
	)
	if err != nil {
		return AgentActionConfirmation{}, agentEventDatabaseError(
			"confirm Agent action continuation", err)
	}
	if tag.RowsAffected() != 1 {
		return AgentActionConfirmation{}, agentEventIntegrityError()
	}
	if err := tx.Commit(ctx); err != nil {
		return AgentActionConfirmation{}, agentEventDatabaseError(
			"commit Agent action confirmation", err)
	}
	return AgentActionConfirmation{
		Handled: true, Accepted: true, Status: AgentActionStatusConfirmed,
	}, nil
}

func validateActiveAgentActionAuthority(
	ctx context.Context,
	tx pgx.Tx,
	actionID string,
) error {
	rows, err := tx.Query(ctx,
		`SELECT generation,mode
		   FROM agent_action_continuation_authority_events
		  WHERE action_id=$1 ORDER BY generation`,
		actionID,
	)
	if err != nil {
		return agentEventDatabaseError(
			"read Agent action authority history", err)
	}
	defer rows.Close()
	var generation int64
	var mode string
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return agentEventDatabaseError(
				"iterate Agent action authority history", err)
		}
		return agentEventIntegrityError()
	}
	if err := rows.Scan(&generation, &mode); err != nil {
		return agentEventDatabaseError(
			"scan Agent action authority history", err)
	}
	if generation != 1 || mode != AgentActionAuthorityDurable ||
		rows.Next() {
		return agentEventIntegrityError()
	}
	if err := rows.Err(); err != nil {
		return agentEventDatabaseError(
			"iterate Agent action authority history", err)
	}
	return nil
}

func validateAgentActionTerminalRoot(
	actionStatus, rootStatus string,
) error {
	want := ""
	switch actionStatus {
	case AgentActionStatusConfirmed, AgentActionStatusCompleted,
		AgentActionStatusBlocked:
		want = string(types.PendingActionStatusExecuted)
	case AgentActionStatusCancelled:
		want = string(types.PendingActionStatusCancelled)
	case AgentActionStatusExpired:
		want = string(types.PendingActionStatusExpired)
	default:
		return agentEventIntegrityError()
	}
	if rootStatus != want {
		return agentEventIntegrityError()
	}
	return nil
}

func setAgentActionContinuatorContext(
	ctx context.Context,
	tx pgx.Tx,
	tenantID int64,
) error {
	if err := validateAgentActionContinuator(ctx, tx); err != nil {
		return err
	}
	if tenantID <= 0 {
		return agentEventValidationError(
			"Agent action tenant is invalid")
	}
	if _, err := tx.Exec(ctx,
		`SELECT set_config('app.tenant_id',$1,true)`,
		fmt.Sprintf("%d", tenantID),
	); err != nil {
		return agentEventDatabaseError(
			"set Agent action continuation tenant", err)
	}
	if _, err := tx.Exec(
		ctx, `SET LOCAL ROLE vane_agent_action_continuator`,
	); err != nil {
		return agentEventDatabaseError(
			"enter Agent action continuator role", err)
	}
	return nil
}

func setAgentActionOperatorContext(
	ctx context.Context,
	tx pgx.Tx,
	tenantID int64,
) error {
	if err := validateAgentActionOperator(ctx, tx); err != nil {
		return err
	}
	if tenantID <= 0 {
		return agentEventValidationError(
			"Agent action tenant is invalid")
	}
	if _, err := tx.Exec(ctx,
		`SELECT set_config('app.tenant_id',$1,true)`,
		fmt.Sprintf("%d", tenantID),
	); err != nil {
		return agentEventDatabaseError(
			"set Agent action operator tenant", err)
	}
	if _, err := tx.Exec(
		ctx, `SET LOCAL ROLE vane_agent_action_operator`,
	); err != nil {
		return agentEventDatabaseError(
			"enter Agent action operator role", err)
	}
	return nil
}
