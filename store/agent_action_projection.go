package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/types"
)

const (
	agentActionCancelledSessionMessages = `[{"role":"user","content":"[卡片回调] 用户已取消重新启用信源；未产生变更。"}]`
	agentActionExpiredSessionMessages   = `[{"role":"user","content":"[卡片回调] 重新启用信源确认卡已过期；未产生变更。"}]`
	agentActionBlockedSessionMessages   = `[{"role":"user","content":"[卡片回调] 重新启用信源操作已因安全检查停止；未产生额外变更。"}]`
)

func (s *Store) ListDueAgentActionContinuationTenantIDs(
	ctx context.Context,
	before time.Time,
	afterTenantID int64,
	limit int,
) ([]int64, error) {
	if before.IsZero() || afterTenantID < 0 || limit <= 0 ||
		limit > maxAgentActionPage {
		return nil, agentEventValidationError(
			"Agent action tenant page is invalid")
	}
	rows, err := s.pool.Query(ctx,
		`SELECT DISTINCT tenant_id
		   FROM agent_action_continuations
		  WHERE status='confirmed' AND tenant_id>$2
		    AND next_attempt_at<=LEAST($1,clock_timestamp())
		    AND (lease_expires_at IS NULL OR
		         lease_expires_at<=clock_timestamp())
		  ORDER BY tenant_id LIMIT $3`,
		before, afterTenantID, limit,
	)
	if err != nil {
		return nil, agentEventDatabaseError(
			"list due Agent action tenants", err)
	}
	defer rows.Close()
	tenantIDs := make([]int64, 0)
	for rows.Next() {
		var tenantID int64
		if err := rows.Scan(&tenantID); err != nil {
			return nil, agentEventDatabaseError(
				"scan due Agent action tenant", err)
		}
		tenantIDs = append(tenantIDs, tenantID)
	}
	if err := rows.Err(); err != nil {
		return nil, agentEventDatabaseError(
			"iterate due Agent action tenants", err)
	}
	return tenantIDs, nil
}

func (s *Store) ListDueAgentActionContinuations(
	ctx context.Context,
	tenantID int64,
	before time.Time,
	limit int,
) ([]AgentActionContinuation, error) {
	if tenantID <= 0 || before.IsZero() || limit <= 0 ||
		limit > maxAgentActionPage {
		return nil, agentEventValidationError(
			"Agent action page is invalid")
	}
	rows, err := s.pool.Query(ctx,
		`SELECT `+agentActionContinuationColumns+`
		   FROM agent_action_continuations
		  WHERE tenant_id=$1 AND status='confirmed'
		    AND next_attempt_at<=LEAST($2,clock_timestamp())
		    AND (lease_expires_at IS NULL OR
		         lease_expires_at<=clock_timestamp())
		  ORDER BY next_attempt_at,action_id LIMIT $3`,
		tenantID, before, limit,
	)
	if err != nil {
		return nil, agentEventDatabaseError(
			"list due Agent actions", err)
	}
	defer rows.Close()
	actions := make([]AgentActionContinuation, 0)
	for rows.Next() {
		action, err := scanAgentActionContinuation(rows)
		if err != nil {
			return nil, agentEventDatabaseError(
				"scan due Agent action", err)
		}
		actions = append(actions, action)
	}
	if err := rows.Err(); err != nil {
		return nil, agentEventDatabaseError(
			"iterate due Agent actions", err)
	}
	return actions, nil
}

func (s *Store) AcquireAgentActionContinuation(
	ctx context.Context,
	actionID string,
	tenantID, userID int64,
	owner string,
	leaseDuration time.Duration,
) (*AgentActionContinuation, error) {
	if actionID == "" || tenantID <= 0 || userID <= 0 ||
		!validAgentSessionFactOwner(owner) || leaseDuration <= 0 ||
		leaseDuration > maxAgentActionLease {
		return nil, agentEventValidationError(
			"Agent action acquisition is invalid")
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, agentEventDatabaseError(
			"begin Agent action acquisition", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := setAgentActionContinuatorContext(ctx, tx, tenantID); err != nil {
		return nil, err
	}
	var rootVersion int
	var rootStatus string
	var rootSessionID *int64
	if err := tx.QueryRow(ctx,
		`SELECT execution_version,status,session_id FROM pending_actions
		  WHERE id=$1 AND tenant_id=$2 AND user_id=$3
		  FOR UPDATE`,
		actionID, tenantID, userID,
	).Scan(&rootVersion, &rootStatus, &rootSessionID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, agentEventIntegrityError()
		}
		return nil, agentEventDatabaseError(
			"lock Agent action acquisition root", err)
	}
	if rootVersion != AgentActionExecutionVersion ||
		rootStatus != string(types.PendingActionStatusExecuted) {
		return nil, agentEventIntegrityError()
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
			return nil, agentEventIntegrityError()
		}
		return nil, agentEventDatabaseError(
			"lock Agent action acquisition continuation", err)
	}
	if rootSessionID == nil || *rootSessionID != action.SessionID {
		return nil, agentEventIntegrityError()
	}
	if action.Status != AgentActionStatusConfirmed {
		return nil, ErrAgentActionTerminal
	}
	if err := validateActiveAgentActionAuthority(
		ctx, tx, action.ActionID,
	); err != nil {
		if agentSessionFactShouldBlock(err) {
			blockReason, stageErr :=
				stageAgentActionBlockedSessionTx(
					ctx, tx, action, "authority_integrity",
				)
			if stageErr != nil {
				return nil, stageErr
			}
			tag, blockErr := tx.Exec(ctx,
				`UPDATE agent_action_continuations
				    SET status='blocked',
				        blocked_reason=$4,
				        lease_owner=NULL,lease_expires_at=NULL,
				        updated_at=clock_timestamp()
				  WHERE action_id=$1 AND tenant_id=$2 AND user_id=$3
				    AND status='confirmed'`,
				action.ActionID, action.TenantID, action.UserID,
				blockReason,
			)
			if blockErr != nil {
				return nil, agentEventDatabaseError(
					"block corrupt Agent action authority", blockErr)
			}
			if tag.RowsAffected() != 1 {
				return nil, err
			}
			if commitErr := tx.Commit(ctx); commitErr != nil {
				return nil, agentEventDatabaseError(
					"commit corrupt Agent action authority", commitErr)
			}
			return nil, ErrAgentActionTerminal
		}
		return nil, err
	}
	if err := validateFrozenAgentAction(action); err != nil {
		blockReason, stageErr := stageAgentActionBlockedSessionTx(
			ctx, tx, action, "payload_integrity",
		)
		if stageErr != nil {
			return nil, stageErr
		}
		tag, blockErr := tx.Exec(ctx,
			`UPDATE agent_action_continuations
			    SET status='blocked',blocked_reason=$4,
			        lease_owner=NULL,lease_expires_at=NULL,
			        updated_at=clock_timestamp()
			  WHERE action_id=$1 AND tenant_id=$2 AND user_id=$3
			    AND status='confirmed'`,
			action.ActionID, action.TenantID, action.UserID,
			blockReason,
		)
		if blockErr != nil {
			return nil, agentEventDatabaseError(
				"block corrupt Agent action acquisition", blockErr)
		}
		if tag.RowsAffected() != 1 {
			return nil, err
		}
		if commitErr := tx.Commit(ctx); commitErr != nil {
			return nil, agentEventDatabaseError(
				"commit corrupt Agent action acquisition", commitErr)
		}
		return nil, ErrAgentActionTerminal
	}
	var databaseNow time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).
		Scan(&databaseNow); err != nil {
		return nil, agentEventDatabaseError(
			"read Agent action acquisition clock", err)
	}
	if databaseNow.Before(action.NextAttemptAt) ||
		(action.LeaseExpiresAt != nil &&
			databaseNow.Before(*action.LeaseExpiresAt) &&
			(action.LeaseOwner == nil || *action.LeaseOwner != owner)) {
		return nil, ErrAgentActionBusy
	}
	if action.LeaseExpiresAt != nil &&
		databaseNow.Before(*action.LeaseExpiresAt) &&
		action.LeaseOwner != nil && *action.LeaseOwner == owner {
		if err := tx.Commit(ctx); err != nil {
			return nil, agentEventDatabaseError(
				"commit Agent action lease replay", err)
		}
		return &action, nil
	}
	updated, err := scanAgentActionContinuation(tx.QueryRow(ctx,
		`UPDATE agent_action_continuations
		    SET lease_owner=$4,
		        lease_expires_at=clock_timestamp()+
		            ($6*interval '1 microsecond'),
		        lease_fence=$5+1,attempt_count=attempt_count+1,
		        updated_at=clock_timestamp()
		  WHERE action_id=$1 AND tenant_id=$2 AND user_id=$3
		    AND status='confirmed' AND lease_fence=$5
		    AND next_attempt_at<=clock_timestamp()
		    AND (lease_expires_at IS NULL OR
		         lease_expires_at<=clock_timestamp())
		  RETURNING `+agentActionContinuationColumns,
		actionID, tenantID, userID, owner, action.LeaseFence,
		leaseDuration.Microseconds(),
	))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrAgentActionBusy
	}
	if err != nil {
		return nil, agentEventDatabaseError(
			"acquire Agent action", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, agentEventDatabaseError(
			"commit Agent action acquisition", err)
	}
	return &updated, nil
}

func (a AgentActionContinuation) Lease() (
	AgentActionContinuationLease,
	error,
) {
	if a.ActionID == "" || a.TenantID <= 0 || a.UserID <= 0 ||
		a.SessionID <= 0 || a.SourceID <= 0 || a.LeaseOwner == nil ||
		a.LeaseFence <= 0 {
		return AgentActionContinuationLease{},
			agentEventValidationError(
				"Agent action lease is incomplete")
	}
	return AgentActionContinuationLease{
		ActionID: a.ActionID, TenantID: a.TenantID,
		UserID: a.UserID, SessionID: a.SessionID,
		SourceID: a.SourceID, Owner: *a.LeaseOwner,
		Fence: a.LeaseFence,
	}, nil
}

func (s *Store) ProjectAgentActionContinuation(
	ctx context.Context,
	lease AgentActionContinuationLease,
) error {
	if err := validateAgentActionLease(lease); err != nil {
		return err
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return agentEventDatabaseError(
			"begin Agent action projection", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := setAgentActionContinuatorContext(
		ctx, tx, lease.TenantID,
	); err != nil {
		return err
	}
	var rootVersion int
	var rootStatus string
	var rootSessionID *int64
	if err := tx.QueryRow(ctx,
		`SELECT execution_version,status,session_id FROM pending_actions
		  WHERE id=$1 AND tenant_id=$2 AND user_id=$3
		  FOR UPDATE`,
		lease.ActionID, lease.TenantID, lease.UserID,
	).Scan(&rootVersion, &rootStatus, &rootSessionID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return agentEventIntegrityError()
		}
		return agentEventDatabaseError(
			"lock Agent action projection root", err)
	}
	if rootVersion != AgentActionExecutionVersion ||
		rootStatus != string(types.PendingActionStatusExecuted) {
		return agentEventIntegrityError()
	}
	action, err := scanAgentActionContinuation(tx.QueryRow(ctx,
		`SELECT `+agentActionContinuationColumns+`
		   FROM agent_action_continuations
		  WHERE action_id=$1 AND tenant_id=$2 AND user_id=$3
		  FOR UPDATE`,
		lease.ActionID, lease.TenantID, lease.UserID,
	))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return agentEventIntegrityError()
		}
		return agentEventDatabaseError(
			"lock Agent action projection continuation", err)
	}
	if rootSessionID == nil || *rootSessionID != action.SessionID {
		return agentEventIntegrityError()
	}
	if err := validateFrozenAgentAction(action); err != nil {
		if action.Status != AgentActionStatusConfirmed ||
			action.LeaseOwner == nil ||
			*action.LeaseOwner != lease.Owner ||
			action.LeaseFence != lease.Fence {
			return err
		}
		blockReason, stageErr := stageAgentActionBlockedSessionTx(
			ctx, tx, action, "payload_integrity",
		)
		if stageErr != nil {
			return stageErr
		}
		tag, blockErr := tx.Exec(ctx,
			`UPDATE agent_action_continuations
			    SET status='blocked',blocked_reason=$6,
			        lease_owner=NULL,lease_expires_at=NULL,
			        updated_at=clock_timestamp()
			  WHERE action_id=$1 AND tenant_id=$2 AND user_id=$3
			    AND status='confirmed' AND lease_owner=$4
			    AND lease_fence=$5`,
			lease.ActionID, lease.TenantID, lease.UserID,
			lease.Owner, lease.Fence, blockReason,
		)
		if blockErr != nil {
			return agentEventDatabaseError(
				"block corrupt Agent action projection", blockErr)
		}
		if tag.RowsAffected() != 1 {
			return ErrAgentActionBusy
		}
		if commitErr := tx.Commit(ctx); commitErr != nil {
			return agentEventDatabaseError(
				"commit corrupt Agent action projection", commitErr)
		}
		return nil
	}
	if err := validateActiveAgentActionAuthority(
		ctx, tx, action.ActionID,
	); err != nil {
		if agentSessionFactShouldBlock(err) &&
			action.Status == AgentActionStatusConfirmed &&
			action.LeaseOwner != nil &&
			*action.LeaseOwner == lease.Owner &&
			action.LeaseFence == lease.Fence {
			blockReason, stageErr :=
				stageAgentActionBlockedSessionTx(
					ctx, tx, action, "authority_integrity",
				)
			if stageErr != nil {
				return stageErr
			}
			tag, blockErr := tx.Exec(ctx,
				`UPDATE agent_action_continuations
				    SET status='blocked',
				        blocked_reason=$6,
				        lease_owner=NULL,lease_expires_at=NULL,
				        updated_at=clock_timestamp()
				  WHERE action_id=$1 AND tenant_id=$2 AND user_id=$3
				    AND status='confirmed' AND lease_owner=$4
				    AND lease_fence=$5`,
				lease.ActionID, lease.TenantID, lease.UserID,
				lease.Owner, lease.Fence, blockReason,
			)
			if blockErr != nil {
				return agentEventDatabaseError(
					"block corrupt Agent action authority", blockErr)
			}
			if tag.RowsAffected() != 1 {
				return ErrAgentActionBusy
			}
			if commitErr := tx.Commit(ctx); commitErr != nil {
				return agentEventDatabaseError(
					"commit corrupt Agent action authority", commitErr)
			}
			return nil
		}
		return err
	}
	if action.SessionID != lease.SessionID ||
		action.SourceID != lease.SourceID {
		return agentEventIntegrityError()
	}
	if action.Status == AgentActionStatusCompleted {
		messages, err := terminalAgentActionMessages(action)
		if err != nil {
			return err
		}
		_, replayed, err := commitAgentSessionAppendTx(
			ctx, tx, action.TenantID, action.UserID, action.SessionID,
			"agent-action:enable-source:"+action.ActionID,
			json.RawMessage(messages), true,
		)
		if err != nil {
			return err
		}
		if !replayed {
			return agentEventIntegrityError()
		}
		if err := tx.Commit(ctx); err != nil {
			return agentEventDatabaseError(
				"commit Agent action projection replay", err)
		}
		return nil
	}
	if action.Status != AgentActionStatusConfirmed ||
		action.LeaseOwner == nil || *action.LeaseOwner != lease.Owner ||
		action.LeaseFence != lease.Fence ||
		action.LeaseExpiresAt == nil {
		return ErrAgentActionBusy
	}
	var databaseNow time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).
		Scan(&databaseNow); err != nil {
		return agentEventDatabaseError(
			"read Agent action projection clock", err)
	}
	if !databaseNow.Before(*action.LeaseExpiresAt) {
		return ErrAgentActionBusy
	}

	effectTx, err := tx.Begin(ctx)
	if err != nil {
		return agentEventDatabaseError(
			"begin Agent action effect savepoint", err)
	}
	tag, effectErr := effectTx.Exec(ctx,
		`UPDATE sources
		    SET status=$4,fail_count=0,next_fetch_at=clock_timestamp(),
		        updated_at=clock_timestamp()
		  WHERE id=$1
		    AND (
		      EXISTS (
		        SELECT 1 FROM subscriptions
		         WHERE tenant_id=$3 AND source_id=$1
		           AND user_id=$2 AND status=$5
		      )
		      OR EXISTS (
		        SELECT 1 FROM schedule_sources ss
		        JOIN schedules sc ON sc.id=ss.schedule_id
		         WHERE ss.source_id=$1 AND sc.tenant_id=$3
		           AND sc.user_id=$2
		      )
		    )`,
		action.SourceID, action.UserID, action.TenantID,
		types.SourceStatusActive, types.SubscriptionStatusActive,
	)
	if effectErr != nil {
		_ = effectTx.Rollback(context.WithoutCancel(ctx))
		return agentEventDatabaseError(
			"apply enable_source durable effect", effectErr)
	}
	terminalCode := agentActionTerminalNotFound
	messages := action.NotFoundMessages
	if tag.RowsAffected() == 1 {
		terminalCode = agentActionTerminalEnabled
		messages = action.SuccessMessages
	}
	_, _, appendErr := commitAgentSessionAppendTx(
		ctx, effectTx, action.TenantID, action.UserID, action.SessionID,
		"agent-action:enable-source:"+action.ActionID,
		json.RawMessage(messages), false,
	)
	if appendErr != nil {
		if rollbackErr := effectTx.Rollback(
			context.WithoutCancel(ctx),
		); rollbackErr != nil {
			return errors.Join(
				appendErr,
				agentEventDatabaseError(
					"rollback Agent action effect savepoint",
					rollbackErr),
			)
		}
		if !agentSessionFactShouldBlock(appendErr) {
			return appendErr
		}
		// A deterministic session-ledger integrity failure cannot safely
		// carry its own terminal fact. The effect savepoint above is fully
		// rolled back; only this explicit operator-visible blocked checkpoint
		// is committed so the action cannot retry the business effect.
		blockTag, blockErr := tx.Exec(ctx,
			`UPDATE agent_action_continuations
			    SET status='blocked',blocked_reason='projection_integrity',
			        lease_owner=NULL,lease_expires_at=NULL,
			        updated_at=clock_timestamp()
			  WHERE action_id=$1 AND tenant_id=$2 AND user_id=$3
			    AND status='confirmed' AND lease_owner=$4
			    AND lease_fence=$5`,
			lease.ActionID, lease.TenantID, lease.UserID,
			lease.Owner, lease.Fence,
		)
		if blockErr != nil {
			return agentEventDatabaseError(
				"block invalid Agent action projection", blockErr)
		}
		if blockTag.RowsAffected() != 1 {
			return ErrAgentActionBusy
		}
		if err := tx.Commit(ctx); err != nil {
			return agentEventDatabaseError(
				"commit blocked Agent action projection", err)
		}
		return nil
	}
	checkpointTag, err := effectTx.Exec(ctx,
		`UPDATE agent_action_continuations
		    SET status='completed',terminal_code=$6,
		        completed_at=clock_timestamp(),
		        lease_owner=NULL,lease_expires_at=NULL,
		        updated_at=clock_timestamp()
		  WHERE action_id=$1 AND tenant_id=$2 AND user_id=$3
		    AND status='confirmed' AND lease_owner=$4
		    AND lease_fence=$5`,
		lease.ActionID, lease.TenantID, lease.UserID,
		lease.Owner, lease.Fence, terminalCode,
	)
	if err != nil {
		_ = effectTx.Rollback(context.WithoutCancel(ctx))
		return agentEventDatabaseError(
			"checkpoint Agent action completion", err)
	}
	if checkpointTag.RowsAffected() != 1 {
		_ = effectTx.Rollback(context.WithoutCancel(ctx))
		return ErrAgentActionBusy
	}
	if err := effectTx.Commit(ctx); err != nil {
		return agentEventDatabaseError(
			"release Agent action effect savepoint", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return agentEventDatabaseError(
			"commit Agent action projection", err)
	}
	return nil
}

func (s *Store) ReleaseAgentActionContinuation(
	ctx context.Context,
	lease AgentActionContinuationLease,
	retryAfter time.Duration,
) error {
	if err := validateAgentActionLease(lease); err != nil {
		return err
	}
	if retryAfter <= 0 || retryAfter > 30*24*time.Hour {
		return agentEventValidationError(
			"Agent action retry boundary is invalid")
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return agentEventDatabaseError(
			"begin Agent action release", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := setAgentActionContinuatorContext(
		ctx, tx, lease.TenantID,
	); err != nil {
		return err
	}
	var rootVersion int
	var rootStatus string
	var rootSessionID *int64
	if err := tx.QueryRow(ctx,
		`SELECT execution_version,status,session_id FROM pending_actions
		  WHERE id=$1 AND tenant_id=$2 AND user_id=$3
		  FOR UPDATE`,
		lease.ActionID, lease.TenantID, lease.UserID,
	).Scan(&rootVersion, &rootStatus, &rootSessionID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return agentEventIntegrityError()
		}
		return agentEventDatabaseError(
			"lock Agent action release root", err)
	}
	if rootVersion != AgentActionExecutionVersion ||
		rootStatus != string(types.PendingActionStatusExecuted) {
		return agentEventIntegrityError()
	}
	action, err := scanAgentActionContinuation(tx.QueryRow(ctx,
		`SELECT `+agentActionContinuationColumns+`
		   FROM agent_action_continuations
		  WHERE action_id=$1 AND tenant_id=$2 AND user_id=$3
		  FOR UPDATE`,
		lease.ActionID, lease.TenantID, lease.UserID,
	))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return agentEventIntegrityError()
		}
		return agentEventDatabaseError(
			"lock Agent action release continuation", err)
	}
	if action.Status != AgentActionStatusConfirmed ||
		rootSessionID == nil || *rootSessionID != action.SessionID ||
		action.LeaseOwner == nil || *action.LeaseOwner != lease.Owner ||
		action.LeaseFence != lease.Fence {
		return agentEventIntegrityError()
	}
	if err := validateActiveAgentActionAuthority(
		ctx, tx, action.ActionID,
	); err != nil {
		if agentSessionFactShouldBlock(err) {
			blockReason, stageErr :=
				stageAgentActionBlockedSessionTx(
					ctx, tx, action, "authority_integrity",
				)
			if stageErr != nil {
				return stageErr
			}
			tag, blockErr := tx.Exec(ctx,
				`UPDATE agent_action_continuations
				    SET status='blocked',
				        blocked_reason=$6,
				        lease_owner=NULL,lease_expires_at=NULL,
				        updated_at=clock_timestamp()
				  WHERE action_id=$1 AND tenant_id=$2 AND user_id=$3
				    AND status='confirmed' AND lease_owner=$4
				    AND lease_fence=$5`,
				lease.ActionID, lease.TenantID, lease.UserID,
				lease.Owner, lease.Fence, blockReason,
			)
			if blockErr != nil {
				return agentEventDatabaseError(
					"block corrupt Agent action authority release", blockErr)
			}
			if tag.RowsAffected() != 1 {
				return ErrAgentActionBusy
			}
			if commitErr := tx.Commit(ctx); commitErr != nil {
				return agentEventDatabaseError(
					"commit corrupt Agent action authority release", commitErr)
			}
			return nil
		}
		return err
	}
	if err := validateFrozenAgentAction(action); err != nil {
		return err
	}
	tag, err := tx.Exec(ctx,
		`UPDATE agent_action_continuations
		    SET lease_owner=NULL,lease_expires_at=NULL,
		        next_attempt_at=clock_timestamp()+
		            ($6*interval '1 microsecond'),
		        updated_at=clock_timestamp()
		  WHERE action_id=$1 AND tenant_id=$2 AND user_id=$3
		    AND status='confirmed' AND lease_owner=$4
		    AND lease_fence=$5`,
		lease.ActionID, lease.TenantID, lease.UserID,
		lease.Owner, lease.Fence, retryAfter.Microseconds(),
	)
	if err != nil {
		return agentEventDatabaseError(
			"release Agent action", err)
	}
	if tag.RowsAffected() != 1 {
		return ErrAgentActionBusy
	}
	if err := tx.Commit(ctx); err != nil {
		return agentEventDatabaseError(
			"commit Agent action release", err)
	}
	return nil
}

func validateAgentActionLease(
	lease AgentActionContinuationLease,
) error {
	if lease.ActionID == "" || lease.TenantID <= 0 ||
		lease.UserID <= 0 || lease.SessionID <= 0 ||
		lease.SourceID <= 0 || lease.Owner == "" || lease.Fence <= 0 {
		return agentEventValidationError(
			"Agent action lease is invalid")
	}
	return nil
}

func validateFrozenAgentAction(
	action AgentActionContinuation,
) error {
	if action.ActionID == "" || action.TenantID <= 0 ||
		action.UserID <= 0 || action.SessionID <= 0 ||
		action.SourceID <= 0 ||
		action.ToolSpecVersion != agentActionToolSpecVersion ||
		action.ToolPolicyVersion != agentActionToolPolicyVersion ||
		action.AdapterVersion != agentActionAdapterVersion {
		return agentEventIntegrityError()
	}
	sourceID, canonicalArgs, err := canonicalEnableSourceArgs(
		action.CanonicalArgs,
	)
	if err != nil || sourceID != action.SourceID ||
		string(canonicalArgs) != string(action.CanonicalArgs) {
		return agentEventIntegrityError()
	}
	frozen, err := freezeEnableSourceAction(action.SourceID)
	if err != nil {
		return agentEventIntegrityError()
	}
	payloads := []struct {
		got    []byte
		want   []byte
		digest string
	}{
		{action.CanonicalArgs, canonicalArgs, action.ArgsDigest},
		{action.ToolSpec, frozen.toolSpec, action.ToolSpecDigest},
		{action.ToolPolicy, frozen.toolPolicy, action.ToolPolicyDigest},
		{action.SuccessMessages, frozen.successMessages, action.SuccessDigest},
		{action.NotFoundMessages, frozen.notFoundMessages, action.NotFoundDigest},
	}
	for _, payload := range payloads {
		if string(payload.got) != string(payload.want) ||
			!constantTimeAgentEventDigestEqual(
				AgentActionPayloadDigest(payload.got), payload.digest,
			) {
			return agentEventIntegrityError()
		}
	}
	return nil
}

func terminalAgentActionMessages(
	action AgentActionContinuation,
) ([]byte, error) {
	if action.TerminalCode == nil {
		return nil, agentEventIntegrityError()
	}
	switch *action.TerminalCode {
	case agentActionTerminalEnabled:
		return action.SuccessMessages, nil
	case agentActionTerminalNotFound:
		return action.NotFoundMessages, nil
	default:
		return nil, agentEventIntegrityError()
	}
}

func commitAgentActionTerminalSessionTx(
	ctx context.Context,
	tx pgx.Tx,
	action AgentActionContinuation,
	status string,
	replayOnly bool,
) error {
	var messages []byte
	switch status {
	case AgentActionStatusCompleted:
		var err error
		messages, err = terminalAgentActionMessages(action)
		if err != nil {
			return err
		}
	case AgentActionStatusCancelled:
		messages = []byte(agentActionCancelledSessionMessages)
	case AgentActionStatusExpired:
		messages = []byte(agentActionExpiredSessionMessages)
	case AgentActionStatusBlocked:
		messages = []byte(agentActionBlockedSessionMessages)
	default:
		return agentEventIntegrityError()
	}
	_, replayed, err := commitAgentSessionAppendTx(
		ctx, tx, action.TenantID, action.UserID, action.SessionID,
		"agent-action:enable-source:"+action.ActionID,
		json.RawMessage(messages), replayOnly,
	)
	if err != nil {
		return err
	}
	if replayOnly && !replayed {
		return agentEventIntegrityError()
	}
	return nil
}

func stageAgentActionBlockedSessionTx(
	ctx context.Context,
	tx pgx.Tx,
	action AgentActionContinuation,
	reason string,
) (string, error) {
	switch reason {
	case "authority_integrity", "payload_integrity":
	default:
		return "", agentEventIntegrityError()
	}
	factTx, err := tx.Begin(ctx)
	if err != nil {
		return "", agentEventDatabaseError(
			"begin Agent action blocked session savepoint", err)
	}
	if appendErr := commitAgentActionTerminalSessionTx(
		ctx, factTx, action, AgentActionStatusBlocked, false,
	); appendErr != nil {
		if rollbackErr := factTx.Rollback(
			context.WithoutCancel(ctx),
		); rollbackErr != nil {
			return "", errors.Join(
				appendErr,
				agentEventDatabaseError(
					"rollback Agent action blocked session savepoint",
					rollbackErr),
			)
		}
		if agentSessionFactShouldBlock(appendErr) {
			return "projection_integrity", nil
		}
		return "", appendErr
	}
	if err := factTx.Commit(ctx); err != nil {
		return "", agentEventDatabaseError(
			"release Agent action blocked session savepoint", err)
	}
	return reason, nil
}

func agentActionProjectionError(action string, err error) error {
	return fmt.Errorf("%s: %w", action, err)
}
