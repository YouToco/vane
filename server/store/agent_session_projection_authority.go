package store

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/server/agentledger"
)

type AgentSessionProjectionRoute string

const (
	AgentSessionProjectionRouteLegacy AgentSessionProjectionRoute = "legacy"
	AgentSessionProjectionRouteLedger AgentSessionProjectionRoute = "ledger"
)

type AgentSessionProjectionAuthorityAction string

const (
	AgentSessionProjectionAuthorityActivate AgentSessionProjectionAuthorityAction = "activate"
	AgentSessionProjectionAuthorityRollback AgentSessionProjectionAuthorityAction = "rollback"
)

func (action AgentSessionProjectionAuthorityAction) valid() bool {
	return action == AgentSessionProjectionAuthorityActivate ||
		action == AgentSessionProjectionAuthorityRollback
}

type AgentSessionProjectionAuthorityStatus struct {
	TenantID           int64                       `json:"tenant_id"`
	UserID             int64                       `json:"user_id"`
	SessionID          int64                       `json:"session_id"`
	Route              AgentSessionProjectionRoute `json:"route"`
	Generation         int64                       `json:"generation"`
	EventID            int64                       `json:"event_id,omitempty"`
	LedgerHeadSequence int64                       `json:"ledger_head_sequence,omitempty"`
}

type agentSessionProjectionAuthorityEvent struct {
	ID                 int64
	Generation         int64
	Action             AgentSessionProjectionAuthorityAction
	LedgerHeadSequence int64
	LegacyDigest       string
	LedgerDigest       string
}

func agentSessionProjectionLedgerAuthoritative(
	ctx context.Context,
	tx pgx.Tx,
	scope agentledger.Scope,
) (bool, error) {
	status, err := loadAgentSessionProjectionAuthorityStatus(ctx, tx, scope)
	if err != nil {
		return false, err
	}
	if err := validateAgentSessionProjectionAuthorityEvidence(
		ctx, tx, scope, status.Generation,
	); err != nil {
		return false, err
	}
	return status.Route == AgentSessionProjectionRouteLedger, nil
}

func loadAgentSessionProjectionAuthorityStatus(
	ctx context.Context,
	tx pgx.Tx,
	scope agentledger.Scope,
) (AgentSessionProjectionAuthorityStatus, error) {
	status := AgentSessionProjectionAuthorityStatus{
		TenantID: scope.TenantID, UserID: scope.UserID,
		SessionID: scope.SessionID,
		Route:     AgentSessionProjectionRouteLegacy,
	}
	rows, err := tx.Query(ctx,
		`SELECT id,generation,action,ledger_head_sequence,
		        legacy_digest,ledger_digest
		   FROM public.agent_session_projection_authority_events
		  WHERE tenant_id=$1 AND user_id=$2 AND session_id=$3
		  ORDER BY generation`,
		scope.TenantID, scope.UserID, scope.SessionID,
	)
	if err != nil {
		return AgentSessionProjectionAuthorityStatus{},
			agentEventDatabaseError(
				"load agent session projection authority", err)
	}
	defer rows.Close()

	var previousAction AgentSessionProjectionAuthorityAction
	var previousHead int64
	for rows.Next() {
		var event agentSessionProjectionAuthorityEvent
		var rawAction string
		if err := rows.Scan(
			&event.ID, &event.Generation, &rawAction,
			&event.LedgerHeadSequence,
			&event.LegacyDigest, &event.LedgerDigest,
		); err != nil {
			return AgentSessionProjectionAuthorityStatus{},
				agentEventDatabaseError(
					"scan agent session projection authority", err)
		}
		event.Action = AgentSessionProjectionAuthorityAction(rawAction)
		if event.ID <= 0 || event.Generation != status.Generation+1 ||
			!event.Action.valid() || event.LedgerHeadSequence <= 0 ||
			(previousHead > 0 &&
				event.LedgerHeadSequence < previousHead) ||
			!validAgentEventDigest(event.LegacyDigest) ||
			!constantTimeAgentEventDigestEqual(
				event.LegacyDigest, event.LedgerDigest) ||
			(event.Generation == 1 &&
				event.Action != AgentSessionProjectionAuthorityActivate) ||
			(previousAction != "" && event.Action == previousAction) {
			return AgentSessionProjectionAuthorityStatus{},
				agentEventIntegrityError()
		}
		status.Generation = event.Generation
		status.EventID = event.ID
		status.LedgerHeadSequence = event.LedgerHeadSequence
		if event.Action == AgentSessionProjectionAuthorityActivate {
			status.Route = AgentSessionProjectionRouteLedger
		} else {
			status.Route = AgentSessionProjectionRouteLegacy
		}
		previousAction = event.Action
		previousHead = event.LedgerHeadSequence
	}
	if err := rows.Err(); err != nil {
		return AgentSessionProjectionAuthorityStatus{},
			agentEventDatabaseError(
				"iterate agent session projection authority", err)
	}
	return status, nil
}

func (s *Store) GetAgentSessionProjectionAuthorityStatus(
	ctx context.Context,
	tenantID, userID, sessionID int64,
) (AgentSessionProjectionAuthorityStatus, error) {
	scope := agentledger.Scope{
		TenantID: tenantID, UserID: userID, SessionID: sessionID,
	}
	if err := validateAgentEventScope(scope); err != nil {
		return AgentSessionProjectionAuthorityStatus{}, err
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if err != nil {
		return AgentSessionProjectionAuthorityStatus{},
			agentEventDatabaseError(
				"begin agent session projection authority status", err)
	}
	defer func() {
		_ = tx.Rollback(context.WithoutCancel(ctx))
	}()
	if err := setAgentEventTenantContext(ctx, tx, tenantID); err != nil {
		return AgentSessionProjectionAuthorityStatus{}, err
	}
	legacy, err := loadAgentSessionProjection(ctx, tx, scope, false)
	if err != nil {
		return AgentSessionProjectionAuthorityStatus{}, err
	}
	status, err := loadAgentSessionProjectionAuthorityStatus(ctx, tx, scope)
	if err != nil {
		return AgentSessionProjectionAuthorityStatus{}, err
	}
	if err := validateAgentSessionProjectionAuthorityEvidence(
		ctx, tx, scope, status.Generation,
	); err != nil {
		return AgentSessionProjectionAuthorityStatus{}, err
	}
	if status.Route == AgentSessionProjectionRouteLedger {
		if _, err := loadVerifiedLedgerSessionProjection(
			ctx, tx, scope, legacy,
		); err != nil {
			return AgentSessionProjectionAuthorityStatus{}, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return AgentSessionProjectionAuthorityStatus{},
			agentEventDatabaseError(
				"commit agent session projection authority status", err)
	}
	return status, nil
}

func (s *Store) ControlAgentSessionProjectionAuthority(
	ctx context.Context,
	tenantID, userID, sessionID int64,
	action AgentSessionProjectionAuthorityAction,
) (AgentSessionProjectionAuthorityStatus, error) {
	scope := agentledger.Scope{
		TenantID: tenantID, UserID: userID, SessionID: sessionID,
	}
	if err := validateAgentEventScope(scope); err != nil {
		return AgentSessionProjectionAuthorityStatus{}, err
	}
	if !action.valid() {
		return AgentSessionProjectionAuthorityStatus{},
			agentEventValidationError(
				"agent session projection authority action is invalid")
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return AgentSessionProjectionAuthorityStatus{},
			agentEventDatabaseError(
				"begin agent session projection authority control", err)
	}
	defer func() {
		_ = tx.Rollback(context.WithoutCancel(ctx))
	}()
	if err := setAgentEventTenantContext(ctx, tx, tenantID); err != nil {
		return AgentSessionProjectionAuthorityStatus{}, err
	}
	legacy, err := loadAgentSessionProjection(ctx, tx, scope, true)
	if err != nil {
		return AgentSessionProjectionAuthorityStatus{}, err
	}
	status, err := loadAgentSessionProjectionAuthorityStatus(ctx, tx, scope)
	if err != nil {
		return AgentSessionProjectionAuthorityStatus{}, err
	}
	if err := validateAgentSessionProjectionAuthorityEvidence(
		ctx, tx, scope, status.Generation,
	); err != nil {
		return AgentSessionProjectionAuthorityStatus{}, err
	}
	targetRoute := AgentSessionProjectionRouteLedger
	if action == AgentSessionProjectionAuthorityRollback {
		targetRoute = AgentSessionProjectionRouteLegacy
	}
	if status.Route == targetRoute {
		if status.Route == AgentSessionProjectionRouteLedger {
			if _, err := loadVerifiedLedgerSessionProjection(
				ctx, tx, scope, legacy,
			); err != nil {
				return AgentSessionProjectionAuthorityStatus{}, err
			}
		}
		if err := tx.Commit(ctx); err != nil {
			return AgentSessionProjectionAuthorityStatus{},
				agentEventDatabaseError(
					"commit replayed agent session projection authority", err)
		}
		return status, nil
	}

	events, err := loadCompleteAgentEventLedger(ctx, tx, scope)
	if err != nil {
		return AgentSessionProjectionAuthorityStatus{}, err
	}
	projected, err := verifiedLedgerSessionProjection(legacy, events)
	if err != nil {
		return AgentSessionProjectionAuthorityStatus{}, err
	}
	digest, err := agentledger.ProjectionDigest(projected)
	if err != nil || len(events) == 0 {
		return AgentSessionProjectionAuthorityStatus{},
			agentEventIntegrityError()
	}
	head := events[len(events)-1].Sequence
	if head <= 0 {
		return AgentSessionProjectionAuthorityStatus{},
			agentEventIntegrityError()
	}
	if err := validateAgentSessionProjectionOperator(ctx, tx); err != nil {
		return AgentSessionProjectionAuthorityStatus{}, err
	}
	if _, err := tx.Exec(ctx,
		`SET LOCAL ROLE vane_agent_session_projection_operator`); err != nil {
		return AgentSessionProjectionAuthorityStatus{},
			agentEventDatabaseError(
				"enter agent session projection operator", err)
	}
	next := AgentSessionProjectionAuthorityStatus{
		TenantID: tenantID, UserID: userID, SessionID: sessionID,
		Route: targetRoute, Generation: status.Generation + 1,
		LedgerHeadSequence: head,
	}
	if _, err := tx.Exec(ctx,
		`SET LOCAL search_path = pg_catalog, public`); err != nil {
		return AgentSessionProjectionAuthorityStatus{},
			agentEventDatabaseError(
				"fix agent session projection operator search path", err)
	}
	if err := tx.QueryRow(ctx,
		`INSERT INTO public.agent_session_projection_authority_events (
		    tenant_id,user_id,session_id,generation,action,
		    ledger_head_sequence,legacy_digest,ledger_digest
		 ) VALUES ($1,$2,$3,$4,$5,$6,$7,$7)
		 RETURNING id`,
		tenantID, userID, sessionID, next.Generation,
		string(action), head, digest,
	).Scan(&next.EventID); err != nil {
		return AgentSessionProjectionAuthorityStatus{},
			agentEventDatabaseError(
				"append agent session projection authority", err)
	}
	if next.EventID <= 0 {
		return AgentSessionProjectionAuthorityStatus{},
			agentEventIntegrityError()
	}
	if err := tx.Commit(ctx); err != nil {
		return AgentSessionProjectionAuthorityStatus{},
			agentEventDatabaseError(
				"commit agent session projection authority", err)
	}
	return next, nil
}

// validateAgentSessionProjectionAuthorityEvidence proves that every immutable
// authority generation names a complete ledger prefix and that the latest
// session projection at that head matches its recorded digest. Checking only
// that the two stored digests equal each other would let an owner-side mutation
// rewrite both to an arbitrary well-formed value without changing the route.
func validateAgentSessionProjectionAuthorityEvidence(
	ctx context.Context,
	tx pgx.Tx,
	scope agentledger.Scope,
	generation int64,
) error {
	if generation == 0 {
		return nil
	}
	events, err := loadCompleteAgentEventLedger(ctx, tx, scope)
	if err != nil {
		return err
	}
	rows, err := tx.Query(ctx,
		`SELECT generation,ledger_head_sequence,
		        legacy_digest,ledger_digest
		   FROM public.agent_session_projection_authority_events
		  WHERE tenant_id=$1 AND user_id=$2 AND session_id=$3
		  ORDER BY generation`,
		scope.TenantID, scope.UserID, scope.SessionID,
	)
	if err != nil {
		return agentEventDatabaseError(
			"load agent session projection authority evidence", err)
	}
	defer rows.Close()
	var count int64
	for rows.Next() {
		var storedGeneration, head int64
		var legacyDigest, ledgerDigest string
		if err := rows.Scan(
			&storedGeneration, &head, &legacyDigest, &ledgerDigest,
		); err != nil {
			return agentEventDatabaseError(
				"scan agent session projection authority evidence", err)
		}
		count++
		if storedGeneration != count || head <= 0 ||
			head > int64(len(events)) ||
			events[head-1].Sequence != head {
			return agentEventIntegrityError()
		}
		projected, err := agentledger.ProjectLatestSessionSnapshot(events[:head])
		if err != nil {
			return agentEventIntegrityError()
		}
		digest, err := agentledger.ProjectionDigest(projected)
		if err != nil ||
			!constantTimeAgentEventDigestEqual(digest, legacyDigest) ||
			!constantTimeAgentEventDigestEqual(digest, ledgerDigest) {
			return agentEventIntegrityError()
		}
	}
	if err := rows.Err(); err != nil {
		return agentEventDatabaseError(
			"iterate agent session projection authority evidence", err)
	}
	if count != generation {
		return agentEventIntegrityError()
	}
	return nil
}

func loadAgentSessionProjection(
	ctx context.Context,
	tx pgx.Tx,
	scope agentledger.Scope,
	forUpdate bool,
) (agentledger.SessionProjection, error) {
	query := `SELECT messages,turn_count,activated_tools
	            FROM agent_sessions
	           WHERE id=$1 AND tenant_id=$2 AND user_id=$3`
	if forUpdate {
		query += ` FOR UPDATE /* agent projection authority session root */`
	}
	var projection agentledger.SessionProjection
	err := tx.QueryRow(ctx, query,
		scope.SessionID, scope.TenantID, scope.UserID,
	).Scan(
		&projection.Messages,
		&projection.TurnCount,
		&projection.ActivatedTools,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return agentledger.SessionProjection{}, agentEventNotFound()
	}
	if err != nil {
		return agentledger.SessionProjection{},
			agentEventDatabaseError(
				"load agent session projection authority root", err)
	}
	return projection, nil
}

func validateAgentSessionProjectionOperator(
	ctx context.Context,
	tx pgx.Tx,
) error {
	var valid bool
	err := tx.QueryRow(ctx, `
		SELECT
		  NOT op.rolsuper AND NOT op.rolcreatedb AND NOT op.rolcreaterole AND
		  NOT op.rolcanlogin AND NOT op.rolinherit AND NOT op.rolreplication AND
		  NOT op.rolbypassrls AND
		  op.rolconfig =
		    ARRAY['search_path=pg_catalog, public']::TEXT[] AND
		  pg_has_role(CURRENT_USER, op.oid, 'SET') AND
		  NOT pg_has_role('vane_app', op.oid, 'MEMBER') AND
		  NOT pg_has_role(op.oid, 'vane_app', 'MEMBER') AND
		  has_schema_privilege(op.oid, 'public', 'USAGE') AND
		  NOT has_schema_privilege(op.oid, 'public', 'CREATE') AND
		  NOT EXISTS (
		    SELECT 1
		      FROM pg_namespace n
		     WHERE n.nspname <> 'public'
		       AND n.nspname <> 'information_schema'
		       AND n.nspname NOT LIKE 'pg_%'
		       AND (
		         n.nspowner = op.oid OR
		         has_schema_privilege(op.oid, n.oid, 'USAGE') OR
		         has_schema_privilege(op.oid, n.oid, 'CREATE')
		       )
		  ) AND
		  has_table_privilege(op.oid, 'agent_events', 'SELECT') AND
		  NOT has_table_privilege(
		    op.oid, 'agent_events',
		    'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER'
		  ) AND
		  has_table_privilege(
		    op.oid, 'agent_session_projection_authority_events', 'SELECT'
		  ) AND
		  NOT has_table_privilege(
		    op.oid, 'agent_session_projection_authority_events',
		    'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER'
		  ) AND
		  has_sequence_privilege(
		    op.oid,
		    'agent_session_projection_authority_events_id_seq',
		    'USAGE'
		  ) AND
		  NOT has_sequence_privilege(
		    op.oid,
		    'agent_session_projection_authority_events_id_seq',
		    'SELECT,UPDATE'
		  ) AND
		  ARRAY(
		    SELECT a.attname::TEXT
		      FROM pg_attribute a
		     WHERE a.attrelid = 'agent_sessions'::regclass
		       AND a.attnum > 0 AND NOT a.attisdropped
		       AND has_column_privilege(
		             op.oid, a.attrelid, a.attname, 'SELECT'
		           )
		     ORDER BY a.attname
		  ) = ARRAY[
		    'activated_tools','id','messages','tenant_id','turn_count','user_id'
		  ]::TEXT[] AND
		  ARRAY(
		    SELECT a.attname::TEXT
		      FROM pg_attribute a
		     WHERE a.attrelid =
		             'agent_session_projection_authority_events'::regclass
		       AND a.attnum > 0 AND NOT a.attisdropped
		       AND has_column_privilege(
		             op.oid, a.attrelid, a.attname, 'INSERT'
		           )
		     ORDER BY a.attname
		  ) = ARRAY[
		    'action','generation','ledger_digest','ledger_head_sequence',
		    'legacy_digest','session_id','tenant_id','user_id'
		  ]::TEXT[] AND
		  NOT EXISTS (
		    SELECT 1
		      FROM pg_class c
		      JOIN pg_namespace n ON n.oid = c.relnamespace
		     WHERE n.nspname = 'public' AND c.relkind IN ('r','p','v','m','f')
		       AND (
		         has_table_privilege(
		           op.oid, c.oid,
		           'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER'
		         ) OR (
		           has_table_privilege(op.oid, c.oid, 'SELECT') AND
		           c.oid NOT IN (
		             'agent_events'::regclass,
		             'agent_session_projection_authority_events'::regclass
		           )
		         )
		       )
		  ) AND
		  NOT EXISTS (
		    SELECT 1
		      FROM pg_attribute a
		      JOIN pg_class c ON c.oid = a.attrelid
		      JOIN pg_namespace n ON n.oid = c.relnamespace
		     WHERE n.nspname = 'public' AND c.relkind IN ('r','p','v','m','f')
		       AND a.attnum > 0 AND NOT a.attisdropped
		       AND has_column_privilege(op.oid, a.attrelid, a.attname, 'UPDATE')
		  ) AND
		  NOT EXISTS (
		    SELECT 1
		      FROM pg_attribute a
		      JOIN pg_class c ON c.oid = a.attrelid
		      JOIN pg_namespace n ON n.oid = c.relnamespace
		     WHERE n.nspname = 'public' AND c.relkind IN ('r','p','v','m','f')
		       AND a.attnum > 0 AND NOT a.attisdropped
		       AND has_column_privilege(op.oid, a.attrelid, a.attname, 'SELECT')
		       AND NOT (
		         c.oid IN (
		           'agent_events'::regclass,
		           'agent_session_projection_authority_events'::regclass
		         ) OR (
		           c.oid = 'agent_sessions'::regclass AND
		           a.attname IN (
		             'activated_tools','id','messages',
		             'tenant_id','turn_count','user_id'
		           )
		         )
		       )
		  ) AND
		  NOT EXISTS (
		    SELECT 1
		      FROM pg_attribute a
		      JOIN pg_class c ON c.oid = a.attrelid
		      JOIN pg_namespace n ON n.oid = c.relnamespace
		     WHERE n.nspname = 'public' AND c.relkind IN ('r','p','v','m','f')
		       AND a.attnum > 0 AND NOT a.attisdropped
		       AND has_column_privilege(op.oid, a.attrelid, a.attname, 'INSERT')
		       AND NOT (
		         c.oid =
		           'agent_session_projection_authority_events'::regclass AND
		         a.attname IN (
		           'action','generation','ledger_digest',
		           'ledger_head_sequence','legacy_digest',
		           'session_id','tenant_id','user_id'
		         )
		       )
		  ) AND
		  NOT EXISTS (
		    SELECT 1
		      FROM pg_class c
		      JOIN pg_namespace n ON n.oid = c.relnamespace
		     WHERE n.nspname = 'public'
		       AND c.oid <>
		         'agent_session_projection_authority_events_id_seq'::regclass
		       AND CASE
		             WHEN c.relkind = 'S' THEN has_sequence_privilege(
		               op.oid, c.oid, 'USAGE,SELECT,UPDATE'
		             )
		             ELSE FALSE
		           END
		  ) AND
		  NOT EXISTS (
		    SELECT 1
		      FROM pg_proc p
		      JOIN pg_namespace n ON n.oid = p.pronamespace
		     WHERE n.nspname = 'public' AND p.prosecdef
		       AND has_function_privilege(op.oid, p.oid, 'EXECUTE')
		  ) AND
		  NOT EXISTS (
		    SELECT 1
		      FROM pg_auth_members am
		     WHERE am.roleid = op.oid
		       AND am.member <> (SELECT oid FROM pg_roles
		                           WHERE rolname = CURRENT_USER)
		  ) AND
		  EXISTS (
		    SELECT 1
		      FROM pg_auth_members am
		     WHERE am.roleid = op.oid
		       AND am.member = (SELECT oid FROM pg_roles
		                          WHERE rolname = CURRENT_USER)
		  ) AND
		  NOT EXISTS (
		    SELECT 1 FROM pg_auth_members am WHERE am.member = op.oid
		  )
		  FROM pg_roles op
		 WHERE op.rolname = 'vane_agent_session_projection_operator'`,
	).Scan(&valid)
	if err != nil || !valid {
		return agentEventIntegrityError()
	}
	return nil
}
