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
	"unicode/utf8"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/types"
)

const (
	agentSessionFactTypeFeedback = "feedback"

	AgentSessionFactStatusPending    = "pending"
	AgentSessionFactStatusCompleted  = "completed"
	AgentSessionFactStatusSuppressed = "suppressed"
	AgentSessionFactStatusBlocked    = "blocked"

	agentSessionFactSuppressedNoActive = "no_active_session"
	maxAgentSessionFactLease           = 24 * time.Hour
	maxAgentSessionFactPage            = 1000
)

var (
	ErrAgentSessionFactBusy     = errors.New("agent session fact is busy")
	ErrAgentSessionFactTerminal = errors.New("agent session fact is terminal")
)

type AgentSessionFact struct {
	ID                int64
	TenantID          int64
	UserID            int64
	FactID            int64
	SourceIdentity    string
	SessionID         *int64
	SessionMessages   []byte
	PayloadDigest     *string
	Status            string
	SuppressionReason *string
	LeaseOwner        *string
	LeaseFence        int64
	LeaseExpiresAt    *time.Time
	AttemptCount      int
	NextAttemptAt     time.Time
	SessionRecordedAt *time.Time
	BlockedReason     *string
}

type AcquireAgentSessionFactParams struct {
	ID            int64
	TenantID      int64
	UserID        int64
	LeaseOwner    string
	LeaseDuration time.Duration
}

type AgentSessionFactLease struct {
	ID        int64
	TenantID  int64
	UserID    int64
	FactID    int64
	SessionID int64
	Owner     string
	Fence     int64
	Messages  []byte
	Digest    string
	Source    string
}

const agentSessionFactColumns = `id,tenant_id,user_id,fact_id,
	source_identity,session_id,session_messages,payload_digest,status,
	suppression_reason,lease_owner,lease_fence,lease_expires_at,
	attempt_count,next_attempt_at,session_recorded_at,blocked_reason`

type agentSessionFactScanner interface {
	Scan(...any) error
}

func scanAgentSessionFact(row agentSessionFactScanner) (AgentSessionFact, error) {
	var fact AgentSessionFact
	err := row.Scan(
		&fact.ID, &fact.TenantID, &fact.UserID, &fact.FactID,
		&fact.SourceIdentity, &fact.SessionID, &fact.SessionMessages,
		&fact.PayloadDigest, &fact.Status, &fact.SuppressionReason,
		&fact.LeaseOwner, &fact.LeaseFence, &fact.LeaseExpiresAt,
		&fact.AttemptCount, &fact.NextAttemptAt, &fact.SessionRecordedAt,
		&fact.BlockedReason,
	)
	return fact, err
}

func (s *Store) ListDueAgentSessionFactTenantIDs(
	ctx context.Context,
	before time.Time,
	afterTenantID int64,
	limit int,
) ([]int64, error) {
	if before.IsZero() || afterTenantID < 0 || limit <= 0 ||
		limit > maxAgentSessionFactPage {
		return nil, agentEventValidationError(
			"agent session fact tenant page is invalid")
	}
	rows, err := s.pool.Query(ctx,
		`SELECT DISTINCT tenant_id
		   FROM agent_session_fact_outbox
		  WHERE status='pending' AND tenant_id>$2
		    AND next_attempt_at<=LEAST($1,clock_timestamp())
		    AND (lease_expires_at IS NULL OR
		         lease_expires_at<=clock_timestamp())
		  ORDER BY tenant_id LIMIT $3`,
		before, afterTenantID, limit,
	)
	if err != nil {
		return nil, agentEventDatabaseError(
			"list due agent session fact tenants", err)
	}
	defer rows.Close()
	var tenantIDs []int64
	for rows.Next() {
		var tenantID int64
		if err := rows.Scan(&tenantID); err != nil {
			return nil, agentEventDatabaseError(
				"scan due agent session fact tenant", err)
		}
		tenantIDs = append(tenantIDs, tenantID)
	}
	if err := rows.Err(); err != nil {
		return nil, agentEventDatabaseError(
			"iterate due agent session fact tenants", err)
	}
	return tenantIDs, nil
}

func (s *Store) ListDueAgentSessionFacts(
	ctx context.Context,
	tenantID int64,
	before time.Time,
	limit int,
) ([]AgentSessionFact, error) {
	if tenantID <= 0 || before.IsZero() || limit <= 0 ||
		limit > maxAgentSessionFactPage {
		return nil, agentEventValidationError(
			"agent session fact page is invalid")
	}
	rows, err := s.pool.Query(ctx,
		`SELECT `+agentSessionFactColumns+`
		   FROM agent_session_fact_outbox
		  WHERE tenant_id=$1 AND status='pending'
		    AND next_attempt_at<=LEAST($2,clock_timestamp())
		    AND (lease_expires_at IS NULL OR
		         lease_expires_at<=clock_timestamp())
		  ORDER BY next_attempt_at,id LIMIT $3`,
		tenantID, before, limit,
	)
	if err != nil {
		return nil, agentEventDatabaseError(
			"list due agent session facts", err)
	}
	defer rows.Close()
	facts := make([]AgentSessionFact, 0)
	for rows.Next() {
		fact, err := scanAgentSessionFact(rows)
		if err != nil {
			return nil, agentEventDatabaseError(
				"scan due agent session fact", err)
		}
		facts = append(facts, fact)
	}
	if err := rows.Err(); err != nil {
		return nil, agentEventDatabaseError(
			"iterate due agent session facts", err)
	}
	return facts, nil
}

func (s *Store) AcquireAgentSessionFact(
	ctx context.Context,
	params AcquireAgentSessionFactParams,
) (*AgentSessionFact, error) {
	if params.ID <= 0 || params.TenantID <= 0 || params.UserID <= 0 ||
		params.LeaseDuration <= 0 ||
		params.LeaseDuration > maxAgentSessionFactLease ||
		!validAgentSessionFactOwner(params.LeaseOwner) {
		return nil, agentEventValidationError(
			"agent session fact acquisition is invalid")
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, agentEventDatabaseError(
			"begin agent session fact acquisition", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := setAgentSessionFactProjectorContext(
		ctx, tx, params.TenantID,
	); err != nil {
		return nil, err
	}
	fact, err := scanAgentSessionFact(tx.QueryRow(ctx,
		`SELECT `+agentSessionFactColumns+`
		   FROM agent_session_fact_outbox
		  WHERE id=$1 AND tenant_id=$2 AND user_id=$3
		  FOR UPDATE`,
		params.ID, params.TenantID, params.UserID,
	))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, agentEventNotFound()
	}
	if err != nil {
		return nil, agentEventDatabaseError(
			"lock agent session fact", err)
	}
	if fact.Status != AgentSessionFactStatusPending {
		return nil, ErrAgentSessionFactTerminal
	}
	var databaseNow time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).
		Scan(&databaseNow); err != nil {
		return nil, agentEventDatabaseError(
			"read agent session fact database clock", err)
	}
	if databaseNow.Before(fact.NextAttemptAt) ||
		(fact.LeaseExpiresAt != nil &&
			databaseNow.Before(*fact.LeaseExpiresAt) &&
			(fact.LeaseOwner == nil ||
				*fact.LeaseOwner != params.LeaseOwner)) {
		return nil, ErrAgentSessionFactBusy
	}
	if fact.LeaseExpiresAt != nil &&
		databaseNow.Before(*fact.LeaseExpiresAt) &&
		fact.LeaseOwner != nil &&
		*fact.LeaseOwner == params.LeaseOwner {
		if err := tx.Commit(ctx); err != nil {
			return nil, agentEventDatabaseError(
				"commit agent session fact lease replay", err)
		}
		return &fact, nil
	}
	fact.LeaseFence++
	fact.AttemptCount++
	updated, err := scanAgentSessionFact(tx.QueryRow(ctx,
		`UPDATE agent_session_fact_outbox
		    SET lease_owner=$4,
		        lease_expires_at=clock_timestamp()+
		            ($6*interval '1 microsecond'),
		        lease_fence=$5+1,attempt_count=attempt_count+1,
		        updated_at=clock_timestamp()
		  WHERE id=$1 AND tenant_id=$2 AND user_id=$3
		    AND status='pending' AND lease_fence=$5
		    AND next_attempt_at<=clock_timestamp()
		    AND (lease_expires_at IS NULL OR
		         lease_expires_at<=clock_timestamp())
		  RETURNING `+agentSessionFactColumns,
		params.ID, params.TenantID, params.UserID, params.LeaseOwner,
		fact.LeaseFence-1, params.LeaseDuration.Microseconds(),
	))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrAgentSessionFactBusy
	}
	if err != nil {
		return nil, agentEventDatabaseError(
			"acquire agent session fact", err)
	}
	fact = updated
	if err := tx.Commit(ctx); err != nil {
		return nil, agentEventDatabaseError(
			"commit agent session fact acquisition", err)
	}
	return &fact, nil
}

func (fact AgentSessionFact) Lease() (AgentSessionFactLease, error) {
	if fact.SessionID == nil || fact.PayloadDigest == nil ||
		fact.LeaseOwner == nil {
		return AgentSessionFactLease{},
			agentEventValidationError(
				"agent session fact lease payload is incomplete")
	}
	return AgentSessionFactLease{
		ID: fact.ID, TenantID: fact.TenantID, UserID: fact.UserID,
		FactID: fact.FactID, SessionID: *fact.SessionID,
		Owner: *fact.LeaseOwner, Fence: fact.LeaseFence,
		Messages: append([]byte(nil), fact.SessionMessages...),
		Digest:   *fact.PayloadDigest, Source: fact.SourceIdentity,
	}, nil
}

// ProjectAgentSessionFact appends the frozen fact to its frozen exact session
// and checkpoints the outbox in one transaction. A committed response-loss
// retry validates and replays the same ledger identity.
func (s *Store) ProjectAgentSessionFact(
	ctx context.Context,
	lease AgentSessionFactLease,
) error {
	if err := validateAgentSessionFactLeaseScope(lease); err != nil {
		return err
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return agentEventDatabaseError(
			"begin agent session fact projection", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := setAgentSessionFactProjectorContext(
		ctx, tx, lease.TenantID,
	); err != nil {
		return err
	}
	fact, err := scanAgentSessionFact(tx.QueryRow(ctx,
		`SELECT `+agentSessionFactColumns+`
		   FROM agent_session_fact_outbox
		  WHERE id=$1 AND tenant_id=$2 AND user_id=$3
		  FOR UPDATE`,
		lease.ID, lease.TenantID, lease.UserID,
	))
	if errors.Is(err, pgx.ErrNoRows) {
		return agentEventNotFound()
	}
	if err != nil {
		return agentEventDatabaseError(
			"lock projected agent session fact", err)
	}
	if agentSessionFactDurablePayloadInvalid(fact) {
		if fact.Status != AgentSessionFactStatusPending ||
			fact.LeaseOwner == nil || *fact.LeaseOwner != lease.Owner ||
			fact.LeaseFence != lease.Fence {
			return agentEventIntegrityError()
		}
		tag, updateErr := tx.Exec(ctx,
			`UPDATE agent_session_fact_outbox
			    SET status='blocked',blocked_reason='payload_integrity',
			        lease_owner=NULL,lease_expires_at=NULL,
			        updated_at=clock_timestamp()
			  WHERE id=$1 AND tenant_id=$2 AND user_id=$3
			    AND status='pending' AND lease_owner=$4
			    AND lease_fence=$5`,
			lease.ID, lease.TenantID, lease.UserID,
			lease.Owner, lease.Fence,
		)
		if updateErr != nil {
			return agentEventDatabaseError(
				"block corrupt agent session fact", updateErr)
		}
		if tag.RowsAffected() != 1 {
			return ErrAgentSessionFactBusy
		}
		if err := tx.Commit(ctx); err != nil {
			return agentEventDatabaseError(
				"commit corrupt agent session fact block", err)
		}
		return nil
	}
	if err := validateAgentSessionFactReplay(fact, lease); err != nil {
		return err
	}
	if fact.Status == AgentSessionFactStatusBlocked ||
		fact.Status == AgentSessionFactStatusSuppressed {
		return ErrAgentSessionFactTerminal
	}
	if fact.Status == AgentSessionFactStatusPending {
		var databaseNow time.Time
		if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).
			Scan(&databaseNow); err != nil {
			return agentEventDatabaseError(
				"read projected agent session fact clock", err)
		}
		if fact.LeaseOwner == nil || *fact.LeaseOwner != lease.Owner ||
			fact.LeaseFence != lease.Fence ||
			fact.LeaseExpiresAt == nil ||
			!databaseNow.Before(*fact.LeaseExpiresAt) {
			return ErrAgentSessionFactBusy
		}
	}

	replayOnly := fact.Status == AgentSessionFactStatusCompleted
	appendTarget := pgx.Tx(tx)
	var appendSavepoint pgx.Tx
	if !replayOnly {
		appendSavepoint, err = tx.Begin(ctx)
		if err != nil {
			return agentEventDatabaseError(
				"begin agent session fact append savepoint", err)
		}
		appendTarget = appendSavepoint
	}
	_, replayed, appendErr := commitAgentSessionAppendTx(
		ctx, appendTarget, lease.TenantID, lease.UserID, lease.SessionID,
		lease.Source, json.RawMessage(lease.Messages), replayOnly,
	)
	if replayOnly {
		if appendErr != nil {
			return appendErr
		}
		if !replayed {
			return agentEventIntegrityError()
		}
		if err := tx.Commit(ctx); err != nil {
			return agentEventDatabaseError(
				"commit agent session fact exact replay", err)
		}
		return nil
	}
	if appendErr != nil {
		if rollbackErr := appendSavepoint.Rollback(
			context.WithoutCancel(ctx),
		); rollbackErr != nil {
			return errors.Join(
				appendErr,
				agentEventDatabaseError(
					"rollback invalid agent session fact append",
					rollbackErr),
			)
		}
		if !agentSessionFactShouldBlock(appendErr) {
			return appendErr
		}
		tag, updateErr := tx.Exec(ctx,
			`UPDATE agent_session_fact_outbox
			    SET status='blocked',blocked_reason='projection_integrity',
			        lease_owner=NULL,lease_expires_at=NULL,
			        updated_at=clock_timestamp()
			  WHERE id=$1 AND tenant_id=$2 AND user_id=$3
			    AND status='pending' AND lease_owner=$4
			    AND lease_fence=$5`,
			lease.ID, lease.TenantID, lease.UserID,
			lease.Owner, lease.Fence,
		)
		if updateErr != nil {
			return agentEventDatabaseError(
				"block invalid agent session fact", updateErr)
		}
		if tag.RowsAffected() != 1 {
			return ErrAgentSessionFactBusy
		}
		if err := tx.Commit(ctx); err != nil {
			return agentEventDatabaseError(
				"commit blocked agent session fact", err)
		}
		return nil
	}
	if err := appendSavepoint.Commit(ctx); err != nil {
		return agentEventDatabaseError(
			"release agent session fact append savepoint", err)
	}
	tag, err := tx.Exec(ctx,
		`UPDATE agent_session_fact_outbox
		    SET status='completed',
		        session_recorded_at=clock_timestamp(),
		        lease_owner=NULL,lease_expires_at=NULL,
		        updated_at=clock_timestamp()
		  WHERE id=$1 AND tenant_id=$2 AND user_id=$3
		    AND status='pending' AND lease_owner=$4 AND lease_fence=$5`,
		lease.ID, lease.TenantID, lease.UserID, lease.Owner, lease.Fence,
	)
	if err != nil {
		return agentEventDatabaseError(
			"checkpoint completed agent session fact", err)
	}
	if tag.RowsAffected() != 1 {
		return ErrAgentSessionFactBusy
	}
	if err := tx.Commit(ctx); err != nil {
		return agentEventDatabaseError(
			"commit agent session fact projection", err)
	}
	return nil
}

func (s *Store) ReleaseAgentSessionFact(
	ctx context.Context,
	lease AgentSessionFactLease,
	retryAfter time.Duration,
) error {
	if err := validateAgentSessionFactLeaseScope(lease); err != nil ||
		retryAfter <= 0 || retryAfter > 30*24*time.Hour {
		if err != nil {
			return err
		}
		return agentEventValidationError(
			"agent session fact retry boundary is invalid")
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return agentEventDatabaseError(
			"begin agent session fact release", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := setAgentSessionFactProjectorContext(
		ctx, tx, lease.TenantID,
	); err != nil {
		return err
	}
	tag, err := tx.Exec(ctx,
		`UPDATE agent_session_fact_outbox
		    SET lease_owner=NULL,lease_expires_at=NULL,
		        next_attempt_at=clock_timestamp()+
		            ($6*interval '1 microsecond'),
		        updated_at=clock_timestamp()
		  WHERE id=$1 AND tenant_id=$2 AND user_id=$3
		    AND status='pending' AND lease_owner=$4 AND lease_fence=$5`,
		lease.ID, lease.TenantID, lease.UserID, lease.Owner, lease.Fence,
		retryAfter.Microseconds(),
	)
	if err != nil {
		return agentEventDatabaseError(
			"release agent session fact", err)
	}
	if tag.RowsAffected() != 1 {
		return ErrAgentSessionFactBusy
	}
	if err := tx.Commit(ctx); err != nil {
		return agentEventDatabaseError(
			"commit agent session fact release", err)
	}
	return nil
}

func validateAgentSessionFactReplay(
	fact AgentSessionFact,
	lease AgentSessionFactLease,
) error {
	if fact.FactID != lease.FactID ||
		fact.SourceIdentity != lease.Source ||
		fact.SessionID == nil || *fact.SessionID != lease.SessionID ||
		fact.PayloadDigest == nil ||
		*fact.PayloadDigest != lease.Digest ||
		!constantTimeAgentEventDigestEqual(
			agentSessionFactDigest(fact.SessionMessages), lease.Digest) ||
		!constantTimeAgentEventDigestEqual(
			agentSessionFactDigest(lease.Messages), lease.Digest) ||
		string(fact.SessionMessages) != string(lease.Messages) {
		return agentEventIntegrityError()
	}
	return nil
}

func validateAgentSessionFactLeaseScope(lease AgentSessionFactLease) error {
	if lease.ID <= 0 || lease.TenantID <= 0 || lease.UserID <= 0 ||
		lease.FactID <= 0 || lease.SessionID <= 0 ||
		lease.Source != fmt.Sprintf("feedback-click:%d", lease.FactID) ||
		!validAgentSessionFactOwner(lease.Owner) || lease.Fence <= 0 {
		return agentEventValidationError(
			"agent session fact lease is invalid")
	}
	return nil
}

func agentSessionFactDurablePayloadInvalid(fact AgentSessionFact) bool {
	return fact.ID <= 0 || fact.TenantID <= 0 || fact.UserID <= 0 ||
		fact.FactID <= 0 || fact.SessionID == nil || *fact.SessionID <= 0 ||
		fact.SourceIdentity !=
			fmt.Sprintf("feedback-click:%d", fact.FactID) ||
		fact.PayloadDigest == nil ||
		!validAgentEventDigest(*fact.PayloadDigest) ||
		len(fact.SessionMessages) < 2 ||
		len(fact.SessionMessages) > 16384 ||
		!json.Valid(fact.SessionMessages) ||
		!constantTimeAgentEventDigestEqual(
			agentSessionFactDigest(fact.SessionMessages),
			*fact.PayloadDigest)
}

func validAgentSessionFactOwner(owner string) bool {
	return owner != "" && len(owner) <= 255 &&
		utf8.ValidString(owner) && strings.TrimSpace(owner) == owner
}

func agentSessionFactShouldBlock(err error) bool {
	var appError *types.AppError
	if !errors.As(err, &appError) {
		return false
	}
	switch appError.Code {
	case types.CodeValidation, types.CodeInternal,
		types.CodeConflict, types.CodeNotFound:
		return true
	default:
		return false
	}
}

func agentSessionFactDigest(messages []byte) string {
	sum := sha256.Sum256(messages)
	return hex.EncodeToString(sum[:])
}

func setAgentSessionFactProjectorContext(
	ctx context.Context,
	tx pgx.Tx,
	tenantID int64,
) error {
	if err := validateAgentSessionFactProjector(ctx, tx); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx,
		`SET LOCAL ROLE vane_agent_session_fact_projector`); err != nil {
		return agentEventDatabaseError(
			"enter agent session fact projector role", err)
	}
	return setAgentEventTenantContext(ctx, tx, tenantID)
}

// validateAgentSessionFactProjector re-proves the entire projector role
// boundary before every Acquire/Project/Release mutation. Cluster role and ACL
// drift after migration is therefore fail-closed at the write boundary.
func validateAgentSessionFactProjector(
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
		  has_table_privilege(
		    op.oid, 'agent_session_projection_authority_events', 'SELECT'
		  ) AND
		  has_table_privilege(
		    op.oid, 'agent_session_fact_outbox', 'SELECT'
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
		     WHERE a.attrelid = 'agent_events'::regclass
		       AND a.attnum > 0 AND NOT a.attisdropped
		       AND has_column_privilege(
		             op.oid, a.attrelid, a.attname, 'INSERT'
		           )
		     ORDER BY a.attname
		  ) = ARRAY[
		    'batch_digest','batch_idempotency_key','batch_index','batch_size',
		    'kind','payload','payload_digest','schema_version','sequence',
		    'session_id','tenant_id','user_id'
		  ]::TEXT[] AND
		  ARRAY(
		    SELECT a.attname::TEXT
		      FROM pg_attribute a
		     WHERE a.attrelid = 'agent_session_fact_outbox'::regclass
		       AND a.attnum > 0 AND NOT a.attisdropped
		       AND has_column_privilege(
		             op.oid, a.attrelid, a.attname, 'UPDATE'
		           )
		     ORDER BY a.attname
		  ) = ARRAY[
		    'attempt_count','blocked_reason','lease_expires_at','lease_fence',
		    'lease_owner','next_attempt_at','session_recorded_at','status',
		    'updated_at'
		  ]::TEXT[] AND
		  ARRAY(
		    SELECT a.attname::TEXT
		      FROM pg_attribute a
		     WHERE a.attrelid = 'agent_sessions'::regclass
		       AND a.attnum > 0 AND NOT a.attisdropped
		       AND has_column_privilege(
		             op.oid, a.attrelid, a.attname, 'UPDATE'
		           )
		     ORDER BY a.attname
		  ) = ARRAY['messages']::TEXT[] AND
		  NOT EXISTS (
		    SELECT 1
		      FROM pg_class c
		      JOIN pg_namespace n ON n.oid = c.relnamespace
		     WHERE n.nspname = 'public' AND c.relkind IN ('r','p','v','m','f')
		       AND (
		         has_table_privilege(
		           op.oid, c.oid,
		           'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER,MAINTAIN'
		         ) OR (
		           has_table_privilege(op.oid, c.oid, 'SELECT') AND
		           c.oid NOT IN (
		             'agent_events'::regclass,
		             'agent_session_projection_authority_events'::regclass,
		             'agent_session_fact_outbox'::regclass
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
		       AND has_column_privilege(op.oid, a.attrelid, a.attname, 'SELECT')
		       AND NOT (
		         c.oid IN (
		           'agent_events'::regclass,
		           'agent_session_projection_authority_events'::regclass,
		           'agent_session_fact_outbox'::regclass
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
		         c.oid = 'agent_events'::regclass AND
		         a.attname IN (
		           'batch_digest','batch_idempotency_key','batch_index',
		           'batch_size','kind','payload','payload_digest',
		           'schema_version','sequence','session_id','tenant_id','user_id'
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
		       AND NOT (
		         (c.oid = 'agent_sessions'::regclass AND
		          a.attname = 'messages') OR
		         (c.oid = 'agent_session_fact_outbox'::regclass AND
		          a.attname IN (
		            'attempt_count','blocked_reason','lease_expires_at',
		            'lease_fence','lease_owner','next_attempt_at',
		            'session_recorded_at','status','updated_at'
		          ))
		       )
		  ) AND
		  NOT EXISTS (
		    SELECT 1
		      FROM pg_attribute a
		      JOIN pg_class c ON c.oid = a.attrelid
		      JOIN pg_namespace n ON n.oid = c.relnamespace
		     WHERE n.nspname = 'public' AND c.relkind IN ('r','p','v','m','f')
		       AND a.attnum > 0 AND NOT a.attisdropped
		       AND has_column_privilege(
		             op.oid, a.attrelid, a.attname, 'REFERENCES'
		           )
		  ) AND
		  has_sequence_privilege(
		    op.oid, 'agent_events_id_seq', 'USAGE'
		  ) AND
		  NOT has_sequence_privilege(
		    op.oid, 'agent_events_id_seq', 'SELECT,UPDATE'
		  ) AND
		  NOT EXISTS (
		    SELECT 1
		      FROM pg_class c
		      JOIN pg_namespace n ON n.oid = c.relnamespace
		     WHERE n.nspname = 'public'
		       AND c.oid <> 'agent_events_id_seq'::regclass
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
		 WHERE op.rolname = 'vane_agent_session_fact_projector'`,
	).Scan(&valid)
	if err != nil || !valid {
		return agentEventIntegrityError()
	}
	return nil
}
