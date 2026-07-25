package store

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/YouToco/vane/agentledger"
	"github.com/YouToco/vane/types"
)

const (
	maxAgentEventBatchSize = 64
	maxAgentEventListSize  = 200
	maxAgentEventKeyBytes  = 255
)

const agentEventColumns = `id, tenant_id, user_id, session_id, sequence,
	batch_idempotency_key, batch_index, batch_size, kind, schema_version,
	payload, payload_digest, batch_digest, created_at`

// AppendAgentEvents atomically appends one ordered semantic event batch.
//
// The session row is the per-session sequence allocator and serialization
// point. A response-lost retry with the same idempotency key returns the exact
// stored batch; a different canonical batch under that key is a conflict.
func (s *Store) AppendAgentEvents(
	ctx context.Context,
	batch agentledger.AppendBatch,
) ([]agentledger.Event, error) {
	canonical, batchDigest, err := prepareAgentEventBatch(batch)
	if err != nil {
		return nil, err
	}

	tx, err := s.beginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, agentEventDatabaseError("begin agent event append transaction", err)
	}
	defer func() {
		_ = tx.Rollback(context.WithoutCancel(ctx))
	}()

	if err := setAgentEventTenantContext(ctx, tx, batch.Scope.TenantID); err != nil {
		return nil, err
	}
	if err := lockAgentEventSession(ctx, tx, batch.Scope); err != nil {
		return nil, err
	}
	existing, err := loadAgentEventBatch(
		ctx, tx, batch.Scope, batch.IdempotencyKey,
	)
	if err != nil {
		return nil, err
	}
	if len(existing) > 0 {
		if err := validateAgentEventReplay(
			existing, canonical, batchDigest,
		); err != nil {
			return nil, err
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, agentEventDatabaseError(
				"commit agent event exact replay transaction", err)
		}
		return existing, nil
	}

	inserted, err := insertAgentEventBatch(
		ctx, tx, batch, canonical, batchDigest,
	)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, agentEventDatabaseError("commit agent event append transaction", err)
	}
	return inserted, nil
}

// ListAgentEvents returns one contiguous sequence page strictly after
// afterSequence. Every row is decoded and digest-checked before exposure.
func (s *Store) ListAgentEvents(
	ctx context.Context,
	scope agentledger.Scope,
	afterSequence int64,
	limit int,
) ([]agentledger.Event, error) {
	if err := validateAgentEventScope(scope); err != nil {
		return nil, err
	}
	if afterSequence < 0 || limit <= 0 || limit > maxAgentEventListSize {
		return nil, agentEventValidationError("agent event page is invalid")
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if err != nil {
		return nil, agentEventDatabaseError("begin agent event list transaction", err)
	}
	defer func() {
		_ = tx.Rollback(context.WithoutCancel(ctx))
	}()
	if err := setAgentEventTenantContext(ctx, tx, scope.TenantID); err != nil {
		return nil, err
	}
	rows, err := tx.Query(ctx,
		`SELECT `+agentEventColumns+`
		   FROM agent_events
		  WHERE tenant_id=$1 AND user_id=$2 AND session_id=$3
		    AND sequence>$4
		  ORDER BY sequence
		  LIMIT $5`,
		scope.TenantID, scope.UserID, scope.SessionID, afterSequence, limit,
	)
	if err != nil {
		return nil, agentEventDatabaseError("list agent events", err)
	}
	defer rows.Close()

	events := make([]agentledger.Event, 0, limit)
	expected := afterSequence + 1
	for rows.Next() {
		event, scanErr := scanAgentEvent(rows)
		if scanErr != nil {
			return nil, agentEventDatabaseError("scan agent event", scanErr)
		}
		if event.Scope != scope || event.Sequence != expected {
			return nil, agentEventIntegrityError()
		}
		if _, validateErr := validateStoredAgentEvent(event); validateErr != nil {
			return nil, validateErr
		}
		events = append(events, event)
		expected++
	}
	rowsErr := rows.Err()
	rows.Close()
	if rowsErr != nil {
		return nil, agentEventDatabaseError("iterate agent events", rowsErr)
	}
	// A page may begin/end inside an atomic batch. Load and validate every
	// referenced complete batch in the same repeatable-read snapshot before any
	// page row becomes visible.
	checked := make(map[string]struct{})
	for i := range events {
		key := events[i].IdempotencyKey
		if _, ok := checked[key]; ok {
			continue
		}
		batch, err := loadAgentEventBatch(ctx, tx, scope, key)
		if err != nil {
			return nil, err
		}
		if _, err := validateStoredAgentEventBatch(batch); err != nil {
			return nil, err
		}
		checked[key] = struct{}{}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, agentEventDatabaseError("commit agent event list transaction", err)
	}
	return events, nil
}

// ReplayAgentEvents reads a repeatable snapshot of the full session ledger and
// proves sequence continuity, complete atomic batches, and every payload/batch
// digest. 7.7-B's retained projector consumes only rows returned by this
// integrity boundary.
func (s *Store) ReplayAgentEvents(
	ctx context.Context,
	scope agentledger.Scope,
) ([]agentledger.Event, error) {
	if err := validateAgentEventScope(scope); err != nil {
		return nil, err
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if err != nil {
		return nil, agentEventDatabaseError("begin agent event replay transaction", err)
	}
	defer func() {
		_ = tx.Rollback(context.WithoutCancel(ctx))
	}()
	if err := setAgentEventTenantContext(ctx, tx, scope.TenantID); err != nil {
		return nil, err
	}
	if err := requireAgentEventSession(ctx, tx, scope); err != nil {
		return nil, err
	}
	events, err := loadCompleteAgentEventLedger(ctx, tx, scope)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, agentEventDatabaseError("commit agent event replay transaction", err)
	}
	return events, nil
}

// CommitAgentSessionTurn atomically keeps agent_sessions as the primary
// projection and appends one self-contained normal-turn event generation.
// Exact response-loss replay never overwrites a later session projection.
func (s *Store) CommitAgentSessionTurn(
	ctx context.Context,
	projection agentledger.SessionProjection,
	batch agentledger.AppendBatch,
) (agentledger.ProjectionShadowAudit, error) {
	canonical, batchDigest, err := prepareAgentEventBatch(batch)
	if err != nil {
		return agentledger.ProjectionShadowAudit{}, err
	}
	batchProjection, err := agentledger.ProjectCanonicalSessionSnapshot(canonical)
	if err != nil {
		return agentledger.ProjectionShadowAudit{},
			agentEventValidationError(
				"agent session event batch is not a complete projection snapshot",
			)
	}
	desiredDigest, err := agentledger.ProjectionDigest(projection)
	if err != nil {
		return agentledger.ProjectionShadowAudit{},
			agentEventValidationError("agent session projection is invalid")
	}
	batchProjectionDigest, err := agentledger.ProjectionDigest(batchProjection)
	if err != nil {
		return agentledger.ProjectionShadowAudit{},
			agentEventValidationError(
				"agent session event batch projection is invalid",
			)
	}

	tx, err := s.beginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return agentledger.ProjectionShadowAudit{},
			agentEventDatabaseError("begin agent session dual-write transaction", err)
	}
	defer func() {
		_ = tx.Rollback(context.WithoutCancel(ctx))
	}()
	if err := setAgentEventRuntimeContext(
		ctx, tx, batch.Scope.TenantID,
	); err != nil {
		return agentledger.ProjectionShadowAudit{}, err
	}

	current, err := loadAgentSessionProjectionForUpdate(ctx, tx, batch.Scope)
	if err != nil {
		return agentledger.ProjectionShadowAudit{}, err
	}
	beforeEvents, err := loadCompleteAgentEventLedger(ctx, tx, batch.Scope)
	if err != nil {
		return agentledger.ProjectionShadowAudit{}, err
	}
	priorState := comparePriorAgentSessionProjection(current, beforeEvents)

	existing, err := loadAgentEventBatch(
		ctx, tx, batch.Scope, batch.IdempotencyKey,
	)
	if err != nil {
		return agentledger.ProjectionShadowAudit{}, err
	}
	if len(existing) > 0 {
		if err := validateAgentEventReplay(
			existing, canonical, batchDigest,
		); err != nil {
			return agentledger.ProjectionShadowAudit{}, err
		}
		if !constantTimeAgentEventDigestEqual(
			batchProjectionDigest, desiredDigest,
		) {
			return agentledger.ProjectionShadowAudit{},
				agentEventValidationError(
					"agent session event batch does not match projection",
				)
		}
		audit, err := auditAgentSessionProjection(current, beforeEvents, priorState)
		if err != nil {
			return agentledger.ProjectionShadowAudit{}, err
		}
		if !audit.Match {
			return agentledger.ProjectionShadowAudit{}, agentEventIntegrityError()
		}
		if err := tx.Commit(ctx); err != nil {
			return agentledger.ProjectionShadowAudit{},
				agentEventDatabaseError(
					"commit agent session dual-write replay transaction", err,
				)
		}
		return audit, nil
	}

	if !constantTimeAgentEventDigestEqual(
		batchProjectionDigest, desiredDigest,
	) {
		return agentledger.ProjectionShadowAudit{},
			agentEventValidationError(
				"agent session event batch does not match projection",
			)
	}
	baseDigest, err := agentledger.ProjectionSnapshotBaseDigest(canonical[0])
	if err != nil {
		return agentledger.ProjectionShadowAudit{},
			agentEventValidationError("agent session snapshot base is invalid")
	}
	currentDigest, err := agentledger.ProjectionDigest(current)
	if err != nil {
		return agentledger.ProjectionShadowAudit{}, agentEventIntegrityError()
	}
	if !constantTimeAgentEventDigestEqual(baseDigest, currentDigest) {
		return agentledger.ProjectionShadowAudit{},
			agentSessionProjectionConflict()
	}

	inserted, err := insertAgentEventBatch(
		ctx, tx, batch, canonical, batchDigest,
	)
	if err != nil {
		return agentledger.ProjectionShadowAudit{}, err
	}
	messages := projection.Messages
	if len(messages) == 0 {
		messages = json.RawMessage("[]")
	}
	activatedTools := projection.ActivatedTools
	if len(activatedTools) == 0 {
		activatedTools = json.RawMessage("[]")
	}
	tag, err := tx.Exec(ctx,
		`UPDATE agent_sessions
		    SET messages=$4, turn_count=$5, activated_tools=$6,
		        updated_at=now()
		  WHERE id=$1 AND tenant_id=$2 AND user_id=$3`,
		batch.Scope.SessionID, batch.Scope.TenantID, batch.Scope.UserID,
		messages, projection.TurnCount, activatedTools,
	)
	if err != nil {
		return agentledger.ProjectionShadowAudit{},
			agentEventDatabaseError("update agent session primary projection", err)
	}
	if tag.RowsAffected() != 1 {
		return agentledger.ProjectionShadowAudit{}, agentEventNotFound()
	}

	allEvents := append([]agentledger.Event(nil), beforeEvents...)
	allEvents = append(allEvents, inserted...)
	updated := agentledger.SessionProjection{
		Messages:       messages,
		TurnCount:      projection.TurnCount,
		ActivatedTools: activatedTools,
	}
	audit, err := auditAgentSessionProjection(updated, allEvents, priorState)
	if err != nil {
		return agentledger.ProjectionShadowAudit{}, err
	}
	if !audit.Match ||
		!constantTimeAgentEventDigestEqual(
			audit.LegacyDigest, desiredDigest,
		) ||
		!constantTimeAgentEventDigestEqual(
			audit.EventDigest, desiredDigest,
		) {
		return agentledger.ProjectionShadowAudit{}, agentEventIntegrityError()
	}
	if err := tx.Commit(ctx); err != nil {
		return agentledger.ProjectionShadowAudit{},
			agentEventDatabaseError("commit agent session dual-write transaction", err)
	}
	return audit, nil
}

func insertAgentEventBatch(
	ctx context.Context,
	tx pgx.Tx,
	batch agentledger.AppendBatch,
	canonical []agentledger.CanonicalEvent,
	batchDigest string,
) ([]agentledger.Event, error) {
	nextSequence, err := nextAgentEventSequence(ctx, tx, batch.Scope)
	if err != nil {
		return nil, err
	}
	inserted := make([]agentledger.Event, 0, len(canonical))
	for i := range canonical {
		event, scanErr := scanAgentEvent(tx.QueryRow(ctx,
			`INSERT INTO agent_events (
				tenant_id, user_id, session_id, sequence,
				batch_idempotency_key, batch_index, batch_size,
				kind, schema_version, payload, payload_digest, batch_digest
			 ) VALUES (
				$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12
			 ) RETURNING `+agentEventColumns,
			batch.Scope.TenantID, batch.Scope.UserID, batch.Scope.SessionID,
			nextSequence+int64(i), batch.IdempotencyKey, i, len(canonical),
			string(canonical[i].Kind()), agentledger.SchemaVersion,
			canonical[i].Payload(), canonical[i].Digest(), batchDigest,
		))
		if scanErr != nil {
			return nil, agentEventDatabaseError("insert agent event", scanErr)
		}
		if _, validateErr := validateStoredAgentEvent(event); validateErr != nil {
			return nil, validateErr
		}
		inserted = append(inserted, event)
	}
	return inserted, nil
}

func loadCompleteAgentEventLedger(
	ctx context.Context,
	tx pgx.Tx,
	scope agentledger.Scope,
) ([]agentledger.Event, error) {
	rows, err := tx.Query(ctx,
		`SELECT `+agentEventColumns+`
		   FROM agent_events
		  WHERE tenant_id=$1 AND user_id=$2 AND session_id=$3
		  ORDER BY sequence`,
		scope.TenantID, scope.UserID, scope.SessionID,
	)
	if err != nil {
		return nil, agentEventDatabaseError("replay agent events", err)
	}
	events := make([]agentledger.Event, 0)
	for rows.Next() {
		event, scanErr := scanAgentEvent(rows)
		if scanErr != nil {
			rows.Close()
			return nil, agentEventDatabaseError("scan replayed agent event", scanErr)
		}
		events = append(events, event)
	}
	rowsErr := rows.Err()
	rows.Close()
	if rowsErr != nil {
		return nil, agentEventDatabaseError("iterate replayed agent events", rowsErr)
	}
	if err := validateCompleteAgentEventLedger(scope, events); err != nil {
		return nil, err
	}
	return events, nil
}

func setAgentEventTenantContext(
	ctx context.Context,
	tx pgx.Tx,
	tenantID int64,
) error {
	if tenantID <= 0 {
		return agentEventValidationError("agent event tenant is invalid")
	}
	if _, err := tx.Exec(ctx,
		`SELECT set_config('app.tenant_id', $1, true)`,
		fmt.Sprintf("%d", tenantID),
	); err != nil {
		return agentEventDatabaseError("set agent event tenant context", err)
	}
	return nil
}

func setAgentEventRuntimeContext(
	ctx context.Context,
	tx pgx.Tx,
	tenantID int64,
) error {
	if _, err := tx.Exec(ctx, `SET LOCAL ROLE vane_app`); err != nil {
		return agentEventDatabaseError("set agent event runtime role", err)
	}
	return setAgentEventTenantContext(ctx, tx, tenantID)
}

func loadAgentSessionProjectionForUpdate(
	ctx context.Context,
	tx pgx.Tx,
	scope agentledger.Scope,
) (agentledger.SessionProjection, error) {
	var projection agentledger.SessionProjection
	err := tx.QueryRow(ctx,
		`SELECT messages, turn_count, activated_tools
		   FROM agent_sessions
		  WHERE id=$1 AND tenant_id=$2 AND user_id=$3
		  FOR UPDATE /* agent ledger normal-turn session lock */`,
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
			agentEventDatabaseError("lock agent session primary projection", err)
	}
	return projection, nil
}

func comparePriorAgentSessionProjection(
	legacy agentledger.SessionProjection,
	events []agentledger.Event,
) string {
	if len(events) == 0 {
		return "uninitialized"
	}
	projected, err := agentledger.ProjectLatestSessionSnapshot(events)
	if err != nil {
		return "unsupported_event_history"
	}
	legacyDigest, err := agentledger.ProjectionDigest(legacy)
	if err != nil {
		return "invalid_legacy_projection"
	}
	eventDigest, err := agentledger.ProjectionDigest(projected)
	if err != nil {
		return "invalid_event_projection"
	}
	if constantTimeAgentEventDigestEqual(legacyDigest, eventDigest) {
		return "match"
	}
	// The known unsupported writers are AppendAgentSessionMessages and the two
	// restricted receipt transactions. 7.7-B records/resynchronizes this drift
	// but does not pretend those paths are transactionally dual-written.
	return "unsupported_writer_or_projection_drift"
}

func auditAgentSessionProjection(
	legacy agentledger.SessionProjection,
	events []agentledger.Event,
	priorState string,
) (agentledger.ProjectionShadowAudit, error) {
	projected, err := agentledger.ProjectLatestSessionSnapshot(events)
	if err != nil {
		return agentledger.ProjectionShadowAudit{}, agentEventIntegrityError()
	}
	legacyDigest, err := agentledger.ProjectionDigest(legacy)
	if err != nil {
		return agentledger.ProjectionShadowAudit{}, agentEventIntegrityError()
	}
	eventDigest, err := agentledger.ProjectionDigest(projected)
	if err != nil {
		return agentledger.ProjectionShadowAudit{}, agentEventIntegrityError()
	}
	legacyCount, err := agentSessionProjectionMessageCount(legacy.Messages)
	if err != nil {
		return agentledger.ProjectionShadowAudit{}, agentEventIntegrityError()
	}
	eventCount, err := agentSessionProjectionMessageCount(projected.Messages)
	if err != nil {
		return agentledger.ProjectionShadowAudit{}, agentEventIntegrityError()
	}
	match := constantTimeAgentEventDigestEqual(legacyDigest, eventDigest)
	reason := "match"
	switch {
	case !match:
		reason = "projection_mismatch"
	case priorState == "uninitialized":
		reason = "initialized"
	case priorState != "match":
		reason = "resynced_after_" + priorState
	}
	return agentledger.ProjectionShadowAudit{
		Match:              match,
		PriorState:         priorState,
		Reason:             reason,
		LegacyDigest:       legacyDigest,
		EventDigest:        eventDigest,
		LegacyMessageCount: legacyCount,
		EventMessageCount:  eventCount,
	}, nil
}

func agentSessionProjectionMessageCount(raw json.RawMessage) (int, error) {
	var messages []json.RawMessage
	if err := json.Unmarshal(raw, &messages); err != nil {
		return 0, err
	}
	return len(messages), nil
}

func prepareAgentEventBatch(
	batch agentledger.AppendBatch,
) ([]agentledger.CanonicalEvent, string, error) {
	if err := validateAgentEventScope(batch.Scope); err != nil {
		return nil, "", err
	}
	if !validAgentEventIdempotencyKey(batch.IdempotencyKey) {
		return nil, "", agentEventValidationError(
			"agent event idempotency key is invalid")
	}
	if len(batch.Events) == 0 || len(batch.Events) > maxAgentEventBatchSize {
		return nil, "", agentEventValidationError(
			"agent event batch size is invalid")
	}
	canonical := make([]agentledger.CanonicalEvent, len(batch.Events))
	for i := range batch.Events {
		event, err := agentledger.Canonicalize(batch.Events[i])
		if err != nil {
			return nil, "", agentEventValidationError(
				"agent event payload is invalid")
		}
		canonical[i] = event
	}
	batchDigest, err := agentledger.BatchDigest(canonical)
	if err != nil {
		return nil, "", agentEventValidationError(
			"agent event batch is invalid")
	}
	return canonical, batchDigest, nil
}

func lockAgentEventSession(
	ctx context.Context,
	tx pgx.Tx,
	scope agentledger.Scope,
) error {
	if err := requireAgentEventSessionForUpdate(ctx, tx, scope); err != nil {
		return err
	}
	return nil
}

func requireAgentEventSession(
	ctx context.Context,
	tx pgx.Tx,
	scope agentledger.Scope,
) error {
	var found int
	err := tx.QueryRow(ctx,
		`SELECT 1
		   FROM agent_sessions
		  WHERE id=$1 AND tenant_id=$2 AND user_id=$3`,
		scope.SessionID, scope.TenantID, scope.UserID,
	).Scan(&found)
	if errors.Is(err, pgx.ErrNoRows) {
		return agentEventNotFound()
	}
	if err != nil {
		return agentEventDatabaseError("load agent event session scope", err)
	}
	return nil
}

func requireAgentEventSessionForUpdate(
	ctx context.Context,
	tx pgx.Tx,
	scope agentledger.Scope,
) error {
	var found int
	err := tx.QueryRow(ctx,
		`SELECT 1
		   FROM agent_sessions
		  WHERE id=$1 AND tenant_id=$2 AND user_id=$3
		  FOR UPDATE`,
		scope.SessionID, scope.TenantID, scope.UserID,
	).Scan(&found)
	if errors.Is(err, pgx.ErrNoRows) {
		return agentEventNotFound()
	}
	if err != nil {
		return agentEventDatabaseError("lock agent event session scope", err)
	}
	return nil
}

func nextAgentEventSequence(
	ctx context.Context,
	tx pgx.Tx,
	scope agentledger.Scope,
) (int64, error) {
	var count, maximum int64
	if err := tx.QueryRow(ctx,
		`SELECT count(*), COALESCE(max(sequence), 0)
		   FROM agent_events
		  WHERE tenant_id=$1 AND user_id=$2 AND session_id=$3`,
		scope.TenantID, scope.UserID, scope.SessionID,
	).Scan(&count, &maximum); err != nil {
		return 0, agentEventDatabaseError("read agent event sequence head", err)
	}
	if count != maximum {
		return 0, agentEventIntegrityError()
	}
	return maximum + 1, nil
}

func loadAgentEventBatch(
	ctx context.Context,
	tx pgx.Tx,
	scope agentledger.Scope,
	idempotencyKey string,
) ([]agentledger.Event, error) {
	rows, err := tx.Query(ctx,
		`SELECT `+agentEventColumns+`
		   FROM agent_events
		  WHERE tenant_id=$1 AND user_id=$2 AND session_id=$3
		    AND batch_idempotency_key=$4
		  ORDER BY batch_index`,
		scope.TenantID, scope.UserID, scope.SessionID, idempotencyKey,
	)
	if err != nil {
		return nil, agentEventDatabaseError("load agent event replay batch", err)
	}
	defer rows.Close()
	var events []agentledger.Event
	for rows.Next() {
		event, scanErr := scanAgentEvent(rows)
		if scanErr != nil {
			return nil, agentEventDatabaseError(
				"scan agent event replay batch", scanErr)
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, agentEventDatabaseError(
			"iterate agent event replay batch", err)
	}
	return events, nil
}

type agentEventScanner interface {
	Scan(...any) error
}

func scanAgentEvent(row agentEventScanner) (agentledger.Event, error) {
	var event agentledger.Event
	var kind string
	err := row.Scan(
		&event.ID,
		&event.Scope.TenantID,
		&event.Scope.UserID,
		&event.Scope.SessionID,
		&event.Sequence,
		&event.IdempotencyKey,
		&event.BatchIndex,
		&event.BatchSize,
		&kind,
		&event.SchemaVersion,
		&event.Payload,
		&event.PayloadDigest,
		&event.BatchDigest,
		&event.CreatedAt,
	)
	event.Kind = agentledger.Kind(kind)
	return event, err
}

func validateAgentEventReplay(
	stored []agentledger.Event,
	requested []agentledger.CanonicalEvent,
	requestedBatchDigest string,
) error {
	storedCanonical, err := validateStoredAgentEventBatch(stored)
	if err != nil {
		return err
	}
	if len(storedCanonical) != len(requested) {
		return agentEventConflict()
	}
	storedBatchDigest := stored[0].BatchDigest
	if !constantTimeAgentEventDigestEqual(
		storedBatchDigest, requestedBatchDigest) {
		return agentEventConflict()
	}
	for i := range storedCanonical {
		if storedCanonical[i].Kind() != requested[i].Kind() ||
			!constantTimeAgentEventDigestEqual(
				storedCanonical[i].Digest(), requested[i].Digest()) {
			return agentEventConflict()
		}
	}
	return nil
}

func validateStoredAgentEventBatch(
	stored []agentledger.Event,
) ([]agentledger.CanonicalEvent, error) {
	if len(stored) == 0 || stored[0].BatchSize != len(stored) ||
		stored[0].BatchIndex != 0 {
		return nil, agentEventIntegrityError()
	}
	canonical := make([]agentledger.CanonicalEvent, len(stored))
	for i := range stored {
		if stored[i].Scope != stored[0].Scope ||
			stored[i].BatchIndex != i ||
			stored[i].BatchSize != len(stored) ||
			stored[i].IdempotencyKey != stored[0].IdempotencyKey ||
			stored[i].BatchDigest != stored[0].BatchDigest ||
			stored[i].Sequence != stored[0].Sequence+int64(i) {
			return nil, agentEventIntegrityError()
		}
		decoded, err := validateStoredAgentEvent(stored[i])
		if err != nil {
			return nil, err
		}
		canonical[i] = decoded
	}
	batchDigest, err := agentledger.BatchDigest(canonical)
	if err != nil ||
		!constantTimeAgentEventDigestEqual(
			batchDigest, stored[0].BatchDigest) {
		return nil, agentEventIntegrityError()
	}
	return canonical, nil
}

func validateCompleteAgentEventLedger(
	scope agentledger.Scope,
	events []agentledger.Event,
) error {
	for i := 0; i < len(events); {
		event := events[i]
		if event.Scope != scope || event.Sequence != int64(i+1) ||
			event.BatchIndex != 0 || event.BatchSize <= 0 ||
			i+event.BatchSize > len(events) {
			return agentEventIntegrityError()
		}
		canonicalBatch := make(
			[]agentledger.CanonicalEvent, event.BatchSize,
		)
		for batchIndex := 0; batchIndex < event.BatchSize; batchIndex++ {
			current := events[i+batchIndex]
			if current.Scope != scope ||
				current.Sequence != int64(i+batchIndex+1) ||
				current.IdempotencyKey != event.IdempotencyKey ||
				current.BatchIndex != batchIndex ||
				current.BatchSize != event.BatchSize ||
				current.BatchDigest != event.BatchDigest {
				return agentEventIntegrityError()
			}
			canonical, err := validateStoredAgentEvent(current)
			if err != nil {
				return err
			}
			canonicalBatch[batchIndex] = canonical
		}
		digest, err := agentledger.BatchDigest(canonicalBatch)
		if err != nil ||
			!constantTimeAgentEventDigestEqual(digest, event.BatchDigest) {
			return agentEventIntegrityError()
		}
		i += event.BatchSize
	}
	return nil
}

func validateStoredAgentEvent(
	event agentledger.Event,
) (agentledger.CanonicalEvent, error) {
	if event.ID <= 0 || event.Sequence <= 0 ||
		event.SchemaVersion != agentledger.SchemaVersion ||
		!event.Kind.Valid() ||
		!validAgentEventIdempotencyKey(event.IdempotencyKey) ||
		event.BatchSize <= 0 || event.BatchSize > maxAgentEventBatchSize ||
		event.BatchIndex < 0 || event.BatchIndex >= event.BatchSize ||
		!validAgentEventDigest(event.BatchDigest) {
		return agentledger.CanonicalEvent{}, agentEventIntegrityError()
	}
	canonical, err := agentledger.Decode(event.Payload, event.PayloadDigest)
	if err != nil || canonical.Kind() != event.Kind {
		return agentledger.CanonicalEvent{}, agentEventIntegrityError()
	}
	return canonical, nil
}

func validateAgentEventScope(scope agentledger.Scope) error {
	if scope.TenantID <= 0 || scope.UserID <= 0 || scope.SessionID <= 0 {
		return agentEventValidationError("agent event scope is invalid")
	}
	return nil
}

func validAgentEventIdempotencyKey(value string) bool {
	if value == "" || len(value) > maxAgentEventKeyBytes {
		return false
	}
	for i, char := range []byte(value) {
		if (char >= 'A' && char <= 'Z') ||
			(char >= 'a' && char <= 'z') ||
			(char >= '0' && char <= '9') ||
			(i > 0 && strings.ContainsRune("._:/-", rune(char))) {
			continue
		}
		return false
	}
	return true
}

func validAgentEventDigest(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	for _, char := range value {
		if (char < '0' || char > '9') && (char < 'a' || char > 'f') {
			return false
		}
	}
	return true
}

func constantTimeAgentEventDigestEqual(left, right string) bool {
	return len(left) == sha256.Size*2 && len(right) == sha256.Size*2 &&
		subtle.ConstantTimeCompare([]byte(left), []byte(right)) == 1
}

func agentEventValidationError(message string) error {
	return types.NewAppError(types.CodeValidation, message, nil)
}

func agentEventNotFound() error {
	return types.NewAppError(types.CodeNotFound,
		"agent event session is unavailable", nil)
}

func agentEventConflict() error {
	return types.NewAppError(types.CodeConflict,
		"agent event idempotency key has different canonical bytes", nil)
}

func agentSessionProjectionConflict() error {
	return types.NewAppError(types.CodeConflict,
		"agent session projection changed before dual-write commit", nil)
}

func agentEventIntegrityError() error {
	return types.NewAppError(types.CodeInternal,
		"agent event ledger integrity check failed", nil)
}

// Keep raw payloads and database details out of returned errors. PostgreSQL
// constraint details can quote an entire BYTEA value.
func agentEventDatabaseError(action string, cause error) error {
	var safeCause error
	switch {
	case cause == nil:
		safeCause = errors.New("database operation did not converge")
	case errors.Is(cause, context.Canceled),
		errors.Is(cause, context.DeadlineExceeded):
		safeCause = cause
	default:
		var pgErr *pgconn.PgError
		if errors.As(cause, &pgErr) {
			safeCause = fmt.Errorf("postgres sqlstate %s", pgErr.Code)
		} else {
			safeCause = errors.New("database operation failed")
		}
	}
	return types.NewAppError(types.CodeDatabase, action, safeCause)
}
