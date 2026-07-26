package store

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/YouToco/vane/types"
)

func TestGetAgentActionContinuationStatusVerifiesExactAuthority(t *testing.T) {
	f := newAgentEventFixture(t)
	ctx := t.Context()
	actionIDs := make([]string, 0, 3)
	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(
			cleanupCtx, t, f.store,
			`DELETE FROM pending_actions WHERE id=ANY($1)`,
			actionIDs,
		)
	})
	create := func(sourceID int64) string {
		t.Helper()
		actionID := uuid.NewString()
		actionIDs = append(actionIDs, actionID)
		if err := f.store.CreatePendingAction(
			ctx,
			&types.PendingAction{
				ID: actionID, UserID: f.userA,
				SessionID: &f.sessionA, ToolName: "enable_source",
				Args: []byte(fmt.Sprintf(
					`{"source_id":%d}`, sourceID,
				)),
				Summary: "status", Status: types.PendingActionStatusPending,
				ExpiresAt: time.Now().Add(time.Hour),
			},
		); err != nil {
			t.Fatal(err)
		}
		return actionID
	}

	legacyID := create(930001)
	legacy, err := f.store.GetAgentActionContinuationStatus(
		ctx, f.tenantA, f.userA, legacyID,
	)
	if err != nil {
		t.Fatal(err)
	}
	if legacy.ExecutionVersion != 0 ||
		legacy.Route != AgentActionAuthorityLegacy ||
		legacy.Generation != 0 || !legacy.ActivationEligible ||
		legacy.RollbackEligible || legacy.Status !=
		string(types.PendingActionStatusPending) {
		t.Fatalf("legacy status=%+v", legacy)
	}
	if _, err := f.store.pool.Exec(ctx,
		`UPDATE pending_actions
		    SET expires_at=clock_timestamp()-interval '1 second'
		  WHERE id=$1`,
		legacyID,
	); err != nil {
		t.Fatal(err)
	}
	expiredLegacy, err := f.store.GetAgentActionContinuationStatus(
		ctx, f.tenantA, f.userA, legacyID,
	)
	if err != nil {
		t.Fatalf("read ineligible expired legacy status: %v", err)
	}
	if expiredLegacy.ActivationEligible ||
		expiredLegacy.Route != AgentActionAuthorityLegacy ||
		expiredLegacy.Generation != 0 {
		t.Fatalf("expired legacy status=%+v", expiredLegacy)
	}

	rollbackID := create(930002)
	if _, err := f.store.ActivateAgentActionContinuation(
		ctx, f.tenantA, f.userA, rollbackID, "status activation",
	); err != nil {
		t.Fatal(err)
	}
	active, err := f.store.GetAgentActionContinuationStatus(
		ctx, f.tenantA, f.userA, rollbackID,
	)
	if err != nil {
		t.Fatal(err)
	}
	if active.ExecutionVersion != AgentActionExecutionVersion ||
		active.Route != AgentActionAuthorityDurable ||
		active.Generation != 1 || active.ActivationEligible ||
		!active.RollbackEligible ||
		active.Status != AgentActionStatusPending {
		t.Fatalf("active status=%+v", active)
	}
	if _, err := f.store.RollbackAgentActionContinuation(
		ctx, f.tenantA, f.userA, rollbackID, "status rollback",
	); err != nil {
		t.Fatal(err)
	}
	rolledBack, err := f.store.GetAgentActionContinuationStatus(
		ctx, f.tenantA, f.userA, rollbackID,
	)
	if err != nil {
		t.Fatal(err)
	}
	if rolledBack.ExecutionVersion != 0 ||
		rolledBack.Route != AgentActionAuthorityLegacy ||
		rolledBack.Generation != 2 ||
		rolledBack.Status != AgentActionStatusRolledBack ||
		rolledBack.ActivationEligible || rolledBack.RollbackEligible {
		t.Fatalf("rolled-back status=%+v", rolledBack)
	}

	confirmedID := create(930003)
	if _, err := f.store.ActivateAgentActionContinuation(
		ctx, f.tenantA, f.userA, confirmedID, "status confirmed",
	); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.ConfirmAgentActionContinuation(
		ctx, f.userA, confirmedID,
	); err != nil {
		t.Fatal(err)
	}
	confirmed, err := f.store.GetAgentActionContinuationStatus(
		ctx, f.tenantA, f.userA, confirmedID,
	)
	if err != nil {
		t.Fatal(err)
	}
	if confirmed.Status != AgentActionStatusConfirmed ||
		confirmed.ConfirmedAt == nil || confirmed.RollbackEligible ||
		confirmed.NextAttemptAt == nil {
		t.Fatalf("confirmed status=%+v", confirmed)
	}

	if _, err := f.store.pool.Exec(ctx,
		`UPDATE pending_actions SET args='{"source_id":930004}'::jsonb
		  WHERE id=$1`,
		confirmedID,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.GetAgentActionContinuationStatus(
		ctx, f.tenantA, f.userA, confirmedID,
	); !errors.Is(err, types.ErrInternal) {
		t.Fatalf("status accepted root drift: %v", err)
	}
}

func TestAgentActionRollbackReplaySurvivesLegacyRootTerminalization(
	t *testing.T,
) {
	f := newAgentEventFixture(t)
	ctx := t.Context()
	actionIDs := make([]string, 0, 5)
	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(
			cleanupCtx, t, f.store,
			`DELETE FROM pending_actions WHERE id=ANY($1)`,
			actionIDs,
		)
	})

	tests := []struct {
		name        string
		sourceID    int64
		materialize func(string)
		wantRoot    types.PendingActionStatus
	}{
		{
			name: "unconsumed pending", sourceID: 930101,
			materialize: func(string) {},
			wantRoot:    types.PendingActionStatusPending,
		},
		{
			name: "expired but unmaterialized pending", sourceID: 930102,
			materialize: func(actionID string) {
				t.Helper()
				if _, err := f.store.pool.Exec(ctx,
					`UPDATE pending_actions
					    SET expires_at=clock_timestamp()-interval '1 second'
					  WHERE id=$1`,
					actionID,
				); err != nil {
					t.Fatal(err)
				}
			},
			wantRoot: types.PendingActionStatusPending,
		},
		{
			name: "legacy executed", sourceID: 930103,
			materialize: func(actionID string) {
				t.Helper()
				if _, err := f.store.ClaimPendingAction(
					ctx, actionID, f.userA,
				); err != nil {
					t.Fatal(err)
				}
			},
			wantRoot: types.PendingActionStatusExecuted,
		},
		{
			name: "legacy cancelled", sourceID: 930104,
			materialize: func(actionID string) {
				t.Helper()
				if _, err := f.store.CancelPendingAction(
					ctx, actionID, f.userA,
				); err != nil {
					t.Fatal(err)
				}
			},
			wantRoot: types.PendingActionStatusCancelled,
		},
		{
			name: "materialized expired", sourceID: 930105,
			materialize: func(actionID string) {
				t.Helper()
				if _, err := f.store.pool.Exec(ctx,
					`UPDATE pending_actions
					    SET status='expired',
					        updated_at=clock_timestamp()
					  WHERE id=$1`,
					actionID,
				); err != nil {
					t.Fatal(err)
				}
			},
			wantRoot: types.PendingActionStatusExpired,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actionID := uuid.NewString()
			actionIDs = append(actionIDs, actionID)
			if err := f.store.CreatePendingAction(
				ctx,
				&types.PendingAction{
					ID: actionID, UserID: f.userA,
					SessionID: &f.sessionA, ToolName: "enable_source",
					Args: []byte(fmt.Sprintf(
						`{"source_id":%d}`, tt.sourceID,
					)),
					Summary:   "rollback response loss",
					Status:    types.PendingActionStatusPending,
					ExpiresAt: time.Now().Add(time.Hour),
				},
			); err != nil {
				t.Fatal(err)
			}
			if _, err := f.store.ActivateAgentActionContinuation(
				ctx, f.tenantA, f.userA, actionID,
				"response-loss activation",
			); err != nil {
				t.Fatal(err)
			}
			const evidence = "response-loss rollback"
			if _, err := f.store.RollbackAgentActionContinuation(
				ctx, f.tenantA, f.userA, actionID, evidence,
			); err != nil {
				t.Fatal(err)
			}

			tt.materialize(actionID)
			if generation, err :=
				f.store.RollbackAgentActionContinuation(
					ctx, f.tenantA, f.userA, actionID, evidence,
				); err != nil || generation != 2 {
				t.Fatalf(
					"rollback replay generation=%d err=%v",
					generation, err,
				)
			}
			status, err := f.store.GetAgentActionContinuationStatus(
				ctx, f.tenantA, f.userA, actionID,
			)
			if err != nil {
				t.Fatal(err)
			}
			if status.ExecutionVersion != 0 ||
				status.Generation != 2 ||
				status.Route != AgentActionAuthorityLegacy ||
				status.Status != AgentActionStatusRolledBack ||
				status.ActivationEligible || status.RollbackEligible {
				t.Fatalf("rolled-back status=%+v", status)
			}

			var rootStatus types.PendingActionStatus
			var continuationStatus string
			var authorityEvents int
			if err := f.store.pool.QueryRow(ctx,
				`SELECT p.status,c.status,
				        (SELECT count(*)
				           FROM agent_action_continuation_authority_events e
				          WHERE e.action_id=p.id)
				   FROM pending_actions p
				   JOIN agent_action_continuations c ON c.action_id=p.id
				  WHERE p.id=$1`,
				actionID,
			).Scan(
				&rootStatus, &continuationStatus, &authorityEvents,
			); err != nil {
				t.Fatal(err)
			}
			if rootStatus != tt.wantRoot ||
				continuationStatus != AgentActionStatusRolledBack ||
				authorityEvents != 2 {
				t.Fatalf(
					"root/continuation/events=%s/%s/%d",
					rootStatus, continuationStatus, authorityEvents,
				)
			}
			if _, err := f.store.RollbackAgentActionContinuation(
				ctx, f.tenantA, f.userA, actionID,
				"different rollback evidence",
			); !errors.Is(err, types.ErrInternal) {
				t.Fatalf("different evidence replay err=%v", err)
			}
		})
	}
}

func TestAgentActionRollbackReplayAndStatusRejectGenerationTwoDrift(
	t *testing.T,
) {
	f := newAgentEventFixture(t)
	ctx := t.Context()
	var actionIDs []string
	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(
			cleanupCtx, t, f.store,
			`DELETE FROM pending_actions WHERE id=ANY($1)`,
			actionIDs,
		)
	})

	tests := []struct {
		name   string
		mutate func(string)
	}{
		{
			name: "invalid legacy root status",
			mutate: func(actionID string) {
				t.Helper()
				if _, err := f.store.pool.Exec(ctx,
					`UPDATE pending_actions SET status='blocked'
					  WHERE id=$1`,
					actionID,
				); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "attempt count drift",
			mutate: func(actionID string) {
				t.Helper()
				if _, err := f.store.pool.Exec(ctx,
					`UPDATE agent_action_continuations
					    SET attempt_count=1
					  WHERE action_id=$1`,
					actionID,
				); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "lease fence drift",
			mutate: func(actionID string) {
				t.Helper()
				if _, err := f.store.pool.Exec(ctx,
					`UPDATE agent_action_continuations
					    SET lease_fence=1
					  WHERE action_id=$1`,
					actionID,
				); err != nil {
					t.Fatal(err)
				}
			},
		},
	}

	type snapshot struct {
		rootStatus        string
		executionVersion  int
		continuationState string
		attemptCount      int
		leaseFence        int64
		rootUpdatedAt     time.Time
		actionUpdatedAt   time.Time
		authorityHistory  string
	}
	readSnapshot := func(actionID string) snapshot {
		t.Helper()
		var got snapshot
		if err := f.store.pool.QueryRow(ctx,
			`SELECT p.status,p.execution_version,c.status,
			        c.attempt_count,c.lease_fence,p.updated_at,c.updated_at,
			        (SELECT string_agg(
			             generation::text||':'||mode||':'||evidence,
			             ',' ORDER BY generation
			           )
			           FROM agent_action_continuation_authority_events e
			          WHERE e.action_id=p.id)
			   FROM pending_actions p
			   JOIN agent_action_continuations c ON c.action_id=p.id
			  WHERE p.id=$1`,
			actionID,
		).Scan(
			&got.rootStatus, &got.executionVersion,
			&got.continuationState, &got.attemptCount,
			&got.leaseFence, &got.rootUpdatedAt,
			&got.actionUpdatedAt, &got.authorityHistory,
		); err != nil {
			t.Fatal(err)
		}
		return got
	}

	for index, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actionID := uuid.NewString()
			actionIDs = append(actionIDs, actionID)
			if err := f.store.CreatePendingAction(
				ctx,
				&types.PendingAction{
					ID: actionID, UserID: f.userA,
					SessionID: &f.sessionA, ToolName: "enable_source",
					Args: []byte(fmt.Sprintf(
						`{"source_id":%d}`, 930201+index,
					)),
					Summary:   "rollback drift rejection",
					Status:    types.PendingActionStatusPending,
					ExpiresAt: time.Now().Add(time.Hour),
				},
			); err != nil {
				t.Fatal(err)
			}
			if _, err := f.store.ActivateAgentActionContinuation(
				ctx, f.tenantA, f.userA, actionID,
				"drift activation",
			); err != nil {
				t.Fatal(err)
			}
			const evidence = "drift rollback"
			if _, err := f.store.RollbackAgentActionContinuation(
				ctx, f.tenantA, f.userA, actionID, evidence,
			); err != nil {
				t.Fatal(err)
			}
			tt.mutate(actionID)
			before := readSnapshot(actionID)

			if _, err := f.store.RollbackAgentActionContinuation(
				ctx, f.tenantA, f.userA, actionID, evidence,
			); !errors.Is(err, types.ErrInternal) {
				t.Fatalf("rollback accepted generation-2 drift: %v", err)
			}
			if _, err := f.store.GetAgentActionContinuationStatus(
				ctx, f.tenantA, f.userA, actionID,
			); !errors.Is(err, types.ErrInternal) {
				t.Fatalf("status accepted generation-2 drift: %v", err)
			}
			after := readSnapshot(actionID)
			if before != after {
				t.Fatalf(
					"rejected generation-2 drift wrote state\nbefore=%+v\nafter=%+v",
					before, after,
				)
			}
		})
	}
}
