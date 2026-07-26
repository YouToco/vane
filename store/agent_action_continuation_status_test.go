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
