package store

import (
	"slices"
	"testing"

	"github.com/YouToco/vane/types"
)

func TestListNonterminalTaskDefinitionEditsPaginatesByTenantAndOperation(t *testing.T) {
	first := newTaskDefinitionEditOperationFixture(t)
	second := newTaskDefinitionEditOperationFixture(t)
	ctx := t.Context()

	tenantIDs, err := first.state.store.ListNonterminalTaskDefinitionEditTenantIDs(
		ctx, 0, 1,
	)
	if err != nil || len(tenantIDs) != 1 {
		t.Fatalf("first tenant page = %v, %v", tenantIDs, err)
	}
	nextTenantIDs, err := first.state.store.ListNonterminalTaskDefinitionEditTenantIDs(
		ctx, tenantIDs[0], 1,
	)
	if err != nil || len(nextTenantIDs) != 1 {
		t.Fatalf("second tenant page = %v, %v", nextTenantIDs, err)
	}
	wantTenantIDs := []int64{first.op.TenantID, second.op.TenantID}
	slices.Sort(wantTenantIDs)
	if !slices.Equal(
		[]int64{tenantIDs[0], nextTenantIDs[0]},
		wantTenantIDs,
	) {
		t.Fatalf("tenant pages = %v/%v, want %v",
			tenantIDs, nextTenantIDs, wantTenantIDs)
	}

	operations, err := first.state.store.ListNonterminalTaskDefinitionEditOperations(
		ctx, first.op.TenantID, "", 1,
	)
	if err != nil || len(operations) != 1 || operations[0].ID != first.op.ID {
		t.Fatalf("first operation page = %+v, %v", operations, err)
	}
	operations, err = first.state.store.ListNonterminalTaskDefinitionEditOperations(
		ctx, first.op.TenantID, operations[0].ID, 1,
	)
	if err != nil || len(operations) != 0 {
		t.Fatalf("operation cursor did not terminate page scan: %+v, %v",
			operations, err)
	}

	if _, err := first.state.store.pool.Exec(ctx, `
		UPDATE task_definition_edit_operations
		   SET status=$2, tombstoned_at=clock_timestamp()
		 WHERE id=$1`,
		first.op.ID, types.TaskDefinitionEditOperationStatusCancelled,
	); err != nil {
		t.Fatalf("terminalize operation: %v", err)
	}
	operations, err = first.state.store.ListNonterminalTaskDefinitionEditOperations(
		ctx, first.op.TenantID, "", 1,
	)
	if err != nil || len(operations) != 0 {
		t.Fatalf("terminal operation leaked into preflight page: %+v, %v",
			operations, err)
	}
}
