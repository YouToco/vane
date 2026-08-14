package store

import (
	"testing"

	"github.com/YouToco/vane/server/types"
)

func TestListNonterminalTaskDefinitionEditsPaginatesByTenantAndOperation(t *testing.T) {
	first := newTaskDefinitionEditOperationFixture(t)
	ctx := t.Context()

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
