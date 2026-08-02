package store

import (
	"context"

	"github.com/YouToco/vane/types"
)

const maxTaskDefinitionEditPreflightOperationIDBytes = 512

func (s *Store) ListNonterminalTaskDefinitionEditOperations(
	ctx context.Context,
	tenantID int64,
	afterOperationID string,
	limit int,
) ([]types.TaskDefinitionEditOperation, error) {
	if tenantID <= 0 || limit <= 0 || limit > 1000 ||
		(afterOperationID != "" &&
			!validTaskDefinitionEditReference(
				afterOperationID, maxTaskDefinitionEditPreflightOperationIDBytes)) {
		return nil, taskDefinitionEditValidation(
			"nonterminal operation query is invalid")
	}
	tx, err := s.beginTaskDefinitionEditTx(ctx, tenantID)
	if err != nil {
		return nil, taskDefinitionEditDatabaseError(
			"begin nonterminal operation scan", err)
	}
	defer rollbackTaskDefinitionEditTx(ctx, tx)
	rows, err := tx.Query(ctx,
		`SELECT `+taskDefinitionEditOperationColumns+`
		   FROM task_definition_edit_operations
		  WHERE tenant_id=$1 AND id>$2
		    AND status IN ($3,$4)
		    AND tombstoned_at IS NULL
		  ORDER BY id
		  LIMIT $5`,
		tenantID, afterOperationID,
		types.TaskDefinitionEditOperationStatusPending,
		types.TaskDefinitionEditOperationStatusExecuting,
		limit,
	)
	if err != nil {
		return nil, taskDefinitionEditDatabaseError(
			"list nonterminal operations", err)
	}
	defer rows.Close()
	operations := make([]types.TaskDefinitionEditOperation, 0)
	for rows.Next() {
		var op types.TaskDefinitionEditOperation
		if err := scanTaskDefinitionEditOperation(rows, &op); err != nil {
			return nil, taskDefinitionEditDatabaseError(
				"scan nonterminal operation", err)
		}
		operations = append(operations, *cloneTaskDefinitionEditOperation(&op))
	}
	if err := rows.Err(); err != nil {
		return nil, taskDefinitionEditDatabaseError(
			"iterate nonterminal operations", err)
	}
	rows.Close()
	if err := tx.Commit(ctx); err != nil {
		return nil, taskDefinitionEditDatabaseError(
			"commit nonterminal operation scan", err)
	}
	return operations, nil
}
