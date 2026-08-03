package store

import (
	"context"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/types"
)

const maxTaskDefinitionEditPreflightOperationIDBytes = 512

// ListNonterminalTaskDefinitionEditTenantIDs is the bounded owner read-only
// discovery exception used only by the startup namespace preflight. It returns
// tenant identities, never operation payloads; all payload reads re-enter the
// tenant-scoped restricted coordinator role.
func (s *Store) ListNonterminalTaskDefinitionEditTenantIDs(
	ctx context.Context,
	afterTenantID int64,
	limit int,
) ([]int64, error) {
	if afterTenantID < 0 || limit <= 0 || limit > 1000 {
		return nil, taskDefinitionEditValidation(
			"nonterminal operation tenant query is invalid")
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, taskDefinitionEditDatabaseError(
			"begin nonterminal operation tenant scan", err)
	}
	defer rollbackTaskDefinitionEditTx(ctx, tx)
	rows, err := tx.Query(ctx, `
		SELECT DISTINCT tenant_id
		  FROM task_definition_edit_operations
		 WHERE tenant_id > $1
		   AND operation_protocol=$5
		   AND status IN ($2,$3)
		   AND tombstoned_at IS NULL
		 ORDER BY tenant_id
		 LIMIT $4`,
		afterTenantID,
		types.TaskDefinitionEditOperationStatusPending,
		types.TaskDefinitionEditOperationStatusExecuting,
		limit,
		types.TaskDefinitionEditProtocolLegacyV1V2,
	)
	if err != nil {
		return nil, taskDefinitionEditDatabaseError(
			"list nonterminal operation tenant shards", err)
	}
	defer rows.Close()
	tenantIDs := make([]int64, 0)
	for rows.Next() {
		var tenantID int64
		if err := rows.Scan(&tenantID); err != nil {
			return nil, taskDefinitionEditDatabaseError(
				"scan nonterminal operation tenant shard", err)
		}
		tenantIDs = append(tenantIDs, tenantID)
	}
	if err := rows.Err(); err != nil {
		return nil, taskDefinitionEditDatabaseError(
			"iterate nonterminal operation tenant shards", err)
	}
	rows.Close()
	if err := tx.Commit(ctx); err != nil {
		return nil, taskDefinitionEditDatabaseError(
			"commit nonterminal operation tenant scan", err)
	}
	return tenantIDs, nil
}

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
		    AND operation_protocol=$6
		    AND status IN ($3,$4)
		    AND tombstoned_at IS NULL
		  ORDER BY id
		  LIMIT $5`,
		tenantID, afterOperationID,
		types.TaskDefinitionEditOperationStatusPending,
		types.TaskDefinitionEditOperationStatusExecuting,
		limit,
		types.TaskDefinitionEditProtocolLegacyV1V2,
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
