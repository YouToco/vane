package store

import (
	"context"
	"fmt"
	"strconv"

	"github.com/jackc/pgx/v5"
)

func (s *Store) beginTaskCreationTenantTx(
	ctx context.Context, tenantID int64, options pgx.TxOptions,
) (pgx.Tx, error) {
	if tenantID <= 0 {
		return nil, taskCreationValidation("task creation tenant scope is invalid")
	}
	tx, err := s.beginTx(ctx, options)
	if err != nil {
		return nil, err
	}
	fail := func(cause error) (pgx.Tx, error) {
		rollbackTaskCreationTransaction(ctx, tx)
		return nil, cause
	}
	if _, err := tx.Exec(ctx, `SET LOCAL search_path=pg_catalog,public`); err != nil {
		return fail(taskCreationDatabaseError("pin task creation search path", err))
	}
	if _, err := tx.Exec(ctx,
		`SELECT set_config('app.tenant_id',$1,true)`,
		strconv.FormatInt(tenantID, 10)); err != nil {
		return fail(taskCreationDatabaseError("bind task creation tenant", err))
	}
	if _, err := tx.Exec(ctx, `SET LOCAL ROLE vane_app`); err != nil {
		return fail(taskCreationDatabaseError("enter task creation runtime role", err))
	}
	var currentRole, tenantContext, searchPath string
	var readOnly bool
	if err := tx.QueryRow(ctx, `
		SELECT current_user,current_setting('app.tenant_id',true),
		       current_setting('search_path'),
		       current_setting('transaction_read_only')::boolean`,
	).Scan(&currentRole, &tenantContext, &searchPath, &readOnly); err != nil {
		return fail(taskCreationDatabaseError("verify task creation runtime scope", err))
	}
	expectReadOnly := options.AccessMode == pgx.ReadOnly
	if currentRole != "vane_app" ||
		tenantContext != strconv.FormatInt(tenantID, 10) ||
		searchPath != "pg_catalog, public" || readOnly != expectReadOnly {
		return fail(taskCreationDatabaseError(
			"task creation runtime scope is invalid",
			fmt.Errorf(
				"role=%s tenant=%s search_path=%s read_only=%t expected_read_only=%t",
				currentRole, tenantContext, searchPath, readOnly, expectReadOnly,
			),
		))
	}
	return tx, nil
}
