package store

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
)

const taskDefinitionEditRollbackTimeout = 2 * time.Second

type taskDefinitionEditRoleExpectation struct {
	role                    string
	rlsTable                string
	probeTenantIsolation    bool
	mayUpdateMarker         bool
	mayUpdateReceiptBody    bool
	mayExecuteCutoverRebase bool
}

// ValidateTaskDefinitionEditRuntimeRoles proves the exact production
// connection can enter both scoped restricted roles before any edit ingress is
// admitted. This is a runtime provisioning gate, not a replacement for the
// migration privilege matrix.
func (s *Store) ValidateTaskDefinitionEditRuntimeRoles(ctx context.Context) error {
	var maxTenantID, tenantCount int64
	if err := s.pool.QueryRow(ctx,
		`SELECT COALESCE(max(id),0), count(*) FROM tenants`,
	).Scan(&maxTenantID, &tenantCount); err != nil {
		return fmt.Errorf("load nonempty task definition edit RLS probe set: %w", err)
	}
	if tenantCount <= 0 || maxTenantID <= 0 || maxTenantID == int64(^uint64(0)>>1) {
		return fmt.Errorf(
			"task definition edit RLS probe set is unusable (count=%d max_tenant_id=%d)",
			tenantCount, maxTenantID,
		)
	}
	// max+1 is guaranteed absent at the owner snapshot. Every retained tenant
	// row is therefore cross-tenant for the scoped coordinator probe.
	probeTenantID := maxTenantID + 1
	for _, expected := range []taskDefinitionEditRoleExpectation{
		{
			role:                    "vane_edit_coordinator",
			rlsTable:                "schedules",
			probeTenantIsolation:    true,
			mayUpdateMarker:         true,
			mayExecuteCutoverRebase: true,
		},
		{
			role:                 "vane_edit_receipt",
			rlsTable:             "task_definition_edit_receipts",
			probeTenantIsolation: true,
			mayUpdateReceiptBody: true,
		},
	} {
		tx, err := s.beginTaskDefinitionEditRoleTx(ctx, probeTenantID, expected.role)
		if err != nil {
			return fmt.Errorf("validate task definition edit runtime role %s: %w",
				expected.role, err)
		}
		var (
			currentRole, tenantContext                        string
			superuser, bypassRLS, canLogin, inherits          bool
			createDB, createRole, replication, mayDeleteTasks bool
			mayUpdateMarker, mayUpdateReceipt                 bool
			mayExecuteCutoverRebase, mayUpdateCutoverPointer  bool
			mayInsertCutoverEvent, mayUseCutoverSequence      bool
			rowSecurityActive, ownsRLSTable                   bool
		)
		err = tx.QueryRow(ctx, `
			SELECT current_user,
			       current_setting('app.tenant_id', true),
			       r.rolsuper,
			       r.rolbypassrls,
			       r.rolcanlogin,
			       r.rolinherit,
			       r.rolcreatedb,
			       r.rolcreaterole,
			       r.rolreplication,
			       has_table_privilege(current_user, 'schedules', 'DELETE'),
			       has_column_privilege(
			           current_user, 'schedules',
			           'definition_edit_operation_id', 'UPDATE'
			       ),
			       has_column_privilege(
			           current_user, 'task_definition_edit_receipts',
			           'payload', 'UPDATE'
			       ),
			       has_function_privilege(
			           current_user,
			           'public.task_run_snapshot_v2_rebase_definition_edit(text,bigint,text)',
			           'EXECUTE'
			       ),
			       has_column_privilege(
			           current_user, 'schedules',
			           'run_snapshot_cutover_event_id', 'UPDATE'
			       ),
			       has_table_privilege(
			           current_user, 'task_run_snapshot_v2_cutover_events',
			           'INSERT'
			       ),
			       has_sequence_privilege(
			           current_user,
			           'task_run_snapshot_v2_cutover_events_id_seq',
			           'USAGE'
			       ),
			       row_security_active($1::regclass),
			       c.relowner = r.oid
			  FROM pg_roles r
			  JOIN pg_class c ON c.oid = $1::regclass
			 WHERE r.rolname = current_user`,
			expected.rlsTable,
		).Scan(
			&currentRole, &tenantContext, &superuser, &bypassRLS, &canLogin,
			&inherits, &createDB, &createRole, &replication,
			&mayDeleteTasks, &mayUpdateMarker, &mayUpdateReceipt,
			&mayExecuteCutoverRebase, &mayUpdateCutoverPointer,
			&mayInsertCutoverEvent, &mayUseCutoverSequence,
			&rowSecurityActive, &ownsRLSTable,
		)
		if err != nil {
			rollbackTaskDefinitionEditTx(ctx, tx)
			return fmt.Errorf("inspect task definition edit runtime role %s: %w",
				expected.role, err)
		}
		var crossTenantRows int64
		if expected.probeTenantIsolation {
			var probeErr error
			switch expected.role {
			case "vane_edit_coordinator":
				probeErr = tx.QueryRow(ctx, `SELECT count(*) FROM tenants`).Scan(
					&crossTenantRows,
				)
			case "vane_edit_receipt":
				probeErr = tx.QueryRow(ctx,
					`SELECT count(*) FROM task_definition_edit_receipts`,
				).Scan(&crossTenantRows)
			default:
				probeErr = fmt.Errorf("unsupported tenant isolation probe role")
			}
			if probeErr != nil {
				rollbackTaskDefinitionEditTx(ctx, tx)
				return fmt.Errorf(
					"probe task definition edit runtime role %s tenant isolation: %w",
					expected.role, probeErr,
				)
			}
		}
		rollbackTaskDefinitionEditTx(ctx, tx)
		if err := validateTaskDefinitionEditRLSObservation(
			expected.role,
			expected.rlsTable,
			rowSecurityActive,
			ownsRLSTable,
			expected.probeTenantIsolation,
			crossTenantRows,
			tenantCount,
		); err != nil {
			return err
		}
		if currentRole != expected.role ||
			tenantContext != strconv.FormatInt(probeTenantID, 10) ||
			superuser || bypassRLS || canLogin || inherits || createDB ||
			createRole || replication || mayDeleteTasks ||
			mayUpdateMarker != expected.mayUpdateMarker ||
			mayUpdateReceipt != expected.mayUpdateReceiptBody ||
			mayExecuteCutoverRebase != expected.mayExecuteCutoverRebase ||
			mayUpdateCutoverPointer || mayInsertCutoverEvent ||
			mayUseCutoverSequence {
			return fmt.Errorf(
				"task definition edit runtime role %s has unsafe capabilities "+
					"(current=%s tenant=%s superuser=%t bypass_rls=%t login=%t inherit=%t createdb=%t createrole=%t replication=%t delete_schedule=%t marker_update=%t receipt_payload_update=%t cutover_rebase_execute=%t cutover_pointer_update=%t cutover_event_insert=%t cutover_sequence_usage=%t rls_table=%s row_security_active=%t owns_rls_table=%t cross_tenant_rows=%d owner_probe_rows=%d)",
				expected.role, currentRole, tenantContext, superuser, bypassRLS,
				canLogin, inherits, createDB, createRole, replication,
				mayDeleteTasks, mayUpdateMarker, mayUpdateReceipt,
				mayExecuteCutoverRebase, mayUpdateCutoverPointer,
				mayInsertCutoverEvent, mayUseCutoverSequence,
				expected.rlsTable, rowSecurityActive, ownsRLSTable,
				crossTenantRows, tenantCount,
			)
		}
	}
	return nil
}

func validateTaskDefinitionEditRLSObservation(
	role, table string,
	rowSecurityActive, ownsTable, probesTenantIsolation bool,
	crossTenantRows, ownerProbeRows int64,
) error {
	if ownerProbeRows <= 0 || !rowSecurityActive || ownsTable ||
		(probesTenantIsolation && crossTenantRows != 0) {
		return fmt.Errorf(
			"task definition edit runtime role %s failed RLS probe "+
				"(table=%s row_security_active=%t owns_table=%t probes_tenant_isolation=%t cross_tenant_rows=%d owner_probe_rows=%d)",
			role, table, rowSecurityActive, ownsTable, probesTenantIsolation,
			crossTenantRows, ownerProbeRows,
		)
	}
	return nil
}

// beginTaskDefinitionEditTx starts a tenant-scoped transaction under the
// restricted edit-coordinator role. Cross-tenant discovery is deliberately
// unavailable here; the separately audited owner READ ONLY due-index discovery
// exception does not use this helper.
func (s *Store) beginTaskDefinitionEditTx(
	ctx context.Context,
	tenantID int64,
) (pgx.Tx, error) {
	return s.beginTaskDefinitionEditRoleTx(ctx, tenantID, "vane_edit_coordinator")
}

// beginTaskDefinitionEditReceiptTx is the receipt-dispatch counterpart of
// beginTaskDefinitionEditTx. It cannot update operations, schedules, or
// Approved Definition state at the database privilege boundary.
func (s *Store) beginTaskDefinitionEditReceiptTx(
	ctx context.Context,
	tenantID int64,
) (pgx.Tx, error) {
	return s.beginTaskDefinitionEditRoleTx(ctx, tenantID, "vane_edit_receipt")
}

func (s *Store) beginTaskDefinitionEditRoleTx(
	ctx context.Context,
	tenantID int64,
	role string,
) (pgx.Tx, error) {
	if tenantID <= 0 {
		return nil, fmt.Errorf("begin task definition edit transaction: tenant id is not positive")
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("begin task definition edit transaction: %w", err)
	}

	tenantContext := strconv.FormatInt(tenantID, 10)
	if _, err := tx.Exec(ctx,
		`SELECT set_config('app.tenant_id', $1, true)`, tenantContext); err != nil {
		rollbackTaskDefinitionEditTx(ctx, tx)
		return nil, fmt.Errorf("set task definition edit tenant context: %w", err)
	}

	var setRoleSQL string
	switch role {
	case "vane_edit_coordinator":
		setRoleSQL = `SET LOCAL ROLE vane_edit_coordinator`
	case "vane_edit_receipt":
		setRoleSQL = `SET LOCAL ROLE vane_edit_receipt`
	default:
		rollbackTaskDefinitionEditTx(ctx, tx)
		return nil, fmt.Errorf("set task definition edit role: unknown role")
	}
	if _, err := tx.Exec(ctx, setRoleSQL); err != nil {
		rollbackTaskDefinitionEditTx(ctx, tx)
		return nil, fmt.Errorf("set task definition edit role: %w", err)
	}
	return tx, nil
}

func rollbackTaskDefinitionEditTx(parent context.Context, tx pgx.Tx) {
	rollbackCtx, cancel := context.WithTimeout(
		context.WithoutCancel(parent), taskDefinitionEditRollbackTimeout)
	defer cancel()
	_ = tx.Rollback(rollbackCtx)
}
