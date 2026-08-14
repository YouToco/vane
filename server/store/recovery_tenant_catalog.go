package store

import (
	"context"
	"fmt"
	"strconv"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/server/types"
)

const maxRecoveryTenantCatalogPage = 1000

// ListRecoveryTenantCatalogPage is the only cross-tenant discovery primitive
// used by background recovery. It deliberately returns tenant identities only;
// every business-row read must re-enter a tenant-scoped transaction afterwards.
//
// The explicit vane_app role and fixed search path make this safe under both the
// migration owner used by tests and the NOINHERIT vane_server_runtime login used
// in production. The transaction is read-only and keyset bounded, so this does
// not become a general owner/admin query surface.
func (s *Store) ListRecoveryTenantCatalogPage(
	ctx context.Context,
	afterTenantID int64,
	limit int,
) ([]int64, error) {
	if afterTenantID < 0 || limit <= 0 || limit > maxRecoveryTenantCatalogPage {
		return nil, types.NewAppError(
			types.CodeValidation, "恢复租户目录游标无效", types.ErrValidation,
		)
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, types.NewAppError(
			types.CodeDatabase, "开启恢复租户目录事务", err,
		)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if _, err := tx.Exec(ctx, `SET LOCAL search_path=pg_catalog,public`); err != nil {
		return nil, types.NewAppError(
			types.CodeDatabase, "固定恢复租户目录搜索路径", err,
		)
	}
	if _, err := tx.Exec(ctx, `SET LOCAL ROLE vane_app`); err != nil {
		return nil, types.NewAppError(
			types.CodeDatabase, "进入恢复租户目录只读角色", err,
		)
	}
	var currentRole, searchPath string
	var readOnly bool
	if err := tx.QueryRow(ctx, `
		SELECT current_user,current_setting('search_path'),
		       current_setting('transaction_read_only')::boolean`,
	).Scan(&currentRole, &searchPath, &readOnly); err != nil {
		return nil, types.NewAppError(
			types.CodeDatabase, "校验恢复租户目录权限", err,
		)
	}
	if currentRole != "vane_app" || searchPath != "pg_catalog, public" || !readOnly {
		return nil, types.NewAppError(
			types.CodeInternal, "恢复租户目录权限边界无效",
			fmt.Errorf("role=%s search_path=%s read_only=%t", currentRole, searchPath, readOnly),
		)
	}
	rows, err := tx.Query(ctx, `
		SELECT id
		  FROM public.tenants
		 WHERE id>$1
		 ORDER BY id
		 LIMIT $2`, afterTenantID, limit)
	if err != nil {
		return nil, types.NewAppError(
			types.CodeDatabase, "读取恢复租户目录", err,
		)
	}
	defer rows.Close()
	ids := make([]int64, 0, limit)
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, types.NewAppError(
				types.CodeDatabase, "扫描恢复租户目录", err,
			)
		}
		if id <= afterTenantID {
			return nil, types.NewAppError(
				types.CodeInternal, "恢复租户目录顺序损坏", types.ErrInternal,
			)
		}
		ids = append(ids, id)
		afterTenantID = id
	}
	if err := rows.Err(); err != nil {
		return nil, types.NewAppError(
			types.CodeDatabase, "遍历恢复租户目录", err,
		)
	}
	rows.Close()
	if err := tx.Commit(ctx); err != nil {
		return nil, types.NewAppError(
			types.CodeDatabase, "提交恢复租户目录事务", err,
		)
	}
	return ids, nil
}

// beginRecoveryTenantRead enters one closed, least-privilege recovery role and
// binds exactly one tenant before any business relation is read. Callers must
// keep an explicit tenant_id predicate as a second boundary in every query.
func (s *Store) beginRecoveryTenantRead(
	ctx context.Context,
	tenantID int64,
	role string,
) (pgx.Tx, error) {
	if tenantID <= 0 || !allowedRecoveryTenantReadRole(role) {
		return nil, types.NewAppError(
			types.CodeValidation, "恢复租户读取范围无效", types.ErrValidation,
		)
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "开启恢复租户读取", err)
	}
	fail := func(message string, cause error) (pgx.Tx, error) {
		_ = tx.Rollback(context.WithoutCancel(ctx))
		return nil, types.NewAppError(types.CodeDatabase, message, cause)
	}
	if _, err := tx.Exec(ctx, `SET LOCAL search_path=pg_catalog,public`); err != nil {
		return fail("固定恢复租户读取搜索路径", err)
	}
	if _, err := tx.Exec(ctx,
		`SELECT set_config('app.tenant_id',$1,true)`,
		strconv.FormatInt(tenantID, 10)); err != nil {
		return fail("绑定恢复租户读取范围", err)
	}
	// role is selected from allowedRecoveryTenantReadRole's exact literals.
	if _, err := tx.Exec(ctx, `SET LOCAL ROLE `+role); err != nil {
		return fail("进入恢复租户读取角色", err)
	}
	var currentRole, tenantContext, searchPath string
	var readOnly bool
	if err := tx.QueryRow(ctx, `
		SELECT current_user,current_setting('app.tenant_id',true),
		       current_setting('search_path'),
		       current_setting('transaction_read_only')::boolean`,
	).Scan(&currentRole, &tenantContext, &searchPath, &readOnly); err != nil {
		return fail("校验恢复租户读取权限", err)
	}
	if currentRole != role || tenantContext != strconv.FormatInt(tenantID, 10) ||
		searchPath != "pg_catalog, public" || !readOnly {
		return fail("恢复租户读取权限边界无效", fmt.Errorf(
			"role=%s tenant=%s search_path=%s read_only=%t",
			currentRole, tenantContext, searchPath, readOnly))
	}
	return tx, nil
}

func allowedRecoveryTenantReadRole(role string) bool {
	switch role {
	case "vane_app",
		"vane_edit_coordinator",
		"vane_schedule_commander":
		return true
	default:
		return false
	}
}
