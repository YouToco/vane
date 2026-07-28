package store

import (
	"context"
	"strconv"

	"github.com/jackc/pgx/v5"
)

const (
	tenantAdmissionRootLockNamespace = "vane/tenant-admission/v1/"
	tenantAdmissionRootLockSeed      = int64(0x56414e45)
)

// lockTenantAdmissionRoot serializes a tenant-scoped writer with irreversible
// tenant purge without taking a cross-tenant table lock. Callers must acquire
// this lock before any child aggregate lock.
//
// The advisory lock is deliberately separate from the tenants row lock:
// PurgeTenant must retain schedule -> definition operation -> receipt row-lock
// order, while definition-edit workers do not participate in this admission
// protocol. Taking the tenant row first would add a tenant -> schedule edge and
// deadlock against their existing schedule -> tenant edge.
func lockTenantAdmissionRoot(
	ctx context.Context,
	tx pgx.Tx,
	tenantID int64,
) (bool, error) {
	key := tenantAdmissionRootLockNamespace + strconv.FormatInt(tenantID, 10)
	if _, err := tx.Exec(ctx,
		`SELECT pg_advisory_xact_lock(hashtextextended($1, $2))`,
		key, tenantAdmissionRootLockSeed); err != nil {
		return false, err
	}

	var exists bool
	if err := tx.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM tenants WHERE id = $1)`,
		tenantID).Scan(&exists); err != nil {
		return false, err
	}
	return exists, nil
}

// lockTenantAdmissionRootShared admits a read that must finish before an
// irreversible tenant purge, while allowing same-tenant readers and writers
// to proceed in their established membership -> schedule row-lock order.
func lockTenantAdmissionRootShared(
	ctx context.Context,
	tx pgx.Tx,
	tenantID int64,
) (bool, error) {
	key := tenantAdmissionRootLockNamespace + strconv.FormatInt(tenantID, 10)
	if _, err := tx.Exec(ctx,
		`SELECT pg_advisory_xact_lock_shared(hashtextextended($1, $2))`,
		key, tenantAdmissionRootLockSeed); err != nil {
		return false, err
	}

	var exists bool
	if err := tx.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM tenants WHERE id = $1)`,
		tenantID).Scan(&exists); err != nil {
		return false, err
	}
	return exists, nil
}
