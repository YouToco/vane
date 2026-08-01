// Package store 是数据访问层：持有 pgx 连接池，按实体拆分 .go 文件提供查询方法。
package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Store 持有数据库连接池，是所有数据访问方法的接收者。
// 零值不可用，必须通过 New 构造。
type Store struct {
	pool                    *pgxpool.Pool
	researchPool            *pgxpool.Pool
	beginTx                 func(context.Context, pgx.TxOptions) (pgx.Tx, error)
	beginResearchTx         func(context.Context, pgx.TxOptions) (pgx.Tx, error)
	intelligenceCursorState *intelligenceCursorState
}

var errResearchRuntimeUnavailable = errors.New("store: V3 research runtime database is not configured")

const (
	researchRuntimeLoginRole      = "vane_research_runtime"
	researchRuntimeCapabilityRole = "vane_research_v3_executor"
)

var researchRuntimeRelations = []string{
	"tenants",
	"users",
	"memberships",
	"schedules",
	"task_approved_definition_versions",
	"task_run_snapshots",
	"tenant_quota",
	"research_run_plans",
	"research_run_steps",
	"research_run_evidence",
	"research_brief_syntheses",
	"research_run_step_spend_reservations",
	"research_run_step_spend_settlements",
	"tool_calls",
}

var researchRuntimeScopedRelations = []string{
	"tenants",
	"memberships",
	"schedules",
	"task_approved_definition_versions",
	"task_run_snapshots",
	"research_run_plans",
	"research_run_steps",
	"research_run_evidence",
	"research_brief_syntheses",
	"research_run_step_spend_reservations",
	"research_run_step_spend_settlements",
	"tool_calls",
}

type intelligenceCursorState struct {
	sync.Mutex
	keys       map[int][]byte
	activeKey  int
	keysLoaded time.Time
}

// New 解析连接串、建立连接池并做一次连通性检查。
// 池参数按单 VPS + MVP 负载设定：MaxConns=10 足够覆盖 HTTP + 后台任务。
func New(ctx context.Context, dbURL string) (*Store, error) {
	pool, err := newStorePool(ctx, dbURL, nil)
	if err != nil {
		return nil, err
	}
	return newStore(pool, nil), nil
}

// NewWithResearchRuntime creates the normal schema-owner pool and a separate
// V3 runtime pool. The latter must authenticate as a non-owner login whose only
// V3 data access is obtained by SET ROLE vane_research_v3_executor. Keeping session_user
// unprivileged means RESET ROLE cannot recover schema-owner DELETE authority.
func NewWithResearchRuntime(
	ctx context.Context, dbURL, researchRuntimeURL string,
) (*Store, error) {
	pool, err := newStorePool(ctx, dbURL, nil)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(researchRuntimeURL) == "" {
		return newStore(pool, nil), nil
	}
	researchPool, err := newStorePool(ctx, researchRuntimeURL, validateResearchRuntimeConnection)
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("store: V3 research runtime: %w", err)
	}
	return newStore(pool, researchPool), nil
}

func newStorePool(
	ctx context.Context, dbURL string,
	afterConnect func(context.Context, *pgx.Conn) error,
) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dbURL)
	if err != nil {
		return nil, fmt.Errorf("store: 解析数据库连接串: %w", err)
	}
	cfg.MaxConns = 10
	cfg.MinConns = 2
	cfg.MaxConnLifetime = time.Hour
	cfg.MaxConnIdleTime = 30 * time.Minute
	cfg.AfterConnect = afterConnect

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("store: 创建连接池: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("store: 数据库连通性检查: %w", err)
	}

	return pool, nil
}

func newStore(pool, researchPool *pgxpool.Pool) *Store {
	store := &Store{
		pool: pool, beginTx: pool.BeginTx,
		intelligenceCursorState: &intelligenceCursorState{},
		beginResearchTx: func(context.Context, pgx.TxOptions) (pgx.Tx, error) {
			return nil, errResearchRuntimeUnavailable
		},
	}
	if researchPool != nil {
		store.researchPool = researchPool
		store.beginResearchTx = researchPool.BeginTx
	}
	return store
}

func (s *Store) beginResearchTransaction(
	ctx context.Context, options pgx.TxOptions,
) (pgx.Tx, error) {
	if s == nil || s.beginResearchTx == nil {
		return nil, errResearchRuntimeUnavailable
	}
	return s.beginResearchTx(ctx, options)
}

func validateResearchRuntimeConnection(ctx context.Context, conn *pgx.Conn) error {
	if conn == nil {
		return errors.New("runtime connection is nil")
	}
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin authority probe: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	var sessionUser string
	var canLogin, superuser, bypassRLS, createDB, createRole, replication, inherit bool
	if err := tx.QueryRow(ctx,
		`SELECT role.rolname,role.rolcanlogin,role.rolsuper,role.rolbypassrls,
		        role.rolcreatedb,role.rolcreaterole,role.rolreplication,role.rolinherit
		   FROM pg_catalog.pg_roles role WHERE role.rolname=session_user`,
	).Scan(&sessionUser, &canLogin, &superuser, &bypassRLS,
		&createDB, &createRole, &replication, &inherit); err != nil {
		return fmt.Errorf("read runtime identity: %w", err)
	}
	if sessionUser != researchRuntimeLoginRole || !canLogin || superuser || bypassRLS ||
		createDB || createRole || replication || inherit {
		return fmt.Errorf("runtime login %q has unsafe role attributes", sessionUser)
	}
	var unexpectedMemberships []string
	membershipRows, err := tx.Query(ctx,
		`SELECT candidate.rolname
		   FROM pg_catalog.pg_roles candidate
		  WHERE candidate.rolname NOT IN (session_user,$1)
		    AND pg_catalog.pg_has_role(session_user,candidate.oid,'MEMBER')
		  ORDER BY candidate.rolname`, researchRuntimeCapabilityRole)
	if err != nil {
		return fmt.Errorf("inspect runtime memberships: %w", err)
	}
	for membershipRows.Next() {
		var role string
		if err := membershipRows.Scan(&role); err != nil {
			membershipRows.Close()
			return fmt.Errorf("scan runtime membership: %w", err)
		}
		unexpectedMemberships = append(unexpectedMemberships, role)
	}
	if err := membershipRows.Err(); err != nil {
		membershipRows.Close()
		return fmt.Errorf("iterate runtime memberships: %w", err)
	}
	membershipRows.Close()
	if len(unexpectedMemberships) != 0 {
		return fmt.Errorf("runtime login %q has unexpected memberships: %v",
			sessionUser, unexpectedMemberships)
	}

	var owned []string
	rows, err := tx.Query(ctx,
		`SELECT class.relname
		   FROM pg_catalog.pg_class class
		   JOIN pg_catalog.pg_namespace namespace ON namespace.oid=class.relnamespace
		   JOIN pg_catalog.pg_roles owner ON owner.oid=class.relowner
		  WHERE namespace.nspname='public' AND class.relname=ANY($1::text[])
		    AND owner.rolname=session_user
		  ORDER BY class.relname`, researchRuntimeRelations)
	if err != nil {
		return fmt.Errorf("inspect runtime ownership: %w", err)
	}
	for rows.Next() {
		var relation string
		if err := rows.Scan(&relation); err != nil {
			rows.Close()
			return fmt.Errorf("scan runtime ownership: %w", err)
		}
		owned = append(owned, relation)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("iterate runtime ownership: %w", err)
	}
	rows.Close()
	if len(owned) != 0 {
		return fmt.Errorf("runtime login %q owns protected relations: %v", sessionUser, owned)
	}

	var capabilityCanLogin, capabilitySuper, capabilityBypass, capabilityCreateDB,
		capabilityCreateRole, capabilityReplication, capabilityInherit,
		capabilityInheritsVaneApp bool
	if err := tx.QueryRow(ctx,
		`SELECT role.rolcanlogin,role.rolsuper,role.rolbypassrls,
		        role.rolcreatedb,role.rolcreaterole,role.rolreplication,role.rolinherit,
		        pg_catalog.pg_has_role(role.oid,'vane_app','MEMBER')
		   FROM pg_catalog.pg_roles role WHERE role.rolname=$1`,
		researchRuntimeCapabilityRole,
	).Scan(&capabilityCanLogin, &capabilitySuper, &capabilityBypass,
		&capabilityCreateDB, &capabilityCreateRole, &capabilityReplication,
		&capabilityInherit,
		&capabilityInheritsVaneApp); err != nil {
		return fmt.Errorf("inspect research capability: %w", err)
	}
	if capabilityCanLogin || capabilitySuper || capabilityBypass || capabilityCreateDB ||
		capabilityCreateRole || capabilityReplication || capabilityInherit ||
		capabilityInheritsVaneApp {
		return fmt.Errorf("research capability %q has unsafe attributes", researchRuntimeCapabilityRole)
	}
	policyRows, err := tx.Query(ctx,
		`SELECT class.relname
		   FROM pg_catalog.pg_class class
		   JOIN pg_catalog.pg_namespace namespace ON namespace.oid=class.relnamespace
		   JOIN pg_catalog.pg_policy policy ON policy.polrelid=class.oid
		   JOIN pg_catalog.pg_roles role ON role.oid=ANY(policy.polroles)
		  WHERE namespace.nspname='public' AND class.relname=ANY($1::text[])
		    AND class.relrowsecurity AND policy.polname='research_v3_scope'
		    AND policy.polpermissive=false AND role.rolname=$2
		  ORDER BY class.relname`, researchRuntimeScopedRelations,
		researchRuntimeCapabilityRole)
	if err != nil {
		return fmt.Errorf("inspect research capability RLS: %w", err)
	}
	var scoped []string
	for policyRows.Next() {
		var relation string
		if err := policyRows.Scan(&relation); err != nil {
			policyRows.Close()
			return fmt.Errorf("scan research capability RLS: %w", err)
		}
		scoped = append(scoped, relation)
	}
	if err := policyRows.Err(); err != nil {
		policyRows.Close()
		return fmt.Errorf("iterate research capability RLS: %w", err)
	}
	policyRows.Close()
	if len(scoped) != len(researchRuntimeScopedRelations) {
		return fmt.Errorf("research capability RLS coverage is incomplete: %v", scoped)
	}
	if _, err := tx.Exec(ctx, `SET LOCAL ROLE `+researchRuntimeCapabilityRole); err != nil {
		return fmt.Errorf("runtime login cannot SET ROLE %s: %w", researchRuntimeCapabilityRole, err)
	}
	var activeRole string
	if err := tx.QueryRow(ctx, `SELECT current_user`).Scan(&activeRole); err != nil {
		return fmt.Errorf("read runtime active role: %w", err)
	}
	if activeRole != researchRuntimeCapabilityRole {
		return fmt.Errorf("runtime SET ROLE selected unexpected role %q", activeRole)
	}
	var hasDelete, hasTruncate, canMutateTenant, canMutateMembership, canMutateTool bool
	if err := tx.QueryRow(ctx,
		`SELECT
		    has_table_privilege(current_user,'tenants','DELETE'),
		    has_table_privilege(current_user,'research_run_steps','TRUNCATE'),
		    has_table_privilege(current_user,'tenants','UPDATE,INSERT'),
		    has_table_privilege(current_user,'memberships','UPDATE,INSERT'),
		    has_table_privilege(current_user,'tool_calls','UPDATE,DELETE')`,
	).Scan(&hasDelete, &hasTruncate, &canMutateTenant, &canMutateMembership,
		&canMutateTool); err != nil {
		return fmt.Errorf("inspect research capability privileges: %w", err)
	}
	if hasDelete || hasTruncate || canMutateTenant || canMutateMembership || canMutateTool {
		return fmt.Errorf("research capability %q retains destructive privileges", activeRole)
	}
	if _, err := tx.Exec(ctx, `RESET ROLE`); err != nil {
		return fmt.Errorf("reset runtime role: %w", err)
	}
	if err := tx.QueryRow(ctx, `SELECT current_user`).Scan(&activeRole); err != nil {
		return fmt.Errorf("read reset runtime role: %w", err)
	}
	if activeRole != sessionUser {
		return fmt.Errorf("RESET ROLE escaped runtime login: current=%q session=%q", activeRole, sessionUser)
	}
	if _, err := tx.Exec(ctx, `SAVEPOINT research_runtime_legacy_role_probe`); err != nil {
		return fmt.Errorf("start legacy role probe: %w", err)
	}
	_, legacyRoleErr := tx.Exec(ctx, `SET LOCAL ROLE vane_app`)
	var legacyPgErr *pgconn.PgError
	if legacyRoleErr == nil || !errors.As(legacyRoleErr, &legacyPgErr) || legacyPgErr.Code != "42501" {
		return fmt.Errorf("runtime login can still enter legacy vane_app role: %v", legacyRoleErr)
	}
	if _, err := tx.Exec(ctx, `ROLLBACK TO SAVEPOINT research_runtime_legacy_role_probe`); err != nil {
		return fmt.Errorf("rollback legacy role probe: %w", err)
	}

	// A zero-row DELETE exercises PostgreSQL's effective privilege check without
	// mutating data. Permission denial is the required outcome after RESET ROLE.
	if _, err := tx.Exec(ctx, `SAVEPOINT research_runtime_delete_probe`); err != nil {
		return fmt.Errorf("start runtime delete probe: %w", err)
	}
	_, deleteErr := tx.Exec(ctx, `DELETE FROM public.tool_calls WHERE false`)
	if deleteErr == nil {
		return fmt.Errorf("runtime login %q retains DELETE after RESET ROLE", sessionUser)
	}
	var pgErr *pgconn.PgError
	if !errors.As(deleteErr, &pgErr) || pgErr.Code != "42501" {
		return fmt.Errorf("runtime delete probe failed unexpectedly: %w", deleteErr)
	}
	if _, err := tx.Exec(ctx, `ROLLBACK TO SAVEPOINT research_runtime_delete_probe`); err != nil {
		return fmt.Errorf("rollback runtime delete probe: %w", err)
	}
	return nil
}

func (s *Store) ensureIntelligenceCursorKeys(ctx context.Context) error {
	state := s.intelligenceCursorState
	if state == nil {
		return fmt.Errorf("store: 情报查询游标状态未初始化")
	}
	state.Lock()
	defer state.Unlock()
	if state.activeKey != 0 && len(state.keys) > 0 &&
		time.Since(state.keysLoaded) < time.Minute {
		return nil
	}
	return s.loadIntelligenceCursorKeysLocked(ctx)
}

func (s *Store) reloadIntelligenceCursorKeys(ctx context.Context) error {
	state := s.intelligenceCursorState
	if state == nil {
		return fmt.Errorf("store: 情报查询游标状态未初始化")
	}
	state.Lock()
	defer state.Unlock()
	return s.loadIntelligenceCursorKeysLocked(ctx)
}

func (s *Store) loadIntelligenceCursorKeysLocked(ctx context.Context) error {
	rows, err := s.pool.Query(ctx,
		`SELECT key_version,key_bytes,active
		   FROM public.agent_intelligence_cursor_keys ORDER BY key_version`)
	if err != nil {
		return fmt.Errorf("store: 读取情报查询游标签名: %w", err)
	}
	defer rows.Close()
	keys := make(map[int][]byte)
	activeVersion := 0
	for rows.Next() {
		var version int
		var key []byte
		var active bool
		if err := rows.Scan(&version, &key, &active); err != nil {
			return fmt.Errorf("store: 扫描情报查询游标签名: %w", err)
		}
		if version <= 0 || len(key) < 16 || len(key) > 64 {
			return fmt.Errorf("store: 情报查询游标签名材料无效")
		}
		keys[version] = append([]byte(nil), key...)
		if active {
			if activeVersion != 0 {
				return fmt.Errorf("store: 存在多个 active 情报查询游标签名")
			}
			activeVersion = version
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("store: 遍历情报查询游标签名: %w", err)
	}
	if activeVersion == 0 {
		return fmt.Errorf("store: 缺少 active 情报查询游标签名")
	}
	state := s.intelligenceCursorState
	state.keys = keys
	state.activeKey = activeVersion
	state.keysLoaded = time.Now()
	return nil
}

// Ping 检查数据库连通性，供 /readyz 就绪探针使用。
func (s *Store) Ping(ctx context.Context) error {
	if err := s.pool.Ping(ctx); err != nil {
		return err
	}
	if s.researchPool != nil {
		return s.researchPool.Ping(ctx)
	}
	return nil
}

// Close 关闭连接池，等待已借出的连接归还后释放。
func (s *Store) Close() {
	if s.researchPool != nil {
		s.researchPool.Close()
	}
	s.pool.Close()
}
