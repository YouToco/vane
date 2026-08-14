package store

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"io/fs"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pressly/goose/v3"
)

type migration032Fixture struct {
	db       *sql.DB
	provider *goose.Provider
	tenantA  int64
	userA    int64
	taskA    string
	tenantB  int64
	userB    int64
	taskB    string
}

func migration032Scratch(t *testing.T) migration032Fixture {
	t.Helper()
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("未设置 DATABASE_URL，跳过 032 迁移集成测试")
	}
	ctx := t.Context()
	scratchURL, drop := createScratchDB(ctx, t, dbURL)
	t.Cleanup(drop)
	db, err := sql.Open("pgx", scratchURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	dir, err := fs.Sub(migrationsFS, "migrations")
	if err != nil {
		t.Fatal(err)
	}
	provider, err := goose.NewProvider(goose.DialectPostgres, db, dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := provider.UpTo(ctx, 31); err != nil {
		t.Fatalf("迁移到 031 失败: %v", err)
	}

	f := migration032Fixture{
		db: db, provider: provider, tenantA: 1,
		taskA: "migration-032-task-a", taskB: "migration-032-task-b",
	}
	if err := db.QueryRowContext(ctx, `
		INSERT INTO users (feishu_open_id, name)
		VALUES ('migration-032-user-a', 'migration 032 A') RETURNING id`,
	).Scan(&f.userA); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx,
		`INSERT INTO memberships (tenant_id, user_id, role) VALUES (1, $1, 'owner')`,
		f.userA); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO schedules (id, tenant_id, user_id, nl_description, status)
		VALUES ($2, 1, $1, 'legacy compiled task A', 'paused')`,
		f.userA, f.taskA); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `
		INSERT INTO tenants (status, plan) VALUES ('active', 'free') RETURNING id`,
	).Scan(&f.tenantB); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `
		INSERT INTO users (feishu_open_id, name)
		VALUES ('migration-032-user-b', 'migration 032 B') RETURNING id`,
	).Scan(&f.userB); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx,
		`INSERT INTO memberships (tenant_id, user_id, role) VALUES ($1, $2, 'owner')`,
		f.tenantB, f.userB); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO schedules (id, tenant_id, user_id, nl_description, status)
		VALUES ($3, $1, $2, 'legacy compiled task B', 'paused')`,
		f.tenantB, f.userB, f.taskB); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.UpTo(ctx, 32); err != nil {
		t.Fatalf("迁移到 032 失败（PG18 sha256/FK 形状须可执行）: %v", err)
	}
	return f
}

func digest032(payload []byte) string {
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func insertApproved032(
	t *testing.T,
	f migration032Fixture,
	tenantID, userID int64,
	taskID string,
	version int64,
	mode, approvalRef string,
	payload []byte,
) string {
	t.Helper()
	digest := digest032(payload)
	if _, err := f.db.ExecContext(t.Context(), `
		INSERT INTO task_approved_definition_versions (
			tenant_id, user_id, task_id, version, schema_version,
			execution_mode, definition_digest, payload, approval_ref
		) VALUES ($1, $2, $3, $4, 'approved-definition/v1', $5, $6, $7, $8)`,
		tenantID, userID, taskID, version, mode, digest, payload, approvalRef); err != nil {
		t.Fatalf("插入 approved definition 失败: %v", err)
	}
	return digest
}

func TestMigration032BackfillConstraintsPermissionsAndRLS(t *testing.T) {
	f := migration032Scratch(t)
	ctx := t.Context()

	var legacyCompiled, legacyHeadless int
	if err := f.db.QueryRowContext(ctx, `
		SELECT count(*) FILTER (WHERE execution_mode = 'compiled'),
		       count(*) FILTER (WHERE approved_definition_version IS NULL AND
		                              approved_definition_digest IS NULL)
		  FROM schedules`,
	).Scan(&legacyCompiled, &legacyHeadless); err != nil {
		t.Fatal(err)
	}
	if legacyCompiled != 2 || legacyHeadless != 2 {
		t.Fatalf("存量任务必须显式回填 compiled 且保持无 head: compiled=%d headless=%d",
			legacyCompiled, legacyHeadless)
	}
	const taskDefault = "migration-032-task-default-mode"
	if _, err := f.db.ExecContext(ctx, `
		INSERT INTO schedules (id, tenant_id, user_id, nl_description, status)
		VALUES ($1, $2, $3, 'new default task', 'paused')`,
		taskDefault, f.tenantA, f.userA); err != nil {
		t.Fatal(err)
	}
	var defaultMode string
	if err := f.db.QueryRowContext(ctx,
		`SELECT execution_mode FROM schedules WHERE id=$1`, taskDefault).Scan(&defaultMode); err != nil {
		t.Fatal(err)
	}
	if defaultMode != "compiled" {
		t.Fatalf("新任务省略 execution_mode 时应 fail-safe 为 compiled，实得 %q", defaultMode)
	}

	var approvedRead, approvedInsert, approvedUpdate, approvedDelete bool
	var adaptiveRead, adaptiveInsert, adaptiveUpdate, adaptiveDelete bool
	if err := f.db.QueryRowContext(ctx, `
		SELECT has_table_privilege('vane_app', 'task_approved_definition_versions', 'SELECT'),
		       has_table_privilege('vane_app', 'task_approved_definition_versions', 'INSERT'),
		       has_table_privilege('vane_app', 'task_approved_definition_versions', 'UPDATE'),
		       has_table_privilege('vane_app', 'task_approved_definition_versions', 'DELETE'),
		       has_table_privilege('vane_app', 'task_adaptive_states', 'SELECT'),
		       has_table_privilege('vane_app', 'task_adaptive_states', 'INSERT'),
		       has_table_privilege('vane_app', 'task_adaptive_states', 'UPDATE'),
		       has_table_privilege('vane_app', 'task_adaptive_states', 'DELETE')`,
	).Scan(
		&approvedRead, &approvedInsert, &approvedUpdate, &approvedDelete,
		&adaptiveRead, &adaptiveInsert, &adaptiveUpdate, &adaptiveDelete,
	); err != nil {
		t.Fatal(err)
	}
	if !approvedRead || !approvedInsert || approvedUpdate || approvedDelete {
		t.Fatalf("approved definition 应只允许 SELECT/INSERT: select=%v insert=%v update=%v delete=%v",
			approvedRead, approvedInsert, approvedUpdate, approvedDelete)
	}
	if !adaptiveRead || !adaptiveInsert || !adaptiveUpdate || adaptiveDelete {
		t.Fatalf("adaptive state 应只允许 SELECT/INSERT/UPDATE: select=%v insert=%v update=%v delete=%v",
			adaptiveRead, adaptiveInsert, adaptiveUpdate, adaptiveDelete)
	}

	// Unknown/任意字符串不能落库；head 的 version/digest 必须严格成对。
	if _, err := f.db.ExecContext(ctx,
		`UPDATE schedules SET execution_mode = 'unknown' WHERE id = $1`, f.taskA); err == nil {
		t.Fatal("unknown execution mode 不得持久化")
	}
	if _, err := f.db.ExecContext(ctx, `
		UPDATE schedules SET approved_definition_version = 1,
		                     approved_definition_digest = NULL
		 WHERE id = $1`, f.taskA); err == nil {
		t.Fatal("半截 approved head 必须被 CHECK 拒绝")
	}
	if _, err := f.db.ExecContext(ctx,
		`UPDATE schedules SET execution_mode='discover_at_run' WHERE id=$1`,
		f.taskA); err == nil {
		t.Fatal("discover_at_run 不得在无 approved head 时持久化")
	}

	payloadA := []byte(`{"schema":"approved-definition/v1","topic":"A"}`)
	digestA := insertApproved032(t, f, f.tenantA, f.userA, f.taskA, 1,
		"compiled", "pending-action:032-a", payloadA)
	if _, err := f.db.ExecContext(ctx, `
		UPDATE schedules
		   SET approved_definition_version = 1, approved_definition_digest = $2
		 WHERE id = $1`, f.taskA, digestA); err != nil {
		t.Fatalf("绑定精确 approved head 失败: %v", err)
	}

	// DB 必须真正重算 SHA-256，而不只是检查 64 位外观。
	if _, err := f.db.ExecContext(ctx, `
		INSERT INTO task_approved_definition_versions (
			tenant_id, user_id, task_id, version, schema_version,
			execution_mode, definition_digest, payload, approval_ref
		) VALUES ($1, $2, $3, 2, 'approved-definition/v1', 'compiled',
		          repeat('0', 64), $4, 'pending-action:bad-digest')`,
		f.tenantA, f.userA, f.taskA, payloadA); err == nil {
		t.Fatal("伪造的 definition_digest 必须被 payload digest CHECK 拒绝")
	}
	for _, tc := range []struct {
		name    string
		payload []byte
	}{
		{name: "empty", payload: []byte{}},
		{name: "over two MiB", payload: make([]byte, 2097153)},
	} {
		t.Run("approved payload size "+tc.name, func(t *testing.T) {
			if _, err := f.db.ExecContext(ctx, `
				INSERT INTO task_approved_definition_versions (
					tenant_id, user_id, task_id, version, schema_version,
					execution_mode, definition_digest, payload, approval_ref
				) VALUES ($1, $2, $3, 2, 'approved-definition/v1', 'compiled',
				          $4, $5, $6)`,
				f.tenantA, f.userA, f.taskA, digest032(tc.payload), tc.payload,
				"pending-action:size-"+tc.name); err == nil {
				t.Fatalf("%s payload 必须被 size CHECK 拒绝", tc.name)
			}
		})
	}
	if _, err := f.db.ExecContext(ctx, `
		INSERT INTO task_approved_definition_versions (
			tenant_id, user_id, task_id, version, schema_version,
			execution_mode, definition_digest, payload, approval_ref
		) VALUES ($1, $2, $3, 2, 'approved-definition/v1', 'compiled',
		          $4, $5, 'pending-action:032-a')`,
		f.tenantA, f.userA, f.taskA, digestA, payloadA); err == nil {
		t.Fatal("同一 approval_ref 重放不得产生第二个 definition version")
	}
	if _, err := f.db.ExecContext(ctx, `
		INSERT INTO task_approved_definition_versions (
			tenant_id, user_id, task_id, version, schema_version,
			execution_mode, definition_digest, payload, approval_ref
		) VALUES ($1, $2, $3, 1, 'approved-definition/v1', 'compiled',
		          $4, $5, 'pending-action:cross-scope')`,
		f.tenantA, f.userA, f.taskB, digestA, payloadA); err == nil {
		t.Fatal("approved definition 不得借用另一 tenant/user 的 task id")
	}
	if _, err := f.db.ExecContext(ctx, `
		UPDATE schedules SET approved_definition_digest = repeat('f', 64)
		 WHERE id = $1`, f.taskA); err == nil {
		t.Fatal("head digest 不匹配 immutable definition 时必须被 FK 拒绝")
	}
	if _, err := f.db.ExecContext(ctx, `
		UPDATE schedules SET execution_mode = 'discover_at_run' WHERE id = $1`, f.taskA); err == nil {
		t.Fatal("head mode 不匹配 immutable definition 时必须被 FK 拒绝")
	}
	nonHeadPayload := []byte(`{"schema":"approved-definition/v1","topic":"historical"}`)
	insertApproved032(t, f, f.tenantA, f.userA, f.taskA, 2,
		"compiled", "pending-action:032-non-head", nonHeadPayload)
	if _, err := f.db.ExecContext(ctx, `
		DELETE FROM task_approved_definition_versions
		 WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3 AND version=2`,
		f.tenantA, f.userA, f.taskA); err != nil {
		t.Fatal(err)
	}
	var retainedHeadVersion int64
	var retainedHeadDigest, retainedHeadMode string
	if err := f.db.QueryRowContext(ctx, `
		SELECT approved_definition_version, approved_definition_digest, execution_mode
		  FROM schedules WHERE id=$1`, f.taskA).Scan(
		&retainedHeadVersion, &retainedHeadDigest, &retainedHeadMode); err != nil {
		t.Fatal(err)
	}
	if retainedHeadVersion != 1 || retainedHeadDigest != digestA || retainedHeadMode != "compiled" {
		t.Fatalf("删除 non-head definition 改变 current head: v=%d digest=%q mode=%q",
			retainedHeadVersion, retainedHeadDigest, retainedHeadMode)
	}

	adaptiveA := []byte(`{"schema":"adaptive-state/v1","query_variants":["A"]}`)
	adaptiveDigestA := digest032(adaptiveA)
	if _, err := f.db.ExecContext(ctx, `
		INSERT INTO task_adaptive_states (
			tenant_id, user_id, task_id, version, schema_version,
			payload_digest, payload, basis_definition_version, basis_definition_digest
		) VALUES ($1, $2, $3, 1, 'adaptive-state/v1', $4, $5, 1, repeat('f', 64))`,
		f.tenantA, f.userA, f.taskA, adaptiveDigestA, adaptiveA); err == nil {
		t.Fatal("adaptive basis digest 必须精确指向 immutable definition bytes")
	}
	if _, err := f.db.ExecContext(ctx, `
		INSERT INTO task_adaptive_states (
			tenant_id, user_id, task_id, version, schema_version,
			payload_digest, payload, basis_definition_version,
			basis_definition_digest, last_known_good_definition_version
		) VALUES ($1, $2, $3, 1, 'adaptive-state/v1', repeat('0', 64), $4, 1, $5, 1)`,
		f.tenantA, f.userA, f.taskA, adaptiveA, digestA); err == nil {
		t.Fatal("伪造的 adaptive payload_digest 必须被 digest CHECK 拒绝")
	}
	for _, tc := range []struct {
		name    string
		payload []byte
	}{
		{name: "empty", payload: []byte{}},
		{name: "over 256 KiB", payload: make([]byte, 262145)},
	} {
		t.Run("adaptive payload size "+tc.name, func(t *testing.T) {
			if _, err := f.db.ExecContext(ctx, `
				INSERT INTO task_adaptive_states (
					tenant_id, user_id, task_id, version, schema_version,
					payload_digest, payload, basis_definition_version,
					basis_definition_digest, last_known_good_definition_version
				) VALUES ($1, $2, $3, 1, 'adaptive-state/v1', $4, $5, 1, $6, 1)`,
				f.tenantA, f.userA, f.taskA, digest032(tc.payload), tc.payload,
				digestA); err == nil {
				t.Fatalf("%s adaptive payload 必须被 size CHECK 拒绝", tc.name)
			}
		})
	}
	if _, err := f.db.ExecContext(ctx, `
		INSERT INTO task_adaptive_states (
			tenant_id, user_id, task_id, version, schema_version,
			payload_digest, payload, basis_definition_version,
			basis_definition_digest, last_known_good_definition_version
		) VALUES ($1, $2, $3, 1, 'adaptive-state/v1', $4, $5, 1, $6, 1)`,
		f.tenantA, f.userA, f.taskA, adaptiveDigestA, adaptiveA, digestA); err != nil {
		t.Fatalf("插入 adaptive state 失败: %v", err)
	}
	if _, err := f.db.ExecContext(ctx, `
		UPDATE task_adaptive_states SET last_known_good_definition_version = 999
		 WHERE tenant_id = $1 AND user_id = $2 AND task_id = $3`,
		f.tenantA, f.userA, f.taskA); err == nil {
		t.Fatal("last-known-good 必须精确指向同 scope 的 approved version")
	}
	insertApproved032(t, f, f.tenantA, f.userA, f.taskA, 2,
		"compiled", "pending-action:032-historical-lkg", nonHeadPayload)
	if _, err := f.db.ExecContext(ctx, `
		UPDATE task_adaptive_states SET last_known_good_definition_version = 2
		 WHERE tenant_id = $1 AND user_id = $2 AND task_id = $3`,
		f.tenantA, f.userA, f.taskA); err == nil {
		t.Fatal("C2a 无等价证明时不得把历史 approved version 设为 LKG")
	}
	if _, err := f.db.ExecContext(ctx, `
		DELETE FROM task_approved_definition_versions
		 WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3 AND version=2`,
		f.tenantA, f.userA, f.taskA); err != nil {
		t.Fatal(err)
	}
	adaptiveA2 := []byte(`{"schema":"adaptive-state/v1","query_variants":["A2"]}`)
	tag, err := f.db.ExecContext(ctx, `
		UPDATE task_adaptive_states
		   SET version = 2, payload_digest = $4, payload = $5, updated_at = now()
		 WHERE tenant_id = $1 AND user_id = $2 AND task_id = $3 AND version = 1`,
		f.tenantA, f.userA, f.taskA, digest032(adaptiveA2), adaptiveA2)
	if err != nil {
		t.Fatalf("adaptive CAS advance 失败: %v", err)
	}
	if n, _ := tag.RowsAffected(); n != 1 {
		t.Fatalf("首次 CAS advance rows=%d, want 1", n)
	}
	tag, err = f.db.ExecContext(ctx, `
		UPDATE task_adaptive_states SET version = 3
		 WHERE tenant_id = $1 AND user_id = $2 AND task_id = $3 AND version = 1`,
		f.tenantA, f.userA, f.taskA)
	if err != nil {
		t.Fatalf("stale CAS 应返回零行而非技术错误: %v", err)
	}
	if n, _ := tag.RowsAffected(); n != 0 {
		t.Fatalf("stale CAS rows=%d, want 0", n)
	}

	payloadB := []byte(`{"schema":"approved-definition/v1","topic":"B"}`)
	digestB := insertApproved032(t, f, f.tenantB, f.userB, f.taskB, 1,
		"compiled", "pending-action:032-b", payloadB)
	adaptiveB := []byte(`{"schema":"adaptive-state/v1","query_variants":["B"]}`)
	if _, err := f.db.ExecContext(ctx, `
		INSERT INTO task_adaptive_states (
			tenant_id, user_id, task_id, version, schema_version,
			payload_digest, payload, basis_definition_version,
			basis_definition_digest, last_known_good_definition_version
		) VALUES ($1, $2, $3, 1, 'adaptive-state/v1', $4, $5, 1, $6, 1)`,
		f.tenantB, f.userB, f.taskB, digest032(adaptiveB), adaptiveB, digestB); err != nil {
		t.Fatal(err)
	}

	// Normal task deletion must still work despite the intentional two-way
	// schedule/head relation: the definition child cascades away and the parent
	// schedule disappears without leaving an orphan.
	defaultPayload := []byte(`{"schema":"approved-definition/v1","topic":"default"}`)
	defaultDigest := insertApproved032(t, f, f.tenantA, f.userA, taskDefault, 1,
		"compiled", "pending-action:default-delete", defaultPayload)
	if _, err := f.db.ExecContext(ctx, `
		UPDATE schedules
		   SET approved_definition_version=1, approved_definition_digest=$2
		 WHERE id=$1`, taskDefault, defaultDigest); err != nil {
		t.Fatal(err)
	}
	defaultAdaptive := []byte(`{"schema":"adaptive-state/v1","query_variants":[]}`)
	if _, err := f.db.ExecContext(ctx, `
		INSERT INTO task_adaptive_states (
			tenant_id, user_id, task_id, version, schema_version,
			payload_digest, payload, basis_definition_version,
			basis_definition_digest, last_known_good_definition_version
		) VALUES ($1, $2, $3, 1, 'adaptive-state/v1', $4, $5, 1, $6, 1)`,
		f.tenantA, f.userA, taskDefault, digest032(defaultAdaptive), defaultAdaptive,
		defaultDigest); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.ExecContext(ctx,
		`DELETE FROM schedules WHERE id=$1`, taskDefault); err != nil {
		t.Fatalf("删除带 approved head 的 task 失败: %v", err)
	}
	var orphanDefinitions, orphanAdaptive int
	if err := f.db.QueryRowContext(ctx, `
		SELECT
		  (SELECT count(*) FROM task_approved_definition_versions WHERE task_id=$1),
		  (SELECT count(*) FROM task_adaptive_states WHERE task_id=$1)`,
		taskDefault).Scan(&orphanDefinitions, &orphanAdaptive); err != nil {
		t.Fatal(err)
	}
	if orphanDefinitions != 0 || orphanAdaptive != 0 {
		t.Fatalf("删除 task 后遗留 approved/adaptive=%d/%d",
			orphanDefinitions, orphanAdaptive)
	}

	assertVisible := func(tenantID string, want int) {
		t.Helper()
		tx, err := f.db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback() }()
		if tenantID != "" {
			if _, err := tx.ExecContext(ctx,
				`SELECT set_config('app.tenant_id', $1, true)`, tenantID); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := tx.ExecContext(ctx, `SET LOCAL ROLE vane_app`); err != nil {
			t.Fatal(err)
		}
		var definitions, states int
		if err := tx.QueryRowContext(ctx, `
			SELECT (SELECT count(*) FROM task_approved_definition_versions),
			       (SELECT count(*) FROM task_adaptive_states)`,
		).Scan(&definitions, &states); err != nil {
			t.Fatal(err)
		}
		if definitions != want || states != want {
			t.Fatalf("RLS tenant=%q definitions/states=%d/%d want=%d/%d",
				tenantID, definitions, states, want, want)
		}
	}
	assertVisible("1", 1)
	assertVisible(stringInt64(f.tenantB), 1)
	assertVisible("", 0)

	// A valid foreign-scope insert would pass every FK/CHECK; RLS must be the
	// layer that rejects it for tenant A's application transaction.
	tx, err := f.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx,
		`SELECT set_config('app.tenant_id', '1', true)`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, `SET LOCAL ROLE vane_app`); err != nil {
		t.Fatal(err)
	}
	localPayload := []byte(`{"schema":"approved-definition/v1","topic":"A2"}`)
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO task_approved_definition_versions (
			tenant_id, user_id, task_id, version, schema_version,
			execution_mode, definition_digest, payload, approval_ref
		) VALUES ($1, $2, $3, 2, 'approved-definition/v1', 'compiled',
		          $4, $5, 'pending-action:local-write')`,
		f.tenantA, f.userA, f.taskA, digest032(localPayload), localPayload); err != nil {
		t.Fatalf("tenant A 应能写自己的 approved definition: %v", err)
	}
	localAdaptive := []byte(`{"schema":"adaptive-state/v1","query_variants":["A3"]}`)
	tag, err = tx.ExecContext(ctx, `
		UPDATE task_adaptive_states
		   SET version=3, payload_digest=$4, payload=$5, updated_at=now()
		 WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3 AND version=2`,
		f.tenantA, f.userA, f.taskA, digest032(localAdaptive), localAdaptive)
	if err != nil {
		t.Fatalf("tenant A 应能更新自己的 adaptive state: %v", err)
	}
	if n, _ := tag.RowsAffected(); n != 1 {
		t.Fatalf("tenant A 更新自己的 adaptive state rows=%d, want 1", n)
	}
	foreignPayload := []byte(`{"schema":"approved-definition/v1","topic":"B2"}`)
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO task_approved_definition_versions (
			tenant_id, user_id, task_id, version, schema_version,
			execution_mode, definition_digest, payload, approval_ref
		) VALUES ($1, $2, $3, 2, 'approved-definition/v1', 'compiled',
		          $4, $5, 'pending-action:foreign-write')`,
		f.tenantB, f.userB, f.taskB, digest032(foreignPayload), foreignPayload); err == nil {
		t.Fatal("tenant A 不得写入 tenant B 的 approved definition")
	}
}

func TestMigration032DowngradeGuardsAreAtomic(t *testing.T) {
	t.Run("untouched foundation can downgrade", func(t *testing.T) {
		f := migration032Scratch(t)
		if _, err := f.provider.Down(t.Context()); err != nil {
			t.Fatalf("空 C2a 地基应可回滚: %v", err)
		}
		var tables, columns int
		if err := f.db.QueryRowContext(t.Context(), `
			SELECT
			  (SELECT count(*) FROM information_schema.tables
			    WHERE table_schema='public' AND table_name IN
			          ('task_approved_definition_versions','task_adaptive_states')),
			  (SELECT count(*) FROM information_schema.columns
			    WHERE table_schema='public' AND table_name='schedules'
			      AND column_name IN
			          ('execution_mode','approved_definition_version','approved_definition_digest'))`,
		).Scan(&tables, &columns); err != nil {
			t.Fatal(err)
		}
		if tables != 0 || columns != 0 {
			t.Fatalf("032 Down 留下 schema: tables=%d columns=%d", tables, columns)
		}
	})

	f := migration032Scratch(t)
	ctx := t.Context()
	assertRefused := func(label string) {
		t.Helper()
		if _, err := f.provider.Down(ctx); err == nil ||
			!strings.Contains(err.Error(), "refusing downgrade") {
			t.Fatalf("%s 必须拒绝回滚: %v", label, err)
		}
		var version int
		var tables int
		if err := f.db.QueryRowContext(ctx, `
			SELECT
			  (SELECT max(version_id) FROM goose_db_version WHERE is_applied),
			  (SELECT count(*) FROM information_schema.tables
			    WHERE table_schema='public' AND table_name IN
			          ('task_approved_definition_versions','task_adaptive_states'))`,
		).Scan(&version, &tables); err != nil {
			t.Fatal(err)
		}
		if version != 32 || tables != 2 {
			t.Fatalf("%s 拒绝回滚后状态漂移: version=%d tables=%d", label, version, tables)
		}
	}

	dynamicPayload := []byte(`{"schema":"approved-definition/v1","mode":"discover"}`)
	dynamicDigest := insertApproved032(t, f, f.tenantA, f.userA, f.taskA, 1,
		"discover_at_run", "pending-action:032-dynamic", dynamicPayload)
	if _, err := f.db.ExecContext(ctx, `
		UPDATE schedules
		   SET execution_mode='discover_at_run', approved_definition_version=1,
		       approved_definition_digest=$2
		 WHERE id=$1`, f.taskA, dynamicDigest); err != nil {
		t.Fatal(err)
	}
	assertRefused("discover_at_run mode")
	if _, err := f.db.ExecContext(ctx,
		`DELETE FROM task_approved_definition_versions
		  WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3 AND version=1`,
		f.tenantA, f.userA, f.taskA); err != nil {
		t.Fatal(err)
	}
	var convergedMode string
	var convergedHeadless bool
	if err := f.db.QueryRowContext(ctx, `
		SELECT execution_mode,
		       approved_definition_version IS NULL AND approved_definition_digest IS NULL
		  FROM schedules WHERE id=$1`, f.taskA).Scan(&convergedMode, &convergedHeadless); err != nil {
		t.Fatal(err)
	}
	if convergedMode != "compiled" || !convergedHeadless {
		t.Fatalf("删除 dynamic current head 后应收敛 compiled/headless: mode=%q headless=%v",
			convergedMode, convergedHeadless)
	}

	adaptiveBasisPayload := []byte(`{"schema":"approved-definition/v1","for":"adaptive"}`)
	adaptiveBasisDigest := insertApproved032(t, f, f.tenantA, f.userA, f.taskA, 1,
		"compiled", "pending-action:032-adaptive-basis", adaptiveBasisPayload)
	adaptive := []byte(`{"schema":"adaptive-state/v1"}`)
	if _, err := f.db.ExecContext(ctx, `
		INSERT INTO task_adaptive_states (
			tenant_id, user_id, task_id, version, schema_version,
			payload_digest, payload, basis_definition_version, basis_definition_digest
		) VALUES ($1, $2, $3, 1, 'adaptive-state/v1', $4, $5, 1, $6)`,
		f.tenantA, f.userA, f.taskA, digest032(adaptive), adaptive,
		adaptiveBasisDigest); err != nil {
		t.Fatal(err)
	}
	assertRefused("adaptive state")
	if _, err := f.db.ExecContext(ctx,
		`DELETE FROM task_adaptive_states WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3`,
		f.tenantA, f.userA, f.taskA); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.ExecContext(ctx,
		`DELETE FROM task_approved_definition_versions
		  WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3`,
		f.tenantA, f.userA, f.taskA); err != nil {
		t.Fatal(err)
	}

	payload := []byte(`{"schema":"approved-definition/v1"}`)
	digest := insertApproved032(t, f, f.tenantA, f.userA, f.taskA, 1,
		"compiled", "pending-action:032-down", payload)
	if _, err := f.db.ExecContext(ctx, `
		UPDATE schedules
		   SET approved_definition_version=1, approved_definition_digest=$2
		 WHERE id=$1`, f.taskA, digest); err != nil {
		t.Fatal(err)
	}
	assertRefused("approved definition/head")

	// Owner-only tenant lifecycle deletion of the immutable row converges to the
	// only valid headless representation: compiled mode with a null head pair.
	if _, err := f.db.ExecContext(ctx, `
		DELETE FROM task_approved_definition_versions
		 WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3`,
		f.tenantA, f.userA, f.taskA); err != nil {
		t.Fatalf("owner purge of approved definition failed: %v", err)
	}
	var mode string
	var headless bool
	if err := f.db.QueryRowContext(ctx, `
		SELECT execution_mode,
		       approved_definition_version IS NULL AND approved_definition_digest IS NULL
		  FROM schedules WHERE id=$1`, f.taskA).Scan(&mode, &headless); err != nil {
		t.Fatal(err)
	}
	if mode != "compiled" || !headless {
		t.Fatalf("definition purge 应只清 head pair: mode=%q headless=%v", mode, headless)
	}
	if _, err := f.provider.Down(ctx); err != nil {
		t.Fatalf("清空状态并恢复 compiled 后应可回滚: %v", err)
	}
}

func TestMigration032DowngradeSerializesWithApprovedWriter(t *testing.T) {
	f := migration032Scratch(t)
	ctx := t.Context()

	writer, err := f.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	writerOpen := true
	downDone := make(chan error, 1)
	downFinished := false
	t.Cleanup(func() {
		if writerOpen {
			_ = writer.Rollback()
		}
		if downFinished {
			return
		}
		select {
		case <-downDone:
		case <-time.After(5 * time.Second):
			t.Error("032 Down did not finish after writer cleanup")
		}
	})

	// Match the production writer lock order: schedule first, immutable row,
	// then head update. Pausing here creates the exact old guard/DDL race.
	var lockedTask string
	if err := writer.QueryRowContext(ctx,
		`SELECT id FROM schedules WHERE id=$1 FOR UPDATE`, f.taskA,
	).Scan(&lockedTask); err != nil {
		t.Fatal(err)
	}

	go func() {
		_, downErr := f.provider.Down(ctx)
		downDone <- downErr
	}()

	deadline := time.Now().Add(5 * time.Second)
	for {
		var waiting bool
		if err := f.db.QueryRowContext(ctx, `
			SELECT EXISTS (
				SELECT 1
				  FROM pg_locks l
				  JOIN pg_class c ON c.oid = l.relation
				 WHERE c.relname = 'schedules'
				   AND l.mode = 'AccessExclusiveLock'
				   AND NOT l.granted
			)`,
		).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		select {
		case downErr := <-downDone:
			downFinished = true
			t.Fatalf("032 Down completed before serializing with writer: %v", downErr)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("032 Down never waited for the in-flight schedule-first writer")
		}
		time.Sleep(10 * time.Millisecond)
	}

	payload := []byte(`{"schema":"approved-definition/v1","race":"down"}`)
	digest := digest032(payload)
	if _, err := writer.ExecContext(ctx, `
		INSERT INTO task_approved_definition_versions (
			tenant_id, user_id, task_id, version, schema_version,
			execution_mode, definition_digest, payload, approval_ref
		) VALUES ($1, $2, $3, 1, 'approved-definition/v1', 'compiled',
		          $4, $5, 'pending-action:032-down-race')`,
		f.tenantA, f.userA, f.taskA, digest, payload); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.ExecContext(ctx, `
		UPDATE schedules
		   SET approved_definition_version=1, approved_definition_digest=$2
		 WHERE id=$1`, f.taskA, digest); err != nil {
		t.Fatal(err)
	}
	if err := writer.Commit(); err != nil {
		t.Fatal(err)
	}
	writerOpen = false

	select {
	case downErr := <-downDone:
		downFinished = true
		if downErr == nil || !strings.Contains(downErr.Error(), "refusing downgrade") {
			t.Fatalf("032 Down must observe and preserve the committed definition: %v", downErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("032 Down did not resume after writer commit")
	}

	var definitions, headVersion, migrationVersion int
	if err := f.db.QueryRowContext(ctx, `
		SELECT
		  (SELECT count(*) FROM task_approved_definition_versions
		    WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3),
		  (SELECT approved_definition_version FROM schedules WHERE id=$3),
		  (SELECT max(version_id) FROM goose_db_version WHERE is_applied)`,
		f.tenantA, f.userA, f.taskA,
	).Scan(&definitions, &headVersion, &migrationVersion); err != nil {
		t.Fatal(err)
	}
	if definitions != 1 || headVersion != 1 || migrationVersion != 32 {
		t.Fatalf("refused concurrent Down lost state: definitions=%d head=%d migration=%d",
			definitions, headVersion, migrationVersion)
	}
}

func stringInt64(v int64) string {
	return strconv.FormatInt(v, 10)
}
