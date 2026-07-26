package store

import (
	"context"
	"strings"
	"testing"
)

const scheduleCommanderRole = "vane_schedule_commander"

func TestMigration054RestrictedRoleAndTenantIsolation(t *testing.T) {
	db, provider := migration035Scratch(t)
	ctx := t.Context()
	if _, err := provider.UpTo(ctx, 54); err != nil {
		t.Fatalf("migrate to 054: %v", err)
	}
	var (
		noLogin, noInherit, noBypass, ownerCanSet, ownsTable bool
		commandSelect, commandInsert, commandUpdate          bool
		commandDelete, scheduleStatusUpdate, scheduleDelete  bool
		scheduleDescriptionUpdate, appSelect, appInsert      bool
		appUpdate, appDelete                                 bool
	)
	if err := db.QueryRowContext(ctx, `
		SELECT
		  NOT rolcanlogin, NOT rolinherit, NOT rolbypassrls,
		  pg_has_role(current_user, r.oid, 'SET'),
		  c.relowner=r.oid,
		  has_table_privilege($1,'schedule_commands','SELECT'),
		  has_column_privilege(
		      $1,'schedule_commands','id','INSERT'
		  ),
		  has_column_privilege(
		      $1,'schedule_commands','status','UPDATE'
		  ),
		  has_table_privilege($1,'schedule_commands','DELETE'),
		  has_column_privilege($1,'schedules','status','UPDATE'),
		  has_table_privilege($1,'schedules','DELETE'),
		  has_column_privilege(
		      $1,'schedules','nl_description','UPDATE'
		  ),
		  has_table_privilege('vane_app','schedule_commands','SELECT'),
		  has_table_privilege('vane_app','schedule_commands','INSERT'),
		  has_table_privilege('vane_app','schedule_commands','UPDATE'),
		  has_table_privilege('vane_app','schedule_commands','DELETE')
		  FROM pg_roles r
		  JOIN pg_class c ON c.relname='schedule_commands'
		 WHERE r.rolname=$1`,
		scheduleCommanderRole,
	).Scan(
		&noLogin, &noInherit, &noBypass, &ownerCanSet, &ownsTable,
		&commandSelect, &commandInsert, &commandUpdate, &commandDelete,
		&scheduleStatusUpdate, &scheduleDelete, &scheduleDescriptionUpdate,
		&appSelect, &appInsert, &appUpdate, &appDelete,
	); err != nil {
		t.Fatal(err)
	}
	if !noLogin || !noInherit || !noBypass || !ownerCanSet || ownsTable ||
		!commandSelect || !commandInsert || !commandUpdate || commandDelete ||
		!scheduleStatusUpdate || !scheduleDelete ||
		scheduleDescriptionUpdate || appSelect || appInsert || appUpdate ||
		appDelete {
		t.Fatalf(
			"unsafe role matrix: nologin=%t noinherit=%t nobypass=%t set=%t "+
				"owner=%t command=%t/%t/%t/%t schedule=%t/%t desc=%t "+
				"app=%t/%t/%t/%t",
			noLogin, noInherit, noBypass, ownerCanSet, ownsTable,
			commandSelect, commandInsert, commandUpdate, commandDelete,
			scheduleStatusUpdate, scheduleDelete, scheduleDescriptionUpdate,
			appSelect, appInsert, appUpdate, appDelete,
		)
	}

	if _, err := db.ExecContext(ctx, `
		INSERT INTO tenants DEFAULT VALUES;
		INSERT INTO users (feishu_open_id,name)
		VALUES ('migration-054-a','a'),('migration-054-b','b');
		INSERT INTO schedule_commands (
		    id,tenant_id,user_id,task_id,idempotency_key,kind,
		    payload_digest,remote_request_id
		)
		SELECT
		    (
		        '00000000-0000-0000-0000-' ||
		        lpad(id::text,12,'0')
		    )::uuid,
		    id,
		    (
		        SELECT u.id FROM users u
		         WHERE u.feishu_open_id =
		           CASE WHEN tenants.id=1
		                THEN 'migration-054-a'
		                ELSE 'migration-054-b' END
		    ),
		    'task-' || id::text, 'key-' || id::text, 'run',
		    repeat('a',64), repeat('b',64)
		  FROM tenants WHERE id IN (1,2)`); err != nil {
		t.Fatal(err)
	}
	for tenantID, want := range map[string]int{"1": 1, "2": 1, "": 0} {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tx.ExecContext(
			ctx, `SELECT set_config('app.tenant_id',$1,true)`, tenantID,
		); err != nil {
			_ = tx.Rollback()
			t.Fatal(err)
		}
		if _, err := tx.ExecContext(
			ctx, `SET LOCAL ROLE vane_schedule_commander`,
		); err != nil {
			_ = tx.Rollback()
			t.Fatal(err)
		}
		var got int
		if err := tx.QueryRowContext(
			ctx, `SELECT count(*) FROM schedule_commands`,
		).Scan(&got); err != nil {
			_ = tx.Rollback()
			t.Fatal(err)
		}
		if err := tx.Rollback(); err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("tenant context %q saw %d commands, want %d",
				tenantID, got, want)
		}
	}
}

func TestMigration054RejectsUnsafePreexistingMembership(t *testing.T) {
	db, provider := migration035Scratch(t)
	ctx := t.Context()
	if _, err := db.ExecContext(ctx, `
		DO $$
		BEGIN
		  IF NOT EXISTS (
		    SELECT 1 FROM pg_roles
		     WHERE rolname='vane_schedule_commander'
		  ) THEN
		    CREATE ROLE vane_schedule_commander NOLOGIN;
		  END IF;
		  IF NOT EXISTS (
		    SELECT 1 FROM pg_roles
		     WHERE rolname='vane_schedule_commander_unsafe'
		  ) THEN
		    CREATE ROLE vane_schedule_commander_unsafe NOLOGIN;
		  END IF;
		END $$;
		GRANT vane_schedule_commander
		  TO vane_schedule_commander_unsafe`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := db.ExecContext(context.WithoutCancel(ctx), `
			REVOKE vane_schedule_commander
			  FROM vane_schedule_commander_unsafe;
			DROP ROLE IF EXISTS vane_schedule_commander_unsafe`,
		); err != nil {
			t.Errorf("cleanup unsafe role: %v", err)
		}
	})
	if _, err := provider.UpTo(ctx, 54); err == nil ||
		!strings.Contains(err.Error(), "only migration owner") {
		t.Fatalf("054 accepted unsafe pre-existing membership: %v", err)
	}
}

func TestMigration054DowngradeFence(t *testing.T) {
	t.Run("empty can downgrade", func(t *testing.T) {
		db, provider := migration035Scratch(t)
		if _, err := provider.UpTo(t.Context(), 54); err != nil {
			t.Fatal(err)
		}
		if _, err := provider.Down(t.Context()); err != nil {
			t.Fatalf("empty 054 down: %v", err)
		}
		var exists bool
		if err := db.QueryRowContext(t.Context(),
			`SELECT to_regclass('public.schedule_commands') IS NOT NULL`,
		).Scan(&exists); err != nil {
			t.Fatal(err)
		}
		if exists {
			t.Fatal("schedule_commands survived 054 down")
		}
	})

	t.Run("audit refuses downgrade", func(t *testing.T) {
		db, provider := migration035Scratch(t)
		ctx := t.Context()
		if _, err := provider.UpTo(ctx, 54); err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(ctx, `
			INSERT INTO users (feishu_open_id,name)
			VALUES ('migration-054-down-user','down');
			INSERT INTO schedule_commands (
			    id,tenant_id,user_id,task_id,idempotency_key,kind,
			    payload_digest,remote_request_id
			) VALUES (
			    '00000000-0000-0000-0000-000000000054',
			    1,(SELECT max(id) FROM users),'task-down','key-down','run',
			    repeat('a',64),repeat('b',64)
			)`); err != nil {
			t.Fatal(err)
		}
		if _, err := provider.Down(ctx); err == nil ||
			!strings.Contains(err.Error(), "refusing downgrade") {
			t.Fatalf("054 down accepted retained audit: %v", err)
		}
	})
}
