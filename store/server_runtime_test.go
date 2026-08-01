package store

import (
	"context"
	"database/sql"
	"io/fs"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/pressly/goose/v3"
)

func TestServerRuntimeBoundaryPostgres(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Skip("DATABASE_URL is required")
	}
	scratchURL, drop := createScratchDB(t.Context(), t, databaseURL)
	t.Cleanup(drop)
	owner, err := sql.Open("pgx", scratchURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.Close() })
	dir, err := fs.Sub(migrationsFS, "migrations")
	if err != nil {
		t.Fatal(err)
	}
	provider, err := goose.NewProvider(goose.DialectPostgres, owner, dir,
		goose.WithAllowOutofOrder(true))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := provider.UpTo(t.Context(), 98); err != nil {
		t.Fatal(err)
	}
	var roleCount int
	if err := owner.QueryRowContext(t.Context(), `SELECT count(*)
		FROM pg_catalog.pg_roles WHERE rolname='vane_server_runtime'`).Scan(
		&roleCount); err != nil {
		t.Fatal(err)
	}
	if roleCount != 0 {
		t.Fatal("migration 098 provisioned a cluster-global runtime implicitly")
	}
	if err := ProvisionServerRuntime(t.Context(), scratchURL); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := DeprovisionServerRuntime(ctx, scratchURL); err != nil {
			t.Errorf("deprovision server runtime test role: %v", err)
		}
	})

	const password = "server-runtime-test-password"
	if _, err := owner.ExecContext(t.Context(),
		`ALTER ROLE vane_server_runtime LOGIN PASSWORD '`+password+`'`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := cleanupContext()
		defer cancel()
		_, _ = owner.ExecContext(ctx,
			`ALTER ROLE vane_server_runtime NOLOGIN PASSWORD NULL`)
	})
	runtimeURL := serverRuntimeTestURL(t, scratchURL, password)
	const researchPassword = "research-runtime-test-password"
	if _, err := owner.ExecContext(t.Context(),
		`ALTER ROLE vane_research_runtime LOGIN PASSWORD '`+researchPassword+`'`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := cleanupContext()
		defer cancel()
		_, _ = owner.ExecContext(ctx,
			`ALTER ROLE vane_research_runtime NOLOGIN PASSWORD NULL`)
	})
	researchURL := roleTestURL(t, scratchURL, researchRuntimeLoginRole,
		researchPassword)

	t.Run("valid runtime supports default and explicit Store capabilities", func(t *testing.T) {
		st, err := NewServerRuntime(t.Context(), runtimeURL)
		if err != nil {
			t.Fatal(err)
		}
		defer st.Close()
		if err := st.Ping(t.Context()); err != nil {
			t.Fatal(err)
		}
		user, err := st.UpsertUserByOpenID(t.Context(),
			"ou-server-runtime-smoke", "runtime smoke")
		if err != nil || user.ID <= 0 {
			t.Fatalf("default vane_app Store smoke user=%+v err=%v", user, err)
		}
		tx, err := st.pool.Begin(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(t.Context()) }()
		var sessionUser, currentUser string
		if err := tx.QueryRow(t.Context(), `SELECT session_user,current_user`).Scan(
			&sessionUser, &currentUser); err != nil {
			t.Fatal(err)
		}
		if sessionUser != serverRuntimeLoginRole || currentUser != "vane_app" {
			t.Fatalf("runtime identity=%s/%s", sessionUser, currentUser)
		}
		if _, err := tx.Exec(t.Context(),
			`SET LOCAL ROLE vane_intelligence_reader`); err != nil {
			t.Fatalf("explicit allowlisted Store role: %v", err)
		}
	})

	t.Run("constructor keeps paid research on separate restricted pool", func(t *testing.T) {
		st, err := NewServerRuntimeWithResearchRuntimeCapability(
			t.Context(), runtimeURL, researchURL, ResearchRunCapabilityConfigV1{
				ActiveKeyID:  "server-runtime-constructor-smoke",
				ActiveKeyHex: strings.Repeat("51", 32),
			})
		if err != nil {
			t.Fatal(err)
		}
		defer st.Close()
		if st.researchPool == nil || st.gatewayPool != nil ||
			!st.researchCapabilityConfigured {
			t.Fatalf("constructor pools/capability are unsafe: research=%v gateway=%v capability=%v",
				st.researchPool != nil, st.gatewayPool != nil,
				st.researchCapabilityConfigured)
		}
		tx, err := st.beginResearchTransaction(t.Context(), pgx.TxOptions{})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(t.Context()) }()
		var sessionUser, currentUser string
		if err := tx.QueryRow(t.Context(), `SELECT session_user,current_user`).Scan(
			&sessionUser, &currentUser); err != nil {
			t.Fatal(err)
		}
		if sessionUser != researchRuntimeLoginRole || currentUser != researchRuntimeLoginRole {
			t.Fatalf("research identity=%s/%s", sessionUser, currentUser)
		}
	})

	t.Run("schema owner URL is rejected", func(t *testing.T) {
		if st, err := NewServerRuntime(t.Context(), scratchURL); err == nil {
			st.Close()
			t.Fatal("schema-owner URL entered the server runtime constructor")
		}
	})

	t.Run("deprovision refuses a live login", func(t *testing.T) {
		if err := DeprovisionServerRuntime(t.Context(), scratchURL); err == nil {
			t.Fatal("deprovision dropped a LOGIN runtime")
		} else if !strings.Contains(err.Error(), "can login") {
			t.Fatalf("unexpected live-login deprovision error: %v", err)
		}
	})

	t.Run("provision control functions are owner only", func(t *testing.T) {
		runtime, err := sql.Open("pgx", runtimeURL)
		if err != nil {
			t.Fatal(err)
		}
		defer runtime.Close()
		for _, function := range []string{
			"provision_vane_server_runtime_v1",
			"deprovision_vane_server_runtime_v1",
		} {
			if _, err := runtime.ExecContext(t.Context(),
				"SELECT public."+function+"()"); err == nil {
				t.Fatalf("runtime executed owner-only function %s", function)
			}
		}
	})

	t.Run("extra provider gateway membership is rejected", func(t *testing.T) {
		if _, err := owner.ExecContext(t.Context(),
			`GRANT vane_research_llm_gateway TO vane_server_runtime`); err != nil {
			t.Fatal(err)
		}
		defer func() {
			_, _ = owner.ExecContext(t.Context(),
				`REVOKE vane_research_llm_gateway FROM vane_server_runtime`)
		}()
		if st, err := NewServerRuntime(t.Context(), runtimeURL); err == nil {
			st.Close()
			t.Fatal("server runtime accepted provider gateway membership")
		} else if !strings.Contains(err.Error(), "memberships differ") &&
			!strings.Contains(err.Error(), "forbidden role") {
			t.Fatalf("unexpected gateway membership error: %v", err)
		}
	})

	t.Run("direct protected mutation grant is rejected", func(t *testing.T) {
		if _, err := owner.ExecContext(t.Context(),
			`GRANT INSERT ON research_llm_gateway_attempts TO vane_server_runtime`); err != nil {
			t.Fatal(err)
		}
		defer func() {
			_, _ = owner.ExecContext(t.Context(),
				`REVOKE INSERT ON research_llm_gateway_attempts FROM vane_server_runtime`)
		}()
		if st, err := NewServerRuntime(t.Context(), runtimeURL); err == nil {
			st.Close()
			t.Fatal("server runtime accepted protected direct mutation")
		} else if !strings.Contains(err.Error(), "protected data privileges") {
			t.Fatalf("unexpected protected mutation error: %v", err)
		}
	})

	t.Run("protected mutation drift on default app role is rejected", func(t *testing.T) {
		if _, err := owner.ExecContext(t.Context(),
			`GRANT INSERT ON research_llm_gateway_attempts TO vane_app`); err != nil {
			t.Fatal(err)
		}
		defer func() {
			_, _ = owner.ExecContext(t.Context(),
				`REVOKE INSERT ON research_llm_gateway_attempts FROM vane_app`)
		}()
		if st, err := NewServerRuntime(t.Context(), runtimeURL); err == nil {
			st.Close()
			t.Fatal("server runtime accepted protected vane_app mutation")
		} else if !strings.Contains(err.Error(), "vane_app has direct protected") {
			t.Fatalf("unexpected default-role drift error: %v", err)
		}
	})

	t.Run("gateway function grant is rejected", func(t *testing.T) {
		if _, err := owner.ExecContext(t.Context(),
			`GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO vane_server_runtime`); err != nil {
			t.Fatal(err)
		}
		defer func() {
			_, _ = owner.ExecContext(t.Context(),
				`REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA public FROM vane_server_runtime`)
		}()
		if st, err := NewServerRuntime(t.Context(), runtimeURL); err == nil {
			st.Close()
			t.Fatal("server runtime accepted provider gateway finalizer grant")
		} else if !strings.Contains(err.Error(), "provider gateway functions") {
			t.Fatalf("unexpected gateway function error: %v", err)
		}
	})

	t.Run("owned object is rejected", func(t *testing.T) {
		if _, err := owner.ExecContext(t.Context(),
			`CREATE SCHEMA server_runtime_drift AUTHORIZATION vane_server_runtime`); err != nil {
			t.Fatal(err)
		}
		defer func() {
			_, _ = owner.ExecContext(t.Context(), `DROP SCHEMA server_runtime_drift`)
		}()
		if st, err := NewServerRuntime(t.Context(), runtimeURL); err == nil {
			st.Close()
			t.Fatal("server runtime accepted owned database object")
		} else if !strings.Contains(err.Error(), "is unsafe") {
			t.Fatalf("unexpected ownership error: %v", err)
		}
	})
}

func TestSchemaMigrationsCoexistWithoutServerRuntimeProvisionPostgres(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Skip("DATABASE_URL is required")
	}
	admin, err := sql.Open("pgx", databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	var existing int
	if err := admin.QueryRowContext(t.Context(), `SELECT count(*)
		FROM pg_catalog.pg_roles WHERE rolname='vane_server_runtime'`).Scan(
		&existing); err != nil {
		t.Fatal(err)
	}
	if existing != 0 {
		t.Skip("cluster must be explicitly deprovisioned before fresh-database migration")
	}

	for _, name := range []string{"first schema", "second schema"} {
		t.Run(name, func(t *testing.T) {
			scratchURL, drop := createScratchDB(t.Context(), t, databaseURL)
			defer drop()
			if err := Migrate(t.Context(), scratchURL); err != nil {
				t.Fatal(err)
			}
			var count int
			if err := admin.QueryRowContext(t.Context(), `SELECT count(*)
				FROM pg_catalog.pg_roles WHERE rolname='vane_server_runtime'`).Scan(
				&count); err != nil {
				t.Fatal(err)
			}
			if count != 0 {
				t.Fatal("ordinary schema migration leaked server runtime into cluster")
			}
		})
	}
}

func serverRuntimeTestURL(t *testing.T, databaseURL, password string) string {
	return roleTestURL(t, databaseURL, serverRuntimeLoginRole, password)
}

func roleTestURL(t *testing.T, databaseURL, role, password string) string {
	t.Helper()
	parsed, err := url.Parse(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	parsed.User = url.UserPassword(role, password)
	return parsed.String()
}
