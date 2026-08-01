package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strconv"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestResearchRunCapabilityV1PostgresAttackMatrix(t *testing.T) {
	fixtureA := newResearchRunSpendFixtureWithToolBudgetV3(t, 20_000, 2, false)
	fixtureB := newResearchRunSpendFixtureWithToolBudgetV3(t, 20_000, 2, false)
	st := fixtureA.store
	ctx := t.Context()

	capA, err := st.resolveResearchRunCapabilityV1(ctx, fixtureA.snapshotRef)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.resolveResearchRunCapabilityV1(ctx, fixtureA.snapshotRef); err != nil {
		t.Fatalf("response-loss replay did not recover capability: %v", err)
	}
	var registrations int
	if err := st.pool.QueryRow(ctx,
		`SELECT count(*) FROM research_run_capabilities WHERE run_snapshot_id=$1`,
		fixtureA.snapshotID).Scan(&registrations); err != nil || registrations != 1 {
		t.Fatalf("capability replay registrations=%d err=%v", registrations, err)
	}
	var lifetimeHours float64
	if err := st.pool.QueryRow(ctx,
		`SELECT EXTRACT(EPOCH FROM(capability.not_after-snapshot.created_at))/3600
		   FROM research_run_capabilities capability
		   JOIN task_run_snapshots snapshot ON snapshot.id=capability.run_snapshot_id
		  WHERE capability.run_snapshot_id=$1`, fixtureA.snapshotID,
	).Scan(&lifetimeHours); err != nil || lifetimeHours != 90*24 {
		t.Fatalf("capability lifetime hours=%v err=%v", lifetimeHours, err)
	}

	beginExecutor := func() pgx.Tx {
		t.Helper()
		tx, err := st.pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = tx.Rollback(ctx) })
		if _, err := tx.Exec(ctx, `SET LOCAL ROLE vane_research_v3_executor`); err != nil {
			t.Fatal(err)
		}
		return tx
	}
	setLegacyScope := func(tx pgx.Tx, tenantID, userID int64) {
		t.Helper()
		if _, err := tx.Exec(ctx,
			`SELECT set_config('app.tenant_id',$1,true),
			        set_config('app.user_id',$2,true)`,
			strconv.FormatInt(tenantID, 10), strconv.FormatInt(userID, 10)); err != nil {
			t.Fatal(err)
		}
	}
	countSnapshots := func(tx pgx.Tx) int {
		t.Helper()
		var count int
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM task_run_snapshots`).Scan(&count); err != nil {
			t.Fatal(err)
		}
		return count
	}

	t.Run("legacy GUCs cannot self-authorize", func(t *testing.T) {
		tx := beginExecutor()
		setLegacyScope(tx, fixtureA.tenantID, fixtureA.userID)
		if count := countSnapshots(tx); count != 0 {
			t.Fatalf("caller-selected GUC exposed %d snapshots", count)
		}
	})

	t.Run("random bearer is denied", func(t *testing.T) {
		tx := beginExecutor()
		setLegacyScope(tx, fixtureA.tenantID, fixtureA.userID)
		if _, err := tx.Exec(ctx,
			`SELECT set_config('app.research_run_capability_v1',$1,true)`,
			strings.Repeat("ab", sha256.Size)); err != nil {
			t.Fatal(err)
		}
		if count := countSnapshots(tx); count != 0 {
			t.Fatalf("random bearer exposed %d snapshots", count)
		}
	})

	t.Run("exact bearer sees one exact run", func(t *testing.T) {
		tx := beginExecutor()
		setLegacyScope(tx, fixtureA.tenantID, fixtureA.userID)
		if _, err := tx.Exec(ctx,
			`SELECT set_config('app.research_run_capability_v1',$1,true)`,
			hex.EncodeToString(capA.raw[:])); err != nil {
			t.Fatal(err)
		}
		if count := countSnapshots(tx); count != 1 {
			t.Fatalf("exact bearer saw %d snapshots, want 1", count)
		}
		var snapshotID int64
		if err := tx.QueryRow(ctx, `SELECT id FROM task_run_snapshots`).Scan(&snapshotID); err != nil ||
			snapshotID != fixtureA.snapshotID {
			t.Fatalf("exact bearer snapshot=%d err=%v", snapshotID, err)
		}
	})

	t.Run("bearer cannot cross tenant through rewritten GUCs", func(t *testing.T) {
		tx := beginExecutor()
		setLegacyScope(tx, fixtureB.tenantID, fixtureB.userID)
		if _, err := tx.Exec(ctx,
			`SELECT set_config('app.research_run_capability_v1',$1,true)`,
			hex.EncodeToString(capA.raw[:])); err != nil {
			t.Fatal(err)
		}
		if count := countSnapshots(tx); count != 0 {
			t.Fatalf("tenant rewrite exposed %d snapshots", count)
		}
	})

	t.Run("capability registry and hash are unreadable", func(t *testing.T) {
		tx := beginExecutor()
		if _, err := tx.Exec(ctx, `SELECT capability_hash FROM research_run_capabilities`); err == nil {
			t.Fatal("executor read capability hash registry")
		} else {
			var pgErr *pgconn.PgError
			if !errors.As(err, &pgErr) || pgErr.Code != "42501" {
				t.Fatalf("registry read returned %v", err)
			}
		}
	})

	t.Run("standalone Tool quota drain is unreachable", func(t *testing.T) {
		tx := beginExecutor()
		setLegacyScope(tx, fixtureA.tenantID, fixtureA.userID)
		if _, err := tx.Exec(ctx,
			`SELECT set_config('app.research_run_capability_v1',$1,true)`,
			hex.EncodeToString(capA.raw[:])); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx,
			`SELECT reserve_research_run_quota_v3($1,'exa_calls',1)`,
			fixtureA.tenantID); err == nil {
			t.Fatal("executor retained repeatable standalone quota debit")
		}
	})
}

func TestResearchRunCapabilityV1KeyRotation(t *testing.T) {
	fixture := newResearchRunSpendFixtureWithToolBudgetV3(t, 20_000, 2, false)
	st := fixture.store
	ctx := t.Context()
	original, err := st.resolveResearchRunCapabilityV1(ctx, fixture.snapshotRef)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.configureResearchRunCapabilityV1(ResearchRunCapabilityConfigV1{
		ActiveKeyID: "rotated-active", ActiveKeyHex: strings.Repeat("73", 32),
		RetiredKeys: "store-tests-active=" + strings.Repeat("42", 32),
	}); err != nil {
		t.Fatal(err)
	}
	recovered, err := st.resolveResearchRunCapabilityV1(ctx, fixture.snapshotRef)
	if err != nil {
		t.Fatalf("retained key could not recover old run: %v", err)
	}
	if original.raw != recovered.raw {
		t.Fatal("rotation changed existing run bearer")
	}
	if err := st.configureResearchRunCapabilityV1(ResearchRunCapabilityConfigV1{
		ActiveKeyID: "rotated-active", ActiveKeyHex: strings.Repeat("73", 32),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.resolveResearchRunCapabilityV1(ctx, fixture.snapshotRef); err == nil {
		t.Fatal("dropping a still-required retired key did not fail closed")
	}
}

// setResearchRunScopeV3 is retained only for legacy attack tests. It proves
// caller-selected GUCs no longer authorize rows after 093; production has no
// equivalent helper.
func setResearchRunScopeV3(
	ctx context.Context, tx pgx.Tx, tenantID, userID int64,
) error {
	if _, err := tx.Exec(ctx,
		`SELECT set_config('app.tenant_id',$1,true),
		        set_config('app.user_id',$2,true)`,
		strconv.FormatInt(tenantID, 10), strconv.FormatInt(userID, 10)); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `SET LOCAL ROLE vane_research_v3_executor`)
	return err
}
