package store

import (
	"encoding/hex"
	"errors"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestResearchToolStepAdmissionV1PostgresAttackMatrix(t *testing.T) {
	f := newResearchRunSpendFixtureV3(t, 1_000_000)
	foreign := newResearchRunSpendFixtureV3(t, 1_000_000)
	ctx := t.Context()

	beginExecutor := func(t *testing.T) pgx.Tx {
		t.Helper()
		tx, err := f.store.pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = tx.Rollback(ctx) })
		if _, err := tx.Exec(ctx, `SET LOCAL ROLE vane_research_v3_executor`); err != nil {
			t.Fatal(err)
		}
		return tx
	}
	beginScoped := func(t *testing.T) pgx.Tx {
		t.Helper()
		tx, ref, err := f.store.beginScopedResearchRunTransactionV3(
			ctx, pgx.TxOptions{}, f.identity, f.snapshotID)
		if err != nil {
			t.Fatal(err)
		}
		if ref != f.snapshotRef {
			_ = tx.Rollback(ctx)
			t.Fatalf("scoped ref=%+v want %+v", ref, f.snapshotRef)
		}
		t.Cleanup(func() { _ = tx.Rollback(ctx) })
		return tx
	}

	t.Run("legacy GUC and random bearer cannot admit", func(t *testing.T) {
		for name, bearer := range map[string]string{
			"legacy-only": "",
			"random":      strings.Repeat("ab", 32),
		} {
			t.Run(name, func(t *testing.T) {
				tx := beginExecutor(t)
				if _, err := tx.Exec(ctx,
					`SELECT set_config('app.tenant_id',$1,true),
					        set_config('app.user_id',$2,true),
					        set_config('app.research_run_capability_v1',$3,true)`,
					strconv.FormatInt(f.tenantID, 10), strconv.FormatInt(f.userID, 10), bearer,
				); err != nil {
					t.Fatal(err)
				}
				if _, err := tx.Exec(ctx,
					`SELECT * FROM admit_research_run_tool_step_cap_v1($1,$2,0)`,
					f.snapshotID, f.planRef.PlanID); err == nil {
					t.Fatal("untrusted GUCs admitted Tool spend")
				}
			})
		}
	})

	t.Run("capability cannot select foreign plan", func(t *testing.T) {
		tx := beginScoped(t)
		if _, err := tx.Exec(ctx,
			`SELECT * FROM admit_research_run_tool_step_cap_v1($1,$2,0)`,
			f.snapshotID, foreign.planRef.PlanID); err == nil {
			t.Fatal("capability selected another tenant plan")
		}
	})

	t.Run("ordinal and caller-selected Tool or cost fail closed", func(t *testing.T) {
		tx := beginScoped(t)
		if _, err := tx.Exec(ctx,
			`SELECT * FROM admit_research_run_tool_step_cap_v1($1,$2,15)`,
			f.snapshotID, f.planRef.PlanID); err == nil {
			t.Fatal("out-of-plan ordinal was admitted")
		}

		tx = beginScoped(t)
		if _, err := tx.Exec(ctx,
			`SELECT * FROM admit_research_run_tool_step_cap_v1(
			    $1,$2,0,'web_contents',1)`, f.snapshotID, f.planRef.PlanID); err == nil {
			t.Fatal("caller found a Tool/cost admission overload")
		}
	})

	t.Run("direct writes sequences and quota primitive are denied", func(t *testing.T) {
		tx := beginScoped(t)
		_, err := tx.Exec(ctx,
			`INSERT INTO research_run_steps (
			 tenant_id,user_id,task_id,plan_id,temporal_run_id,plan_digest,
			 step_ordinal,phase,invocation_id,tool_name,request_digest,
			 result_digest,cost_micro_usd,error_code,schema_version
			) VALUES ($1,$2,$3,$4,$5,$6,0,'started','invoke-0','web_search',$7,
			 NULL,0,NULL,'vane.research-run-step/v3')`,
			f.tenantID, f.userID, f.identity.TaskID, f.planRef.PlanID,
			f.identity.TemporalRunID, f.planRef.PlanDigest,
			digestResearchRunStepRequestV3(f.planRef.PlanDigest, 0))
		if err == nil {
			t.Fatal("executor directly inserted an exact-scope started step")
		}
		requirePGCode095(t, err, "42501")

		attacks := []string{
			`INSERT INTO research_run_step_spend_reservations (
			 tenant_id,user_id,task_id,run_snapshot_id,plan_id,started_step_id,
			 temporal_run_id,plan_digest,step_ordinal,invocation_id,tool_name,
			 request_digest,tool_policy_digest,quota_bucket,reserved_quota_units,
			 reserved_cost_micro_usd,schema_version
			) VALUES (1,1,'x',1,1,1,'x',repeat('a',64),0,'x','web_search',
			 repeat('b',64),repeat('c',64),'exa_calls',1,1,
			 'vane.research-run-step-spend-reservation/v1')`,
			`SELECT nextval('research_run_step_spend_reservations_id_seq')`,
			`SELECT reserve_research_run_quota_v3(1,'exa_calls',1)`,
		}
		for _, attack := range attacks {
			tx := beginScoped(t)
			_, err := tx.Exec(ctx, attack)
			if err == nil {
				t.Fatalf("executor retained direct admission authority: %s", attack)
			}
			requirePGCode095(t, err, "42501")
		}
	})

	t.Run("exact replay returns one claim and one debit", func(t *testing.T) {
		first, err := f.begin(t, 0)
		if err != nil || !first.FirstWriter {
			t.Fatalf("first admission=%+v err=%v", first, err)
		}
		replay, err := f.begin(t, 0)
		if err != nil || replay.FirstWriter || replay.StepID != first.StepID ||
			replay.SpendReservationID != first.SpendReservationID ||
			replay.ToolName != first.ToolName || replay.RequestDigest != first.RequestDigest {
			t.Fatalf("replay=%+v first=%+v err=%v", replay, first, err)
		}
		starts, reservations := f.spendCounts(t)
		if starts != 1 || reservations != 1 || f.quotaTokens(t) != 9 {
			t.Fatalf("replay mutated authority starts=%d reservations=%d quota=%v",
				starts, reservations, f.quotaTokens(t))
		}
	})
}

func TestResearchToolStepAdmissionV1ConcurrentFirstWriter(t *testing.T) {
	f := newResearchRunSpendFixtureV3(t, 1_000_000)
	type result struct {
		execution ResearchRunStepExecutionV3
		err       error
	}
	ready := make(chan struct{})
	results := make(chan result, 8)
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-ready
			execution, err := f.begin(t, 0)
			results <- result{execution: execution, err: err}
		}()
	}
	close(ready)
	wg.Wait()
	close(results)
	firstWriters := 0
	var stepID, reservationID int64
	for result := range results {
		if result.err != nil {
			t.Fatal(result.err)
		}
		if result.execution.FirstWriter {
			firstWriters++
		}
		if stepID == 0 {
			stepID, reservationID = result.execution.StepID,
				result.execution.SpendReservationID
		}
		if result.execution.StepID != stepID ||
			result.execution.SpendReservationID != reservationID {
			t.Fatalf("concurrent claim drifted: %+v", result.execution)
		}
	}
	starts, reservations := f.spendCounts(t)
	if firstWriters != 1 || starts != 1 || reservations != 1 || f.quotaTokens(t) != 9 {
		t.Fatalf("first=%d starts=%d reservations=%d quota=%v",
			firstWriters, starts, reservations, f.quotaTokens(t))
	}
}

func TestResearchToolStepAdmissionV1RetiredCapabilityKey(t *testing.T) {
	f := newResearchRunSpendFixtureV3(t, 1_000_000)
	if err := f.store.configureResearchRunCapabilityV1(ResearchRunCapabilityConfigV1{
		ActiveKeyID: "tool-admission-rotated", ActiveKeyHex: strings.Repeat("73", 32),
		RetiredKeys: "store-tests-active=" + strings.Repeat("42", 32),
	}); err != nil {
		t.Fatal(err)
	}
	execution, err := f.begin(t, 0)
	if err != nil || !execution.FirstWriter {
		t.Fatalf("retired-key admission=%+v err=%v", execution, err)
	}
	capability, err := f.store.resolveResearchRunCapabilityV1(t.Context(), f.snapshotRef)
	if err != nil || hex.EncodeToString(capability.raw[:]) == "" {
		t.Fatalf("retired capability unavailable: %v", err)
	}
}

func requirePGCode095(t *testing.T, err error, code string) {
	t.Helper()
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != code {
		t.Fatalf("postgres error=%v want code %s", err, code)
	}
}
