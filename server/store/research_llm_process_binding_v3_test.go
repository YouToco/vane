package store

import (
	"bytes"
	"context"
	"encoding"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/YouToco/vane/types"
)

func newResearchProcessBindingFixtureV1(t *testing.T, prompt string) (
	researchRunSpendFixtureV3, ResearchRunLLMSpendReservationV3,
) {
	t.Helper()
	if os.Getenv("DATABASE_URL") == "" {
		t.Skip("DATABASE_URL required")
	}
	seed := tenantTestStore(t)
	ensureResearchLLMPriceV3(t, seed)
	f := newResearchRunSpendFixtureWithToolBudgetV3(t, 1_000_000, 16, false)
	useOwnerResearchRuntimeForTest(f.store)
	reservation, err := f.store.BeginResearchRunLLMSpendV3(t.Context(),
		researchPlannerBeginV3(f, 1, prompt))
	if err != nil {
		t.Fatal(err)
	}
	return f, reservation
}

func newResearchProcessBindingServerRuntimeV1(t *testing.T) *Store {
	t.Helper()
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Skip("DATABASE_URL required")
	}
	scratchURL, drop := createScratchDB(t.Context(), t, databaseURL)
	t.Cleanup(drop)
	t.Setenv("DATABASE_URL", scratchURL)
	if err := Migrate(t.Context(), scratchURL); err != nil {
		t.Fatal(err)
	}
	if err := ProvisionServerRuntime(t.Context(), scratchURL); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := DeprovisionServerRuntime(ctx, scratchURL); err != nil {
			t.Errorf("deprovision server runtime: %v", err)
		}
	})
	owner := tenantTestStore(t)
	const runtimePassword = "research-binding-server-runtime"
	const researchPassword = "research-binding-executor-runtime"
	if _, err := owner.pool.Exec(t.Context(),
		`ALTER ROLE vane_server_runtime LOGIN PASSWORD '`+runtimePassword+`'`); err != nil {
		t.Fatal(err)
	}
	if _, err := owner.pool.Exec(t.Context(),
		`ALTER ROLE vane_research_runtime LOGIN PASSWORD '`+researchPassword+`'`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := cleanupContext()
		defer cancel()
		_, _ = owner.pool.Exec(ctx, `ALTER ROLE vane_server_runtime NOLOGIN PASSWORD NULL`)
		_, _ = owner.pool.Exec(ctx, `ALTER ROLE vane_research_runtime NOLOGIN PASSWORD NULL`)
	})
	runtime, err := NewServerRuntimeWithResearchRuntimeCapability(
		t.Context(), serverRuntimeTestURL(t, scratchURL, runtimePassword),
		roleTestURL(t, scratchURL, researchRuntimeLoginRole, researchPassword),
		ResearchRunCapabilityConfigV1{
			ActiveKeyID: "store-tests-active", ActiveKeyHex: strings.Repeat("42", 32),
			RetiredKeys: "store-tests-retired=" + strings.Repeat("24", 32),
		})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(runtime.Close)
	return runtime
}

func TestResearchLLMProcessGatewayBindingV1ReplayAndRedactionPostgres(t *testing.T) {
	runtime := newResearchProcessBindingServerRuntimeV1(t)
	f, reservation := newResearchProcessBindingFixtureV1(t,
		"process binding replay and redaction")
	var controlSession, controlRole, researchSession, researchRole string
	if err := runtime.pool.QueryRow(t.Context(), `SELECT session_user,current_user`).Scan(
		&controlSession, &controlRole,
	); err != nil {
		t.Fatal(err)
	}
	if err := runtime.researchPool.QueryRow(t.Context(), `SELECT session_user,current_user`).Scan(
		&researchSession, &researchRole,
	); err != nil {
		t.Fatal(err)
	}
	if controlSession != serverRuntimeLoginRole || controlRole != "vane_app" ||
		researchSession != researchRuntimeLoginRole ||
		researchRole != researchRuntimeLoginRole {
		t.Fatalf("unsafe split runtime identities: control=%s/%s research=%s/%s",
			controlSession, controlRole, researchSession, researchRole)
	}
	if _, err := f.store.ResolveResearchLLMProcessGatewayBindingV1(
		t.Context(), f.identity, f.snapshotRef, reservation.ReservationID,
	); err == nil || !strings.Contains(err.Error(), "invalid process gateway binding scope") {
		t.Fatalf("schema owner unexpectedly substituted for research control: %v", err)
	}
	var denied int64
	if err := runtime.researchPool.QueryRow(t.Context(),
		`SELECT out_reservation_id
		   FROM bind_research_llm_process_gateway_v1($1,$2,$3,$4,$5,$6,$7,$8)`,
		f.identity.TenantID, f.identity.UserID, f.identity.TaskID,
		f.identity.TemporalWorkflowID, f.identity.TemporalRunID,
		f.snapshotRef.SnapshotID, f.snapshotRef.ReferenceDigest,
		reservation.ReservationID,
	).Scan(&denied); err == nil {
		t.Fatal("research executor unexpectedly substituted for research control")
	}
	first, err := runtime.ResolveResearchLLMProcessGatewayBindingV1(
		t.Context(), f.identity, f.snapshotRef, reservation.ReservationID)
	if err != nil {
		t.Fatal(err)
	}
	second, err := runtime.ResolveResearchLLMProcessGatewayBindingV1(
		t.Context(), f.identity, f.snapshotRef, reservation.ReservationID)
	if err != nil {
		t.Fatal(err)
	}
	firstID, firstDigest, firstCapability, err := first.OpenForProcessGatewayV1()
	if err != nil {
		t.Fatal(err)
	}
	secondID, secondDigest, secondCapability, err := second.OpenForProcessGatewayV1()
	if err != nil {
		t.Fatal(err)
	}
	if firstID != reservation.ReservationID || firstDigest != reservation.RequestDigest ||
		secondID != firstID || secondDigest != firstDigest ||
		secondCapability != firstCapability || len(firstCapability) != 64 {
		t.Fatalf("binding replay differs: first=(%d,%s,%d) second=(%d,%s,%d)",
			firstID, firstDigest, len(firstCapability), secondID, secondDigest,
			len(secondCapability))
	}
	for name, rendered := range map[string]string{
		"string": fmt.Sprint(first), "go_string": fmt.Sprintf("%#v", first),
	} {
		if rendered != "<redacted>" || strings.Contains(rendered, firstCapability) {
			t.Fatalf("%s leaked binding: %q", name, rendered)
		}
	}
	if _, err := json.Marshal(first); err == nil {
		t.Fatal("JSON serialization unexpectedly accepted process gateway binding")
	}
	if _, ok := any(first).(encoding.TextMarshaler); !ok {
		t.Fatal("binding must explicitly refuse text serialization")
	} else if _, err := first.MarshalText(); err == nil {
		t.Fatal("text serialization unexpectedly accepted process gateway binding")
	}
	var log bytes.Buffer
	slog.New(slog.NewTextHandler(&log, nil)).Info("binding", "binding", first)
	if strings.Contains(log.String(), firstCapability) ||
		!strings.Contains(log.String(), "<redacted>") {
		t.Fatalf("slog did not redact binding: %q", log.String())
	}

	mutatedID := first
	mutatedID.ReservationID++
	if _, _, _, err := mutatedID.OpenForProcessGatewayV1(); err == nil {
		t.Fatal("mutated reservation id opened")
	}
	mutatedDigest := first
	mutatedDigest.RequestDigest = strings.Repeat("a", 64)
	if _, _, _, err := mutatedDigest.OpenForProcessGatewayV1(); err == nil {
		t.Fatal("mutated request digest opened")
	}
	mutatedCapability := first
	mutatedCapability.runCapability[0] ^= 0xff
	if _, _, _, err := mutatedCapability.OpenForProcessGatewayV1(); err == nil {
		t.Fatal("mutated run capability opened")
	}
	for _, relation := range []string{
		"research_run_capabilities", "research_llm_gateway_frozen_requests",
	} {
		if _, err := runtime.pool.Exec(t.Context(), "SELECT * FROM "+relation+" LIMIT 1"); err == nil {
			t.Fatalf("non-owner server runtime directly read %s", relation)
		}
	}
}

func TestResolveResearchLLMProcessGatewayBindingV1RejectsCrossScopePostgres(t *testing.T) {
	runtime := newResearchProcessBindingServerRuntimeV1(t)
	left, leftReservation := newResearchProcessBindingFixtureV1(t, "left binding")
	right, rightReservation := newResearchProcessBindingFixtureV1(t, "right binding")

	cases := []struct {
		name        string
		identity    types.RunIdentity
		snapshot    types.ResearchRunSnapshotRefV3
		reservation int64
	}{
		{"cross tenant user task snapshot", right.identity, right.snapshotRef,
			leftReservation.ReservationID},
		{"cross reservation", left.identity, left.snapshotRef,
			rightReservation.ReservationID},
		{"unknown reservation", left.identity, left.snapshotRef,
			leftReservation.ReservationID + rightReservation.ReservationID + 1000},
	}
	for _, field := range []string{"tenant", "user", "task"} {
		identity := left.identity
		switch field {
		case "tenant":
			identity.TenantID = right.identity.TenantID
		case "user":
			identity.UserID = right.identity.UserID
		case "task":
			identity.TaskID = right.identity.TaskID
		}
		cases = append(cases, struct {
			name        string
			identity    types.RunIdentity
			snapshot    types.ResearchRunSnapshotRefV3
			reservation int64
		}{"forged " + field, identity, left.snapshotRef, leftReservation.ReservationID})
	}
	for _, mutation := range []struct {
		name  string
		apply func(*types.ResearchRunSnapshotRefV3)
	}{
		{"snapshot id", func(ref *types.ResearchRunSnapshotRefV3) { ref.SnapshotID++ }},
		{"snapshot digest", func(ref *types.ResearchRunSnapshotRefV3) {
			ref.ReferenceDigest = strings.Repeat("a", 64)
		}},
	} {
		ref := left.snapshotRef
		mutation.apply(&ref)
		cases = append(cases, struct {
			name        string
			identity    types.RunIdentity
			snapshot    types.ResearchRunSnapshotRefV3
			reservation int64
		}{"forged " + mutation.name, left.identity, ref, leftReservation.ReservationID})
	}

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			if _, err := runtime.ResolveResearchLLMProcessGatewayBindingV1(
				t.Context(), test.identity, test.snapshot, test.reservation,
			); err == nil {
				t.Fatal("cross-scope binding unexpectedly resolved")
			}
		})
	}
}

func TestResolveResearchLLMProcessGatewayBindingV1RejectsRevokedExpiredAndForgedPostgres(t *testing.T) {
	runtime := newResearchProcessBindingServerRuntimeV1(t)
	// Bind negative fixtures to stored/fixed times so a virtualized CI clock
	// stepping backwards cannot make otherwise-valid mutations violate the
	// capability lifetime constraint before the resolver is exercised.
	t.Run("revoked", func(t *testing.T) {
		f, reservation := newResearchProcessBindingFixtureV1(t, "revoked binding")
		if _, err := f.store.pool.Exec(t.Context(),
			`UPDATE research_run_capabilities SET revoked_at=issued_at
			  WHERE run_snapshot_id=$1`, f.snapshotID); err != nil {
			t.Fatal(err)
		}
		if _, err := runtime.ResolveResearchLLMProcessGatewayBindingV1(
			t.Context(), f.identity, f.snapshotRef, reservation.ReservationID,
		); err == nil {
			t.Fatal("revoked capability resolved")
		}
	})

	t.Run("expired", func(t *testing.T) {
		f, reservation := newResearchProcessBindingFixtureV1(t, "expired binding")
		if _, err := f.store.pool.Exec(t.Context(),
			`UPDATE research_run_capabilities
			    SET issued_at=TIMESTAMPTZ '2000-01-01 00:00:00+00',
			        not_after=TIMESTAMPTZ '2000-01-02 00:00:00+00'
			  WHERE run_snapshot_id=$1`, f.snapshotID); err != nil {
			t.Fatal(err)
		}
		if _, err := runtime.ResolveResearchLLMProcessGatewayBindingV1(
			t.Context(), f.identity, f.snapshotRef, reservation.ReservationID,
		); err == nil {
			t.Fatal("expired capability resolved")
		}
	})

	t.Run("forged hash", func(t *testing.T) {
		f, reservation := newResearchProcessBindingFixtureV1(t, "forged binding")
		if _, err := f.store.pool.Exec(t.Context(),
			`UPDATE research_run_capabilities SET capability_hash=$1
			  WHERE run_snapshot_id=$2`, bytes.Repeat([]byte{0x5a}, 32), f.snapshotID); err != nil {
			t.Fatal(err)
		}
		if _, err := runtime.ResolveResearchLLMProcessGatewayBindingV1(
			t.Context(), f.identity, f.snapshotRef, reservation.ReservationID,
		); err == nil {
			t.Fatal("forged capability hash resolved")
		}
	})
}
