package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/YouToco/vane/server/internal/testgate"
	"github.com/YouToco/vane/server/store"
	"github.com/YouToco/vane/server/types"
)

func TestWorkspaceMemoryUATHelpersRejectFalsePositives(t *testing.T) {
	team := "team-token"
	personal := "personal-token"
	if recallContains(nil, team) || recallContainsExactly(nil, team) {
		t.Fatal("nil recall matched")
	}
	teamResult := &types.MemoryRecallResult{Memories: []types.MemoryRecallItem{
		{Memory: types.MemoryRecord{Text: team}},
	}}
	if !recallContains(teamResult, team) || !recallContainsExactly(teamResult, team) ||
		recallContains(teamResult, personal) {
		t.Fatal("exact recall helper drifted")
	}
	teamResult.Memories = append(teamResult.Memories,
		types.MemoryRecallItem{Memory: types.MemoryRecord{Text: personal}})
	if recallContainsExactly(teamResult, team) {
		t.Fatal("multi-result recall passed exact oracle")
	}
}

type fakeWorkspaceMemoryRuntime struct {
	calls        int
	failAt       int
	inconsistent bool
}

func (f *fakeWorkspaceMemoryRuntime) Close() {}

func (f *fakeWorkspaceMemoryRuntime) step() error {
	f.calls++
	if f.calls == f.failAt {
		return errors.New("injected runtime refusal")
	}
	return nil
}

func (f *fakeWorkspaceMemoryRuntime) PrepareMemoryAuthorization(
	context.Context, int64, int64, int64, types.MemoryAction,
) (string, error) {
	return "personal-authorization", f.step()
}

func (f *fakeWorkspaceMemoryRuntime) ApplyMemoryAction(
	context.Context, int64, int64, string, types.MemoryAction,
) (*types.MemoryActionResult, error) {
	return &types.MemoryActionResult{}, f.step()
}

func (f *fakeWorkspaceMemoryRuntime) PrepareWorkspaceMemoryAuthorization(
	context.Context, int64, int64, int64, types.MemoryAction,
) (string, error) {
	return "team-authorization", f.step()
}

func (f *fakeWorkspaceMemoryRuntime) ApplyWorkspaceMemoryAction(
	context.Context, int64, int64, string, types.MemoryAction,
) (*types.MemoryActionResult, error) {
	return &types.MemoryActionResult{}, f.step()
}

func (f *fakeWorkspaceMemoryRuntime) RecallMemories(
	_ context.Context, _, userID int64, query types.MemoryRecallQuery,
) (*types.MemoryRecallResult, error) {
	if err := f.step(); err != nil {
		return nil, err
	}
	if userID == 2 {
		return nil, types.NewAppError(types.CodeForbidden,
			"cross-user personal memory denied", types.ErrForbidden)
	}
	if f.calls == 5 && !f.inconsistent {
		return recallResult(query.Query), nil
	}
	return &types.MemoryRecallResult{}, nil
}

func (f *fakeWorkspaceMemoryRuntime) RecallWorkspaceMemories(
	_ context.Context, _, _ int64, query types.MemoryRecallQuery,
) (*types.MemoryRecallResult, error) {
	if err := f.step(); err != nil {
		return nil, err
	}
	if f.calls == 6 && !f.inconsistent {
		return recallResult(query.Query), nil
	}
	return &types.MemoryRecallResult{}, nil
}

func recallResult(text string) *types.MemoryRecallResult {
	return &types.MemoryRecallResult{Memories: []types.MemoryRecallItem{
		{Memory: types.MemoryRecord{Text: text}},
	}}
}

func TestExerciseWorkspaceMemoryRuntimeFailsClosedAtEveryStep(t *testing.T) {
	fixture := workspaceMemoryUATFixture{creatorUserID: 1, memberUserID: 2,
		personalID: 3, teamID: 4, personalSID: 5, teamSID: 6}
	operationID := uuid.NewString()
	for step := 1; step <= 10; step++ {
		t.Run(fmt.Sprintf("step-%d", step), func(t *testing.T) {
			fake := &fakeWorkspaceMemoryRuntime{failAt: step}
			if _, _, err := exerciseWorkspaceMemoryRuntime(
				t.Context(), fake, fixture, operationID); err == nil {
				t.Fatal("injected runtime refusal passed")
			}
			if fake.calls != step {
				t.Fatalf("calls=%d want=%d", fake.calls, step)
			}
		})
	}
	fake := &fakeWorkspaceMemoryRuntime{}
	personal, team, err := exerciseWorkspaceMemoryRuntime(
		t.Context(), fake, fixture, operationID)
	if err != nil || personal == "" || team == "" || personal == team || fake.calls != 10 {
		t.Fatalf("success personal=%q team=%q calls=%d err=%v",
			personal, team, fake.calls, err)
	}
	fake = &fakeWorkspaceMemoryRuntime{inconsistent: true}
	if _, _, err := exerciseWorkspaceMemoryRuntime(
		t.Context(), fake, fixture, operationID); err == nil {
		t.Fatal("inconsistent recall passed")
	}
}

func TestWorkspaceMemoryUATCommandRequiresExactReleaseAuthority(t *testing.T) {
	const (
		revision   = "0123456789abcdef0123456789abcdef01234567"
		operation  = "5d358bef-7093-4327-9f66-f5af9f194e51"
		ownerURL   = "postgres://owner/db"
		runtimeURL = "postgres://runtime/db"
	)
	t.Setenv(migrationDatabaseURLEnv, ownerURL)
	t.Setenv("VANE_DB_URL", runtimeURL)
	valid := []string{"--operation-id", operation, "--expected-revision", revision,
		"--confirm", workspaceMemoryUATConfirmation}
	var output bytes.Buffer
	called := 0
	runner := func(_ context.Context, gotOwner, gotRuntime, gotOperation, gotRevision string,
	) (*workspaceMemoryUATReport, error) {
		called++
		if gotOwner != ownerURL || gotRuntime != runtimeURL || gotOperation != operation ||
			gotRevision != revision {
			t.Fatalf("runner authority owner=%q runtime=%q operation=%q revision=%q",
				gotOwner, gotRuntime, gotOperation, gotRevision)
		}
		return &workspaceMemoryUATReport{Schema: workspaceMemoryUATConfirmation,
			Revision: revision, OperationID: operation, CleanupVerified: true}, nil
	}
	if err := executeWorkspaceMemoryUATCommand(valid, &output,
		func() (string, bool) { return revision, true }, runner); err != nil {
		t.Fatal(err)
	}
	if called != 1 {
		t.Fatalf("runner calls=%d", called)
	}
	var report workspaceMemoryUATReport
	if err := json.Unmarshal(output.Bytes(), &report); err != nil ||
		report.OperationID != operation || !report.CleanupVerified {
		t.Fatalf("output=%q report=%+v err=%v", output.String(), report, err)
	}

	mutations := []struct {
		name      string
		arguments []string
		revision  func() (string, bool)
		owner     string
		runtime   string
	}{
		{"unknown flag", []string{"--unknown"}, func() (string, bool) { return revision, true }, ownerURL, runtimeURL},
		{"missing confirm", valid[:4], func() (string, bool) { return revision, true }, ownerURL, runtimeURL},
		{"bad operation", []string{"--operation-id", uuid.Nil.String(),
			"--expected-revision", revision, "--confirm", workspaceMemoryUATConfirmation},
			func() (string, bool) { return revision, true }, ownerURL, runtimeURL},
		{"dirty binary", valid, func() (string, bool) { return "", false }, ownerURL, runtimeURL},
		{"wrong binary", valid, func() (string, bool) { return strings.Repeat("f", 40), true }, ownerURL, runtimeURL},
		{"same authority", valid, func() (string, bool) { return revision, true }, ownerURL, ownerURL},
	}
	for _, mutation := range mutations {
		t.Run(mutation.name, func(t *testing.T) {
			t.Setenv(migrationDatabaseURLEnv, mutation.owner)
			t.Setenv("VANE_DB_URL", mutation.runtime)
			called = 0
			if err := executeWorkspaceMemoryUATCommand(mutation.arguments, &bytes.Buffer{},
				mutation.revision, runner); err == nil {
				t.Fatal("authority mutation passed")
			}
			if called != 0 {
				t.Fatal("authority mutation reached runner")
			}
		})
	}
	if err := run(append([]string{workspaceMemoryUATCommand}, valid...)); err == nil {
		t.Fatal("development binary passed release-bound UAT command")
	}

	expected := errors.New("runner refused")
	if err := executeWorkspaceMemoryUATCommand(valid, &bytes.Buffer{},
		func() (string, bool) { return revision, true },
		func(context.Context, string, string, string, string) (*workspaceMemoryUATReport, error) {
			return nil, expected
		}); !errors.Is(err, expected) {
		t.Fatalf("runner error=%v", err)
	}
}

func TestWorkspaceMemoryRuntimeDatabaseURLUsesDedicatedCredential(t *testing.T) {
	t.Setenv("VANE_DB_URL", "")
	directory := t.TempDir()
	t.Setenv(migrationDatabaseCredentialEnv, directory)
	if _, err := workspaceMemoryRuntimeDatabaseURL(); err == nil {
		t.Fatal("missing runtime credential passed")
	}
	if err := os.WriteFile(directory+string(os.PathSeparator)+
		workspaceMemoryRuntimeCredential, []byte("postgres://runtime/db\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	value, err := workspaceMemoryRuntimeDatabaseURL()
	if err != nil || value != "postgres://runtime/db" {
		t.Fatalf("value=%q err=%v", value, err)
	}
	t.Setenv("VANE_DB_URL", "postgres://explicit/runtime")
	value, err = workspaceMemoryRuntimeDatabaseURL()
	if err != nil || value != "postgres://explicit/runtime" {
		t.Fatalf("env value=%q err=%v", value, err)
	}
}

func TestWorkspaceMemoryRuntimeUATPostgres(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		testgate.Database(t)
	}
	admin, err := pgxpool.New(t.Context(), databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	databaseName := "vane_memory_uat_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := admin.Exec(t.Context(), "CREATE DATABASE "+pgx.Identifier{databaseName}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	parsed, err := url.Parse(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	parsed.Path = "/" + databaseName
	scratchURL := parsed.String()
	runtimeProvisioned := false
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if runtimeProvisioned {
			if _, err := admin.Exec(cleanupCtx,
				`ALTER ROLE vane_server_runtime NOLOGIN PASSWORD NULL`); err != nil {
				t.Errorf("disable runtime login during cleanup: %v", err)
			}
			if err := store.DeprovisionServerRuntime(cleanupCtx, scratchURL); err != nil {
				t.Errorf("deprovision runtime during cleanup: %v", err)
			}
		}
		if _, err := admin.Exec(cleanupCtx, "DROP DATABASE IF EXISTS "+
			pgx.Identifier{databaseName}.Sanitize()+" WITH (FORCE)"); err != nil {
			t.Errorf("drop scratch database during cleanup: %v", err)
		}
	})
	if err := store.Migrate(t.Context(), scratchURL); err != nil {
		t.Fatal(err)
	}
	if err := store.ProvisionServerRuntime(t.Context(), scratchURL); err != nil {
		t.Fatal(err)
	}
	runtimeProvisioned = true
	const password = "workspace-memory-uat-runtime-password"
	if _, err := admin.Exec(t.Context(), `ALTER ROLE vane_server_runtime LOGIN PASSWORD '`+
		password+`'`); err != nil {
		t.Fatal(err)
	}
	runtimeURL := *parsed
	runtimeURL.User = url.UserPassword("vane_server_runtime", password)
	operationID := uuid.NewString()
	report, err := runWorkspaceMemoryRuntimeUAT(t.Context(), scratchURL,
		runtimeURL.String(), operationID, strings.Repeat("a", 40))
	if err != nil {
		t.Fatal(err)
	}
	if report.Schema != workspaceMemoryUATConfirmation || report.OperationID != operationID ||
		!report.RuntimeBoundaryVerified || !report.PersonalWriteVerified ||
		!report.TeamWriteVerified || !report.CrossMemberRecallVerified ||
		!report.PersonalExcludedFromTeam || !report.TeamExcludedFromPersonal ||
		!report.CrossUserPersonalDenied ||
		!report.CleanupVerified || len(report.PersonalEvidenceDigest) != 64 ||
		len(report.TeamEvidenceDigest) != 64 ||
		report.PersonalEvidenceDigest == report.TeamEvidenceDigest {
		t.Fatalf("workspace memory UAT report=%+v", report)
	}
	pool, err := pgxpool.New(t.Context(), scratchURL)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	var users, tenants, personalRecords, teamRecords int
	if err := pool.QueryRow(t.Context(), `SELECT
		(SELECT count(*) FROM public.users WHERE feishu_open_id LIKE $1),
		(SELECT count(*) FROM public.tenants WHERE display_name LIKE $1),
		(SELECT count(*) FROM public.memory_records),
		(SELECT count(*) FROM public.workspace_memory_records)`,
		workspaceMemoryUATPrefix+"%").Scan(&users, &tenants,
		&personalRecords, &teamRecords); err != nil {
		t.Fatal(err)
	}
	if users != 0 || tenants != 0 || personalRecords != 0 || teamRecords != 0 {
		t.Fatalf("UAT residue users=%d tenants=%d personal=%d team=%d",
			users, tenants, personalRecords, teamRecords)
	}

	failedCtx, failedCancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer failedCancel()
	invalidRuntime := runtimeURL
	invalidRuntime.Host = "127.0.0.1:1"
	if _, err := runWorkspaceMemoryRuntimeUAT(failedCtx, scratchURL,
		invalidRuntime.String(), uuid.NewString(), strings.Repeat("c", 40)); err == nil {
		t.Fatal("unreachable runtime authority passed UAT")
	}
	assertNoWorkspaceMemoryUATResidue(t, pool)
	exhaustedCtx, exhaustedCancel := context.WithCancel(t.Context())
	defer exhaustedCancel()
	factoryCalled := false
	if _, err := runWorkspaceMemoryRuntimeUATWithFactory(exhaustedCtx, scratchURL,
		runtimeURL.String(), uuid.NewString(), strings.Repeat("e", 40),
		func(ctx context.Context, _ string) (workspaceMemoryRuntimeStore, error) {
			factoryCalled = true
			exhaustedCancel()
			<-ctx.Done()
			return nil, ctx.Err()
		}); err == nil {
		t.Fatal("exhausted main UAT window passed")
	}
	if !factoryCalled {
		t.Fatal("main UAT window expired before runtime factory")
	}
	assertNoWorkspaceMemoryUATResidue(t, pool)
	if err := verifyWorkspaceMemoryUATOwner(t.Context(), pool); err != nil {
		t.Fatalf("owner authority rejected: %v", err)
	}
	runtimePool, err := pgxpool.New(t.Context(), runtimeURL.String())
	if err != nil {
		t.Fatal(err)
	}
	if err := verifyWorkspaceMemoryUATOwner(t.Context(), runtimePool); err == nil {
		t.Fatal("runtime authority accepted as fixture owner")
	}
	runtimePool.Close()
	cancelledCtx, cancelNow := context.WithCancel(t.Context())
	cancelNow()
	if _, err := createWorkspaceMemoryUATFixture(cancelledCtx, pool,
		uuid.NewString()); err == nil {
		t.Fatal("cancelled fixture creation passed")
	}
	ownerStore, err := store.New(t.Context(), scratchURL)
	if err != nil {
		t.Fatal(err)
	}
	defer ownerStore.Close()
	if err := cleanupStaleWorkspaceMemoryUAT(cancelledCtx, pool, ownerStore); err == nil {
		t.Fatal("cancelled stale cleanup passed")
	}

	// Simulate a process death after fixture commit. The next independently
	// identified run must purge that residue before creating its own fixture.
	stale, err := createWorkspaceMemoryUATFixture(t.Context(), pool, uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	if stale.teamID <= 0 || stale.personalID <= 0 {
		t.Fatalf("stale fixture=%+v", stale)
	}
	second, err := runWorkspaceMemoryRuntimeUAT(t.Context(), scratchURL,
		runtimeURL.String(), uuid.NewString(), strings.Repeat("b", 40))
	if err != nil || !second.CleanupVerified {
		t.Fatalf("stale recovery report=%+v err=%v", second, err)
	}
	assertNoWorkspaceMemoryUATResidue(t, pool)
	var staleRows int
	if err := pool.QueryRow(t.Context(), `SELECT
		(SELECT count(*) FROM public.tenants WHERE id=ANY($1::bigint[]))+
		(SELECT count(*) FROM public.users WHERE id=ANY($2::bigint[]))`,
		[]int64{stale.personalID, stale.teamID},
		[]int64{stale.creatorUserID, stale.memberUserID}).Scan(&staleRows); err != nil {
		t.Fatal(err)
	}
	if staleRows != 0 {
		t.Fatalf("stale UAT fixture retained %d rows", staleRows)
	}

	assertWorkspaceMemoryUATSerializes(t, scratchURL, runtimeURL.String(), pool)
	if err := cleanupWorkspaceMemoryUATFixture(t.Context(), pool, ownerStore,
		workspaceMemoryUATFixture{}); err != nil {
		t.Fatalf("zero fixture cleanup: %v", err)
	}
}

func assertNoWorkspaceMemoryUATResidue(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	var rows int
	if err := pool.QueryRow(t.Context(), `SELECT
		(SELECT count(*) FROM public.users WHERE feishu_open_id LIKE $1)+
		(SELECT count(*) FROM public.tenants WHERE display_name LIKE $1)`,
		workspaceMemoryUATPrefix+"%").Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 0 {
		t.Fatalf("workspace memory UAT retained %d reserved rows", rows)
	}
}

func assertWorkspaceMemoryUATSerializes(
	t *testing.T, ownerURL, runtimeURL string, observer *pgxpool.Pool,
) {
	t.Helper()
	blocker, err := observer.Acquire(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Release()
	if _, err := blocker.Exec(t.Context(), `SELECT pg_catalog.pg_advisory_lock(
		pg_catalog.hashtextextended('vane/workspace-memory-runtime-uat/v1',0))`); err != nil {
		t.Fatal(err)
	}
	locked := true
	defer func() {
		if locked {
			_, _ = blocker.Exec(context.Background(), `SELECT pg_catalog.pg_advisory_unlock(
				pg_catalog.hashtextextended('vane/workspace-memory-runtime-uat/v1',0))`)
		}
	}()
	parsed, err := url.Parse(ownerURL)
	if err != nil {
		t.Fatal(err)
	}
	query := parsed.Query()
	applicationName := "vane-memory-uat-serialization-" + uuid.NewString()
	query.Set("application_name", applicationName)
	parsed.RawQuery = query.Encode()
	done := make(chan error, 1)
	go func() {
		_, runErr := runWorkspaceMemoryRuntimeUAT(context.Background(), parsed.String(),
			runtimeURL, uuid.NewString(), strings.Repeat("d", 40))
		done <- runErr
	}()
	deadline := time.Now().Add(5 * time.Second)
	for {
		var waiting bool
		err := observer.QueryRow(t.Context(), `SELECT EXISTS(
			SELECT 1 FROM pg_catalog.pg_locks l
			WHERE l.database=(SELECT oid FROM pg_catalog.pg_database
				WHERE datname=pg_catalog.current_database())
			AND l.locktype='advisory' AND NOT l.granted)`).Scan(&waiting)
		if err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		select {
		case runErr := <-done:
			t.Fatalf("UAT did not wait for serialization lock: %v", runErr)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("UAT did not expose an advisory lock wait")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := blocker.Exec(t.Context(), `SELECT pg_catalog.pg_advisory_unlock(
		pg_catalog.hashtextextended('vane/workspace-memory-runtime-uat/v1',0))`); err != nil {
		t.Fatal(err)
	}
	locked = false
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("serialized UAT did not complete after lock release")
	}
	assertNoWorkspaceMemoryUATResidue(t, observer)
}
