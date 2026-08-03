package store

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/YouToco/vane/taskstate"
	"github.com/YouToco/vane/types"
)

func TestNativeResearchTaskCreationV3PostgreSQLAtomicLifecycle(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Skip("DATABASE_URL is required")
	}
	scratchURL, drop := createScratchDB(t.Context(), t, databaseURL)
	t.Cleanup(drop)
	if err := Migrate(t.Context(), scratchURL); err != nil {
		t.Fatal(err)
	}
	st, err := New(t.Context(), scratchURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)

	user, err := st.UpsertUserByOpenID(t.Context(), "native-v3-"+uuid.NewString(), "owner")
	if err != nil {
		t.Fatal(err)
	}
	var tenantID int64
	if err := st.pool.QueryRow(t.Context(),
		`INSERT INTO tenants(status,plan) VALUES ('active','free') RETURNING id`).Scan(&tenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := st.pool.Exec(t.Context(),
		`INSERT INTO memberships(tenant_id,user_id,role) VALUES($1,$2,'owner')`,
		tenantID, user.ID); err != nil {
		t.Fatal(err)
	}

	operationID := "native-v3-" + uuid.NewString()
	taskID := nativeResearchTaskIDV3Test(tenantID, user.ID, operationID)
	definition, err := taskstate.BuildApprovedDefinitionV3(taskstate.ApprovedDefinitionInputV3{
		TenantID: tenantID, UserID: user.ID, TaskID: taskID,
		TaskName:      "Kimi 套餐可购买状态",
		TaskManual:    "检查 Kimi 官方套餐页，交叉核验并和历史结论比较；没有重大更新不推送。",
		SpecJSON:      json.RawMessage(`{"tz":"Asia/Shanghai","cron":"0 9 * * 1"}`),
		ExecutionMode: types.ExecutionModeDiscoverAtRun,
		Notification: taskstate.NotificationPolicyV3{
			MinimumSignificance: taskstate.NotificationThresholdMajorV3,
			SuppressEmpty:       true,
		},
		Output: taskstate.OutputPreferenceV3{
			Language:             taskstate.OutputLanguageZhCNV3,
			Format:               taskstate.OutputFormatExecutiveBriefV3,
			IncludeEvidenceLinks: true,
		},
		PlannerBudget: types.PlannerBudget{
			MaxPlannerRounds: 8, MaxToolCalls: 16, MaxTokens: 32768,
			MaxCostMicroUSD: 1_000_000, DurationMs: 300_000,
		},
		DeliveryPolicy:     taskstate.DeliveryPolicyOwnerFeishu,
		TenantBudgetPolicy: taskstate.BudgetPolicyInheritTenantQuota,
	})
	if err != nil {
		t.Fatal(err)
	}
	payload, _ := taskstate.EncodeApprovedDefinitionV3(definition)
	digest, _ := taskstate.DigestApprovedDefinitionV3(definition)
	createParams := types.CreateResearchTaskCreationOperationV3Params{
		ID: operationID, TenantID: tenantID, UserID: user.ID,
		Args: payload, Summary: definition.TaskName,
		ExpiresAt: time.Now().Add(time.Hour).UTC().Truncate(time.Microsecond),
	}
	op, err := st.CreateResearchTaskCreationOperationV3(t.Context(), createParams)
	if err != nil || op.ExecutionVersion != types.TaskCreationExecutionVersionV2 {
		t.Fatalf("operation=%+v err=%v", op, err)
	}
	if replay, err := st.CreateResearchTaskCreationOperationV3(
		t.Context(), createParams); err != nil || replay.ID != op.ID {
		t.Fatalf("operation replay=%+v err=%v", replay, err)
	}
	prepared, receipt, action := []byte(`{"prepared":true}`), []byte(`{"ensured":true}`), []byte(`{"runtime":"research-v3"}`)
	leaseOwner := "native-v3-test-owner"
	if _, err := st.pool.Exec(t.Context(), `
		UPDATE task_creation_operations
		   SET status='executing',phase='schedule_ensured',lease_owner=$2,
		       lease_until=clock_timestamp()+interval '1 hour',
		       takeover_not_before=clock_timestamp()+interval '2 hours',
		       fence=1,attempt=1,compiled_definition=$3,compiled_digest=$4,
		       prepared_schedule=$5,ensure_receipt=$6,task_id=$7
		 WHERE id=$1 AND execution_version=2`, operationID, leaseOwner, payload,
		digest, prepared, receipt, taskID); err != nil {
		t.Fatal(err)
	}
	lease := types.TaskCreationLease{
		ID: operationID, TenantID: tenantID, UserID: user.ID,
		LeaseOwner: leaseOwner, Fence: 1,
	}
	params := types.CommitPausedResearchTaskDefinitionV3ForCreationParams{
		Lease: lease, TaskID: taskID, DefinitionPayload: payload,
		DefinitionDigest: digest, PreparedSchedule: prepared, EnsureReceipt: receipt,
		TargetAction: action, TargetActionDigest: hexDigestV3Test(action),
		ActionAuthorizationDigest: hexDigestV3Test([]byte("authorization")),
	}
	foreignTenant := params
	foreignTenant.Lease.TenantID++
	if err := st.CommitPausedResearchTaskDefinitionV3ForCreation(
		t.Context(), foreignTenant); err == nil {
		t.Fatal("cross-tenant native V3 commit succeeded")
	}
	foreignUser := params
	foreignUser.Lease.UserID++
	if err := st.CommitPausedResearchTaskDefinitionV3ForCreation(
		t.Context(), foreignUser); err == nil {
		t.Fatal("cross-user native V3 commit succeeded")
	}
	if err := st.CommitPausedResearchTaskDefinitionV3ForCreation(t.Context(), params); err != nil {
		t.Fatal(err)
	}
	// Response-loss replay must adopt the exact four-row aggregate.
	if err := st.CommitPausedResearchTaskDefinitionV3ForCreation(t.Context(), params); err != nil {
		t.Fatalf("commit replay: %v", err)
	}

	var status, mode, playbook, authorityStatus string
	var version, authorityCount, fetchTargetCount int64
	if err := st.pool.QueryRow(t.Context(), `
		SELECT schedule.status,schedule.execution_mode,playbook.content,
		       schedule.approved_definition_version,
		       (SELECT count(*) FROM research_v3_delivery_authorities authority
		         WHERE authority.tenant_id=schedule.tenant_id AND authority.user_id=schedule.user_id
		           AND authority.task_id=schedule.id AND authority.status='staged'),
		       (SELECT count(*) FROM task_fetch_targets target WHERE target.schedule_id=schedule.id)
		  FROM schedules schedule JOIN schedule_playbooks playbook ON playbook.schedule_id=schedule.id
		 WHERE schedule.id=$1`, taskID).Scan(
		&status, &mode, &playbook, &version, &authorityCount, &fetchTargetCount); err != nil {
		t.Fatal(err)
	}
	if status != "paused" || mode != "discover_at_run" || version != 1 ||
		playbook != definition.TaskManual || authorityCount != 1 || fetchTargetCount != 0 {
		t.Fatalf("paused aggregate status=%s mode=%s version=%d authority=%d targets=%d playbook=%q",
			status, mode, version, authorityCount, fetchTargetCount, playbook)
	}
	assertNativeV3TaskHidden(t, st, user.ID, taskID)
	if _, err := st.pool.Exec(t.Context(),
		`UPDATE schedules SET status='active' WHERE id=$1`, taskID); err != nil {
		t.Fatal(err)
	}
	if err := st.CommitPausedResearchTaskDefinitionV3ForCreation(
		t.Context(), params); err == nil {
		t.Fatal("definition_committed replay adopted an active schedule drift")
	}
	unfinishedIdentity := types.RunIdentity{
		TemporalWorkflowID: "workflow-unfinished-" + uuid.NewString(),
		TemporalRunID:      "run-unfinished-" + uuid.NewString(),
		RunKind:            types.RunSnapshotKindScheduled,
		TenantID:           tenantID, UserID: user.ID, TaskID: taskID,
	}
	if _, err := st.CreateOrGetResearchRunSnapshotV3(t.Context(), unfinishedIdentity,
		testCompiledRunPolicyV1(t), testResearchToolPolicyStoreV3(t),
		testResearchModelPolicyStoreV3(t)); err == nil {
		t.Fatal("unfinished V2 aggregate entered the paid research snapshot/spend path")
	}
	deliveryProbe, err := st.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := authorizeResearchBriefDeliveryPrepareV3(
		t.Context(), deliveryProbe, unfinishedIdentity); err == nil {
		_ = deliveryProbe.Rollback(t.Context())
		t.Fatal("unfinished V2 aggregate entered the delivery path")
	}
	if err := deliveryProbe.Rollback(t.Context()); err != nil {
		t.Fatal(err)
	}
	var unfinishedSnapshots int
	if err := st.pool.QueryRow(t.Context(), `
		SELECT count(*) FROM task_run_snapshots
		 WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3`,
		tenantID, user.ID, taskID).Scan(&unfinishedSnapshots); err != nil {
		t.Fatal(err)
	}
	if unfinishedSnapshots != 0 {
		t.Fatalf("unfinished V2 aggregate persisted %d paid-run snapshots", unfinishedSnapshots)
	}
	if _, err := st.pool.Exec(t.Context(),
		`UPDATE schedules SET status='paused' WHERE id=$1`, taskID); err != nil {
		t.Fatal(err)
	}

	activationBinding := nativeV3ActivationBindingTest(params)
	driftedBinding := activationBinding
	driftedBinding.TargetActionDigest = hexDigestV3Test([]byte("different target action"))
	if _, err := st.BeginResearchTaskCreationActivationV3(
		t.Context(), lease, driftedBinding); err == nil {
		t.Fatal("activation begin accepted a different immutable action binding")
	}
	started, err := st.BeginResearchTaskCreationActivationV3(t.Context(), lease, activationBinding)
	if err != nil || !started {
		t.Fatalf("begin activation started=%v err=%v", started, err)
	}
	started, err = st.BeginResearchTaskCreationActivationV3(t.Context(), lease, activationBinding)
	if err != nil || started {
		t.Fatalf("begin replay started=%v err=%v", started, err)
	}
	if err := st.CommitResearchTaskCreationActivationV3(
		t.Context(), lease, driftedBinding); err == nil {
		t.Fatal("activation commit accepted a different immutable action binding")
	}
	if _, err := st.pool.Exec(t.Context(),
		`UPDATE tenants SET status='suspended' WHERE id=$1`, tenantID); err != nil {
		t.Fatal(err)
	}
	if err := st.CommitResearchTaskCreationActivationV3(
		t.Context(), lease, activationBinding); err == nil {
		t.Fatal("activation enabled a task after owner scope was suspended")
	}
	if err := st.pool.QueryRow(t.Context(), `
		SELECT schedule.status,authority.status
		  FROM schedules schedule JOIN research_v3_delivery_authorities authority
		    ON authority.tenant_id=schedule.tenant_id AND authority.user_id=schedule.user_id
		   AND authority.task_id=schedule.id AND authority.generation=1
		 WHERE schedule.id=$1`, taskID).Scan(&status, &authorityStatus); err != nil {
		t.Fatal(err)
	}
	if status != "paused" || authorityStatus != "staged" {
		t.Fatalf("failed activation was not atomic: schedule=%s authority=%s", status, authorityStatus)
	}
	if _, err := st.pool.Exec(t.Context(),
		`UPDATE tenants SET status='active' WHERE id=$1`, tenantID); err != nil {
		t.Fatal(err)
	}
	if err := st.CommitResearchTaskCreationActivationV3(t.Context(), lease, activationBinding); err != nil {
		t.Fatal(err)
	}
	if err := st.CommitResearchTaskCreationActivationV3(t.Context(), lease, activationBinding); err != nil {
		t.Fatalf("activation replay: %v", err)
	}
	var operationPhase string
	if err := st.pool.QueryRow(t.Context(), `
		SELECT operation.phase,schedule.status,authority.status
		  FROM task_creation_operations operation
		  JOIN schedules schedule ON schedule.id=operation.task_id
		  JOIN research_v3_delivery_authorities authority
		    ON authority.tenant_id=operation.tenant_id AND authority.user_id=operation.user_id
		   AND authority.task_id=operation.task_id AND authority.generation=1
		 WHERE operation.id=$1`, operationID).Scan(
		&operationPhase, &status, &authorityStatus); err != nil {
		t.Fatal(err)
	}
	if operationPhase != "activated" || status != "active" || authorityStatus != "enabled" {
		t.Fatalf("activation was not atomic: phase=%s schedule=%s authority=%s",
			operationPhase, status, authorityStatus)
	}
	assertNativeV3TaskHidden(t, st, user.ID, taskID)
}

func TestNativeResearchTaskCreationV3ReplayRejectsMissingOrRevokedAuthority(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Skip("DATABASE_URL is required")
	}
	scratchURL, drop := createScratchDB(t.Context(), t, databaseURL)
	t.Cleanup(drop)
	if err := Migrate(t.Context(), scratchURL); err != nil {
		t.Fatal(err)
	}
	st, err := New(t.Context(), scratchURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	user, err := st.UpsertUserByOpenID(t.Context(), "native-v3-replay-"+uuid.NewString(), "owner")
	if err != nil {
		t.Fatal(err)
	}
	var tenantID int64
	if err := st.pool.QueryRow(t.Context(),
		`INSERT INTO tenants(status,plan) VALUES ('active','free') RETURNING id`).Scan(&tenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := st.pool.Exec(t.Context(),
		`INSERT INTO memberships(tenant_id,user_id,role) VALUES($1,$2,'owner')`,
		tenantID, user.ID); err != nil {
		t.Fatal(err)
	}

	deleted := prepareNativeV3CommitFixture(t, st, tenantID, user.ID, "deleted-aggregate")
	if err := st.CommitPausedResearchTaskDefinitionV3ForCreation(t.Context(), deleted); err != nil {
		t.Fatal(err)
	}
	if _, err := st.pool.Exec(t.Context(), `DELETE FROM schedules WHERE id=$1`, deleted.TaskID); err != nil {
		t.Fatal(err)
	}
	if err := st.CommitPausedResearchTaskDefinitionV3ForCreation(t.Context(), deleted); err == nil {
		t.Fatal("definition replay adopted a deleted aggregate")
	}

	missing := prepareNativeV3CommitFixture(t, st, tenantID, user.ID, "missing-authority")
	if err := st.CommitPausedResearchTaskDefinitionV3ForCreation(t.Context(), missing); err != nil {
		t.Fatal(err)
	}
	if _, err := st.pool.Exec(t.Context(), `
		DELETE FROM research_v3_delivery_authorities
		 WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3 AND generation=1`,
		tenantID, user.ID, missing.TaskID); err != nil {
		t.Fatal(err)
	}
	if err := st.CommitPausedResearchTaskDefinitionV3ForCreation(t.Context(), missing); err == nil {
		t.Fatal("definition replay adopted a missing authority")
	}

	revoked := prepareNativeV3CommitFixture(t, st, tenantID, user.ID, "revoked-authority")
	if err := st.CommitPausedResearchTaskDefinitionV3ForCreation(t.Context(), revoked); err != nil {
		t.Fatal(err)
	}
	if _, err := st.pool.Exec(t.Context(), `
		UPDATE research_v3_delivery_authorities
		   SET status='revoked',revoked_at=clock_timestamp()
		 WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3 AND generation=1`,
		tenantID, user.ID, revoked.TaskID); err != nil {
		t.Fatal(err)
	}
	if err := st.CommitPausedResearchTaskDefinitionV3ForCreation(t.Context(), revoked); err == nil {
		t.Fatal("definition replay adopted a revoked authority")
	}
}

func TestNativeResearchTaskCreationV3WaitsForAdvisoryBeforeRowLock(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Skip("DATABASE_URL is required")
	}
	scratchURL, drop := createScratchDB(t.Context(), t, databaseURL)
	t.Cleanup(drop)
	if err := Migrate(t.Context(), scratchURL); err != nil {
		t.Fatal(err)
	}
	st, err := New(t.Context(), scratchURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	user, err := st.UpsertUserByOpenID(t.Context(), "native-v3-lock-"+uuid.NewString(), "owner")
	if err != nil {
		t.Fatal(err)
	}
	var tenantID int64
	if err := st.pool.QueryRow(t.Context(),
		`INSERT INTO tenants(status,plan) VALUES ('active','free') RETURNING id`).Scan(&tenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := st.pool.Exec(t.Context(),
		`INSERT INTO memberships(tenant_id,user_id,role) VALUES($1,$2,'owner')`,
		tenantID, user.ID); err != nil {
		t.Fatal(err)
	}
	params := prepareNativeV3CommitFixture(t, st, tenantID, user.ID, "lock-order")
	blocker, err := st.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = blocker.Rollback(t.Context()) }()
	lockKey := fmt.Sprintf("%d/%d/%s", tenantID, user.ID, params.TaskID)
	if _, err := blocker.Exec(t.Context(),
		`SELECT pg_advisory_xact_lock(hashtextextended($1,101))`, lockKey); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		result <- st.CommitPausedResearchTaskDefinitionV3ForCreation(t.Context(), params)
	}()
	deadline := time.Now().Add(5 * time.Second)
	waiting := false
	for time.Now().Before(deadline) {
		if err := st.pool.QueryRow(t.Context(), `
			SELECT EXISTS (
			 SELECT 1 FROM pg_stat_activity
			  WHERE query LIKE '%commit_native_research_task_creation_v3_v1%'
			    AND wait_event_type='Lock' AND wait_event='advisory')`).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !waiting {
		t.Fatal("native V3 commit did not block on the exact-task advisory lock")
	}
	probe, err := st.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := probe.Exec(t.Context(), `
		SELECT 1 FROM task_creation_operations WHERE id=$1 FOR UPDATE NOWAIT`,
		params.Lease.ID); err != nil {
		_ = probe.Rollback(t.Context())
		t.Fatalf("operation row was locked before advisory authority: %v", err)
	}
	if err := probe.Rollback(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := blocker.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("native V3 commit after advisory release: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("native V3 commit remained blocked after advisory release")
	}
}

func TestNativeResearchTaskCreationV3ActivationWaitsForAdvisoryBeforeRowLock(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Skip("DATABASE_URL is required")
	}
	scratchURL, drop := createScratchDB(t.Context(), t, databaseURL)
	t.Cleanup(drop)
	if err := Migrate(t.Context(), scratchURL); err != nil {
		t.Fatal(err)
	}
	st, err := New(t.Context(), scratchURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	user, err := st.UpsertUserByOpenID(t.Context(), "native-v3-activation-lock-"+uuid.NewString(), "owner")
	if err != nil {
		t.Fatal(err)
	}
	var tenantID int64
	if err := st.pool.QueryRow(t.Context(),
		`INSERT INTO tenants(status,plan) VALUES ('active','free') RETURNING id`).Scan(&tenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := st.pool.Exec(t.Context(),
		`INSERT INTO memberships(tenant_id,user_id,role) VALUES($1,$2,'owner')`,
		tenantID, user.ID); err != nil {
		t.Fatal(err)
	}
	params := prepareNativeV3CommitFixture(t, st, tenantID, user.ID, "activation-lock")
	if err := st.CommitPausedResearchTaskDefinitionV3ForCreation(t.Context(), params); err != nil {
		t.Fatal(err)
	}
	binding := nativeV3ActivationBindingTest(params)
	lockKey := fmt.Sprintf("%d/%d/%s", tenantID, user.ID, params.TaskID)

	assertWaits := func(t *testing.T, functionName string, invoke func() error) {
		t.Helper()
		blocker, err := st.pool.Begin(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = blocker.Rollback(t.Context()) }()
		if _, err := blocker.Exec(t.Context(),
			`SELECT pg_advisory_xact_lock(hashtextextended($1,101))`, lockKey); err != nil {
			t.Fatal(err)
		}
		result := make(chan error, 1)
		go func() { result <- invoke() }()
		deadline := time.Now().Add(5 * time.Second)
		waiting := false
		for time.Now().Before(deadline) {
			if err := st.pool.QueryRow(t.Context(), `
				SELECT EXISTS (
				 SELECT 1 FROM pg_stat_activity
				  WHERE query LIKE $1 AND wait_event_type='Lock' AND wait_event='advisory')`,
				"%"+functionName+"%").Scan(&waiting); err != nil {
				t.Fatal(err)
			}
			if waiting {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		if !waiting {
			t.Fatalf("%s did not wait for exact-task advisory", functionName)
		}
		probe, err := st.pool.Begin(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := probe.Exec(t.Context(), `
			SELECT 1 FROM task_creation_operations WHERE id=$1 FOR UPDATE NOWAIT`,
			params.Lease.ID); err != nil {
			_ = probe.Rollback(t.Context())
			t.Fatalf("%s locked operation before advisory authority: %v", functionName, err)
		}
		if err := probe.Rollback(t.Context()); err != nil {
			t.Fatal(err)
		}
		if err := blocker.Commit(t.Context()); err != nil {
			t.Fatal(err)
		}
		select {
		case err := <-result:
			if err != nil {
				t.Fatalf("%s after advisory release: %v", functionName, err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("%s remained blocked after advisory release", functionName)
		}
	}

	assertWaits(t, "begin_native_research_task_activation_v3_v1", func() error {
		started, err := st.BeginResearchTaskCreationActivationV3(t.Context(), params.Lease, binding)
		if err == nil && !started {
			return errors.New("activation begin replayed before first authorization")
		}
		return err
	})
	assertWaits(t, "commit_native_research_task_activation_v3_v1", func() error {
		return st.CommitResearchTaskCreationActivationV3(t.Context(), params.Lease, binding)
	})
}

func TestNativeResearchTaskCreationV3CapacitySerializesConcurrentCommits(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Skip("DATABASE_URL is required")
	}
	scratchURL, drop := createScratchDB(t.Context(), t, databaseURL)
	t.Cleanup(drop)
	if err := Migrate(t.Context(), scratchURL); err != nil {
		t.Fatal(err)
	}
	st, err := New(t.Context(), scratchURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	user, err := st.UpsertUserByOpenID(t.Context(), "native-v3-capacity-"+uuid.NewString(), "owner")
	if err != nil {
		t.Fatal(err)
	}
	var tenantID int64
	if err := st.pool.QueryRow(t.Context(),
		`INSERT INTO tenants(status,plan) VALUES ('active','free') RETURNING id`).Scan(&tenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := st.pool.Exec(t.Context(),
		`INSERT INTO memberships(tenant_id,user_id,role) VALUES($1,$2,'owner')`,
		tenantID, user.ID); err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 19; index++ {
		if _, err := st.pool.Exec(t.Context(), `
			INSERT INTO schedules(id,tenant_id,user_id,nl_description,spec_json,scope_json,status)
			VALUES($1,$2,$3,'existing','{"every_seconds":3600}','{}','active')`,
			"existing-"+uuid.NewString(), tenantID, user.ID); err != nil {
			t.Fatal(err)
		}
	}
	left := prepareNativeV3CommitFixture(t, st, tenantID, user.ID, "left")
	right := prepareNativeV3CommitFixture(t, st, tenantID, user.ID, "right")
	start := make(chan struct{})
	results := make(chan error, 2)
	var workers sync.WaitGroup
	for _, params := range []types.CommitPausedResearchTaskDefinitionV3ForCreationParams{left, right} {
		workers.Add(1)
		go func(candidate types.CommitPausedResearchTaskDefinitionV3ForCreationParams) {
			defer workers.Done()
			<-start
			results <- st.CommitPausedResearchTaskDefinitionV3ForCreation(t.Context(), candidate)
		}(params)
	}
	close(start)
	workers.Wait()
	close(results)
	succeeded, failed := 0, 0
	for result := range results {
		if result == nil {
			succeeded++
		} else {
			failed++
		}
	}
	if succeeded != 1 || failed != 1 {
		t.Fatalf("capacity race succeeded=%d failed=%d", succeeded, failed)
	}
	var created int
	if err := st.pool.QueryRow(t.Context(), `
		SELECT count(*) FROM schedules WHERE id IN ($1,$2)`,
		left.TaskID, right.TaskID).Scan(&created); err != nil {
		t.Fatal(err)
	}
	if created != 1 {
		t.Fatalf("capacity race created %d native schedules, want 1", created)
	}
}

func TestTaskCreationCapacityIsSymmetricAcrossV1AndV2(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Skip("DATABASE_URL is required")
	}
	scratchURL, drop := createScratchDB(t.Context(), t, databaseURL)
	t.Cleanup(drop)
	if err := Migrate(t.Context(), scratchURL); err != nil {
		t.Fatal(err)
	}
	st, err := New(t.Context(), scratchURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)

	newScope := func(t *testing.T, label string) (*compiledTaskFixture, int64, int64) {
		t.Helper()
		user, err := st.UpsertUserByOpenID(t.Context(),
			"native-v3-cross-capacity-"+label+"-"+uuid.NewString(), "owner")
		if err != nil {
			t.Fatal(err)
		}
		var tenantID int64
		if err := st.pool.QueryRow(t.Context(),
			`INSERT INTO tenants(status,plan) VALUES ('active','free') RETURNING id`).Scan(&tenantID); err != nil {
			t.Fatal(err)
		}
		if _, err := st.pool.Exec(t.Context(),
			`INSERT INTO memberships(tenant_id,user_id,role) VALUES($1,$2,'owner')`,
			tenantID, user.ID); err != nil {
			t.Fatal(err)
		}
		for index := 0; index < 19; index++ {
			if _, err := st.pool.Exec(t.Context(), `
				INSERT INTO schedules(id,tenant_id,user_id,nl_description,spec_json,scope_json,status)
				VALUES($1,$2,$3,'existing','{"every_seconds":3600}','{}','active')`,
				"existing-cross-"+uuid.NewString(), tenantID, user.ID); err != nil {
				t.Fatal(err)
			}
		}
		return &compiledTaskFixture{
			st: st, tenantID: tenantID, userID: user.ID,
			urlRoot: "vane://cross-capacity/" + uuid.NewString(),
		}, tenantID, user.ID
	}
	assertTwenty := func(t *testing.T, userID int64) {
		t.Helper()
		var activeOrReserved int
		tx, err := st.pool.Begin(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(t.Context()) }()
		activeOrReserved, err = countTaskCreationCapacity(t.Context(), tx, userID)
		if err != nil || activeOrReserved != 20 {
			t.Fatalf("cross-protocol capacity=%d want=20 err=%v", activeOrReserved, err)
		}
	}

	t.Run("serial V2 then V1", func(t *testing.T) {
		fixture, tenantID, userID := newScope(t, "serial-v2-v1")
		v1 := preparedA5Commit(t, st, fixture, "serial-v2-v1")
		v2 := prepareNativeV3CommitFixture(t, st, tenantID, userID, "serial-v2-v1")
		if err := st.CommitPausedResearchTaskDefinitionV3ForCreation(t.Context(), v2); err != nil {
			t.Fatalf("V2 first commit: %v", err)
		}
		if err := st.CommitPausedCompiledTaskDefinitionForCreation(t.Context(), v1); !errors.Is(err, types.ErrTaskCreationLimit) {
			t.Fatalf("V1 did not observe V2 reservation: %v", err)
		}
		assertTwenty(t, userID)
	})

	t.Run("serial V1 then V2", func(t *testing.T) {
		fixture, tenantID, userID := newScope(t, "serial-v1-v2")
		v1 := preparedA5Commit(t, st, fixture, "serial-v1-v2")
		v2 := prepareNativeV3CommitFixture(t, st, tenantID, userID, "serial-v1-v2")
		if err := st.CommitPausedCompiledTaskDefinitionForCreation(t.Context(), v1); err != nil {
			t.Fatalf("V1 first commit: %v", err)
		}
		if err := st.CommitPausedResearchTaskDefinitionV3ForCreation(t.Context(), v2); err == nil {
			t.Fatal("V2 did not observe V1 reservation")
		}
		assertTwenty(t, userID)
	})

	for _, first := range []string{"V1", "V2"} {
		t.Run("concurrent "+first+" launched first", func(t *testing.T) {
			fixture, tenantID, userID := newScope(t, "concurrent-"+first)
			v1 := preparedA5Commit(t, st, fixture, "concurrent-"+first)
			v2 := prepareNativeV3CommitFixture(t, st, tenantID, userID, "concurrent-"+first)
			results := make(chan error, 2)
			launchV1 := func() { results <- st.CommitPausedCompiledTaskDefinitionForCreation(t.Context(), v1) }
			launchV2 := func() { results <- st.CommitPausedResearchTaskDefinitionV3ForCreation(t.Context(), v2) }
			if first == "V1" {
				go launchV1()
				time.Sleep(20 * time.Millisecond)
				go launchV2()
			} else {
				go launchV2()
				time.Sleep(20 * time.Millisecond)
				go launchV1()
			}
			succeeded := 0
			for range 2 {
				if err := <-results; err == nil {
					succeeded++
				}
			}
			if succeeded != 1 {
				t.Fatalf("cross-protocol concurrent success=%d want=1", succeeded)
			}
			assertTwenty(t, userID)
		})
	}

	t.Run("V1 quarantine blocks V2", func(t *testing.T) {
		fixture, tenantID, userID := newScope(t, "blocked-v1-v2")
		v1 := preparedA5Commit(t, st, fixture, "blocked-v1-v2")
		if err := st.BlockTaskCreationOperationAfterSideEffect(t.Context(),
			v1.Lease, v1.Definition.TaskID, "REMOTE_RETAINED", "remote schedule retained"); err != nil {
			t.Fatal(err)
		}
		v2 := prepareNativeV3CommitFixture(t, st, tenantID, userID, "blocked-v1-v2")
		if err := st.CommitPausedResearchTaskDefinitionV3ForCreation(t.Context(), v2); err == nil {
			t.Fatal("V2 ignored retained V1 quarantine")
		}
		assertTwenty(t, userID)
	})

	t.Run("V2 quarantine blocks V1", func(t *testing.T) {
		fixture, tenantID, userID := newScope(t, "blocked-v2-v1")
		v2 := prepareNativeV3CommitFixture(t, st, tenantID, userID, "blocked-v2-v1")
		if err := st.BlockResearchTaskCreationOperationV3(t.Context(), v2.Lease,
			v2.TaskID, "REMOTE_RETAINED", "remote schedule retained"); err != nil {
			t.Fatal(err)
		}
		v1 := preparedA5Commit(t, st, fixture, "blocked-v2-v1")
		if err := st.CommitPausedCompiledTaskDefinitionForCreation(t.Context(), v1); !errors.Is(err, types.ErrTaskCreationLimit) {
			t.Fatalf("V1 ignored retained V2 quarantine: %v", err)
		}
		assertTwenty(t, userID)
	})
}

func prepareNativeV3CommitFixture(
	t *testing.T, st *Store, tenantID, userID int64, label string,
) types.CommitPausedResearchTaskDefinitionV3ForCreationParams {
	t.Helper()
	operationID := "native-v3-" + label + "-" + uuid.NewString()
	taskID := nativeResearchTaskIDV3Test(tenantID, userID, operationID)
	definition, err := taskstate.BuildApprovedDefinitionV3(taskstate.ApprovedDefinitionInputV3{
		TenantID: tenantID, UserID: userID, TaskID: taskID,
		TaskName: "research " + label, TaskManual: "check official evidence for " + label,
		SpecJSON:      json.RawMessage(`{"tz":"Asia/Shanghai","every_seconds":3600}`),
		ExecutionMode: types.ExecutionModeDiscoverAtRun,
		Notification: taskstate.NotificationPolicyV3{
			MinimumSignificance: taskstate.NotificationThresholdMajorV3, SuppressEmpty: true,
		},
		Output: taskstate.OutputPreferenceV3{
			Language:             taskstate.OutputLanguageEnV3,
			Format:               taskstate.OutputFormatConciseBriefV3,
			IncludeEvidenceLinks: true,
		},
		PlannerBudget: types.PlannerBudget{
			MaxPlannerRounds: 8, MaxToolCalls: 16, MaxTokens: 32768,
			MaxCostMicroUSD: 1_000_000, DurationMs: 300_000,
		},
		DeliveryPolicy:     taskstate.DeliveryPolicyOwnerFeishu,
		TenantBudgetPolicy: taskstate.BudgetPolicyInheritTenantQuota,
	})
	if err != nil {
		t.Fatal(err)
	}
	payload, _ := taskstate.EncodeApprovedDefinitionV3(definition)
	digest, _ := taskstate.DigestApprovedDefinitionV3(definition)
	if _, err := st.CreateResearchTaskCreationOperationV3(t.Context(),
		types.CreateResearchTaskCreationOperationV3Params{
			ID: operationID, TenantID: tenantID, UserID: userID,
			Args: payload, Summary: definition.TaskName,
			ExpiresAt: time.Now().Add(time.Hour).UTC().Truncate(time.Microsecond),
		}); err != nil {
		t.Fatal(err)
	}
	prepared := []byte(`{"prepared":true}`)
	receipt := []byte(`{"ensured":true}`)
	action := []byte(`{"runtime":"research-v3"}`)
	leaseOwner := "native-v3-" + label
	if _, err := st.pool.Exec(t.Context(), `
		UPDATE task_creation_operations
		   SET status='executing',phase='schedule_ensured',lease_owner=$2,
		       lease_until=clock_timestamp()+interval '1 hour',
		       takeover_not_before=clock_timestamp()+interval '2 hours',
		       fence=1,attempt=1,compiled_definition=$3,compiled_digest=$4,
		       prepared_schedule=$5,ensure_receipt=$6,task_id=$7
		 WHERE id=$1 AND tool_name='manage_tasks' AND execution_version=2`,
		operationID, leaseOwner, payload, digest, prepared, receipt, taskID); err != nil {
		t.Fatal(err)
	}
	return types.CommitPausedResearchTaskDefinitionV3ForCreationParams{
		Lease: types.TaskCreationLease{
			ID: operationID, TenantID: tenantID, UserID: userID,
			LeaseOwner: leaseOwner, Fence: 1,
		},
		TaskID: taskID, DefinitionPayload: payload, DefinitionDigest: digest,
		PreparedSchedule: prepared, EnsureReceipt: receipt,
		TargetAction: action, TargetActionDigest: hexDigestV3Test(action),
		ActionAuthorizationDigest: hexDigestV3Test([]byte("authorization-" + label)),
	}
}

func assertNativeV3TaskHidden(t *testing.T, st *Store, userID int64, taskID string) {
	t.Helper()
	schedules, err := st.ListSchedulesByUser(t.Context(), userID)
	if err != nil {
		t.Fatal(err)
	}
	for _, schedule := range schedules {
		if schedule.ID == taskID {
			t.Fatalf("unfinished native V3 aggregate %s escaped mature schedule fence", taskID)
		}
	}
}

func hexDigestV3Test(payload []byte) string {
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func nativeV3ActivationBindingTest(
	params types.CommitPausedResearchTaskDefinitionV3ForCreationParams,
) types.ResearchTaskCreationActivationBindingV3 {
	return types.ResearchTaskCreationActivationBindingV3{
		TaskID: params.TaskID, DefinitionDigest: params.DefinitionDigest,
		TargetActionDigest:        params.TargetActionDigest,
		ActionAuthorizationDigest: params.ActionAuthorizationDigest,
	}
}

func nativeResearchTaskIDV3Test(tenantID, userID int64, operationID string) string {
	payload, _ := json.Marshal(struct {
		Version     string `json:"version"`
		TenantID    int64  `json:"tenant_id"`
		UserID      int64  `json:"user_id"`
		OperationID string `json:"operation_id"`
	}{"v1", tenantID, userID, operationID})
	return "task-v1-" + hexDigestV3Test(payload)
}
