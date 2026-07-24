package store

import (
	"bytes"
	"context"
	"errors"
	"os"
	"sync"
	"testing"

	"github.com/google/uuid"

	"github.com/YouToco/vane/types"
)

func TestTaskDefinitionBaselineDryRunApplyVerifyAndReplay(t *testing.T) {
	f := newTaskDefinitionStateFixture(t)
	ctx := t.Context()

	before := loadTaskDefinitionBaselineLegacyBytes(t, f)
	dryRun := taskDefinitionBaselineResultForTask(
		t, f.store, TaskDefinitionBaselineDryRun, f.taskID)
	if dryRun.Status != TaskDefinitionBaselineWouldApply ||
		dryRun.Version != initialApprovedDefinitionVersion ||
		dryRun.Digest == "" {
		t.Fatalf("dry-run result=%+v", dryRun)
	}
	if _, err := f.store.GetCurrentApprovedDefinition(
		ctx, f.tenantID, f.userID, f.taskID); !errors.Is(err, types.ErrNotFound) {
		t.Fatalf("dry-run wrote a head: %v", err)
	}

	applied := taskDefinitionBaselineResultForTask(
		t, f.store, TaskDefinitionBaselineApply, f.taskID)
	if applied.Status != TaskDefinitionBaselineApplied ||
		applied.Version != dryRun.Version || applied.Digest != dryRun.Digest {
		t.Fatalf("apply=%+v dry-run=%+v", applied, dryRun)
	}
	after := loadTaskDefinitionBaselineLegacyBytes(t, f)
	if !bytes.Equal(before, after) {
		t.Fatal("baseline apply changed retained legacy projection")
	}
	head, err := f.store.GetCurrentApprovedDefinition(
		ctx, f.tenantID, f.userID, f.taskID)
	if err != nil {
		t.Fatalf("load applied head: %v", err)
	}
	if head.Version != applied.Version || head.Digest != applied.Digest ||
		head.Definition.Strictness != types.PushStrictness("loose") {
		t.Fatalf("applied head=%+v result=%+v", head, applied)
	}
	if head.ApprovalRef != taskDefinitionBaselineApprovalRef(
		TaskDefinitionBaselineCursor{
			TenantID: f.tenantID, UserID: f.userID, TaskID: f.taskID,
		}) {
		t.Fatalf("approval_ref=%q", head.ApprovalRef)
	}

	replay := taskDefinitionBaselineResultForTask(
		t, f.store, TaskDefinitionBaselineApply, f.taskID)
	verified := taskDefinitionBaselineResultForTask(
		t, f.store, TaskDefinitionBaselineVerify, f.taskID)
	for name, got := range map[string]TaskDefinitionBaselineResult{
		"response-loss replay": replay,
		"verify":               verified,
	} {
		if got.Status != TaskDefinitionBaselineVerified ||
			got.Version != applied.Version || got.Digest != applied.Digest {
			t.Fatalf("%s=%+v applied=%+v", name, got, applied)
		}
	}
	var versions int
	if err := f.store.pool.QueryRow(ctx,
		`SELECT count(*) FROM task_approved_definition_versions
		  WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3`,
		f.tenantID, f.userID, f.taskID,
	).Scan(&versions); err != nil {
		t.Fatal(err)
	}
	if versions != 1 {
		t.Fatalf("response-loss replay wrote %d versions", versions)
	}

	target := f.definition
	target.Intent = "后续用户确认的完整新意图"
	target.PlaybookContent = target.Intent
	target.NLDescription = "后续用户确认的新描述"
	advanced, err := f.store.CommitApprovedDefinitionEdit(
		ctx, ApprovedDefinitionEditParams{
			ExpectedHead: ApprovedDefinitionFence{
				Version: applied.Version,
				Digest:  applied.Digest,
			},
			Definition:  target,
			ApprovalRef: "baseline-late-edit:" + f.taskID,
		})
	if err != nil {
		t.Fatalf("advance head after baseline: %v", err)
	}
	if advanced.Version != 2 {
		t.Fatalf("advanced version=%d", advanced.Version)
	}
	late, err := f.store.InsertInitialApprovedDefinition(
		ctx, f.definition, taskDefinitionBaselineApprovalRef(
			TaskDefinitionBaselineCursor{
				TenantID: f.tenantID, UserID: f.userID, TaskID: f.taskID,
			}))
	if err != nil || late.Version != applied.Version ||
		late.Digest != applied.Digest {
		t.Fatalf("late baseline replay=%+v err=%v", late, err)
	}
}

func TestTaskDefinitionBaselineUsesRetainedProjectionWithoutRewritingSources(t *testing.T) {
	f := newTaskDefinitionStateFixture(t)
	const changedGlobalTitle = "mutable global source title"
	if _, err := f.store.pool.Exec(t.Context(),
		`UPDATE sources SET title=$2 WHERE id=$1`,
		f.sourceID, changedGlobalTitle); err != nil {
		t.Fatal(err)
	}

	applied := taskDefinitionBaselineResultForTask(
		t, f.store, TaskDefinitionBaselineApply, f.taskID)
	if applied.Status != TaskDefinitionBaselineApplied {
		t.Fatalf("apply=%+v", applied)
	}
	head, err := f.store.GetCurrentApprovedDefinition(
		t.Context(), f.tenantID, f.userID, f.taskID)
	if err != nil {
		t.Fatal(err)
	}
	if len(head.Definition.Sources) != 1 ||
		head.Definition.Sources[0].Title != f.definition.Sources[0].Title {
		t.Fatalf("baseline adopted mutable global metadata: %+v", head.Definition.Sources)
	}
	var title string
	if err := f.store.pool.QueryRow(t.Context(),
		`SELECT title FROM sources WHERE id=$1`, f.sourceID).Scan(&title); err != nil {
		t.Fatal(err)
	}
	if title != changedGlobalTitle {
		t.Fatalf("baseline rewrote global source title to %q", title)
	}
}

func TestTaskDefinitionBaselineUnsupportedReasons(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(t *testing.T, f taskDefinitionStateFixture)
		reason string
	}{
		{
			name: "missing playbook",
			mutate: func(t *testing.T, f taskDefinitionStateFixture) {
				if _, err := f.store.pool.Exec(t.Context(),
					`DELETE FROM schedule_playbooks WHERE schedule_id=$1`,
					f.taskID); err != nil {
					t.Fatal(err)
				}
			},
			reason: TaskDefinitionBaselineReasonMissingPlaybook,
		},
		{
			name: "empty playbook",
			mutate: func(t *testing.T, f taskDefinitionStateFixture) {
				if _, err := f.store.pool.Exec(t.Context(),
					`UPDATE schedule_playbooks SET content=' ' WHERE schedule_id=$1`,
					f.taskID); err != nil {
					t.Fatal(err)
				}
			},
			reason: TaskDefinitionBaselineReasonEmptyPlaybook,
		},
		{
			name: "invalid description",
			mutate: func(t *testing.T, f taskDefinitionStateFixture) {
				if _, err := f.store.pool.Exec(t.Context(),
					`UPDATE schedules SET nl_description='' WHERE id=$1`,
					f.taskID); err != nil {
					t.Fatal(err)
				}
			},
			reason: TaskDefinitionBaselineReasonDescription,
		},
		{
			name: "plan link mismatch",
			mutate: func(t *testing.T, f taskDefinitionStateFixture) {
				if _, err := f.store.pool.Exec(t.Context(),
					`DELETE FROM schedule_sources WHERE schedule_id=$1`,
					f.taskID); err != nil {
					t.Fatal(err)
				}
			},
			reason: TaskDefinitionBaselineReasonProjection,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newTaskDefinitionStateFixture(t)
			tt.mutate(t, f)
			got := taskDefinitionBaselineResultForTask(
				t, f.store, TaskDefinitionBaselineApply, f.taskID)
			if got.Status != TaskDefinitionBaselineUnsupported ||
				got.Reason != tt.reason || got.Version != 0 || got.Digest != "" {
				t.Fatalf("unsupported result=%+v want reason=%q", got, tt.reason)
			}
			if _, err := f.store.GetCurrentApprovedDefinition(
				t.Context(), f.tenantID, f.userID, f.taskID); !errors.Is(err, types.ErrNotFound) {
				t.Fatalf("unsupported task gained a head: %v", err)
			}
		})
	}
}

func TestTaskDefinitionBaselineConcurrentApplyHasOneVersion(t *testing.T) {
	f := newTaskDefinitionStateFixture(t)
	st2, err := New(t.Context(), os.Getenv("DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st2.Close)

	start := make(chan struct{})
	results := make(chan TaskDefinitionBaselineResult, 2)
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for _, st := range []*Store{f.store, st2} {
		wg.Add(1)
		go func(st *Store) {
			defer wg.Done()
			<-start
			got, err := taskDefinitionBaselineResultForTaskE(
				t.Context(), st, TaskDefinitionBaselineApply, f.taskID)
			if err != nil {
				errs <- err
				return
			}
			results <- got
		}(st)
	}
	close(start)
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent apply: %v", err)
	}
	var applied, verified int
	for result := range results {
		switch result.Status {
		case TaskDefinitionBaselineApplied:
			applied++
		case TaskDefinitionBaselineVerified:
			verified++
		default:
			t.Fatalf("concurrent result=%+v", result)
		}
	}
	if applied != 1 || verified != 1 {
		t.Fatalf("concurrent applied/verified=%d/%d", applied, verified)
	}
	var versions int
	if err := f.store.pool.QueryRow(t.Context(),
		`SELECT count(*) FROM task_approved_definition_versions
		  WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3`,
		f.tenantID, f.userID, f.taskID,
	).Scan(&versions); err != nil {
		t.Fatal(err)
	}
	if versions != 1 {
		t.Fatalf("concurrent apply wrote %d versions", versions)
	}
}

func TestTaskDefinitionBaselineDeleteWinsAfterDiscovery(t *testing.T) {
	f := newTaskDefinitionStateFixture(t)
	scope := TaskDefinitionBaselineCursor{
		TenantID: f.tenantID, UserID: f.userID, TaskID: f.taskID,
	}
	if err := f.store.DeleteSchedule(
		t.Context(), f.taskID, f.userID); err != nil {
		t.Fatal(err)
	}
	result, err := f.store.reconcileTaskDefinitionBaseline(
		t.Context(), TaskDefinitionBaselineApply, scope)
	if err != nil {
		t.Fatalf("deleted task reconcile: %v", err)
	}
	if result.Status != TaskDefinitionBaselineDeleted ||
		result.Reason != TaskDefinitionBaselineReasonDeleted {
		t.Fatalf("deleted task result=%+v", result)
	}
	var versions int
	if err := f.store.pool.QueryRow(t.Context(),
		`SELECT count(*) FROM task_approved_definition_versions
		  WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3`,
		f.tenantID, f.userID, f.taskID,
	).Scan(&versions); err != nil {
		t.Fatal(err)
	}
	if versions != 0 {
		t.Fatalf("deleted task retained %d versions", versions)
	}
}

func TestTaskDefinitionBaselineIncludesSuspendedTenant(t *testing.T) {
	f := newTaskDefinitionStateFixture(t)
	if _, err := f.store.pool.Exec(t.Context(),
		`UPDATE tenants SET status=$2 WHERE id=$1`,
		f.tenantID, types.TenantStatusSuspended); err != nil {
		t.Fatal(err)
	}
	result := taskDefinitionBaselineResultForTask(
		t, f.store, TaskDefinitionBaselineApply, f.taskID)
	if result.Status != TaskDefinitionBaselineApplied {
		t.Fatalf("suspended tenant baseline=%+v", result)
	}
}

func TestTaskDefinitionBaselineRejectsProvisioningTask(t *testing.T) {
	f := newTaskDefinitionStateFixture(t)
	operationID := uuid.NewString()
	if _, err := f.store.pool.Exec(t.Context(), `
		INSERT INTO pending_actions (
			id, tenant_id, user_id, tool_name, args, summary, status, expires_at,
			execution_version, phase, task_id
		) VALUES (
			$1, $2, $3, 'create_schedule', '{}', 'provisioning baseline guard',
			'pending', clock_timestamp() + interval '1 hour', 1, 'prepared', $4
		)`,
		operationID, f.tenantID, f.userID, f.taskID,
	); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(ctx, t, f.store,
			`DELETE FROM pending_actions WHERE id=$1`, operationID)
	})
	result, err := f.store.reconcileTaskDefinitionBaseline(
		t.Context(), TaskDefinitionBaselineApply, TaskDefinitionBaselineCursor{
			TenantID: f.tenantID, UserID: f.userID, TaskID: f.taskID,
		})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != TaskDefinitionBaselineUnsupported ||
		result.Reason != TaskDefinitionBaselineReasonNotMature {
		t.Fatalf("provisioning baseline=%+v", result)
	}
	if _, err := f.store.GetCurrentApprovedDefinition(
		t.Context(), f.tenantID, f.userID, f.taskID); !errors.Is(err, types.ErrNotFound) {
		t.Fatalf("provisioning task gained a head: %v", err)
	}
}

func TestTaskDefinitionBaselineKeysetPagination(t *testing.T) {
	first := newTaskDefinitionStateFixture(t)
	second := newTaskDefinitionStateFixture(t)
	st := first.store
	cursor := TaskDefinitionBaselineCursor{}
	seen := make(map[TaskDefinitionBaselineCursor]struct{})
	found := map[string]bool{first.taskID: false, second.taskID: false}
	for pageNumber := 0; pageNumber < 10000; pageNumber++ {
		scopes, more, err := st.listTaskDefinitionBaselineScopes(
			t.Context(), cursor, 1)
		if err != nil {
			t.Fatal(err)
		}
		if len(scopes) == 0 {
			if more {
				t.Fatal("empty keyset page reported more rows")
			}
			break
		}
		scope := scopes[0]
		if _, duplicate := seen[scope]; duplicate {
			t.Fatalf("duplicate keyset scope=%+v", scope)
		}
		seen[scope] = struct{}{}
		if _, tracked := found[scope.TaskID]; tracked {
			found[scope.TaskID] = true
		}
		cursor = scope
		if !more {
			break
		}
	}
	if !found[first.taskID] || !found[second.taskID] {
		t.Fatalf("pagination missed fixtures: %+v", found)
	}
}

func taskDefinitionBaselineResultForTask(
	t *testing.T,
	st *Store,
	mode TaskDefinitionBaselineMode,
	taskID string,
) TaskDefinitionBaselineResult {
	t.Helper()
	result, err := taskDefinitionBaselineResultForTaskE(
		t.Context(), st, mode, taskID)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func taskDefinitionBaselineResultForTaskE(
	ctx context.Context,
	st *Store,
	mode TaskDefinitionBaselineMode,
	taskID string,
) (TaskDefinitionBaselineResult, error) {
	cursor := TaskDefinitionBaselineCursor{}
	for pageNumber := 0; pageNumber < 10000; pageNumber++ {
		page, err := st.ReconcileTaskDefinitionBaselines(ctx, mode, cursor, 1000)
		if err != nil {
			return TaskDefinitionBaselineResult{}, err
		}
		for _, item := range page.Items {
			if item.TaskID == taskID {
				return item, nil
			}
		}
		if page.Next == nil {
			break
		}
		cursor = *page.Next
	}
	return TaskDefinitionBaselineResult{}, errors.New("baseline test task was not listed")
}

func loadTaskDefinitionBaselineLegacyBytes(
	t *testing.T,
	f taskDefinitionStateFixture,
) []byte {
	t.Helper()
	var payload []byte
	if err := f.store.pool.QueryRow(t.Context(), `
		SELECT convert_to(
			s.nl_description || '|' || s.spec_json::text || '|' ||
			s.scope_json::text || '|' || coalesce(s.push_strictness, '<NULL>') || '|' ||
			p.content || '|' || p.fetch_plan::text || '|' ||
			coalesce((
				SELECT string_agg(source_id::text, ',' ORDER BY source_id)
				  FROM schedule_sources
				 WHERE schedule_id=s.id
			), ''),
			'UTF8')
		  FROM schedules s
		  JOIN schedule_playbooks p ON p.schedule_id=s.id
		 WHERE s.tenant_id=$1 AND s.user_id=$2 AND s.id=$3`,
		f.tenantID, f.userID, f.taskID,
	).Scan(&payload); err != nil {
		t.Fatal(err)
	}
	return bytes.Clone(payload)
}
