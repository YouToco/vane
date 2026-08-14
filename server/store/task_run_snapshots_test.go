package store

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/types"
)

type taskRunSnapshotFixture struct {
	st        *Store
	tenantID  int64
	userID    int64
	urlPrefix string
}

func newTaskRunSnapshotFixture(t *testing.T) *taskRunSnapshotFixture {
	t.Helper()
	st := tenantTestStore(t)
	ctx := t.Context()
	userID := testUser(t, st)
	var tenantID int64
	if err := st.pool.QueryRow(ctx,
		`INSERT INTO tenants (status, plan) VALUES ('active', 'free') RETURNING id`,
	).Scan(&tenantID); err != nil {
		t.Fatalf("创建 run snapshot 测试租户: %v", err)
	}
	if _, err := st.pool.Exec(ctx,
		`INSERT INTO memberships (tenant_id, user_id, role) VALUES ($1, $2, 'owner')`,
		tenantID, userID); err != nil {
		t.Fatalf("创建 run snapshot 测试成员关系: %v", err)
	}
	f := &taskRunSnapshotFixture{
		st: st, tenantID: tenantID, userID: userID,
		urlPrefix: "https://snapshot.test/" + uuid.NewString(),
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(cleanupCtx, t, st,
			`DELETE FROM task_run_snapshots WHERE tenant_id = $1`, tenantID)
		cleanupExec(cleanupCtx, t, st,
			`DELETE FROM schedules WHERE tenant_id = $1`, tenantID)
		cleanupExec(cleanupCtx, t, st,
			`DELETE FROM memberships WHERE tenant_id = $1`, tenantID)
		cleanupExec(cleanupCtx, t, st,
			`DELETE FROM tenant_quota WHERE tenant_id = $1`, tenantID)
		cleanupExec(cleanupCtx, t, st,
			`DELETE FROM fetch_targets WHERE url LIKE $1`, f.urlPrefix+"%")
		cleanupExec(cleanupCtx, t, st,
			`DELETE FROM users WHERE id = $1`, userID)
		cleanupExec(cleanupCtx, t, st,
			`DELETE FROM tenants WHERE id = $1`, tenantID)
	})
	return f
}

func (f *taskRunSnapshotFixture) taskID() string {
	return "push-snapshot-" + uuid.NewString()
}

func (f *taskRunSnapshotFixture) params(taskID, runID string) CreateOrGetTaskRunSnapshotParams {
	return CreateOrGetTaskRunSnapshotParams{
		TenantID: f.tenantID, UserID: f.userID, TaskID: taskID,
		TemporalWorkflowID:    "workflow-" + taskID,
		TemporalRunID:         runID,
		Mode:                  types.ExecutionModeCompiled,
		AdaptiveVersion:       0,
		CapabilityCatalogJSON: json.RawMessage(`{"capabilities":["web/feed"]}`),
		ToolPolicyJSON:        json.RawMessage(`{"allow":["fetch"]}`),
		PromptPolicyJSON:      json.RawMessage(`{"score":"prompt-v2"}`),
		ModelPolicyJSON:       json.RawMessage(`{"provider":"deepseek","model":"v3.2"}`),
		QuotaPolicyJSON:       json.RawMessage(`{"bucket":"fetch","limit":100}`),
		BudgetJSON: json.RawMessage(`{
			"max_planner_rounds":0,"max_tool_calls":0,"max_tokens":0,
			"max_cost_micro_usd":0,"duration_ms":0
		}`),
	}
}

func (f *taskRunSnapshotFixture) createApprovedTask(t *testing.T, taskID string, count int) []int64 {
	t.Helper()
	ctx := t.Context()
	sourceIDs := make([]int64, 0, count)
	planned := make([]compiledPlanTarget, 0, count)
	for i := range count {
		sourceURL := fmt.Sprintf("%s/approved/%s/%d", f.urlPrefix, taskID, i)
		config := json.RawMessage(fmt.Sprintf(`{"query":"topic-%d","num_results":%d}`, i, i+3))
		var sourceID int64
		if err := f.st.pool.QueryRow(ctx,
			`INSERT INTO fetch_targets (platform, capability, url, title, config, status)
			 VALUES ('web', 'search', $1, $2, $3, 'active') RETURNING id`,
			sourceURL, fmt.Sprintf("approved %d", i), config,
		).Scan(&sourceID); err != nil {
			t.Fatalf("创建 approved source: %v", err)
		}
		sourceIDs = append(sourceIDs, sourceID)
		planned = append(planned, compiledPlanTarget{
			Platform: "web", Capability: "search", Title: fmt.Sprintf("approved %d", i),
			URL: sourceURL, Config: config,
		})
	}
	plan, err := json.Marshal(compiledFetchPlan{Targets: planned})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.st.pool.Exec(ctx,
		`INSERT INTO schedules
			(id, tenant_id, user_id, nl_description, spec_json, scope_json, status, push_strictness)
		 VALUES ($1, $2, $3, 'monitor approved sources',
		         '{"cron":"0 8 * * *","tz":"Asia/Shanghai"}',
		         '{"max_items":5}', 'active', 'normal')`,
		taskID, f.tenantID, f.userID); err != nil {
		t.Fatalf("创建 approved schedule: %v", err)
	}
	if _, err := f.st.pool.Exec(ctx,
		`INSERT INTO schedule_playbooks (schedule_id, content, fetch_plan)
		 VALUES ($1, 'only trusted sources', $2)`, taskID, plan); err != nil {
		t.Fatalf("创建 approved playbook: %v", err)
	}
	for _, sourceID := range sourceIDs {
		if _, err := f.st.pool.Exec(ctx,
			`INSERT INTO task_fetch_targets (schedule_id, fetch_target_id) VALUES ($1, $2)`,
			taskID, sourceID); err != nil {
			t.Fatalf("创建 approved source link: %v", err)
		}
	}
	return sourceIDs
}

func TestCreateOrGetTaskRunSnapshot_CreateRetryAndNewRun(t *testing.T) {
	f := newTaskRunSnapshotFixture(t)
	taskID := f.taskID()
	sourceIDs := f.createApprovedTask(t, taskID, 2)
	firstParams := f.params(taskID, "run-"+uuid.NewString())

	first, err := f.st.createOrGetTaskRunSnapshot(t.Context(), firstParams)
	if err != nil {
		t.Fatalf("首次创建 snapshot: %v", err)
	}
	if first.ID <= 0 || first.Mode != types.ExecutionModeCompiled ||
		!validSHA256Digest(first.DefinitionDigest) ||
		!validSHA256Digest(first.PlanDigest) || !validSHA256Digest(first.PayloadDigest) ||
		!validSHA256Digest(first.ReferenceDigest) {
		t.Fatalf("首次 snapshot 字段不完整: id=%d mode=%q", first.ID, first.Mode)
	}
	safeRef, err := first.safeRef()
	if err != nil {
		t.Fatalf("safe ref projection failed: %v", err)
	}
	expectedIdentity := types.RunIdentity{
		TemporalWorkflowID: firstParams.TemporalWorkflowID,
		TemporalRunID:      firstParams.TemporalRunID,
		RunKind:            types.RunSnapshotKindScheduled,
		TenantID:           firstParams.TenantID,
		UserID:             firstParams.UserID,
		TaskID:             firstParams.TaskID,
	}
	if err := safeRef.ValidateFor(expectedIdentity); err != nil || safeRef.SnapshotID != first.ID {
		t.Fatalf("safe ref 未绑定完整 identity/row: ref=%+v err=%v", safeRef, err)
	}
	safeWire, err := json.Marshal(safeRef)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{
		"only trusted sources", f.urlPrefix, "deepseek", "prompt-v2", "lookback_days",
	} {
		if bytes.Contains(safeWire, []byte(forbidden)) {
			t.Fatalf("safe ref 泄露 private payload canary %q", forbidden)
		}
	}
	var payload taskRunSnapshotPayload
	if err := json.Unmarshal(first.Payload, &payload); err != nil {
		t.Fatalf("解析 snapshot payload: %v", err)
	}
	if payload.Definition.SourceScope != taskRunSourceScopeApproved ||
		len(payload.Definition.Sources) != 2 {
		t.Fatalf("approved payload source scope 不符: %+v", payload.Definition)
	}
	firstPayload := bytes.Clone(first.Payload)

	// Live definition and source tuning change after the first commit. Even an
	// otherwise-invalid live policy input must be ignored for the same RunID.
	if _, err := f.st.pool.Exec(t.Context(),
		`UPDATE schedules SET nl_description = 'edited live definition',
		 spec_json = '{"cron":"0 9 * * *","tz":"UTC"}' WHERE id = $1`, taskID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.st.pool.Exec(t.Context(),
		`UPDATE schedule_playbooks
		    SET content = 'edited live playbook',
		        fetch_plan = jsonb_set(
		            fetch_plan, '{targets,0,config,lookback_days}', '30'::jsonb, true)
		  WHERE schedule_id = $1`,
		taskID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.st.pool.Exec(t.Context(),
		`UPDATE fetch_targets SET config = config || '{"lookback_days":30}'::jsonb WHERE id = $1`,
		sourceIDs[0]); err != nil {
		t.Fatal(err)
	}
	retryParams := firstParams
	retryParams.Mode = types.ExecutionModeDiscoverAtRun
	retryParams.AdaptiveVersion = -999
	retryParams.CapabilityCatalogJSON = json.RawMessage(`not-json`)
	retryParams.ToolPolicyJSON = nil
	retryParams.PromptPolicyJSON = json.RawMessage(`null`)
	retryParams.ModelPolicyJSON = json.RawMessage(`{"provider":"new-deployment"}`)
	retryParams.QuotaPolicyJSON = json.RawMessage(`[]`)
	retryParams.BudgetJSON = json.RawMessage(`not-json`)
	retry, err := f.st.createOrGetTaskRunSnapshot(t.Context(), retryParams)
	if err != nil {
		t.Fatalf("相同 RunID 重试应复用已提交快照: %v", err)
	}
	if retry.ID != first.ID || !bytes.Equal(retry.Payload, firstPayload) ||
		retry.ModelPolicyDigest != first.ModelPolicyDigest {
		t.Fatalf("重试未逐字复用 first writer: first=%d retry=%d", first.ID, retry.ID)
	}
	identityMismatch := firstParams
	identityMismatch.TemporalWorkflowID = "different-workflow-" + uuid.NewString()
	if _, err := f.st.createOrGetTaskRunSnapshot(
		t.Context(), identityMismatch); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("相同 scope/run 的 workflow identity 漂移应 Conflict，实际 %v", err)
	}

	secondParams := f.params(taskID, "run-"+uuid.NewString())
	second, err := f.st.createOrGetTaskRunSnapshot(t.Context(), secondParams)
	if err != nil {
		t.Fatalf("新 RunID 应读取最新 live definition: %v", err)
	}
	if second.ID == first.ID || second.DefinitionDigest == first.DefinitionDigest ||
		second.PlanDigest == first.PlanDigest || second.PayloadDigest == first.PayloadDigest {
		t.Fatalf("新 run 未观察定义/执行源变化: first_id=%d second_id=%d", first.ID, second.ID)
	}

	// task_id has no FK: deleting the live task keeps audit and a response-lost
	// retry still returns the committed row without consulting the deleted task.
	if _, err := f.st.pool.Exec(t.Context(), `DELETE FROM schedules WHERE id = $1`, taskID); err != nil {
		t.Fatal(err)
	}
	deletedRetry, err := f.st.createOrGetTaskRunSnapshot(t.Context(), firstParams)
	if err != nil || deletedRetry.ID != first.ID {
		var gotID int64
		if deletedRetry != nil {
			gotID = deletedRetry.ID
		}
		t.Fatalf("删除任务后的相同 RunID 应复用审计行: id=%d err=%v", gotID, err)
	}
}

func TestCreateOrGetTaskRunSnapshot_PolicyBodiesAreIndependentlyFrozen(t *testing.T) {
	f := newTaskRunSnapshotFixture(t)
	taskID := f.taskID()
	f.createApprovedTask(t, taskID, 1)

	baseline, err := f.st.createOrGetTaskRunSnapshot(
		t.Context(), f.params(taskID, "run-"+uuid.NewString()))
	if err != nil {
		t.Fatal(err)
	}
	baselineDigests := []string{
		baseline.CapabilityCatalogDigest,
		baseline.ToolPolicyDigest,
		baseline.PromptPolicyDigest,
		baseline.ModelPolicyDigest,
		baseline.QuotaPolicyDigest,
	}
	tests := []struct {
		name   string
		index  int
		mutate func(*CreateOrGetTaskRunSnapshotParams)
	}{
		{"capability catalog", 0, func(p *CreateOrGetTaskRunSnapshotParams) {
			p.CapabilityCatalogJSON = json.RawMessage(`{"capabilities":["web/feed","web/search"]}`)
		}},
		{"tool policy", 1, func(p *CreateOrGetTaskRunSnapshotParams) {
			p.ToolPolicyJSON = json.RawMessage(`{"allow":["fetch","read_page"]}`)
		}},
		{"prompt policy", 2, func(p *CreateOrGetTaskRunSnapshotParams) {
			p.PromptPolicyJSON = json.RawMessage(`{"score":"prompt-v3"}`)
		}},
		{"model policy", 3, func(p *CreateOrGetTaskRunSnapshotParams) {
			p.ModelPolicyJSON = json.RawMessage(`{"model":"v4","provider":"test"}`)
		}},
		{"quota policy", 4, func(p *CreateOrGetTaskRunSnapshotParams) {
			p.QuotaPolicyJSON = json.RawMessage(`{"bucket":"fetch","limit":101}`)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			params := f.params(taskID, "run-"+uuid.NewString())
			test.mutate(&params)
			got, err := f.st.createOrGetTaskRunSnapshot(t.Context(), params)
			if err != nil {
				t.Fatal(err)
			}
			gotDigests := []string{
				got.CapabilityCatalogDigest,
				got.ToolPolicyDigest,
				got.PromptPolicyDigest,
				got.ModelPolicyDigest,
				got.QuotaPolicyDigest,
			}
			for i := range gotDigests {
				changed := gotDigests[i] != baselineDigests[i]
				if changed != (i == test.index) {
					t.Fatalf("policy digest 隔离失败: index=%d changed=%v want=%v",
						i, changed, i == test.index)
				}
			}
			if got.DefinitionDigest != baseline.DefinitionDigest ||
				got.PlanDigest != baseline.PlanDigest ||
				got.PayloadDigest == baseline.PayloadDigest ||
				got.ReferenceDigest == baseline.ReferenceDigest {
				t.Fatal("policy body 变化应只改变对应 policy、payload 与 reference digest")
			}
		})
	}
}

func TestCreateOrGetTaskRunSnapshot_PolicyDigestDomainsAreSeparated(t *testing.T) {
	f := newTaskRunSnapshotFixture(t)
	taskID := f.taskID()
	f.createApprovedTask(t, taskID, 1)
	params := f.params(taskID, "run-"+uuid.NewString())
	params.CapabilityCatalogJSON = json.RawMessage(`{}`)
	params.ToolPolicyJSON = json.RawMessage(`{}`)
	params.PromptPolicyJSON = json.RawMessage(`{}`)
	params.ModelPolicyJSON = json.RawMessage(`{}`)
	params.QuotaPolicyJSON = json.RawMessage(`{}`)

	got, err := f.st.createOrGetTaskRunSnapshot(t.Context(), params)
	if err != nil {
		t.Fatal(err)
	}
	digests := []string{
		got.CapabilityCatalogDigest,
		got.ToolPolicyDigest,
		got.PromptPolicyDigest,
		got.ModelPolicyDigest,
		got.QuotaPolicyDigest,
	}
	seen := make(map[string]struct{}, len(digests))
	for _, digest := range digests {
		if _, duplicate := seen[digest]; duplicate {
			t.Fatal("相同 policy body 在不同 kind 下必须由 domain separation 得到不同 digest")
		}
		seen[digest] = struct{}{}
	}
	const modelPolicyV1EmptyDigest = "64730effc8050b03880ce1096591c11ce7790ba4e446a2aebf94c080e4e9e9f7"
	if got.ModelPolicyDigest != modelPolicyV1EmptyDigest {
		t.Fatalf("model policy digest envelope/version drifted: got %s want %s",
			got.ModelPolicyDigest, modelPolicyV1EmptyDigest)
	}
}

func TestCreateOrGetTaskRunSnapshot_ExactPayloadBytesRoundTrip(t *testing.T) {
	f := newTaskRunSnapshotFixture(t)
	taskID := f.taskID()
	f.createApprovedTask(t, taskID, 1)
	params := f.params(taskID, "run-"+uuid.NewString())
	params.ModelPolicyJSON = json.RawMessage(
		`{"z":{"wide":922337203685477580812345,"exponent":1e3},"a":1}`)

	created, err := f.st.createOrGetTaskRunSnapshot(t.Context(), params)
	if err != nil {
		t.Fatal(err)
	}
	var persisted []byte
	var persistedDigest, payloadType string
	if err := f.st.pool.QueryRow(t.Context(),
		`SELECT payload, payload_digest, pg_typeof(payload)::text
		   FROM task_run_snapshots WHERE id=$1`, created.ID,
	).Scan(&persisted, &persistedDigest, &payloadType); err != nil {
		t.Fatal(err)
	}
	if payloadType != "bytea" || !bytes.Equal(persisted, created.Payload) ||
		persistedDigest != created.PayloadDigest || sha256Hex(persisted) != persistedDigest {
		t.Fatalf("BYTEA 往返必须逐字节且 digest 稳定: type=%q equal=%v",
			payloadType, bytes.Equal(persisted, created.Payload))
	}
	var payload taskRunSnapshotPayload
	if err := json.Unmarshal(persisted, &payload); err != nil {
		t.Fatal(err)
	}
	modelPolicy := payload.Policies.ModelPolicy
	if !bytes.Contains(modelPolicy, []byte(`922337203685477580812345`)) ||
		!bytes.Contains(modelPolicy, []byte(`1e3`)) ||
		bytes.Index(modelPolicy, []byte(`"a"`)) > bytes.Index(modelPolicy, []byte(`"z"`)) {
		t.Fatal("大整数、指数表示与 canonical key order 未被逐字节保留")
	}
	retry, err := f.st.createOrGetTaskRunSnapshot(t.Context(), params)
	if err != nil || !bytes.Equal(retry.Payload, persisted) || retry.PayloadDigest != persistedDigest {
		t.Fatalf("相同 RunID 重试未复用 exact payload bytes: err=%v", err)
	}
}

func TestCreateOrGetTaskRunSnapshot_ConcurrentFirstWriterWins(t *testing.T) {
	f := newTaskRunSnapshotFixture(t)
	taskID := f.taskID()
	f.createApprovedTask(t, taskID, 1)
	runID := "run-" + uuid.NewString()

	const workers = 8
	results := make([]*taskRunSnapshot, workers)
	errs := make([]error, workers)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			params := f.params(taskID, runID)
			params.ModelPolicyJSON = json.RawMessage(fmt.Sprintf(
				`{"provider":"candidate","model":"%d"}`, i))
			<-start
			results[i], errs[i] = f.st.createOrGetTaskRunSnapshot(t.Context(), params)
		}()
	}
	close(start)
	wg.Wait()

	var winner *taskRunSnapshot
	for i := range workers {
		if errs[i] != nil {
			t.Fatalf("并发 writer %d: %v", i, errs[i])
		}
		if winner == nil {
			winner = results[i]
			continue
		}
		if results[i].ID != winner.ID || !bytes.Equal(results[i].Payload, winner.Payload) ||
			results[i].ModelPolicyDigest != winner.ModelPolicyDigest {
			t.Fatalf("并发 loser 未读 winner: winner_id=%d loser_id=%d", winner.ID, results[i].ID)
		}
	}
	var count int
	if err := f.st.pool.QueryRow(t.Context(),
		`SELECT count(*) FROM task_run_snapshots
		  WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3 AND temporal_run_id=$4`,
		f.tenantID, f.userID, taskID, runID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("并发同 RunID 应恰好一行，实际 %d", count)
	}
}

func TestCreateOrGetTaskRunSnapshot_RunIDAntiMisrouting(t *testing.T) {
	a := newTaskRunSnapshotFixture(t)
	taskA := a.taskID()
	a.createApprovedTask(t, taskA, 1)
	runID := "run-" + uuid.NewString()
	baseParams := a.params(taskA, runID)
	base, err := a.st.createOrGetTaskRunSnapshot(t.Context(), baseParams)
	if err != nil {
		t.Fatal(err)
	}

	taskOther := a.taskID()
	a.createApprovedTask(t, taskOther, 1)
	otherUserID := testUser(t, a.st)
	if _, err := a.st.pool.Exec(t.Context(),
		`INSERT INTO memberships (tenant_id, user_id, role) VALUES ($1, $2, 'member')`,
		a.tenantID, otherUserID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(ctx, t, a.st,
			`DELETE FROM schedules WHERE tenant_id=$1 AND user_id=$2`, a.tenantID, otherUserID)
		cleanupExec(ctx, t, a.st,
			`DELETE FROM memberships WHERE tenant_id=$1 AND user_id=$2`, a.tenantID, otherUserID)
		cleanupExec(ctx, t, a.st, `DELETE FROM users WHERE id=$1`, otherUserID)
	})
	otherUserFixture := &taskRunSnapshotFixture{
		st: a.st, tenantID: a.tenantID, userID: otherUserID, urlPrefix: a.urlPrefix,
	}
	taskOtherUser := otherUserFixture.taskID()
	otherUserFixture.createApprovedTask(t, taskOtherUser, 1)
	b := newTaskRunSnapshotFixture(t)
	taskB := b.taskID()
	b.createApprovedTask(t, taskB, 1)

	conflicts := []struct {
		name   string
		params CreateOrGetTaskRunSnapshotParams
	}{
		{"workflow", func() CreateOrGetTaskRunSnapshotParams {
			p := baseParams
			p.TemporalWorkflowID = "other-workflow-" + uuid.NewString()
			return p
		}()},
		{"task", a.params(taskOther, runID)},
		{"user", otherUserFixture.params(taskOtherUser, runID)},
		{"tenant", b.params(taskB, runID)},
	}
	for _, test := range conflicts {
		t.Run(test.name, func(t *testing.T) {
			if _, err := a.st.createOrGetTaskRunSnapshot(
				t.Context(), test.params); !errors.Is(err, types.ErrConflict) {
				t.Fatalf("同 Temporal RunID 换 %s 必须 Conflict，实际 %v", test.name, err)
			}
		})
	}
	var rows int
	if err := a.st.pool.QueryRow(t.Context(),
		`SELECT count(*) FROM task_run_snapshots WHERE temporal_run_id=$1`, runID,
	).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Fatalf("同一 Temporal RunID 全局只能保留一行，实际 %d", rows)
	}

	newRun := baseParams
	newRun.TemporalRunID = "run-" + uuid.NewString()
	second, err := a.st.createOrGetTaskRunSnapshot(t.Context(), newRun)
	if err != nil {
		t.Fatalf("同 workflow 新 RunID 应创建新 snapshot: %v", err)
	}
	if second.ID == base.ID || second.ReferenceDigest == base.ReferenceDigest {
		t.Fatal("新 RunID 必须产生新的 row/reference identity")
	}
}

func TestCreateOrGetTaskRunSnapshot_FailsClosed(t *testing.T) {
	t.Run("cross scope", func(t *testing.T) {
		owner := newTaskRunSnapshotFixture(t)
		taskID := owner.taskID()
		owner.createApprovedTask(t, taskID, 1)
		stranger := newTaskRunSnapshotFixture(t)
		params := stranger.params(taskID, "run-"+uuid.NewString())
		if _, err := stranger.st.createOrGetTaskRunSnapshot(t.Context(), params); !errors.Is(err, types.ErrNotFound) {
			t.Fatalf("跨 scope 应统一 NotFound，实际 %v", err)
		}
	})

	t.Run("paused task", func(t *testing.T) {
		f := newTaskRunSnapshotFixture(t)
		taskID := f.taskID()
		f.createApprovedTask(t, taskID, 1)
		if _, err := f.st.pool.Exec(t.Context(),
			`UPDATE schedules SET status='paused' WHERE id=$1`, taskID); err != nil {
			t.Fatal(err)
		}
		if _, err := f.st.createOrGetTaskRunSnapshot(
			t.Context(), f.params(taskID, "run-"+uuid.NewString())); !errors.Is(err, types.ErrNotFound) {
			t.Fatalf("paused task 应 NotFound，实际 %v", err)
		}
	})

	t.Run("inactive tenant", func(t *testing.T) {
		f := newTaskRunSnapshotFixture(t)
		taskID := f.taskID()
		f.createApprovedTask(t, taskID, 1)
		if _, err := f.st.pool.Exec(t.Context(),
			`UPDATE tenants SET status='suspended' WHERE id=$1`, f.tenantID); err != nil {
			t.Fatal(err)
		}
		if _, err := f.st.createOrGetTaskRunSnapshot(
			t.Context(), f.params(taskID, "run-"+uuid.NewString())); !errors.Is(err, types.ErrNotFound) {
			t.Fatalf("inactive tenant 应 NotFound，实际 %v", err)
		}
	})

	t.Run("approved plan links mismatch", func(t *testing.T) {
		f := newTaskRunSnapshotFixture(t)
		taskID := f.taskID()
		sourceIDs := f.createApprovedTask(t, taskID, 2)
		if _, err := f.st.pool.Exec(t.Context(),
			`DELETE FROM task_fetch_targets WHERE schedule_id=$1 AND fetch_target_id=$2`,
			taskID, sourceIDs[0]); err != nil {
			t.Fatal(err)
		}
		if _, err := f.st.createOrGetTaskRunSnapshot(
			t.Context(), f.params(taskID, "run-"+uuid.NewString())); !errors.Is(err, types.ErrInternal) {
			t.Fatalf("plan-links 漂移应 integrity failure，实际 %v", err)
		}
		assertTaskRunSnapshotCount(t, f, taskID, 0)
	})

	t.Run("json null fetch plan", func(t *testing.T) {
		f := newTaskRunSnapshotFixture(t)
		taskID := f.taskID()
		f.createApprovedTask(t, taskID, 1)
		if _, err := f.st.pool.Exec(t.Context(),
			`UPDATE schedule_playbooks SET fetch_plan='null'::jsonb WHERE schedule_id=$1`,
			taskID); err != nil {
			t.Fatal(err)
		}
		if _, err := f.st.createOrGetTaskRunSnapshot(
			t.Context(), f.params(taskID, "run-"+uuid.NewString())); !errors.Is(err, types.ErrInternal) {
			t.Fatalf("JSON null 不得被当 legacy，实际 %v", err)
		}
	})

	t.Run("missing playbook with approved links", func(t *testing.T) {
		f := newTaskRunSnapshotFixture(t)
		taskID := f.taskID()
		f.createApprovedTask(t, taskID, 1)
		if _, err := f.st.pool.Exec(t.Context(),
			`DELETE FROM schedule_playbooks WHERE schedule_id=$1`, taskID); err != nil {
			t.Fatal(err)
		}
		if _, err := f.st.createOrGetTaskRunSnapshot(
			t.Context(), f.params(taskID, "run-"+uuid.NewString())); !errors.Is(err, types.ErrValidation) {
			t.Fatalf("缺 playbook 但仍有 approved links 必须 fail closed，实际 %v", err)
		}
	})

	t.Run("task without playbook fails closed", func(t *testing.T) {
		f := newTaskRunSnapshotFixture(t)
		taskID := f.taskID()
		if _, err := f.st.pool.Exec(t.Context(), `
			INSERT INTO schedules
			    (id,tenant_id,user_id,nl_description,spec_json,scope_json,status)
			VALUES($1,$2,$3,'missing plan','{"every_seconds":3600}','{}','active')`,
			taskID, f.tenantID, f.userID); err != nil {
			t.Fatal(err)
		}
		if _, err := f.st.createOrGetTaskRunSnapshot(
			t.Context(), f.params(taskID, "run-"+uuid.NewString())); !errors.Is(err, types.ErrValidation) {
			t.Fatalf("无手册/计划任务必须 fail closed，实际 %v", err)
		}
	})

	t.Run("legacy marker with approved links", func(t *testing.T) {
		f := newTaskRunSnapshotFixture(t)
		taskID := f.taskID()
		f.createApprovedTask(t, taskID, 1)
		if _, err := f.st.pool.Exec(t.Context(),
			`UPDATE schedule_playbooks SET fetch_plan='{}'::jsonb WHERE schedule_id=$1`,
			taskID); err != nil {
			t.Fatal(err)
		}
		if _, err := f.st.createOrGetTaskRunSnapshot(
			t.Context(), f.params(taskID, "run-"+uuid.NewString())); !errors.Is(err, types.ErrValidation) {
			t.Fatalf("{} + task_fetch_targets 必须 fail closed，实际 %v", err)
		}
	})

	t.Run("empty approved sources", func(t *testing.T) {
		f := newTaskRunSnapshotFixture(t)
		taskID := f.taskID()
		f.createApprovedTask(t, taskID, 1)
		if _, err := f.st.pool.Exec(t.Context(),
			`UPDATE schedule_playbooks SET fetch_plan='{"targets":[]}'::jsonb
			  WHERE schedule_id=$1`, taskID); err != nil {
			t.Fatal(err)
		}
		if _, err := f.st.createOrGetTaskRunSnapshot(
			t.Context(), f.params(taskID, "run-"+uuid.NewString())); !errors.Is(err, types.ErrInternal) {
			t.Fatalf("空 targets 必须 fail closed，实际 %v", err)
		}
	})
}

func TestCreateOrGetTaskRunSnapshot_ApprovedPlanIgnoresGlobalMetadataMutation(t *testing.T) {
	f := newTaskRunSnapshotFixture(t)
	taskID := f.taskID()
	sourceIDs := f.createApprovedTask(t, taskID, 1)
	first, err := f.st.createOrGetTaskRunSnapshot(
		t.Context(), f.params(taskID, "run-"+uuid.NewString()))
	if err != nil {
		t.Fatal(err)
	}
	var firstPayload taskRunSnapshotPayload
	if err := json.Unmarshal(first.Payload, &firstPayload); err != nil {
		t.Fatal(err)
	}
	approvedSource := firstPayload.Definition.Sources[0]

	// fetch_targets is a global materialization table. Another tenant/entry point may
	// tune the same URL, but an approved task must continue using its fetch_plan
	// fields until the owner confirms a definition change.
	if _, err := f.st.pool.Exec(t.Context(),
		`UPDATE fetch_targets
		    SET platform='unapproved-platform', capability='unapproved-capability',
		        title='unapproved title', config='{"query":"unapproved"}'::jsonb
		  WHERE id=$1`, sourceIDs[0]); err != nil {
		t.Fatal(err)
	}
	second, err := f.st.createOrGetTaskRunSnapshot(
		t.Context(), f.params(taskID, "run-"+uuid.NewString()))
	if err != nil {
		t.Fatal(err)
	}
	if second.DefinitionDigest != first.DefinitionDigest ||
		second.PlanDigest != first.PlanDigest || second.PayloadDigest != first.PayloadDigest {
		t.Fatal("未确认的 global source metadata 变化不得改变 approved execution payload")
	}
	if second.ReferenceDigest == first.ReferenceDigest {
		t.Fatal("不同 SnapshotID/RunID 的 safe reference digest 必须不同")
	}
	var secondPayload taskRunSnapshotPayload
	if err := json.Unmarshal(second.Payload, &secondPayload); err != nil {
		t.Fatal(err)
	}
	if len(secondPayload.Definition.Sources) != 1 {
		t.Fatal("approved snapshot source 数量漂移")
	}
	secondSource := secondPayload.Definition.Sources[0]
	if secondSource.SourceID != approvedSource.SourceID ||
		secondSource.Platform != approvedSource.Platform ||
		secondSource.Capability != approvedSource.Capability ||
		secondSource.Title != approvedSource.Title || secondSource.URL != approvedSource.URL ||
		!bytes.Equal(secondSource.Config, approvedSource.Config) {
		t.Fatal("approved snapshot source 必须保持 fetch_plan 的已批准字段和稳定 SourceID")
	}
}

func TestTaskRunSnapshots_RLSScopeBehavior(t *testing.T) {
	a := newTaskRunSnapshotFixture(t)
	taskA := a.taskID()
	a.createApprovedTask(t, taskA, 1)
	snapshotA, err := a.st.createOrGetTaskRunSnapshot(
		t.Context(), a.params(taskA, "run-"+uuid.NewString()))
	if err != nil {
		t.Fatal(err)
	}
	b := newTaskRunSnapshotFixture(t)
	taskB := b.taskID()
	b.createApprovedTask(t, taskB, 1)
	snapshotB, err := b.st.createOrGetTaskRunSnapshot(
		t.Context(), b.params(taskB, "run-"+uuid.NewString()))
	if err != nil {
		t.Fatal(err)
	}

	assertVisible := func(tenantID int64, want int) {
		t.Helper()
		asTenant(t, a.st, tenantID, func(tx pgx.Tx) {
			var count int
			if err := tx.QueryRow(t.Context(),
				`SELECT count(*) FROM task_run_snapshots WHERE id=$1 OR id=$2`,
				snapshotA.ID, snapshotB.ID).Scan(&count); err != nil {
				t.Fatal(err)
			}
			if count != want {
				t.Fatalf("tenant=%d snapshot visibility=%d want=%d", tenantID, count, want)
			}
		})
	}
	assertVisible(a.tenantID, 1)
	assertVisible(b.tenantID, 1)
	assertVisible(0, 0)

	insertSQL := `INSERT INTO task_run_snapshots (
			tenant_id, user_id, task_id, temporal_workflow_id, temporal_run_id,
			run_kind, execution_mode, adaptive_version,
			capability_catalog_digest, tool_policy_digest, prompt_policy_digest,
			model_policy_digest, quota_policy_digest, definition_digest, plan_digest,
			payload_digest, reference_digest, reference_schema_version, payload, budget
		 )
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
		         $11, $12, $13, $14, $15, $16, $17, $18, $19, $20)`
	insert := func(tx pgx.Tx, tenantID, userID int64, runID string) error {
		_, err := tx.Exec(t.Context(), insertSQL,
			tenantID, userID, snapshotA.TaskID, snapshotA.TemporalWorkflowID, runID,
			snapshotA.RunKind, snapshotA.Mode, snapshotA.AdaptiveVersion,
			snapshotA.CapabilityCatalogDigest, snapshotA.ToolPolicyDigest,
			snapshotA.PromptPolicyDigest, snapshotA.ModelPolicyDigest,
			snapshotA.QuotaPolicyDigest, snapshotA.DefinitionDigest, snapshotA.PlanDigest,
			snapshotA.PayloadDigest, snapshotA.ReferenceDigest,
			snapshotA.ReferenceSchemaVersion, []byte(snapshotA.Payload), snapshotA.BudgetJSON)
		return err
	}
	asTenant(t, a.st, a.tenantID, func(tx pgx.Tx) {
		if err := insert(tx, a.tenantID, a.userID, "rls-own-"+uuid.NewString()); err != nil {
			t.Fatalf("tenant A 应能 INSERT 自己的 immutable snapshot: %v", err)
		}
	})
	asTenant(t, a.st, a.tenantID, func(tx pgx.Tx) {
		if err := insert(tx, b.tenantID, b.userID, "rls-cross-"+uuid.NewString()); err == nil {
			t.Fatal("tenant A 不得 INSERT tenant B 的 snapshot")
		}
	})
	asTenant(t, a.st, 0, func(tx pgx.Tx) {
		if err := insert(tx, a.tenantID, a.userID, "rls-no-context-"+uuid.NewString()); err == nil {
			t.Fatal("无 tenant context 必须 fail closed，不能 INSERT snapshot")
		}
	})
}

func TestSortTaskRunSources_UsesGoByteOrder(t *testing.T) {
	sources := []taskRunSourceIdentity{
		{SourceID: 3, URL: "https://example.test/é"},
		{SourceID: 2, URL: "https://example.test/a"},
		{SourceID: 1, URL: "https://example.test/Z"},
	}
	sortTaskRunSources(sources)
	got := []string{sources[0].URL, sources[1].URL, sources[2].URL}
	want := []string{
		"https://example.test/Z",
		"https://example.test/a",
		"https://example.test/é",
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("source order 必须与 Go/UTF-8 byte order 一致: got=%q want=%q", got, want)
	}
}

func TestCreateOrGetTaskRunSnapshot_InvalidModesAndCorruption(t *testing.T) {
	f := newTaskRunSnapshotFixture(t)
	taskID := f.taskID()
	f.createApprovedTask(t, taskID, 1)

	tests := []struct {
		name   string
		mutate func(*CreateOrGetTaskRunSnapshotParams)
	}{
		{
			name: "unknown mode",
			mutate: func(p *CreateOrGetTaskRunSnapshotParams) {
				p.Mode = types.ExecutionModeUnknown
			},
		},
		{
			name: "discover at run not implemented",
			mutate: func(p *CreateOrGetTaskRunSnapshotParams) {
				p.Mode = types.ExecutionModeDiscoverAtRun
			},
		},
		{
			name: "compiled adaptive version not implemented",
			mutate: func(p *CreateOrGetTaskRunSnapshotParams) {
				p.AdaptiveVersion = 1
			},
		},
		{
			name: "compiled nonzero planner budget",
			mutate: func(p *CreateOrGetTaskRunSnapshotParams) {
				p.BudgetJSON = json.RawMessage(`{
					"max_planner_rounds":1,"max_tool_calls":1,"max_tokens":1,
					"max_cost_micro_usd":0,"duration_ms":1
				}`)
			},
		},
		{
			name: "policy must be object",
			mutate: func(p *CreateOrGetTaskRunSnapshotParams) {
				p.ToolPolicyJSON = json.RawMessage(`[]`)
			},
		},
		{
			name: "policy duplicate key",
			mutate: func(p *CreateOrGetTaskRunSnapshotParams) {
				p.ModelPolicyJSON = json.RawMessage(`{"model":"a","model":"b"}`)
			},
		},
		{
			name: "budget unknown field",
			mutate: func(p *CreateOrGetTaskRunSnapshotParams) {
				p.BudgetJSON = json.RawMessage(`{
					"max_planner_rounds":0,"max_tool_calls":0,"max_tokens":0,
					"max_cost_micro_usd":0,"duration_ms":0,"unbounded":true
				}`)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			params := f.params(taskID, "run-"+uuid.NewString())
			test.mutate(&params)
			if _, err := f.st.createOrGetTaskRunSnapshot(t.Context(), params); !errors.Is(err, types.ErrValidation) {
				t.Fatalf("非法模式/预算应 Validation，实际 %v", err)
			}
		})
	}
	assertTaskRunSnapshotCount(t, f, taskID, 0)

	params := f.params(taskID, "run-"+uuid.NewString())
	created, err := f.st.createOrGetTaskRunSnapshot(t.Context(), params)
	if err != nil {
		t.Fatal(err)
	}
	var corrupted taskRunSnapshotPayload
	if err := json.Unmarshal(created.Payload, &corrupted); err != nil {
		t.Fatal(err)
	}
	corrupted.Policies.ModelPolicy = json.RawMessage(`{"provider":"corrupted"}`)
	corruptedBytes, err := json.Marshal(corrupted)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.st.pool.Exec(t.Context(),
		`UPDATE task_run_snapshots SET payload=$2 WHERE id=$1`,
		created.ID, corruptedBytes); err != nil {
		t.Fatal(err)
	}
	if _, err := f.st.createOrGetTaskRunSnapshot(t.Context(), params); !errors.Is(err, types.ErrInternal) {
		t.Fatalf("payload corruption 应 fail closed，实际 %v", err)
	}
	lexicalMutation := append(bytes.Clone(created.Payload), '\n')
	if _, err := f.st.pool.Exec(t.Context(),
		`UPDATE task_run_snapshots SET payload=$2 WHERE id=$1`,
		created.ID, lexicalMutation); err != nil {
		t.Fatal(err)
	}
	if _, err := f.st.createOrGetTaskRunSnapshot(t.Context(), params); !errors.Is(err, types.ErrInternal) {
		t.Fatalf("BYTEA lexical corruption 应由 exact payload digest fail closed，实际 %v", err)
	}
	if _, err := f.st.pool.Exec(t.Context(),
		`UPDATE task_run_snapshots SET payload=$2, reference_digest=repeat('0', 64)
		  WHERE id=$1`, created.ID, []byte(created.Payload)); err != nil {
		t.Fatal(err)
	}
	if _, err := f.st.createOrGetTaskRunSnapshot(t.Context(), params); !errors.Is(err, types.ErrInternal) {
		t.Fatalf("reference digest corruption 应 fail closed，实际 %v", err)
	}
}

func TestTaskRunSnapshots_MigrationShape(t *testing.T) {
	f := newTaskRunSnapshotFixture(t)
	ctx := t.Context()

	var rls bool
	if err := f.st.pool.QueryRow(ctx,
		`SELECT relrowsecurity FROM pg_class WHERE oid='task_run_snapshots'::regclass`,
	).Scan(&rls); err != nil || !rls {
		t.Fatalf("task_run_snapshots 必须启用 RLS: rls=%v err=%v", rls, err)
	}
	rows, err := f.st.pool.Query(ctx,
		`SELECT privilege_type
		   FROM information_schema.table_privileges
		  WHERE table_schema='public' AND table_name='task_run_snapshots'
		    AND grantee='vane_app'
		  ORDER BY privilege_type`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var privileges []string
	for rows.Next() {
		var privilege string
		if err := rows.Scan(&privilege); err != nil {
			t.Fatal(err)
		}
		privileges = append(privileges, privilege)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if strings.Join(privileges, ",") != "INSERT,SELECT" {
		t.Fatalf("immutable table 对 vane_app 只应 INSERT/SELECT，实际 %v", privileges)
	}
	var currentTaskDependencyPaths int
	if err := f.st.pool.QueryRow(ctx,
		`WITH RECURSIVE referenced(relid) AS (
		    SELECT c.confrelid
		      FROM pg_constraint c
		     WHERE c.conrelid='task_run_snapshots'::regclass
		       AND c.contype='f'
		    UNION
		    SELECT c.confrelid
		      FROM pg_constraint c
		      JOIN referenced r ON r.relid=c.conrelid
		     WHERE c.contype='f'
		)
		SELECT count(*) FROM referenced
		 WHERE relid IN (
		    'schedules'::regclass,
		    'task_approved_definition_versions'::regclass
		 )`,
	).Scan(&currentTaskDependencyPaths); err != nil {
		t.Fatal(err)
	}
	if currentTaskDependencyPaths != 0 {
		t.Fatalf("task run snapshot 不得经任何 FK 路径回指 current task，实际 %d",
			currentTaskDependencyPaths)
	}
	var payloadType string
	if err := f.st.pool.QueryRow(ctx,
		`SELECT data_type FROM information_schema.columns
		  WHERE table_schema='public' AND table_name='task_run_snapshots'
		    AND column_name='payload'`,
	).Scan(&payloadType); err != nil {
		t.Fatal(err)
	}
	if payloadType != "bytea" {
		t.Fatalf("immutable exact payload 必须用 BYTEA，实际 %q", payloadType)
	}
	var payloadCheck string
	if err := f.st.pool.QueryRow(ctx,
		`SELECT pg_get_constraintdef(oid)
		   FROM pg_constraint
		  WHERE conrelid='task_run_snapshots'::regclass
		    AND conname='task_run_snapshots_payload_size_valid'`,
	).Scan(&payloadCheck); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(payloadCheck, "octet_length(payload)") ||
		!strings.Contains(payloadCheck, "2097152") {
		t.Fatalf("payload 必须有与 Store 一致的 2MiB DB 上限，实际 %q", payloadCheck)
	}
	var indexDefinition string
	if err := f.st.pool.QueryRow(ctx,
		`SELECT indexdef FROM pg_indexes
		  WHERE schemaname='public'
		    AND indexname='idx_task_run_snapshots_tenant_user_task_created'`,
	).Scan(&indexDefinition); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(indexDefinition,
		"(tenant_id, user_id, task_id, created_at DESC, id DESC)") {
		t.Fatalf("secondary index 必须以 Tenant/User/Task 起始，实际 %q", indexDefinition)
	}
}

func TestCreateOrGetTaskRunSnapshot_HasSinglePreparedProductionPath(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller 无法定位测试文件")
	}
	repoRoot := filepath.Dir(filepath.Dir(thisFile))
	fset := token.NewFileSet()
	var references []string
	var rawCalls, typedCalls, facadeCalls int
	typedAdapterPath := filepath.Join(repoRoot, "store", "task_run_snapshot_typed.go")
	rawPrimitivePath := filepath.Join(repoRoot, "store", "task_run_snapshots.go")
	prepareRunPath := filepath.Join(repoRoot, "workflow", "activities.go")
	err := filepath.WalkDir(repoRoot, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			if entry.Name() == ".git" || entry.Name() == "vendor" {
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, parseErr := parser.ParseFile(fset, path, nil, 0)
		if parseErr != nil {
			return parseErr
		}
		for _, declaration := range file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || function.Body == nil {
				continue
			}
			ast.Inspect(function.Body, func(node ast.Node) bool {
				selector, ok := node.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				allowed := false
				switch selector.Sel.Name {
				case "createOrGetTaskRunSnapshotWithShadowV2":
					allowed = filepath.Clean(path) == filepath.Clean(typedAdapterPath) &&
						function.Name.Name == "createOrGetCompiledTaskRunSnapshotV1"
					if allowed {
						rawCalls++
					} else {
						allowed = filepath.Clean(path) ==
							filepath.Clean(rawPrimitivePath) &&
							function.Name.Name == "createOrGetTaskRunSnapshot"
					}
				case "CreateOrGetCompiledTaskRunSnapshotV1":
					allowed = filepath.Clean(path) == filepath.Clean(typedAdapterPath) &&
						function.Name.Name == "CreateOrGetCompiledRunSnapshotV1"
					if allowed {
						typedCalls++
					}
				case "CreateOrGetCompiledRunSnapshotV1":
					allowed = filepath.Clean(path) == filepath.Clean(prepareRunPath) &&
						function.Name.Name == "PrepareRun"
					if allowed {
						facadeCalls++
					}
				default:
					return true
				}
				if !allowed {
					position := fset.Position(selector.Pos())
					references = append(references,
						fmt.Sprintf("%s:%d", position.Filename, position.Line))
				}
				return true
			})
		}
		return nil
	})
	if err != nil {
		t.Fatalf("扫描生产调用点: %v", err)
	}
	if len(references) != 0 {
		t.Fatalf("C1b snapshot persistence escaped the prepared typed path: %v",
			references)
	}
	if rawCalls != 1 || typedCalls != 1 || facadeCalls != 1 {
		t.Fatalf("C1b snapshot path calls raw=%d typed=%d facade=%d, want exactly 1 each",
			rawCalls, typedCalls, facadeCalls)
	}
}

func assertTaskRunSnapshotCount(
	t *testing.T,
	f *taskRunSnapshotFixture,
	taskID string,
	want int,
) {
	t.Helper()
	var count int
	if err := f.st.pool.QueryRow(t.Context(),
		`SELECT count(*) FROM task_run_snapshots
		  WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3`,
		f.tenantID, f.userID, taskID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != want {
		t.Fatalf("snapshot count=%d, want %d", count, want)
	}
}
