package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/YouToco/vane/types"
)

type compiledTaskFixture struct {
	st       *Store
	tenantID int64
	userID   int64
	urlRoot  string
}

func newCompiledTaskFixture(t *testing.T, st *Store) *compiledTaskFixture {
	t.Helper()
	ctx := t.Context()
	userID := testUser(t, st)
	var tenantID int64
	if err := st.pool.QueryRow(ctx,
		`INSERT INTO tenants (status, plan) VALUES ('active', 'free') RETURNING id`,
	).Scan(&tenantID); err != nil {
		t.Fatalf("创建 A2 测试租户: %v", err)
	}
	if _, err := st.pool.Exec(ctx,
		`INSERT INTO memberships (tenant_id, user_id, role) VALUES ($1, $2, 'owner')`,
		tenantID, userID); err != nil {
		t.Fatalf("创建 A2 测试成员关系: %v", err)
	}

	f := &compiledTaskFixture{
		st:       st,
		tenantID: tenantID,
		userID:   userID,
		urlRoot:  "vane://a2-test/" + uuid.NewString(),
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(cleanupCtx, t, st,
			`DELETE FROM task_run_snapshots WHERE tenant_id = $1`, tenantID)
		cleanupExec(cleanupCtx, t, st, `DELETE FROM schedules WHERE tenant_id = $1`, tenantID)
		cleanupExec(cleanupCtx, t, st, `DELETE FROM memberships WHERE tenant_id = $1`, tenantID)
		cleanupExec(cleanupCtx, t, st, `DELETE FROM fetch_targets WHERE url LIKE $1`, f.urlRoot+"%")
		cleanupExec(cleanupCtx, t, st, `DELETE FROM users WHERE id = $1`, userID)
		cleanupExec(cleanupCtx, t, st, `DELETE FROM tenants WHERE id = $1`, tenantID)
	})
	return f
}

func (f *compiledTaskFixture) taskID() string {
	return "push-a2-" + uuid.NewString()
}

func (f *compiledTaskFixture) url(suffix string) string {
	return f.urlRoot + "/" + suffix + "/" + uuid.NewString()
}

func (f *compiledTaskFixture) definition(
	taskID string,
	strictness types.PushStrictness,
	urls ...string,
) types.PausedCompiledTaskDefinition {
	sources := make([]compiledPlanTarget, 0, len(urls))
	for i, sourceURL := range urls {
		sources = append(sources, compiledPlanTarget{
			Platform:   "web",
			Capability: "search",
			Title:      fmt.Sprintf("计划源 %d", i+1),
			URL:        sourceURL,
			Config:     json.RawMessage(fmt.Sprintf(`{"query":"topic-%d"}`, i+1)),
		})
	}
	plan, err := json.Marshal(compiledFetchPlan{Targets: sources})
	if err != nil {
		panic(err)
	}
	return types.PausedCompiledTaskDefinition{
		TaskID:          taskID,
		TenantID:        f.tenantID,
		UserID:          f.userID,
		NLDescription:   "每天早上监控指定主题",
		SpecJSON:        json.RawMessage(`{"cron":"0 8 * * *","tz":"Asia/Shanghai"}`),
		ScopeJSON:       json.RawMessage(`{"max_items":5}`),
		PlaybookContent: "只看官方与高可信来源。",
		FetchPlan:       plan,
		Strictness:      strictness,
	}
}

func TestValidatePausedCompiledTaskDefinition(t *testing.T) {
	valid := types.PausedCompiledTaskDefinition{
		TaskID:          "push-a2-valid",
		TenantID:        7,
		UserID:          9,
		SpecJSON:        json.RawMessage(`{}`),
		ScopeJSON:       json.RawMessage(`{}`),
		PlaybookContent: "x",
		FetchPlan: json.RawMessage(`{"targets":[{
			"platform":"web","capability":"search","url":"vane://web/search?q=ai"
		}]}`),
		Strictness: "",
	}

	plan, err := validatePausedCompiledTaskDefinition(valid)
	if err != nil {
		t.Fatalf("合法定义被拒绝: %v", err)
	}
	if len(plan.Targets) != 1 || string(plan.Targets[0].Config) != "{}" {
		t.Fatalf("省略 config 应归一为 {}，实得 %+v", plan.Targets)
	}

	cases := []struct {
		name   string
		mutate func(*types.PausedCompiledTaskDefinition)
	}{
		{"空 task id", func(d *types.PausedCompiledTaskDefinition) { d.TaskID = "" }},
		{"task id 首尾空白", func(d *types.PausedCompiledTaskDefinition) { d.TaskID = " push-a2 " }},
		{"task id 过长", func(d *types.PausedCompiledTaskDefinition) {
			d.TaskID = strings.Repeat("x", 256)
		}},
		{"tenant 非正数", func(d *types.PausedCompiledTaskDefinition) { d.TenantID = 0 }},
		{"user 非正数", func(d *types.PausedCompiledTaskDefinition) { d.UserID = -1 }},
		{"spec 缺失", func(d *types.PausedCompiledTaskDefinition) { d.SpecJSON = nil }},
		{"spec null", func(d *types.PausedCompiledTaskDefinition) { d.SpecJSON = json.RawMessage(`null`) }},
		{"spec 非对象", func(d *types.PausedCompiledTaskDefinition) { d.SpecJSON = json.RawMessage(`[]`) }},
		{"spec 重复 key", func(d *types.PausedCompiledTaskDefinition) {
			d.SpecJSON = json.RawMessage(`{"cron":"a","cron":"b"}`)
		}},
		{"scope 非法 JSON", func(d *types.PausedCompiledTaskDefinition) { d.ScopeJSON = json.RawMessage(`{`) }},
		{"strictness 非法", func(d *types.PausedCompiledTaskDefinition) { d.Strictness = "extreme" }},
		{"playbook 过大", func(d *types.PausedCompiledTaskDefinition) {
			d.PlaybookContent = strings.Repeat("x", maxCompiledTaskPlaybookBytes+1)
		}},
		{"计划缺失", func(d *types.PausedCompiledTaskDefinition) { d.FetchPlan = nil }},
		{"计划 null", func(d *types.PausedCompiledTaskDefinition) { d.FetchPlan = json.RawMessage(`null`) }},
		{"计划非对象", func(d *types.PausedCompiledTaskDefinition) { d.FetchPlan = json.RawMessage(`[]`) }},
		{"sources 缺失", func(d *types.PausedCompiledTaskDefinition) { d.FetchPlan = json.RawMessage(`{}`) }},
		{"sources null", func(d *types.PausedCompiledTaskDefinition) {
			d.FetchPlan = json.RawMessage(`{"targets":null}`)
		}},
		{"sources 空", func(d *types.PausedCompiledTaskDefinition) {
			d.FetchPlan = json.RawMessage(`{"targets":[]}`)
		}},
		{"sources 过多", func(d *types.PausedCompiledTaskDefinition) {
			sources := make([]compiledPlanTarget, maxCompiledTaskTargets+1)
			for i := range sources {
				sources[i] = compiledPlanTarget{
					Platform: "web", Capability: "feed", URL: fmt.Sprintf("vane://many/%d", i),
				}
			}
			raw, err := json.Marshal(compiledFetchPlan{Targets: sources})
			if err != nil {
				panic(err)
			}
			d.FetchPlan = raw
		}},
		{"计划重复 sources key", func(d *types.PausedCompiledTaskDefinition) {
			d.FetchPlan = json.RawMessage(`{"targets":[],"targets":[]}`)
		}},
		{"URL 重复", func(d *types.PausedCompiledTaskDefinition) {
			d.FetchPlan = json.RawMessage(`{"targets":[
				{"platform":"web","capability":"search","url":"vane://same"},
				{"platform":"web","capability":"search","url":"vane://same"}
			]}`)
		}},
		{"platform 缺失", func(d *types.PausedCompiledTaskDefinition) {
			d.FetchPlan = json.RawMessage(`{"targets":[
				{"capability":"search","url":"vane://one"}
			]}`)
		}},
		{"capability 首尾空白", func(d *types.PausedCompiledTaskDefinition) {
			d.FetchPlan = json.RawMessage(`{"targets":[
				{"platform":"web","capability":" search ","url":"vane://one"}
			]}`)
		}},
		{"URL 缺失", func(d *types.PausedCompiledTaskDefinition) {
			d.FetchPlan = json.RawMessage(`{"targets":[
				{"platform":"web","capability":"search"}
			]}`)
		}},
		{"URL 首尾空白", func(d *types.PausedCompiledTaskDefinition) {
			d.FetchPlan = json.RawMessage(`{"targets":[
				{"platform":"web","capability":"search","url":" vane://one "}
			]}`)
		}},
		{"config null", func(d *types.PausedCompiledTaskDefinition) {
			d.FetchPlan = json.RawMessage(`{"targets":[
				{"platform":"web","capability":"search","url":"vane://one","config":null}
			]}`)
		}},
		{"config 非对象", func(d *types.PausedCompiledTaskDefinition) {
			d.FetchPlan = json.RawMessage(`{"targets":[
				{"platform":"web","capability":"search","url":"vane://one","config":[]}
			]}`)
		}},
		{"config 嵌套重复 key", func(d *types.PausedCompiledTaskDefinition) {
			d.FetchPlan = json.RawMessage(`{"targets":[
				{"platform":"web","capability":"search","url":"vane://one","config":{"query":"a","query":"b"}}
			]}`)
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			def := valid
			tc.mutate(&def)
			plan, err := validatePausedCompiledTaskDefinition(def)
			if err == nil {
				t.Fatalf("非法定义必须被拒绝，实得 plan=%+v", plan)
			}
			if !errors.Is(err, types.ErrValidation) {
				t.Fatalf("非法定义应返回 CodeValidation，实得 %v", err)
			}
		})
	}
}

func TestInsertPausedCompiledTaskDefinition_RejectsInvalidInputBeforeBegin(t *testing.T) {
	valid := types.PausedCompiledTaskDefinition{
		TaskID:          "push-a2-public-validation",
		TenantID:        7,
		UserID:          9,
		SpecJSON:        json.RawMessage(`{}`),
		ScopeJSON:       json.RawMessage(`{}`),
		PlaybookContent: "x",
		FetchPlan: json.RawMessage(`{"targets":[{
			"platform":"web","capability":"search","url":"vane://web/search?q=ai"
		}]}`),
		Strictness: types.StrictnessNormal,
	}
	cases := []struct {
		name   string
		mutate func(*types.PausedCompiledTaskDefinition)
	}{
		{"missing plan", func(d *types.PausedCompiledTaskDefinition) { d.FetchPlan = nil }},
		{"null plan", func(d *types.PausedCompiledTaskDefinition) {
			d.FetchPlan = json.RawMessage(`null`)
		}},
		{"missing sources", func(d *types.PausedCompiledTaskDefinition) {
			d.FetchPlan = json.RawMessage(`{}`)
		}},
		{"null sources", func(d *types.PausedCompiledTaskDefinition) {
			d.FetchPlan = json.RawMessage(`{"targets":null}`)
		}},
		{"empty sources", func(d *types.PausedCompiledTaskDefinition) {
			d.FetchPlan = json.RawMessage(`{"targets":[]}`)
		}},
		{"duplicate URL", func(d *types.PausedCompiledTaskDefinition) {
			d.FetchPlan = json.RawMessage(`{"targets":[
				{"platform":"web","capability":"search","url":"vane://same"},
				{"platform":"web","capability":"search","url":"vane://same"}
			]}`)
		}},
		{"invalid strictness", func(d *types.PausedCompiledTaskDefinition) {
			d.Strictness = "extreme"
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			beginCalls := 0
			st := &Store{
				beginTx: func(context.Context, pgx.TxOptions) (pgx.Tx, error) {
					beginCalls++
					return nil, errors.New("validation must prevent BeginTx")
				},
			}
			def := valid
			tc.mutate(&def)
			err := st.InsertPausedCompiledTaskDefinition(t.Context(), def)
			if !errors.Is(err, types.ErrValidation) {
				t.Fatalf("公共方法必须返回 CodeValidation，实得 %v", err)
			}
			if beginCalls != 0 {
				t.Fatalf("非法输入必须在事务前拒绝，BeginTx 调用了 %d 次", beginCalls)
			}
		})
	}
}

func TestInsertPausedCompiledTaskDefinition_CanonicalizesGlobalSourceLockOrder(t *testing.T) {
	st := tenantTestStore(t)
	left := newCompiledTaskFixture(t, st)
	right := newCompiledTaskFixture(t, st)
	ctx := t.Context()
	st2, err := New(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	registerStoreClose(t, st2)

	urls := make([]string, 48)
	for i := range urls {
		urls[i] = left.url(fmt.Sprintf("inverse-lock-%02d", i))
	}
	reversed := append([]string(nil), urls...)
	slices.Reverse(reversed)
	leftDef := left.definition(left.taskID(), types.StrictnessNormal, urls...)
	rightDef := right.definition(right.taskID(), types.StrictnessNormal, reversed...)
	var rightPlan compiledFetchPlan
	if err := json.Unmarshal(rightDef.FetchPlan, &rightPlan); err != nil {
		t.Fatal(err)
	}
	for i := range rightPlan.Targets {
		// The test exercises inverse lock order, not semantic conflict. Match
		// each shared URL's acquisition config while leaving display titles free.
		rightPlan.Targets[i].Config = json.RawMessage(
			fmt.Sprintf(`{"query":"topic-%d"}`, len(rightPlan.Targets)-i),
		)
	}
	rightDef.FetchPlan, err = json.Marshal(rightPlan)
	if err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	results := make(chan error, 2)
	go func() {
		<-start
		results <- st.InsertPausedCompiledTaskDefinition(ctx, leftDef)
	}()
	go func() {
		<-start
		results <- st2.InsertPausedCompiledTaskDefinition(ctx, rightDef)
	}()
	close(start)
	for range 2 {
		if err := <-results; err != nil {
			t.Fatalf("inverse source order must not deadlock: %v", err)
		}
	}
	for _, def := range []types.PausedCompiledTaskDefinition{leftDef, rightDef} {
		var stored []byte
		if err := st.pool.QueryRow(ctx,
			`SELECT fetch_plan FROM schedule_playbooks WHERE schedule_id = $1`, def.TaskID,
		).Scan(&stored); err != nil {
			t.Fatal(err)
		}
		if !taskCreationJSONEqual(stored, def.FetchPlan) {
			t.Fatalf("lock ordering must not change approved fetch_plan for %s", def.TaskID)
		}
	}
}

func TestInsertPausedCompiledTaskDefinition_WritesExactAggregate(t *testing.T) {
	st := tenantTestStore(t)
	f := newCompiledTaskFixture(t, st)
	ctx := t.Context()

	existingURL := f.url("existing")
	var existingID int64
	if err := st.pool.QueryRow(ctx,
		`INSERT INTO fetch_targets
			(platform, capability, url, title, config, status,
			 fetch_interval_seconds, next_fetch_at, last_fetched_at, fail_count)
		 VALUES ('web', 'search', $1, '原展示名',
		         '{"query":"same","lookback_days":7}', 'disabled',
		         4321, now() + interval '1 day', now() - interval '2 days', 9)
		 RETURNING id`,
		existingURL).Scan(&existingID); err != nil {
		t.Fatalf("预置全局源: %v", err)
	}
	existingBefore := sourceByID(t, st, existingID)

	newURL := f.url("new")
	def := f.definition(f.taskID(), types.StrictnessStrict, existingURL, newURL)
	// 同 URL 可携带不同展示 title；抓取语义必须一致。A2 只引用既有
	// 全局行，不能把它改成任务私有形态。
	var plan compiledFetchPlan
	if err := json.Unmarshal(def.FetchPlan, &plan); err != nil {
		t.Fatal(err)
	}
	plan.Targets[0].Title = "不得覆写"
	plan.Targets[0].Config = json.RawMessage(`{"lookback_days":7,"query":"same"}`)
	def.FetchPlan, _ = json.Marshal(plan)

	if err := st.InsertPausedCompiledTaskDefinition(ctx, def); err != nil {
		t.Fatalf("InsertPausedCompiledTaskDefinition: %v", err)
	}

	var (
		tenantID, userID int64
		desc, status     string
		spec, scope      []byte
		strictness       *string
	)
	if err := st.pool.QueryRow(ctx,
		`SELECT tenant_id, user_id, nl_description, status, spec_json, scope_json, push_strictness
		   FROM schedules WHERE id = $1`,
		def.TaskID).Scan(&tenantID, &userID, &desc, &status, &spec, &scope, &strictness); err != nil {
		t.Fatalf("回查 schedule: %v", err)
	}
	if tenantID != def.TenantID || userID != def.UserID {
		t.Errorf("显式租户/用户漂移: got tenant=%d user=%d", tenantID, userID)
	}
	if desc != def.NLDescription || status != string(types.ScheduleStatusPaused) {
		t.Errorf("paused 镜像不符: desc=%q status=%q", desc, status)
	}
	if strictness == nil || *strictness != string(types.StrictnessStrict) {
		t.Errorf("strictness 未精确写入: %v", strictness)
	}
	assertCompiledJSONEqual(t, spec, def.SpecJSON)
	assertCompiledJSONEqual(t, scope, def.ScopeJSON)

	var content string
	var storedPlan []byte
	if err := st.pool.QueryRow(ctx,
		`SELECT content, fetch_plan FROM schedule_playbooks WHERE schedule_id = $1`,
		def.TaskID).Scan(&content, &storedPlan); err != nil {
		t.Fatalf("回查 playbook: %v", err)
	}
	if content != def.PlaybookContent {
		t.Errorf("手册正文不符: %q", content)
	}
	assertCompiledJSONEqual(t, storedPlan, def.FetchPlan)

	linked := linkedSourceURLs(t, st, def.TaskID)
	wantLinked := []string{existingURL, newURL}
	if !reflect.DeepEqual(linked, wantLinked) {
		t.Errorf("计划 URL 与链接 URL 不精确相等: got=%v want=%v", linked, wantLinked)
	}

	existingAfter := sourceByID(t, st, existingID)
	if !reflect.DeepEqual(existingAfter, existingBefore) {
		t.Errorf("既有全局源任一字段都不得被改写:\nbefore=%+v\nafter=%+v",
			existingBefore, existingAfter)
	}

	newSource := sourceByURL(t, st, newURL)
	if newSource.ID == existingID {
		t.Error("不同 URL 不应引用同一 source id")
	}
	if newSource.Platform != types.PlatformWeb ||
		newSource.Capability != types.CapSearch ||
		newSource.Title != "计划源 2" ||
		newSource.Status != types.FetchTargetStatusActive ||
		newSource.FetchIntervalSeconds != 1800 ||
		newSource.FailCount != 0 ||
		newSource.LastFetchedAt != nil ||
		newSource.NextFetchAt.IsZero() ||
		newSource.CreatedAt.IsZero() ||
		newSource.UpdatedAt.IsZero() {
		t.Errorf("新源字段或数据库默认值不符: %+v", newSource)
	}
	assertCompiledJSONEqual(t, newSource.Config, []byte(`{"query":"topic-2"}`))

}

func TestInsertPausedCompiledTaskDefinition_RejectsURLWithDifferentAcquisitionSemantics(t *testing.T) {
	st := tenantTestStore(t)
	f := newCompiledTaskFixture(t, st)
	ctx := t.Context()

	targetURL := f.url("semantic-conflict")
	if _, err := st.pool.Exec(ctx,
		`INSERT INTO fetch_targets (platform, capability, url, title, config)
		 VALUES ('web', 'search', $1, 'existing', '{"query":"existing"}')`,
		targetURL,
	); err != nil {
		t.Fatal(err)
	}
	def := f.definition(f.taskID(), types.StrictnessStrict, targetURL)
	var plan compiledFetchPlan
	if err := json.Unmarshal(def.FetchPlan, &plan); err != nil {
		t.Fatal(err)
	}
	plan.Targets[0].Config = json.RawMessage(`{"query":"different"}`)
	def.FetchPlan, _ = json.Marshal(plan)

	err := st.InsertPausedCompiledTaskDefinition(ctx, def)
	if !errors.Is(err, types.ErrConflict) {
		t.Fatalf("同 URL 不同抓取语义应 fail closed: %v", err)
	}
	var count int
	if err := st.pool.QueryRow(ctx,
		`SELECT count(*) FROM schedules WHERE id=$1`, def.TaskID,
	).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatal("语义冲突后不应留下半写任务")
	}
}

func TestInsertPausedCompiledTaskDefinition_PreservesUnsetStrictnessAsNull(t *testing.T) {
	st := tenantTestStore(t)
	f := newCompiledTaskFixture(t, st)
	def := f.definition(f.taskID(), "", f.url("unset"))

	if err := st.InsertPausedCompiledTaskDefinition(t.Context(), def); err != nil {
		t.Fatalf("空 strictness 应表示未设置，不应失败: %v", err)
	}
	var isNull bool
	if err := st.pool.QueryRow(t.Context(),
		`SELECT push_strictness IS NULL FROM schedules WHERE id = $1`,
		def.TaskID).Scan(&isNull); err != nil {
		t.Fatalf("回查 strictness: %v", err)
	}
	if !isNull {
		t.Error("空 strictness 必须写 SQL NULL，不能写空串或偷换成 loose")
	}
}

func TestInsertPausedCompiledTaskDefinition_RejectsInvalidMembership(t *testing.T) {
	st := tenantTestStore(t)
	f := newCompiledTaskFixture(t, st)
	ctx := t.Context()

	var otherTenant int64
	if err := st.pool.QueryRow(ctx,
		`INSERT INTO tenants (status, plan) VALUES ('active', 'free') RETURNING id`,
	).Scan(&otherTenant); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanCtx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(cleanCtx, t, st, `DELETE FROM tenants WHERE id = $1`, otherTenant)
	})

	cases := []struct {
		name    string
		prepare func(t *testing.T)
		tenant  int64
	}{
		{"明确 tenant 与 user 不是成员", func(*testing.T) {}, otherTenant},
		{"租户 suspended", func(t *testing.T) {
			if _, err := st.pool.Exec(ctx,
				`UPDATE tenants SET status = 'suspended', deleted_at = NULL WHERE id = $1`,
				f.tenantID); err != nil {
				t.Fatal(err)
			}
		}, f.tenantID},
		{"租户已软删", func(t *testing.T) {
			if _, err := st.pool.Exec(ctx,
				`UPDATE tenants SET status = 'active', deleted_at = now() WHERE id = $1`,
				f.tenantID); err != nil {
				t.Fatal(err)
			}
		}, f.tenantID},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// 每轮先恢复，避免上一个 case 的 tenant 状态污染下一轮。
			if _, err := st.pool.Exec(ctx,
				`UPDATE tenants SET status = 'active', deleted_at = NULL WHERE id = $1`,
				f.tenantID); err != nil {
				t.Fatal(err)
			}
			tc.prepare(t)
			def := f.definition(f.taskID(), types.StrictnessNormal, f.url("invalid-member"))
			def.TenantID = tc.tenant
			err := st.InsertPausedCompiledTaskDefinition(ctx, def)
			if !errors.Is(err, types.ErrValidation) {
				t.Fatalf("无效成员关系应 CodeValidation，实得 %v", err)
			}
			assertNoCompiledTaskAggregate(t, st, def.TaskID)
			assertSourceURLCount(t, st, planURLs(t, def.FetchPlan), 0)
		})
	}
}

type compiledTaskAggregateSnapshot struct {
	ScheduleJSON string
	PlaybookJSON string
	SourceURLs   []string
}

func TestInsertPausedCompiledTaskDefinition_ConflictLeavesOriginalUntouched(t *testing.T) {
	st := tenantTestStore(t)
	f := newCompiledTaskFixture(t, st)
	ctx := t.Context()
	taskID := f.taskID()
	firstURL := f.url("first")
	first := f.definition(taskID, types.StrictnessNormal, firstURL)
	if err := st.InsertPausedCompiledTaskDefinition(ctx, first); err != nil {
		t.Fatalf("首建失败: %v", err)
	}
	before := snapshotCompiledTaskAggregate(t, st, taskID)

	secondURL := f.url("second")
	second := f.definition(taskID, types.StrictnessStrict, secondURL)
	second.NLDescription = "企图覆盖原任务"
	second.PlaybookContent = "企图覆盖原手册"
	second.SpecJSON = json.RawMessage(`{"every_seconds":60}`)
	err := st.InsertPausedCompiledTaskDefinition(ctx, second)
	if !errors.Is(err, types.ErrConflict) {
		t.Fatalf("TaskID 冲突应 CodeConflict，实得 %v", err)
	}

	after := snapshotCompiledTaskAggregate(t, st, taskID)
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("冲突必须保持原聚合逐字不变:\nbefore=%+v\nafter=%+v", before, after)
	}
	assertSourceURLCount(t, st, []string{secondURL}, 0)
}

var errInjectedCompiledTask = errors.New("injected compiled-task transaction failure")

type compiledTaskFaultTx struct {
	pgx.Tx
	failContains       string
	skipContains       string
	commitErr          error
	cancelOnCommit     context.CancelFunc
	fired              bool
	rollbackCalls      int
	rollbackContextErr error
}

func (tx *compiledTaskFaultTx) Exec(
	ctx context.Context,
	sql string,
	arguments ...any,
) (pgconn.CommandTag, error) {
	if tx.hit(sql, tx.failContains) {
		return pgconn.CommandTag{}, errInjectedCompiledTask
	}
	if tx.hit(sql, tx.skipContains) {
		return pgconn.CommandTag{}, nil
	}
	return tx.Tx.Exec(ctx, sql, arguments...)
}

func (tx *compiledTaskFaultTx) QueryRow(
	ctx context.Context,
	sql string,
	args ...any,
) pgx.Row {
	if tx.hit(sql, tx.failContains) {
		return compiledTaskErrorRow{err: errInjectedCompiledTask}
	}
	return tx.Tx.QueryRow(ctx, sql, args...)
}

func (tx *compiledTaskFaultTx) Commit(ctx context.Context) error {
	if tx.commitErr == nil {
		return tx.Tx.Commit(ctx)
	}
	if tx.cancelOnCommit != nil {
		tx.cancelOnCommit()
	}
	// 不把 COMMIT 转发给真实事务：模拟服务器在接受提交前拒绝。生产方法自己的
	// deferred Rollback 必须负责清理；若测试包装器先 Rollback，会形成自证式假绿。
	return tx.commitErr
}

func (tx *compiledTaskFaultTx) Rollback(ctx context.Context) error {
	tx.rollbackCalls++
	tx.rollbackContextErr = ctx.Err()
	return tx.Tx.Rollback(ctx)
}

func (tx *compiledTaskFaultTx) hit(sql, needle string) bool {
	if tx.fired || needle == "" || !strings.Contains(sql, needle) {
		return false
	}
	tx.fired = true
	return true
}

type compiledTaskErrorRow struct{ err error }

func (row compiledTaskErrorRow) Scan(...any) error { return row.err }

func TestInsertPausedCompiledTaskDefinition_AllFailuresRollback(t *testing.T) {
	st := tenantTestStore(t)
	f := newCompiledTaskFixture(t, st)
	ctx := t.Context()

	cases := []struct {
		name             string
		failContains     string
		skipContains     string
		commitErr        error
		precreateSource  bool
		failBegin        bool
		expectedExisting int
		expectedError    error
	}{
		{name: "begin", failBegin: true, expectedError: types.ErrDatabase},
		{name: "membership query", failContains: "FROM memberships m", expectedError: types.ErrDatabase},
		{name: "schedule insert", failContains: "INSERT INTO schedules", expectedError: types.ErrDatabase},
		{name: "playbook insert", failContains: "INSERT INTO schedule_playbooks", expectedError: types.ErrDatabase},
		{name: "source insert", failContains: "INSERT INTO fetch_targets", expectedError: types.ErrDatabase},
		{
			name:            "existing source lookup",
			failContains:    "SELECT id, platform, capability, config",
			precreateSource: true, expectedExisting: 1, expectedError: types.ErrDatabase,
		},
		{
			name: "schedule source link", failContains: "INSERT INTO task_fetch_targets",
			expectedError: types.ErrDatabase,
		},
		{
			name: "exact-set verification query", failContains: "cardinality",
			expectedError: types.ErrDatabase,
		},
		{
			name: "plan-link mismatch", skipContains: "INSERT INTO task_fetch_targets",
			expectedError: types.ErrInternal,
		},
		{name: "commit", commitErr: errInjectedCompiledTask, expectedError: types.ErrDatabase},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			taskID := f.taskID()
			sourceURL := f.url("rollback")
			if tc.precreateSource {
				if _, err := st.pool.Exec(ctx,
					`INSERT INTO fetch_targets (platform, capability, url, title, config)
					 VALUES ('web', 'search', $1, '既有', '{"query":"topic-1"}')`,
					sourceURL); err != nil {
					t.Fatalf("预置 existing source: %v", err)
				}
			}
			def := f.definition(taskID, types.StrictnessNormal, sourceURL)

			faultStore := *st
			var wrapped *compiledTaskFaultTx
			callCtx := ctx
			var cancelCall context.CancelFunc
			if tc.commitErr != nil {
				callCtx, cancelCall = context.WithCancel(ctx)
				defer cancelCall()
			}
			if tc.failBegin {
				faultStore.beginTx = func(context.Context, pgx.TxOptions) (pgx.Tx, error) {
					return nil, errInjectedCompiledTask
				}
			} else {
				faultStore.beginTx = func(ctx context.Context, opts pgx.TxOptions) (pgx.Tx, error) {
					realTx, err := st.pool.BeginTx(ctx, opts)
					if err != nil {
						return nil, err
					}
					wrapped = &compiledTaskFaultTx{
						Tx:             realTx,
						failContains:   tc.failContains,
						skipContains:   tc.skipContains,
						commitErr:      tc.commitErr,
						cancelOnCommit: cancelCall,
					}
					return wrapped, nil
				}
			}

			err := faultStore.InsertPausedCompiledTaskDefinition(callCtx, def)
			if !errors.Is(err, tc.expectedError) {
				t.Fatalf("注入失败错误类别不符，want=%v got=%v", tc.expectedError, err)
			}
			if !tc.failBegin {
				if wrapped == nil {
					t.Fatal("BeginTx 成功后未捕获事务包装器")
				}
				if wrapped.rollbackCalls != 1 {
					t.Fatalf("失败路径必须精确调用一次 Rollback，实得 %d", wrapped.rollbackCalls)
				}
				if wrapped.rollbackContextErr != nil {
					t.Fatalf("Rollback 必须使用不受调用方取消影响的 context，实得 %v",
						wrapped.rollbackContextErr)
				}
			}
			assertNoCompiledTaskAggregate(t, st, taskID)
			assertSourceURLCount(t, st, []string{sourceURL}, tc.expectedExisting)
		})
	}
}

type blockBeforeScheduleTx struct {
	pgx.Tx
	ready   chan struct{}
	release chan struct{}
	once    sync.Once
}

func (tx *blockBeforeScheduleTx) Exec(
	ctx context.Context,
	sql string,
	arguments ...any,
) (pgconn.CommandTag, error) {
	if strings.Contains(sql, "INSERT INTO schedules") {
		tx.once.Do(func() { close(tx.ready) })
		select {
		case <-tx.release:
		case <-ctx.Done():
			return pgconn.CommandTag{}, ctx.Err()
		}
	}
	return tx.Tx.Exec(ctx, sql, arguments...)
}

func TestInsertPausedCompiledTaskDefinition_LocksMembershipAndTenant(t *testing.T) {
	st := tenantTestStore(t)
	f := newCompiledTaskFixture(t, st)
	def := f.definition(f.taskID(), types.StrictnessNormal, f.url("lock"))

	ready := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseWriter := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseWriter)

	lockingStore := *st
	lockingStore.beginTx = func(ctx context.Context, opts pgx.TxOptions) (pgx.Tx, error) {
		realTx, err := st.pool.BeginTx(ctx, opts)
		if err != nil {
			return nil, err
		}
		return &blockBeforeScheduleTx{Tx: realTx, ready: ready, release: release}, nil
	}

	callCtx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		result <- lockingStore.InsertPausedCompiledTaskDefinition(callCtx, def)
	}()

	select {
	case <-ready:
		// membership SELECT 已 Scan 完成；此刻应同时持有 membership 与 tenant 的 SHARE 行锁。
	case <-callCtx.Done():
		t.Fatalf("等待 A2 持锁超时: %v", callCtx.Err())
	}

	assertRowLockTimeout(t, st,
		`DELETE FROM memberships WHERE tenant_id = $1 AND user_id = $2`,
		f.tenantID, f.userID)
	assertRowLockTimeout(t, st,
		`UPDATE tenants SET status = 'deleting' WHERE id = $1`,
		f.tenantID)

	releaseWriter()
	if err := <-result; err != nil {
		t.Fatalf("释放锁后 A2 应成功: %v", err)
	}
}

func TestInsertPausedCompiledTaskDefinition_HasZeroProductionCallPoints(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller 无法定位测试文件")
	}
	storeDir := filepath.Dir(thisFile)
	repoRoot := filepath.Dir(storeDir)
	fset := token.NewFileSet()
	var references []string
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
		ast.Inspect(file, func(node ast.Node) bool {
			selector, ok := node.(*ast.SelectorExpr)
			if ok && selector.Sel.Name == "InsertPausedCompiledTaskDefinition" {
				position := fset.Position(selector.Pos())
				references = append(references,
					fmt.Sprintf("%s:%d", position.Filename, position.Line))
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("扫描生产调用点: %v", err)
	}
	if len(references) != 0 {
		t.Fatalf("A2 必须保持零生产调用点，发现 %v", references)
	}
}

func assertCompiledJSONEqual(t *testing.T, got, want []byte) {
	t.Helper()
	var gotValue, wantValue any
	if err := json.Unmarshal(got, &gotValue); err != nil {
		t.Fatalf("got 不是合法 JSON: %v (%s)", err, got)
	}
	if err := json.Unmarshal(want, &wantValue); err != nil {
		t.Fatalf("want 不是合法 JSON: %v (%s)", err, want)
	}
	if !reflect.DeepEqual(gotValue, wantValue) {
		t.Errorf("JSON 不相等:\ngot:  %s\nwant: %s", got, want)
	}
}

func linkedSourceURLs(t *testing.T, st *Store, taskID string) []string {
	t.Helper()
	rows, err := st.pool.Query(t.Context(),
		`SELECT s.url
		   FROM task_fetch_targets ss
		   JOIN fetch_targets s ON s.id = ss.fetch_target_id
		  WHERE ss.schedule_id = $1
		  ORDER BY s.url`,
		taskID)
	if err != nil {
		t.Fatalf("查询任务链接 URL: %v", err)
	}
	defer rows.Close()
	var urls []string
	for rows.Next() {
		var sourceURL string
		if err := rows.Scan(&sourceURL); err != nil {
			t.Fatalf("扫描任务链接 URL: %v", err)
		}
		urls = append(urls, sourceURL)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("遍历任务链接 URL: %v", err)
	}
	return urls
}

func sourceByID(t *testing.T, st *Store, sourceID int64) types.FetchTarget {
	t.Helper()
	var source types.FetchTarget
	if err := scanFetchTarget(st.pool.QueryRow(t.Context(),
		`SELECT `+fetchTargetColumns+` FROM fetch_targets s WHERE s.id = $1`,
		sourceID), &source); err != nil {
		t.Fatalf("按 id 回查 source: %v", err)
	}
	return source
}

func sourceByURL(t *testing.T, st *Store, sourceURL string) types.FetchTarget {
	t.Helper()
	var source types.FetchTarget
	if err := scanFetchTarget(st.pool.QueryRow(t.Context(),
		`SELECT `+fetchTargetColumns+` FROM fetch_targets s WHERE s.url = $1`,
		sourceURL), &source); err != nil {
		t.Fatalf("按 URL 回查 source: %v", err)
	}
	return source
}

func planURLs(t *testing.T, raw json.RawMessage) []string {
	t.Helper()
	var plan compiledFetchPlan
	if err := json.Unmarshal(raw, &plan); err != nil {
		t.Fatalf("解析测试 fetch_plan: %v", err)
	}
	urls := make([]string, 0, len(plan.Targets))
	for _, src := range plan.Targets {
		urls = append(urls, src.URL)
	}
	return urls
}

func assertNoCompiledTaskAggregate(t *testing.T, st *Store, taskID string) {
	t.Helper()
	for table, column := range map[string]string{
		"schedules":                         "id",
		"schedule_playbooks":                "schedule_id",
		"task_fetch_targets":                "schedule_id",
		"task_approved_definition_versions": "task_id",
	} {
		var count int
		query := fmt.Sprintf(`SELECT count(*) FROM %s WHERE %s = $1`, table, column)
		if err := st.pool.QueryRow(t.Context(), query, taskID).Scan(&count); err != nil {
			t.Fatalf("回查 %s: %v", table, err)
		}
		if count != 0 {
			t.Errorf("失败后 %s 应零残留，实得 %d", table, count)
		}
	}
}

func assertSourceURLCount(t *testing.T, st *Store, urls []string, want int) {
	t.Helper()
	var count int
	if err := st.pool.QueryRow(t.Context(),
		`SELECT count(*) FROM fetch_targets WHERE url = ANY($1::text[])`,
		urls).Scan(&count); err != nil {
		t.Fatalf("回查 source URL: %v", err)
	}
	if count != want {
		t.Errorf("source 残留数不符: got=%d want=%d urls=%v", count, want, urls)
	}
}

func snapshotCompiledTaskAggregate(
	t *testing.T,
	st *Store,
	taskID string,
) compiledTaskAggregateSnapshot {
	t.Helper()
	var snapshot compiledTaskAggregateSnapshot
	if err := st.pool.QueryRow(t.Context(),
		`SELECT row_to_json(x)::text
		   FROM (
		        SELECT id, tenant_id, user_id, nl_description, spec_json, scope_json,
		               status, push_strictness, created_at, updated_at
		          FROM schedules WHERE id = $1
		   ) x`,
		taskID).Scan(&snapshot.ScheduleJSON); err != nil {
		t.Fatalf("快照 schedule: %v", err)
	}
	if err := st.pool.QueryRow(t.Context(),
		`SELECT row_to_json(x)::text
		   FROM (
		        SELECT schedule_id, content, fetch_plan, updated_at
		          FROM schedule_playbooks WHERE schedule_id = $1
		   ) x`,
		taskID).Scan(&snapshot.PlaybookJSON); err != nil {
		t.Fatalf("快照 playbook: %v", err)
	}
	snapshot.SourceURLs = linkedSourceURLs(t, st, taskID)
	return snapshot
}

func assertRowLockTimeout(t *testing.T, st *Store, query string, args ...any) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	tx, err := st.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("开启锁竞争事务: %v", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if _, err := tx.Exec(ctx, `SET LOCAL lock_timeout = '150ms'`); err != nil {
		t.Fatalf("设置 lock_timeout: %v", err)
	}
	_, err = tx.Exec(ctx, query, args...)
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "55P03" {
		t.Errorf("并发写应被 SHARE 行锁挡到 55P03，实得 %v", err)
	}
}
