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
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/YouToco/vane/sourcespec"
	"github.com/YouToco/vane/taskstate"
	"github.com/YouToco/vane/types"
)

type taskDefinitionStateFixture struct {
	store      *Store
	tenantID   int64
	userID     int64
	taskID     string
	sourceID   int64
	definition taskstate.ApprovedDefinitionV1
}

// historicalCompiledPlanFixture is deliberately test-owned rather than an
// alias of either the current A2 writer or the frozen C2 adapter. If either
// production shape drifts, this fixture keeps exercising the bytes that were
// already persisted before C2.
type historicalCompiledPlanFixture struct {
	Sources []historicalCompiledSourceFixture `json:"sources"`
}

type historicalCompiledSourceFixture struct {
	Platform   string          `json:"platform"`
	Capability string          `json:"capability"`
	Title      string          `json:"title,omitempty"`
	URL        string          `json:"url"`
	Config     json.RawMessage `json:"config,omitempty"`
}

func newTaskDefinitionStateFixture(t *testing.T) taskDefinitionStateFixture {
	t.Helper()
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("未设置 DATABASE_URL，跳过 C2a task state 真库测试")
	}
	if err := Migrate(t.Context(), dbURL); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	st, err := New(t.Context(), dbURL)
	if err != nil {
		t.Fatalf("New Store: %v", err)
	}
	t.Cleanup(st.Close)
	base := newCompiledTaskFixture(t, st)
	taskID := base.taskID()
	query := "c2a-" + uuid.NewString()
	source, message := sourcespec.Build(sourcespec.Spec{
		Platform: string(types.PlatformWeb), Capability: string(types.CapSearch),
		Params: map[string]string{"query": query}, Title: "C2a approved search",
	})
	if message != "" || source == nil {
		t.Fatalf("build source: %q", message)
	}
	planBytes, err := json.Marshal(historicalCompiledPlanFixture{
		Sources: []historicalCompiledSourceFixture{{
			Platform: string(source.Platform), Capability: string(source.Capability),
			Title: source.Title, URL: source.URL, Config: source.Config,
		}}})
	if err != nil {
		t.Fatalf("marshal legacy plan: %v", err)
	}
	legacyDefinition := types.PausedCompiledTaskDefinition{
		TaskID: taskID, TenantID: base.tenantID, UserID: base.userID,
		NLDescription: "每天查看 C2a 搜索", SpecJSON: json.RawMessage(`{"cron":"0 8 * * *"}`),
		ScopeJSON: json.RawMessage(`{}`), PlaybookContent: "持续监控 C2a 主题",
		FetchPlan: planBytes, Strictness: "",
	}
	if err := st.InsertPausedCompiledTaskDefinition(t.Context(), legacyDefinition); err != nil {
		t.Fatalf("seed compiled task: %v", err)
	}
	var sourceID int64
	if err := st.pool.QueryRow(t.Context(),
		`SELECT source_id FROM schedule_sources WHERE schedule_id=$1`, taskID,
	).Scan(&sourceID); err != nil {
		t.Fatalf("load materialized source id: %v", err)
	}
	exactPlan, err := json.Marshal(taskstate.FetchPlanV1{Sources: []taskstate.PlanSourceV1{{
		Platform: source.Platform, Capability: source.Capability,
		Title: source.Title, URL: source.URL, Config: source.Config,
	}}})
	if err != nil {
		t.Fatalf("marshal exact plan: %v", err)
	}
	approved, err := taskstate.BuildApprovedDefinitionV1(taskstate.ApprovedDefinitionInputV1{
		TenantID: base.tenantID, UserID: base.userID, TaskID: taskID,
		Intent:        legacyDefinition.PlaybookContent,
		NLDescription: legacyDefinition.NLDescription,
		SpecJSON:      legacyDefinition.SpecJSON, ScopeJSON: legacyDefinition.ScopeJSON,
		PlaybookContent: legacyDefinition.PlaybookContent,
		SourceScope:     taskstate.SourceScopeApprovedPlan, FetchPlan: exactPlan,
		Strictness: legacyDefinition.Strictness,
		Sources: []taskstate.ApprovedSourceV1{{
			SourceID: sourceID, Platform: source.Platform, Capability: source.Capability,
			Title: source.Title, URL: source.URL, Config: source.Config,
		}},
		ExecutionMode:  types.ExecutionModeCompiled,
		DeliveryPolicy: taskstate.DeliveryPolicyOwnerFeishu,
		BudgetPolicy:   taskstate.BudgetPolicyInheritTenantQuota,
	})
	if err != nil {
		t.Fatalf("build approved definition: %v", err)
	}
	return taskDefinitionStateFixture{
		store: st, tenantID: base.tenantID, userID: base.userID,
		taskID: taskID, sourceID: sourceID, definition: approved,
	}
}

func TestInitialApprovedDefinitionExactProjectionAndReplay(t *testing.T) {
	f := newTaskDefinitionStateFixture(t)
	ctx := t.Context()
	if _, err := f.store.GetCurrentApprovedDefinition(
		ctx, f.tenantID, f.userID, f.taskID); !errors.Is(err, types.ErrNotFound) {
		t.Fatalf("headless schedule error=%v, want NotFound", err)
	}

	created, err := f.store.InsertInitialApprovedDefinition(
		ctx, f.definition, "c2a-baseline:"+f.taskID)
	if err != nil {
		t.Fatalf("InsertInitialApprovedDefinition: %v", err)
	}
	if created.Version != 1 || created.Definition.Strictness != types.PushStrictness("loose") ||
		created.Definition.ExecutionMode != types.ExecutionModeCompiled {
		t.Fatalf("created record=%+v", created)
	}
	loaded, err := f.store.GetCurrentApprovedDefinition(
		ctx, f.tenantID, f.userID, f.taskID)
	if err != nil {
		t.Fatalf("GetCurrentApprovedDefinition: %v", err)
	}
	if loaded.Digest != created.Digest || !bytes.Equal(loaded.Payload, created.Payload) {
		t.Fatalf("loaded immutable bytes differ: loaded=%+v created=%+v", loaded, created)
	}
	replayed, err := f.store.InsertInitialApprovedDefinition(
		ctx, f.definition, "c2a-baseline:"+f.taskID)
	if err != nil {
		t.Fatalf("response-loss replay: %v", err)
	}
	if replayed.Version != created.Version || replayed.Digest != created.Digest ||
		!replayed.CreatedAt.Equal(created.CreatedAt) {
		t.Fatalf("replay generated a different version: first=%+v replay=%+v", created, replayed)
	}
	var rows int
	if err := f.store.pool.QueryRow(ctx,
		`SELECT count(*) FROM task_approved_definition_versions
		  WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3`,
		f.tenantID, f.userID, f.taskID).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Fatalf("replay wrote %d versions, want 1", rows)
	}
	advanceApprovedDefinitionForTest(t, f, 2, "用户随后确认的 v2 动态主题",
		types.ExecutionModeDiscoverAtRun)
	lateReplay, err := f.store.InsertInitialApprovedDefinition(
		ctx, f.definition, "c2a-baseline:"+f.taskID)
	if err != nil || lateReplay.Version != created.Version ||
		lateReplay.Digest != created.Digest || !lateReplay.CreatedAt.Equal(created.CreatedAt) {
		t.Fatalf("response-loss replay after head advance=%+v err=%v", lateReplay, err)
	}

	if _, err := f.store.GetCurrentApprovedDefinition(
		ctx, f.tenantID, f.userID+1, f.taskID); !errors.Is(err, types.ErrNotFound) {
		t.Fatalf("wrong user error=%v, want NotFound", err)
	}
	if _, err := f.store.GetCurrentApprovedDefinition(
		ctx, f.tenantID+1, f.userID, f.taskID); !errors.Is(err, types.ErrNotFound) {
		t.Fatalf("wrong tenant error=%v, want NotFound", err)
	}
}

func TestInitialApprovedDefinitionRejectsProjectionDriftWithoutHead(t *testing.T) {
	f := newTaskDefinitionStateFixture(t)
	drifted := f.definition
	drifted.NLDescription = "未写入旧投影的新名字"
	if _, err := f.store.InsertInitialApprovedDefinition(
		t.Context(), drifted, "c2a-drift:"+f.taskID); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("projection drift error=%v, want Conflict", err)
	}
	var versions int
	var head *int64
	if err := f.store.pool.QueryRow(t.Context(),
		`SELECT (SELECT count(*) FROM task_approved_definition_versions
		          WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3),
		        approved_definition_version
		   FROM schedules WHERE tenant_id=$1 AND user_id=$2 AND id=$3`,
		f.tenantID, f.userID, f.taskID).Scan(&versions, &head); err != nil {
		t.Fatal(err)
	}
	if versions != 0 || head != nil {
		t.Fatalf("rejected baseline left versions/head=%d/%v", versions, head)
	}
}

func TestAdaptiveStateCASScopeBoundsAndReplay(t *testing.T) {
	f := newTaskDefinitionStateFixture(t)
	ctx := t.Context()
	approved, err := f.store.InsertInitialApprovedDefinition(
		ctx, f.definition, "c2a-adaptive:"+f.taskID)
	if err != nil {
		t.Fatal(err)
	}
	basis := ApprovedDefinitionFence{Version: approved.Version, Digest: approved.Digest}
	state := buildAdaptiveState(t, f, taskstate.RunStatsV1{})
	lkg := int64(1)
	first, err := f.store.CompareAndSwapAdaptiveState(ctx, 0, basis, state, &lkg)
	if err != nil {
		t.Fatalf("first adaptive CAS: %v", err)
	}
	if first.Version != 1 || first.LastKnownGoodDefinitionVersion == nil ||
		*first.LastKnownGoodDefinitionVersion != 1 {
		t.Fatalf("first adaptive record=%+v", first)
	}
	replay, err := f.store.CompareAndSwapAdaptiveState(ctx, 0, basis, state, &lkg)
	if err != nil || replay.Version != first.Version || replay.Digest != first.Digest {
		t.Fatalf("response-loss replay=%+v err=%v", replay, err)
	}

	next := buildAdaptiveState(t, f, taskstate.RunStatsV1{
		AttemptedRuns: 1, SuccessfulRuns: 1,
	})
	advanced, err := f.store.CompareAndSwapAdaptiveState(ctx, 1, basis, next, &lkg)
	if err != nil || advanced.Version != 2 {
		t.Fatalf("advance=%+v err=%v", advanced, err)
	}
	if _, err := f.store.CompareAndSwapAdaptiveState(
		ctx, 1, basis, state, &lkg); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("stale CAS error=%v, want Conflict", err)
	}
	if _, err := f.store.CompareAndSwapAdaptiveState(
		ctx, 2, basis, state, &lkg); !errors.Is(err, types.ErrValidation) {
		t.Fatalf("decreasing counters error=%v, want Validation", err)
	}

	unapproved, err := taskstate.BuildAdaptiveStateV1(taskstate.AdaptiveStateInputV1{
		TenantID: f.tenantID, UserID: f.userID, TaskID: f.taskID,
		QueryVariants:   []taskstate.QueryVariantV1{},
		CapabilityOrder: []taskstate.ReadCapabilityV1{}, SourceOrder: []int64{999999},
		RunStats: next.RunStats,
	})
	if err != nil {
		t.Fatalf("build structurally valid unapproved state: %v", err)
	}
	if _, err := f.store.CompareAndSwapAdaptiveState(
		ctx, 2, basis, unapproved, &lkg); !errors.Is(err, types.ErrValidation) {
		t.Fatalf("unapproved source error=%v, want Validation", err)
	}
	omittedCapability, err := taskstate.BuildAdaptiveStateV1(taskstate.AdaptiveStateInputV1{
		TenantID: f.tenantID, UserID: f.userID, TaskID: f.taskID,
		QueryVariants:   []taskstate.QueryVariantV1{},
		CapabilityOrder: []taskstate.ReadCapabilityV1{},
		SourceOrder:     []int64{f.sourceID}, RunStats: next.RunStats,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.CompareAndSwapAdaptiveState(
		ctx, 2, basis, omittedCapability, &lkg); !errors.Is(err, types.ErrValidation) {
		t.Fatalf("omitted approved capability error=%v, want Validation", err)
	}
	badLKG := int64(99)
	if _, err := f.store.CompareAndSwapAdaptiveState(
		ctx, 2, basis, next, &badLKG); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("foreign LKG error=%v, want Conflict", err)
	}
	loaded, found, err := f.store.GetAdaptiveStateForDefinition(
		ctx, f.tenantID, f.userID, f.taskID, basis)
	if err != nil || !found || loaded.Version != 2 || loaded.Digest != advanced.Digest ||
		loaded.BasisDefinitionVersion != basis.Version ||
		loaded.BasisDefinitionDigest != basis.Digest {
		t.Fatalf("GetAdaptiveState found=%v record=%+v err=%v", found, loaded, err)
	}

	// A run that started under definition v1 must not persist after the user
	// confirms v2, even when the source/capability shape is still compatible.
	v2Basis := advanceApprovedDefinitionForTest(t, f, 2, "用户已确认的新主题")
	if _, _, err := f.store.GetAdaptiveStateForDefinition(
		ctx, f.tenantID, f.userID, f.taskID, basis); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("stale adaptive read fence error=%v, want Conflict", err)
	}
	if _, _, err := f.store.GetAdaptiveStateForDefinition(
		ctx, f.tenantID, f.userID, f.taskID, v2Basis); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("old adaptive state under new definition error=%v, want Conflict", err)
	}
	if _, err := f.store.CompareAndSwapAdaptiveState(
		ctx, 2, basis, next, &lkg); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("stale definition fence error=%v, want Conflict", err)
	}
	if _, err := f.store.CompareAndSwapAdaptiveState(
		ctx, 2, v2Basis, next, &lkg); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("historical LKG without equivalence proof error=%v, want Conflict", err)
	}
	var retainedVersion int64
	if err := f.store.pool.QueryRow(ctx, `
		SELECT version FROM task_adaptive_states
		 WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3`,
		f.tenantID, f.userID, f.taskID).Scan(&retainedVersion); err != nil {
		t.Fatal(err)
	}
	if retainedVersion != 2 {
		t.Fatalf("stale definition write changed adaptive version to %d", retainedVersion)
	}
}

func advanceApprovedDefinitionForTest(
	t *testing.T,
	f taskDefinitionStateFixture,
	version int64,
	intent string,
	modes ...types.ExecutionMode,
) ApprovedDefinitionFence {
	t.Helper()
	definition := f.definition
	definition.Intent = intent
	definition.PlaybookContent = intent
	if len(modes) > 1 {
		t.Fatal("advanceApprovedDefinitionForTest accepts at most one mode")
	}
	if len(modes) == 1 {
		definition.ExecutionMode = modes[0]
	}
	payload, err := taskstate.EncodeApprovedDefinitionV1(definition)
	if err != nil {
		t.Fatal(err)
	}
	digest := digestTaskStatePayload(payload)
	if _, err := f.store.pool.Exec(t.Context(), `
		INSERT INTO task_approved_definition_versions (
			tenant_id, user_id, task_id, version, schema_version,
			execution_mode, definition_digest, payload, approval_ref
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		f.tenantID, f.userID, f.taskID, version, definition.SchemaVersion,
		definition.ExecutionMode, digest, payload,
		fmt.Sprintf("c2a-v%d:%s", version, f.taskID)); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.pool.Exec(t.Context(), `
		UPDATE schedules
		   SET approved_definition_version=$4, approved_definition_digest=$5,
		       execution_mode=$6
		 WHERE tenant_id=$1 AND user_id=$2 AND id=$3`,
		f.tenantID, f.userID, f.taskID, version, digest,
		definition.ExecutionMode); err != nil {
		t.Fatal(err)
	}
	return ApprovedDefinitionFence{Version: version, Digest: digest}
}

func TestAdaptiveStateConcurrentDifferentCASHasOneWinner(t *testing.T) {
	f := newTaskDefinitionStateFixture(t)
	ctx := t.Context()
	approved, err := f.store.InsertInitialApprovedDefinition(
		ctx, f.definition, "c2a-race:"+f.taskID)
	if err != nil {
		t.Fatal(err)
	}
	basis := ApprovedDefinitionFence{Version: approved.Version, Digest: approved.Digest}
	initial := buildAdaptiveState(t, f, taskstate.RunStatsV1{})
	if _, err := f.store.CompareAndSwapAdaptiveState(ctx, 0, basis, initial, nil); err != nil {
		t.Fatal(err)
	}
	left := buildAdaptiveState(t, f, taskstate.RunStatsV1{AttemptedRuns: 1, EmptyRuns: 1})
	right := buildAdaptiveState(t, f, taskstate.RunStatsV1{AttemptedRuns: 1, FailedRuns: 1,
		ConsecutiveFailures: 1})
	start := make(chan struct{})
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for _, candidate := range []taskstate.AdaptiveStateV1{left, right} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := f.store.CompareAndSwapAdaptiveState(ctx, 1, basis, candidate, nil)
			errs <- err
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	var succeeded, conflicted int
	for err := range errs {
		switch {
		case err == nil:
			succeeded++
		case errors.Is(err, types.ErrConflict):
			conflicted++
		default:
			t.Fatalf("concurrent CAS unexpected error: %v", err)
		}
	}
	if succeeded != 1 || conflicted != 1 {
		t.Fatalf("concurrent CAS success/conflict=%d/%d, want 1/1", succeeded, conflicted)
	}
}

func TestAdaptiveStateRejectsLegacySubscriptionBasis(t *testing.T) {
	f := newTaskDefinitionStateFixture(t)
	ctx := t.Context()
	taskID := "c2a-legacy-" + uuid.NewString()
	const playbook = "继续兼容既有订阅，但尚未确认长期信源"
	if err := f.store.InsertSchedule(ctx, &types.Schedule{
		ID: taskID, UserID: f.userID, NLDescription: "存量订阅兼容任务",
		SpecJSON:  json.RawMessage(`{"cron":"0 9 * * *"}`),
		ScopeJSON: json.RawMessage(`{}`), Status: types.ScheduleStatusPaused,
	}); err != nil {
		t.Fatal(err)
	}
	if ok, err := f.store.UpsertSchedulePlaybook(ctx, f.userID, taskID, playbook); err != nil || !ok {
		t.Fatalf("seed legacy playbook ok=%v err=%v", ok, err)
	}
	definition, err := taskstate.BuildApprovedDefinitionV1(taskstate.ApprovedDefinitionInputV1{
		TenantID: f.tenantID, UserID: f.userID, TaskID: taskID,
		Intent: playbook, NLDescription: "存量订阅兼容任务",
		SpecJSON: json.RawMessage(`{"cron":"0 9 * * *"}`), ScopeJSON: json.RawMessage(`{}`),
		PlaybookContent: playbook, SourceScope: taskstate.SourceScopeLegacySubscriptions,
		FetchPlan: json.RawMessage(`{}`), Strictness: types.DefaultStrictness,
		Sources: []taskstate.ApprovedSourceV1{}, ExecutionMode: types.ExecutionModeCompiled,
		DeliveryPolicy: taskstate.DeliveryPolicyOwnerFeishu,
		BudgetPolicy:   taskstate.BudgetPolicyInheritTenantQuota,
	})
	if err != nil {
		t.Fatal(err)
	}
	approved, err := f.store.InsertInitialApprovedDefinition(
		ctx, definition, "c2a-legacy:"+taskID)
	if err != nil {
		t.Fatal(err)
	}
	state, err := taskstate.BuildAdaptiveStateV1(taskstate.AdaptiveStateInputV1{
		TenantID: f.tenantID, UserID: f.userID, TaskID: taskID,
		QueryVariants:   []taskstate.QueryVariantV1{},
		CapabilityOrder: []taskstate.ReadCapabilityV1{}, SourceOrder: []int64{},
		RunStats: taskstate.RunStatsV1{},
	})
	if err != nil {
		t.Fatal(err)
	}
	basis := ApprovedDefinitionFence{Version: approved.Version, Digest: approved.Digest}
	if _, err := f.store.CompareAndSwapAdaptiveState(
		ctx, 0, basis, state, &approved.Version); !errors.Is(err, types.ErrValidation) {
		t.Fatalf("legacy subscriptions adaptive/LKG error=%v, want Validation", err)
	}
}

func buildAdaptiveState(
	t *testing.T,
	f taskDefinitionStateFixture,
	stats taskstate.RunStatsV1,
) taskstate.AdaptiveStateV1 {
	t.Helper()
	state, err := taskstate.BuildAdaptiveStateV1(taskstate.AdaptiveStateInputV1{
		TenantID: f.tenantID, UserID: f.userID, TaskID: f.taskID,
		QueryVariants: []taskstate.QueryVariantV1{},
		CapabilityOrder: []taskstate.ReadCapabilityV1{{
			Platform: types.PlatformWeb, Capability: types.CapSearch,
		}},
		SourceOrder: []int64{f.sourceID}, RunStats: stats,
	})
	if err != nil {
		t.Fatalf("BuildAdaptiveStateV1: %v", err)
	}
	return state
}

func TestTaskDefinitionStateMutatorsHaveZeroProductionCallPoints(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve current source file")
	}
	repoRoot := filepath.Dir(filepath.Dir(thisFile))
	want := map[string]struct{}{
		"InsertInitialApprovedDefinition": {},
		"CompareAndSwapAdaptiveState":     {},
	}
	var references []string
	err := filepath.WalkDir(repoRoot, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			base := entry.Name()
			if base == ".git" || strings.HasPrefix(base, ".tmp") {
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, parseErr := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if parseErr != nil {
			return parseErr
		}
		ast.Inspect(file, func(node ast.Node) bool {
			// The guarded method bodies necessarily contain their own identifiers.
			// Skip only those declarations, not their entire source file: a helper
			// or future production method beside them is still a call point.
			if declaration, ok := node.(*ast.FuncDecl); ok {
				if _, guarded := want[declaration.Name.Name]; guarded {
					return false
				}
			}
			selector, ok := node.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			if _, guarded := want[selector.Sel.Name]; guarded {
				references = append(references,
					filepath.ToSlash(strings.TrimPrefix(path, repoRoot+string(filepath.Separator)))+
						":"+selector.Sel.Name)
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("scan C2a production call points: %v", err)
	}
	if !slices.Equal(references, []string{}) {
		t.Fatalf("C2a mutators must remain dark, found %v", references)
	}
}

func TestTaskDefinitionBaselineAdapterDoesNotReuseCurrentPlanTypes(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve current source file")
	}
	sourceFile := strings.TrimSuffix(thisFile, "_test.go") + ".go"
	file, err := parser.ParseFile(token.NewFileSet(), sourceFile, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	forbidden := map[string]struct{}{
		"compiledFetchPlan": {}, "compiledPlanSource": {},
	}
	ast.Inspect(file, func(node ast.Node) bool {
		identifier, ok := node.(*ast.Ident)
		if ok {
			if _, found := forbidden[identifier.Name]; found {
				t.Errorf("frozen baseline adapter references current type %s", identifier.Name)
			}
		}
		return true
	})
}

func TestTaskStatePayloadMutationIsDetectedOnRead(t *testing.T) {
	f := newTaskDefinitionStateFixture(t)
	created, err := f.store.InsertInitialApprovedDefinition(
		t.Context(), f.definition, "c2a-tamper:"+f.taskID)
	if err != nil {
		t.Fatal(err)
	}
	mutated := bytes.Replace(created.Payload,
		[]byte(`"intent":"持续监控 C2a 主题"`),
		[]byte(`"intent":"被篡改的 C2a 主题"`), 1)
	if bytes.Equal(mutated, created.Payload) {
		t.Fatal("tamper fixture did not change payload")
	}
	row := approvedDefinitionTestRow{
		version: 1, schemaVersion: taskstate.ApprovedDefinitionSchemaVersionV1,
		mode: string(types.ExecutionModeCompiled), digest: created.Digest,
		payload: mutated, approvalRef: created.ApprovalRef, createdAt: time.Now(),
	}
	if _, err := scanApprovedDefinitionVersion(
		row, f.tenantID, f.userID, f.taskID); !errors.Is(err, types.ErrInternal) {
		t.Fatalf("corrupted payload error=%v, want Internal", err)
	}
}

type approvedDefinitionTestRow struct {
	version       int64
	schemaVersion string
	mode          string
	digest        string
	payload       []byte
	approvalRef   string
	createdAt     time.Time
}

func (r approvedDefinitionTestRow) Scan(dest ...any) error {
	*dest[0].(*int64) = r.version
	*dest[1].(*string) = r.schemaVersion
	*dest[2].(*string) = r.mode
	*dest[3].(*string) = r.digest
	*dest[4].(*[]byte) = bytes.Clone(r.payload)
	*dest[5].(*string) = r.approvalRef
	*dest[6].(*time.Time) = r.createdAt
	return nil
}
