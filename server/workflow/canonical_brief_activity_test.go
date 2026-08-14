package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.temporal.io/sdk/testsuite"

	"github.com/YouToco/vane/feedback"
	"github.com/YouToco/vane/types"
)

type canonicalBriefStoreFake struct {
	mu sync.Mutex

	loadDraft        types.BriefDraftV1
	loadFound        bool
	loadErr          error
	loadEmpty        bool
	loadEmptyErr     error
	loadEmptyBatchID int64

	prepareCalls   int
	prepareIDs     []int64
	prepared       types.BriefDraftV1
	prepareErr     error
	sealEmptyCalls int
	sealEmptyErr   error
}

func (f *canonicalBriefStoreFake) LoadSealedEmptyBriefBatchV1(
	context.Context,
	types.RunIdentity,
	types.RunSnapshotRef,
	types.RunOutcomeMarkerV1,
	string,
) (int64, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.loadEmptyBatchID, f.loadEmpty, f.loadEmptyErr
}

func (f *canonicalBriefStoreFake) SealEmptyBriefBatchV1(
	context.Context,
	types.RunIdentity,
	types.RunSnapshotRef,
	types.RunOutcomeMarkerV1,
	int64,
) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sealEmptyCalls++
	return f.sealEmptyErr
}

func (f *canonicalBriefStoreFake) LoadPreparedBriefDraftV1(
	context.Context,
	types.RunIdentity,
	types.RunSnapshotRef,
	types.RunOutcomeMarkerV1,
) (types.BriefDraftV1, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.loadDraft, f.loadFound, f.loadErr
}

func (f *canonicalBriefStoreFake) PrepareBriefDraftV1(
	_ context.Context,
	_ types.RunIdentity,
	_ types.RunSnapshotRef,
	marker types.RunOutcomeMarkerV1,
	batchID int64,
	generatedAt time.Time,
	ids []int64,
) (types.BriefDraftV1, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.prepareCalls++
	f.prepareIDs = append([]int64(nil), ids...)
	if f.prepareErr != nil {
		return types.BriefDraftV1{}, f.prepareErr
	}
	f.prepared = types.BriefDraftV1{
		SchemaVersion: types.BriefSchemaVersionV1,
		RunOutcomeID:  marker.ID, RunSnapshotID: marker.RunSnapshotID,
		PushBatchID: batchID, TenantID: marker.TenantID,
		UserID: marker.UserID, TaskID: marker.TaskID,
		GeneratedAt: generatedAt,
		Insights: []types.InsightV1{{
			ID: ids[0], RankPosition: 1,
			Title: "item", BodyMD: "body",
			SourceTitle:  "Frozen Source",
			SourceURL:    "https://example.com/item",
			DiscoveredAt: time.Unix(1, 0).UTC(),
		}},
	}
	return f.prepared, nil
}

func executeCanonicalBriefPrepareActivity(
	t *testing.T,
	env *testsuite.TestActivityEnvironment,
	a *Activities,
	in CanonicalBriefPrepareIn,
) (CanonicalBriefPrepareResult, error) {
	t.Helper()
	encoded, err := env.ExecuteActivity(a.PrepareCanonicalBriefV1, in)
	if err != nil {
		return CanonicalBriefPrepareResult{}, err
	}
	var result CanonicalBriefPrepareResult
	if err := encoded.Get(&result); err != nil {
		t.Fatal(err)
	}
	return result, nil
}

func TestPrepareCanonicalBriefV1FreezesBeforeAnyRenderer(t *testing.T) {
	identity, ref, snapshot := compiledActivityFixture("Frozen Task")
	compiledStore := &compiledRunStoreFake{
		snapshot: snapshot, authorize: true,
		attributionID: 10, attributionOK: true,
		deliveryIDSequence: []int64{301},
	}
	briefStore := new(canonicalBriefStoreFake)
	effectStore := newPRBActivityEffectStore(nil, nil)
	var renders atomic.Int32
	a := NewActivities(
		fakeFetcher{}, fakeScorer{}, fakeCardGen{}, new(fakePusher),
		new(fakeStore), fakeFeishu{}, nil, nil,
		func(feedback.AggregateCardInput) string {
			renders.Add(1)
			return `{}`
		},
		func(string, int) (string, string) { return "title", "blue" },
		WithCompiledRuntimeV1(
			compiledStore, nil, new(compiledModelResolverFake)),
		WithCanonicalBriefStoreV1(briefStore),
		// Empty current canary intentionally proves an already-frozen P1-C
		// command can finish after the rollout switch is turned off.
		WithPushEffectCanary(effectStore, ""),
	)
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	env.RegisterActivity(a.PrepareCanonicalBriefV1)
	marker := types.RunOutcomeMarkerV1{
		ID: 401, SchemaVersion: types.RunOutcomeSchemaVersionV1,
		RunSnapshotID: ref.SnapshotID, TenantID: identity.TenantID,
		UserID: identity.UserID, TaskID: identity.TaskID,
	}
	result, err := executeCanonicalBriefPrepareActivity(
		t, env, a, CanonicalBriefPrepareIn{
			UserID: identity.UserID, TraceID: "trace-canonical-stage",
			Run: CompiledRunInputV1{
				TenantID: identity.TenantID,
				TaskID:   identity.TaskID, Snapshot: ref,
			},
			Marker: marker,
			Cards: []GeneratedCard{{
				Scored: types.ScoredItem{
					Item: types.ContentItem{
						ID: 501, SourceID: 10, Title: "item",
						URL: "https://example.com/item",
					},
					Score: 88,
				},
				BodyMD: "body",
			}},
			GeneratedAt: time.Date(
				2026, 7, 27, 2, 3, 4, 0, time.UTC),
		})
	if err != nil {
		t.Fatal(err)
	}
	if result.Draft == nil || result.Draft.RunOutcomeID != marker.ID {
		t.Fatalf("prepared draft = %+v", result.Draft)
	}
	if renders.Load() != 0 {
		t.Fatalf("pre-render stage invoked renderer %d times", renders.Load())
	}
	briefStore.mu.Lock()
	prepareCalls := briefStore.prepareCalls
	prepareIDs := append([]int64(nil), briefStore.prepareIDs...)
	briefStore.mu.Unlock()
	if prepareCalls != 1 || len(prepareIDs) != 1 || prepareIDs[0] != 301 {
		t.Fatalf("stage calls=%d ids=%v", prepareCalls, prepareIDs)
	}
	effectStore.mu.Lock()
	desired := append(
		[]types.PushBatchDeliveryAuthority(nil), effectStore.desired...)
	effectStore.mu.Unlock()
	if len(desired) != 1 ||
		desired[0] != types.PushBatchDeliveryAuthorityEffect {
		t.Fatalf("effect authority claims = %v", desired)
	}
}

func TestPrepareCanonicalBriefV1FailsClosedOnAnyDeliveryWrite(t *testing.T) {
	identity, ref, snapshot := compiledActivityFixture("Frozen Task")
	compiledStore := &compiledRunStoreFake{
		snapshot: snapshot, authorize: true,
		attributionID: 10, attributionOK: true,
		deliveryErr: errors.New("delivery unavailable"),
	}
	briefStore := new(canonicalBriefStoreFake)
	a := NewActivities(
		fakeFetcher{}, fakeScorer{}, fakeCardGen{}, new(fakePusher),
		new(fakeStore), fakeFeishu{}, nil, nil,
		func(feedback.AggregateCardInput) string { return `{}` },
		func(string, int) (string, string) { return "title", "blue" },
		WithCompiledRuntimeV1(
			compiledStore, nil, new(compiledModelResolverFake)),
		WithCanonicalBriefStoreV1(briefStore),
		WithPushEffectCanary(newPRBActivityEffectStore(nil, nil), ""),
	)
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	env.RegisterActivity(a.PrepareCanonicalBriefV1)
	_, err := executeCanonicalBriefPrepareActivity(
		t, env, a, CanonicalBriefPrepareIn{
			UserID: identity.UserID, TraceID: "trace-canonical-fail",
			Run: CompiledRunInputV1{
				TenantID: identity.TenantID,
				TaskID:   identity.TaskID, Snapshot: ref,
			},
			Marker: types.RunOutcomeMarkerV1{
				ID: 402, SchemaVersion: types.RunOutcomeSchemaVersionV1,
				RunSnapshotID: ref.SnapshotID,
				TenantID:      identity.TenantID,
				UserID:        identity.UserID, TaskID: identity.TaskID,
			},
			Cards: []GeneratedCard{{
				Scored: types.ScoredItem{
					Item: types.ContentItem{
						ID: 502, SourceID: 10, Title: "item",
						URL: "https://example.com/item",
					},
					Score: 88,
				},
				BodyMD: "body",
			}},
			GeneratedAt: time.Date(
				2026, 7, 27, 2, 3, 5, 0, time.UTC),
		})
	if err == nil {
		t.Fatal("canonical stage skipped a failed delivery write")
	}
	briefStore.mu.Lock()
	defer briefStore.mu.Unlock()
	if briefStore.prepareCalls != 0 {
		t.Fatalf("stage persisted after failed delivery: %d",
			briefStore.prepareCalls)
	}
}

func TestPrepareCanonicalBriefV1ReplaysSealedEmptyBeforeMutableRun(t *testing.T) {
	identity, ref, _ := compiledActivityFixture("Frozen Task")
	briefStore := &canonicalBriefStoreFake{
		loadEmpty: true, loadEmptyBatchID: 778,
	}
	compiledStore := &compiledRunStoreFake{
		authorize: false,
	}
	a := NewActivities(
		fakeFetcher{}, fakeScorer{}, fakeCardGen{}, new(fakePusher),
		new(fakeStore), fakeFeishu{}, nil, nil,
		func(feedback.AggregateCardInput) string { return `{}` },
		func(string, int) (string, string) { return "title", "blue" },
		WithCompiledRuntimeV1(
			compiledStore, nil, new(compiledModelResolverFake)),
		WithCanonicalBriefStoreV1(briefStore),
		WithPushEffectCanary(newPRBActivityEffectStore(nil, nil), ""),
	)
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	env.RegisterActivity(a.PrepareCanonicalBriefV1)
	marker := types.RunOutcomeMarkerV1{
		ID: 404, SchemaVersion: types.RunOutcomeSchemaVersionV1,
		RunSnapshotID: ref.SnapshotID,
		TenantID:      identity.TenantID,
		UserID:        identity.UserID,
		TaskID:        identity.TaskID,
	}
	result, err := executeCanonicalBriefPrepareActivity(
		t, env, a, CanonicalBriefPrepareIn{
			UserID: identity.UserID, TraceID: "trace-empty-replay",
			Run: CompiledRunInputV1{
				TenantID: identity.TenantID,
				TaskID:   identity.TaskID,
				Snapshot: ref,
			},
			Marker: marker,
			Cards: []GeneratedCard{{
				Scored: types.ScoredItem{
					Item: types.ContentItem{
						ID: 1, SourceID: 10, Title: "item",
						URL: "https://example.com/item",
					},
					Score: 80,
				},
				BodyMD: "body",
			}},
			GeneratedAt: time.Date(
				2026, 7, 27, 2, 3, 6, 0, time.UTC),
		})
	if err != nil {
		t.Fatal(err)
	}
	if result.Draft != nil || !result.Empty || result.BatchID != 778 {
		t.Fatalf("sealed empty replay = %+v", result)
	}
}

func TestPushCanonicalEmptyCompletesWithoutMutableRunRead(t *testing.T) {
	identity, ref, _ := compiledActivityFixture("Frozen Task")
	effectStore := newPRBActivityEffectStore(nil, nil)
	a := NewActivities(
		fakeFetcher{}, fakeScorer{}, fakeCardGen{}, new(fakePusher),
		new(fakeStore), fakeFeishu{}, nil, nil,
		func(feedback.AggregateCardInput) string { return `{}` },
		func(string, int) (string, string) { return "title", "blue" },
		// No compiled Store is installed: reaching mutable run loading would
		// fail, proving the exact empty receipt returns first.
		WithPushEffectCanary(effectStore, ""),
	)
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	env.RegisterActivity(a.Push)
	marker := types.RunOutcomeMarkerV1{
		ID: 403, SchemaVersion: types.RunOutcomeSchemaVersionV1,
		RunSnapshotID: ref.SnapshotID,
		TenantID:      identity.TenantID,
		UserID:        identity.UserID,
		TaskID:        identity.TaskID,
	}
	if _, err := env.ExecuteActivity(a.Push, PushIn{
		UserID: identity.UserID, ScheduleID: identity.TaskID,
		TraceID: "trace-empty-receipt",
		Run: &CompiledRunInputV1{
			TenantID: identity.TenantID,
			TaskID:   identity.TaskID,
			Snapshot: ref,
		},
		CanonicalOutcome: &marker,
		CanonicalBatchID: 777,
		Cards: []GeneratedCard{{
			Scored: types.ScoredItem{
				Item: types.ContentItem{
					ID: 1, SourceID: 10, Title: "not re-planned",
					URL: "https://example.com/not-replanned",
				},
				Score: 80,
			},
			BodyMD: "not re-planned",
		}},
	}); err != nil {
		t.Fatal(err)
	}
	effectStore.mu.Lock()
	defer effectStore.mu.Unlock()
	if len(effectStore.empty) != 1 ||
		effectStore.empty[0].BatchID != 777 ||
		effectStore.empty[0].TenantID != identity.TenantID ||
		len(effectStore.prepared) != 0 {
		t.Fatalf("empty receipt calls=%+v prepared=%d",
			effectStore.empty, len(effectStore.prepared))
	}
}

func TestCanonicalBriefShadowPanicDoesNotBlockLegacyRender(t *testing.T) {
	a := new(Activities)
	pending := []pushPendingItem{{
		delID: 301,
		input: feedback.CardInput{
			DeliveryID: 301, Title: "legacy title",
			BodyMD: "legacy body", URL: "https://example.com/item",
		},
	}}
	draft := &types.BriefDraftV1{
		RunOutcomeID: 401,
		Insights: []types.InsightV1{{
			ID: 301, RankPosition: 1,
			Title: "canonical title", BodyMD: "canonical body",
			SourceURL: "https://example.com/item",
		}},
	}
	var calls atomic.Int32
	build := func([]pushPendingItem, string) string {
		if calls.Add(1) == 1 {
			panic("shadow renderer failure")
		}
		return `{"legacy":"still-authoritative"}`
	}
	if shadow := a.renderCanonicalBriefShadowV1(
		draft, pending, "", build); shadow != nil {
		t.Fatalf("panicked shadow returned cards: %v", shadow)
	}
	actual := planPushChunks(pending, "", build)
	if len(actual) != 1 ||
		actual[0].cardJSON != `{"legacy":"still-authoritative"}` {
		t.Fatalf("legacy render after shadow panic = %+v", actual)
	}
	if pending[0].input.Title != "legacy title" ||
		pending[0].input.BodyMD != "legacy body" {
		t.Fatalf("shadow mutated legacy input: %+v", pending[0].input)
	}
}

func TestCanonicalBriefAuthorityProjectsFrozenStructuredInsight(t *testing.T) {
	const deliveryID int64 = 301
	structured, err := types.SealStructuredInsightEvidenceV1(
		types.StructuredInsightV1{
			SchemaVersion:    types.StructuredInsightSchemaVersionV1,
			BodyMD:           "compatible body",
			WhatChanged:      "frozen change",
			WhyItMatters:     "frozen relevance",
			ImportanceReason: "frozen evidence",
			Claims: []types.StructuredClaimV1{{
				Text: "claim", Excerpt: "shared excerpt",
				SourceRefs: []string{"source-1"},
			}},
		},
		map[string]string{"source-1": "shared excerpt"},
	)
	if err != nil {
		t.Fatal(err)
	}
	provenance, err := types.SealObservedEventProvenanceV1(
		7, strings.Repeat("a", 64), strings.Repeat("b", 64),
		"release", "subject", time.Unix(3, 0).UTC(),
		json.RawMessage(`{"evidence_content_ids":[301]}`),
	)
	if err != nil {
		t.Fatal(err)
	}
	insight := types.InsightV1{
		ID: deliveryID, RankPosition: 1,
		Title: "canonical title", BodyMD: "compatible body",
		SourceTitle:  "Frozen Source",
		SourceURL:    "https://example.com/item",
		DiscoveredAt: time.Unix(1, 0).UTC(),
		Structured:   &structured,
		EventEvidence: &types.StructuredEventEvidenceV1{
			SchemaVersion:  types.StructuredEventEvidenceSchemaVersionV1,
			Provenance:     provenance,
			EvidenceDigest: structured.EvidenceDigest,
			Sources: []types.StructuredEvidenceSourceV1{{
				Ref: "source-1", Title: "frozen source item",
				SourceTitle: "Frozen Evidence", Platform: "web",
				SourceURL:    "https://evidence.example/item",
				DiscoveredAt: time.Unix(2, 0).UTC(),
			}},
		},
	}
	a := &Activities{
		canonicalBriefDashboardOrigin: "https://vane.example",
	}
	items, meta, err := a.canonicalBriefAuthorityItemsV1(
		&types.BriefDraftV1{
			PushBatchID: 401, TaskID: "task-structured",
			Insights: []types.InsightV1{insight},
		},
		[]pushPendingItem{{
			delID: deliveryID,
			input: feedback.CardInput{
				DeliveryID: deliveryID, BodyMD: "mutable body",
			},
			eventKey: "event-1",
		}},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 ||
		items[0].input.BodyMD !=
			feedback.CanonicalInsightBodyMDV1(insight) ||
		items[0].input.BodyMD == insight.BodyMD ||
		len(items[0].input.EvidenceSources) != 1 ||
		items[0].input.EvidenceSources[0].Ref != "source-1" ||
		items[0].input.EvidenceSources[0].SourceURL !=
			"https://evidence.example/item" {
		t.Fatalf("structured authority items = %+v", items)
	}
	if meta.BatchID != 401 || meta.TotalItems != 1 ||
		meta.WebURL != "https://vane.example/#/tasks/task-structured" {
		t.Fatalf("canonical metadata = %+v", meta)
	}
}
