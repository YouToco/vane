package workflow

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"go.temporal.io/sdk/testsuite"

	"github.com/YouToco/vane/server/feedback"
	"github.com/YouToco/vane/server/pusheffect"
	"github.com/YouToco/vane/server/runtimepolicy"
	storepkg "github.com/YouToco/vane/server/store"
	"github.com/YouToco/vane/server/types"
)

type aggregateChannelStoreFake struct {
	*effectCountingStore
	plan         *storepkg.ArtifactDeliveryPlan
	prepareCalls int
	settleCalls  int
}

func (f *aggregateChannelStoreFake) ResolveDeliveryChannelPreference(
	context.Context, int64, int64, string,
) (storepkg.DeliveryChannelPreference, error) {
	routeID := int64(44)
	return storepkg.DeliveryChannelPreference{
		Selection: storepkg.DeliveryChannelTelegram, TelegramRouteID: &routeID,
	}, nil
}
func (f *aggregateChannelStoreFake) LoadArtifactDeliveryPlan(
	context.Context, int64, int64, string, string, string,
) (storepkg.ArtifactDeliveryPlan, error) {
	if f.plan == nil {
		return storepkg.ArtifactDeliveryPlan{}, types.NewAppError(
			types.CodeNotFound, "missing", types.ErrNotFound)
	}
	return *f.plan, nil
}
func (f *aggregateChannelStoreFake) PrepareArtifactDeliveryPlan(
	_ context.Context, tenantID, userID int64, taskID, kind, key string,
	preference storepkg.DeliveryChannelPreference,
) (storepkg.ArtifactDeliveryPlan, error) {
	f.prepareCalls++
	f.plan = &storepkg.ArtifactDeliveryPlan{
		ID:       "97e600e8-245a-59b3-a62b-3da056806f37",
		TenantID: tenantID, UserID: userID, TaskID: taskID,
		ArtifactKind: kind, ArtifactKey: key,
		Selection:       preference.Selection,
		TelegramRouteID: preference.TelegramRouteID,
	}
	return *f.plan, nil
}
func (f *aggregateChannelStoreFake) PrepareAggregateTelegramOutbound(
	_ context.Context, _ string, tenantID, userID int64, _ string,
	_ int64, _, _ int, _ []int64, routeID int64, effectID, body string,
) (storepkg.ChannelOutboundEffect, error) {
	f.prepareCalls++
	return storepkg.ChannelOutboundEffect{
		EffectID: effectID, TenantID: tenantID, UserID: userID,
		RouteID: routeID, EffectKind: "aggregate_brief",
		PayloadText: body, Status: "prepared",
	}, nil
}
func (f *aggregateChannelStoreFake) SettleAggregateTelegramOutbound(
	context.Context, int64, int64, string, string,
) error {
	f.settleCalls++
	return nil
}

type aggregateTelegramSenderFake struct {
	calls int
	body  string
}

func (f *aggregateTelegramSenderFake) SendTextEffect(
	_ context.Context, _, _ int64, _ int64, _, _ string, body string,
) error {
	f.calls++
	f.body = body
	return nil
}

func TestPRBCompletePushEffectPlan(t *testing.T) {
	effect := func(index, count int, status pusheffect.Status) *pusheffect.Effect {
		return &pusheffect.Effect{
			Prepared: pusheffect.Prepared{
				ID:         string(rune('a' + index)),
				ChunkIndex: index,
				ChunkCount: count,
			},
			Status: status,
		}
	}
	tests := []struct {
		name         string
		effects      []*pusheffect.Effect
		wantComplete bool
		wantFinish   bool
	}{
		{
			name: "complete frozen plan",
			effects: []*pusheffect.Effect{
				effect(0, 2, pusheffect.StatusPrepared),
				effect(1, 2, pusheffect.StatusPrepared),
			},
			wantComplete: true,
		},
		{
			name: "prepared prefix may be finished before any send",
			effects: []*pusheffect.Effect{
				effect(0, 2, pusheffect.StatusPrepared),
			},
			wantFinish: true,
		},
		{
			name: "partial plan after sending began is frozen closed",
			effects: []*pusheffect.Effect{
				effect(0, 2, pusheffect.StatusSending),
			},
		},
		{
			name: "non contiguous plan is invalid",
			effects: []*pusheffect.Effect{
				effect(1, 2, pusheffect.StatusPrepared),
			},
		},
		{
			name: "mixed chunk counts are invalid",
			effects: []*pusheffect.Effect{
				effect(0, 2, pusheffect.StatusPrepared),
				effect(1, 3, pusheffect.StatusPrepared),
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			complete, safeToFinish := completePushEffectPlan(test.effects)
			if complete != test.wantComplete ||
				safeToFinish != test.wantFinish {
				t.Fatalf(
					"complete/safeToFinish=%v/%v, want %v/%v",
					complete,
					safeToFinish,
					test.wantComplete,
					test.wantFinish,
				)
			}
		})
	}
}

func TestPRBSendFrozenPushEffectsFailsFast(t *testing.T) {
	effects := []*pusheffect.Effect{
		prbFrozenEffect("effect-0", 0, 2),
		prbFrozenEffect("effect-1", 1, 2),
	}
	effectStore := new(prbPushEffectStore)
	pusher := &prbDurablePusher{
		err: errors.New("provider response unknown"),
	}
	compiledStore := &compiledRunStoreFake{authorize: true}
	activities := &Activities{
		pusher:          pusher,
		compiledStore:   compiledStore,
		pushEffectStore: effectStore,
	}
	run := &CompiledRunInputV1{
		TenantID: 7,
		TaskID:   "prb-fail-fast",
	}
	activityFn := func(ctx context.Context) error {
		return activities.sendFrozenPushEffects(ctx, 9, run, effects)
	}
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	env.RegisterActivity(activityFn)
	if _, err := env.ExecuteActivity(activityFn); err == nil {
		t.Fatal("ambiguous first provider attempt returned nil")
	}

	claimed, ambiguous := effectStore.snapshot()
	if len(claimed) != 1 || claimed[0] != effects[0].ID {
		t.Fatalf("claimed effects=%v, want only first effect", claimed)
	}
	if len(ambiguous) != 1 || ambiguous[0] != effects[0].ID {
		t.Fatalf("ambiguous effects=%v, want only first effect", ambiguous)
	}
	if calls := pusher.callCount(); calls != 1 {
		t.Fatalf("provider calls=%d, want fail-fast after first", calls)
	}
	compiledStore.mu.Lock()
	gateCalls := len(compiledStore.authorizeIdentities)
	compiledStore.mu.Unlock()
	if gateCalls != 1 {
		t.Fatalf("side-effect gate calls=%d, want one before first send", gateCalls)
	}
}

func TestPRBSentEffectReplaySkipsProviderAndLiveAuthorization(t *testing.T) {
	effect := prbFrozenEffect("effect-sent", 0, 1)
	effect.Status = pusheffect.StatusSent
	effect.Fence = 3
	effect.ProviderMessageID = "om_already_sent"
	effectStore := new(prbPushEffectStore)
	pusher := new(prbDurablePusher)
	compiledStore := &compiledRunStoreFake{authorize: false}
	activities := &Activities{
		pusher:          pusher,
		compiledStore:   compiledStore,
		pushEffectStore: effectStore,
	}
	run := &CompiledRunInputV1{TenantID: 7, TaskID: "prb-fail-fast"}
	activityFn := func(ctx context.Context) error {
		return activities.sendFrozenPushEffects(
			ctx, 9, run, []*pusheffect.Effect{effect},
		)
	}
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	env.RegisterActivity(activityFn)
	if _, err := env.ExecuteActivity(activityFn); err != nil {
		t.Fatalf("sent receipt replay: %v", err)
	}
	if calls := pusher.callCount(); calls != 0 {
		t.Fatalf("provider calls=%d, want none for sent replay", calls)
	}
	compiledStore.mu.Lock()
	gateCalls := len(compiledStore.authorizeIdentities)
	compiledStore.mu.Unlock()
	if gateCalls != 0 {
		t.Fatalf("live authorization calls=%d, want none for local receipt replay",
			gateCalls)
	}
	if receipts := effectStore.receiptCount(); receipts != 1 {
		t.Fatalf("receipt replay calls=%d, want one", receipts)
	}
}

func TestPRBPushPreparesCompletePlanBeforeFirstProvider(t *testing.T) {
	identity, ref, snapshot := compiledActivityFixture("PRB frozen task")
	deliveryIDs := prbDeliveryIDs(301, aggMaxItemsPerCard+1)
	compiledStore := &compiledRunStoreFake{
		snapshot:           snapshot,
		authorize:          true,
		deliveryIDSequence: append([]int64(nil), deliveryIDs...),
	}
	log := new(prbPushCallLog)
	effectStore := newPRBActivityEffectStore(log, nil)
	pusher := &prbActivityPusher{
		log: log, chatID: "oc_prb_full",
	}
	feishu := &prbEffectFeishu{
		owner: "ou_prb_full",
		chat:  "oc_prb_full",
		app:   "prb-app-full",
	}
	legacyStore := &effectCountingStore{fakeStore: new(fakeStore)}
	activities := prbFullPushActivities(
		compiledStore,
		effectStore,
		pusher,
		feishu,
		legacyStore,
		identity.TaskID,
	)

	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	env.RegisterActivity(activities.Push)
	err := executePushActivity(t, env, activities, PushIn{
		UserID:     identity.UserID,
		ScheduleID: identity.TaskID,
		TraceID:    "trace-prb-prepare-all",
		Run: &CompiledRunInputV1{
			TenantID: identity.TenantID,
			TaskID:   identity.TaskID,
			Snapshot: ref,
		},
		Cards: prbGeneratedCards(len(deliveryIDs)),
	})
	if err != nil {
		t.Fatalf("full effect Push: %v", err)
	}

	events := log.snapshot()
	firstProvider := prbCallIndex(events, "provider:")
	if firstProvider != 2 ||
		!strings.HasPrefix(events[0], "create:0:") ||
		!strings.HasPrefix(events[1], "create:1:") {
		t.Fatalf(
			"first provider occurred before complete preparation: %v",
			events,
		)
	}
	prepared, claimed, receipts := effectStore.snapshot()
	if len(prepared) != 2 || len(claimed) != 2 || len(receipts) != 2 {
		t.Fatalf(
			"effect calls prepared/claimed/receipts=%d/%d/%d, want 2/2/2",
			len(prepared),
			len(claimed),
			len(receipts),
		)
	}
	if !reflect.DeepEqual(
		prepared[0].DeliveryIDs,
		deliveryIDs[:aggMaxItemsPerCard],
	) || !reflect.DeepEqual(
		prepared[1].DeliveryIDs,
		deliveryIDs[aggMaxItemsPerCard:],
	) {
		t.Fatalf(
			"chunk delivery plan drifted: %v / %v",
			prepared[0].DeliveryIDs,
			prepared[1].DeliveryIDs,
		)
	}
	if calls := pusher.snapshot(); len(calls) != 2 {
		t.Fatalf("provider calls=%d, want 2", len(calls))
	}
	if calls := feishu.targetCallCount(); calls != 1 {
		t.Fatalf("provider generation snapshots=%d, want one", calls)
	}
	assertPRBZeroLegacyTerminalWriters(
		t,
		compiledStore,
		legacyStore,
	)
}

func TestPRBAggregateTelegramOnlyUsesProviderChildReceipt(t *testing.T) {
	identity, ref, snapshot := compiledActivityFixture("Telegram aggregate task")
	compiledStore := &compiledRunStoreFake{
		snapshot: snapshot, authorize: true,
		deliveryIDSequence: []int64{701},
	}
	effectStore := newPRBActivityEffectStore(nil, nil)
	feishuPusher := &prbActivityPusher{chatID: "oc_unused"}
	feishu := &prbEffectFeishu{
		owner: "ou_unused", chat: "oc_unused", app: "app_unused",
	}
	channelStore := &aggregateChannelStoreFake{
		effectCountingStore: &effectCountingStore{fakeStore: new(fakeStore)},
	}
	activities := prbFullPushActivities(compiledStore, effectStore,
		feishuPusher, feishu, channelStore, identity.TaskID)
	telegramSender := &aggregateTelegramSenderFake{}
	activities.SetAggregateTelegramSender(telegramSender)
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	env.RegisterActivity(activities.Push)
	err := executePushActivity(t, env, activities, PushIn{
		UserID: identity.UserID, ScheduleID: identity.TaskID,
		TraceID: "trace-telegram-aggregate",
		Run: &CompiledRunInputV1{
			TenantID: identity.TenantID, TaskID: identity.TaskID, Snapshot: ref,
		},
		Cards: prbGeneratedCards(1),
	})
	if err != nil {
		t.Fatal(err)
	}
	prepared, _, _ := effectStore.snapshot()
	if telegramSender.calls != 1 || channelStore.settleCalls != 1 ||
		channelStore.prepareCalls != 2 || len(prepared) != 0 ||
		len(feishuPusher.snapshot()) != 0 ||
		!strings.Contains(telegramSender.body, "PRB item 0") {
		t.Fatalf("telegram=%+v prepare=%d settle=%d feishu_effects=%d feishu_calls=%d",
			telegramSender, channelStore.prepareCalls, channelStore.settleCalls,
			len(prepared), len(feishuPusher.snapshot()))
	}
}

func TestP1ECanonicalRendererSendsOneFrozenPrefixWithWholeBriefReceipt(t *testing.T) {
	identity, ref, snapshot := compiledActivityFixture(
		"P1-E canonical task")
	deliveryIDs := prbDeliveryIDs(351, 5)
	compiledStore := &compiledRunStoreFake{
		snapshot:           snapshot,
		authorize:          true,
		deliveryIDSequence: append([]int64(nil), deliveryIDs...),
	}
	log := new(prbPushCallLog)
	effectStore := newPRBActivityEffectStore(log, nil)
	pusher := &prbActivityPusher{
		log: log, chatID: "oc_p1e",
	}
	feishu := &prbEffectFeishu{
		owner: "ou_p1e", chat: "oc_p1e", app: "p1e-app",
	}
	activities := prbFullPushActivities(
		compiledStore, effectStore, pusher, feishu,
		&effectCountingStore{fakeStore: new(fakeStore)},
		identity.TaskID,
	)
	WithCanonicalBriefRendererV1(
		identity.TaskID, "https://vane.example",
	)(activities)
	var rendered []feedback.AggregateCardInput
	activities.buildAggCard = func(in feedback.AggregateCardInput) string {
		rendered = append(rendered, in)
		return prbEffectCard(in)
	}
	marker := types.RunOutcomeMarkerV1{
		ID: 71, SchemaVersion: types.RunOutcomeSchemaVersionV1,
		RunSnapshotID: ref.SnapshotID,
		TenantID:      identity.TenantID, UserID: identity.UserID,
		TaskID: identity.TaskID,
	}
	insights := make([]types.InsightV1, len(deliveryIDs))
	for i, deliveryID := range deliveryIDs {
		insights[i] = types.InsightV1{
			ID: deliveryID, RankPosition: i + 1,
			Title:        fmt.Sprintf("frozen title %d", i),
			BodyMD:       fmt.Sprintf("frozen body %d", i),
			SourceTitle:  "Frozen Source",
			SourceURL:    fmt.Sprintf("https://frozen.example/%d", i),
			DiscoveredAt: time.Unix(int64(100+i), 0).UTC(),
		}
	}
	draft := &types.BriefDraftV1{
		SchemaVersion: types.BriefSchemaVersionV1,
		RunOutcomeID:  marker.ID, RunSnapshotID: ref.SnapshotID,
		PushBatchID: 101, TenantID: identity.TenantID,
		UserID: identity.UserID, TaskID: identity.TaskID,
		GeneratedAt: time.Unix(200, 0).UTC(),
		Insights:    insights,
	}

	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	env.RegisterActivity(activities.Push)
	err := executePushActivity(t, env, activities, PushIn{
		UserID: identity.UserID, ScheduleID: identity.TaskID,
		TraceID: "trace-p1e-prefix",
		Run: &CompiledRunInputV1{
			TenantID: identity.TenantID, TaskID: identity.TaskID,
			Snapshot: ref,
		},
		Cards:            prbGeneratedCards(len(deliveryIDs)),
		CanonicalOutcome: &marker, CanonicalBrief: draft,
		CanonicalBatchID: 101,
	})
	if err != nil {
		t.Fatalf("canonical renderer Push: %v", err)
	}
	prepared, _, _ := effectStore.snapshot()
	if len(prepared) != 1 {
		t.Fatalf("canonical effects=%d, want one", len(prepared))
	}
	if !reflect.DeepEqual(prepared[0].DeliveryIDs, deliveryIDs) {
		t.Fatalf("canonical receipt ids=%v want=%v",
			prepared[0].DeliveryIDs, deliveryIDs)
	}
	providerCalls := pusher.snapshot()
	if len(providerCalls) != 1 {
		t.Fatalf("canonical provider calls=%d, want one",
			len(providerCalls))
	}
	if len(rendered) < 2 {
		t.Fatalf("canonical render calls=%d, want planning+effect",
			len(rendered))
	}
	finalRender := rendered[len(rendered)-1]
	if finalRender.CanonicalBrief == nil ||
		finalRender.CanonicalBrief.TotalItems != len(deliveryIDs) ||
		finalRender.CanonicalBrief.VisibleItems !=
			canonicalBriefFeishuPrefixItemsV1 ||
		len(finalRender.Items) != canonicalBriefFeishuPrefixItemsV1 {
		t.Fatalf("canonical render envelope=%+v", finalRender)
	}
	for i, item := range finalRender.Items {
		if item.DeliveryID != insights[i].ID ||
			item.Title != insights[i].Title ||
			item.BodyMD != insights[i].BodyMD ||
			item.SourceTitle != insights[i].SourceTitle ||
			item.URL != insights[i].SourceURL ||
			item.Score != 0 || item.Platform != "" {
			t.Fatalf("canonical render item[%d]=%+v want=%+v",
				i, item, insights[i])
		}
	}
	for i, deliveryID := range deliveryIDs {
		visible := i < canonicalBriefFeishuPrefixItemsV1
		if got := strings.Contains(
			providerCalls[0].card,
			strconv.FormatInt(deliveryID, 10),
		); got != visible {
			t.Fatalf("delivery %d visible=%v card=%s",
				deliveryID, got, providerCalls[0].card)
		}
	}
}

func TestP1ERendererRollbackPreservesLegacyChunkPlan(t *testing.T) {
	identity, ref, snapshot := compiledActivityFixture(
		"P1-E rollback task")
	deliveryIDs := prbDeliveryIDs(371, 5)
	compiledStore := &compiledRunStoreFake{
		snapshot: snapshot, authorize: true,
		deliveryIDSequence: append([]int64(nil), deliveryIDs...),
	}
	effectStore := newPRBActivityEffectStore(nil, nil)
	pusher := &prbActivityPusher{chatID: "oc_p1e_rollback"}
	activities := prbFullPushActivities(
		compiledStore, effectStore, pusher,
		&prbEffectFeishu{
			owner: "ou_p1e_rollback", chat: "oc_p1e_rollback",
			app: "p1e-rollback-app",
		},
		&effectCountingStore{fakeStore: new(fakeStore)},
		identity.TaskID,
	)
	// Empty renderer task ID is the explicit rollback state.
	WithCanonicalBriefRendererV1("", "https://vane.example")(activities)
	marker := types.RunOutcomeMarkerV1{
		ID: 72, SchemaVersion: types.RunOutcomeSchemaVersionV1,
		RunSnapshotID: ref.SnapshotID,
		TenantID:      identity.TenantID, UserID: identity.UserID,
		TaskID: identity.TaskID,
	}
	insights := make([]types.InsightV1, len(deliveryIDs))
	for i, deliveryID := range deliveryIDs {
		insights[i] = types.InsightV1{
			ID: deliveryID, RankPosition: i + 1,
			Title: fmt.Sprintf("frozen %d", i), BodyMD: "frozen",
			SourceURL:    fmt.Sprintf("https://frozen.example/%d", i),
			DiscoveredAt: time.Unix(int64(300+i), 0).UTC(),
		}
	}
	draft := &types.BriefDraftV1{
		SchemaVersion: types.BriefSchemaVersionV1,
		RunOutcomeID:  marker.ID, RunSnapshotID: ref.SnapshotID,
		PushBatchID: 101, TenantID: identity.TenantID,
		UserID: identity.UserID, TaskID: identity.TaskID,
		GeneratedAt: time.Unix(400, 0).UTC(), Insights: insights,
	}
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	env.RegisterActivity(activities.Push)
	if err := executePushActivity(t, env, activities, PushIn{
		UserID: identity.UserID, ScheduleID: identity.TaskID,
		TraceID: "trace-p1e-rollback",
		Run: &CompiledRunInputV1{
			TenantID: identity.TenantID, TaskID: identity.TaskID,
			Snapshot: ref,
		},
		Cards:            prbGeneratedCards(len(deliveryIDs)),
		CanonicalOutcome: &marker, CanonicalBrief: draft,
		CanonicalBatchID: 101,
	}); err != nil {
		t.Fatalf("rollback Push: %v", err)
	}
	prepared, _, _ := effectStore.snapshot()
	if len(prepared) != 1 ||
		len(prepared[0].DeliveryIDs) != len(deliveryIDs) {
		t.Fatalf("legacy plan after rollback=%+v", prepared)
	}
	if calls := pusher.snapshot(); len(calls) != 1 ||
		!strings.Contains(calls[0].card,
			strconv.FormatInt(deliveryIDs[len(deliveryIDs)-1], 10)) {
		t.Fatalf("rollback did not preserve full legacy card: %+v", calls)
	}
}

func TestPRBPushCompletesPartialPreparedPlanExactly(t *testing.T) {
	identity, ref, snapshot := compiledActivityFixture("PRB frozen task")
	deliveryIDs := prbDeliveryIDs(401, aggMaxItemsPerCard+1)
	effect0ID := pushEffectID(identity, 0)
	effect1ID := pushEffectID(identity, 1)
	target := &prbEffectFeishu{
		owner: "ou_prb_partial",
		chat:  "oc_prb_partial",
		app:   "prb-app-partial",
	}
	firstCard := prbEffectCard(feedback.AggregateCardInput{
		EffectID: effect0ID,
		Items:    prbCardInputs(deliveryIDs[:aggMaxItemsPerCard]),
	})
	partial := &pusheffect.Effect{
		Prepared: pusheffect.Prepared{
			ID:             effect0ID,
			TenantID:       identity.TenantID,
			UserID:         identity.UserID,
			TaskID:         identity.TaskID,
			RunSnapshotID:  ref.SnapshotID,
			RunID:          identity.TemporalRunID,
			StepID:         pushEffectStepID,
			ChunkIndex:     0,
			ChunkCount:     2,
			BatchID:        101,
			DeliveryIDs:    append([]int64(nil), deliveryIDs[:aggMaxItemsPerCard]...),
			Provider:       pushEffectProvider,
			AppIdentity:    target.app,
			ProviderChatID: target.chat,
			Target:         target.owner,
			Card:           []byte(firstCard),
			ProviderUUID:   effect0ID,
			IdempotencyExpiresAt: time.Now().UTC().
				Add(time.Hour),
		},
		Status: pusheffect.StatusPrepared,
	}
	compiledStore := &compiledRunStoreFake{
		snapshot:           snapshot,
		authorize:          true,
		deliveryIDSequence: append([]int64(nil), deliveryIDs...),
	}
	log := new(prbPushCallLog)
	effectStore := newPRBActivityEffectStore(
		log,
		[]*pusheffect.Effect{partial},
	)
	pusher := &prbActivityPusher{
		log: log, chatID: target.chat,
	}
	legacyStore := &effectCountingStore{fakeStore: new(fakeStore)}
	activities := prbFullPushActivities(
		compiledStore,
		effectStore,
		pusher,
		target,
		legacyStore,
		identity.TaskID,
	)
	WithCanonicalBriefRendererV1(
		identity.TaskID, "https://vane.example",
	)(activities)
	var rendered []feedback.AggregateCardInput
	activities.buildAggCard = func(in feedback.AggregateCardInput) string {
		rendered = append(rendered, in)
		return prbEffectCard(in)
	}
	marker := types.RunOutcomeMarkerV1{
		ID: 72, SchemaVersion: types.RunOutcomeSchemaVersionV1,
		RunSnapshotID: ref.SnapshotID,
		TenantID:      identity.TenantID, UserID: identity.UserID,
		TaskID: identity.TaskID,
	}
	insights := make([]types.InsightV1, len(deliveryIDs))
	for i, deliveryID := range deliveryIDs {
		insights[i] = types.InsightV1{
			ID: deliveryID, RankPosition: i + 1,
			Title:        fmt.Sprintf("canonical retry title %d", i),
			BodyMD:       fmt.Sprintf("canonical retry body %d", i),
			SourceURL:    fmt.Sprintf("https://frozen.example/%d", i),
			DiscoveredAt: time.Unix(int64(300+i), 0).UTC(),
		}
	}
	draft := &types.BriefDraftV1{
		SchemaVersion: types.BriefSchemaVersionV1,
		RunOutcomeID:  marker.ID, RunSnapshotID: ref.SnapshotID,
		PushBatchID: 101, TenantID: identity.TenantID,
		UserID: identity.UserID, TaskID: identity.TaskID,
		GeneratedAt: time.Unix(400, 0).UTC(),
		Insights:    insights,
	}

	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	env.RegisterActivity(activities.Push)
	err := executePushActivity(t, env, activities, PushIn{
		UserID:     identity.UserID,
		ScheduleID: identity.TaskID,
		TraceID:    "trace-prb-partial",
		Run: &CompiledRunInputV1{
			TenantID: identity.TenantID,
			TaskID:   identity.TaskID,
			Snapshot: ref,
		},
		Cards:            prbGeneratedCards(len(deliveryIDs)),
		CanonicalOutcome: &marker, CanonicalBrief: draft,
		CanonicalBatchID: 101,
	})
	if err != nil {
		t.Fatalf("partial prepared plan Push: %v", err)
	}

	prepared, claimed, receipts := effectStore.snapshot()
	if len(prepared) != 2 || len(claimed) != 2 || len(receipts) != 2 {
		t.Fatalf(
			"partial completion prepared/claimed/receipts=%d/%d/%d",
			len(prepared),
			len(claimed),
			len(receipts),
		)
	}
	if prepared[0].ID != effect0ID ||
		prepared[1].ID != effect1ID ||
		prepared[0].ChunkIndex != 0 ||
		prepared[1].ChunkIndex != 1 ||
		prepared[0].ChunkCount != 2 ||
		prepared[1].ChunkCount != 2 {
		t.Fatalf(
			"partial plan was renumbered: %+v / %+v",
			prepared[0],
			prepared[1],
		)
	}
	if !bytes.Equal(prepared[0].Card, partial.Card) ||
		prepared[0].Target != partial.Target ||
		prepared[0].ProviderChatID != partial.ProviderChatID ||
		prepared[0].AppIdentity != partial.AppIdentity ||
		!reflect.DeepEqual(
			prepared[0].DeliveryIDs,
			partial.DeliveryIDs,
		) {
		t.Fatalf(
			"partial chunk zero drifted card/target/binding: %+v",
			prepared[0],
		)
	}
	providerCalls := pusher.snapshot()
	if len(providerCalls) != 2 ||
		providerCalls[0].uuid != effect0ID ||
		providerCalls[1].uuid != effect1ID ||
		providerCalls[0].target != partial.Target ||
		providerCalls[0].card != string(partial.Card) {
		t.Fatalf("partial replay provider calls drifted: %+v", providerCalls)
	}
	if calls := target.targetCallCount(); calls != 1 {
		t.Fatalf("provider generation snapshots=%d, want one", calls)
	}
	for i, render := range rendered {
		if render.CanonicalBrief != nil {
			t.Fatalf(
				"partial legacy plan switched to canonical render[%d]=%+v",
				i, render.CanonicalBrief,
			)
		}
	}
	assertPRBZeroLegacyTerminalWriters(
		t,
		compiledStore,
		legacyStore,
	)
}

func TestP1ECanonicalRendererReservesWorstCaseFeedbackBytes(t *testing.T) {
	identity, ref, snapshot := compiledActivityFixture(
		"P1-E feedback byte reserve")
	deliveryIDs := prbDeliveryIDs(451, 4)
	compiledStore := &compiledRunStoreFake{
		snapshot: snapshot, authorize: true,
		deliveryIDSequence: append([]int64(nil), deliveryIDs...),
	}
	log := new(prbPushCallLog)
	effectStore := newPRBActivityEffectStore(log, nil)
	pusher := &prbActivityPusher{log: log, chatID: "oc_p1e_size"}
	target := &prbEffectFeishu{
		owner: "ou_p1e_size", chat: "oc_p1e_size", app: "p1e-size-app",
	}
	activities := prbFullPushActivities(
		compiledStore, effectStore, pusher, target,
		&effectCountingStore{fakeStore: new(fakeStore)}, identity.TaskID,
	)
	WithCanonicalBriefRendererV1(
		identity.TaskID, "https://vane.example",
	)(activities)
	var rendered []feedback.AggregateCardInput
	activities.buildAggCard = func(in feedback.AggregateCardInput) string {
		rendered = append(rendered, in)
		if len(in.Items) == canonicalBriefFeishuPrefixItemsV1 {
			completeWorstCase := true
			openForms := 0
			for _, item := range in.Items {
				if item.State.BadFeedbackOpen {
					openForms++
				} else if !item.State.Misjudged {
					completeWorstCase = false
				}
				if item.State.Preference !=
					types.FeedbackActionNotInterested ||
					!item.State.DeepDiveRequested {
					completeWorstCase = false
				}
			}
			if completeWorstCase && openForms == 1 {
				return strings.Repeat(
					"x", feedback.AggregateCardMaxBytesV1+1)
			}
		}
		return prbEffectCard(in)
	}
	marker := types.RunOutcomeMarkerV1{
		ID: 73, SchemaVersion: types.RunOutcomeSchemaVersionV1,
		RunSnapshotID: ref.SnapshotID,
		TenantID:      identity.TenantID, UserID: identity.UserID,
		TaskID: identity.TaskID,
	}
	insights := make([]types.InsightV1, len(deliveryIDs))
	for i, deliveryID := range deliveryIDs {
		insights[i] = types.InsightV1{
			ID: deliveryID, RankPosition: i + 1,
			Title:        fmt.Sprintf("size title %d", i),
			BodyMD:       fmt.Sprintf("size body %d", i),
			SourceURL:    fmt.Sprintf("https://size.example/%d", i),
			DiscoveredAt: time.Unix(int64(500+i), 0).UTC(),
		}
	}
	draft := &types.BriefDraftV1{
		SchemaVersion: types.BriefSchemaVersionV1,
		RunOutcomeID:  marker.ID, RunSnapshotID: ref.SnapshotID,
		PushBatchID: 101, TenantID: identity.TenantID,
		UserID: identity.UserID, TaskID: identity.TaskID,
		GeneratedAt: time.Unix(600, 0).UTC(), Insights: insights,
	}
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	env.RegisterActivity(activities.Push)
	err := executePushActivity(t, env, activities, PushIn{
		UserID: identity.UserID, ScheduleID: identity.TaskID,
		TraceID: "trace-p1e-size",
		Run: &CompiledRunInputV1{
			TenantID: identity.TenantID, TaskID: identity.TaskID,
			Snapshot: ref,
		},
		Cards:            prbGeneratedCards(len(deliveryIDs)),
		CanonicalOutcome: &marker, CanonicalBrief: draft,
		CanonicalBatchID: 101,
	})
	if err != nil {
		t.Fatalf("canonical feedback reserve Push: %v", err)
	}
	prepared, _, _ := effectStore.snapshot()
	if len(prepared) != 1 {
		t.Fatalf("prepared canonical effects=%d, want one", len(prepared))
	}
	var sent *feedback.AggregateCardInput
	for i := range rendered {
		render := &rendered[i]
		if render.CanonicalBrief != nil &&
			render.CanonicalBrief.VisibleItems == 2 &&
			!render.Items[0].State.BadFeedbackOpen &&
			!render.Items[1].State.BadFeedbackOpen {
			sent = render
		}
	}
	if sent == nil || len(sent.Items) != 2 {
		t.Fatalf("canonical visible prefix did not reserve feedback bytes: %+v",
			rendered)
	}
}

func TestPRBPushRevokedBeforeSecondChunkStopsSecondProvider(t *testing.T) {
	identity, ref, snapshot := compiledActivityFixture("PRB frozen task")
	deliveryIDs := prbDeliveryIDs(501, aggMaxItemsPerCard+1)
	compiledStore := &compiledRunStoreFake{
		snapshot:           snapshot,
		authorize:          true,
		deliveryIDSequence: append([]int64(nil), deliveryIDs...),
	}
	log := new(prbPushCallLog)
	effectStore := newPRBActivityEffectStore(log, nil)
	pusher := &prbActivityPusher{
		log: log, chatID: "oc_prb_revoke",
		afterCall: func(call int) {
			if call != 1 {
				return
			}
			compiledStore.mu.Lock()
			compiledStore.authorize = false
			compiledStore.mu.Unlock()
		},
	}
	target := &prbEffectFeishu{
		owner: "ou_prb_revoke",
		chat:  "oc_prb_revoke",
		app:   "prb-app-revoke",
	}
	legacyStore := &effectCountingStore{fakeStore: new(fakeStore)}
	activities := prbFullPushActivities(
		compiledStore,
		effectStore,
		pusher,
		target,
		legacyStore,
		identity.TaskID,
	)

	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	env.RegisterActivity(activities.Push)
	err := executePushActivity(t, env, activities, PushIn{
		UserID:     identity.UserID,
		ScheduleID: identity.TaskID,
		TraceID:    "trace-prb-revoke-second",
		Run: &CompiledRunInputV1{
			TenantID: identity.TenantID,
			TaskID:   identity.TaskID,
			Snapshot: ref,
		},
		Cards: prbGeneratedCards(len(deliveryIDs)),
	})
	if err == nil {
		t.Fatal("revocation before second chunk returned nil")
	}
	prepared, claimed, receipts := effectStore.snapshot()
	if len(prepared) != 2 {
		t.Fatalf("plan was not fully prepared before send: %d", len(prepared))
	}
	if len(claimed) == 0 ||
		claimed[0] != prepared[0].ID ||
		len(receipts) != 1 {
		t.Fatalf(
			"revoked second chunk claims/receipts=%v/%d, want first claimed and receipted",
			claimed,
			len(receipts),
		)
	}
	if calls := pusher.snapshot(); len(calls) != 1 {
		t.Fatalf("provider calls=%d, want second provider=0", len(calls))
	}
	if released := effectStore.definiteFailures(); !reflect.DeepEqual(
		released,
		[]string{prepared[1].ID},
	) {
		t.Fatalf(
			"revoked no-attempt effect releases=%v, want [%s]",
			released,
			prepared[1].ID,
		)
	}
	assertPRBZeroLegacyTerminalWriters(
		t,
		compiledStore,
		legacyStore,
	)
}

func TestPRBCanaryOffEffectWinnerOnlySettlesCompleteSentPlan(t *testing.T) {
	identity, ref, snapshot := compiledActivityFixture("PRB rollback task")
	effects := []*pusheffect.Effect{
		prbCanaryOffEffect(identity, ref, 0, 2, pusheffect.StatusSent),
		prbCanaryOffEffect(identity, ref, 1, 2, pusheffect.StatusSent),
	}
	effectStore := newPRBActivityEffectStore(nil, effects)
	compiledStore := &compiledRunStoreFake{
		snapshot:  snapshot,
		authorize: true,
	}
	pusher := new(prbActivityPusher)
	target := &prbEffectFeishu{
		owner: "ou_rollback",
		chat:  "oc_rollback",
		app:   "prb-app-rollback",
	}
	legacyStore := &effectCountingStore{fakeStore: new(fakeStore)}
	activities := prbFullPushActivities(
		compiledStore,
		effectStore,
		pusher,
		target,
		legacyStore,
		"",
	)

	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	env.RegisterActivity(activities.Push)
	err := executePushActivity(t, env, activities, PushIn{
		UserID:     identity.UserID,
		ScheduleID: identity.TaskID,
		TraceID:    "trace-prb-canary-off-sent",
		Run: &CompiledRunInputV1{
			TenantID: identity.TenantID,
			TaskID:   identity.TaskID,
			Snapshot: ref,
		},
	})
	if err != nil {
		t.Fatalf("canary-off sent settlement: %v", err)
	}
	_, claimed, receipts := effectStore.snapshot()
	if len(claimed) != 0 || len(receipts) != len(effects) ||
		effectStore.settlementCount() != 1 {
		t.Fatalf("canary-off local settlement claims/receipts/settles=%d/%d/%d",
			len(claimed), len(receipts), effectStore.settlementCount())
	}
	if calls := pusher.snapshot(); len(calls) != 0 {
		t.Fatalf("canary-off sent replay called provider %d times", len(calls))
	}
	compiledStore.mu.Lock()
	authorizationCalls := len(compiledStore.authorizeIdentities)
	compiledStore.mu.Unlock()
	desired := effectStore.authorityDesired()
	if authorizationCalls != 1 {
		t.Fatalf("canary-off sent replay authorization calls=%d, want only batch admission",
			authorizationCalls)
	}
	if !reflect.DeepEqual(
		desired,
		[]types.PushBatchDeliveryAuthority{
			types.PushBatchDeliveryAuthorityLegacy,
		},
	) {
		t.Fatalf("canary-off desired authority=%v, want legacy request", desired)
	}
	assertPRBZeroLegacyTerminalWriters(t, compiledStore, legacyStore)
}

func TestPRBCanaryOffEffectWinnerFreezesEveryNonterminalPlan(t *testing.T) {
	identity, ref, snapshot := compiledActivityFixture("PRB rollback task")
	tests := []struct {
		name    string
		effects []*pusheffect.Effect
	}{
		{
			name: "prepared",
			effects: []*pusheffect.Effect{
				prbCanaryOffEffect(
					identity, ref, 0, 1, pusheffect.StatusPrepared),
			},
		},
		{
			name: "definite failed",
			effects: []*pusheffect.Effect{
				prbCanaryOffEffect(
					identity, ref, 0, 1, pusheffect.StatusDefiniteFailed),
			},
		},
		{
			name: "sending",
			effects: []*pusheffect.Effect{
				prbCanaryOffEffect(
					identity, ref, 0, 1, pusheffect.StatusSending),
			},
		},
		{
			name: "ambiguous",
			effects: []*pusheffect.Effect{
				prbCanaryOffEffect(
					identity, ref, 0, 1, pusheffect.StatusAmbiguous),
			},
		},
		{
			name: "incomplete sent and prepared",
			effects: []*pusheffect.Effect{
				prbCanaryOffEffect(
					identity, ref, 0, 3, pusheffect.StatusSent),
				prbCanaryOffEffect(
					identity, ref, 1, 3, pusheffect.StatusPrepared),
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			effectStore := newPRBActivityEffectStore(nil, test.effects)
			compiledStore := &compiledRunStoreFake{
				snapshot:  snapshot,
				authorize: true,
			}
			pusher := new(prbActivityPusher)
			legacyStore := &effectCountingStore{fakeStore: new(fakeStore)}
			activities := prbFullPushActivities(
				compiledStore,
				effectStore,
				pusher,
				&prbEffectFeishu{
					owner: "ou_rollback",
					chat:  "oc_rollback",
					app:   "prb-app-rollback",
				},
				legacyStore,
				"",
			)
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestActivityEnvironment()
			env.RegisterActivity(activities.Push)
			err := executePushActivity(t, env, activities, PushIn{
				UserID:     identity.UserID,
				ScheduleID: identity.TaskID,
				TraceID:    "trace-prb-canary-off-" + test.name,
				Run: &CompiledRunInputV1{
					TenantID: identity.TenantID,
					TaskID:   identity.TaskID,
					Snapshot: ref,
				},
			})
			if err == nil {
				t.Fatal("canary-off nonterminal plan returned nil")
			}
			_, claimed, receipts := effectStore.snapshot()
			compiledStore.mu.Lock()
			authorizationCalls := len(compiledStore.authorizeIdentities)
			compiledStore.mu.Unlock()
			desired := effectStore.authorityDesired()
			if len(claimed) != 0 || len(receipts) != 0 ||
				effectStore.settlementCount() != 0 ||
				authorizationCalls != 1 ||
				len(pusher.snapshot()) != 0 ||
				!reflect.DeepEqual(
					desired,
					[]types.PushBatchDeliveryAuthority{
						types.PushBatchDeliveryAuthorityLegacy,
					},
				) {
				t.Fatalf(
					"canary-off nonterminal escaped freeze claims=%d receipts=%d settles=%d auth=%d provider=%d desired=%v",
					len(claimed),
					len(receipts),
					effectStore.settlementCount(),
					authorizationCalls,
					len(pusher.snapshot()),
					desired,
				)
			}
			assertPRBZeroLegacyTerminalWriters(
				t, compiledStore, legacyStore)
		})
	}
}

func prbCanaryOffEffect(
	identity types.RunIdentity,
	ref types.RunSnapshotRef,
	index, count int,
	status pusheffect.Status,
) *pusheffect.Effect {
	id := pushEffectID(identity, index)
	effect := &pusheffect.Effect{
		Prepared: pusheffect.Prepared{
			ID:             id,
			TenantID:       identity.TenantID,
			UserID:         identity.UserID,
			TaskID:         identity.TaskID,
			RunSnapshotID:  ref.SnapshotID,
			RunID:          identity.TemporalRunID,
			StepID:         pushEffectStepID,
			ChunkIndex:     index,
			ChunkCount:     count,
			BatchID:        101,
			DeliveryIDs:    []int64{int64(900 + index)},
			Provider:       pushEffectProvider,
			AppIdentity:    "prb-app-rollback",
			ProviderChatID: "oc_rollback",
			Target:         "ou_rollback",
			Card:           []byte(`{"rollback":true}`),
			ProviderUUID:   id,
			IdempotencyExpiresAt: time.Now().UTC().
				Add(time.Hour),
		},
		Status: status,
	}
	if status == pusheffect.StatusSent {
		effect.Fence = 1
		effect.ProviderMessageID = fmt.Sprintf("om_rollback_%d", index)
	}
	return effect
}

func prbFrozenEffect(
	id string,
	chunkIndex, chunkCount int,
) *pusheffect.Effect {
	return &pusheffect.Effect{
		Prepared: pusheffect.Prepared{
			ID:             id,
			TenantID:       7,
			UserID:         9,
			TaskID:         "prb-fail-fast",
			RunSnapshotID:  11,
			RunID:          testActivityRunID,
			StepID:         pushEffectStepID,
			ChunkIndex:     chunkIndex,
			ChunkCount:     chunkCount,
			BatchID:        13,
			DeliveryIDs:    []int64{int64(20 + chunkIndex)},
			Provider:       pushEffectProvider,
			AppIdentity:    "prb-app",
			ProviderChatID: "oc_prb",
			Target:         "ou_prb",
			Card:           []byte(`{"prb":true}`),
			ProviderUUID:   id,
			IdempotencyExpiresAt: time.Now().UTC().
				Add(time.Hour),
		},
		Status: pusheffect.StatusPrepared,
	}
}

type prbPushEffectStore struct {
	mu        sync.Mutex
	claimed   []string
	ambiguous []string
	receipts  int
}

func (s *prbPushEffectStore) ClaimPushBatchDeliveryAuthority(
	context.Context,
	types.PushBatchScope,
	types.PushBatchDeliveryAuthority,
) (types.PushBatchDeliveryAuthority, error) {
	return types.PushBatchDeliveryAuthorityEffect, nil
}

func (s *prbPushEffectStore) CreatePushEffect(
	_ context.Context,
	prepared pusheffect.Prepared,
) (*pusheffect.Effect, error) {
	return &pusheffect.Effect{
		Prepared: prepared,
		Status:   pusheffect.StatusPrepared,
	}, nil
}

func (s *prbPushEffectStore) ClaimPushEffect(
	_ context.Context,
	params pusheffect.ClaimParams,
) (*pusheffect.Effect, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.claimed = append(s.claimed, params.ID)
	return &pusheffect.Effect{
		Prepared: pusheffect.Prepared{
			ID:             params.ID,
			TenantID:       params.TenantID,
			UserID:         params.UserID,
			AppIdentity:    "prb-app",
			ProviderChatID: "oc_prb",
			Target:         "ou_prb",
			Card:           []byte(`{"prb":true}`),
			ProviderUUID:   params.ID,
		},
		Status:     pusheffect.StatusSending,
		LeaseOwner: params.LeaseOwner,
		Fence:      1,
	}, nil
}

func (s *prbPushEffectStore) RecordPushEffectDefiniteFailure(
	context.Context,
	pusheffect.FailureParams,
) error {
	return nil
}

func (s *prbPushEffectStore) RecordPushEffectAmbiguous(
	_ context.Context,
	params pusheffect.FailureParams,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ambiguous = append(s.ambiguous, params.ID)
	return nil
}

func (s *prbPushEffectStore) ListPushEffectsForBatch(
	context.Context,
	types.PushBatchScope,
	int64,
) ([]*pusheffect.Effect, error) {
	return nil, nil
}

func (s *prbPushEffectStore) CompleteEmptyPushEffectBatch(
	context.Context,
	types.PushBatchScope,
	int64,
) error {
	return nil
}

func (s *prbPushEffectStore) RecordPushEffectSentWithDeliveries(
	context.Context,
	pusheffect.SentReceipt,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.receipts++
	return nil
}

func (s *prbPushEffectStore) SettlePushEffectBatchReceipt(
	context.Context,
	types.PushBatchScope,
	int64,
) error {
	return nil
}

func (s *prbPushEffectStore) snapshot() (
	claimed []string,
	ambiguous []string,
) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.claimed...),
		append([]string(nil), s.ambiguous...)
}

func (s *prbPushEffectStore) receiptCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.receipts
}

type prbDurablePusher struct {
	mu    sync.Mutex
	calls int
	err   error
}

func (p *prbDurablePusher) Push(
	context.Context,
	string,
	string,
) (string, error) {
	return "", errors.New("legacy push must not be called")
}

func (p *prbDurablePusher) PushWithUUID(
	_ context.Context,
	appIdentity string,
	_ string,
	_ string,
	_ string,
) (pusheffect.ProviderObservation, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls++
	return pusheffect.ProviderObservation{
		Disposition: pusheffect.AttemptAmbiguous,
		AppIdentity: appIdentity,
	}, p.err
}

func (p *prbDurablePusher) callCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls
}

type prbPushCallLog struct {
	mu     sync.Mutex
	events []string
}

func (l *prbPushCallLog) add(event string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, event)
}

func (l *prbPushCallLog) snapshot() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.events...)
}

func prbCallIndex(events []string, prefix string) int {
	for index, event := range events {
		if strings.HasPrefix(event, prefix) {
			return index
		}
	}
	return -1
}

type prbActivityEffectStore struct {
	mu sync.Mutex

	log      *prbPushCallLog
	initial  []*pusheffect.Effect
	effects  map[string]*pusheffect.Effect
	order    []string
	prepared []pusheffect.Prepared
	claimed  []string
	receipts []pusheffect.SentReceipt
	definite []string
	settles  int
	desired  []types.PushBatchDeliveryAuthority
	empty    []types.PushBatchScope
}

func newPRBActivityEffectStore(
	log *prbPushCallLog,
	initial []*pusheffect.Effect,
) *prbActivityEffectStore {
	store := &prbActivityEffectStore{
		log:     log,
		effects: make(map[string]*pusheffect.Effect),
	}
	for _, effect := range initial {
		cloned := prbCloneEffect(effect)
		store.initial = append(store.initial, cloned)
		store.effects[cloned.ID] = cloned
		store.order = append(store.order, cloned.ID)
	}
	return store
}

func (s *prbActivityEffectStore) ClaimPushBatchDeliveryAuthority(
	_ context.Context,
	_ types.PushBatchScope,
	desired types.PushBatchDeliveryAuthority,
) (types.PushBatchDeliveryAuthority, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.desired = append(s.desired, desired)
	return types.PushBatchDeliveryAuthorityEffect, nil
}

func (s *prbActivityEffectStore) CreatePushEffect(
	_ context.Context,
	prepared pusheffect.Prepared,
) (*pusheffect.Effect, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	prepared = prbClonePrepared(prepared)
	s.prepared = append(s.prepared, prepared)
	if s.log != nil {
		s.log.add(fmt.Sprintf(
			"create:%d:%s",
			prepared.ChunkIndex,
			prepared.ID,
		))
	}
	if existing := s.effects[prepared.ID]; existing != nil {
		return prbCloneEffect(existing), nil
	}
	effect := &pusheffect.Effect{
		Prepared: prepared,
		Status:   pusheffect.StatusPrepared,
	}
	s.effects[prepared.ID] = effect
	s.order = append(s.order, prepared.ID)
	return prbCloneEffect(effect), nil
}

func (s *prbActivityEffectStore) ClaimPushEffect(
	_ context.Context,
	params pusheffect.ClaimParams,
) (*pusheffect.Effect, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	effect := s.effects[params.ID]
	if effect == nil {
		return nil, errors.New("claim unknown PRB effect")
	}
	s.claimed = append(s.claimed, params.ID)
	effect.Status = pusheffect.StatusSending
	effect.LeaseOwner = params.LeaseOwner
	effect.Fence++
	return prbCloneEffect(effect), nil
}

func (s *prbActivityEffectStore) RecordPushEffectDefiniteFailure(
	_ context.Context,
	params pusheffect.FailureParams,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.definite = append(s.definite, params.ID)
	if effect := s.effects[params.ID]; effect != nil {
		effect.Status = pusheffect.StatusDefiniteFailed
	}
	return nil
}

func (s *prbActivityEffectStore) RecordPushEffectAmbiguous(
	_ context.Context,
	params pusheffect.FailureParams,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if effect := s.effects[params.ID]; effect != nil {
		effect.Status = pusheffect.StatusAmbiguous
	}
	return nil
}

func (s *prbActivityEffectStore) ListPushEffectsForBatch(
	context.Context,
	types.PushBatchScope,
	int64,
) ([]*pusheffect.Effect, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]*pusheffect.Effect, 0, len(s.initial))
	for _, effect := range s.initial {
		result = append(result, prbCloneEffect(effect))
	}
	return result, nil
}

func (s *prbActivityEffectStore) CompleteEmptyPushEffectBatch(
	_ context.Context,
	scope types.PushBatchScope,
	_ int64,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.empty = append(s.empty, scope)
	return nil
}

func (s *prbActivityEffectStore) RecordPushEffectSentWithDeliveries(
	_ context.Context,
	receipt pusheffect.SentReceipt,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	effect := s.effects[receipt.ID]
	if effect == nil {
		return errors.New("receipt for unknown PRB effect")
	}
	s.receipts = append(s.receipts, receipt)
	effect.Status = pusheffect.StatusSent
	effect.ProviderMessageID = receipt.ProviderMessageID
	if s.log != nil {
		s.log.add("receipt:" + receipt.ID)
	}
	return nil
}

func (s *prbActivityEffectStore) SettlePushEffectBatchReceipt(
	context.Context,
	types.PushBatchScope,
	int64,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.settles++
	return nil
}

func (s *prbActivityEffectStore) snapshot() (
	prepared []pusheffect.Prepared,
	claimed []string,
	receipts []pusheffect.SentReceipt,
) {
	s.mu.Lock()
	defer s.mu.Unlock()
	prepared = make([]pusheffect.Prepared, len(s.prepared))
	for index := range s.prepared {
		prepared[index] = prbClonePrepared(s.prepared[index])
	}
	return prepared,
		append([]string(nil), s.claimed...),
		append([]pusheffect.SentReceipt(nil), s.receipts...)
}

func (s *prbActivityEffectStore) definiteFailures() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.definite...)
}

func (s *prbActivityEffectStore) settlementCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.settles
}

func (s *prbActivityEffectStore) authorityDesired() []types.PushBatchDeliveryAuthority {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]types.PushBatchDeliveryAuthority(nil), s.desired...)
}

type prbProviderCall struct {
	app    string
	target string
	card   string
	uuid   string
}

type prbActivityPusher struct {
	mu sync.Mutex

	log       *prbPushCallLog
	chatID    string
	calls     []prbProviderCall
	legacy    int
	afterCall func(int)
}

func (p *prbActivityPusher) Push(
	context.Context,
	string,
	string,
) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.legacy++
	return "", errors.New("legacy provider must not be called")
}

func (p *prbActivityPusher) PushWithUUID(
	_ context.Context,
	appIdentity string,
	target string,
	card string,
	providerUUID string,
) (pusheffect.ProviderObservation, error) {
	p.mu.Lock()
	p.calls = append(p.calls, prbProviderCall{
		app:    appIdentity,
		target: target,
		card:   card,
		uuid:   providerUUID,
	})
	call := len(p.calls)
	afterCall := p.afterCall
	p.mu.Unlock()
	if p.log != nil {
		p.log.add(fmt.Sprintf("provider:%d:%s", call-1, providerUUID))
	}
	if afterCall != nil {
		afterCall(call)
	}
	return pusheffect.ProviderObservation{
		Disposition: pusheffect.AttemptSent,
		AppIdentity: appIdentity,
		MessageID:   fmt.Sprintf("om_prb_%d", call),
		ChatID:      p.chatID,
	}, nil
}

func (p *prbActivityPusher) snapshot() []prbProviderCall {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]prbProviderCall(nil), p.calls...)
}

type prbEffectFeishu struct {
	mu sync.Mutex

	owner       string
	chat        string
	app         string
	targetCalls int
}

func (f *prbEffectFeishu) OwnerOpenID() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.owner
}

func (f *prbEffectFeishu) OwnerChatID() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.chat
}

func (f *prbEffectFeishu) AppIdentity() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.app
}

func (f *prbEffectFeishu) PushEffectTarget() (
	ownerOpenID, ownerChatID, appIdentity string,
) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.targetCalls++
	return f.owner, f.chat, f.app
}

func (f *prbEffectFeishu) targetCallCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.targetCalls
}

func prbFullPushActivities(
	compiledStore *compiledRunStoreFake,
	effectStore PushEffectStore,
	pusher Pusher,
	feishu FeishuManager,
	legacyStore Store,
	taskID string,
) *Activities {
	return NewActivities(
		fakeFetcher{},
		fakeScorer{},
		fakeCardGen{},
		pusher,
		legacyStore,
		feishu,
		nil,
		nil,
		prbEffectCard,
		func(string, int) (string, string) {
			return "PRB", "blue"
		},
		WithCompiledRuntimeV1(
			compiledStore,
			func(
				context.Context,
				int64,
				bool,
			) (runtimepolicy.BundleV1, error) {
				return runtimepolicy.BundleV1{}, nil
			},
			new(compiledModelResolverFake),
		),
		WithPushEffectCanary(effectStore, taskID),
	)
}

func prbEffectCard(in feedback.AggregateCardInput) string {
	deliveryIDs := make([]string, len(in.Items))
	for index := range in.Items {
		deliveryIDs[index] = strconv.FormatInt(
			in.Items[index].DeliveryID,
			10,
		)
	}
	return fmt.Sprintf(
		`{"effect_id":%q,"delivery_ids":[%s]}`,
		in.EffectID,
		strings.Join(deliveryIDs, ","),
	)
}

func prbCardInputs(deliveryIDs []int64) []feedback.CardInput {
	inputs := make([]feedback.CardInput, len(deliveryIDs))
	for index, deliveryID := range deliveryIDs {
		inputs[index] = feedback.CardInput{
			DeliveryID: deliveryID,
		}
	}
	return inputs
}

func prbGeneratedCards(count int) []GeneratedCard {
	cards := make([]GeneratedCard, count)
	for index := range cards {
		cards[index] = GeneratedCard{
			Scored: types.ScoredItem{
				Item: types.ContentItem{
					ID:       int64(700 + index),
					SourceID: 10,
					Title:    fmt.Sprintf("PRB item %d", index),
					URL:      fmt.Sprintf("https://prb.invalid/%d", index),
				},
				Score: 88,
			},
			BodyMD: fmt.Sprintf("PRB body %d", index),
		}
	}
	return cards
}

func prbDeliveryIDs(first int64, count int) []int64 {
	ids := make([]int64, count)
	for index := range ids {
		ids[index] = first + int64(index)
	}
	return ids
}

func prbClonePrepared(in pusheffect.Prepared) pusheffect.Prepared {
	out := in
	out.DeliveryIDs = append([]int64(nil), in.DeliveryIDs...)
	out.ObservationEventKeys = append(
		[]string(nil),
		in.ObservationEventKeys...,
	)
	out.Card = append([]byte(nil), in.Card...)
	return out
}

func prbCloneEffect(in *pusheffect.Effect) *pusheffect.Effect {
	if in == nil {
		return nil
	}
	out := *in
	out.Prepared = prbClonePrepared(in.Prepared)
	return &out
}

func assertPRBZeroLegacyTerminalWriters(
	t *testing.T,
	compiledStore *compiledRunStoreFake,
	legacyStore *effectCountingStore,
) {
	t.Helper()
	compiledStore.mu.Lock()
	deliveryReceipts := compiledStore.deliveryReceipts
	batchStatuses := compiledStore.batchStatuses
	compiledStore.mu.Unlock()
	if deliveryReceipts != 0 || batchStatuses != 0 {
		t.Fatalf(
			"effect path called legacy compiled terminal writers: deliveries=%d batches=%d",
			deliveryReceipts,
			batchStatuses,
		)
	}
	_, _, batches, inserts, marks, statuses, _ :=
		legacyStore.effectCounts()
	if batches != 0 || inserts != 0 || marks != 0 || statuses != 0 {
		t.Fatalf(
			"effect path escaped to legacy store: batches=%d inserts=%d marks=%d statuses=%d",
			batches,
			inserts,
			marks,
			statuses,
		)
	}
}
