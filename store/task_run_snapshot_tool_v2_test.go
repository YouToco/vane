package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/YouToco/vane/observation"
	"github.com/YouToco/vane/runcontext"
	"github.com/YouToco/vane/runtimepolicy"
	"github.com/YouToco/vane/taskstate"
	"github.com/YouToco/vane/types"
)

func TestCompiledTaskRunSnapshotV2_FreezesSourceFreeRunAndReplaysExactly(
	t *testing.T,
) {
	st := tenantTestStore(t)
	f := newCompiledTaskFixture(t, st)
	cleanupA5Fixture(t, st, f)
	query := "snapshot-v2-" + uuid.NewString()
	target := validA5PlanSource(t, query, "Source-free snapshot Tool")
	target.ToolName = "web_search"
	target.ToolArgs = json.RawMessage(`{"query":"` + query + `"}`)
	creation := preparedA5CommitWithSources(
		t, st, f, "snapshot-tool-v2", target)
	ctx := t.Context()

	if err := st.CommitPausedCompiledTaskDefinitionForCreation(
		ctx, creation); err != nil {
		t.Fatalf("commit Source-free task: %v", err)
	}
	started, err := st.BeginTaskCreationActivation(
		ctx, creation.Lease, creation.Definition.TaskID)
	if err != nil || !started {
		t.Fatalf("begin Source-free task activation: started=%v err=%v", started, err)
	}
	if err := st.CommitTaskCreationActivation(
		ctx, creation.Lease, creation.Definition.TaskID); err != nil {
		t.Fatalf("activate Source-free task: %v", err)
	}
	if err := st.CompleteTaskCreationOperation(
		ctx, creation.Lease, creation.Definition.TaskID,
		json.RawMessage(`{"created":true}`)); err != nil {
		t.Fatalf("complete Source-free task: %v", err)
	}

	policy := testCompiledRunPolicyV1(t)
	identity := types.RunIdentity{
		TemporalWorkflowID: scheduledTaskWorkflowID(creation.Definition.TaskID),
		TemporalRunID:      uuid.NewString(),
		RunKind:            types.RunSnapshotKindScheduled,
		TenantID:           creation.Lease.TenantID,
		UserID:             creation.Lease.UserID,
		TaskID:             creation.Definition.TaskID,
	}
	ref, err := st.CreateOrGetCompiledRunSnapshotV2(
		ctx, identity, policy, observation.RolloutAuthority)
	if err != nil {
		t.Fatalf("create Source-free run snapshot: %v", err)
	}
	if ref.SchemaVersion != types.RunSnapshotSchemaVersionV2 ||
		ref.AdaptiveVersion != 1 || ref.PlannerBudget != (types.PlannerBudget{}) ||
		ref.ValidateFor(identity) != nil {
		t.Fatalf("Source-free run reference differs: %+v", ref)
	}
	if authorized, err := st.AuthorizeTaskRunSideEffectV2(
		ctx, identity, ref); err != nil || !authorized {
		t.Fatalf("authorize Source-free run: authorized=%v err=%v",
			authorized, err)
	}
	assertToolRunSnapshotV2RecoveryWaitsForWriter(t, st, identity)
	assertToolRunSnapshotV2AdmissionRejects(t, st, identity, ref.SnapshotID,
		"non-v2 adaptive schema",
		`UPDATE task_adaptive_states
		    SET schema_version='vane.task-adaptive-state/not-v2'
		  WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3`,
		types.ExecutionModeCompiled)
	assertToolRunSnapshotV2AdmissionRejects(t, st, identity, ref.SnapshotID,
		"adaptive state with legacy fallback",
		`UPDATE task_adaptive_states
		    SET last_known_good_definition_version=basis_definition_version
		  WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3`,
		types.ExecutionModeCompiled)
	assertToolRunSnapshotV2AdmissionRejects(t, st, identity, ref.SnapshotID,
		"non-compiled snapshot mode", "", types.ExecutionModeDiscoverAtRun)
	tx, err := st.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `SELECT set_config('app.tenant_id',$1,true)`,
		fmt.Sprintf("%d", identity.TenantID)); err != nil {
		t.Fatal(err)
	}
	var legacyPointer int64
	if err := tx.QueryRow(ctx,
		`INSERT INTO task_run_snapshot_v2_cutover_events (
			tenant_id,user_id,task_id,generation,action,
			approved_definition_version,approved_definition_digest,
			snapshot_high_watermark,audit_from_snapshot_id,
			audit_count,audit_through_id
		 ) VALUES ($1,$2,$3,1,'activate',1,$4,$5,$5,1,$5)
		 RETURNING id`,
		identity.TenantID, identity.UserID, identity.TaskID,
		ref.DefinitionDigest, ref.SnapshotID).Scan(&legacyPointer); err != nil {
		t.Fatalf("seed legacy cutover pointer event: %v", err)
	}
	if _, err := tx.Exec(ctx,
		`UPDATE schedules SET run_snapshot_cutover_event_id=$4
		  WHERE tenant_id=$1 AND user_id=$2 AND id=$3`,
		identity.TenantID, identity.UserID, identity.TaskID,
		legacyPointer); err != nil {
		t.Fatalf("attach legacy cutover pointer: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := st.ControlTaskRunSnapshotCutover(
		ctx, identity.TenantID, identity.UserID, identity.TaskID,
		TaskRunSnapshotCutoverRollback); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("legacy cutover accepted a Tool runtime task: %v", err)
	}

	approved, err := st.GetCurrentToolApprovedDefinition(
		ctx, identity.TenantID, identity.UserID, identity.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	adaptive, err := st.GetToolAdaptiveStateForDefinition(
		ctx, identity.TenantID, identity.UserID, identity.TaskID,
		ApprovedDefinitionFence{Version: approved.Version, Digest: approved.Digest})
	if err != nil {
		t.Fatal(err)
	}
	adaptive.State.InvocationStates[0].Cursor =
		json.RawMessage(`{"next":"live-state-after-snapshot"}`)
	adaptivePayload, err := taskstate.EncodeAdaptiveStateV2(adaptive.State)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.pool.Exec(ctx,
		`UPDATE task_adaptive_states
		    SET version=2, payload=$4, payload_digest=$5
		  WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3`,
		identity.TenantID, identity.UserID, identity.TaskID,
		adaptivePayload, digestTaskStatePayload(adaptivePayload)); err != nil {
		t.Fatalf("mutate live adaptive state: %v", err)
	}
	if _, err := st.pool.Exec(ctx,
		`UPDATE schedule_playbooks SET content='live manual changed after snapshot'
		  WHERE schedule_id=$1`, identity.TaskID); err != nil {
		t.Fatalf("mutate live task manual: %v", err)
	}
	changedPolicy := testCompiledRunPolicyV1(t)
	changedPolicy.CapabilityCatalog.Allowed[0].CredentialRef.Generation = 9
	changedPolicy.ModelPolicy.CredentialRef.Generation = 9
	changedPolicy.ModelPolicy.Endpoint.Generation = 9
	replayed, err := st.CreateOrGetCompiledRunSnapshotV2(
		ctx, identity, changedPolicy, observation.RolloutOff)
	if err != nil || replayed != ref {
		t.Fatalf("exact RunID replay changed snapshot: ref=%+v err=%v",
			replayed, err)
	}
	nextIdentity := identity
	nextIdentity.TemporalRunID = uuid.NewString()
	nextRef, err := st.CreateOrGetCompiledRunSnapshotV2(
		ctx, nextIdentity, changedPolicy, observation.RolloutOff)
	if err != nil || nextRef.AdaptiveVersion != 2 {
		t.Fatalf("Tool run after legacy pointer failed: ref=%+v err=%v",
			nextRef, err)
	}
	purgeTx, err := st.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := purgeTx.Exec(ctx,
		`SELECT 1 FROM schedules
		  WHERE tenant_id=$1 AND user_id=$2 AND id=$3 FOR UPDATE`,
		identity.TenantID, identity.UserID, identity.TaskID); err != nil {
		t.Fatal(err)
	}
	blockedIdentity := identity
	blockedIdentity.TemporalRunID = uuid.NewString()
	snapshotResult := make(chan error, 1)
	go func() {
		_, createErr := st.CreateOrGetCompiledRunSnapshotV2(
			context.Background(), blockedIdentity, changedPolicy,
			observation.RolloutOff)
		snapshotResult <- createErr
	}()
	waiting := false
	for deadline := time.Now().Add(3 * time.Second); time.Now().Before(deadline); {
		var count int
		if err := st.pool.QueryRow(ctx,
			`SELECT count(*)
			   FROM pg_stat_activity
			  WHERE datname=current_database()
			    AND pid<>pg_backend_pid()
			    AND wait_event_type='Lock'
			    AND query LIKE '%JOIN task_approved_definition_versions d%'`,
		).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count > 0 {
			waiting = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !waiting {
		_ = purgeTx.Rollback(ctx)
		t.Fatal("snapshot writer did not reach the schedule lock pause point")
	}
	deleteCtx, cancelDelete := context.WithTimeout(ctx, time.Second)
	_, deleteErr := purgeTx.Exec(deleteCtx,
		`DELETE FROM memberships WHERE tenant_id=$1 AND user_id=$2`,
		identity.TenantID, identity.UserID)
	cancelDelete()
	if deleteErr != nil {
		_ = purgeTx.Rollback(ctx)
		t.Fatalf("snapshot held membership before schedule and can deadlock purge: %v",
			deleteErr)
	}
	if err := purgeTx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case createErr := <-snapshotResult:
		if createErr != nil {
			t.Fatalf("snapshot did not resume after purge rollback: %v", createErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("snapshot remained blocked after purge rollback")
	}

	recovered, found, err := st.LoadCompiledRunSnapshotRefV2(ctx, identity)
	if err != nil || !found || recovered != ref {
		t.Fatalf("recover Source-free ref: ref=%+v found=%v err=%v",
			recovered, found, err)
	}
	compiled, err := st.LoadCompiledTaskRunSnapshotV2(ctx, identity, ref)
	if err != nil {
		t.Fatalf("load Source-free compiled snapshot: %v", err)
	}
	if err := compiled.ValidateFor(identity); err != nil {
		t.Fatalf("validate Source-free compiled snapshot: %v", err)
	}
	if compiled.DefinitionVersion != 1 || compiled.AdaptiveVersion != 1 ||
		compiled.AdaptiveBasisDefinitionVersion !=
			compiled.DefinitionVersion ||
		compiled.AdaptiveBasisDefinitionDigest != ref.DefinitionDigest ||
		compiled.ObservationRollout != observation.RolloutAuthority ||
		compiled.Definition.TaskManual != creation.Definition.PlaybookContent ||
		len(compiled.Definition.ToolCalls) != 1 ||
		len(compiled.Adaptive.InvocationStates) != 1 ||
		!bytes.Equal(compiled.Adaptive.InvocationStates[0].Cursor, []byte(`{}`)) ||
		len(compiled.ToolBindings) != 1 ||
		compiled.ToolBindings[0].InvocationDigest !=
			compiled.Definition.ToolCalls[0].Digest ||
		compiled.ToolBindings[0].Contract.ToolName != "web_search" ||
		compiled.ToolBindings[0].Contract.ImplementationVersion !=
			"fetcher.exa/v1" ||
		compiled.ToolBindings[0].Capability.ImplementationVersion !=
			"fetcher.exa/v1" ||
		compiled.ToolBindings[0].Capability.CredentialRef.Generation != 1 ||
		compiled.Policy.ModelPolicy.CredentialRef.Generation != 1 {
		t.Fatalf("Source-free compiled snapshot did not remain frozen: %+v", compiled)
	}
	invocationDigest := compiled.Definition.ToolCalls[0].Digest
	if recovered, found, err := st.LoadContentObservationForTaskRunV2(
		ctx, identity, ref, invocationDigest,
	); err != nil || found || recovered != nil {
		t.Fatalf("unexpected Source-free observation before commit: items=%+v found=%v err=%v",
			recovered, found, err)
	}
	observed := []types.ContentItem{{
		ExternalID:   uuid.NewString(),
		CanonicalKey: "https://source-free.example/" + uuid.NewString(),
		Kind:         types.KindArticle, URL: "https://source-free.example/post",
		Title: "Source-free content", Content: "exact observed body",
		ContentHash: strings.Repeat("c", 64), FetchedAt: time.Now().UTC(),
	}}
	persisted, err := st.CommitContentObservationForTaskRunV2(
		ctx, identity, ref, invocationDigest, observed)
	if err != nil || len(persisted) != 1 || persisted[0].ID <= 0 ||
		persisted[0].SourceID != 0 {
		t.Fatalf("commit Source-free observation: items=%+v err=%v",
			persisted, err)
	}
	contentID := persisted[0].ID
	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(cleanupCtx, t, st,
			`DELETE FROM task_run_content_provenance
			  WHERE run_snapshot_id=$1 AND invocation_digest=$2`,
			ref.SnapshotID, invocationDigest)
		cleanupExec(cleanupCtx, t, st,
			`DELETE FROM content_items WHERE id=$1`, contentID)
	})
	recoveredObservation, found, err :=
		st.LoadContentObservationForTaskRunV2(
			ctx, identity, ref, invocationDigest)
	if err != nil || !found || len(recoveredObservation) != 1 ||
		recoveredObservation[0].ID != contentID ||
		recoveredObservation[0].Content != observed[0].Content {
		t.Fatalf("recover Source-free observation: items=%+v found=%v err=%v",
			recoveredObservation, found, err)
	}
	replayedObservation, err := st.CommitContentObservationForTaskRunV2(
		ctx, identity, ref, invocationDigest,
		[]types.ContentItem{{
			ExternalID:   "must-not-replace",
			CanonicalKey: "https://must-not-replace.example",
			Kind:         types.KindArticle, URL: "https://must-not-replace.example",
			ContentHash: strings.Repeat("d", 64),
		}})
	if err != nil || len(replayedObservation) != 1 ||
		replayedObservation[0].ID != contentID ||
		replayedObservation[0].CanonicalKey != observed[0].CanonicalKey {
		t.Fatalf("first-writer observation replay drifted: items=%+v err=%v",
			replayedObservation, err)
	}
	var nullSource, provenanceRows, appearanceRows int
	if err := st.pool.QueryRow(ctx,
		`SELECT
		    (SELECT count(*) FROM content_items
		      WHERE id=$1 AND source_id IS NULL),
		    (SELECT count(*) FROM task_run_content_provenance
		      WHERE run_snapshot_id=$2 AND invocation_digest=$3),
		    (SELECT count(*) FROM content_sources WHERE content_item_id=$1)`,
		contentID, ref.SnapshotID, invocationDigest,
	).Scan(&nullSource, &provenanceRows, &appearanceRows); err != nil {
		t.Fatal(err)
	}
	if nullSource != 1 || provenanceRows != 1 || appearanceRows != 0 {
		t.Fatalf("Source-free observation leaked source state: null=%d provenance=%d appearances=%d",
			nullSource, provenanceRows, appearanceRows)
	}

	nonmemberDigest := strings.Repeat("f", 64)
	_, nonmemberPayload, nonmemberObservationDigest, err :=
		runcontext.BuildToolObservationSetV1(
			ref.SnapshotID, nonmemberDigest, []types.ContentItem{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = st.pool.Exec(ctx,
		`INSERT INTO task_run_content_provenance (
		    tenant_id,user_id,task_id,run_snapshot_id,invocation_digest,
		    content_item_ids,observation_payload,observation_digest
		 ) VALUES ($1,$2,$3,$4,$5,'{}',$6,$7)`,
		identity.TenantID, identity.UserID, identity.TaskID,
		ref.SnapshotID, nonmemberDigest,
		nonmemberPayload, nonmemberObservationDigest)
	var pgErr *pgconn.PgError
	if err == nil || !errors.As(err, &pgErr) || pgErr.Code != "23514" {
		t.Fatalf("database admitted invocation outside frozen Tool plan: %v", err)
	}

	_, validEmptyPayload, _, err := runcontext.BuildToolObservationSetV1(
		nextRef.SnapshotID, invocationDigest, []types.ContentItem{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = st.pool.Exec(ctx,
		`INSERT INTO task_run_content_provenance (
		    tenant_id,user_id,task_id,run_snapshot_id,invocation_digest,
		    content_item_ids,observation_payload,observation_digest
		 ) VALUES ($1,$2,$3,$4,$5,'{}',$6,$7)`,
		nextIdentity.TenantID, nextIdentity.UserID, nextIdentity.TaskID,
		nextRef.SnapshotID, invocationDigest,
		validEmptyPayload, strings.Repeat("0", 64))
	pgErr = nil
	if err == nil || !errors.As(err, &pgErr) || pgErr.Code != "23514" {
		t.Fatalf("database admitted forged observation digest: %v", err)
	}

	_, mismatchedPayload, mismatchedObservationDigest, err :=
		runcontext.BuildToolObservationSetV1(
			nextRef.SnapshotID+1, invocationDigest, []types.ContentItem{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = st.pool.Exec(ctx,
		`INSERT INTO task_run_content_provenance (
		    tenant_id,user_id,task_id,run_snapshot_id,invocation_digest,
		    content_item_ids,observation_payload,observation_digest
		 ) VALUES ($1,$2,$3,$4,$5,'{}',$6,$7)`,
		nextIdentity.TenantID, nextIdentity.UserID, nextIdentity.TaskID,
		nextRef.SnapshotID, invocationDigest,
		mismatchedPayload, mismatchedObservationDigest)
	pgErr = nil
	if err == nil || !errors.As(err, &pgErr) || pgErr.Code != "23514" {
		t.Fatalf("database admitted mismatched observation identity: %v", err)
	}

	type concurrentObservationResult struct {
		items []types.ContentItem
		err   error
	}
	concurrentItems := [2]types.ContentItem{
		{
			ExternalID:   uuid.NewString(),
			CanonicalKey: "https://source-free.example/race-a-" + uuid.NewString(),
			Kind:         types.KindArticle,
			URL:          "https://source-free.example/race-a",
			Title:        "race A",
			Content:      "first writer A",
			ContentHash:  strings.Repeat("a", 64),
			FetchedAt:    time.Now().UTC(),
		},
		{
			ExternalID:   uuid.NewString(),
			CanonicalKey: "https://source-free.example/race-b-" + uuid.NewString(),
			Kind:         types.KindArticle,
			URL:          "https://source-free.example/race-b",
			Title:        "race B",
			Content:      "first writer B",
			ContentHash:  strings.Repeat("b", 64),
			FetchedAt:    time.Now().UTC(),
		},
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(cleanupCtx, t, st,
			`DELETE FROM task_run_content_provenance
			  WHERE run_snapshot_id=$1 AND invocation_digest=$2`,
			nextRef.SnapshotID, invocationDigest)
		cleanupExec(cleanupCtx, t, st,
			`DELETE FROM content_items WHERE canonical_key IN ($1,$2)`,
			concurrentItems[0].CanonicalKey, concurrentItems[1].CanonicalKey)
	})
	startConcurrentCommit := make(chan struct{})
	concurrentResults := make(chan concurrentObservationResult, 2)
	for i := range concurrentItems {
		item := concurrentItems[i]
		go func() {
			<-startConcurrentCommit
			got, commitErr := st.CommitContentObservationForTaskRunV2(
				context.Background(), nextIdentity, nextRef,
				invocationDigest, []types.ContentItem{item})
			concurrentResults <- concurrentObservationResult{
				items: got, err: commitErr,
			}
		}()
	}
	close(startConcurrentCommit)
	firstConcurrent := <-concurrentResults
	secondConcurrent := <-concurrentResults
	results := []concurrentObservationResult{
		firstConcurrent, secondConcurrent,
	}
	var committed, conflicted int
	for _, result := range results {
		switch {
		case result.err == nil && len(result.items) == 1:
			committed++
		case errors.Is(result.err, types.ErrConflict) &&
			len(result.items) == 0:
			conflicted++
		default:
			t.Fatalf("unexpected concurrent first-writer result: %+v", result)
		}
	}
	if committed != 1 || conflicted != 1 {
		t.Fatalf("concurrent first-writer outcomes: committed=%d conflicted=%d",
			committed, conflicted)
	}
	var concurrentProvenanceRows, concurrentContentRows int
	if err := st.pool.QueryRow(ctx,
		`SELECT
		    (SELECT count(*) FROM task_run_content_provenance
		      WHERE run_snapshot_id=$1 AND invocation_digest=$2),
		    (SELECT count(*) FROM content_items
		      WHERE canonical_key IN ($3,$4))`,
		nextRef.SnapshotID, invocationDigest,
		concurrentItems[0].CanonicalKey, concurrentItems[1].CanonicalKey,
	).Scan(&concurrentProvenanceRows, &concurrentContentRows); err != nil {
		t.Fatal(err)
	}
	if concurrentProvenanceRows != 1 || concurrentContentRows != 1 {
		t.Fatalf("concurrent observation rows: provenance=%d content=%d, want 1/1",
			concurrentProvenanceRows, concurrentContentRows)
	}

	testSimhash := int64(424242)
	if _, err := st.pool.Exec(ctx,
		`UPDATE content_items
		    SET simhash=$2, fetched_at=now()
		  WHERE id=$1`,
		contentID, testSimhash); err != nil {
		t.Fatal(err)
	}
	recentSimhashes, err := st.ListRecentSimhashesForTaskRunV2(
		ctx, identity, ref, time.Now().Add(-time.Hour), []int64{})
	if err != nil || !slices.Contains(recentSimhashes, testSimhash) {
		t.Fatalf("Source-free simhash history=%v err=%v",
			recentSimhashes, err)
	}

	candidates, err := st.ListContentCandidatesForTaskRunV2(
		ctx, identity, ref, 256)
	if err != nil || len(candidates) != 1 ||
		candidates[0].InvocationDigest != invocationDigest ||
		candidates[0].Item.ID != contentID ||
		candidates[0].Item.SourceID != 0 {
		t.Fatalf("Source-free candidates=%+v err=%v", candidates, err)
	}
	insertCompiledTestDelivery(
		t, st, identity.TenantID, identity.UserID,
		identity.TaskID, contentID)
	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(cleanupCtx, t, st,
			`DELETE FROM deliveries
			  WHERE tenant_id=$1 AND user_id=$2 AND content_item_id=$3`,
			identity.TenantID, identity.UserID, contentID)
		cleanupExec(cleanupCtx, t, st,
			`DELETE FROM push_batches
			  WHERE tenant_id=$1 AND user_id=$2 AND schedule_id=$3`,
			identity.TenantID, identity.UserID, identity.TaskID)
	})
	candidates, err = st.ListContentCandidatesForTaskRunV2(
		ctx, identity, ref, 256)
	if err != nil || len(candidates) != 0 {
		t.Fatalf("delivered Source-free candidate remained: %+v err=%v",
			candidates, err)
	}

	rlsTx, err := st.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rlsTx.Rollback(ctx) }()
	if _, err := rlsTx.Exec(ctx,
		`SELECT set_config('app.tenant_id',$1,true)`,
		fmt.Sprintf("%d", identity.TenantID+1)); err != nil {
		t.Fatal(err)
	}
	if _, err := rlsTx.Exec(ctx, `SET LOCAL ROLE vane_app`); err != nil {
		t.Fatal(err)
	}
	var crossTenantVisible int
	if err := rlsTx.QueryRow(ctx,
		`SELECT count(*) FROM task_run_content_provenance
		  WHERE run_snapshot_id=$1`, ref.SnapshotID,
	).Scan(&crossTenantVisible); err != nil {
		t.Fatal(err)
	}
	if crossTenantVisible != 0 {
		t.Fatalf("cross-tenant provenance rows visible=%d", crossTenantVisible)
	}
	if err := rlsTx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}

	var parentCount, shadowCount, taskLinks, globalTargets int
	if err := st.pool.QueryRow(ctx,
		`SELECT
		    (SELECT count(*) FROM task_run_snapshots
		      WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3),
		    (SELECT count(*) FROM task_run_snapshot_v2_shadows
		      WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3),
		    (SELECT count(*) FROM task_fetch_targets WHERE schedule_id=$3),
		    (SELECT count(*) FROM fetch_targets WHERE url=$4)`,
		identity.TenantID, identity.UserID, identity.TaskID, target.URL,
	).Scan(&parentCount, &shadowCount, &taskLinks, &globalTargets); err != nil {
		t.Fatal(err)
	}
	if parentCount != 3 || shadowCount != 0 ||
		taskLinks != 0 || globalTargets != 0 {
		t.Fatalf("Source-free persistence leaked legacy state: parent=%d shadow=%d links=%d targets=%d",
			parentCount, shadowCount, taskLinks, globalTargets)
	}

	tampered := ref
	tampered.AdaptiveVersion++
	if _, err := st.LoadCompiledTaskRunSnapshotV2(
		ctx, identity, tampered); err == nil {
		t.Fatal("tampered Source-free reference was accepted")
	}
	if _, err := st.AuthorizeTaskRunSideEffectV2(
		ctx, identity, tampered); err == nil {
		t.Fatal("tampered Source-free reference reached live authorization")
	}
	setBucket(t, st, identity.TenantID, QuotaLLMTokens,
		100, 0.000001, 100)
	t.Cleanup(func() {
		if _, err := st.pool.Exec(context.Background(),
			`DELETE FROM tenant_quota WHERE tenant_id=$1`,
			identity.TenantID); err != nil {
			t.Errorf("cleanup Source-free quota: %v", err)
		}
	})
	quotaRule := runtimepolicy.QuotaBucketV1{
		Name: string(QuotaLLMTokens), Financial: true,
		EnforcementVersion: runtimepolicy.QuotaEnforcementLLMPrechargeV1,
	}
	if err := st.AuthorizeAndConsumeTaskRunLLMQuotaV2(
		ctx, identity, ref, quotaRule, 20); err != nil {
		t.Fatalf("Source-free quota reserve: %v", err)
	}
	quotaBeforePause := runtimeQuotaTokens(
		t, st, identity.TenantID, QuotaLLMTokens)
	if quotaBeforePause < 79.9 || quotaBeforePause > 80.1 {
		t.Fatalf("Source-free quota tokens=%.6f, want about 80",
			quotaBeforePause)
	}
	if _, err := st.pool.Exec(ctx,
		`UPDATE schedules SET status=$4
		  WHERE tenant_id=$1 AND user_id=$2 AND id=$3`,
		identity.TenantID, identity.UserID, identity.TaskID,
		types.ScheduleStatusPaused); err != nil {
		t.Fatalf("pause Source-free task: %v", err)
	}
	if authorized, err := st.AuthorizeTaskRunSideEffectV2(
		ctx, identity, ref); err != nil || authorized {
		t.Fatalf("paused Source-free run authorization: authorized=%v err=%v",
			authorized, err)
	}
	if err := st.AuthorizeAndConsumeTaskRunLLMQuotaV2(
		ctx, identity, ref, quotaRule, 1,
	); !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("paused Source-free quota reserve=%v, want quota denial", err)
	}
	quotaAfterPause := runtimeQuotaTokens(
		t, st, identity.TenantID, QuotaLLMTokens)
	if quotaAfterPause < quotaBeforePause ||
		quotaAfterPause-quotaBeforePause > 0.01 {
		t.Fatalf("paused Source-free quota mutated balance: before=%.6f after=%.6f",
			quotaBeforePause, quotaAfterPause)
	}
	if recovered, found, err := st.LoadContentObservationForTaskRunV2(
		ctx, identity, ref, invocationDigest,
	); err != nil || !found || len(recovered) != 1 {
		t.Fatalf("revoked observation receipt was not recoverable: items=%+v found=%v err=%v",
			recovered, found, err)
	}
}

func assertToolRunSnapshotV2RecoveryWaitsForWriter(
	t *testing.T,
	st *Store,
	identity types.RunIdentity,
) {
	t.Helper()
	ctx := t.Context()
	waitingIdentity := identity
	waitingIdentity.TemporalRunID = uuid.NewString()
	lockTx, err := st.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := setTaskRunTenantContext(
		ctx, lockTx, identity.TenantID); err != nil {
		_ = lockTx.Rollback(ctx)
		t.Fatal(err)
	}
	if err := lockTaskRunSnapshotRun(
		ctx, lockTx, waitingIdentity.TemporalRunID); err != nil {
		_ = lockTx.Rollback(ctx)
		t.Fatal(err)
	}
	type loadResult struct {
		found bool
		err   error
	}
	result := make(chan loadResult, 1)
	go func() {
		_, found, loadErr := st.LoadCompiledRunSnapshotRefV2(
			context.Background(), waitingIdentity)
		result <- loadResult{found: found, err: loadErr}
	}()
	waiting := false
	for deadline := time.Now().Add(3 * time.Second); time.Now().Before(deadline); {
		select {
		case early := <-result:
			_ = lockTx.Rollback(ctx)
			t.Fatalf("V2 recovery bypassed the first-writer fence: %+v", early)
		default:
		}
		var count int
		if err := st.pool.QueryRow(ctx,
			`SELECT count(*)
			   FROM pg_stat_activity
			  WHERE datname=current_database()
			    AND pid<>pg_backend_pid()
			    AND wait_event_type='Lock'
			    AND query LIKE '%pg_advisory_xact_lock(hashtextextended%'`,
		).Scan(&count); err != nil {
			_ = lockTx.Rollback(ctx)
			t.Fatal(err)
		}
		if count > 0 {
			waiting = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !waiting {
		_ = lockTx.Rollback(ctx)
		t.Fatal("V2 recovery did not wait behind the first-writer fence")
	}
	if err := lockTx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case loaded := <-result:
		if loaded.err != nil || loaded.found {
			t.Fatalf("empty fenced V2 recovery = %+v", loaded)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("V2 recovery remained blocked after writer rollback")
	}
}

func assertToolRunSnapshotV2AdmissionRejects(
	t *testing.T,
	st *Store,
	identity types.RunIdentity,
	sourceSnapshotID int64,
	name string,
	adaptiveMutation string,
	mode types.ExecutionMode,
) {
	t.Helper()
	ctx := t.Context()
	tx, err := st.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT set_config('app.tenant_id',$1,true)`,
		fmt.Sprintf("%d", identity.TenantID)); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `SET LOCAL ROLE vane_app`); err != nil {
		t.Fatal(err)
	}
	if adaptiveMutation != "" {
		if _, err := tx.Exec(ctx, adaptiveMutation,
			identity.TenantID, identity.UserID, identity.TaskID); err != nil {
			t.Fatalf("%s fixture mutation: %v", name, err)
		}
	}
	_, err = tx.Exec(ctx,
		`INSERT INTO task_run_snapshots (
			id, tenant_id, user_id, task_id, temporal_workflow_id, temporal_run_id,
			run_kind, execution_mode, adaptive_version, capability_catalog_digest,
			tool_policy_digest, prompt_policy_digest, model_policy_digest,
			quota_policy_digest, definition_digest, plan_digest, payload_digest,
			reference_digest, reference_schema_version, payload, budget
		 )
		 SELECT nextval('task_run_snapshots_id_seq'),
		        tenant_id, user_id, task_id, $2, $3,
		        run_kind, $4, adaptive_version, capability_catalog_digest,
		        tool_policy_digest, prompt_policy_digest, model_policy_digest,
		        quota_policy_digest, definition_digest, plan_digest, payload_digest,
		        reference_digest, reference_schema_version, payload, budget
		   FROM task_run_snapshots
		  WHERE id=$1`,
		sourceSnapshotID,
		identity.TemporalWorkflowID+"-admission-negative",
		uuid.NewString(),
		string(mode),
	)
	var pgErr *pgconn.PgError
	if err == nil || !errors.As(err, &pgErr) || pgErr.Code != "23514" {
		t.Fatalf("%s was not rejected by V2 admission fence: %v", name, err)
	}
}

func TestTaskRunToolBindingRejectsIncompatibleImplementation(t *testing.T) {
	call, err := taskstate.BuildToolInvocationV1(
		"web_search", "v1", json.RawMessage(`{"query":"AI"}`))
	if err != nil {
		t.Fatal(err)
	}
	definition, err := taskstate.BuildApprovedDefinitionV2(
		taskstate.ApprovedDefinitionInputV2{
			TenantID: 1, UserID: 2, TaskID: "route-mismatch",
			SpecJSON: json.RawMessage(`{}`), ScopeJSON: json.RawMessage(`{}`),
			NLDescription: "monitor AI",
			TaskManual:    "monitor AI", Strictness: types.StrictnessNormal,
			ToolCalls:      []taskstate.ToolInvocationV1{call},
			ExecutionMode:  types.ExecutionModeCompiled,
			DeliveryPolicy: taskstate.DeliveryPolicyOwnerFeishu,
			BudgetPolicy:   taskstate.BudgetPolicyInheritTenantQuota,
		})
	if err != nil {
		t.Fatal(err)
	}
	policy := testCompiledRunPolicyV1(t)
	policy.CapabilityCatalog.Allowed[0].ImplementationVersion =
		"fetcher.binding/v1"
	policy.CapabilityCatalog.Allowed[0].CredentialRef.ID =
		"tikhub-primary"
	if err := policy.Validate(); err != nil {
		t.Fatalf("mismatched policy must be otherwise structurally valid: %v", err)
	}
	if _, err := buildTaskRunToolBindingsV1(
		definition, policy); err == nil {
		t.Fatal("web_search accepted the binding implementation")
	}
}
