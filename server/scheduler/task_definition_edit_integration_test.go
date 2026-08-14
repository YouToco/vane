//go:build integration

package scheduler

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"go.temporal.io/api/serviceerror"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"

	"github.com/YouToco/vane/workflow"
)

func TestTaskDefinitionEditIntegration_RealDevServerLifecycle(t *testing.T) {
	const (
		namespace = "c2b3-task-definition-edit-integration"
		taskQueue = "c2b3-task-definition-edit-integration"
	)
	startCtx, cancelStart := context.WithTimeout(t.Context(), 2*time.Minute)
	server, err := testsuite.StartDevServer(startCtx, testsuite.DevServerOptions{
		ClientOptions: &client.Options{Namespace: namespace},
		LogLevel:      "error",
	})
	cancelStart()
	if err != nil {
		t.Fatalf("start Temporal dev server: %v", err)
	}
	t.Cleanup(func() {
		if err := server.Stop(); err != nil {
			t.Errorf("stop Temporal dev server: %v", err)
		}
		server.Client().Close()
	})

	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	preparer := New(
		server.Client(), taskQueue, nil,
		WithTaskScheduleNamespace(namespace),
		WithCompiledRuntimeRollout(true, "", true),
	)
	request := integrationTaskScheduleRequest()
	request.OperationID = "integration-operation-c2b3-definition-edit"
	request.NLDescription = "C2b3 real Temporal definition edit base"
	creation, err := preparer.PrepareTaskSchedule(ctx, request)
	if err != nil {
		t.Fatalf("prepare creation ownership: %v", err)
	}
	if creation.FingerprintVersion != taskScheduleFingerprintVersion {
		t.Fatalf("creation fingerprint version = %q, want current %q", creation.FingerprintVersion, taskScheduleFingerprintVersion)
	}
	handle := server.Client().ScheduleClient().GetHandle(ctx, creation.TaskID)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		if err := handle.Delete(cleanupCtx); err != nil {
			if _, notFound := errors.AsType[*serviceerror.NotFound](err); !notFound {
				t.Errorf("delete integration definition-edit schedule: %v", err)
			}
		}
	})

	scheduler := New(server.Client(), taskQueue, nil, WithTaskScheduleNamespace(namespace))
	ensured, err := scheduler.EnsurePausedTask(ctx, creation)
	if err != nil {
		t.Fatalf("ensure paused creation: %v", err)
	}
	if _, err := scheduler.ActivateTask(ctx, creation, ensured.Snapshot); err != nil {
		t.Fatalf("activate creation: %v", err)
	}

	base := integrationDefinitionEditDefinition(request)
	targetV2 := integrationChangedDefinitionEditDefinition(base, 4, "v2")
	headV1 := integrationDefinitionEditHead(1, "a")
	headV2 := integrationDefinitionEditHead(2, "b")
	preparedV2, baseSnapshot, err := scheduler.PrepareTaskDefinitionEdit(ctx, TaskDefinitionEditRequest{
		OperationID:   "integration-edit-v1-v2",
		Creation:      creation,
		BaseHead:      headV1,
		TargetHead:    headV2,
		OriginalState: TaskDefinitionEditOriginalStateActive,
		Base:          base,
		Target:        targetV2,
	})
	if err != nil {
		t.Fatalf("prepare active v1->v2 edit: %v", err)
	}
	preparedV2 = integrationRoundTripPreparedDefinitionEdit(t, preparedV2)
	if baseSnapshot.Phase != TaskDefinitionEditPhaseBaseOriginal || baseSnapshot.Revision != preparedV2.BaseRevision {
		t.Fatalf("active prepared base = %+v, want frozen base_original revision %q", baseSnapshot, preparedV2.BaseRevision)
	}

	pausedV1, err := scheduler.PauseTaskDefinitionEdit(ctx, preparedV2)
	if err != nil {
		t.Fatalf("pause active v1 base: %v", err)
	}
	if pausedV1.Phase != TaskDefinitionEditPhaseBasePaused || pausedV1.Revision == baseSnapshot.Revision {
		t.Fatalf("paused v1 snapshot = %+v, want new base_paused revision", pausedV1)
	}
	baseConflictToken, err := base64.RawURLEncoding.DecodeString(baseSnapshot.Revision)
	if err != nil {
		t.Fatalf("decode prepared base conflict token: %v", err)
	}
	dc, err := scheduler.taskDefinitionEditEnvironment(ctx, preparedV2.Creation, "integration_request_replay")
	if err != nil {
		t.Fatalf("resolve converter for raw request replay: %v", err)
	}
	replayedPause, err := buildTaskDefinitionEditUpdateRequest(
		preparedV2, preparedV2.BasePaused, baseConflictToken, "base_paused", dc,
	)
	if err != nil {
		t.Fatalf("rebuild deterministic pause request: %v", err)
	}
	if _, err := server.Client().WorkflowService().UpdateSchedule(ctx, replayedPause); err != nil {
		t.Fatalf("real Temporal rejected identical RequestID replay with stale conflict token: %v", err)
	}
	afterReplay, err := scheduler.DescribeTaskDefinitionEdit(ctx, preparedV2)
	if err != nil {
		t.Fatalf("Describe after identical raw request replay: %v", err)
	}
	if afterReplay != pausedV1 {
		t.Fatalf("identical RequestID replay changed remote revision: got %+v want %+v", afterReplay, pausedV1)
	}
	appliedV2, err := scheduler.ApplyTaskDefinitionEdit(ctx, preparedV2, pausedV1)
	if err != nil {
		t.Fatalf("apply v2 target: %v", err)
	}
	if appliedV2.Phase != TaskDefinitionEditPhaseTargetPaused || appliedV2.Revision == pausedV1.Revision {
		t.Fatalf("applied v2 snapshot = %+v, want new target_paused revision", appliedV2)
	}
	finalV2, err := scheduler.RestoreTaskDefinitionEdit(ctx, preparedV2, appliedV2)
	if err != nil {
		t.Fatalf("restore v2 target active: %v", err)
	}
	if finalV2.Phase != TaskDefinitionEditPhaseTargetFinal || finalV2.Revision == appliedV2.Revision {
		t.Fatalf("final v2 snapshot = %+v, want new target_final revision", finalV2)
	}
	assertIntegrationDefinitionEditRemote(
		t, ctx, scheduler, preparedV2, preparedV2.TargetFinal,
		TaskDefinitionEditPhaseTargetFinal, targetV2, false,
	)

	const externalPauseNote = "external operator maintenance pause"
	if err := handle.Pause(ctx, client.SchedulePauseOptions{Note: externalPauseNote}); err != nil {
		t.Fatalf("externally pause finalized v2 schedule: %v", err)
	}
	rawPaused, err := handle.Describe(ctx)
	if err != nil {
		t.Fatalf("raw Describe externally paused v2: %v", err)
	}
	if rawPaused.Schedule.State == nil || !rawPaused.Schedule.State.Paused ||
		rawPaused.Schedule.State.Note != externalPauseNote {
		t.Fatalf("external paused state = %+v, want paused with note %q", rawPaused.Schedule.State, externalPauseNote)
	}

	targetV3 := integrationChangedDefinitionEditDefinition(targetV2, 5, "v3")
	headV3 := integrationDefinitionEditHead(3, "c")
	preparedV3, pausedV2, err := scheduler.PrepareTaskDefinitionEdit(ctx, TaskDefinitionEditRequest{
		OperationID:   "integration-edit-v2-v3-paused",
		Creation:      creation,
		BaseHead:      headV2,
		TargetHead:    headV3,
		OriginalState: TaskDefinitionEditOriginalStatePaused,
		Base:          targetV2,
		Target:        targetV3,
	})
	if err != nil {
		t.Fatalf("prepare externally paused v2->v3 edit: %v", err)
	}
	preparedV3 = integrationRoundTripPreparedDefinitionEdit(t, preparedV3)
	if preparedV3.BaseOriginal.State.Note != externalPauseNote ||
		preparedV3.TargetFinal.State.Note != externalPauseNote {
		t.Fatalf("paused v2->v3 did not freeze note: base=%q final=%q", preparedV3.BaseOriginal.State.Note, preparedV3.TargetFinal.State.Note)
	}

	observedV2, err := scheduler.PauseTaskDefinitionEdit(ctx, preparedV3)
	if err != nil {
		t.Fatalf("observe originally paused v2: %v", err)
	}
	if observedV2 != pausedV2 {
		t.Fatalf("paused v2 observation = %+v, want prepared %+v", observedV2, pausedV2)
	}
	finalV3, err := scheduler.ApplyTaskDefinitionEdit(ctx, preparedV3, observedV2)
	if err != nil {
		t.Fatalf("apply paused v3 target: %v", err)
	}
	if finalV3.Phase != TaskDefinitionEditPhaseTargetFinal {
		t.Fatalf("paused v3 apply phase = %q, want target_final", finalV3.Phase)
	}
	reobservedV3, err := scheduler.RestoreTaskDefinitionEdit(ctx, preparedV3, finalV3)
	if err != nil {
		t.Fatalf("reobserve originally paused v3: %v", err)
	}
	if reobservedV3 != finalV3 {
		t.Fatalf("paused restore observation = %+v, want unchanged %+v", reobservedV3, finalV3)
	}
	assertIntegrationDefinitionEditRemote(
		t, ctx, scheduler, preparedV3, preparedV3.TargetFinal,
		TaskDefinitionEditPhaseTargetFinal, targetV3, true,
	)
	mismatchStartCtx, cancelMismatchStart := context.WithTimeout(
		t.Context(), 2*time.Minute,
	)
	mismatchServer, err := testsuite.StartDevServer(
		mismatchStartCtx,
		testsuite.DevServerOptions{
			ClientOptions: &client.Options{Namespace: namespace},
			LogLevel:      "error",
		},
	)
	cancelMismatchStart()
	if err != nil {
		t.Fatalf("start namespace-mismatch Temporal dev server: %v", err)
	}
	t.Cleanup(func() {
		if err := mismatchServer.Stop(); err != nil {
			t.Errorf("stop namespace-mismatch Temporal dev server: %v", err)
		}
		mismatchServer.Client().Close()
	})
	mismatchScheduler := New(
		mismatchServer.Client(), taskQueue, nil,
		WithTaskScheduleNamespace(namespace),
	)
	if err := mismatchScheduler.ValidateTaskDefinitionEditEnvironment(
		ctx, preparedV3,
	); err == nil || !strings.Contains(err.Error(), "namespace id") {
		t.Fatalf("real Temporal namespace identity mismatch did not fail closed: %v", err)
	}
	finalRaw, err := handle.Describe(ctx)
	if err != nil {
		t.Fatalf("raw Describe final paused v3: %v", err)
	}
	if finalRaw.Schedule.State == nil || !finalRaw.Schedule.State.Paused ||
		finalRaw.Schedule.State.Note != externalPauseNote {
		t.Fatalf("final v3 state = %+v, want paused with preserved note %q", finalRaw.Schedule.State, externalPauseNote)
	}
}

func integrationDefinitionEditDefinition(request TaskScheduleRequest) TaskDefinitionEditDefinition {
	return TaskDefinitionEditDefinition{
		Spec:          request.Spec,
		Scope:         cloneTaskDefinitionEditScope(request.Scope),
		NLDescription: request.NLDescription,
	}
}

func integrationChangedDefinitionEditDefinition(
	base TaskDefinitionEditDefinition,
	hour int,
	version string,
) TaskDefinitionEditDefinition {
	target := cloneTaskDefinitionEditDefinition(base)
	target.Spec = ScheduleSpec{Cron: integrationCronAtHour(hour), TZ: "UTC"}
	target.Scope = workflow.PushScope{SourceIDs: []int64{202, 303}, TopN: hour}
	target.NLDescription = "C2b3 real Temporal definition edit " + version
	return target
}

func integrationDefinitionEditHead(version int64, char string) TaskDefinitionEditHead {
	return TaskDefinitionEditHead{Version: version, Digest: strings.Repeat(char, 64)}
}

func integrationRoundTripPreparedDefinitionEdit(
	t *testing.T,
	prepared PreparedTaskDefinitionEdit,
) PreparedTaskDefinitionEdit {
	t.Helper()
	checkpoint, err := json.Marshal(prepared)
	if err != nil {
		t.Fatalf("marshal prepared definition edit checkpoint: %v", err)
	}
	var recovered PreparedTaskDefinitionEdit
	if err := json.Unmarshal(checkpoint, &recovered); err != nil {
		t.Fatalf("unmarshal prepared definition edit checkpoint: %v", err)
	}
	return recovered
}

func assertIntegrationDefinitionEditRemote(
	t *testing.T,
	ctx context.Context,
	scheduler *Scheduler,
	prepared PreparedTaskDefinitionEdit,
	representation PreparedTaskDefinitionEditSchedule,
	wantPhase TaskDefinitionEditPhase,
	wantDefinition TaskDefinitionEditDefinition,
	wantPaused bool,
) {
	t.Helper()
	snapshot, err := scheduler.DescribeTaskDefinitionEdit(ctx, prepared)
	if err != nil {
		t.Fatalf("strict Describe final definition edit: %v", err)
	}
	if snapshot.Phase != wantPhase || snapshot.RepresentationDigest != representation.Digest || snapshot.Revision == "" {
		t.Fatalf("strict final snapshot = %+v, want phase %q representation %q", snapshot, wantPhase, representation.Digest)
	}
	raw, err := scheduler.c.WorkflowService().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
		Namespace: prepared.Creation.Namespace, ScheduleId: prepared.Creation.TaskID,
	})
	if err != nil {
		t.Fatalf("raw Describe final definition edit: %v", err)
	}
	dc, err := scheduler.taskDefinitionEditEnvironment(ctx, prepared.Creation, "integration_assert")
	if err != nil {
		t.Fatalf("resolve definition edit converter: %v", err)
	}
	matches, err := taskDefinitionEditDescriptionMatches(raw, representation, dc)
	if err != nil {
		t.Fatalf("strict raw representation verification: %v", err)
	}
	if !matches {
		t.Fatal("real Temporal Describe did not match the frozen full representation")
	}
	if raw.GetSchedule().GetState().GetPaused() != wantPaused {
		t.Fatalf("raw paused = %v, want %v", raw.GetSchedule().GetState().GetPaused(), wantPaused)
	}
	if !taskScheduleProtoSpecMatches(raw.GetSchedule().GetSpec(), representation.Timing) {
		t.Fatal("real Temporal normalized spec differs from frozen target timing")
	}
	fingerprint, params, _, err := decodeTaskDefinitionEditAction(
		raw.GetSchedule().GetAction(), prepared.Creation, dc,
	)
	if err != nil {
		t.Fatalf("decode real Temporal target action: %v", err)
	}
	if fingerprint != representation.Fingerprint {
		t.Fatalf("real Temporal fingerprint = %+v, want %+v", fingerprint, representation.Fingerprint)
	}
	if params.NLDesc != wantDefinition.NLDescription || params.Scope.TopN != wantDefinition.Scope.TopN ||
		!slices.Equal(params.Scope.SourceIDs, wantDefinition.Scope.SourceIDs) {
		t.Fatalf("real Temporal params = %+v, want definition %+v", params, wantDefinition)
	}
}
