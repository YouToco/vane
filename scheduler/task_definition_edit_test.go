package scheduler

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"go.temporal.io/api/serviceerror"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"

	"github.com/YouToco/vane/definitioneditwire"
	"github.com/YouToco/vane/workflow"
)

const taskDefinitionEditPausedNote = "operator-paused-before-edit"

type taskDefinitionEditFixture struct {
	scheduler *taskScheduleTestScheduler
	fake      *taskScheduleFakeClient
	creation  PreparedTaskSchedule
	base      TaskDefinitionEditDefinition
	state     TaskDefinitionEditOriginalState
}

func newTaskDefinitionEditFixture(
	t *testing.T,
	state TaskDefinitionEditOriginalState,
) *taskDefinitionEditFixture {
	t.Helper()

	fake := newTaskScheduleFakeClient()
	scheduler := newTaskScheduleTestScheduler(fake)
	WithCompiledRuntimeRollout(true, "", true)(scheduler.Scheduler)

	request := validTaskScheduleRequest()
	creation := preparedTaskSchedule(t, scheduler, request)
	if creation.FingerprintVersion != taskScheduleFingerprintVersion {
		t.Fatalf("creation fingerprint version = %q, want current %q", creation.FingerprintVersion, taskScheduleFingerprintVersion)
	}
	ensured, err := scheduler.Scheduler.EnsurePausedTask(t.Context(), creation)
	if err != nil {
		t.Fatalf("ensure paused creation: %v", err)
	}
	if _, err := scheduler.Scheduler.ActivateTask(t.Context(), creation, ensured.Snapshot); err != nil {
		t.Fatalf("activate creation: %v", err)
	}
	if state == TaskDefinitionEditOriginalStatePaused {
		if !fake.mutate(creation.TaskID, func(description *client.ScheduleDescription) {
			if description.Schedule.State == nil {
				description.Schedule.State = &client.ScheduleState{}
			}
			description.Schedule.State.Paused = true
			description.Schedule.State.Note = taskDefinitionEditPausedNote
		}) {
			t.Fatal("pause fixture schedule: missing fake record")
		}
	}

	return &taskDefinitionEditFixture{
		scheduler: scheduler,
		fake:      fake,
		creation:  creation,
		state:     state,
		base: TaskDefinitionEditDefinition{
			Spec:          request.Spec,
			Scope:         cloneTaskDefinitionEditScope(request.Scope),
			NLDescription: request.NLDescription,
		},
	}
}

func (f *taskDefinitionEditFixture) prepare(
	t *testing.T,
	operationID string,
	baseHead TaskDefinitionEditHead,
	targetHead TaskDefinitionEditHead,
	base TaskDefinitionEditDefinition,
	target TaskDefinitionEditDefinition,
) (PreparedTaskDefinitionEdit, TaskDefinitionEditSnapshot) {
	t.Helper()
	prepared, snapshot, err := f.scheduler.PrepareTaskDefinitionEdit(t.Context(), TaskDefinitionEditRequest{
		OperationID:   operationID,
		Creation:      f.creation,
		BaseHead:      baseHead,
		TargetHead:    targetHead,
		OriginalState: f.state,
		Base:          cloneTaskDefinitionEditDefinition(base),
		Target:        cloneTaskDefinitionEditDefinition(target),
	})
	if err != nil {
		t.Fatalf("PrepareTaskDefinitionEdit: %v", err)
	}
	return prepared, snapshot
}

func taskDefinitionEditHead(version int64, char string) TaskDefinitionEditHead {
	return TaskDefinitionEditHead{Version: version, Digest: strings.Repeat(char, sha256.Size*2)}
}

func changedTaskDefinitionEditDefinition(
	base TaskDefinitionEditDefinition,
	suffix string,
) TaskDefinitionEditDefinition {
	target := cloneTaskDefinitionEditDefinition(base)
	target.Spec = ScheduleSpec{Cron: "25 9 * * *", TZ: "Asia/Shanghai"}
	target.Scope = workflow.PushScope{SourceIDs: []int64{22, 33}, TopN: 5}
	target.NLDescription = "编辑后的 AI 情报 " + suffix
	return target
}

func expectedTaskDefinitionEditRequestID(phase, operationDigest, requestDigest string) string {
	sum := sha256.Sum256([]byte("definition_edit/" + phase + "/" + operationDigest + ":" + requestDigest))
	return hex.EncodeToString(sum[:])
}

func TestTaskDefinitionEdit_RetainedV1CreationProvenance(t *testing.T) {
	fake := newTaskScheduleFakeClient()
	scheduler := newTaskScheduleTestScheduler(fake)
	request := validTaskScheduleRequest()
	request.Spec = ScheduleSpec{Cron: "0 9 * * 1", TZ: "Asia/Shanghai"}
	request.NLDescription = "每周一上午 9:00 推送 AI 官方重大更新"
	creation := preparedTaskSchedule(t, scheduler, request)
	if creation.FingerprintVersion != taskScheduleFingerprintVersionV1 {
		t.Fatalf("creation fingerprint version = %q, want retained v1", creation.FingerprintVersion)
	}
	ensured, err := scheduler.Scheduler.EnsurePausedTask(t.Context(), creation)
	if err != nil {
		t.Fatalf("ensure paused creation: %v", err)
	}
	if _, err := scheduler.Scheduler.ActivateTask(t.Context(), creation, ensured.Snapshot); err != nil {
		t.Fatalf("activate creation: %v", err)
	}

	base := TaskDefinitionEditDefinition{
		Spec:          request.Spec,
		Scope:         cloneTaskDefinitionEditScope(request.Scope),
		NLDescription: request.NLDescription,
	}
	target := cloneTaskDefinitionEditDefinition(base)
	target.NLDescription = "[C2b3-2d Gate 临时] " + base.NLDescription
	prepared, snapshot, err := scheduler.PrepareTaskDefinitionEdit(
		t.Context(),
		TaskDefinitionEditRequest{
			OperationID:   "edit-retained-v1-creation",
			Creation:      creation,
			BaseHead:      taskDefinitionEditHead(1, "a"),
			TargetHead:    taskDefinitionEditHead(2, "b"),
			OriginalState: TaskDefinitionEditOriginalStateActive,
			Base:          base,
			Target:        target,
		},
	)
	if err != nil {
		t.Fatalf("PrepareTaskDefinitionEdit retained v1 creation: %v", err)
	}
	if prepared.WireVersion != "v2" {
		t.Fatalf("prepared wire version = %q, want v2 compatibility wire", prepared.WireVersion)
	}
	if prepared.Creation.FingerprintVersion != taskScheduleFingerprintVersionV1 {
		t.Fatalf("prepared creation fingerprint version = %q, want retained v1", prepared.Creation.FingerprintVersion)
	}
	if snapshot.Phase != TaskDefinitionEditPhaseBaseOriginal ||
		snapshot.Revision != prepared.BaseRevision {
		t.Fatalf("base snapshot = %+v, want exact base_original", snapshot)
	}
	preparedBytes, err := EncodePreparedTaskDefinitionEdit(prepared)
	if err != nil {
		t.Fatalf("encode retained v1 compatibility edit: %v", err)
	}
	snapshotBytes, err := EncodeTaskDefinitionEditBaseSnapshot(prepared, snapshot)
	if err != nil {
		t.Fatalf("encode retained v1 base snapshot: %v", err)
	}
	if _, err := definitioneditwire.DecodePhaseSnapshotBytes(
		preparedBytes,
		snapshotBytes,
	); err != nil {
		t.Fatalf("retained recovery reader rejected compatibility wire: %v", err)
	}
	reinterpreted := prepared
	reinterpreted.WireVersion = taskDefinitionEditWireVersionV1
	if _, err := EncodePreparedTaskDefinitionEdit(reinterpreted); !errors.Is(
		err,
		ErrTaskScheduleInvalid,
	) {
		t.Fatalf("v1 wire accepted retained v1 creation provenance: %v", err)
	}
}

func TestTaskDefinitionEdit_ActiveRawCASLifecycle(t *testing.T) {
	fixture := newTaskDefinitionEditFixture(t, TaskDefinitionEditOriginalStateActive)
	target := changedTaskDefinitionEditDefinition(fixture.base, "active")
	prepared, base := fixture.prepare(
		t, "edit-active-0001",
		taskDefinitionEditHead(1, "a"), taskDefinitionEditHead(2, "b"),
		fixture.base, target,
	)
	if base.Phase != TaskDefinitionEditPhaseBaseOriginal || base.Revision != prepared.BaseRevision {
		t.Fatalf("prepared base snapshot = %+v, want base_original at frozen revision %q", base, prepared.BaseRevision)
	}

	updatesBefore := fixture.fake.counts().unpause
	paused, err := fixture.scheduler.PauseTaskDefinitionEdit(t.Context(), prepared)
	if err != nil {
		t.Fatalf("PauseTaskDefinitionEdit: %v", err)
	}
	if paused.Phase != TaskDefinitionEditPhaseBasePaused || paused.RepresentationDigest != prepared.BasePaused.Digest {
		t.Fatalf("pause snapshot = %+v, want exact base_paused", paused)
	}
	pauseRequest := fixture.fake.rawRequestSnapshot()
	if len(pauseRequest.updateConflictToken) == 0 {
		t.Fatal("pause UpdateSchedule omitted Describe conflict token")
	}
	wantPauseID := expectedTaskDefinitionEditRequestID("base_paused", prepared.OperationDigest, prepared.RequestDigest)
	if pauseRequest.updateRequestID != wantPauseID {
		t.Fatalf("pause request ID = %q, want deterministic %q", pauseRequest.updateRequestID, wantPauseID)
	}

	applied, err := fixture.scheduler.ApplyTaskDefinitionEdit(t.Context(), prepared, paused)
	if err != nil {
		t.Fatalf("ApplyTaskDefinitionEdit: %v", err)
	}
	if applied.Phase != TaskDefinitionEditPhaseTargetPaused || applied.RepresentationDigest != prepared.TargetPaused.Digest {
		t.Fatalf("apply snapshot = %+v, want exact target_paused", applied)
	}
	applyRequest := fixture.fake.rawRequestSnapshot()
	if len(applyRequest.updateConflictToken) == 0 {
		t.Fatal("apply UpdateSchedule omitted Describe conflict token")
	}
	wantApplyID := expectedTaskDefinitionEditRequestID("target_applied", prepared.OperationDigest, prepared.RequestDigest)
	if applyRequest.updateRequestID != wantApplyID {
		t.Fatalf("apply request ID = %q, want deterministic %q", applyRequest.updateRequestID, wantApplyID)
	}

	restored, err := fixture.scheduler.RestoreTaskDefinitionEdit(t.Context(), prepared, applied)
	if err != nil {
		t.Fatalf("RestoreTaskDefinitionEdit: %v", err)
	}
	if restored.Phase != TaskDefinitionEditPhaseTargetFinal || restored.RepresentationDigest != prepared.TargetFinal.Digest {
		t.Fatalf("restore snapshot = %+v, want exact target_final", restored)
	}
	restoreRequest := fixture.fake.rawRequestSnapshot()
	if len(restoreRequest.updateConflictToken) == 0 {
		t.Fatal("restore UpdateSchedule omitted Describe conflict token")
	}
	wantRestoreID := expectedTaskDefinitionEditRequestID("target_restored", prepared.OperationDigest, prepared.RequestDigest)
	if restoreRequest.updateRequestID != wantRestoreID {
		t.Fatalf("restore request ID = %q, want deterministic %q", restoreRequest.updateRequestID, wantRestoreID)
	}
	if wantPauseID == wantApplyID || wantPauseID == wantRestoreID || wantApplyID == wantRestoreID {
		t.Fatalf("phase request IDs are not distinct: pause=%q apply=%q restore=%q", wantPauseID, wantApplyID, wantRestoreID)
	}
	if got := fixture.fake.counts().unpause - updatesBefore; got != 3 {
		t.Fatalf("raw UpdateSchedule calls = %d, want exactly three", got)
	}

	current, err := fixture.scheduler.DescribeTaskDefinitionEdit(t.Context(), prepared)
	if err != nil {
		t.Fatalf("DescribeTaskDefinitionEdit final: %v", err)
	}
	if current != restored {
		t.Fatalf("final Describe = %+v, want restored %+v", current, restored)
	}
}

func TestTaskDefinitionEdit_OriginallyPausedPreservesPauseAndNote(t *testing.T) {
	fixture := newTaskDefinitionEditFixture(t, TaskDefinitionEditOriginalStatePaused)
	target := changedTaskDefinitionEditDefinition(fixture.base, "paused")
	prepared, _ := fixture.prepare(
		t, "edit-paused-0001",
		taskDefinitionEditHead(1, "a"), taskDefinitionEditHead(2, "b"),
		fixture.base, target,
	)
	if prepared.BaseOriginal.State.Note != taskDefinitionEditPausedNote ||
		prepared.TargetFinal.State.Note != taskDefinitionEditPausedNote {
		t.Fatalf("paused note was not frozen and preserved: base=%q target=%q", prepared.BaseOriginal.State.Note, prepared.TargetFinal.State.Note)
	}

	updatesBefore := fixture.fake.counts().unpause
	paused, err := fixture.scheduler.PauseTaskDefinitionEdit(t.Context(), prepared)
	if err != nil {
		t.Fatalf("observe originally paused task: %v", err)
	}
	if paused.Phase != TaskDefinitionEditPhaseBaseOriginal {
		t.Fatalf("paused observation phase = %q, want base_original", paused.Phase)
	}
	if got := fixture.fake.counts().unpause; got != updatesBefore {
		t.Fatalf("PauseTaskDefinitionEdit wrote originally paused task: updates %d -> %d", updatesBefore, got)
	}
	if _, err := fixture.scheduler.RestoreTaskDefinitionEdit(t.Context(), prepared, paused); !errors.Is(err, ErrTaskScheduleInvalid) {
		t.Fatalf("restore before paused edit apply error = %v, want invalid source", err)
	}
	if got := fixture.fake.counts().unpause; got != updatesBefore {
		t.Fatalf("premature RestoreTaskDefinitionEdit wrote originally paused task: updates %d -> %d", updatesBefore, got)
	}

	applied, err := fixture.scheduler.ApplyTaskDefinitionEdit(t.Context(), prepared, paused)
	if err != nil {
		t.Fatalf("apply paused definition edit: %v", err)
	}
	if applied.Phase != TaskDefinitionEditPhaseTargetFinal || !prepared.TargetFinal.State.Paused {
		t.Fatalf("paused apply = %+v, target state=%+v", applied, prepared.TargetFinal.State)
	}
	afterApply := fixture.fake.counts().unpause
	if afterApply != updatesBefore+1 {
		t.Fatalf("paused edit update count = %d, want %d", afterApply, updatesBefore+1)
	}

	restored, err := fixture.scheduler.RestoreTaskDefinitionEdit(t.Context(), prepared, applied)
	if err != nil {
		t.Fatalf("observe paused final state: %v", err)
	}
	if restored != applied {
		t.Fatalf("paused restore observation = %+v, want unchanged %+v", restored, applied)
	}
	if got := fixture.fake.counts().unpause; got != afterApply {
		t.Fatalf("RestoreTaskDefinitionEdit unpaused originally paused task: updates %d -> %d", afterApply, got)
	}
	request := fixture.fake.rawRequestSnapshot()
	if !request.updatePaused || request.updateNote != taskDefinitionEditPausedNote {
		t.Fatalf("paused target raw state = paused:%v note:%q, want paused with original note", request.updatePaused, request.updateNote)
	}
}

func TestTaskDefinitionEdit_ResponseLossAdoptsCommittedDestination(t *testing.T) {
	fixture := newTaskDefinitionEditFixture(t, TaskDefinitionEditOriginalStateActive)
	prepared, _ := fixture.prepare(
		t, "edit-response-loss",
		taskDefinitionEditHead(1, "a"), taskDefinitionEditHead(2, "b"),
		fixture.base, changedTaskDefinitionEditDefinition(fixture.base, "loss"),
	)

	ctx, cancel := context.WithCancel(t.Context())
	fixture.fake.unpauseCommitErr = serviceerror.NewUnavailable("response lost after commit")
	fixture.fake.afterCommit = cancel
	paused, err := fixture.scheduler.PauseTaskDefinitionEdit(ctx, prepared)
	if err != nil {
		t.Fatalf("response-lost pause did not converge by detached Describe: %v", err)
	}
	if paused.Phase != TaskDefinitionEditPhaseBasePaused {
		t.Fatalf("response-lost pause phase = %q, want base_paused", paused.Phase)
	}
	if ctx.Err() == nil {
		t.Fatal("test did not cancel caller context after the committed update")
	}
}

func TestTaskDefinitionEdit_ExactDestinationProofWinsUpdateNotFound(t *testing.T) {
	fixture := newTaskDefinitionEditFixture(t, TaskDefinitionEditOriginalStateActive)
	prepared, _ := fixture.prepare(
		t, "edit-destination-wins-not-found",
		taskDefinitionEditHead(1, "a"), taskDefinitionEditHead(2, "b"),
		fixture.base, changedTaskDefinitionEditDefinition(fixture.base, "destination-proof"),
	)
	fixture.fake.unpauseCommitErr = serviceerror.NewNotFound("contradictory response after commit")

	paused, err := fixture.scheduler.PauseTaskDefinitionEdit(t.Context(), prepared)
	if err != nil {
		t.Fatalf("exact destination did not win over UpdateSchedule NotFound: %v", err)
	}
	if paused.Phase != TaskDefinitionEditPhaseBasePaused || paused.RepresentationDigest != prepared.BasePaused.Digest {
		t.Fatalf("destination proof = %+v, want exact base_paused", paused)
	}
}

func TestTaskDefinitionEdit_NamespaceNotFoundIsNotScheduleDeletion(t *testing.T) {
	fixture := newTaskDefinitionEditFixture(t, TaskDefinitionEditOriginalStateActive)
	prepared, _ := fixture.prepare(
		t, "edit-namespace-missing",
		taskDefinitionEditHead(1, "a"), taskDefinitionEditHead(2, "b"),
		fixture.base, changedTaskDefinitionEditDefinition(fixture.base, "namespace"),
	)
	fixture.fake.describeErr = serviceerror.NewNamespaceNotFound("missing-namespace")
	updatesBefore := fixture.fake.counts().unpause

	_, err := fixture.scheduler.PauseTaskDefinitionEdit(t.Context(), prepared)
	if !errors.Is(err, ErrTaskScheduleBlocked) {
		t.Fatalf("namespace NotFound error = %v, want blocked rather than schedule not_found", err)
	}
	if got := fixture.fake.counts().unpause; got != updatesBefore {
		t.Fatalf("namespace NotFound issued UpdateSchedule: updates %d -> %d", updatesBefore, got)
	}
}

func TestTaskDefinitionEdit_SuccessWithoutCommitFailsClosed(t *testing.T) {
	fixture := newTaskDefinitionEditFixture(t, TaskDefinitionEditOriginalStateActive)
	prepared, _ := fixture.prepare(
		t, "edit-no-commit",
		taskDefinitionEditHead(1, "a"), taskDefinitionEditHead(2, "b"),
		fixture.base, changedTaskDefinitionEditDefinition(fixture.base, "no-commit"),
	)
	fixture.fake.unpauseNoCommit = true

	_, err := fixture.scheduler.PauseTaskDefinitionEdit(t.Context(), prepared)
	if !errors.Is(err, ErrTaskScheduleOutcomeUnknown) {
		t.Fatalf("nil UpdateSchedule response without commit error = %v, want outcome_unknown", err)
	}
	current, describeErr := fixture.scheduler.DescribeTaskDefinitionEdit(t.Context(), prepared)
	if describeErr != nil {
		t.Fatalf("Describe after rejected no-commit result: %v", describeErr)
	}
	if current.Phase != TaskDefinitionEditPhaseBaseOriginal {
		t.Fatalf("remote phase after no-commit result = %q, want base_original", current.Phase)
	}
}

func TestTaskDefinitionEdit_ResponseLossDecisionTable(t *testing.T) {
	tests := []struct {
		name      string
		configure func(*taskScheduleFakeClient)
		want      error
	}{
		{
			name: "definite rejection leaves exact source",
			configure: func(fake *taskScheduleFakeClient) {
				fake.unpauseErr = serviceerror.NewFailedPrecondition("CAS rejected")
			},
			want: ErrTaskScheduleConflict,
		},
		{
			name: "transient update failure leaves exact source",
			configure: func(fake *taskScheduleFakeClient) {
				fake.unpauseErr = serviceerror.NewUnavailable("update outcome unavailable")
			},
			want: ErrTaskScheduleOutcomeUnknown,
		},
		{
			name: "committed update loses recovery describe",
			configure: func(fake *taskScheduleFakeClient) {
				fake.describeErrors = []error{nil, serviceerror.NewUnavailable("recovery describe unavailable")}
			},
			want: ErrTaskScheduleOutcomeUnknown,
		},
		{
			name: "post update foreign representation",
			configure: func(fake *taskScheduleFakeClient) {
				calls := 0
				fake.rawMutate = func(description *workflowservice.DescribeScheduleResponse) {
					calls++
					if calls == 2 {
						description.GetSchedule().GetState().Notes = "foreign post-update state"
					}
				}
			},
			want: ErrTaskScheduleUnsafeState,
		},
		{
			name: "update deletes before post describe",
			configure: func(fake *taskScheduleFakeClient) {
				fake.unpauseDelete = true
			},
			want: ErrTaskScheduleNotFound,
		},
		{
			name: "source semantic ABA changes revision",
			configure: func(fake *taskScheduleFakeClient) {
				fake.unpauseNoCommit = true
				calls := 0
				fake.rawMutate = func(description *workflowservice.DescribeScheduleResponse) {
					calls++
					if calls == 2 {
						description.ConflictToken = nextTaskScheduleConflictToken(description.GetConflictToken())
					}
				}
			},
			want: ErrTaskScheduleUnsafeState,
		},
		{
			name: "update NotFound wins over failed recovery describe",
			configure: func(fake *taskScheduleFakeClient) {
				fake.unpauseErr = serviceerror.NewNotFound("schedule deleted")
				fake.describeErrors = []error{nil, serviceerror.NewUnavailable("recovery describe unavailable")}
			},
			want: ErrTaskScheduleNotFound,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newTaskDefinitionEditFixture(t, TaskDefinitionEditOriginalStateActive)
			prepared, _ := fixture.prepare(
				t, "edit-response-table",
				taskDefinitionEditHead(1, "a"), taskDefinitionEditHead(2, "b"),
				fixture.base, changedTaskDefinitionEditDefinition(fixture.base, test.name),
			)
			test.configure(fixture.fake)
			createsBefore := fixture.fake.counts().create
			updatesBefore := fixture.fake.counts().unpause

			_, err := fixture.scheduler.PauseTaskDefinitionEdit(t.Context(), prepared)
			if !errors.Is(err, test.want) {
				t.Fatalf("PauseTaskDefinitionEdit error = %v, want %v", err, test.want)
			}
			counts := fixture.fake.counts()
			if counts.create != createsBefore || counts.unpause != updatesBefore+1 {
				t.Fatalf("response-loss branch counts = %+v, want creates=%d updates=%d", counts, createsBefore, updatesBefore+1)
			}
		})
	}
}

func TestTaskDefinitionEdit_StaleSourceRevisionDoesNotWrite(t *testing.T) {
	fixture := newTaskDefinitionEditFixture(t, TaskDefinitionEditOriginalStateActive)
	prepared, _ := fixture.prepare(
		t, "edit-stale-revision",
		taskDefinitionEditHead(1, "a"), taskDefinitionEditHead(2, "b"),
		fixture.base, changedTaskDefinitionEditDefinition(fixture.base, "stale"),
	)
	if !fixture.fake.mutate(prepared.Creation.TaskID, func(*client.ScheduleDescription) {}) {
		t.Fatal("bump fake conflict token: missing schedule")
	}
	updatesBefore := fixture.fake.counts().unpause

	_, err := fixture.scheduler.PauseTaskDefinitionEdit(t.Context(), prepared)
	if !errors.Is(err, ErrTaskScheduleUnsafeState) {
		t.Fatalf("stale source revision error = %v, want unsafe_state", err)
	}
	if got := fixture.fake.counts().unpause; got != updatesBefore {
		t.Fatalf("stale source issued UpdateSchedule: updates %d -> %d", updatesBefore, got)
	}
}

func TestTaskDefinitionEdit_UnknownNestedProtoFieldFailsClosed(t *testing.T) {
	fixture := newTaskDefinitionEditFixture(t, TaskDefinitionEditOriginalStateActive)
	prepared, _ := fixture.prepare(
		t, "edit-unknown-proto",
		taskDefinitionEditHead(1, "a"), taskDefinitionEditHead(2, "b"),
		fixture.base, changedTaskDefinitionEditDefinition(fixture.base, "unknown"),
	)
	fixture.fake.rawMutate = func(description *workflowservice.DescribeScheduleResponse) {
		description.GetSchedule().GetSpec().ProtoReflect().SetUnknown([]byte{0xf8, 0x07, 0x01})
	}
	updatesBefore := fixture.fake.counts().unpause

	_, err := fixture.scheduler.DescribeTaskDefinitionEdit(t.Context(), prepared)
	if !errors.Is(err, ErrTaskScheduleUnsafeState) {
		t.Fatalf("unknown nested protobuf field error = %v, want unsafe_state", err)
	}
	if got := fixture.fake.counts().unpause; got != updatesBefore {
		t.Fatalf("unknown protobuf field issued UpdateSchedule: updates %d -> %d", updatesBefore, got)
	}
}

func TestTaskDefinitionEdit_NotFoundNeverCreates(t *testing.T) {
	fixture := newTaskDefinitionEditFixture(t, TaskDefinitionEditOriginalStateActive)
	prepared, _ := fixture.prepare(
		t, "edit-delete-wins",
		taskDefinitionEditHead(1, "a"), taskDefinitionEditHead(2, "b"),
		fixture.base, changedTaskDefinitionEditDefinition(fixture.base, "deleted"),
	)
	fixture.fake.mu.Lock()
	delete(fixture.fake.schedules, prepared.Creation.TaskID)
	fixture.fake.mu.Unlock()
	createsBefore := fixture.fake.counts().create
	updatesBefore := fixture.fake.counts().unpause

	_, err := fixture.scheduler.PauseTaskDefinitionEdit(t.Context(), prepared)
	if !errors.Is(err, ErrTaskScheduleNotFound) {
		t.Fatalf("deleted task pause error = %v, want not_found", err)
	}
	counts := fixture.fake.counts()
	if counts.create != createsBefore || counts.unpause != updatesBefore {
		t.Fatalf("NotFound mutated Temporal: creates %d->%d updates %d->%d", createsBefore, counts.create, updatesBefore, counts.unpause)
	}
}

func TestTaskDefinitionEdit_DefinitionOnlyEditChangesRemoteMarker(t *testing.T) {
	fixture := newTaskDefinitionEditFixture(t, TaskDefinitionEditOriginalStateActive)
	prepared, _ := fixture.prepare(
		t, "edit-definition-only",
		taskDefinitionEditHead(1, "a"), taskDefinitionEditHead(2, "b"),
		fixture.base, fixture.base,
	)
	fingerprint := prepared.TargetFinal.Fingerprint
	if fingerprint.DefinitionVersion != prepared.TargetHead.Version ||
		fingerprint.DefinitionDigest != prepared.TargetHead.Digest ||
		fingerprint.EditOperationDigest != prepared.OperationDigest ||
		fingerprint.EditPhase != "final_active" {
		t.Fatalf("definition-only target marker = %+v, want target head + operation + final phase", fingerprint)
	}
	if prepared.BaseOriginal.Digest == prepared.TargetFinal.Digest {
		t.Fatal("definition-only edit did not change the frozen remote representation")
	}

	paused, err := fixture.scheduler.PauseTaskDefinitionEdit(t.Context(), prepared)
	if err != nil {
		t.Fatalf("pause definition-only edit: %v", err)
	}
	applied, err := fixture.scheduler.ApplyTaskDefinitionEdit(t.Context(), prepared, paused)
	if err != nil {
		t.Fatalf("apply definition-only edit: %v", err)
	}
	final, err := fixture.scheduler.RestoreTaskDefinitionEdit(t.Context(), prepared, applied)
	if err != nil {
		t.Fatalf("restore definition-only edit: %v", err)
	}
	if final.RepresentationDigest != prepared.TargetFinal.Digest {
		t.Fatalf("definition-only final digest = %q, want %q", final.RepresentationDigest, prepared.TargetFinal.Digest)
	}
}

func TestTaskDefinitionEdit_FinalPausedMarkerRemainsValidAfterActivation(t *testing.T) {
	fixture := newTaskDefinitionEditFixture(t, TaskDefinitionEditOriginalStatePaused)
	head1 := taskDefinitionEditHead(1, "a")
	head2 := taskDefinitionEditHead(2, "b")
	definition2 := changedTaskDefinitionEditDefinition(fixture.base, "v2")
	prepared1, _ := fixture.prepare(
		t, "edit-paused-v1-v2",
		head1, head2,
		fixture.base, definition2,
	)
	base, err := fixture.scheduler.PauseTaskDefinitionEdit(t.Context(), prepared1)
	if err != nil {
		t.Fatalf("observe paused v1 base: %v", err)
	}
	final, err := fixture.scheduler.ApplyTaskDefinitionEdit(t.Context(), prepared1, base)
	if err != nil {
		t.Fatalf("apply paused v2 target: %v", err)
	}
	if _, err := fixture.scheduler.RestoreTaskDefinitionEdit(
		t.Context(), prepared1, final,
	); err != nil {
		t.Fatalf("reobserve paused v2 target: %v", err)
	}
	if prepared1.TargetFinal.Fingerprint.EditPhase != "final_paused" {
		t.Fatalf(
			"paused target marker = %q, want final_paused",
			prepared1.TargetFinal.Fingerprint.EditPhase,
		)
	}

	const activationNote = "runtime cutover activated finalized task"
	if !fixture.fake.mutate(fixture.creation.TaskID, func(
		description *client.ScheduleDescription,
	) {
		description.Schedule.State.Paused = false
		description.Schedule.State.Note = activationNote
	}) {
		t.Fatal("activate finalized paused task: missing fake record")
	}

	definition3 := changedTaskDefinitionEditDefinition(definition2, "v3")
	fixture.state = TaskDefinitionEditOriginalStateActive
	prepared2, snapshot2 := fixture.prepare(
		t, "edit-activated-v2-v3",
		head2, taskDefinitionEditHead(3, "c"),
		definition2, definition3,
	)
	if prepared2.BaseOriginal.State.Paused ||
		prepared2.BaseOriginal.State.Note != activationNote ||
		snapshot2.Phase != TaskDefinitionEditPhaseBaseOriginal {
		t.Fatalf(
			"activated finalized base = %+v snapshot=%+v",
			prepared2.BaseOriginal.State, snapshot2,
		)
	}
}

func TestTaskDefinitionEdit_NewerOperationMarkerRejectsLateRestore(t *testing.T) {
	fixture := newTaskDefinitionEditFixture(t, TaskDefinitionEditOriginalStateActive)
	head1 := taskDefinitionEditHead(1, "a")
	head2 := taskDefinitionEditHead(2, "b")
	head3 := taskDefinitionEditHead(3, "c")
	definition2 := changedTaskDefinitionEditDefinition(fixture.base, "v2")
	prepared1, _ := fixture.prepare(t, "edit-v1-v2", head1, head2, fixture.base, definition2)
	paused1, err := fixture.scheduler.PauseTaskDefinitionEdit(t.Context(), prepared1)
	if err != nil {
		t.Fatalf("pause v1->v2: %v", err)
	}
	applied1, err := fixture.scheduler.ApplyTaskDefinitionEdit(t.Context(), prepared1, paused1)
	if err != nil {
		t.Fatalf("apply v1->v2: %v", err)
	}
	if _, err := fixture.scheduler.RestoreTaskDefinitionEdit(t.Context(), prepared1, applied1); err != nil {
		t.Fatalf("restore v1->v2: %v", err)
	}

	definition3 := changedTaskDefinitionEditDefinition(definition2, "v3")
	definition3.Spec = ScheduleSpec{Cron: "40 10 * * *", TZ: "Asia/Shanghai"}
	prepared2, _ := fixture.prepare(t, "edit-v2-v3", head2, head3, definition2, definition3)
	paused2, err := fixture.scheduler.PauseTaskDefinitionEdit(t.Context(), prepared2)
	if err != nil {
		t.Fatalf("pause v2->v3: %v", err)
	}
	updatesBeforeLateRestore := fixture.fake.counts().unpause

	_, err = fixture.scheduler.RestoreTaskDefinitionEdit(t.Context(), prepared1, applied1)
	if !errors.Is(err, ErrTaskScheduleUnsafeState) {
		t.Fatalf("late v1->v2 restore during v2->v3 pause error = %v, want unsafe_state", err)
	}
	if got := fixture.fake.counts().unpause; got != updatesBeforeLateRestore {
		t.Fatalf("late restore overwrote newer operation pause: updates %d -> %d", updatesBeforeLateRestore, got)
	}
	forgedFreshRevision := applied1
	forgedFreshRevision.Revision = paused2.Revision
	_, err = fixture.scheduler.RestoreTaskDefinitionEdit(t.Context(), prepared1, forgedFreshRevision)
	if !errors.Is(err, ErrTaskScheduleUnsafeState) {
		t.Fatalf("late restore with forged current revision error = %v, want operation-marker unsafe_state", err)
	}
	if got := fixture.fake.counts().unpause; got != updatesBeforeLateRestore {
		t.Fatalf("forged-revision restore overwrote newer operation pause: updates %d -> %d", updatesBeforeLateRestore, got)
	}

	applied2, err := fixture.scheduler.ApplyTaskDefinitionEdit(t.Context(), prepared2, paused2)
	if err != nil {
		t.Fatalf("apply v2->v3 after rejecting late restore: %v", err)
	}
	final2, err := fixture.scheduler.RestoreTaskDefinitionEdit(t.Context(), prepared2, applied2)
	if err != nil {
		t.Fatalf("restore v2->v3: %v", err)
	}
	if final2.RepresentationDigest != prepared2.TargetFinal.Digest {
		t.Fatalf("v3 final digest = %q, want %q", final2.RepresentationDigest, prepared2.TargetFinal.Digest)
	}
}

func TestTaskDefinitionEdit_MutationEntrypointsRejectInvalidContextBeforeIO(t *testing.T) {
	fixture := newTaskDefinitionEditFixture(t, TaskDefinitionEditOriginalStateActive)
	prepared, base := fixture.prepare(
		t, "edit-context-guard",
		taskDefinitionEditHead(1, "a"), taskDefinitionEditHead(2, "b"),
		fixture.base, changedTaskDefinitionEditDefinition(fixture.base, "context"),
	)
	operations := []struct {
		name string
		call func(context.Context) error
	}{
		{
			name: "pause",
			call: func(ctx context.Context) error {
				_, err := fixture.scheduler.PauseTaskDefinitionEdit(ctx, prepared)
				return err
			},
		},
		{
			name: "apply",
			call: func(ctx context.Context) error {
				_, err := fixture.scheduler.ApplyTaskDefinitionEdit(ctx, prepared, base)
				return err
			},
		},
		{
			name: "restore",
			call: func(ctx context.Context) error {
				_, err := fixture.scheduler.RestoreTaskDefinitionEdit(ctx, prepared, base)
				return err
			},
		},
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	contexts := []struct {
		name string
		ctx  context.Context
		want error
	}{
		{name: "nil", ctx: nil, want: ErrTaskScheduleInvalid},
		{name: "canceled", ctx: canceled, want: ErrTaskScheduleTransient},
	}
	for _, operation := range operations {
		for _, contextCase := range contexts {
			t.Run(operation.name+"/"+contextCase.name, func(t *testing.T) {
				before := fixture.fake.counts()
				err := operation.call(contextCase.ctx)
				if !errors.Is(err, contextCase.want) {
					t.Fatalf("error = %v, want %v", err, contextCase.want)
				}
				if after := fixture.fake.counts(); after != before {
					t.Fatalf("invalid context performed schedule I/O: before=%+v after=%+v", before, after)
				}
			})
		}
	}
}

func TestTaskDefinitionEdit_PreparedJSONRoundTripAndDigestTamper(t *testing.T) {
	fixture := newTaskDefinitionEditFixture(t, TaskDefinitionEditOriginalStateActive)
	prepared, _ := fixture.prepare(
		t, "edit-json-roundtrip",
		taskDefinitionEditHead(1, "a"), taskDefinitionEditHead(2, "b"),
		fixture.base, changedTaskDefinitionEditDefinition(fixture.base, "json"),
	)
	encoded, err := json.Marshal(prepared)
	if err != nil {
		t.Fatalf("marshal prepared definition edit: %v", err)
	}
	var recovered PreparedTaskDefinitionEdit
	if err := json.Unmarshal(encoded, &recovered); err != nil {
		t.Fatalf("unmarshal prepared definition edit: %v", err)
	}
	if !reflect.DeepEqual(recovered, prepared) {
		t.Fatalf("prepared JSON round trip changed value\n got: %+v\nwant: %+v", recovered, prepared)
	}
	if _, err := fixture.scheduler.DescribeTaskDefinitionEdit(t.Context(), recovered); err != nil {
		t.Fatalf("Describe with round-tripped prepared wire: %v", err)
	}

	tampered := recovered
	tampered.TargetFinal.State.Note += "-tampered"
	updatesBefore := fixture.fake.counts().unpause
	_, err = fixture.scheduler.PauseTaskDefinitionEdit(t.Context(), tampered)
	if !errors.Is(err, ErrTaskScheduleInvalid) {
		t.Fatalf("tampered prepared representation error = %v, want invalid", err)
	}
	if got := fixture.fake.counts().unpause; got != updatesBefore {
		t.Fatalf("tampered prepared wire issued UpdateSchedule: updates %d -> %d", updatesBefore, got)
	}

	operationTampered := recovered
	operationTampered.OperationID = "edit-json-roundtrip-forged"
	operationTampered.RequestDigest, err = digestPreparedTaskDefinitionEdit(operationTampered)
	if err != nil {
		t.Fatalf("recompute outer request digest for operation tamper: %v", err)
	}
	_, err = fixture.scheduler.PauseTaskDefinitionEdit(t.Context(), operationTampered)
	if !errors.Is(err, ErrTaskScheduleInvalid) {
		t.Fatalf("tampered operation with recomputed outer digest error = %v, want invalid", err)
	}
	if got := fixture.fake.counts().unpause; got != updatesBefore {
		t.Fatalf("tampered operation issued UpdateSchedule: updates %d -> %d", updatesBefore, got)
	}

	runtimeTampered := recovered
	alteredRuntime := workflow.CompiledRuntimeSnapshotV1
	if runtimeTampered.BaseOriginal.Action.Params.RuntimeVersion == alteredRuntime {
		alteredRuntime = ""
	}
	runtimeTampered.TargetPaused.Action.Params.RuntimeVersion = alteredRuntime
	runtimeTampered.TargetFinal.Action.Params.RuntimeVersion = alteredRuntime
	runtimeTampered.OperationDigest, err = digestTaskDefinitionEditOperationSeed(
		taskDefinitionEditOperationSeedFromPrepared(runtimeTampered),
	)
	if err != nil {
		t.Fatalf("recompute operation digest for runtime tamper: %v", err)
	}
	runtimeTampered.BasePaused.Fingerprint = taskDefinitionEditFingerprintFor(
		runtimeTampered.BaseOriginal.Fingerprint,
		runtimeTampered.BaseHead,
		runtimeTampered.OperationDigest,
		"base_paused",
	)
	runtimeTampered.BasePaused.State.Note = taskDefinitionEditNote("base_paused", runtimeTampered.OperationDigest)
	runtimeTampered.TargetPaused.Fingerprint = taskDefinitionEditFingerprintFor(
		runtimeTampered.BaseOriginal.Fingerprint,
		runtimeTampered.TargetHead,
		runtimeTampered.OperationDigest,
		"target_paused",
	)
	runtimeTampered.TargetPaused.State.Note = taskDefinitionEditNote("target_paused", runtimeTampered.OperationDigest)
	runtimeTampered.TargetFinal.Fingerprint = taskDefinitionEditFingerprintFor(
		runtimeTampered.BaseOriginal.Fingerprint,
		runtimeTampered.TargetHead,
		runtimeTampered.OperationDigest,
		"final_active",
	)
	runtimeTampered.TargetFinal.State.Note = taskDefinitionEditNote("final_active", runtimeTampered.OperationDigest)
	for _, representation := range []*PreparedTaskDefinitionEditSchedule{
		&runtimeTampered.BasePaused,
		&runtimeTampered.TargetPaused,
		&runtimeTampered.TargetFinal,
	} {
		representation.Digest, err = digestPreparedTaskDefinitionEditSchedule(*representation)
		if err != nil {
			t.Fatalf("recompute representation digest for runtime tamper: %v", err)
		}
	}
	runtimeTampered.RequestDigest, err = digestPreparedTaskDefinitionEdit(runtimeTampered)
	if err != nil {
		t.Fatalf("recompute outer request digest for runtime tamper: %v", err)
	}
	_, err = fixture.scheduler.PauseTaskDefinitionEdit(t.Context(), runtimeTampered)
	if !errors.Is(err, ErrTaskScheduleInvalid) {
		t.Fatalf("fully resealed runtime-version tamper error = %v, want invalid", err)
	}
	if got := fixture.fake.counts().unpause; got != updatesBefore {
		t.Fatalf("runtime-version tamper issued UpdateSchedule: updates %d -> %d", updatesBefore, got)
	}
}
