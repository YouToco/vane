package task

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/YouToco/vane/scheduler"
	"github.com/YouToco/vane/taskstate"
	"github.com/YouToco/vane/types"
)

type definitionEditControllerFakeStore struct {
	tenantID int64
	status   types.ScheduleStatus
	version  int64
	digest   string
	base     taskstate.ApprovedDefinitionV1
	creation []byte
	basisErr error

	operation *types.TaskDefinitionEditOperation
	loadErr   error
}

func (f *definitionEditControllerFakeStore) LoadTaskDefinitionEditProposalBasis(
	context.Context,
	int64,
	string,
) (
	int64,
	types.ScheduleStatus,
	int64,
	string,
	taskstate.ApprovedDefinitionV1,
	[]byte,
	error,
) {
	return f.tenantID, f.status, f.version, f.digest, f.base, f.creation, f.basisErr
}

func (f *definitionEditControllerFakeStore) LoadTaskDefinitionEditOperationByActor(
	context.Context,
	string,
	int64,
) (*types.TaskDefinitionEditOperation, error) {
	return f.operation, f.loadErr
}

type definitionEditControllerFakeCoordinator struct {
	prepareCalls []PrepareTaskDefinitionEditProposalInput
	prepareOp    *types.TaskDefinitionEditOperation
	prepareErr   error

	confirmScopes   []types.TaskDefinitionEditScope
	confirmReceipts []TaskDefinitionEditReceiptTarget
	confirmOutcome  TaskDefinitionEditOutcome
	confirmErr      error

	cancelScopes   []types.TaskDefinitionEditScope
	cancelReceipts []TaskDefinitionEditReceiptTarget
	cancelOutcome  TaskDefinitionEditOutcome
	cancelErr      error
}

func (f *definitionEditControllerFakeCoordinator) PrepareAndSealProposal(
	_ context.Context,
	in PrepareTaskDefinitionEditProposalInput,
) (*types.TaskDefinitionEditOperation, error) {
	f.prepareCalls = append(f.prepareCalls, in)
	return f.prepareOp, f.prepareErr
}

func (f *definitionEditControllerFakeCoordinator) Confirm(
	_ context.Context,
	scope types.TaskDefinitionEditScope,
	receipt TaskDefinitionEditReceiptTarget,
) (TaskDefinitionEditOutcome, error) {
	f.confirmScopes = append(f.confirmScopes, scope)
	f.confirmReceipts = append(f.confirmReceipts, receipt)
	return f.confirmOutcome, f.confirmErr
}

func (f *definitionEditControllerFakeCoordinator) Cancel(
	_ context.Context,
	scope types.TaskDefinitionEditScope,
	receipt TaskDefinitionEditReceiptTarget,
) (TaskDefinitionEditOutcome, error) {
	f.cancelScopes = append(f.cancelScopes, scope)
	f.cancelReceipts = append(f.cancelReceipts, receipt)
	return f.cancelOutcome, f.cancelErr
}

func TestDefinitionEditController_ProposeSealsExactPatchedDefinition(t *testing.T) {
	base := definitionEditControllerBase(t)
	baseDigest, err := taskstate.DigestApprovedDefinitionV1(base)
	if err != nil {
		t.Fatal(err)
	}
	creation := scheduler.PreparedTaskSchedule{
		TaskID: "task-edit-1", TenantID: 7, UserID: 11,
		PreparedDigest: strings.Repeat("a", 64),
	}
	store := &definitionEditControllerFakeStore{
		tenantID: 7, status: types.ScheduleStatusActive,
		version: 3, digest: baseDigest, base: base,
		creation: mustJSON(t, creation),
	}
	coordinator := &definitionEditControllerFakeCoordinator{
		prepareOp: &types.TaskDefinitionEditOperation{
			ID: "edit-action-1", TenantID: 7, UserID: 11,
			TargetTenantID: 7, TargetUserID: 11, TaskID: "task-edit-1",
		},
	}
	controller := NewDefinitionEditController(store, coordinator)
	sessionID := int64(91)
	proposal, err := controller.Propose(t.Context(), DefinitionEditProposalInput{
		ActionID: "edit-action-1", UserID: 11, SessionID: &sessionID,
		RawArgs: json.RawMessage(`{
			"task_id":"task-edit-1",
			"spec":{"cron":"30 9 * * *","tz":"Asia/Shanghai"},
			"intent":"只监控 AI 官方重大更新",
			"nl_description":"每天 09:30 推送重大更新",
			"strictness":"strict"
		}`),
		ExpiresAt: time.Now().Add(time.Hour),
	})
	if err != nil {
		t.Fatalf("Propose() error = %v", err)
	}
	if proposal.ID != "edit-action-1" {
		t.Fatalf("proposal ID = %q", proposal.ID)
	}
	for _, want := range []string{
		"task-edit-1", "每天 09:30", "只监控 AI 官方重大更新", "严格",
	} {
		if !strings.Contains(proposal.Summary, want) {
			t.Fatalf("summary missing %q: %s", want, proposal.Summary)
		}
	}
	if len(coordinator.prepareCalls) != 1 {
		t.Fatalf("PrepareAndSealProposal calls = %d, want 1", len(coordinator.prepareCalls))
	}
	got := coordinator.prepareCalls[0]
	if got.OperationID != "edit-action-1" ||
		got.ApprovalRef != "definition-edit:edit-action-1" ||
		got.ActorTenantID != 7 || got.ActorUserID != 11 ||
		got.TargetTenantID != 7 || got.TargetUserID != 11 ||
		got.TaskID != "task-edit-1" || got.SessionID != sessionID ||
		got.OriginalStatus != types.ScheduleStatusActive ||
		got.BaseHead.Version != 3 || got.BaseHead.Digest != baseDigest ||
		got.TargetHead.Version != 4 || !reflect.DeepEqual(got.Creation, creation) {
		t.Fatalf("sealed input identity drifted: %+v", got)
	}
	if got.BaseDefinition.Intent != base.Intent ||
		got.TargetDefinition.Intent != "只监控 AI 官方重大更新" ||
		got.TargetDefinition.PlaybookContent != "只监控 AI 官方重大更新" ||
		got.TargetDefinition.NLDescription != "每天 09:30 推送重大更新" ||
		got.TargetDefinition.Strictness != types.StrictnessStrict ||
		string(got.TargetDefinition.SpecJSON) != `{"cron":"30 9 * * *","tz":"Asia/Shanghai"}` {
		t.Fatalf("target definition patch drifted: %+v", got.TargetDefinition)
	}
	targetDigest, err := taskstate.DigestApprovedDefinitionV1(got.TargetDefinition)
	if err != nil {
		t.Fatal(err)
	}
	if got.TargetHead.Digest != targetDigest || got.TargetHead.Digest == got.BaseHead.Digest {
		t.Fatalf("target head = %+v, digest=%q", got.TargetHead, targetDigest)
	}
}

func TestDefinitionEditController_ProposeRejectsNoopBeforeCoordinator(t *testing.T) {
	base := definitionEditControllerBase(t)
	digest, err := taskstate.DigestApprovedDefinitionV1(base)
	if err != nil {
		t.Fatal(err)
	}
	store := &definitionEditControllerFakeStore{
		tenantID: 7, status: types.ScheduleStatusActive,
		version: 1, digest: digest, base: base,
	}
	coordinator := &definitionEditControllerFakeCoordinator{}
	controller := NewDefinitionEditController(store, coordinator)
	sessionID := int64(2)
	_, err = controller.Propose(t.Context(), DefinitionEditProposalInput{
		ActionID: "edit-noop", UserID: 11, SessionID: &sessionID,
		RawArgs: json.RawMessage(`{
			"task_id":"task-edit-1",
			"intent":"监控 AI 官方动态"
		}`),
		ExpiresAt: time.Now().Add(time.Hour),
	})
	if !errors.Is(err, types.ErrValidation) {
		t.Fatalf("no-op error = %v, want validation", err)
	}
	if len(coordinator.prepareCalls) != 0 {
		t.Fatalf("no-op reached coordinator: %+v", coordinator.prepareCalls)
	}
}

func TestDefinitionEditController_ProposeRejectsNullPatchFieldsBeforeStore(t *testing.T) {
	storeErr := errors.New("basis store must not be reached")
	controller := NewDefinitionEditController(
		&definitionEditControllerFakeStore{basisErr: storeErr},
		&definitionEditControllerFakeCoordinator{},
	)
	sessionID := int64(2)
	for name, raw := range map[string]string{
		"null spec":        `{"task_id":"task-edit-1","spec":null}`,
		"null description": `{"task_id":"task-edit-1","nl_description":null}`,
	} {
		t.Run(name, func(t *testing.T) {
			_, err := controller.Propose(
				t.Context(),
				DefinitionEditProposalInput{
					ActionID: "edit-null", UserID: 11, SessionID: &sessionID,
					RawArgs:   json.RawMessage(raw),
					ExpiresAt: time.Now().Add(time.Hour),
				},
			)
			if !errors.Is(err, types.ErrValidation) || errors.Is(err, storeErr) {
				t.Fatalf("null patch error = %v, want pre-store validation", err)
			}
		})
	}
}

func TestDefinitionEditController_ConfirmAndCancelUseRecoveredScope(t *testing.T) {
	scope := types.TaskDefinitionEditScope{
		ID: "edit-action-2", TenantID: 7, UserID: 11,
		TargetTenantID: 7, TargetUserID: 11, TaskID: "task-edit-1",
	}
	store := &definitionEditControllerFakeStore{
		operation: &types.TaskDefinitionEditOperation{
			ID: scope.ID, TenantID: scope.TenantID, UserID: scope.UserID,
			TargetTenantID: scope.TargetTenantID, TargetUserID: scope.TargetUserID,
			TaskID: scope.TaskID,
		},
	}
	coordinator := &definitionEditControllerFakeCoordinator{
		confirmOutcome: TaskDefinitionEditOutcome{
			OperationID: scope.ID, TaskID: scope.TaskID,
			Status: types.TaskDefinitionEditOperationStatusCompleted,
		},
		cancelOutcome: TaskDefinitionEditOutcome{
			OperationID: scope.ID, TaskID: scope.TaskID,
			Status: types.TaskDefinitionEditOperationStatusCancelled,
		},
	}
	controller := NewDefinitionEditController(store, coordinator)
	receipt := TaskDefinitionEditReceiptTarget{
		Provider: "feishu_card_patch/app", Target: "om_original",
	}
	confirmed, err := controller.Confirm(t.Context(), 11, scope.ID, receipt)
	if err != nil || confirmed.OperationID != scope.ID {
		t.Fatalf("Confirm() = %+v, %v", confirmed, err)
	}
	cancelled, err := controller.Cancel(t.Context(), 11, scope.ID, receipt)
	if err != nil || cancelled.Status != types.TaskDefinitionEditOperationStatusCancelled {
		t.Fatalf("Cancel() = %+v, %v", cancelled, err)
	}
	if len(coordinator.confirmScopes) != 1 || coordinator.confirmScopes[0] != scope ||
		len(coordinator.cancelScopes) != 1 || coordinator.cancelScopes[0] != scope ||
		coordinator.confirmReceipts[0] != receipt ||
		coordinator.cancelReceipts[0] != receipt {
		t.Fatalf("coordinator routing drifted: confirm=%+v cancel=%+v",
			coordinator.confirmScopes, coordinator.cancelScopes)
	}
}

func TestDefinitionEditController_NotFoundSentinelIsNarrow(t *testing.T) {
	controller := NewDefinitionEditController(
		&definitionEditControllerFakeStore{loadErr: types.ErrNotFound},
		&definitionEditControllerFakeCoordinator{},
	)
	receipt := TaskDefinitionEditReceiptTarget{Provider: "p", Target: "t"}
	if _, err := controller.Confirm(t.Context(), 11, "missing", receipt); !errors.Is(err, ErrDefinitionEditOperationNotFound) {
		t.Fatalf("Confirm() error = %v, want narrow not-found sentinel", err)
	}

	databaseErr := types.NewAppError(types.CodeDatabase, "read failed", nil)
	controller = NewDefinitionEditController(
		&definitionEditControllerFakeStore{loadErr: databaseErr},
		&definitionEditControllerFakeCoordinator{},
	)
	if _, err := controller.Cancel(t.Context(), 11, "broken", receipt); !errors.Is(err, databaseErr) || errors.Is(err, ErrDefinitionEditOperationNotFound) {
		t.Fatalf("Cancel() error = %v, must not downgrade infrastructure failure", err)
	}
}

func definitionEditControllerBase(t *testing.T) taskstate.ApprovedDefinitionV1 {
	t.Helper()
	source := taskstate.ApprovedSourceV1{
		SourceID: 1, Platform: types.PlatformWeb, Capability: types.CapSearch,
		Title: "搜索: AI", URL: "vane://web/search?q=AI",
		Config: json.RawMessage(`{"query":"AI"}`),
	}
	definition, err := taskstate.BuildApprovedDefinitionV1(
		taskstate.ApprovedDefinitionInputV1{
			TenantID: 7, UserID: 11, TaskID: "task-edit-1",
			Intent: "监控 AI 官方动态", NLDescription: "每天 08:00 推送",
			SpecJSON:        json.RawMessage(`{"cron":"0 8 * * *","tz":"Asia/Shanghai"}`),
			ScopeJSON:       json.RawMessage(`{}`),
			PlaybookContent: "监控 AI 官方动态",
			SourceScope:     taskstate.SourceScopeApprovedPlan,
			FetchPlan: json.RawMessage(
				`{"sources":[{"platform":"web","capability":"search",` +
					`"title":"搜索: AI","url":"vane://web/search?q=AI",` +
					`"config":{"query":"AI"}}]}`,
			),
			Strictness: types.StrictnessNormal, Sources: []taskstate.ApprovedSourceV1{source},
			ExecutionMode:  types.ExecutionModeCompiled,
			DeliveryPolicy: taskstate.DeliveryPolicyOwnerFeishu,
			BudgetPolicy:   taskstate.BudgetPolicyInheritTenantQuota,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	return definition
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
