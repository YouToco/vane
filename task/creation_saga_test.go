package task

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

	"github.com/YouToco/vane/observation"
	"github.com/YouToco/vane/scheduler"
	"github.com/YouToco/vane/sourcespec"
	"github.com/YouToco/vane/types"
)

var testCreationReceiptTarget = CreationReceiptTarget{
	Provider: FeishuCardPatchReceiptProviderForApp("cli_task_test"),
	Target:   "om_task_creation_test",
}

type creationSagaFakeStore struct {
	creationPrepareFakeStore
	tenant types.Tenant

	events                   []string
	createCalls              int
	allowTakeover            bool
	definition               types.PausedCompiledTaskDefinition
	definitionLimit          bool
	activationCommitErr      error
	beginCleanupErr          error
	finishCleanupErr         error
	completeErr              error
	acquireErr               error
	acquireApplyBeforeError  bool
	completeApplyBeforeError bool
	resolveCalls             int
	resolveTenantID          int64
	resolveUserID            int64
	resolveSourceIDs         []int64
	resolveSources           map[int64]types.Source
	resolveErr               error
	membershipCalls          int
	tenantCalls              int
}

func newCreationSagaFakeStore() *creationSagaFakeStore {
	return &creationSagaFakeStore{tenant: types.Tenant{
		ID: 7, Status: types.TenantStatusActive,
	}}
}

func (s *creationSagaFakeStore) ListMembershipsByUser(
	_ context.Context,
	userID int64,
) ([]types.Membership, error) {
	s.membershipCalls++
	return []types.Membership{{TenantID: s.tenant.ID, UserID: userID}}, nil
}

func (s *creationSagaFakeStore) GetTenant(context.Context, int64) (*types.Tenant, error) {
	s.tenantCalls++
	tenant := s.tenant
	return &tenant, nil
}

func (s *creationSagaFakeStore) ResolveTaskCreationSources(
	_ context.Context,
	tenantID, userID int64,
	sourceIDs []int64,
) ([]types.Source, error) {
	s.resolveCalls++
	s.resolveTenantID = tenantID
	s.resolveUserID = userID
	s.resolveSourceIDs = append([]int64(nil), sourceIDs...)
	if s.resolveErr != nil {
		return nil, s.resolveErr
	}
	resolved := make([]types.Source, 0, len(sourceIDs))
	for _, sourceID := range sourceIDs {
		source, ok := s.resolveSources[sourceID]
		if !ok {
			return nil, types.NewAppError(
				types.CodeNotFound, "一个或多个已有信源当前不可用", nil,
			)
		}
		source.Config = bytes.Clone(source.Config)
		resolved = append(resolved, source)
	}
	return resolved, nil
}

func (s *creationSagaFakeStore) LoadTaskCreationOperationByUser(
	_ context.Context,
	id string,
	userID int64,
) (*types.TaskCreationOperation, error) {
	if s.op.ID != id || s.op.UserID != userID ||
		s.op.ExecutionVersion != types.TaskCreationExecutionVersionV1 {
		return nil, types.ErrNotFound
	}
	clone := s.op
	return &clone, nil
}

func (s *creationSagaFakeStore) CreateTaskCreationOperation(
	_ context.Context,
	p types.CreateTaskCreationOperationParams,
) (*types.TaskCreationOperation, error) {
	s.createCalls++
	if s.op.ID != "" {
		return nil, types.ErrConflict
	}
	s.op = types.TaskCreationOperation{
		ID: p.ID, TenantID: p.TenantID, UserID: p.UserID, SessionID: p.SessionID,
		ToolName: "create_schedule", Args: bytes.Clone(p.Args), Summary: p.Summary,
		Status: types.PendingActionStatusPending, ExpiresAt: p.ExpiresAt,
		ExecutionVersion: types.TaskCreationExecutionVersionV1,
	}
	clone := s.op
	return &clone, nil
}

func (s *creationSagaFakeStore) CancelTaskCreationOperation(
	_ context.Context,
	p types.CancelTaskCreationOperationParams,
) (*types.TaskCreationOperation, error) {
	if s.op.ID != p.ID || s.op.TenantID != p.TenantID || s.op.UserID != p.UserID ||
		s.op.ExecutionVersion != types.TaskCreationExecutionVersionV1 {
		return nil, types.ErrNotFound
	}
	if s.op.ReceiptProvider != "" &&
		(s.op.ReceiptProvider != p.ReceiptProvider || s.op.ReceiptTarget != p.ReceiptTarget) {
		return nil, types.ErrConflict
	}
	switch s.op.Status {
	case types.PendingActionStatusCancelled:
		clone := s.op
		return &clone, nil
	case types.PendingActionStatusExecuting:
		if s.op.ReceiptProvider == "" && s.op.ReceiptTarget == "" {
			s.op.ReceiptProvider = p.ReceiptProvider
			s.op.ReceiptTarget = p.ReceiptTarget
		} else if s.op.ReceiptProvider != p.ReceiptProvider ||
			s.op.ReceiptTarget != p.ReceiptTarget {
			return nil, types.ErrConflict
		}
		clone := s.op
		return &clone, types.ErrTaskCreationBusy
	case types.PendingActionStatusPending:
		s.op.ReceiptProvider = p.ReceiptProvider
		s.op.ReceiptTarget = p.ReceiptTarget
		s.op.Status = types.PendingActionStatusCancelled
		s.op.Phase = types.TaskCreationPhaseCancelled
		now := time.Now()
		s.op.TombstonedAt = &now
		clone := s.op
		return &clone, nil
	default:
		return nil, types.ErrTaskCreationTerminal
	}
}

func (s *creationSagaFakeStore) AcquireTaskCreationOperation(
	_ context.Context,
	p types.AcquireTaskCreationOperationParams,
) (*types.TaskCreationOperation, error) {
	if s.op.ID != p.ID || s.op.TenantID != p.TenantID || s.op.UserID != p.UserID {
		return nil, types.ErrNotFound
	}
	injectedErr := s.acquireErr
	if injectedErr != nil && !s.acquireApplyBeforeError {
		s.acquireErr = nil
		return nil, injectedErr
	}
	switch s.op.Status {
	case types.PendingActionStatusPending:
		s.op.ReceiptProvider = p.ReceiptProvider
		s.op.ReceiptTarget = p.ReceiptTarget
		s.op.Status = types.PendingActionStatusExecuting
		s.op.Phase = types.TaskCreationPhaseClaimed
		s.op.LeaseOwner = p.LeaseOwner
		s.op.Fence++
		s.op.Attempt++
	case types.PendingActionStatusExecuting:
		if s.op.ReceiptProvider == "" && s.op.ReceiptTarget == "" &&
			p.ReceiptProvider != "" && p.ReceiptTarget != "" {
			s.op.ReceiptProvider = p.ReceiptProvider
			s.op.ReceiptTarget = p.ReceiptTarget
		} else if s.op.ReceiptProvider != p.ReceiptProvider || s.op.ReceiptTarget != p.ReceiptTarget {
			return nil, types.ErrConflict
		}
		if s.op.LeaseOwner == p.LeaseOwner && s.op.Fence > 0 {
			clone := s.op
			return &clone, nil
		}
		if !s.allowTakeover {
			clone := s.op
			return &clone, types.ErrTaskCreationBusy
		}
		s.allowTakeover = false
		s.op.LeaseOwner = p.LeaseOwner
		s.op.Fence++
		s.op.Attempt++
	default:
		return nil, types.ErrTaskCreationTerminal
	}
	leaseUntil := time.Now().Add(p.LeaseDuration)
	s.op.LeaseUntil = &leaseUntil
	clone := s.op
	if injectedErr != nil {
		s.acquireErr = nil
		return nil, injectedErr
	}
	return &clone, nil
}

func (s *creationSagaFakeStore) CheckpointTaskCreationEnsureReceipt(
	_ context.Context,
	lease types.TaskCreationLease,
	receipt []byte,
	taskID string,
) error {
	if err := s.checkLease(lease); err != nil {
		return err
	}
	s.events = append(s.events, "checkpoint_ensure")
	s.op.EnsureReceipt = bytes.Clone(receipt)
	s.op.TaskID = taskID
	s.op.Phase = types.TaskCreationPhaseScheduleEnsured
	return nil
}

func (s *creationSagaFakeStore) CommitPausedCompiledTaskDefinitionForCreation(
	_ context.Context,
	p types.CommitPausedCompiledTaskDefinitionForCreationParams,
) error {
	if err := s.checkLease(p.Lease); err != nil {
		return err
	}
	s.events = append(s.events, "commit_definition")
	if s.definitionLimit {
		return types.ErrTaskCreationLimit
	}
	if creationPhaseAtLeast(s.op.Phase, types.TaskCreationPhaseDefinitionCommitted) {
		return nil
	}
	if s.op.Phase != types.TaskCreationPhaseScheduleEnsured ||
		s.op.TaskID != p.Definition.TaskID ||
		!bytes.Equal(s.op.PreparedSchedule, p.PreparedSchedule) ||
		!bytes.Equal(s.op.EnsureReceipt, p.EnsureReceipt) {
		return types.ErrConflict
	}
	s.definition = p.Definition
	s.op.Phase = types.TaskCreationPhaseDefinitionCommitted
	return nil
}

func (s *creationSagaFakeStore) BeginTaskCreationActivation(
	_ context.Context,
	lease types.TaskCreationLease,
	_ string,
) (bool, error) {
	if err := s.checkLease(lease); err != nil {
		return false, err
	}
	s.events = append(s.events, "begin_activation")
	if creationPhaseAtLeast(s.op.Phase, types.TaskCreationPhaseActivationStarted) {
		return false, nil
	}
	s.op.Phase = types.TaskCreationPhaseActivationStarted
	return true, nil
}

func (s *creationSagaFakeStore) CommitTaskCreationActivation(
	_ context.Context,
	lease types.TaskCreationLease,
	_ string,
) error {
	if err := s.checkLease(lease); err != nil {
		return err
	}
	s.events = append(s.events, "commit_activation")
	if s.activationCommitErr != nil {
		return s.activationCommitErr
	}
	s.op.Phase = types.TaskCreationPhaseActivated
	return nil
}

func (s *creationSagaFakeStore) BeginTaskCreationCleanup(
	_ context.Context,
	lease types.TaskCreationLease,
	_ string,
	errorCode string,
	errorMessage string,
) (bool, error) {
	if err := s.checkLease(lease); err != nil {
		return false, err
	}
	s.events = append(s.events, "begin_cleanup")
	if s.beginCleanupErr != nil {
		return false, s.beginCleanupErr
	}
	s.op.ErrorCode, s.op.ErrorMessage = errorCode, errorMessage
	s.op.Phase = types.TaskCreationPhaseCleanupPending
	return true, nil
}

func (s *creationSagaFakeStore) FinishTaskCreationCleanup(
	_ context.Context,
	lease types.TaskCreationLease,
	_ string,
	status types.PendingActionStatus,
) error {
	if err := s.checkLease(lease); err != nil {
		return err
	}
	s.events = append(s.events, "finish_cleanup")
	if s.finishCleanupErr != nil {
		return s.finishCleanupErr
	}
	s.op.Status = status
	s.op.Phase = types.TaskCreationPhaseFailed
	now := time.Now()
	s.op.TombstonedAt = &now
	s.op.LeaseUntil = nil
	s.op.TakeoverNotBefore = nil
	return nil
}

func (s *creationSagaFakeStore) BlockTaskCreationOperationAfterSideEffect(
	_ context.Context,
	lease types.TaskCreationLease,
	_ string,
	errorCode string,
	errorMessage string,
) error {
	if err := s.checkLease(lease); err != nil {
		return err
	}
	s.events = append(s.events, "block_side_effect")
	s.op.ErrorCode, s.op.ErrorMessage = errorCode, errorMessage
	s.op.Status = types.PendingActionStatusBlocked
	s.op.Phase = types.TaskCreationPhaseBlocked
	now := time.Now()
	s.op.TombstonedAt = &now
	s.op.LeaseUntil = nil
	s.op.TakeoverNotBefore = nil
	return nil
}

func (s *creationSagaFakeStore) CompleteTaskCreationOperation(
	_ context.Context,
	lease types.TaskCreationLease,
	_ string,
	result json.RawMessage,
) error {
	if err := s.checkLease(lease); err != nil {
		return err
	}
	s.events = append(s.events, "complete")
	if s.completeErr != nil {
		if !s.completeApplyBeforeError {
			return s.completeErr
		}
	}
	s.op.Result = bytes.Clone(result)
	s.op.Status = types.PendingActionStatusExecuted
	s.op.Phase = types.TaskCreationPhaseCompleted
	now := time.Now()
	s.op.ExecutedAt = &now
	s.op.TombstonedAt = &now
	s.op.LeaseUntil = nil
	s.op.TakeoverNotBefore = nil
	if s.terminalWriteCancel != nil {
		s.terminalWriteCancel()
	}
	if s.completeErr != nil {
		return s.completeErr
	}
	return nil
}

func (s *creationSagaFakeStore) ListStaleTaskCreationTenantIDs(
	context.Context, time.Time, int64, int,
) ([]int64, error) {
	return nil, nil
}

func (s *creationSagaFakeStore) ListStaleTaskCreationOperations(
	context.Context, int64, time.Time, int,
) ([]types.TaskCreationOperation, error) {
	return nil, nil
}

type creationSagaFakeScheduler struct {
	events                   []string
	state                    scheduler.TaskScheduleState
	prepared                 scheduler.PreparedTaskSchedule
	prepareErr               error
	ensureErr                error
	activateErr              error
	activateApplyBeforeError bool
	activateCalls            int
	deleteErr                error
	deleteCalls              int
}

type recoveryListingStore struct {
	*creationSagaFakeStore
	tenantIDs []int64
	shardErrs map[int64]error
	fill      bool
	queried   []int64
	limits    []int
	cursors   []int64
}

func (s *recoveryListingStore) ListStaleTaskCreationTenantIDs(
	_ context.Context, _ time.Time, afterTenantID int64, limit int,
) ([]int64, error) {
	s.cursors = append(s.cursors, afterTenantID)
	result := make([]int64, 0, limit)
	for _, tenantID := range s.tenantIDs {
		if tenantID <= afterTenantID {
			continue
		}
		result = append(result, tenantID)
		if len(result) == limit {
			break
		}
	}
	return result, nil
}

func (s *recoveryListingStore) ListStaleTaskCreationOperations(
	_ context.Context,
	tenantID int64,
	_ time.Time,
	limit int,
) ([]types.TaskCreationOperation, error) {
	s.queried = append(s.queried, tenantID)
	s.limits = append(s.limits, limit)
	if err := s.shardErrs[tenantID]; err != nil {
		return nil, err
	}
	if !s.fill {
		return nil, nil
	}
	ops := make([]types.TaskCreationOperation, limit)
	for i := range ops {
		ops[i] = types.TaskCreationOperation{
			ID:       fmt.Sprintf("missing-%d-%d", tenantID, i),
			TenantID: tenantID, UserID: 11,
		}
	}
	return ops, nil
}

func (s *creationSagaFakeScheduler) PrepareTaskSchedule(
	_ context.Context,
	req scheduler.TaskScheduleRequest,
) (scheduler.PreparedTaskSchedule, error) {
	s.events = append(s.events, "prepare")
	if s.prepareErr != nil {
		return scheduler.PreparedTaskSchedule{}, s.prepareErr
	}
	s.prepared = validPreparedSchedule(req)
	return s.prepared, nil
}

func (s *creationSagaFakeScheduler) EnsurePausedTask(
	context.Context,
	scheduler.PreparedTaskSchedule,
) (scheduler.EnsurePausedTaskResult, error) {
	s.events = append(s.events, "ensure")
	if s.ensureErr != nil {
		return scheduler.EnsurePausedTaskResult{}, s.ensureErr
	}
	s.state = scheduler.TaskSchedulePausedProvisioningExact
	return scheduler.EnsurePausedTaskResult{
		Disposition: scheduler.TaskScheduleEnsured,
		Snapshot: scheduler.TaskScheduleSnapshot{
			TaskID: s.prepared.TaskID, RequestDigest: s.prepared.RequestDigest,
			PreparedDigest: s.prepared.PreparedDigest, Revision: "revision-1",
			State: scheduler.TaskSchedulePausedVirginExact,
		},
	}, nil
}

func (s *creationSagaFakeScheduler) DescribeTask(
	context.Context,
	scheduler.PreparedTaskSchedule,
) (scheduler.TaskScheduleSnapshot, error) {
	s.events = append(s.events, "describe")
	return scheduler.TaskScheduleSnapshot{
		TaskID: s.prepared.TaskID, RequestDigest: s.prepared.RequestDigest,
		PreparedDigest: s.prepared.PreparedDigest, Revision: "revision-2",
		State: s.state,
	}, nil
}

func (s *creationSagaFakeScheduler) ActivateTask(
	context.Context,
	scheduler.PreparedTaskSchedule,
	scheduler.TaskScheduleSnapshot,
) (scheduler.TaskScheduleSnapshot, error) {
	s.events = append(s.events, "activate")
	s.activateCalls++
	if s.activateErr != nil {
		if s.activateApplyBeforeError {
			s.state = scheduler.TaskScheduleActiveVirginExact
			s.activateApplyBeforeError = false
		}
		err := s.activateErr
		s.activateErr = nil
		return scheduler.TaskScheduleSnapshot{}, err
	}
	s.state = scheduler.TaskScheduleActiveVirginExact
	return s.DescribeTask(context.Background(), s.prepared)
}

func (s *creationSagaFakeScheduler) DeleteTask(
	context.Context,
	scheduler.PreparedTaskSchedule,
) error {
	s.events = append(s.events, "delete")
	s.deleteCalls++
	if s.deleteErr != nil {
		return s.deleteErr
	}
	s.state = scheduler.TaskScheduleStateUnknown
	return nil
}

func TestCreationCoordinator_ProposalCanonicalizesBeforePersistence(t *testing.T) {
	store := newCreationSagaFakeStore()
	schedules := &creationSagaFakeScheduler{}
	coordinator := NewCreationCoordinator(store, schedules, nil)

	proposal, err := coordinator.Propose(t.Context(), CreationProposalInput{
		ActionID: "action-1", UserID: 11, RawArgs: mustCreateArgs(t, "每天寻找全球 AI 热点", "每天 AI"),
		ExpiresAt: time.Now().Add(time.Hour),
	})
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	if proposal.ID != "action-1" || store.createCalls != 1 || len(schedules.events) != 0 {
		t.Fatalf("proposal=%+v create=%d scheduler_events=%v", proposal, store.createCalls, schedules.events)
	}
	for _, want := range []string{"目标：每天寻找全球 AI 热点", "筛选：标准", "[web/search]", "搜索“AI”"} {
		if !strings.Contains(proposal.Summary, want) {
			t.Fatalf("summary missing %q: %s", want, proposal.Summary)
		}
	}
	command, _, err := normalizeCreateScheduleCommand(store.op.Args)
	if err != nil || !bytes.Equal(command.ApprovedFetchPlan, json.RawMessage(
		`{"sources":[{"platform":"web","capability":"search","title":"A","url":"vane://web/search?q=AI\u0026category=news","config":{"category":"news","query":"AI"}}]}`,
	)) {
		t.Fatalf("durable canonical args invalid: command=%+v err=%v", command, err)
	}
}

func TestCreationCoordinator_ProposalMaterializesVersionedSourceSpecsBeforePersistence(t *testing.T) {
	store := newCreationSagaFakeStore()
	schedules := &creationSagaFakeScheduler{}
	coordinator := NewCreationCoordinator(store, schedules, nil)
	plan := json.RawMessage(`{
		"source_specs":{
			"version":"vane.source-specs/v1",
			"items":[{
				"kind":"web_search",
				"query":"major AI model API pricing deprecation security updates",
				"include_domains":["OpenAI.com","anthropic.com","openai.com","deepmind.google"]
			},{
				"kind":"web_contents",
				"page_url":"https://ai.google.dev/gemini-api/docs/changelog"
			}]
		}
	}`)

	proposal, err := coordinator.Propose(t.Context(), CreationProposalInput{
		ActionID: "action-source-specs", UserID: 11,
		RawArgs: mustCreateArgsWithPlan(
			t, "只监控三家公司官方确认的重大更新", "三家官方 AI 动态", plan,
		),
		ExpiresAt: time.Now().Add(time.Hour),
	})
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	if store.createCalls != 1 || store.resolveCalls != 0 {
		t.Fatalf("纯 source_specs 应直接物化且不读取现有信源: create=%d resolve=%d",
			store.createCalls, store.resolveCalls)
	}
	if bytes.Contains(store.op.Args, []byte("source_specs")) {
		t.Fatalf("durable args 不得保存 transient source_specs: %s", store.op.Args)
	}
	command, _, err := normalizeCreateScheduleCommand(store.op.Args)
	if err != nil {
		t.Fatalf("normalize durable args: %v", err)
	}
	var frozen compiledFetchPlan
	if err := decodeStrictJSON(command.ApprovedFetchPlan, &frozen); err != nil {
		t.Fatalf("decode frozen plan: %v", err)
	}
	if len(frozen.Sources) != 2 {
		t.Fatalf("frozen sources=%d want=2: %s", len(frozen.Sources), command.ApprovedFetchPlan)
	}
	search := frozen.Sources[0]
	if search.URL != "vane://web/search?q=major+AI+model+API+pricing+deprecation+security+updates&include_domains=anthropic.com%2Cdeepmind.google%2Copenai.com" ||
		search.Title != "搜索: major AI model API pricing deprecation security updates" ||
		string(search.Config) != `{"include_domains":["anthropic.com","deepmind.google","openai.com"],"query":"major AI model API pricing deprecation security updates"}` {
		t.Fatalf("web_search 未确定性物化: %+v", search)
	}
	contents := frozen.Sources[1]
	if contents.URL != "vane://web/contents?url=https%3A%2F%2Fai.google.dev%2Fgemini-api%2Fdocs%2Fchangelog" ||
		contents.Title != "页面监控: https://ai.google.dev/gemini-api/docs/changelog" ||
		string(contents.Config) != `{"url":"https://ai.google.dev/gemini-api/docs/changelog"}` {
		t.Fatalf("web_contents 未确定性物化: %+v", contents)
	}
	for _, want := range []string{
		"信息范围（2）",
		"搜索“major AI model API pricing deprecation security updates”",
		"仅限 anthropic.com、deepmind.google、openai.com",
		"页面 https://ai.google.dev/gemini-api/docs/changelog",
	} {
		if !strings.Contains(proposal.Summary, want) {
			t.Fatalf("summary missing %q: %s", want, proposal.Summary)
		}
	}

	// Confirm and every recovery pass consume only the already frozen durable
	// plan. They must not decode source_specs or reinterpret the user's request.
	frozenPlan := bytes.Clone(command.ApprovedFetchPlan)
	result, err := coordinator.Confirm(
		t.Context(), 11, "action-source-specs", testCreationReceiptTarget,
	)
	if err != nil || result.Status != types.PendingActionStatusExecuted ||
		schedules.activateCalls != 1 ||
		!bytes.Equal(store.definition.FetchPlan, frozenPlan) ||
		bytes.Contains(store.definition.FetchPlan, []byte("source_specs")) {
		t.Fatalf("Confirm must execute the exact frozen plan: result=%+v err=%v activate=%d frozen=%s final=%s",
			result, err, schedules.activateCalls, frozenPlan, store.definition.FetchPlan)
	}
}

func TestCreationCoordinator_ProposalPreservesObservationPolicyAcrossCanonicalReplay(t *testing.T) {
	store := newCreationSagaFakeStore()
	schedules := &creationSagaFakeScheduler{}
	coordinator := NewCreationCoordinator(store, schedules, nil)
	policy := observation.PolicySpecV1{
		Schema: observation.SchemaV1,
		Mode:   observation.ModeEvent,
		Window: observation.WindowSpecV1{
			Kind: observation.WindowScheduleInterval,
		},
		LatePolicy: observation.LateStrict,
		Evidence: observation.EvidencePolicyV1{
			Requirement:     observation.EvidenceOfficialRequired,
			OfficialDomains: []string{"openai.com"},
		},
		UnknownTime: observation.UnknownTimeReject,
		Event: &observation.EventPolicyV1{
			Subject:       "OpenAI \u202eAPI",
			EventKind:     "重大版本发布",
			Qualification: observation.QualificationAnnouncement,
		},
		QualifierPrompt: observation.QualifierPromptV1,
	}
	raw := mustMarshal(t, map[string]any{
		"spec":           map[string]any{"every_seconds": 604800, "tz": "Asia/Shanghai"},
		"intent":         "仅监控 OpenAI 官方确认的重大 API 更新；没有重要更新就不发送",
		"nl_description": "每周一上午 9 点检查 OpenAI API 重大更新",
		"strictness":     "strict",
		"approved_fetch_plan": map[string]any{
			"source_specs": map[string]any{
				"version": creationSourceSpecsVersion,
				"items": []any{map[string]any{
					"kind": "web_search", "query": "OpenAI API major updates",
					"include_domains": []string{"openai.com"},
				}},
			},
		},
		"observation_policy": policy,
	})

	proposal, err := coordinator.Propose(t.Context(), CreationProposalInput{
		ActionID: "action-observation-policy", UserID: 11, RawArgs: raw,
		ExpiresAt: time.Now().Add(time.Hour),
	})
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	for _, want := range []string{
		"新鲜度策略：仅事件发生时推送",
		"OpenAI API",
		"重大版本发布",
		"官方宣布即算",
		"窗口 相邻两次计划触发之间",
		"窗口外不补推",
		"日期未知拒绝",
		"仅官方证据：openai.com",
		"无匹配事件不发消息",
	} {
		if !strings.Contains(proposal.Summary, want) {
			t.Fatalf("confirmation summary missing %q: %s", want, proposal.Summary)
		}
	}
	if strings.Contains(proposal.Summary, "\u202e") ||
		len(proposal.Summary) > maxCreationSummaryBytes {
		t.Fatalf("confirmation summary 未安全清洗或超限: len=%d summary=%q",
			len(proposal.Summary), proposal.Summary)
	}
	command, _, err := normalizeCreateScheduleCommand(store.op.Args)
	if err != nil {
		t.Fatalf("normalize durable args: %v", err)
	}
	if command.ObservationPolicy == nil ||
		!bytes.Equal(mustMarshal(t, command.ObservationPolicy), mustMarshal(t, policy)) {
		t.Fatalf("durable command 丢失 observation_policy: args=%s policy=%+v",
			store.op.Args, command.ObservationPolicy)
	}
	canonicalReplay, err := canonicalCreationProposalArgs(command)
	if err != nil || !bytes.Equal(canonicalReplay, store.op.Args) {
		t.Fatalf("canonical replay 漂移: got=%s want=%s err=%v",
			canonicalReplay, store.op.Args, err)
	}
	var equivalent bytes.Buffer
	if err := json.Indent(&equivalent, store.op.Args, "", "  "); err != nil {
		t.Fatal(err)
	}
	if !creationProposalArgsEqual(store.op.Args, equivalent.Bytes()) {
		t.Fatal("JSONB 等价重排必须保持 proposal identity")
	}
	var changed map[string]any
	if err := json.Unmarshal(store.op.Args, &changed); err != nil {
		t.Fatal(err)
	}
	changed["observation_policy"].(map[string]any)["unknown_time"] = "deprioritize"
	if creationProposalArgsEqual(store.op.Args, mustMarshal(t, changed)) {
		t.Fatal("observation_policy 改变不得被 canonical replay 视为同一 proposal")
	}

	store.op.CreatedAt = time.Date(2026, 7, 26, 9, 8, 21, 0, time.UTC)
	result, err := coordinator.Confirm(
		t.Context(), 11, "action-observation-policy", testCreationReceiptTarget,
	)
	if err != nil || result.Status != types.PendingActionStatusExecuted {
		t.Fatalf("Confirm: result=%+v err=%v", result, err)
	}
	var scope compiledTaskScopeV1
	if err := decodeStrictJSON(store.definition.ScopeJSON, &scope); err != nil {
		t.Fatalf("decode compiled scope: %v", err)
	}
	wantCompiled, err := observation.Compile(policy, store.op.CreatedAt)
	if err != nil || scope.Observation == nil ||
		!observationPoliciesEqual(wantCompiled, *scope.Observation) {
		t.Fatalf("compiled observation_policy 漂移: got=%+v want=%+v err=%v",
			scope.Observation, wantCompiled, err)
	}
}

func TestCreationCoordinator_SourceSpecsRejectNonCanonicalObservationPolicyFields(t *testing.T) {
	valid := json.RawMessage(`{
		"spec":{"cron":"0 9 * * 1","tz":"Asia/Shanghai"},
		"intent":"监控官方更新",
		"approved_fetch_plan":{"source_specs":{
			"version":"vane.source-specs/v1",
			"items":[{"kind":"web_search","query":"official updates"}]
		}},
		"observation_policy":{
			"schema":"vane.observation-policy/v1",
			"mode":"content",
			"window":{"kind":"schedule_interval"},
			"late_policy":"strict",
			"evidence":{"requirement":"trusted_allowed"},
			"unknown_time":"reject"
		}
	}`)
	tests := []struct {
		name   string
		mutate func([]byte) []byte
	}{
		{
			name: "unknown field",
			mutate: func(raw []byte) []byte {
				return bytes.Replace(
					raw, []byte(`"unknown_time":"reject"`),
					[]byte(`"unknown_time":"reject","unexpected":true`), 1,
				)
			},
		},
		{
			name: "case folded alias",
			mutate: func(raw []byte) []byte {
				return bytes.Replace(raw, []byte(`"mode"`), []byte(`"Mode"`), 1)
			},
		},
		{
			name: "escaped field alias",
			mutate: func(raw []byte) []byte {
				return bytes.Replace(
					raw, []byte(`"mode"`), []byte(`"mo\u0064e"`), 1,
				)
			},
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			store := newCreationSagaFakeStore()
			coordinator := NewCreationCoordinator(
				store, &creationSagaFakeScheduler{}, nil,
			)
			_, err := coordinator.Propose(t.Context(), CreationProposalInput{
				ActionID: "action-invalid-observation-field", UserID: 11,
				RawArgs:   testCase.mutate(bytes.Clone(valid)),
				ExpiresAt: time.Now().Add(time.Hour),
			})
			if !errors.Is(err, types.ErrValidation) ||
				store.membershipCalls != 0 || store.tenantCalls != 0 ||
				store.resolveCalls != 0 || store.createCalls != 0 {
				t.Fatalf("non-canonical observation field must fail before lookup/write: err=%v store=%+v",
					err, store)
			}
		})
	}
}

func TestSummarizeCreationObservationPolicy_RollingDurationIsExact(t *testing.T) {
	tests := []struct {
		name    string
		seconds int64
		want    string
	}{
		{name: "ninety minutes", seconds: 5400, want: "最近 1小时30分钟"},
		{name: "keeps remaining seconds", seconds: 3601, want: "最近 1小时1秒"},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			got := summarizeCreationObservationPolicy(observation.PolicySpecV1{
				Mode: observation.ModeContent,
				Window: observation.WindowSpecV1{
					Kind:                   observation.WindowRollingDuration,
					RollingDurationSeconds: testCase.seconds,
				},
				LatePolicy:  observation.LateStrict,
				Evidence:    observation.EvidencePolicyV1{Requirement: observation.EvidenceTrustedAllowed},
				UnknownTime: observation.UnknownTimeReject,
			})
			if !strings.Contains(got, testCase.want) {
				t.Fatalf("summary=%q, want exact rolling duration %q", got, testCase.want)
			}
		})
	}
}

func TestCreationCoordinator_SourceSpecsAreStrictAtomicAndUnambiguous(t *testing.T) {
	valid := `{"source_specs":{"version":"vane.source-specs/v1","items":[{"kind":"web_search","query":"AI","include_domains":["openai.com"]}]}}`
	cases := []struct {
		name string
		plan string
		want string
	}{
		{
			name: "wrong version",
			plan: `{"source_specs":{"version":"vane.source-specs/v2","items":[{"kind":"web_search","query":"AI"}]}}`,
			want: "version",
		},
		{
			name: "case folded version key",
			plan: `{"source_specs":{"VERSION":"vane.source-specs/v1","items":[{"kind":"web_search","query":"AI"}]}}`,
			want: "unknown exact field",
		},
		{
			name: "case folded source specs key",
			plan: `{"Source_Specs":{"version":"vane.source-specs/v1","items":[{"kind":"web_search","query":"AI"}]}}`,
			want: "unknown field",
		},
		{
			name: "mixed with materialized sources",
			plan: strings.TrimSuffix(valid, "}") + `,"sources":[{"platform":"web","capability":"search","title":"A","url":"vane://web/search?q=A","config":{"query":"A"}}]}`,
			want: "cannot be mixed",
		},
		{
			name: "null materialized sources cannot hide mixing",
			plan: `{"sources":null,"source_specs":{"version":"vane.source-specs/v1","items":[{"kind":"web_search","query":"AI"}]}}`,
			want: "sources must be an array",
		},
		{
			name: "null source specs is not omission",
			plan: `{"source_specs":null,"sources":[{"platform":"web","capability":"search","url":"vane://web/search?q=AI","config":{"query":"AI"}}]}`,
			want: "source_specs must be an object",
		},
		{
			name: "null existing ids is not omission",
			plan: `{"existing_source_ids":null,"source_specs":{"version":"vane.source-specs/v1","items":[{"kind":"web_search","query":"AI"}]}}`,
			want: "existing_source_ids must be an array",
		},
		{
			name: "unknown field",
			plan: `{"source_specs":{"version":"vane.source-specs/v1","items":[{"kind":"web_search","query":"AI","config":{}}]}}`,
			want: "unknown exact field",
		},
		{
			name: "duplicate inner key",
			plan: `{"source_specs":{"version":"vane.source-specs/v1","items":[{"kind":"web_search","query":"AI","query":"crypto"}]}}`,
			want: "duplicate",
		},
		{
			name: "escaped discriminator key",
			plan: `{"source_specs":{"version":"vane.source-specs/v1","items":[{"\u006bind":"web_search","query":"AI"}]}}`,
			want: "canonical",
		},
		{
			name: "case alias discriminator cannot collapse",
			plan: `{"source_specs":{"version":"vane.source-specs/v1","items":[{"kind":"web_search","KIND":"web_contents","query":"AI"}]}}`,
			want: "unknown exact field",
		},
		{
			name: "irrelevant field",
			plan: `{"source_specs":{"version":"vane.source-specs/v1","items":[{"kind":"web_contents","page_url":"https://openai.com/news","query":"AI"}]}}`,
			want: "unknown exact field",
		},
		{
			name: "null include domains",
			plan: `{"source_specs":{"version":"vane.source-specs/v1","items":[{"kind":"web_search","query":"AI","include_domains":null}]}}`,
			want: "include_domains must be an array",
		},
		{
			name: "null feed categories",
			plan: `{"source_specs":{"version":"vane.source-specs/v1","items":[{"kind":"web_feed","feed_url":"https://openai.com/news/rss.xml","categories":null}]}}`,
			want: "categories must be an array",
		},
		{
			name: "ambiguous identity alternatives",
			plan: `{"source_specs":{"version":"vane.source-specs/v1","items":[{"kind":"xhs_user_posts","user_id":"6a5578b3000000000e03cc00","profile_url":"https://www.xiaohongshu.com/user/profile/6a5578b3000000000e03cc00"}]}}`,
			want: "exactly one",
		},
		{
			name: "invalid xhs user id before any lookup",
			plan: `{"existing_source_ids":[12],"source_specs":{"version":"vane.source-specs/v1","items":[{"kind":"xhs_faved_notes","user_id":"abc"}]}}`,
			want: "24 lowercase hexadecimal",
		},
		{
			name: "invalid include domain",
			plan: `{"source_specs":{"version":"vane.source-specs/v1","items":[{"kind":"web_search","query":"AI","include_domains":["https://openai.com/news"]}]}}`,
			want: "include_domains",
		},
		{
			name: "one bad item rejects whole batch",
			plan: `{"source_specs":{"version":"vane.source-specs/v1","items":[{"kind":"web_search","query":"AI"},{"kind":"web_contents","page_url":"http://127.0.0.1/private"}]}}`,
			want: "network policy",
		},
		{
			name: "canonical duplicate",
			plan: `{"source_specs":{"version":"vane.source-specs/v1","items":[{"kind":"web_search","query":"AI","include_domains":["OpenAI.com"]},{"kind":"web_search","query":"AI","include_domains":["openai.com"]}]}}`,
			want: "duplicated",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := newCreationSagaFakeStore()
			coordinator := NewCreationCoordinator(store, &creationSagaFakeScheduler{}, nil)
			_, err := coordinator.Propose(t.Context(), CreationProposalInput{
				ActionID: "action-invalid-source-spec", UserID: 11,
				RawArgs: mustCreateArgsWithPlan(
					t, "监控官方更新", "官方更新", json.RawMessage(tc.plan),
				),
				ExpiresAt: time.Now().Add(time.Hour),
			})
			if !errors.Is(err, types.ErrValidation) || !strings.Contains(err.Error(), tc.want) ||
				store.createCalls != 0 || store.resolveCalls != 0 ||
				store.membershipCalls != 0 || store.tenantCalls != 0 {
				t.Fatalf("err=%v want_substring=%q create=%d resolve=%d membership=%d tenant=%d",
					err, tc.want, store.createCalls, store.resolveCalls,
					store.membershipCalls, store.tenantCalls)
			}
		})
	}
}

func TestCreationCoordinator_SourceSpecsValidateCommonEnvelopeBeforeMaterializing(t *testing.T) {
	plans := []json.RawMessage{
		json.RawMessage(`{"source_specs":{"version":"bad","items":[]}}`),
		json.RawMessage(`{"source_specs":{"version":"vane.source-specs/v1","items":[]}}`),
		json.RawMessage(`{"existing_source_ids":"not-an-array","source_specs":{
			"version":"vane.source-specs/v1","items":[{"kind":"web_search","query":"AI"}]}}`),
		json.RawMessage(`{"source_specs":{"version":"vane.source-specs/v1","items":[{
			"kind":"web_search","query":"AI","include_domains":["https://openai.com"]
		}]}}`),
	}
	for i, plan := range plans {
		t.Run(fmt.Sprintf("plan_%d", i), func(t *testing.T) {
			store := newCreationSagaFakeStore()
			coordinator := NewCreationCoordinator(store, &creationSagaFakeScheduler{}, nil)
			raw := mustCreateArgsWithPlan(t, "   ", "无效意图", plan)
			_, err := coordinator.Propose(t.Context(), CreationProposalInput{
				ActionID: "action-invalid-envelope-order", UserID: 11, RawArgs: raw,
				ExpiresAt: time.Now().Add(time.Hour),
			})
			if !errors.Is(err, types.ErrValidation) ||
				!strings.Contains(err.Error(), "approved intent must be non-empty") ||
				store.membershipCalls != 0 || store.tenantCalls != 0 ||
				store.resolveCalls != 0 || store.createCalls != 0 {
				t.Fatalf("common envelope validation order drifted: err=%v membership=%d tenant=%d resolve=%d create=%d",
					err, store.membershipCalls, store.tenantCalls,
					store.resolveCalls, store.createCalls)
			}
		})
	}
}

func TestCreationCoordinator_SourceSpecsRejectNullOptionalEnvelopeFields(t *testing.T) {
	plan := `{"source_specs":{"version":"vane.source-specs/v1","items":[{"kind":"web_search","query":"AI"}]}}`
	for _, tc := range []struct {
		name  string
		field string
	}{
		{name: "nl description", field: `"nl_description":null`},
		{name: "strictness", field: `"strictness":null`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newCreationSagaFakeStore()
			coordinator := NewCreationCoordinator(store, &creationSagaFakeScheduler{}, nil)
			raw := json.RawMessage(`{"spec":{"cron":"0 9 * * 1","tz":"Asia/Shanghai"},` +
				`"intent":"监控官方更新","approved_fetch_plan":` + plan + `,` + tc.field + `}`)
			_, err := coordinator.Propose(t.Context(), CreationProposalInput{
				ActionID: "action-null-envelope", UserID: 11, RawArgs: raw,
				ExpiresAt: time.Now().Add(time.Hour),
			})
			if !errors.Is(err, types.ErrValidation) ||
				!strings.Contains(err.Error(), "must be a string") ||
				store.membershipCalls != 0 || store.tenantCalls != 0 ||
				store.resolveCalls != 0 || store.createCalls != 0 {
				t.Fatalf("null optional envelope field must fail before lookup: err=%v store=%+v",
					err, store)
			}
		})
	}
}

func TestMaterializeCreationSourceSpecs_CoversEveryAdvertisedKind(t *testing.T) {
	const hexID = "6a5578b3000000000e03cc00"
	cases := []struct {
		name       string
		item       string
		wantURL    string
		wantConfig string
	}{
		{
			name:    "web search",
			item:    `{"kind":"web_search","query":"AI","category":"news"}`,
			wantURL: "vane://web/search?q=AI&category=news",
		},
		{
			name:    "web feed",
			item:    `{"kind":"web_feed","feed_url":"https://example.com/feed.xml","categories":["AI"]}`,
			wantURL: "https://example.com/feed.xml#vane-categories=ai",
		},
		{
			name:    "web contents",
			item:    `{"kind":"web_contents","page_url":"https://example.com/changelog"}`,
			wantURL: "vane://web/contents?url=https%3A%2F%2Fexample.com%2Fchangelog",
		},
		{
			name:    "x user posts",
			item:    `{"kind":"x_user_posts","screen_name":"OpenAI"}`,
			wantURL: "vane://x/user_posts?screen_name=OpenAI",
		},
		{
			name:    "xhs search",
			item:    `{"kind":"xhs_search","keyword":"人工智能"}`,
			wantURL: "vane://xhs/search?keyword=%E4%BA%BA%E5%B7%A5%E6%99%BA%E8%83%BD",
		},
		{
			name:    "xhs user posts",
			item:    `{"kind":"xhs_user_posts","user_id":"` + hexID + `"}`,
			wantURL: "vane://xhs/user_posts?user_id=" + hexID,
		},
		{
			name:       "xhs hot list",
			item:       `{"kind":"xhs_hot_list"}`,
			wantURL:    "vane://xhs/hot_list",
			wantConfig: `{}`,
		},
		{
			name:    "xhs topic feed",
			item:    `{"kind":"xhs_topic_feed","page_id":"` + hexID + `"}`,
			wantURL: "vane://xhs/topic_feed?page_id=" + hexID,
		},
		{
			name:    "xhs faved notes",
			item:    `{"kind":"xhs_faved_notes","profile_url":"https://www.xiaohongshu.com/user/profile/` + hexID + `"}`,
			wantURL: "vane://xhs/faved_notes?user_id=" + hexID,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := materializeCreationSourceSpecs(&createScheduleSourceSpecs{
				Version: creationSourceSpecsVersion,
				Items:   []json.RawMessage{json.RawMessage(tc.item)},
			})
			if err != nil || len(got) != 1 || got[0].URL != tc.wantURL {
				t.Fatalf("materialize=%+v err=%v want_url=%q", got, err, tc.wantURL)
			}
			source := &types.Source{
				Platform:   types.Platform(got[0].Platform),
				Capability: types.Capability(got[0].Capability),
				Title:      got[0].Title, URL: got[0].URL, Config: got[0].Config,
			}
			if message := sourcespec.ValidateMaterialized(source); message != "" {
				t.Fatalf("materialized source failed registry round-trip: source=%+v message=%q",
					source, message)
			}
			if tc.wantConfig != "" && string(got[0].Config) != tc.wantConfig {
				t.Fatalf("config=%s want=%s", got[0].Config, tc.wantConfig)
			}
		})
	}
}

func TestMaterializeCreationSourceSpecs_ValidatesXHSUserIDs(t *testing.T) {
	const userID = "6a5578b3000000000e03cc00"
	profileURL := "https://www.xiaohongshu.com/user/profile/" + userID
	for _, kind := range []string{"xhs_user_posts", "xhs_faved_notes"} {
		t.Run(kind+"/valid_direct_and_profile_url_match", func(t *testing.T) {
			direct, err := materializeCreationSourceSpecs(&createScheduleSourceSpecs{
				Version: creationSourceSpecsVersion,
				Items: []json.RawMessage{json.RawMessage(
					`{"kind":"` + kind + `","user_id":"` + userID + `"}`,
				)},
			})
			if err != nil || len(direct) != 1 {
				t.Fatalf("direct user_id materialization=%+v err=%v", direct, err)
			}
			profile, err := materializeCreationSourceSpecs(&createScheduleSourceSpecs{
				Version: creationSourceSpecsVersion,
				Items: []json.RawMessage{json.RawMessage(
					`{"kind":"` + kind + `","profile_url":"` + profileURL + `"}`,
				)},
			})
			if err != nil || len(profile) != 1 ||
				direct[0].URL != profile[0].URL ||
				!bytes.Equal(direct[0].Config, profile[0].Config) {
				t.Fatalf("profile_url must normalize identically: direct=%+v profile=%+v err=%v",
					direct, profile, err)
			}
		})

		invalid := []string{
			"abc",
			"6a5578b3000000000e03cc0g",
			userID + "0",
			" " + userID,
			strings.ToUpper(userID),
			profileURL,
		}
		for _, value := range invalid {
			t.Run(kind+"/reject/"+value, func(t *testing.T) {
				_, err := materializeCreationSourceSpecs(&createScheduleSourceSpecs{
					Version: creationSourceSpecsVersion,
					Items: []json.RawMessage{mustMarshal(t, map[string]any{
						"kind": kind, "user_id": value,
					})},
				})
				if err == nil || !strings.Contains(err.Error(), "24 lowercase hexadecimal") {
					t.Fatalf("invalid direct user_id must be rejected: value=%q err=%v", value, err)
				}
			})
		}
	}
}

func TestMaterializeCreationSourceSpecs_ValidatesXHSTopicPageID(t *testing.T) {
	const pageID = "6301c499df9bea0001dc6f47"
	topicURL := "xhsdiscover://rn/sns-discover/topic/normal?id=" + pageID + "&page_source=note_feed"
	materialize := func(field, value string) ([]compiledFetchSource, error) {
		return materializeCreationSourceSpecs(&createScheduleSourceSpecs{
			Version: creationSourceSpecsVersion,
			Items: []json.RawMessage{mustMarshal(t, map[string]any{
				"kind": "xhs_topic_feed", field: value,
			})},
		})
	}
	direct, err := materialize("page_id", pageID)
	if err != nil || len(direct) != 1 {
		t.Fatalf("direct page_id materialization=%+v err=%v", direct, err)
	}
	deepLink, err := materialize("topic_url", topicURL)
	if err != nil || len(deepLink) != 1 ||
		direct[0].URL != deepLink[0].URL ||
		!bytes.Equal(direct[0].Config, deepLink[0].Config) {
		t.Fatalf("topic_url must normalize identically: direct=%+v deep_link=%+v err=%v",
			direct, deepLink, err)
	}
	for _, value := range []string{
		"abc",
		pageID + "0",
		" " + pageID,
		strings.ToUpper(pageID),
		"https://www.xiaohongshu.com/page/topics/" + pageID,
	} {
		t.Run(value, func(t *testing.T) {
			_, err := materialize("page_id", value)
			if err == nil || !strings.Contains(err.Error(), "24 lowercase hexadecimal") {
				t.Fatalf("invalid direct page_id must be rejected: value=%q err=%v", value, err)
			}
		})
	}
}

func TestCreationCoordinator_ProposalCombinesExistingSourcesAndSourceSpecs(t *testing.T) {
	store := newCreationSagaFakeStore()
	store.resolveSources = map[int64]types.Source{
		12: {
			ID: 12, Platform: types.PlatformWeb, Capability: types.CapSearch,
			Title: "现有源", URL: "vane://web/search?q=existing",
			Config: json.RawMessage(`{"query":"existing"}`), Status: types.SourceStatusActive,
		},
	}
	coordinator := NewCreationCoordinator(store, &creationSagaFakeScheduler{}, nil)
	plan := json.RawMessage(`{
		"existing_source_ids":[12],
		"source_specs":{
			"version":"vane.source-specs/v1",
			"items":[{"kind":"web_feed","feed_url":"https://example.com/feed.xml"}]
		}
	}`)
	_, err := coordinator.Propose(t.Context(), CreationProposalInput{
		ActionID: "action-existing-and-spec", UserID: 11,
		RawArgs:   mustCreateArgsWithPlan(t, "监控已有源和新 RSS", "混合来源", plan),
		ExpiresAt: time.Now().Add(time.Hour),
	})
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	if store.resolveCalls != 1 || store.createCalls != 1 ||
		bytes.Contains(store.op.Args, []byte("source_specs")) ||
		bytes.Contains(store.op.Args, []byte("existing_source_ids")) {
		t.Fatalf("引用与 source_specs 应一次冻结为 durable sources: resolve=%d create=%d args=%s",
			store.resolveCalls, store.createCalls, store.op.Args)
	}
	command, _, err := normalizeCreateScheduleCommand(store.op.Args)
	if err != nil {
		t.Fatalf("normalize durable args: %v", err)
	}
	var frozen compiledFetchPlan
	if err := decodeStrictJSON(command.ApprovedFetchPlan, &frozen); err != nil {
		t.Fatalf("decode frozen plan: %v", err)
	}
	if len(frozen.Sources) != 2 || frozen.Sources[0].URL != "vane://web/search?q=existing" ||
		frozen.Sources[1].URL != "https://example.com/feed.xml" ||
		string(frozen.Sources[1].Config) != `{}` {
		t.Fatalf("frozen plan 顺序或 feed 空 config 漂移: %+v", frozen.Sources)
	}
}

func TestCreationCoordinator_LegacyValidationStillPrecedesTenantLookup(t *testing.T) {
	store := newCreationSagaFakeStore()
	store.tenant.Status = types.TenantStatusSuspended
	coordinator := NewCreationCoordinator(store, &creationSagaFakeScheduler{}, nil)
	raw := mustMarshal(t, map[string]any{
		"spec":                map[string]any{"every_seconds": 1, "tz": "UTC"},
		"intent":              "监控 AI",
		"nl_description":      "无效频率",
		"strictness":          "normal",
		"approved_fetch_plan": json.RawMessage(validApprovedFetchPlan()),
	})

	_, err := coordinator.Propose(t.Context(), CreationProposalInput{
		ActionID: "action-invalid-legacy", UserID: 11, RawArgs: raw,
		ExpiresAt: time.Now().Add(time.Hour),
	})
	if err == nil || !strings.Contains(err.Error(), "invalid schedule spec") ||
		strings.Contains(err.Error(), "工作空间") || store.createCalls != 0 {
		t.Fatalf("legacy validation order changed: err=%v create_calls=%d", err, store.createCalls)
	}

	// The new proposal decoder must not reorder two deterministic legacy
	// failures: the original normalizer checks intent before an empty plan.
	raw = mustMarshal(t, map[string]any{
		"spec":                map[string]any{"every_seconds": 3600, "tz": "UTC"},
		"intent":              "   ",
		"nl_description":      "无效旧参数",
		"strictness":          "normal",
		"approved_fetch_plan": map[string]any{"sources": []any{}},
	})
	_, err = coordinator.Propose(t.Context(), CreationProposalInput{
		ActionID: "action-invalid-legacy-precedence", UserID: 11, RawArgs: raw,
		ExpiresAt: time.Now().Add(time.Hour),
	})
	if err == nil || !strings.Contains(err.Error(), "approved intent") ||
		strings.Contains(err.Error(), "at least one source") {
		t.Fatalf("legacy error precedence changed: %v", err)
	}

	longSources := make([]map[string]any, 6)
	for i := range longSources {
		query := fmt.Sprintf("legacy-long-%d", i)
		longSources[i] = map[string]any{
			"platform": "web", "capability": "search",
			"title":  strings.Repeat("长", maxCompiledSourceRunes),
			"url":    "vane://web/search?q=" + query,
			"config": map[string]any{"query": query},
		}
	}
	raw = mustCreateArgsWithPlan(t, "监控旧任务", "超长摘要", mustMarshal(t, map[string]any{
		"sources": longSources,
	}))
	_, err = coordinator.Propose(t.Context(), CreationProposalInput{
		ActionID: "action-invalid-legacy-summary", UserID: 11, RawArgs: raw,
		ExpiresAt: time.Now().Add(time.Hour),
	})
	if err == nil || !strings.Contains(err.Error(), "confirmation summary exceeds") ||
		strings.Contains(err.Error(), "工作空间") || store.createCalls != 0 {
		t.Fatalf("legacy summary validation order changed: err=%v create_calls=%d",
			err, store.createCalls)
	}
}

func TestCreationCoordinator_ProposalFreezesExistingSourceIdentitiesByValue(t *testing.T) {
	store := newCreationSagaFakeStore()
	store.resolveSources = map[int64]types.Source{
		22: {
			ID: 22, Platform: types.PlatformWeb, Capability: types.CapSearch,
			Title: " \n现有 \u202e**B**\u2066\t ", URL: "vane://web/search?q=existing-b",
			Config: json.RawMessage(
				`{"query":"existing-b","lookback_days":-1,"num_results":15,"type":"auto"}`,
			),
			Status: types.SourceStatusActive,
		},
		11: {
			ID: 11, Platform: types.PlatformWeb, Capability: types.CapSearch,
			Title: "", URL: "vane://web/search?q=existing-a",
			Config: json.RawMessage(`{"query":"existing-a"}`), Status: types.SourceStatusActive,
		},
		33: {
			ID: 33, Platform: types.PlatformXHS, Capability: types.CapSearch,
			Title: "现有小红书", URL: "vane://xhs/search?keyword=AI",
			Config: json.RawMessage(
				`{"keyword":"AI","sort_type":"time_descending","note_type":"video"}`,
			),
			Status: types.SourceStatusActive,
		},
		44: {
			ID: 44, Platform: types.PlatformWeb, Capability: types.CapFeed,
			Title: "现有 RSS", URL: "https://example.com/feed#vane-categories=ai,news",
			Config: json.RawMessage(
				`{"categories":["ai","news"],"lookback_days":-1}`,
			),
			Status: types.SourceStatusActive,
		},
	}
	schedules := &creationSagaFakeScheduler{}
	coordinator := NewCreationCoordinator(store, schedules, nil)
	plan := json.RawMessage(`{
		"existing_source_ids":[22,11,33,44],
		"sources":[{
			"platform":"web","capability":"search","title":"新增 C",
			"url":"vane://web/search?q=new-c","config":{"query":"new-c"}
		}]
	}`)
	proposal, err := coordinator.Propose(t.Context(), CreationProposalInput{
		ActionID: "action-existing-sources", UserID: 11,
		RawArgs:   mustCreateArgsWithPlan(t, "监控三个主题", "三源任务", plan),
		ExpiresAt: time.Now().Add(time.Hour),
	})
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	if store.resolveCalls != 1 || store.resolveTenantID != 7 || store.resolveUserID != 11 ||
		!slices.Equal(store.resolveSourceIDs, []int64{22, 11, 33, 44}) {
		t.Fatalf("resolver scope/calls mismatch: calls=%d tenant=%d user=%d ids=%v",
			store.resolveCalls, store.resolveTenantID, store.resolveUserID, store.resolveSourceIDs)
	}
	if store.createCalls != 1 || bytes.Contains(store.op.Args, []byte("existing_source_ids")) {
		t.Fatalf("durable args must contain only frozen sources: create=%d args=%s",
			store.createCalls, store.op.Args)
	}
	for _, forbidden := range [][]byte{
		[]byte("lookback_days"), []byte("num_results"), []byte(`"type"`),
		[]byte("sort_type"), []byte("note_type"),
	} {
		if bytes.Contains(store.op.Args, forbidden) {
			t.Fatalf("runtime tuning leaked into task identity plan: key=%s args=%s",
				forbidden, store.op.Args)
		}
	}
	if !bytes.Contains(store.op.Args, []byte(`"categories":["ai","news"]`)) {
		t.Fatalf("feed identity config was not preserved: %s", store.op.Args)
	}
	command, _, err := normalizeCreateScheduleCommand(store.op.Args)
	if err != nil {
		t.Fatalf("normalize durable args: %v", err)
	}
	var frozen compiledFetchPlan
	if err := decodeStrictJSON(command.ApprovedFetchPlan, &frozen); err != nil {
		t.Fatalf("decode frozen plan: %v", err)
	}
	gotURLs := make([]string, 0, len(frozen.Sources))
	for _, source := range frozen.Sources {
		gotURLs = append(gotURLs, source.URL)
	}
	wantURLs := []string{
		"vane://web/search?q=existing-b",
		"vane://web/search?q=existing-a",
		"vane://xhs/search?keyword=AI",
		"https://example.com/feed#vane-categories=ai,news",
		"vane://web/search?q=new-c",
	}
	if !slices.Equal(gotURLs, wantURLs) {
		t.Fatalf("frozen source order=%v want=%v", gotURLs, wantURLs)
	}
	for _, want := range []string{
		"信息范围（5）", "现有 **B**", "web/search [web/search]", "现有小红书", "现有 RSS", "新增 C",
	} {
		if !strings.Contains(proposal.Summary, want) {
			t.Fatalf("summary missing %q: %s", want, proposal.Summary)
		}
	}
	for _, bidi := range []string{"\u202e", "\u2066"} {
		if strings.Contains(proposal.Summary, bidi) || bytes.Contains(store.op.Args, []byte(bidi)) {
			t.Fatalf("bidi control leaked into confirmation snapshot: %q", bidi)
		}
	}

	// Losing the live entitlement after the confirmation card was displayed
	// must not change the approved bytes or make recovery consult the source
	// resolver again. The proposal snapshot is the sole input from here on.
	store.resolveSources = nil
	result, err := coordinator.Confirm(
		t.Context(), 11, "action-existing-sources", testCreationReceiptTarget,
	)
	if err != nil || result.Status != types.PendingActionStatusExecuted ||
		store.resolveCalls != 1 || !bytes.Equal(store.definition.FetchPlan, command.ApprovedFetchPlan) {
		t.Fatalf("confirm after entitlement loss: result=%+v err=%v resolve_calls=%d final_plan=%s",
			result, err, store.resolveCalls, store.definition.FetchPlan)
	}
}

func TestCreationCoordinator_ProposalRejectsInvalidExistingSourceTuning(t *testing.T) {
	cases := []struct {
		name       string
		platform   types.Platform
		capability types.Capability
		url        string
		config     json.RawMessage
	}{
		{
			name: "integer tuning has string value", platform: types.PlatformWeb,
			capability: types.CapSearch, url: "vane://web/search?q=broken-int",
			config: json.RawMessage(`{"query":"broken-int","num_results":"many"}`),
		},
		{
			name: "web search type is not string", platform: types.PlatformWeb,
			capability: types.CapSearch, url: "vane://web/search?q=broken-type",
			config: json.RawMessage(`{"query":"broken-type","type":7}`),
		},
		{
			name: "xhs sort is not string", platform: types.PlatformXHS,
			capability: types.CapSearch, url: "vane://xhs/search?keyword=broken",
			config: json.RawMessage(`{"keyword":"broken","sort_type":{}}`),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := newCreationSagaFakeStore()
			store.resolveSources = map[int64]types.Source{7: {
				ID: 7, Platform: tc.platform, Capability: tc.capability,
				URL: tc.url, Config: tc.config, Status: types.SourceStatusActive,
			}}
			coordinator := NewCreationCoordinator(store, &creationSagaFakeScheduler{}, nil)
			_, err := coordinator.Propose(t.Context(), CreationProposalInput{
				ActionID: "action-broken-existing-source", UserID: 11,
				RawArgs: mustCreateArgsWithPlan(t, "监控已有源", "已有源", json.RawMessage(
					`{"existing_source_ids":[7]}`,
				)),
				ExpiresAt: time.Now().Add(time.Hour),
			})
			var appErr *types.AppError
			if !errors.As(err, &appErr) || appErr.Code != types.CodeValidation ||
				appErr.Message != "所选已有信源当前无法用于任务，请重新选择" || store.createCalls != 0 {
				t.Fatalf("err=%v create_calls=%d", err, store.createCalls)
			}
		})
	}
}

func TestCreationCoordinator_ProposalReferenceFailuresCreateNothing(t *testing.T) {
	t.Run("unavailable reference is generic", func(t *testing.T) {
		store := newCreationSagaFakeStore()
		store.resolveErr = types.NewAppError(
			types.CodeNotFound, "一个或多个已有信源当前不可用", nil,
		)
		coordinator := NewCreationCoordinator(store, &creationSagaFakeScheduler{}, nil)
		_, err := coordinator.Propose(t.Context(), CreationProposalInput{
			ActionID: "action-missing-source", UserID: 11,
			RawArgs: mustCreateArgsWithPlan(t, "监控已有源", "已有源", json.RawMessage(
				`{"existing_source_ids":[999999]}`,
			)),
			ExpiresAt: time.Now().Add(time.Hour),
		})
		var appErr *types.AppError
		if !errors.As(err, &appErr) || appErr.Code != types.CodeValidation ||
			appErr.Message != "所选已有信源当前不可用，请重新选择" || store.createCalls != 0 {
			t.Fatalf("err=%v create_calls=%d", err, store.createCalls)
		}
	})

	t.Run("duplicate URL across reference and new source", func(t *testing.T) {
		store := newCreationSagaFakeStore()
		store.resolveSources = map[int64]types.Source{1: {
			ID: 1, Platform: types.PlatformWeb, Capability: types.CapSearch,
			URL: "vane://web/search?q=same", Config: json.RawMessage(`{"query":"same"}`),
			Status: types.SourceStatusActive,
		}}
		coordinator := NewCreationCoordinator(store, &creationSagaFakeScheduler{}, nil)
		plan := json.RawMessage(`{
			"existing_source_ids":[1],
			"sources":[{"platform":"web","capability":"search",
				"url":"vane://web/search?q=same","config":{"query":"same"}}]
		}`)
		_, err := coordinator.Propose(t.Context(), CreationProposalInput{
			ActionID: "action-duplicate-source", UserID: 11,
			RawArgs:   mustCreateArgsWithPlan(t, "监控重复源", "重复源", plan),
			ExpiresAt: time.Now().Add(time.Hour),
		})
		if !errors.Is(err, types.ErrValidation) || store.createCalls != 0 {
			t.Fatalf("err=%v create_calls=%d", err, store.createCalls)
		}
	})
}

func TestDecodeCreationProposalArgs_SourceBoundaries(t *testing.T) {
	ids := func(n int) []int64 {
		out := make([]int64, n)
		for i := range out {
			out[i] = int64(i + 1)
		}
		return out
	}
	newSource := map[string]any{
		"platform": "web", "capability": "search",
		"url": "vane://web/search?q=new", "config": map[string]any{"query": "new"},
	}
	cases := []struct {
		name    string
		plan    any
		wantErr bool
	}{
		{name: "one reference", plan: map[string]any{"existing_source_ids": []int64{1}}},
		{name: "one new source", plan: map[string]any{"sources": []any{newSource}}},
		{name: "mixed maximum", plan: map[string]any{
			"existing_source_ids": ids(maxCompiledSources - 1), "sources": []any{newSource},
		}},
		{name: "empty", plan: map[string]any{}, wantErr: true},
		{name: "zero id", plan: map[string]any{"existing_source_ids": []int64{0}}, wantErr: true},
		{name: "duplicate id", plan: map[string]any{"existing_source_ids": []int64{7, 7}}, wantErr: true},
		{name: "combined over maximum", plan: map[string]any{
			"existing_source_ids": ids(maxCompiledSources), "sources": []any{newSource},
		}, wantErr: true},
		{name: "unknown plan field", plan: map[string]any{
			"existing_source_ids": []int64{1}, "source_ids": []int64{1},
		}, wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			raw := mustCreateArgsWithPlan(t, "监控", "任务", mustMarshal(t, tc.plan))
			_, err := decodeCreationProposalArgs(raw)
			if (err != nil) != tc.wantErr {
				t.Fatalf("decodeCreationProposalArgs() err=%v want_err=%v raw=%s",
					err, tc.wantErr, raw)
			}
		})
	}
}

func TestCreationCoordinator_RejectsSSRFBeforePendingAction(t *testing.T) {
	for _, rawURL := range []string{
		"http://127.0.0.1/rss",
		"http://127.1/rss",
		"http://2130706433/rss",
		"http://017700000001/rss",
		"http://0x7f000001/rss",
		"http://[::1%25lo0]/rss",
		"http://[fe80::1%25en0]/rss",
		"http://[fe80::1%25en0]:8080/rss",
		"http://[fe80::1%25%65%6e%30]/rss",
	} {
		t.Run(rawURL, func(t *testing.T) {
			store := newCreationSagaFakeStore()
			coordinator := NewCreationCoordinator(store, &creationSagaFakeScheduler{}, nil)
			raw := mustCreateArgsWithPlan(t, "监控本地服务", "本地", mustMarshal(t, map[string]any{
				"sources": []map[string]any{{
					"platform": "web", "capability": "feed",
					"url": rawURL, "config": map[string]any{},
				}},
			}))
			_, err := coordinator.Propose(t.Context(), CreationProposalInput{
				ActionID: "action-ssrf", UserID: 11, RawArgs: raw,
				ExpiresAt: time.Now().Add(time.Hour),
			})
			if !errors.Is(err, types.ErrValidation) || store.createCalls != 0 ||
				store.membershipCalls != 0 || store.tenantCalls != 0 {
				t.Fatalf("err=%v create=%d membership=%d tenant=%d",
					err, store.createCalls, store.membershipCalls, store.tenantCalls)
			}
		})
	}
}

func TestCreationCoordinator_ConfirmHappyPathAndTerminalReplay(t *testing.T) {
	store := newCreationSagaFakeStore()
	schedules := &creationSagaFakeScheduler{}
	coordinator := NewCreationCoordinator(store, schedules, nil)
	mustProposeCreation(t, coordinator, "action-happy")

	result, err := coordinator.Confirm(t.Context(), 11, "action-happy", testCreationReceiptTarget)
	if err != nil {
		t.Fatalf("Confirm: %v", err)
	}
	if result.Status != types.PendingActionStatusExecuted || result.Recovering || !result.ReceiptBound ||
		store.op.Phase != types.TaskCreationPhaseCompleted || schedules.activateCalls != 1 {
		t.Fatalf("result=%+v phase=%q activate=%d", result, store.op.Phase, schedules.activateCalls)
	}
	if !bytes.Equal(store.definition.FetchPlan, json.RawMessage(
		`{"sources":[{"platform":"web","capability":"search","title":"A","url":"vane://web/search?q=AI\u0026category=news","config":{"category":"news","query":"AI"}}]}`,
	)) {
		t.Fatalf("final plan=%s", store.definition.FetchPlan)
	}
	events := append([]string(nil), store.events...)
	replayed, err := coordinator.Confirm(t.Context(), 11, "action-happy", testCreationReceiptTarget)
	if err != nil || replayed.TaskID != result.TaskID || !bytes.Equal(mustMarshal(t, events), mustMarshal(t, store.events)) {
		t.Fatalf("terminal replay=%+v err=%v before=%v after=%v", replayed, err, events, store.events)
	}
	differentTarget := testCreationReceiptTarget
	differentTarget.Target = "om_forwarded_copy"
	if _, err := coordinator.Confirm(t.Context(), 11, "action-happy", differentTarget); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("terminal replay on a different provider resource must conflict: %v", err)
	}
}

func TestCreationCoordinator_BusyLegacyOperationBindsDurableReceipt(t *testing.T) {
	store := newCreationSagaFakeStore()
	schedules := &creationSagaFakeScheduler{}
	coordinator := NewCreationCoordinator(store, schedules, nil)
	mustProposeCreation(t, coordinator, "action-busy-legacy")

	store.op.Status = types.PendingActionStatusExecuting
	store.op.Phase = types.TaskCreationPhaseClaimed
	store.op.LeaseOwner = "pre-a6-worker"
	store.op.Fence = 1
	store.op.Attempt = 1
	leaseUntil := time.Now().Add(time.Minute)
	store.op.LeaseUntil = &leaseUntil

	result, err := coordinator.Confirm(
		t.Context(), 11, "action-busy-legacy", testCreationReceiptTarget,
	)
	if err != nil || !result.Recovering || !result.ReceiptBound ||
		result.Status != types.PendingActionStatusExecuting {
		t.Fatalf("busy result=%+v err=%v", result, err)
	}
	if store.op.ReceiptProvider != testCreationReceiptTarget.Provider ||
		store.op.ReceiptTarget != testCreationReceiptTarget.Target ||
		len(schedules.events) != 0 {
		t.Fatalf("op=%+v scheduler_events=%v", store.op, schedules.events)
	}
}

func TestCreationCoordinator_ActivationResponseLossAdoptsWithoutSecondWrite(t *testing.T) {
	store := newCreationSagaFakeStore()
	schedules := &creationSagaFakeScheduler{
		activateErr: scheduler.ErrTaskScheduleTransient, activateApplyBeforeError: true,
	}
	coordinator := NewCreationCoordinator(store, schedules, nil)
	mustProposeCreation(t, coordinator, "action-recover")

	first, err := coordinator.Confirm(t.Context(), 11, "action-recover", testCreationReceiptTarget)
	if err != nil || !first.Recovering || store.op.Phase != types.TaskCreationPhaseActivationStarted {
		t.Fatalf("first=%+v phase=%q err=%v", first, store.op.Phase, err)
	}
	store.allowTakeover = true
	second, err := coordinator.Confirm(t.Context(), 11, "action-recover", testCreationReceiptTarget)
	if err != nil || second.Status != types.PendingActionStatusExecuted || schedules.activateCalls != 1 {
		t.Fatalf("second=%+v activate_calls=%d err=%v", second, schedules.activateCalls, err)
	}
	if !containsString(schedules.events, "describe") {
		t.Fatalf("recovery did not Describe before adopt: %v", schedules.events)
	}
}

func TestCreationCoordinator_TaskLimitDeletesPausedTaskAndFails(t *testing.T) {
	store := newCreationSagaFakeStore()
	store.definitionLimit = true
	schedules := &creationSagaFakeScheduler{}
	coordinator := NewCreationCoordinator(store, schedules, nil)
	mustProposeCreation(t, coordinator, "action-limit")

	result, err := coordinator.Confirm(t.Context(), 11, "action-limit", testCreationReceiptTarget)
	if err != nil || result.Status != types.PendingActionStatusFailed ||
		schedules.deleteCalls != 1 || schedules.activateCalls != 0 {
		t.Fatalf("result=%+v delete=%d activate=%d err=%v",
			result, schedules.deleteCalls, schedules.activateCalls, err)
	}
}

func TestCreationCoordinator_DeterministicEnsureFailureDoesNotLoop(t *testing.T) {
	store := newCreationSagaFakeStore()
	schedules := &creationSagaFakeScheduler{ensureErr: scheduler.ErrTaskScheduleConflict}
	coordinator := NewCreationCoordinator(store, schedules, nil)
	mustProposeCreation(t, coordinator, "action-ensure-conflict")

	result, err := coordinator.Confirm(t.Context(), 11, "action-ensure-conflict", testCreationReceiptTarget)
	if err != nil || result.Status != types.PendingActionStatusFailed ||
		schedules.deleteCalls != 1 || containsString(schedules.events, "activate") {
		t.Fatalf("result=%+v events=%v delete=%d err=%v",
			result, schedules.events, schedules.deleteCalls, err)
	}
}

func TestCreationCoordinator_CleanupFinalizationConflictQuarantines(t *testing.T) {
	store := newCreationSagaFakeStore()
	store.definitionLimit = true
	store.finishCleanupErr = types.ErrConflict
	schedules := &creationSagaFakeScheduler{}
	coordinator := NewCreationCoordinator(store, schedules, nil)
	mustProposeCreation(t, coordinator, "action-cleanup-finalize-conflict")

	result, err := coordinator.Confirm(t.Context(), 11, store.op.ID, testCreationReceiptTarget)
	if err != nil || result.Status != types.PendingActionStatusBlocked ||
		store.op.Status != types.PendingActionStatusBlocked ||
		store.op.ErrorCode != "cleanup_finalization_invalid" || schedules.deleteCalls != 1 {
		t.Fatalf("result=%+v op=%+v delete=%d err=%v",
			result, store.op, schedules.deleteCalls, err)
	}
}

func TestCreationCoordinator_CleanupCheckpointConflictQuarantinesWithoutDelete(t *testing.T) {
	store := newCreationSagaFakeStore()
	store.definitionLimit = true
	store.beginCleanupErr = types.ErrConflict
	schedules := &creationSagaFakeScheduler{}
	coordinator := NewCreationCoordinator(store, schedules, nil)
	mustProposeCreation(t, coordinator, "action-cleanup-checkpoint-conflict")

	result, err := coordinator.Confirm(t.Context(), 11, store.op.ID, testCreationReceiptTarget)
	if err != nil || result.Status != types.PendingActionStatusBlocked ||
		store.op.Status != types.PendingActionStatusBlocked ||
		store.op.ErrorCode != "cleanup_checkpoint_invalid" || schedules.deleteCalls != 0 {
		t.Fatalf("result=%+v op=%+v delete=%d err=%v",
			result, store.op, schedules.deleteCalls, err)
	}
}

func TestCreationCoordinator_DeleteBlockedQuarantinesCleanup(t *testing.T) {
	store := newCreationSagaFakeStore()
	store.definitionLimit = true
	schedules := &creationSagaFakeScheduler{deleteErr: scheduler.ErrTaskScheduleBlocked}
	coordinator := NewCreationCoordinator(store, schedules, nil)
	mustProposeCreation(t, coordinator, "action-delete-blocked")

	result, err := coordinator.Confirm(t.Context(), 11, store.op.ID, testCreationReceiptTarget)
	if err != nil || result.Status != types.PendingActionStatusBlocked ||
		store.op.Status != types.PendingActionStatusBlocked || schedules.deleteCalls != 1 {
		t.Fatalf("result=%+v op=%+v delete=%d err=%v",
			result, store.op, schedules.deleteCalls, err)
	}
}

func TestCreationCoordinator_ActivationCommitValidationDeletesActiveTask(t *testing.T) {
	store := newCreationSagaFakeStore()
	store.activationCommitErr = types.ErrValidation
	schedules := &creationSagaFakeScheduler{}
	coordinator := NewCreationCoordinator(store, schedules, nil)
	mustProposeCreation(t, coordinator, "action-activation-commit")

	result, err := coordinator.Confirm(t.Context(), 11, "action-activation-commit", testCreationReceiptTarget)
	if err != nil || result.Status != types.PendingActionStatusFailed ||
		schedules.activateCalls != 1 || schedules.deleteCalls != 1 ||
		schedules.state != scheduler.TaskScheduleStateUnknown {
		t.Fatalf("result=%+v state=%q activate=%d delete=%d err=%v",
			result, schedules.state, schedules.activateCalls, schedules.deleteCalls, err)
	}
}

func TestCreationCoordinator_CompletionConflictQuarantinesActivatedTask(t *testing.T) {
	store := newCreationSagaFakeStore()
	store.completeErr = types.ErrConflict
	schedules := &creationSagaFakeScheduler{}
	coordinator := NewCreationCoordinator(store, schedules, nil)
	mustProposeCreation(t, coordinator, "action-complete-conflict")

	result, err := coordinator.Confirm(t.Context(), 11, "action-complete-conflict", testCreationReceiptTarget)
	if err != nil || result.Status != types.PendingActionStatusBlocked ||
		schedules.activateCalls != 1 || schedules.deleteCalls != 0 ||
		store.op.Phase != types.TaskCreationPhaseBlocked {
		t.Fatalf("result=%+v activate=%d delete=%d err=%v",
			result, schedules.activateCalls, schedules.deleteCalls, err)
	}
}

func TestCreationCoordinator_CancelV1IsScopedAndLinearized(t *testing.T) {
	store := newCreationSagaFakeStore()
	coordinator := NewCreationCoordinator(store, &creationSagaFakeScheduler{}, nil)
	mustProposeCreation(t, coordinator, "action-cancel")

	result, err := coordinator.Cancel(t.Context(), 11, "action-cancel", testCreationReceiptTarget)
	if err != nil || result.Status != types.PendingActionStatusCancelled ||
		store.op.Status != types.PendingActionStatusCancelled ||
		store.op.Phase != types.TaskCreationPhaseCancelled {
		t.Fatalf("cancel result=%+v op=%+v err=%v", result, store.op, err)
	}
	replayed, err := coordinator.Cancel(t.Context(), 11, "action-cancel", testCreationReceiptTarget)
	if err != nil || replayed.Status != types.PendingActionStatusCancelled || !replayed.Replayed {
		t.Fatalf("cancel replay=%+v err=%v", replayed, err)
	}
	if _, err := coordinator.Cancel(t.Context(), 12, "action-cancel", testCreationReceiptTarget); !errors.Is(err, ErrCreationOperationNotFound) {
		t.Fatalf("foreign user must get exact v1-not-found sentinel, got %v", err)
	}
}

func TestCreationCoordinator_CancelRacingAcquireDoesNotUndoCreation(t *testing.T) {
	store := newCreationSagaFakeStore()
	coordinator := NewCreationCoordinator(store, &creationSagaFakeScheduler{}, nil)
	mustProposeCreation(t, coordinator, "action-cancel-busy")
	if _, err := store.AcquireTaskCreationOperation(t.Context(), types.AcquireTaskCreationOperationParams{
		ID: store.op.ID, TenantID: store.op.TenantID, UserID: store.op.UserID,
		LeaseOwner: "worker", LeaseDuration: time.Minute,
		ReceiptProvider: testCreationReceiptTarget.Provider,
		ReceiptTarget:   testCreationReceiptTarget.Target,
	}); err != nil {
		t.Fatal(err)
	}
	result, err := coordinator.Cancel(t.Context(), 11, "action-cancel-busy", testCreationReceiptTarget)
	if err != nil || !result.Recovering || result.Status != types.PendingActionStatusExecuting ||
		store.op.Status != types.PendingActionStatusExecuting {
		t.Fatalf("busy cancel result=%+v op=%+v err=%v", result, store.op, err)
	}
}

func TestCreationCoordinator_CancelBusyLegacyOperationBindsDurableReceipt(t *testing.T) {
	store := newCreationSagaFakeStore()
	coordinator := NewCreationCoordinator(store, &creationSagaFakeScheduler{}, nil)
	mustProposeCreation(t, coordinator, "action-cancel-busy-legacy")
	store.op.Status = types.PendingActionStatusExecuting
	store.op.Phase = types.TaskCreationPhaseClaimed
	store.op.LeaseOwner = "pre-a6-worker"
	store.op.Fence = 1
	store.op.Attempt = 1
	leaseUntil := time.Now().Add(time.Minute)
	store.op.LeaseUntil = &leaseUntil

	result, err := coordinator.Cancel(
		t.Context(), 11, "action-cancel-busy-legacy", testCreationReceiptTarget,
	)
	if err != nil || !result.Recovering || !result.ReceiptBound ||
		result.Status != types.PendingActionStatusExecuting {
		t.Fatalf("busy cancel result=%+v err=%v", result, err)
	}
	if store.op.ReceiptProvider != testCreationReceiptTarget.Provider ||
		store.op.ReceiptTarget != testCreationReceiptTarget.Target {
		t.Fatalf("busy cancel did not retain receipt target: %+v", store.op)
	}
}

func TestCreationCoordinator_AdoptsPreparationTerminalAfterCancelledResponseLoss(t *testing.T) {
	tests := []struct {
		name        string
		scheduleErr error
		wantStatus  types.PendingActionStatus
		configure   func(*creationSagaFakeStore)
	}{
		{
			name:        "fail",
			scheduleErr: scheduler.ErrTaskScheduleInvalid,
			wantStatus:  types.PendingActionStatusFailed,
			configure: func(s *creationSagaFakeStore) {
				s.failErr = errors.New("commit applied but response lost")
				s.failApplyBeforeError = true
			},
		},
		{
			name:        "block",
			scheduleErr: scheduler.ErrTaskScheduleBlocked,
			wantStatus:  types.PendingActionStatusBlocked,
			configure: func(s *creationSagaFakeStore) {
				s.blockErr = errors.New("commit applied but response lost")
				s.blockApplyBeforeError = true
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newCreationSagaFakeStore()
			coordinator := NewCreationCoordinator(store,
				&creationSagaFakeScheduler{prepareErr: tt.scheduleErr}, nil)
			mustProposeCreation(t, coordinator, "action-terminal-loss-"+tt.name)
			ctx, cancel := context.WithCancel(t.Context())
			store.terminalWriteCancel = cancel
			store.honorLoadContext = true
			tt.configure(store)

			result, err := coordinator.Confirm(ctx, 11, store.op.ID, testCreationReceiptTarget)
			if err != nil || result.Status != tt.wantStatus || store.op.Status != tt.wantStatus {
				t.Fatalf("result=%+v op=%+v err=%v", result, store.op, err)
			}
		})
	}
}

func TestCreationCoordinator_AdoptsCompletedTerminalAfterCancelledResponseLoss(t *testing.T) {
	store := newCreationSagaFakeStore()
	coordinator := NewCreationCoordinator(store, &creationSagaFakeScheduler{}, nil)
	mustProposeCreation(t, coordinator, "action-complete-loss")
	ctx, cancel := context.WithCancel(t.Context())
	store.terminalWriteCancel = cancel
	store.honorLoadContext = true
	store.completeErr = errors.New("commit applied but response lost")
	store.completeApplyBeforeError = true

	result, err := coordinator.Confirm(ctx, 11, store.op.ID, testCreationReceiptTarget)
	if err != nil || result.Status != types.PendingActionStatusExecuted ||
		store.op.Status != types.PendingActionStatusExecuted {
		t.Fatalf("result=%+v op=%+v err=%v", result, store.op, err)
	}
}

func TestCreationCoordinator_RecoveryAdoptsAcquireResponseLossSamePass(t *testing.T) {
	store := newCreationSagaFakeStore()
	coordinator := NewCreationCoordinator(store, &creationSagaFakeScheduler{}, nil)
	mustProposeCreation(t, coordinator, "action-recovery-acquire-loss")
	stale, err := store.AcquireTaskCreationOperation(t.Context(), types.AcquireTaskCreationOperationParams{
		ID: store.op.ID, TenantID: store.op.TenantID, UserID: store.op.UserID,
		LeaseOwner: "dead-worker", LeaseDuration: time.Minute,
		ReceiptProvider: testCreationReceiptTarget.Provider,
		ReceiptTarget:   testCreationReceiptTarget.Target,
	})
	if err != nil {
		t.Fatal(err)
	}
	store.allowTakeover = true
	store.acquireErr = errors.New("takeover committed but response lost")
	store.acquireApplyBeforeError = true

	if err := coordinator.recoverOperation(t.Context(), *stale); err != nil {
		t.Fatalf("same recovery pass should adopt takeover: %v", err)
	}
	if store.op.Status != types.PendingActionStatusExecuted || store.op.Attempt != 2 || store.op.Fence != 2 {
		t.Fatalf("op=%+v; takeover must increment fence/attempt exactly once", store.op)
	}
}

func TestCreationCoordinator_RecoveryContinuesAfterShardListError(t *testing.T) {
	store := &recoveryListingStore{
		creationSagaFakeStore: newCreationSagaFakeStore(),
		tenantIDs:             []int64{7, 8},
		shardErrs:             map[int64]error{7: errors.New("tenant 7 unavailable")},
	}
	coordinator := NewCreationCoordinator(store, &creationSagaFakeScheduler{}, nil)
	err := coordinator.RecoverStaleOnce(t.Context())
	if err == nil || !slices.Equal(store.queried, []int64{7, 8}) {
		t.Fatalf("later shard must still be scanned: queried=%v err=%v", store.queried, err)
	}
}

func TestCreationCoordinator_RecoveryPassIsGloballyBoundedAndRotates(t *testing.T) {
	tenantIDs := make([]int64, creationRecoveryTenantLimit)
	for i := range tenantIDs {
		tenantIDs[i] = int64(i + 1)
	}
	store := &recoveryListingStore{
		creationSagaFakeStore: newCreationSagaFakeStore(),
		tenantIDs:             tenantIDs,
		fill:                  true,
	}
	coordinator := NewCreationCoordinator(store, &creationSagaFakeScheduler{}, nil)
	if err := coordinator.RecoverStaleOnce(t.Context()); err != nil {
		t.Fatal(err)
	}
	requested := 0
	for _, limit := range store.limits {
		requested += limit
	}
	if requested != creationRecoveryPassLimit || len(store.queried) !=
		creationRecoveryPassLimit/creationRecoveryPerTenant {
		t.Fatalf("pass not bounded: tenants=%v limits=%v total=%d",
			store.queried, store.limits, requested)
	}
	firstPassTenants := len(store.queried)
	store.queried = nil
	store.limits = nil
	if err := coordinator.RecoverStaleOnce(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(store.queried) == 0 || store.queried[0] != int64(firstPassTenants+1) {
		t.Fatalf("tenant cursor did not rotate: first pass=%d second=%v",
			firstPassTenants, store.queried)
	}
}

func TestCreationCoordinator_RecoveryKeysetCannotStarveTenantAfterFailedFirstPage(t *testing.T) {
	tenantIDs := make([]int64, creationRecoveryTenantLimit+1)
	shardErrs := make(map[int64]error, creationRecoveryTenantLimit)
	for i := range tenantIDs {
		tenantIDs[i] = int64(i + 1)
		if i < creationRecoveryTenantLimit {
			shardErrs[tenantIDs[i]] = errors.New("tenant shard unavailable")
		}
	}
	store := &recoveryListingStore{
		creationSagaFakeStore: newCreationSagaFakeStore(),
		tenantIDs:             tenantIDs,
		shardErrs:             shardErrs,
	}
	coordinator := NewCreationCoordinator(store, &creationSagaFakeScheduler{}, nil)
	if err := coordinator.RecoverStaleOnce(t.Context()); err == nil {
		t.Fatal("first failed page should report its shard errors")
	}
	if slices.Contains(store.queried, int64(creationRecoveryTenantLimit+1)) {
		t.Fatalf("tenant 101 must not appear in the first bounded page: %v", store.queried)
	}
	store.queried = nil
	store.limits = nil
	if err := coordinator.RecoverStaleOnce(t.Context()); err == nil {
		t.Fatal("wrapped pass still includes failing shards and should report them")
	}
	if len(store.queried) == 0 || store.queried[0] != creationRecoveryTenantLimit+1 {
		t.Fatalf("global keyset cursor starved tenant 101: cursors=%v queried=%v",
			store.cursors, store.queried)
	}
}

func TestCreationTerminalResultRejectsIncompleteTombstones(t *testing.T) {
	now := time.Now()
	valid := &types.TaskCreationOperation{
		ID: "op", TenantID: 7, UserID: 11,
		Status: types.PendingActionStatusCancelled,
		Phase:  types.TaskCreationPhaseCancelled, TombstonedAt: &now,
	}
	if result, done, err := creationTerminalResult(valid); err != nil || !done ||
		result.Status != types.PendingActionStatusCancelled {
		t.Fatalf("valid cancellation rejected: result=%+v done=%v err=%v", result, done, err)
	}
	corruptions := []struct {
		name string
		edit func(*types.TaskCreationOperation)
	}{
		{name: "wrong phase", edit: func(op *types.TaskCreationOperation) { op.Phase = types.TaskCreationPhaseFailed }},
		{name: "missing tombstone", edit: func(op *types.TaskCreationOperation) { op.TombstonedAt = nil }},
		{name: "live lease", edit: func(op *types.TaskCreationOperation) { op.LeaseUntil = &now }},
		{name: "unexpected fence", edit: func(op *types.TaskCreationOperation) { op.Fence = 1 }},
	}
	for _, tt := range corruptions {
		t.Run(tt.name, func(t *testing.T) {
			op := *valid
			tt.edit(&op)
			if _, done, err := creationTerminalResult(&op); !done ||
				!errors.Is(err, ErrCreationCheckpointInvalid) {
				t.Fatalf("corruption accepted: done=%v err=%v op=%+v", done, err, op)
			}
		})
	}
}

func TestCreationCoordinator_LateCheckpointCorruptionCleansOrQuarantines(t *testing.T) {
	t.Run("valid prepared binding is deleted", func(t *testing.T) {
		store := newCreationSagaFakeStore()
		schedules := &creationSagaFakeScheduler{}
		coordinator := NewCreationCoordinator(store, schedules, nil)
		mustProposeCreation(t, coordinator, "action-corrupt-compiled")
		op, err := store.AcquireTaskCreationOperation(t.Context(), types.AcquireTaskCreationOperationParams{
			ID: store.op.ID, TenantID: store.op.TenantID, UserID: store.op.UserID,
			LeaseOwner: "seed", LeaseDuration: time.Minute,
			ReceiptProvider: testCreationReceiptTarget.Provider,
			ReceiptTarget:   testCreationReceiptTarget.Target,
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := coordinator.preparer.Prepare(t.Context(), CreationPrepareInput{
			TenantID: op.TenantID, UserID: op.UserID, OperationID: op.ID, Lease: op.Lease(),
		}); err != nil {
			t.Fatal(err)
		}
		store.op.CompiledDigest = strings.Repeat("0", 64)
		store.allowTakeover = true
		if err := coordinator.recoverOperation(t.Context(), store.op); err != nil {
			t.Fatal(err)
		}
		if store.op.Status != types.PendingActionStatusFailed || schedules.deleteCalls != 1 {
			t.Fatalf("status=%q delete=%d", store.op.Status, schedules.deleteCalls)
		}
	})

	t.Run("unprovable prepared binding is quarantined", func(t *testing.T) {
		store := newCreationSagaFakeStore()
		schedules := &creationSagaFakeScheduler{}
		coordinator := NewCreationCoordinator(store, schedules, nil)
		mustProposeCreation(t, coordinator, "action-corrupt-prepared")
		op, err := store.AcquireTaskCreationOperation(t.Context(), types.AcquireTaskCreationOperationParams{
			ID: store.op.ID, TenantID: store.op.TenantID, UserID: store.op.UserID,
			LeaseOwner: "seed", LeaseDuration: time.Minute,
			ReceiptProvider: testCreationReceiptTarget.Provider,
			ReceiptTarget:   testCreationReceiptTarget.Target,
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := coordinator.preparer.Prepare(t.Context(), CreationPrepareInput{
			TenantID: op.TenantID, UserID: op.UserID, OperationID: op.ID, Lease: op.Lease(),
		}); err != nil {
			t.Fatal(err)
		}
		store.op.PreparedSchedule = []byte(`{"corrupt":true}`)
		store.allowTakeover = true
		if err := coordinator.recoverOperation(t.Context(), store.op); err != nil {
			t.Fatal(err)
		}
		if store.op.Status != types.PendingActionStatusBlocked || schedules.deleteCalls != 0 ||
			!containsString(store.events, "block_side_effect") {
			t.Fatalf("status=%q store_events=%v delete=%d",
				store.op.Status, store.events, schedules.deleteCalls)
		}
	})
}

func TestCreationCoordinator_ScopeRevokedBeforeEnsureConvergesThroughExactDelete(t *testing.T) {
	store := newCreationSagaFakeStore()
	schedules := &creationSagaFakeScheduler{}
	coordinator := NewCreationCoordinator(store, schedules, nil)
	mustProposeCreation(t, coordinator, "action-revoked")
	store.tenant.Status = types.TenantStatusSuspended

	// Recovery acquires by the operation's durable scope even though normal
	// intake would no longer resolve this tenant as active.
	store.op.Status = types.PendingActionStatusExecuting
	store.op.Phase = types.TaskCreationPhaseClaimed
	store.allowTakeover = true
	err := coordinator.recoverOperation(t.Context(), store.op)
	if err != nil {
		t.Fatalf("recoverOperation: %v", err)
	}
	if store.op.Status != types.PendingActionStatusFailed || schedules.deleteCalls != 1 ||
		containsString(schedules.events, "ensure") || containsString(schedules.events, "activate") {
		t.Fatalf("status=%q scheduler_events=%v delete_calls=%d",
			store.op.Status, schedules.events, schedules.deleteCalls)
	}
}

func mustProposeCreation(t *testing.T, coordinator *CreationCoordinator, id string) {
	t.Helper()
	if _, err := coordinator.Propose(t.Context(), CreationProposalInput{
		ActionID: id, UserID: 11, RawArgs: mustCreateArgs(t, "每天寻找全球 AI 热点", "每天 AI"),
		ExpiresAt: time.Now().Add(time.Hour),
	}); err != nil {
		t.Fatalf("Propose: %v", err)
	}
}

func TestValidateCreationReceiptTarget_AcceptsAgentAutoIdentity(t *testing.T) {
	op := &types.TaskCreationOperation{ID: "operation-auto"}
	if err := validateCreationReceiptTarget(
		op, AgentAutoReceiptTarget(op.ID),
	); err != nil {
		t.Fatalf("agent auto receipt rejected: %v", err)
	}
	if err := validateCreationReceiptTarget(
		op, AgentAutoReceiptTarget("another-operation"),
	); err == nil {
		t.Fatal("agent auto receipt must be bound to the exact operation id")
	}
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
