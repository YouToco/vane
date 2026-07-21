package task

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	enums "go.temporal.io/api/enums/v1"

	"github.com/YouToco/vane/scheduler"
	"github.com/YouToco/vane/types"
	"github.com/YouToco/vane/workflow"
)

type creationPrepareFakeStore struct {
	op types.TaskCreationOperation

	sealCalls       int
	loadCalls       int
	beginCalls      int
	definitionCalls int
	scheduleCalls   int
	blockCalls      int
	blockCode       string
	blockMessage    string
	failCalls       int
	failCode        string
	failMessage     string

	beginErr                   error
	beginApplyBeforeError      bool
	definitionErr              error
	definitionApplyBeforeError bool
	scheduleErr                error
	scheduleApplyBeforeError   bool
	blockErr                   error
}

func (s *creationPrepareFakeStore) LoadTaskCreationOperation(
	_ context.Context,
	id string,
	tenantID, userID int64,
) (*types.TaskCreationOperation, error) {
	s.loadCalls++
	if s.op.ID != id || s.op.TenantID != tenantID || s.op.UserID != userID {
		return nil, types.ErrNotFound
	}
	clone := s.op
	clone.Args = bytes.Clone(s.op.Args)
	clone.NormalizedCommand = bytes.Clone(s.op.NormalizedCommand)
	clone.CompiledDefinition = bytes.Clone(s.op.CompiledDefinition)
	clone.PreparedSchedule = bytes.Clone(s.op.PreparedSchedule)
	return &clone, nil
}

func (s *creationPrepareFakeStore) SealTaskCreationCommand(
	_ context.Context,
	lease types.TaskCreationLease,
	command []byte,
) error {
	s.sealCalls++
	if err := s.checkLease(lease); err != nil {
		return err
	}
	if len(s.op.NormalizedCommand) != 0 {
		if bytes.Equal(s.op.NormalizedCommand, command) &&
			creationPhaseAtLeast(s.op.Phase, types.TaskCreationPhaseCommandSealed) {
			return nil
		}
		return types.ErrConflict
	}
	if s.op.Phase != types.TaskCreationPhaseClaimed {
		return types.ErrConflict
	}
	s.op.NormalizedCommand = bytes.Clone(command)
	s.op.Phase = types.TaskCreationPhaseCommandSealed
	return nil
}

func (s *creationPrepareFakeStore) BeginTaskCreationTranslation(
	_ context.Context,
	lease types.TaskCreationLease,
) (bool, error) {
	s.beginCalls++
	if err := s.checkLease(lease); err != nil {
		return false, err
	}
	if creationPhaseAtLeast(s.op.Phase, types.TaskCreationPhaseTranslationStarted) {
		return false, nil
	}
	if s.op.Phase != types.TaskCreationPhaseCommandSealed {
		return false, types.ErrConflict
	}
	if s.beginErr != nil {
		if s.beginApplyBeforeError {
			s.op.Phase = types.TaskCreationPhaseTranslationStarted
		}
		return false, s.beginErr
	}
	s.op.Phase = types.TaskCreationPhaseTranslationStarted
	return true, nil
}

func (s *creationPrepareFakeStore) CheckpointTaskCreationDefinition(
	_ context.Context,
	lease types.TaskCreationLease,
	definition []byte,
	digest string,
) error {
	s.definitionCalls++
	if err := s.checkLease(lease); err != nil {
		return err
	}
	if len(s.op.CompiledDefinition) != 0 || s.op.CompiledDigest != "" {
		if bytes.Equal(s.op.CompiledDefinition, definition) && s.op.CompiledDigest == digest {
			return nil
		}
		return types.ErrConflict
	}
	apply := func() {
		s.op.CompiledDefinition = bytes.Clone(definition)
		s.op.CompiledDigest = digest
		s.op.Phase = types.TaskCreationPhaseDefinitionCompiled
	}
	if s.definitionErr != nil {
		if s.definitionApplyBeforeError {
			apply()
			s.definitionApplyBeforeError = false
		}
		err := s.definitionErr
		s.definitionErr = nil
		return err
	}
	if s.op.Phase != types.TaskCreationPhaseTranslationStarted {
		return types.ErrConflict
	}
	apply()
	return nil
}

func (s *creationPrepareFakeStore) CheckpointTaskCreationSchedule(
	_ context.Context,
	lease types.TaskCreationLease,
	prepared []byte,
) error {
	s.scheduleCalls++
	if err := s.checkLease(lease); err != nil {
		return err
	}
	if len(s.op.PreparedSchedule) != 0 {
		if bytes.Equal(s.op.PreparedSchedule, prepared) {
			return nil
		}
		return types.ErrConflict
	}
	apply := func() {
		s.op.PreparedSchedule = bytes.Clone(prepared)
		s.op.Phase = types.TaskCreationPhaseSchedulePrepared
	}
	if s.scheduleErr != nil {
		if s.scheduleApplyBeforeError {
			apply()
			s.scheduleApplyBeforeError = false
		}
		err := s.scheduleErr
		s.scheduleErr = nil
		return err
	}
	if s.op.Phase != types.TaskCreationPhaseDefinitionCompiled {
		return types.ErrConflict
	}
	apply()
	return nil
}

func (s *creationPrepareFakeStore) BlockTaskCreationOperation(
	_ context.Context,
	lease types.TaskCreationLease,
	errorCode, errorMessage string,
) error {
	s.blockCalls++
	s.blockCode = errorCode
	s.blockMessage = errorMessage
	if err := s.checkLease(lease); err != nil {
		return err
	}
	if s.blockErr != nil {
		return s.blockErr
	}
	s.op.Status = types.PendingActionStatusBlocked
	s.op.Phase = types.TaskCreationPhaseBlocked
	return nil
}

func (s *creationPrepareFakeStore) FailTaskCreationOperation(
	_ context.Context,
	lease types.TaskCreationLease,
	errorCode, errorMessage string,
) error {
	s.failCalls++
	s.failCode = errorCode
	s.failMessage = errorMessage
	if err := s.checkLease(lease); err != nil {
		return err
	}
	s.op.Status = types.PendingActionStatusFailed
	s.op.Phase = types.TaskCreationPhaseFailed
	return nil
}

func (s *creationPrepareFakeStore) checkLease(lease types.TaskCreationLease) error {
	if s.op.Lease() != lease {
		return types.ErrTaskCreationLeaseLost
	}
	if creationPhaseTerminal(s.op.Phase) {
		return types.ErrTaskCreationTerminal
	}
	return nil
}

type creationPrepareFakeCompiler struct {
	calls    int
	requests []TaskDefinitionCompileRequest
	plan     json.RawMessage
	err      error
}

func (c *creationPrepareFakeCompiler) CompileTaskDefinition(
	_ context.Context,
	req TaskDefinitionCompileRequest,
) (json.RawMessage, error) {
	c.calls++
	c.requests = append(c.requests, req)
	return bytes.Clone(c.plan), c.err
}

type creationPrepareFakeSchedules struct {
	deriveCalls int
	calls       int
	requests    []scheduler.TaskScheduleRequest
	err         error
	mutate      func(*scheduler.PreparedTaskSchedule)
}

func (s *creationPrepareFakeSchedules) DeriveID(
	tenantID, userID int64,
	operationID string,
) (string, error) {
	s.deriveCalls++
	return scheduler.TaskIDForOperation(tenantID, userID, operationID)
}

func (s *creationPrepareFakeSchedules) Prepare(
	_ context.Context,
	req scheduler.TaskScheduleRequest,
) (scheduler.PreparedTaskSchedule, error) {
	s.calls++
	s.requests = append(s.requests, req)
	if s.err != nil {
		return scheduler.PreparedTaskSchedule{}, s.err
	}
	prepared := validPreparedSchedule(req)
	if s.mutate != nil {
		s.mutate(&prepared)
	}
	recomputePreparedRequestDigest(&prepared)
	return prepared, nil
}

func TestCreationPreparer_PrepareHappyPath(t *testing.T) {
	service, store, compiler, schedules, input := newCreationPrepareFixture(t)

	result, err := service.Prepare(t.Context(), input)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	if compiler.calls != 1 || schedules.calls != 1 || store.beginCalls != 1 ||
		store.definitionCalls != 1 || store.scheduleCalls != 1 || store.blockCalls != 0 {
		t.Fatalf("calls compiler=%d schedules=%d begin=%d definition=%d schedule=%d block=%d",
			compiler.calls, schedules.calls, store.beginCalls, store.definitionCalls,
			store.scheduleCalls, store.blockCalls)
	}
	wantTaskID, err := scheduler.TaskIDForOperation(input.TenantID, input.UserID, input.OperationID)
	if err != nil {
		t.Fatal(err)
	}
	if result.Definition.TaskID != wantTaskID || result.Schedule.TaskID != wantTaskID {
		t.Fatalf("task IDs definition=%q schedule=%q want=%q",
			result.Definition.TaskID, result.Schedule.TaskID, wantTaskID)
	}
	if !validLowerSHA256(result.DefinitionDigest) ||
		result.DefinitionDigest != schedules.requests[0].PreparedDigest ||
		result.Schedule.PreparedDigest != result.DefinitionDigest {
		t.Fatalf("digest handoff result=%q request=%q prepared=%q",
			result.DefinitionDigest, schedules.requests[0].PreparedDigest,
			result.Schedule.PreparedDigest)
	}
	if compiler.requests[0].TenantID != input.TenantID ||
		compiler.requests[0].UserID != input.UserID ||
		compiler.requests[0].OperationID != input.OperationID {
		t.Fatalf("compiler scope=%+v", compiler.requests[0])
	}
	if string(result.Definition.ScopeJSON) != `{}` ||
		len(schedules.requests[0].Scope.SourceIDs) != 0 || schedules.requests[0].Scope.TopN != 0 {
		t.Fatalf("scope definition=%s request=%+v", result.Definition.ScopeJSON, schedules.requests[0].Scope)
	}
	if result.Definition.Strictness != types.StrictnessNormal ||
		result.Definition.NLDescription != "每天 AI" ||
		result.Definition.PlaybookContent != "每天寻找全球 AI 热点" {
		t.Fatalf("definition=%+v", result.Definition)
	}
	wantPlan := `{"sources":[{"platform":"web","capability":"search","title":"A","url":"vane://web/search?q=AI\u0026category=news","config":{"category":"news","query":"AI"}}]}`
	if string(result.Definition.FetchPlan) != wantPlan {
		t.Fatalf("canonical fetch plan=%s want=%s", result.Definition.FetchPlan, wantPlan)
	}
	if store.op.Phase != types.TaskCreationPhaseSchedulePrepared {
		t.Fatalf("phase=%q", store.op.Phase)
	}
	var checkpoint compiledTaskDefinitionCheckpoint
	mustUnmarshal(t, store.op.CompiledDefinition, &checkpoint)
	if checkpoint.TaskID != wantTaskID || schedules.deriveCalls != 1 {
		t.Fatalf("frozen task ID=%q want=%q derive_calls=%d",
			checkpoint.TaskID, wantTaskID, schedules.deriveCalls)
	}
}

func TestCreationPreparer_RecoversPreparedCheckpointWithoutRemoteWork(t *testing.T) {
	service, store, compiler, schedules, input := newCreationPrepareFixture(t)
	first, err := service.Prepare(t.Context(), input)
	if err != nil {
		t.Fatal(err)
	}

	second, err := service.Prepare(t.Context(), input)
	if err != nil {
		t.Fatalf("recovery: %v", err)
	}
	if compiler.calls != 1 || schedules.calls != 1 || store.beginCalls != 1 {
		t.Fatalf("recovery repeated work compiler=%d schedules=%d begin=%d",
			compiler.calls, schedules.calls, store.beginCalls)
	}
	if schedules.deriveCalls != 1 {
		t.Fatalf("prepared recovery re-derived task ID %d times", schedules.deriveCalls)
	}
	if second.DefinitionDigest != first.DefinitionDigest ||
		second.Definition.TaskID != first.Definition.TaskID {
		t.Fatalf("recovery changed result first=%+v second=%+v", first, second)
	}
}

func TestCreationPreparer_RecoversPreparedCheckpointFromLaterSagaPhase(t *testing.T) {
	service, store, compiler, schedules, input := newCreationPrepareFixture(t)
	first, err := service.Prepare(t.Context(), input)
	if err != nil {
		t.Fatal(err)
	}
	store.op.Phase = types.TaskCreationPhaseDefinitionCommitted

	second, err := service.Prepare(t.Context(), input)
	if err != nil {
		t.Fatalf("later-phase recovery: %v", err)
	}
	if compiler.calls != 1 || schedules.calls != 1 || second.DefinitionDigest != first.DefinitionDigest {
		t.Fatalf("later phase repeated work or changed result: compiler=%d schedules=%d first=%q second=%q",
			compiler.calls, schedules.calls, first.DefinitionDigest, second.DefinitionDigest)
	}
}

func TestCreationPreparer_CompiledCheckpointSurvivesA3Failure(t *testing.T) {
	service, _, compiler, schedules, input := newCreationPrepareFixture(t)
	schedules.err = errors.New("namespace unavailable")

	if _, err := service.Prepare(t.Context(), input); err == nil {
		t.Fatal("first Prepare unexpectedly succeeded")
	}
	schedules.err = nil
	result, err := service.Prepare(t.Context(), input)
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if compiler.calls != 1 || schedules.calls != 2 {
		t.Fatalf("calls compiler=%d schedules=%d", compiler.calls, schedules.calls)
	}
	if result.DefinitionDigest == "" {
		t.Fatal("retry returned empty definition digest")
	}
}

func TestCreationPreparer_ClassifiesA3PreparationFailures(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		wantBlock int
		wantFail  int
	}{
		{name: "invalid", err: scheduler.ErrTaskScheduleInvalid, wantFail: 1},
		{name: "blocked", err: scheduler.ErrTaskScheduleBlocked, wantBlock: 1},
		{name: "conflict", err: scheduler.ErrTaskScheduleConflict, wantBlock: 1},
		{name: "transient", err: scheduler.ErrTaskScheduleTransient},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			service, store, compiler, schedules, input := newCreationPrepareFixture(t)
			schedules.err = testCase.err

			if _, err := service.Prepare(t.Context(), input); err == nil {
				t.Fatal("Prepare unexpectedly succeeded")
			}
			if compiler.calls != 1 || store.blockCalls != testCase.wantBlock ||
				store.failCalls != testCase.wantFail {
				t.Fatalf("compiler=%d block=%d fail=%d",
					compiler.calls, store.blockCalls, store.failCalls)
			}
		})
	}
}

func TestCreationPreparer_TranslationStartedWithoutDefinitionBlocks(t *testing.T) {
	service, store, compiler, schedules, input := newCreationPrepareFixture(t)
	_, command, err := normalizeCreateScheduleCommand(store.op.Args)
	if err != nil {
		t.Fatal(err)
	}
	store.op.NormalizedCommand = command
	store.op.Phase = types.TaskCreationPhaseTranslationStarted
	store.op.Attempt = 2

	_, err = service.Prepare(t.Context(), input)
	if !errors.Is(err, ErrTranslationOutcomeAmbiguous) {
		t.Fatalf("err=%v", err)
	}
	if compiler.calls != 0 || schedules.calls != 0 || store.blockCalls != 1 ||
		store.blockCode != "translation_outcome_ambiguous" {
		t.Fatalf("compiler=%d schedules=%d block=%d code=%q",
			compiler.calls, schedules.calls, store.blockCalls, store.blockCode)
	}
}

func TestCreationPreparer_BeginUnknownNeverCompiles(t *testing.T) {
	service, store, compiler, _, input := newCreationPrepareFixture(t)
	store.beginErr = errors.New("commit response lost")
	store.beginApplyBeforeError = true

	if _, err := service.Prepare(t.Context(), input); err == nil {
		t.Fatal("first Prepare unexpectedly succeeded")
	}
	if compiler.calls != 0 || store.blockCalls != 0 {
		t.Fatalf("after ambiguous begin compiler=%d block=%d", compiler.calls, store.blockCalls)
	}
	store.beginErr = nil
	if _, err := service.Prepare(t.Context(), input); !errors.Is(err, types.ErrTaskCreationBusy) {
		t.Fatalf("active first-attempt recovery err=%v", err)
	}
	if store.blockCalls != 0 {
		t.Fatalf("active first attempt was prematurely blocked: %d", store.blockCalls)
	}
	// Only a lease takeover proves the original authorized caller is no longer
	// allowed to checkpoint a result. Recovery then closes the ambiguity.
	store.op.Attempt = 2
	store.op.Fence++
	input.Lease.Fence++
	if _, err := service.Prepare(t.Context(), input); !errors.Is(err, ErrTranslationOutcomeAmbiguous) {
		t.Fatalf("recovery err=%v", err)
	}
	if compiler.calls != 0 || store.blockCalls != 1 {
		t.Fatalf("recovery compiler=%d block=%d", compiler.calls, store.blockCalls)
	}
}

func TestCreationPreparer_CompilerErrorIsAmbiguousAndNeverRetried(t *testing.T) {
	service, store, compiler, schedules, input := newCreationPrepareFixture(t)
	compiler.err = context.DeadlineExceeded

	_, err := service.Prepare(t.Context(), input)
	if !errors.Is(err, ErrTranslationOutcomeAmbiguous) {
		t.Fatalf("err=%v", err)
	}
	if compiler.calls != 1 || schedules.calls != 0 || store.blockCalls != 1 ||
		store.blockCode != "translation_outcome_ambiguous" {
		t.Fatalf("compiler=%d schedules=%d block=%d code=%q",
			compiler.calls, schedules.calls, store.blockCalls, store.blockCode)
	}
	if _, err := service.Prepare(t.Context(), input); !errors.Is(err, types.ErrTaskCreationTerminal) {
		t.Fatalf("terminal retry err=%v", err)
	}
	if compiler.calls != 1 {
		t.Fatalf("terminal recovery repeated compiler: %d", compiler.calls)
	}
}

func TestCreationPreparer_LatePhaseWithoutCheckpointRequiresA5Reconcile(t *testing.T) {
	service, store, compiler, schedules, input := newCreationPrepareFixture(t)
	_, command, err := normalizeCreateScheduleCommand(store.op.Args)
	if err != nil {
		t.Fatal(err)
	}
	store.op.NormalizedCommand = command
	store.op.Phase = types.TaskCreationPhaseScheduleEnsured
	store.op.TaskID = "possibly-created-task"

	_, err = service.Prepare(t.Context(), input)
	if !errors.Is(err, ErrCreationCheckpointInvalid) {
		t.Fatalf("err=%v", err)
	}
	if store.blockCalls != 0 || store.failCalls != 0 || compiler.calls != 0 || schedules.calls != 0 {
		t.Fatalf("late corrupt phase was tombstoned or rerun: block=%d fail=%d compiler=%d schedules=%d",
			store.blockCalls, store.failCalls, compiler.calls, schedules.calls)
	}
}

func TestCreationPreparer_AdoptsDefinitionAfterCheckpointResponseLoss(t *testing.T) {
	service, store, compiler, schedules, input := newCreationPrepareFixture(t)
	store.definitionErr = errors.New("checkpoint response lost")
	store.definitionApplyBeforeError = true

	if _, err := service.Prepare(t.Context(), input); err == nil {
		t.Fatal("first Prepare unexpectedly succeeded")
	}
	if compiler.calls != 1 || schedules.calls != 0 ||
		store.op.Phase != types.TaskCreationPhaseDefinitionCompiled {
		t.Fatalf("after response loss compiler=%d schedules=%d phase=%q",
			compiler.calls, schedules.calls, store.op.Phase)
	}
	if _, err := service.Prepare(t.Context(), input); err != nil {
		t.Fatalf("recovery: %v", err)
	}
	if compiler.calls != 1 || schedules.calls != 1 {
		t.Fatalf("recovery repeated work compiler=%d schedules=%d", compiler.calls, schedules.calls)
	}
}

func TestCreationPreparer_AdoptsScheduleAfterCheckpointResponseLoss(t *testing.T) {
	service, store, compiler, schedules, input := newCreationPrepareFixture(t)
	store.scheduleErr = errors.New("schedule checkpoint response lost")
	store.scheduleApplyBeforeError = true

	if _, err := service.Prepare(t.Context(), input); err == nil {
		t.Fatal("first Prepare unexpectedly succeeded")
	}
	if _, err := service.Prepare(t.Context(), input); err != nil {
		t.Fatalf("recovery: %v", err)
	}
	if compiler.calls != 1 || schedules.calls != 1 {
		t.Fatalf("recovery repeated work compiler=%d schedules=%d", compiler.calls, schedules.calls)
	}
}

func TestCreationPreparer_RejectsInvalidFetchPlanBeforeA3(t *testing.T) {
	tests := []struct {
		name string
		plan string
	}{
		{name: "null", plan: `null`},
		{name: "missing sources", plan: `{}`},
		{name: "empty sources", plan: `{"sources":[]}`},
		{name: "duplicate URL", plan: `{"sources":[{"platform":"web","capability":"feed","url":"https://a","config":{}},{"platform":"web","capability":"contents","url":"https://a","config":{}}]}`},
		{name: "trimmed platform", plan: `{"sources":[{"platform":" web","capability":"feed","url":"https://a","config":{}}]}`},
		{name: "null config", plan: `{"sources":[{"platform":"web","capability":"feed","url":"https://a","config":null}]}`},
		{name: "unknown source field", plan: `{"sources":[{"platform":"web","capability":"feed","url":"https://a","config":{},"write":true}]}`},
		{name: "duplicate config key", plan: `{"sources":[{"platform":"web","capability":"feed","url":"https://a","config":{"q":1,"q":2}}]}`},
		{name: "unknown capability", plan: `{"sources":[{"platform":"web","capability":"invented","url":"vane://web/invented","config":{}}]}`},
		{name: "unavailable capability", plan: `{"sources":[{"platform":"x","capability":"search","url":"vane://x/search?q=ai","config":{}}]}`},
		{name: "synthetic URL mismatch", plan: `{"sources":[{"platform":"web","capability":"search","url":"vane://x/search?q=ai","config":{}}]}`},
		{name: "URL credentials", plan: `{"sources":[{"platform":"web","capability":"feed","url":"https://user:secret@example.com/feed","config":{}}]}`},
		{name: "URL config mismatch", plan: `{"sources":[{"platform":"web","capability":"search","title":"搜索: approved","url":"vane://web/search?q=approved","config":{"query":"different"}}]}`},
		{name: "missing capability config", plan: `{"sources":[{"platform":"web","capability":"search","title":"搜索: approved","url":"vane://web/search?q=approved","config":{}}]}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			service, store, compiler, schedules, input := newCreationPrepareFixture(t)
			compiler.plan = json.RawMessage(tt.plan)

			if _, err := service.Prepare(t.Context(), input); err == nil {
				t.Fatal("Prepare unexpectedly succeeded")
			}
			if compiler.calls != 1 || schedules.calls != 0 || store.failCalls != 1 ||
				store.failCode != "compiled_definition_invalid" || store.blockCalls != 0 {
				t.Fatalf("compiler=%d schedules=%d fail=%d code=%q block=%d",
					compiler.calls, schedules.calls, store.failCalls, store.failCode, store.blockCalls)
			}
		})
	}
}

func TestCreationPreparer_RejectsUnregisteredWideConfigFields(t *testing.T) {
	service, store, compiler, _, input := newCreationPrepareFixture(t)
	compiler.plan = json.RawMessage(
		`{"sources":[{"platform":"web","capability":"feed","url":"https://a","config":{"id":9007199254740993}}]}`,
	)

	if _, err := service.Prepare(t.Context(), input); err == nil {
		t.Fatal("unknown wide config field unexpectedly accepted")
	}
	if store.failCalls != 1 || store.failCode != "compiled_definition_invalid" {
		t.Fatalf("fail=%d code=%q", store.failCalls, store.failCode)
	}
}

func TestCreationPreparer_SealedCommandRejectsChangedDurableArgs(t *testing.T) {
	service, store, compiler, schedules, input := newCreationPrepareFixture(t)
	if _, err := service.Prepare(t.Context(), input); err != nil {
		t.Fatal(err)
	}
	store.op.Args = mustCreateArgs(t, "另一个未经确认的任务", "另一个任务")

	_, err := service.Prepare(t.Context(), input)
	if !errors.Is(err, types.ErrConflict) {
		t.Fatalf("err=%v", err)
	}
	if compiler.calls != 1 || schedules.calls != 1 {
		t.Fatalf("changed args repeated work compiler=%d schedules=%d", compiler.calls, schedules.calls)
	}
}

func TestCreationPreparer_BoundsApprovedIntentByUnicodeRunes(t *testing.T) {
	service, store, compiler, _, input := newCreationPrepareFixture(t)
	intent := strings.Repeat("界", 3999) + "🙂"
	store.op.Args = mustCreateArgs(t, intent, "长任务")

	result, err := service.Prepare(t.Context(), input)
	if err != nil {
		t.Fatal(err)
	}
	if compiler.requests[0].PlaybookContent != intent || result.Definition.PlaybookContent != intent {
		t.Fatalf("playbook rune len compiler=%d definition=%d",
			len([]rune(compiler.requests[0].PlaybookContent)),
			len([]rune(result.Definition.PlaybookContent)))
	}
	if result.Definition.NLDescription != "长任务" {
		t.Fatal("NLDescription changed with the approved intent")
	}

	service, store, compiler, _, input = newCreationPrepareFixture(t)
	store.op.Args = mustCreateArgs(t, intent+"甲", "过长任务")
	if _, err := service.Prepare(t.Context(), input); err == nil {
		t.Fatal("over-limit intent unexpectedly succeeded")
	}
	if compiler.calls != 0 || store.failCalls != 1 || store.failCode != "command_invalid" {
		t.Fatalf("over-limit intent compiler=%d fail=%d code=%q",
			compiler.calls, store.failCalls, store.failCode)
	}
}

func TestCreationPreparer_RejectsCrossScopeAndTamperedCheckpoints(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*creationPrepareFakeStore)
	}{
		{
			name: "compiled tenant",
			mutate: func(store *creationPrepareFakeStore) {
				var compiled compiledTaskDefinitionCheckpoint
				mustUnmarshal(nil, store.op.CompiledDefinition, &compiled)
				compiled.TenantID++
				store.op.CompiledDefinition = mustMarshal(nil, compiled)
				store.op.CompiledDigest = sha256Hex(store.op.CompiledDefinition)
			},
		},
		{
			name: "compiled digest",
			mutate: func(store *creationPrepareFakeStore) {
				store.op.CompiledDigest = strings.Repeat("0", 64)
			},
		},
		{
			name: "prepared tenant",
			mutate: func(store *creationPrepareFakeStore) {
				var prepared scheduler.PreparedTaskSchedule
				mustUnmarshal(nil, store.op.PreparedSchedule, &prepared)
				prepared.TenantID++
				recomputePreparedRequestDigest(&prepared)
				store.op.PreparedSchedule = mustMarshal(nil, prepared)
			},
		},
		{
			name: "prepared task ID",
			mutate: func(store *creationPrepareFakeStore) {
				var prepared scheduler.PreparedTaskSchedule
				mustUnmarshal(nil, store.op.PreparedSchedule, &prepared)
				prepared.TaskID += "-other"
				prepared.Action.Params.ScheduleID = prepared.TaskID
				recomputePreparedRequestDigest(&prepared)
				store.op.PreparedSchedule = mustMarshal(nil, prepared)
			},
		},
		{
			name: "prepared bytes before phase",
			mutate: func(store *creationPrepareFakeStore) {
				store.op.Phase = types.TaskCreationPhaseDefinitionCompiled
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			service, store, compiler, schedules, input := newCreationPrepareFixture(t)
			if _, err := service.Prepare(t.Context(), input); err != nil {
				t.Fatal(err)
			}
			tt.mutate(store)

			_, err := service.Prepare(t.Context(), input)
			if !errors.Is(err, ErrCreationCheckpointInvalid) {
				t.Fatalf("err=%v", err)
			}
			if compiler.calls != 1 || schedules.calls != 1 || store.blockCalls != 1 ||
				store.blockCode != "checkpoint_invalid" {
				t.Fatalf("compiler=%d schedules=%d block=%d code=%q",
					compiler.calls, schedules.calls, store.blockCalls, store.blockCode)
			}
		})
	}
}

func TestCreationPreparer_RejectsInvalidCommandBeforePaidWork(t *testing.T) {
	tests := []struct {
		name string
		args string
	}{
		{name: "unknown root field", args: `{"spec":{"cron":"0 8 * * *"},"intent":"AI","nl_description":"x","extra":true}`},
		{name: "unknown spec field", args: `{"spec":{"cron":"0 8 * * *","seconds":1},"intent":"AI","nl_description":"x"}`},
		{name: "duplicate key", args: `{"spec":{"cron":"0 8 * * *","cron":"0 9 * * *"},"intent":"AI","nl_description":"x"}`},
		{name: "bad cron hour", args: `{"spec":{"cron":"0 25 * * *"},"intent":"AI","nl_description":"x"}`},
		{name: "sub-hour cron", args: `{"spec":{"cron":"*/5 * * * *"},"intent":"AI","nl_description":"x"}`},
		{name: "short interval", args: `{"spec":{"every_seconds":3599},"intent":"AI","nl_description":"x"}`},
		{name: "cron anchor", args: `{"spec":{"cron":"0 8 * * *","anchor_at":"2026-07-21T08:00:00Z"},"intent":"AI","nl_description":"x"}`},
		{name: "fractional anchor", args: `{"spec":{"every_seconds":3600,"anchor_at":"2026-07-21T08:00:00.1Z"},"intent":"AI","nl_description":"x"}`},
		{name: "invalid timezone", args: `{"spec":{"cron":"0 8 * * *","tz":"Mars/Base"},"intent":"AI","nl_description":"x"}`},
		{name: "invalid strictness", args: `{"spec":{"cron":"0 8 * * *"},"intent":"AI","nl_description":"x","strictness":"extreme"}`},
		{name: "empty intent", args: `{"spec":{"cron":"0 8 * * *"},"intent":"  ","nl_description":"x"}`},
		{name: "trailing value", args: `{"spec":{"cron":"0 8 * * *"},"intent":"AI","nl_description":"x"} {}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			service, store, compiler, schedules, input := newCreationPrepareFixture(t)
			store.op.Args = json.RawMessage(tt.args)

			if _, err := service.Prepare(t.Context(), input); err == nil {
				t.Fatal("Prepare unexpectedly succeeded")
			}
			if store.sealCalls != 0 || store.beginCalls != 0 || compiler.calls != 0 ||
				schedules.calls != 0 || store.failCalls != 1 || store.failCode != "command_invalid" {
				t.Fatalf("seal=%d begin=%d compiler=%d schedules=%d fail=%d code=%q",
					store.sealCalls, store.beginCalls, compiler.calls, schedules.calls,
					store.failCalls, store.failCode)
			}
		})
	}
}

func TestCreationPreparer_DerivesOptionalDescriptionFromApprovedIntent(t *testing.T) {
	service, store, _, _, input := newCreationPrepareFixture(t)
	store.op.Args = mustCreateArgs(t, "每天寻找 Anthropic 官方状态变化", "")

	result, err := service.Prepare(t.Context(), input)
	if err != nil {
		t.Fatal(err)
	}
	if result.Definition.NLDescription != "每天寻找 Anthropic 官方状态变化" ||
		result.Definition.PlaybookContent != "每天寻找 Anthropic 官方状态变化" {
		t.Fatalf("derived definition=%+v", result.Definition)
	}
}

func TestCreationPreparer_InvalidArgsAfterSealAreCheckpointCorruption(t *testing.T) {
	service, store, compiler, schedules, input := newCreationPrepareFixture(t)
	_, canonical, err := normalizeCreateScheduleCommand(store.op.Args)
	if err != nil {
		t.Fatal(err)
	}
	store.op.NormalizedCommand = canonical
	store.op.Phase = types.TaskCreationPhaseCommandSealed
	store.op.Args = json.RawMessage(`{"spec":null}`)

	_, err = service.Prepare(t.Context(), input)
	if !errors.Is(err, ErrCreationCheckpointInvalid) {
		t.Fatalf("err=%v", err)
	}
	if store.blockCalls != 1 || store.blockCode != "checkpoint_invalid" ||
		store.failCalls != 0 || compiler.calls != 0 || schedules.calls != 0 {
		t.Fatalf("block=%d code=%q fail=%d compiler=%d schedules=%d",
			store.blockCalls, store.blockCode, store.failCalls, compiler.calls, schedules.calls)
	}
}

func TestCreationPreparer_RejectsLeaseScopeMismatch(t *testing.T) {
	service, store, compiler, schedules, input := newCreationPrepareFixture(t)
	input.TenantID++

	_, err := service.Prepare(t.Context(), input)
	if !errors.Is(err, types.ErrTaskCreationLeaseLost) {
		t.Fatalf("err=%v", err)
	}
	if store.sealCalls != 0 || compiler.calls != 0 || schedules.calls != 0 {
		t.Fatalf("seal=%d compiler=%d schedules=%d", store.sealCalls, compiler.calls, schedules.calls)
	}
}

func newCreationPrepareFixture(t *testing.T) (
	*CreationPreparer,
	*creationPrepareFakeStore,
	*creationPrepareFakeCompiler,
	*creationPrepareFakeSchedules,
	CreationPrepareInput,
) {
	t.Helper()
	lease := types.TaskCreationLease{
		ID: "op-123", TenantID: 7, UserID: 11, LeaseOwner: "worker-1", Fence: 3,
	}
	store := &creationPrepareFakeStore{op: types.TaskCreationOperation{
		ID: lease.ID, TenantID: lease.TenantID, UserID: lease.UserID,
		ToolName: "create_schedule", Status: types.PendingActionStatusExecuting,
		ExecutionVersion: types.TaskCreationExecutionVersionV1,
		Phase:            types.TaskCreationPhaseClaimed, LeaseOwner: lease.LeaseOwner, Fence: lease.Fence, Attempt: 1,
		Args: json.RawMessage(
			`{"spec":{"every_seconds":3600,"tz":"UTC"},"intent":"每天寻找全球 AI 热点","nl_description":"每天 AI","strictness":"normal"}`,
		),
	}}
	compiler := &creationPrepareFakeCompiler{plan: json.RawMessage(
		`{"sources":[{"platform":"web","capability":"search","title":"A","url":"vane://web/search?q=AI&category=news","config":{"query":"AI","category":"news"}}]}`,
	)}
	schedules := &creationPrepareFakeSchedules{}
	return NewCreationPreparer(store, compiler, schedules), store, compiler, schedules,
		CreationPrepareInput{
			TenantID: lease.TenantID, UserID: lease.UserID, OperationID: lease.ID,
			Lease: lease,
		}
}

func validPreparedSchedule(req scheduler.TaskScheduleRequest) scheduler.PreparedTaskSchedule {
	taskID, err := scheduler.TaskIDForOperation(req.TenantID, req.UserID, req.OperationID)
	if err != nil {
		panic(err)
	}
	if req.Spec.EverySeconds <= 0 || req.Spec.AnchorAt != "" {
		panic("test fixture only supports an unanchored interval")
	}
	prepared := scheduler.PreparedTaskSchedule{
		IDSchemeVersion: "v1", FingerprintVersion: "v1",
		Namespace: "default", NamespaceID: "namespace-id", ConverterID: "temporal-default-json-v1",
		TaskID: taskID, TenantID: req.TenantID, UserID: req.UserID,
		OperationID: req.OperationID, PreparedDigest: req.PreparedDigest,
		Timing: scheduler.PreparedTaskScheduleTiming{
			EveryNanos:   int64(time.Duration(req.Spec.EverySeconds) * time.Second),
			TimeZoneName: req.Spec.TZ,
		},
		Action: scheduler.PreparedTaskScheduleAction{
			Params: workflow.PushParams{
				UserID: req.UserID, ScheduleID: taskID, Scope: workflow.PushScope{},
				NLDesc: strings.TrimSpace(req.NLDescription),
			},
			TaskQueue: "vane-test", WorkflowType: "PushPipelineWorkflow",
			ActionID:                 "wf-" + taskID,
			WorkflowTaskTimeoutNanos: int64(10 * time.Second),
			ActivationNote:           "vane/task-schedule/v1:definition-committed",
		},
		Policy: scheduler.PreparedTaskSchedulePolicy{
			Overlap:      int32(enums.SCHEDULE_OVERLAP_POLICY_SKIP),
			CatchupNanos: int64(time.Minute),
		},
		Creation: scheduler.PreparedTaskScheduleCreation{
			Paused: true, Note: "vane/task-schedule/v1:provisioning-paused",
		},
	}
	recomputePreparedRequestDigest(&prepared)
	return prepared
}

func recomputePreparedRequestDigest(prepared *scheduler.PreparedTaskSchedule) {
	prepared.RequestDigest = ""
	prepared.RequestDigest = sha256Hex(mustMarshal(nil, *prepared))
}

func mustCreateArgs(t *testing.T, intent, description string) json.RawMessage {
	t.Helper()
	return mustMarshal(t, map[string]any{
		"spec":           map[string]any{"every_seconds": 3600, "tz": "UTC"},
		"intent":         intent,
		"nl_description": description,
		"strictness":     "normal",
	})
}

func mustMarshal(t *testing.T, value any) []byte {
	if t != nil {
		t.Helper()
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		if t != nil {
			t.Fatal(err)
		}
		panic(err)
	}
	return encoded
}

func mustUnmarshal(t *testing.T, raw []byte, dst any) {
	if t != nil {
		t.Helper()
	}
	if err := json.Unmarshal(raw, dst); err != nil {
		if t != nil {
			t.Fatal(err)
		}
		panic(fmt.Sprintf("unmarshal: %v", err))
	}
}
