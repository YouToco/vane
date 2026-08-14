package task

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/server/store"
	"github.com/YouToco/vane/server/types"
)

// completeResponseLossStore keeps every coordinator checkpoint in real
// PostgreSQL, but loses the response after Complete has committed. Cancelling
// the caller at the same boundary proves that convergence uses a detached
// readback instead of accidentally succeeding through the request context.
type completeResponseLossStore struct {
	*store.Store
	cancel        context.CancelFunc
	completeCalls int
}

type completeResearchV3ResponseLossStore struct {
	*store.Store
	cancel        context.CancelFunc
	completeCalls int
}

type createResearchV3ResponseLossStore struct {
	*store.Store
	cancel      context.CancelFunc
	createCalls int
}

type researchV3OwnerDowngradeStage string

const (
	downgradeAtPausedCommit     researchV3OwnerDowngradeStage = "paused_commit"
	downgradeAtActivationBegin  researchV3OwnerDowngradeStage = "activation_begin"
	downgradeAtActivationCommit researchV3OwnerDowngradeStage = "activation_commit"
)

type researchV3OwnerDowngradeStore struct {
	*store.Store
	databaseURL string
	stage       researchV3OwnerDowngradeStage
	downgraded  bool
}

func (s *researchV3OwnerDowngradeStore) downgradeOwner(
	ctx context.Context, lease types.TaskCreationLease,
) error {
	if s.downgraded {
		return nil
	}
	s.downgraded = true
	conn, err := pgx.Connect(ctx, s.databaseURL)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()
	_, err = conn.Exec(ctx, `UPDATE memberships SET role='member'
		WHERE tenant_id=$1 AND user_id=$2`, lease.TenantID, lease.UserID)
	return err
}

func (s *researchV3OwnerDowngradeStore) CommitPausedResearchTaskDefinitionV3ForCreation(
	ctx context.Context, params types.CommitPausedResearchTaskDefinitionV3ForCreationParams,
) error {
	if s.stage == downgradeAtPausedCommit {
		if err := s.downgradeOwner(ctx, params.Lease); err != nil {
			return err
		}
	}
	return s.Store.CommitPausedResearchTaskDefinitionV3ForCreation(ctx, params)
}

func (s *researchV3OwnerDowngradeStore) BeginResearchTaskCreationActivationV3(
	ctx context.Context, lease types.TaskCreationLease,
	binding types.ResearchTaskCreationActivationBindingV3,
) (bool, error) {
	if s.stage == downgradeAtActivationBegin {
		if err := s.downgradeOwner(ctx, lease); err != nil {
			return false, err
		}
	}
	return s.Store.BeginResearchTaskCreationActivationV3(ctx, lease, binding)
}

func (s *researchV3OwnerDowngradeStore) CommitResearchTaskCreationActivationV3(
	ctx context.Context, lease types.TaskCreationLease,
	binding types.ResearchTaskCreationActivationBindingV3,
) error {
	if s.stage == downgradeAtActivationCommit {
		if err := s.downgradeOwner(ctx, lease); err != nil {
			return err
		}
	}
	return s.Store.CommitResearchTaskCreationActivationV3(ctx, lease, binding)
}

type postgresReceiptSessionRecorder struct{ store *store.Store }

func (r postgresReceiptSessionRecorder) RecordCreationReceiptSession(
	ctx context.Context,
	receipt types.TaskCreationReceipt,
	messages json.RawMessage,
) error {
	return r.store.RecordTaskCreationReceiptSessionMessages(
		ctx, receipt.Lease(), messages,
	)
}

func (s *completeResponseLossStore) CompleteTaskCreationOperation(
	ctx context.Context,
	lease types.TaskCreationLease,
	taskID string,
	result json.RawMessage,
) error {
	s.completeCalls++
	if err := s.Store.CompleteTaskCreationOperation(ctx, lease, taskID, result); err != nil {
		return err
	}
	s.cancel()
	return errors.New("complete committed but response was lost")
}

func (s *completeResearchV3ResponseLossStore) CompleteResearchTaskCreationOperationV3(
	ctx context.Context,
	lease types.TaskCreationLease,
	taskID string,
	result json.RawMessage,
) error {
	s.completeCalls++
	if err := s.Store.CompleteResearchTaskCreationOperationV3(
		ctx, lease, taskID, result); err != nil {
		return err
	}
	s.cancel()
	return errors.New("native V3 complete committed but response was lost")
}

func (s *createResearchV3ResponseLossStore) CreateResearchTaskCreationOperationV3(
	ctx context.Context,
	params types.CreateResearchTaskCreationOperationV3Params,
) (*types.TaskCreationOperation, error) {
	s.createCalls++
	if _, err := s.Store.CreateResearchTaskCreationOperationV3(ctx, params); err != nil {
		return nil, err
	}
	s.cancel()
	return nil, errors.New("native V3 create committed but response was lost")
}

// TestCreationCoordinator_PostgreSQLRoundTrip exercises the complete A5 saga
// through a real Store. In particular, task_creation_operations.args and result are
// JSONB: PostgreSQL rewrites their object bytes, so raw-byte identity checks
// would reject both the first proposal and the terminal replay.
func TestCreationCoordinator_PostgreSQLRoundTrip(t *testing.T) {
	st, tenantID, userID := newCreationCoordinatorPostgreSQLFixture(t)

	t.Run("complete confirm and terminal replay survive JSONB rewrite", func(t *testing.T) {
		schedules := &creationSagaFakeScheduler{}
		coordinator := NewCreationCoordinator(st, schedules, nil)
		actionID := "task-create-jsonb-" + uuid.NewString()
		rawArgs := mustCreateProposalArgs(t, "每天寻找全球 AI 热点", "每天 AI")
		expiresAt := time.Now().Add(time.Hour)

		proposal, err := coordinator.Prepare(t.Context(), CreationProposalInput{
			ActionID: actionID, UserID: userID, RawArgs: rawArgs,
			ExpiresAt: expiresAt,
		})
		if err != nil {
			t.Fatalf("Propose() through PostgreSQL: %v", err)
		}
		if proposal.ID != actionID || proposal.Summary == "" {
			t.Fatalf("proposal = %+v", proposal)
		}

		persisted, err := st.LoadTaskCreationOperation(
			t.Context(), actionID, tenantID, userID,
		)
		if err != nil {
			t.Fatalf("LoadTaskCreationOperation() after proposal: %v", err)
		}
		command, _, err := normalizeCreateScheduleCommand(persisted.Args)
		if err != nil {
			t.Fatalf("normalize persisted fixture: %v", err)
		}
		canonicalArgs, err := canonicalCreationProposalArgs(command)
		if err != nil {
			t.Fatalf("canonicalize persisted fixture: %v", err)
		}
		if bytes.Equal(persisted.Args, canonicalArgs) {
			t.Fatalf("fixture did not exercise JSONB byte rewriting: %s", persisted.Args)
		}
		if !creationProposalArgsEqual(persisted.Args, canonicalArgs) {
			t.Fatalf("persisted args changed meaning: %s", persisted.Args)
		}

		result, err := coordinator.Execute(t.Context(), userID, actionID, testCreationReceiptTarget)
		if err != nil {
			t.Fatalf("Execute() through PostgreSQL: %v", err)
		}
		if result.Status != types.TaskOperationStatusExecuted || result.TaskID == "" ||
			result.Recovering || result.Replayed {
			t.Fatalf("first confirmation result = %+v", result)
		}

		terminal, err := st.LoadTaskCreationOperation(
			t.Context(), actionID, tenantID, userID,
		)
		if err != nil {
			t.Fatalf("LoadTaskCreationOperation() after completion: %v", err)
		}
		canonicalResult, err := marshalCreationSuccess(result.TaskID)
		if err != nil {
			t.Fatalf("marshal result fixture: %v", err)
		}
		if bytes.Equal(terminal.Result, canonicalResult) {
			t.Fatalf("fixture did not exercise terminal JSONB byte rewriting: %s", terminal.Result)
		}

		schedulerEvents := len(schedules.events)
		replayed, err := coordinator.Execute(t.Context(), userID, actionID, testCreationReceiptTarget)
		if err != nil {
			t.Fatalf("Execute() terminal replay: %v", err)
		}
		if replayed.Status != types.TaskOperationStatusExecuted || !replayed.Replayed ||
			replayed.TaskID != result.TaskID || replayed.Message != result.Message {
			t.Fatalf("terminal replay result = %+v; first = %+v", replayed, result)
		}
		if len(schedules.events) != schedulerEvents {
			t.Fatalf("terminal replay touched scheduler: before=%d after=%d events=%v",
				schedulerEvents, len(schedules.events), schedules.events)
		}

		// The same terminal transaction produced one delayed outbox row. A real
		// Store + dispatcher freezes the session fact and commits the receipt
		// without any live request state.
		receipt, err := st.LoadTaskCreationReceiptByOperation(
			t.Context(), actionID, tenantID, userID,
		)
		if err != nil {
			t.Fatalf("load terminal receipt: %v", err)
		}
		receipt = waitForDueTaskCreationReceipt(t, st, *receipt)
		dispatcher, err := NewCreationReceiptDispatcher(CreationReceiptDispatcherDeps{
			Store:    st,
			Sessions: postgresReceiptSessionRecorder{store: st},
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := dispatcher.dispatchReceipt(t.Context(), *receipt); err != nil {
			t.Fatalf("dispatch terminal receipt through PostgreSQL: %v", err)
		}
		receipt, err = st.LoadTaskCreationReceiptByOperation(
			t.Context(), actionID, tenantID, userID,
		)
		if err != nil || receipt.Status != types.TaskCreationReceiptStatusSent ||
			receipt.SessionRecordedAt == nil || receipt.ProviderMessageID != testCreationReceiptTarget.Target {
			t.Fatalf("terminal receipt did not converge: receipt=%+v err=%v", receipt, err)
		}
	})

	t.Run("complete response loss converges with detached readback", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		responseLoss := &completeResponseLossStore{Store: st, cancel: cancel}
		coordinator := NewCreationCoordinator(
			responseLoss, &creationSagaFakeScheduler{}, nil,
		)
		actionID := "task-create-complete-loss-" + uuid.NewString()
		if _, err := coordinator.Prepare(ctx, CreationProposalInput{
			ActionID: actionID, UserID: userID,
			RawArgs:   mustCreateProposalArgs(t, "每天监控 AI 模型发布", "AI 模型发布"),
			ExpiresAt: time.Now().Add(time.Hour),
		}); err != nil {
			t.Fatalf("Propose(): %v", err)
		}

		result, err := coordinator.Execute(ctx, userID, actionID, testCreationReceiptTarget)
		if err != nil {
			t.Fatalf("Execute() should adopt committed terminal row: %v", err)
		}
		if !errors.Is(ctx.Err(), context.Canceled) {
			t.Fatalf("response-loss boundary did not cancel request context: %v", ctx.Err())
		}
		if responseLoss.completeCalls != 1 ||
			result.Status != types.TaskOperationStatusExecuted || result.TaskID == "" ||
			result.Recovering || result.Replayed {
			t.Fatalf("result=%+v complete_calls=%d", result, responseLoss.completeCalls)
		}

		replayed, err := coordinator.Execute(t.Context(), userID, actionID, testCreationReceiptTarget)
		if err != nil {
			t.Fatalf("Execute() after adopted response loss: %v", err)
		}
		if !replayed.Replayed || replayed.Status != types.TaskOperationStatusExecuted ||
			replayed.TaskID != result.TaskID || responseLoss.completeCalls != 1 {
			t.Fatalf("replayed=%+v complete_calls=%d", replayed, responseLoss.completeCalls)
		}
	})
}

func TestCreationCoordinator_NativeV3PostgreSQLTemporalLifecycle(t *testing.T) {
	st, tenantID, userID := newCreationCoordinatorPostgreSQLFixture(t)
	schedules := &creationSagaFakeScheduler{}
	coordinator := NewCreationCoordinator(st, schedules, nil,
		WithResearchV3CreationPolicy(testResearchV3CreationPolicy()))
	input := testResearchV3CreationInput()
	input.ActionID = "native-v3-postgres-" + uuid.NewString()
	input.UserID = userID
	input.SpecJSON = json.RawMessage(`{"tz":"Asia/Shanghai","every_seconds":3600}`)
	if _, err := coordinator.PrepareResearchV3(t.Context(), input); err != nil {
		t.Fatalf("PrepareResearchV3: %v", err)
	}
	result, err := coordinator.ExecuteResearchV3(
		t.Context(), userID, input.ActionID, testCreationReceiptTarget)
	if err != nil {
		t.Fatalf("ExecuteResearchV3: %v", err)
	}
	if result.Status != types.TaskOperationStatusExecuted || result.TaskID == "" ||
		result.Recovering || result.Replayed {
		t.Fatalf("native V3 result=%+v events=%v", result, schedules.events)
	}
	op, err := st.LoadResearchTaskCreationOperationV3(
		t.Context(), input.ActionID, tenantID, userID)
	if err != nil {
		t.Fatal(err)
	}
	if op.ExecutionVersion != types.TaskCreationExecutionVersionV2 ||
		op.ToolName != "manage_tasks" || op.Status != types.TaskOperationStatusExecuted ||
		op.Phase != types.TaskCreationPhaseCompleted || op.TombstonedAt == nil {
		t.Fatalf("native V3 terminal operation=%+v", op)
	}
	schedule, err := st.GetSchedule(t.Context(), result.TaskID, userID)
	if err != nil {
		t.Fatal(err)
	}
	if schedule.TenantID != tenantID || schedule.Status != types.ScheduleStatusActive ||
		schedule.ExecutionMode != types.ExecutionModeDiscoverAtRun {
		t.Fatalf("native V3 schedule=%+v", schedule)
	}
	if _, err := st.LoadTaskCreationReceiptByOperation(
		t.Context(), input.ActionID, tenantID, userID); err != nil {
		t.Fatalf("native V3 terminal receipt: %v", err)
	}
	eventCount := len(schedules.events)
	replayInput := input
	replayInput.ExpiresAt = time.Now().Add(24 * time.Hour)
	replayedProposal, err := coordinator.PrepareResearchV3(t.Context(), replayInput)
	if err != nil || replayedProposal.ID != input.ActionID ||
		replayedProposal.Summary != input.TaskName {
		t.Fatalf("native V3 whole-tool prepare replay=%+v err=%v", replayedProposal, err)
	}
	if len(schedules.events) != eventCount {
		t.Fatalf("native V3 prepare replay touched Temporal: before=%d after=%d",
			eventCount, len(schedules.events))
	}
	replayed, err := coordinator.ExecuteResearchV3(
		t.Context(), userID, input.ActionID, testCreationReceiptTarget)
	if err != nil || !replayed.Replayed || replayed.TaskID != result.TaskID {
		t.Fatalf("native V3 terminal replay=%+v err=%v", replayed, err)
	}
	if len(schedules.events) != eventCount {
		t.Fatalf("native V3 replay touched Temporal: before=%d after=%d", eventCount, len(schedules.events))
	}
}

func TestCreationCoordinator_NativeV3PostgreSQLCreateResponseLoss(t *testing.T) {
	st, tenantID, userID := newCreationCoordinatorPostgreSQLFixture(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	responseLoss := &createResearchV3ResponseLossStore{Store: st, cancel: cancel}
	coordinator := NewCreationCoordinator(responseLoss, &creationSagaFakeScheduler{}, nil,
		WithResearchV3CreationPolicy(testResearchV3CreationPolicy()))
	input := testResearchV3CreationInput()
	input.ActionID = "native-v3-create-loss-" + uuid.NewString()
	input.UserID = userID
	input.SpecJSON = json.RawMessage(`{"tz":"Asia/Shanghai","every_seconds":3600}`)
	proposal, err := coordinator.PrepareResearchV3(ctx, input)
	if err != nil {
		t.Fatalf("PrepareResearchV3 should adopt committed operation: %v", err)
	}
	if !errors.Is(ctx.Err(), context.Canceled) || responseLoss.createCalls != 1 ||
		proposal.ID != input.ActionID || proposal.Summary != input.TaskName {
		t.Fatalf("proposal=%+v create_calls=%d ctx=%v",
			proposal, responseLoss.createCalls, ctx.Err())
	}
	op, err := st.LoadResearchTaskCreationOperationV3(
		t.Context(), input.ActionID, tenantID, userID)
	if err != nil || op.Status != types.TaskOperationStatusPending ||
		op.ExecutionVersion != types.TaskCreationExecutionVersionV2 {
		t.Fatalf("adopted operation=%+v err=%v", op, err)
	}
}

func TestCreationCoordinator_NativeV3PostgreSQLCompleteResponseLoss(t *testing.T) {
	st, _, userID := newCreationCoordinatorPostgreSQLFixture(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	responseLoss := &completeResearchV3ResponseLossStore{Store: st, cancel: cancel}
	schedules := &creationSagaFakeScheduler{}
	coordinator := NewCreationCoordinator(responseLoss, schedules, nil,
		WithResearchV3CreationPolicy(testResearchV3CreationPolicy()))
	input := testResearchV3CreationInput()
	input.ActionID = "native-v3-complete-loss-" + uuid.NewString()
	input.UserID = userID
	input.SpecJSON = json.RawMessage(`{"tz":"Asia/Shanghai","every_seconds":3600}`)
	if _, err := coordinator.PrepareResearchV3(ctx, input); err != nil {
		t.Fatalf("PrepareResearchV3: %v", err)
	}
	result, err := coordinator.ExecuteResearchV3(
		ctx, userID, input.ActionID, testCreationReceiptTarget)
	if err != nil {
		t.Fatalf("ExecuteResearchV3 should adopt committed terminal row: %v", err)
	}
	if !errors.Is(ctx.Err(), context.Canceled) || responseLoss.completeCalls != 1 ||
		result.Status != types.TaskOperationStatusExecuted || result.TaskID == "" ||
		result.Recovering || result.Replayed {
		t.Fatalf("native V3 result=%+v complete_calls=%d ctx=%v",
			result, responseLoss.completeCalls, ctx.Err())
	}
	replayed, err := coordinator.ExecuteResearchV3(
		t.Context(), userID, input.ActionID, testCreationReceiptTarget)
	if err != nil || !replayed.Replayed || replayed.TaskID != result.TaskID ||
		responseLoss.completeCalls != 1 {
		t.Fatalf("native V3 replay=%+v complete_calls=%d err=%v",
			replayed, responseLoss.completeCalls, err)
	}
}

func TestCreationCoordinator_NativeV3PostgreSQLOwnerDowngradeCleansUp(t *testing.T) {
	for _, tc := range []struct {
		name         string
		stage        researchV3OwnerDowngradeStage
		beforeEnsure bool
		afterBegin   bool
		wantActivate bool
	}{
		{name: "before ensure", beforeEnsure: true},
		{name: "paused commit", stage: downgradeAtPausedCommit},
		{name: "activation begin", stage: downgradeAtActivationBegin},
		{name: "after activation begin", afterBegin: true, wantActivate: true},
		{name: "after remote activation", stage: downgradeAtActivationCommit, wantActivate: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st, tenantID, userID := newCreationCoordinatorPostgreSQLFixture(t)
			databaseURL := creationCoordinatorTestDatabaseURL()
			wrapped := &researchV3OwnerDowngradeStore{
				Store: st, databaseURL: databaseURL, stage: tc.stage,
			}
			schedules := &creationSagaFakeScheduler{}
			if tc.afterBegin {
				schedules.beforeActivate = func(ctx context.Context) error {
					return wrapped.downgradeOwner(ctx, types.TaskCreationLease{
						TenantID: tenantID, UserID: userID,
					})
				}
			}
			coordinator := NewCreationCoordinator(wrapped, schedules, nil,
				WithResearchV3CreationPolicy(testResearchV3CreationPolicy()))
			input := testResearchV3CreationInput()
			input.ActionID = "native-v3-owner-downgrade-" + uuid.NewString()
			input.UserID = userID
			input.SpecJSON = json.RawMessage(`{"tz":"Asia/Shanghai","every_seconds":3600}`)
			if _, err := coordinator.PrepareResearchV3(t.Context(), input); err != nil {
				t.Fatalf("PrepareResearchV3: %v", err)
			}
			if tc.beforeEnsure {
				if err := wrapped.downgradeOwner(t.Context(), types.TaskCreationLease{
					TenantID: tenantID, UserID: userID,
				}); err != nil {
					t.Fatal(err)
				}
			}
			result, err := coordinator.ExecuteResearchV3(
				t.Context(), userID, input.ActionID, testCreationReceiptTarget)
			if err != nil || result.Status != types.TaskOperationStatusFailed ||
				result.Recovering || result.TaskID == "" {
				t.Fatalf("owner downgrade result=%+v err=%v events=%v",
					result, err, schedules.events)
			}
			if tc.wantActivate && !slices.Contains(schedules.events, "activate") {
				t.Fatalf("remote activation was not exercised: %v", schedules.events)
			}
			if !slices.Contains(schedules.events, "delete") {
				t.Fatalf("exact Temporal delete was not exercised: %v", schedules.events)
			}
			assertNativeV3CreationCleanupPostgreSQL(
				t, databaseURL, input.ActionID, tenantID, userID, result.TaskID,
				"creation_scope_inactive")
			eventCount := len(schedules.events)
			replayed, err := coordinator.ExecuteResearchV3(
				t.Context(), userID, input.ActionID, testCreationReceiptTarget)
			if err != nil || !replayed.Replayed ||
				replayed.Status != types.TaskOperationStatusFailed ||
				len(schedules.events) != eventCount {
				t.Fatalf("owner downgrade replay=%+v err=%v before=%d after=%d",
					replayed, err, eventCount, len(schedules.events))
			}
		})
	}
}

func TestCreationCoordinator_NativeV3PostgreSQLCapacityLimitCleansUp(t *testing.T) {
	st, tenantID, userID := newCreationCoordinatorPostgreSQLFixture(t)
	databaseURL := creationCoordinatorTestDatabaseURL()
	conn, err := pgx.Connect(t.Context(), databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 20; index++ {
		if _, err := conn.Exec(t.Context(), `
			INSERT INTO schedules(id,tenant_id,user_id,nl_description,spec_json,scope_json,status)
			VALUES($1,$2,$3,'existing','{"every_seconds":3600}','{}','active')`,
			"native-v3-capacity-existing-"+uuid.NewString(), tenantID, userID); err != nil {
			_ = conn.Close(t.Context())
			t.Fatal(err)
		}
	}
	if err := conn.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	schedules := &creationSagaFakeScheduler{}
	coordinator := NewCreationCoordinator(st, schedules, nil,
		WithResearchV3CreationPolicy(testResearchV3CreationPolicy()))
	input := testResearchV3CreationInput()
	input.ActionID = "native-v3-capacity-cleanup-" + uuid.NewString()
	input.UserID = userID
	input.SpecJSON = json.RawMessage(`{"tz":"Asia/Shanghai","every_seconds":3600}`)
	if _, err := coordinator.PrepareResearchV3(t.Context(), input); err != nil {
		t.Fatal(err)
	}
	result, err := coordinator.ExecuteResearchV3(
		t.Context(), userID, input.ActionID, testCreationReceiptTarget)
	if err != nil || result.Status != types.TaskOperationStatusFailed ||
		result.Recovering || result.TaskID == "" ||
		!slices.Contains(schedules.events, "delete") {
		t.Fatalf("capacity result=%+v err=%v events=%v", result, err, schedules.events)
	}
	assertNativeV3CreationCleanupPostgreSQL(
		t, databaseURL, input.ActionID, tenantID, userID, result.TaskID,
		"task_limit_reached")
	eventCount := len(schedules.events)
	replayed, err := coordinator.ExecuteResearchV3(
		t.Context(), userID, input.ActionID, testCreationReceiptTarget)
	if err != nil || !replayed.Replayed ||
		replayed.Status != types.TaskOperationStatusFailed ||
		len(schedules.events) != eventCount {
		t.Fatalf("capacity replay=%+v err=%v before=%d after=%d",
			replayed, err, eventCount, len(schedules.events))
	}
}

func assertNativeV3CreationCleanupPostgreSQL(
	t *testing.T, databaseURL, operationID string,
	tenantID, userID int64, taskID, errorCode string,
) {
	t.Helper()
	conn, err := pgx.Connect(t.Context(), databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(context.WithoutCancel(t.Context())) }()
	var status, phase, gotErrorCode string
	var scheduleCount, authorityCount, receiptCount int
	if err := conn.QueryRow(t.Context(), `
		SELECT operation.status,operation.phase,operation.error_code,
		       (SELECT count(*) FROM schedules
		         WHERE id=$4 AND tenant_id=$2 AND user_id=$3),
		       (SELECT count(*) FROM research_v3_delivery_authorities
		         WHERE task_id=$4 AND tenant_id=$2 AND user_id=$3),
		       (SELECT count(*) FROM task_creation_receipts
		         WHERE operation_id=$1 AND tenant_id=$2 AND user_id=$3)
		  FROM task_creation_operations operation
		 WHERE operation.id=$1 AND operation.tenant_id=$2 AND operation.user_id=$3`,
		operationID, tenantID, userID, taskID).Scan(
		&status, &phase, &gotErrorCode,
		&scheduleCount, &authorityCount, &receiptCount); err != nil {
		t.Fatal(err)
	}
	if status != string(types.TaskOperationStatusFailed) ||
		phase != string(types.TaskCreationPhaseFailed) || gotErrorCode != errorCode ||
		scheduleCount != 0 || authorityCount != 0 || receiptCount != 1 {
		t.Fatalf("cleanup status=%s phase=%s code=%s schedule=%d authority=%d receipt=%d",
			status, phase, gotErrorCode, scheduleCount, authorityCount, receiptCount)
	}
}

func TestCreationCoordinator_NativeV3PostgreSQLStaleRecoveryExecutesLifecycle(t *testing.T) {
	st, tenantID, userID := newCreationCoordinatorPostgreSQLFixture(t)
	schedules := &creationSagaFakeScheduler{}
	coordinator := NewResearchCreationCoordinatorV3(
		st, schedules, nil, testResearchV3CreationPolicy())
	input := testResearchV3CreationInput()
	input.ActionID = "native-v3-stale-recovery-" + uuid.NewString()
	input.UserID = userID
	input.SpecJSON = json.RawMessage(`{"tz":"Asia/Shanghai","every_seconds":3600}`)
	if _, err := coordinator.PrepareResearchV3(t.Context(), input); err != nil {
		t.Fatalf("PrepareResearchV3: %v", err)
	}
	if _, err := st.AcquireResearchTaskCreationOperationV3(t.Context(),
		types.AcquireTaskCreationOperationParams{
			ID: input.ActionID, TenantID: tenantID, UserID: userID,
			LeaseOwner: "crashed-native-v3-worker", LeaseDuration: time.Minute,
			ReceiptProvider: testCreationReceiptTarget.Provider,
			ReceiptTarget:   testCreationReceiptTarget.Target,
		}); err != nil {
		t.Fatalf("acquire crashed native V3 operation: %v", err)
	}
	dbURL := os.Getenv("VANE_TEST_DATABASE_URL")
	if dbURL == "" {
		dbURL = os.Getenv("DATABASE_URL")
	}
	conn, err := pgx.Connect(t.Context(), dbURL)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(t.Context(), `
		UPDATE task_creation_operations
		   SET lease_until=clock_timestamp()-interval '2 hours',
		       takeover_not_before=clock_timestamp()-interval '1 hour'
		 WHERE id=$1 AND tenant_id=$2 AND user_id=$3
		   AND tool_name='manage_tasks' AND execution_version=2`,
		input.ActionID, tenantID, userID); err != nil {
		_ = conn.Close(t.Context())
		t.Fatal(err)
	}
	if err := conn.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.RecoverStaleOnceV3(t.Context()); err != nil {
		t.Fatalf("RecoverStaleOnceV3 native V3: %v", err)
	}
	op, err := st.LoadResearchTaskCreationOperationV3(
		t.Context(), input.ActionID, tenantID, userID)
	if err != nil {
		t.Fatal(err)
	}
	if op.Status != types.TaskOperationStatusExecuted ||
		op.Phase != types.TaskCreationPhaseCompleted || op.TaskID == "" ||
		op.TombstonedAt == nil {
		t.Fatalf("native V3 stale recovery did not converge: %+v events=%v", op, schedules.events)
	}
	for _, want := range []string{"prepare_v3", "ensure", "activate"} {
		found := false
		for _, event := range schedules.events {
			found = found || event == want
		}
		if !found {
			t.Fatalf("native V3 stale recovery missing %q: %v", want, schedules.events)
		}
	}
}

func TestResearchCreationCoordinatorV3PostgreSQLIgnoresStaleV1Journal(t *testing.T) {
	st, tenantID, userID := newCreationCoordinatorPostgreSQLFixture(t)
	schedules := &creationSagaFakeScheduler{}
	legacy := NewCreationCoordinator(st, schedules, nil)
	actionID := "retained-v1-stale-" + uuid.NewString()
	proposal, err := legacy.Prepare(t.Context(), CreationProposalInput{
		ActionID: actionID, UserID: userID,
		RawArgs:   mustCreateProposalArgs(t, "每天检查旧协议任务", "旧协议任务"),
		ExpiresAt: time.Now().Add(time.Hour),
	})
	if err != nil || proposal.ID != actionID {
		t.Fatalf("prepare retained V1 operation=%+v err=%v", proposal, err)
	}
	if _, err := st.AcquireTaskCreationOperation(t.Context(),
		types.AcquireTaskCreationOperationParams{
			ID: actionID, TenantID: tenantID, UserID: userID,
			LeaseOwner: "retained-v1-crashed-worker", LeaseDuration: time.Minute,
			ReceiptProvider: testCreationReceiptTarget.Provider,
			ReceiptTarget:   testCreationReceiptTarget.Target,
		}); err != nil {
		t.Fatalf("acquire retained V1 operation: %v", err)
	}
	dbURL := creationCoordinatorTestDatabaseURL()
	conn, err := pgx.Connect(t.Context(), dbURL)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(t.Context(), `
		UPDATE task_creation_operations
		   SET lease_until=clock_timestamp()-interval '2 hours',
		       takeover_not_before=clock_timestamp()-interval '1 hour'
		 WHERE id=$1 AND tenant_id=$2 AND user_id=$3
		   AND tool_name='create_schedule' AND execution_version=1`,
		actionID, tenantID, userID); err != nil {
		_ = conn.Close(t.Context())
		t.Fatal(err)
	}
	if err := conn.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	production := NewResearchCreationCoordinatorV3(
		st, schedules, nil, testResearchV3CreationPolicy())
	if err := production.RecoverStaleOnceV3(t.Context()); err != nil {
		t.Fatalf("native V3 recovery inspected retained V1 journal: %v", err)
	}
	if len(schedules.events) != 0 {
		t.Fatalf("retained V1 journal caused Temporal mutation: %v", schedules.events)
	}
	op, err := st.LoadTaskCreationOperation(
		t.Context(), actionID, tenantID, userID)
	if err != nil {
		t.Fatal(err)
	}
	if op.ExecutionVersion != types.TaskCreationExecutionVersionV1 ||
		op.Status != types.TaskOperationStatusExecuting ||
		op.LeaseOwner != "retained-v1-crashed-worker" {
		t.Fatalf("retained V1 journal was mutated: %+v", op)
	}
}

func newCreationCoordinatorPostgreSQLFixture(
	t *testing.T,
) (*store.Store, int64, int64) {
	t.Helper()
	dbURL := creationCoordinatorTestDatabaseURL()
	if dbURL == "" {
		t.Skip("未设置 VANE_TEST_DATABASE_URL 或 DATABASE_URL，跳过 Coordinator 真库测试")
	}
	if err := store.Migrate(t.Context(), dbURL); err != nil {
		t.Fatalf("store.Migrate(): %v", err)
	}
	st, err := store.New(t.Context(), dbURL)
	if err != nil {
		t.Fatalf("store.New(): %v", err)
	}
	t.Cleanup(st.Close)

	user, err := st.UpsertUserByOpenID(
		t.Context(), "ou_task_creation_coordinator_"+uuid.NewString(),
		"A5 Coordinator PostgreSQL test",
	)
	if err != nil {
		t.Fatalf("create fixture user: %v", err)
	}
	inviteCode := "task-creation-coordinator-" + uuid.NewString()
	if _, err := st.IssueInvite(t.Context(), inviteCode, nil, 1, nil); err != nil {
		t.Fatalf("create fixture invite: %v", err)
	}
	tenant, err := st.CreateTenantWithInvite(t.Context(), inviteCode, user.ID)
	if err != nil {
		t.Fatalf("create fixture tenant: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if _, err := st.PurgeTenant(ctx, tenant.ID, false); err != nil {
			t.Errorf("purge fixture tenant %d: %v", tenant.ID, err)
			return
		}
		conn, err := pgx.Connect(ctx, dbURL)
		if err != nil {
			t.Errorf("connect for fixture user cleanup: %v", err)
			return
		}
		defer func() {
			if err := conn.Close(ctx); err != nil {
				t.Errorf("close fixture cleanup connection: %v", err)
			}
		}()
		if _, err := conn.Exec(ctx, `DELETE FROM users WHERE id = $1`, user.ID); err != nil {
			t.Errorf("delete fixture user %d: %v", user.ID, err)
		}
	})
	return st, tenant.ID, user.ID
}

func creationCoordinatorTestDatabaseURL() string {
	if databaseURL := os.Getenv("VANE_TEST_DATABASE_URL"); databaseURL != "" {
		return databaseURL
	}
	return os.Getenv("DATABASE_URL")
}

func waitForDueTaskCreationReceipt(
	t *testing.T,
	st *store.Store,
	receipt types.TaskCreationReceipt,
) *types.TaskCreationReceipt {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()

	for {
		// The production due query compares next_attempt_at with the database
		// clock. Poll that same boundary instead of estimating it from the Go
		// process clock, which may be slightly ahead of PostgreSQL on CI hosts.
		due, err := st.ListDueTaskCreationReceipts(
			ctx, receipt.TenantID, time.Now().Add(time.Hour), 100,
		)
		if err != nil {
			t.Fatalf("list due terminal receipts: %v", err)
		}
		for i := range due {
			if due[i].ID == receipt.ID {
				return &due[i]
			}
		}

		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatalf(
				"terminal receipt %d did not become due by database clock: %v",
				receipt.ID, ctx.Err(),
			)
		}
	}
}
