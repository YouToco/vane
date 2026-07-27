package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	temporalworkflow "go.temporal.io/sdk/workflow"

	cardgenpkg "github.com/YouToco/vane/cardgen"
	"github.com/YouToco/vane/config"
	"github.com/YouToco/vane/eventqualifier"
	"github.com/YouToco/vane/feedback"
	vaneFetcher "github.com/YouToco/vane/fetcher"
	"github.com/YouToco/vane/llm"
	"github.com/YouToco/vane/observation"
	"github.com/YouToco/vane/runcontext"
	"github.com/YouToco/vane/runtimeconfig"
	"github.com/YouToco/vane/runtimepolicy"
	scorerpkg "github.com/YouToco/vane/scorer"
	"github.com/YouToco/vane/store"
	"github.com/YouToco/vane/types"
)

const (
	testActivityWorkflowID = "default-test-workflow-id"
	testActivityRunID      = "default-test-run-id"
)

type compiledModelResolverFake struct {
	err    error
	calls  atomic.Int32
	client *llm.Client
}

type compiledRouteFetcherFake struct {
	validateErr   error
	validateCalls atomic.Int32
	fetchCalls    atomic.Int32
}

type countingCompiledFetcher struct {
	inner         *vaneFetcher.Multi
	validateCalls atomic.Int32
	fetchCalls    atomic.Int32
}

func (f *countingCompiledFetcher) Fetch(context.Context, types.Source) ([]types.ContentItem, error) {
	f.fetchCalls.Add(1)
	return nil, errors.New("legacy fetch must not be called during PrepareRun")
}

func (f *countingCompiledFetcher) ValidateRuntimeFetchRouteV1(
	capability runtimepolicy.CapabilityV1,
	source types.Source,
) error {
	f.validateCalls.Add(1)
	return f.inner.ValidateRuntimeFetchRouteV1(capability, source)
}

func (f *countingCompiledFetcher) FetchWithPolicyV1(
	context.Context,
	types.Source,
	runtimepolicy.CapabilityV1,
	func(context.Context) error,
) ([]types.ContentItem, error) {
	f.fetchCalls.Add(1)
	return nil, errors.New("compiled fetch must not be called during PrepareRun")
}

func (*compiledRouteFetcherFake) Fetch(context.Context, types.Source) ([]types.ContentItem, error) {
	return nil, errors.New("legacy fetch must not be called")
}

func (f *compiledRouteFetcherFake) ValidateRuntimeFetchRouteV1(
	runtimepolicy.CapabilityV1,
	types.Source,
) error {
	f.validateCalls.Add(1)
	return f.validateErr
}

func (f *compiledRouteFetcherFake) FetchWithPolicyV1(
	context.Context,
	types.Source,
	runtimepolicy.CapabilityV1,
	func(context.Context) error,
) ([]types.ContentItem, error) {
	f.fetchCalls.Add(1)
	return nil, nil
}

func (f *compiledModelResolverFake) ResolveRuntimeModelPolicyV1(runtimepolicy.ModelPolicyV1) (*llm.Client, error) {
	f.calls.Add(1)
	if f.err != nil {
		return nil, f.err
	}
	if f.client != nil {
		return f.client, nil
	}
	return llm.New(config.LLMConfig{MaxConcurrent: 1}), nil
}

type compiledRunStoreFake struct {
	mu sync.Mutex

	ref      types.RunSnapshotRef
	found    bool
	snapshot runcontext.CompiledSnapshotV1

	loadRefErr      error
	createErr       error
	loadSnapshotErr error
	authorizeErr    error
	quotaErr        error
	authorize       bool
	authorizeScript []bool

	loadRefIdentities      []types.RunIdentity
	createIdentities       []types.RunIdentity
	loadSnapshotIdentities []types.RunIdentity
	authorizeIdentities    []types.RunIdentity
	auditIdentities        []types.RunIdentity
	createPolicies         []runtimepolicy.BundleV1
	createRollouts         []observation.RolloutMode
	shadowCreates          int
	auditResult            store.CompiledRunSnapshotV2AuditResult
	auditErr               error
	auditBlock             bool

	dueSources         []types.Source
	candidates         []types.ContentItem
	attributionID      int64
	attributionOK      bool
	dueSourceIDs       [][]int64
	candidateSourceID  [][]int64
	attributionIDs     [][]int64
	batchWrites        int
	recoveryCalls      int
	deliveryWrites     int
	deliveryIDSequence []int64
	deliveryReceipts   int
	batchStatuses      int
	fetchUpserts       int
	fetchStateWrites   int
	fetchDisables      int
	recoveryOnly       bool
	deliveryReceiptErr error
	authorityWinner    types.PushBatchDeliveryAuthority
	authorityErr       error
	authorityCalls     int
	authorityScopes    []types.PushBatchScope
	authorityDesired   []types.PushBatchDeliveryAuthority
	authorityOrder     *pushAuthorityOrder
}

type pushAuthorityOrder struct {
	claimed         atomic.Bool
	reserveBefore   atomic.Bool
	deliveryBefore  atomic.Bool
	providerBefore  atomic.Bool
	batchDoneBefore atomic.Bool
}

type observationRuntimeStoreFake struct {
	mu sync.Mutex

	status          string
	response        json.RawMessage
	spendCalls      int
	spendRollout    []observation.RolloutMode
	spendRuleNil    []bool
	completeCalls   int
	uncertainCalls  int
	reserveCalls    int
	reserveBatchIDs []int64
	bindBatchIDs    []int64
	bindDeliveryIDs []int64
	authorityOrder  *pushAuthorityOrder
}

func (f *observationRuntimeStoreFake) PrepareObservationQualificationStep(
	context.Context,
	types.RunIdentity,
	types.RunSnapshotRef,
	string,
	string,
) (string, json.RawMessage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.status == "" {
		f.status = store.ObservationStepPrepared
	}
	return f.status, append(json.RawMessage(nil), f.response...), nil
}

func (f *observationRuntimeStoreFake) MarkObservationQualificationSending(
	context.Context,
	types.RunIdentity,
	types.RunSnapshotRef,
	string,
	string,
) error {
	return errors.New("legacy split spend fence must not be called")
}

func (f *observationRuntimeStoreFake) AuthorizeObservationQualificationSpendV1(
	_ context.Context,
	_ types.RunIdentity,
	_ types.RunSnapshotRef,
	_, _ string,
	rollout observation.RolloutMode,
	rule *runtimepolicy.QuotaBucketV1,
	_ float64,
) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.status != store.ObservationStepPrepared {
		return types.ErrConflict
	}
	f.spendCalls++
	f.spendRollout = append(f.spendRollout, rollout)
	f.spendRuleNil = append(f.spendRuleNil, rule == nil)
	f.status = store.ObservationStepSending
	return nil
}

func (f *observationRuntimeStoreFake) CompleteObservationQualificationStep(
	_ context.Context,
	_ types.RunIdentity,
	_ types.RunSnapshotRef,
	_, _ string,
	response json.RawMessage,
) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.status != store.ObservationStepSending {
		return types.ErrConflict
	}
	f.status = store.ObservationStepCompleted
	f.response = append(json.RawMessage(nil), response...)
	f.completeCalls++
	return nil
}

func (f *observationRuntimeStoreFake) MarkObservationQualificationUncertain(
	context.Context,
	types.RunIdentity,
	types.RunSnapshotRef,
	string,
	string,
) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.status = store.ObservationStepUncertain
	f.uncertainCalls++
	return nil
}

func (f *observationRuntimeStoreFake) ReserveObservedEventV1(
	_ context.Context,
	_ types.RunIdentity,
	_ types.RunSnapshotRef,
	batchID int64,
	_ observation.QualifiedEvent,
) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reserveCalls++
	f.reserveBatchIDs = append(f.reserveBatchIDs, batchID)
	if f.authorityOrder != nil && !f.authorityOrder.claimed.Load() {
		f.authorityOrder.reserveBefore.Store(true)
	}
	return true, nil
}

func (f *observationRuntimeStoreFake) BindObservedEventDeliveryV1(
	_ context.Context,
	_ types.RunIdentity,
	_ types.RunSnapshotRef,
	_ string,
	_ string,
	batchID int64,
	deliveryID int64,
) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.bindBatchIDs = append(f.bindBatchIDs, batchID)
	f.bindDeliveryIDs = append(f.bindDeliveryIDs, deliveryID)
	return nil
}

func (*observationRuntimeStoreFake) MarkObservedEventDeliveredV1(
	context.Context,
	types.RunIdentity,
	types.RunSnapshotRef,
	int64,
) error {
	return nil
}

func (f *compiledRunStoreFake) LoadCompiledRunSnapshotRefV1(
	_ context.Context,
	identity types.RunIdentity,
) (types.RunSnapshotRef, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.loadRefIdentities = append(f.loadRefIdentities, identity)
	return f.ref, f.found, f.loadRefErr
}

func (f *compiledRunStoreFake) CreateOrGetCompiledRunSnapshotV1(
	_ context.Context,
	identity types.RunIdentity,
	policy runtimepolicy.BundleV1,
	rollouts ...observation.RolloutMode,
) (types.RunSnapshotRef, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.createIdentities = append(f.createIdentities, identity)
	f.createPolicies = append(f.createPolicies, policy)
	rollout := observation.RolloutOff
	if len(rollouts) == 1 {
		rollout = rollouts[0]
	}
	f.createRollouts = append(f.createRollouts, rollout)
	if f.createErr != nil {
		return types.RunSnapshotRef{}, f.createErr
	}
	ref := mustCompiledRunRef(identity, int64(len(f.createIdentities)))
	f.ref = ref
	f.found = true
	f.snapshot = runcontext.CompiledSnapshotV1{
		Ref:                ref,
		Mode:               types.ExecutionModeCompiled,
		ObservationRollout: rollout,
		Definition:         runcontext.DefinitionV1{TaskID: identity.TaskID, TenantID: identity.TenantID, UserID: identity.UserID, ScopeJSON: json.RawMessage(`{}`)},
		Policy:             policy,
	}
	return ref, nil
}

func (f *compiledRunStoreFake) CreateOrGetCompiledRunSnapshotShadowV2(
	ctx context.Context,
	identity types.RunIdentity,
	policy runtimepolicy.BundleV1,
	rollouts ...observation.RolloutMode,
) (types.RunSnapshotRef, error) {
	f.mu.Lock()
	f.shadowCreates++
	f.mu.Unlock()
	return f.CreateOrGetCompiledRunSnapshotV1(ctx, identity, policy, rollouts...)
}

func (f *compiledRunStoreFake) LoadAuthoritativeCompiledTaskRunSnapshot(
	_ context.Context,
	identity types.RunIdentity,
	ref types.RunSnapshotRef,
) (
	runcontext.CompiledSnapshotV1,
	store.CompiledRunSnapshotAuthority,
	error,
) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.loadSnapshotIdentities = append(f.loadSnapshotIdentities, identity)
	if f.loadSnapshotErr != nil {
		return runcontext.CompiledSnapshotV1{}, "", f.loadSnapshotErr
	}
	if f.snapshot.Ref == (types.RunSnapshotRef{}) {
		if f.snapshot.Definition.TaskID != "" {
			snapshot := cloneCompiledSnapshot(f.snapshot)
			snapshot.Ref = ref
			return snapshot, store.CompiledRunSnapshotAuthorityV1, nil
		}
		return runcontext.CompiledSnapshotV1{
			Ref: ref, Mode: types.ExecutionModeCompiled,
		}, store.CompiledRunSnapshotAuthorityV1, nil
	}
	return cloneCompiledSnapshot(
		f.snapshot), store.CompiledRunSnapshotAuthorityV1, nil
}

type qualifyTwiceWorkflowResult struct {
	First  QualifyEventsResult
	Second QualifyEventsResult
}

func qualifyTwiceTestWorkflow(
	ctx temporalworkflow.Context,
	in QualifyEventsIn,
) (qualifyTwiceWorkflowResult, error) {
	info := temporalworkflow.GetInfo(ctx).WorkflowExecution
	identity := types.RunIdentity{
		TemporalWorkflowID: info.ID,
		TemporalRunID:      info.RunID,
		RunKind:            types.RunSnapshotKindScheduled,
		TenantID:           in.Run.TenantID,
		UserID:             in.UserID,
		TaskID:             in.Run.TaskID,
	}
	in.Run.Snapshot = mustCompiledRunRef(identity, 123)
	activityCtx := temporalworkflow.WithActivityOptions(
		ctx, temporalworkflow.ActivityOptions{
			StartToCloseTimeout: time.Minute,
		})
	var out qualifyTwiceWorkflowResult
	if err := temporalworkflow.ExecuteActivity(
		activityCtx, "QualifyEvents", in,
	).Get(activityCtx, &out.First); err != nil {
		return qualifyTwiceWorkflowResult{}, err
	}
	if err := temporalworkflow.ExecuteActivity(
		activityCtx, "QualifyEvents", in,
	).Get(activityCtx, &out.Second); err != nil {
		return qualifyTwiceWorkflowResult{}, err
	}
	return out, nil
}

func (f *compiledRunStoreFake) AuditCompiledTaskRunSnapshotV2(
	ctx context.Context,
	identity types.RunIdentity,
	_ types.RunSnapshotRef,
) (store.CompiledRunSnapshotV2AuditResult, error) {
	f.mu.Lock()
	f.auditIdentities = append(f.auditIdentities, identity)
	result, err, block := f.auditResult, f.auditErr, f.auditBlock
	f.mu.Unlock()
	if block {
		<-ctx.Done()
		return store.CompiledRunSnapshotV2AuditResult{}, ctx.Err()
	}
	return result, err
}

func (f *compiledRunStoreFake) AuthorizeTaskRunSideEffect(
	_ context.Context,
	identity types.RunIdentity,
	_ types.RunSnapshotRef,
) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.authorizeIdentities = append(f.authorizeIdentities, identity)
	if f.authorizeErr != nil {
		return false, f.authorizeErr
	}
	if len(f.authorizeScript) > 0 {
		decision := f.authorizeScript[0]
		f.authorizeScript = f.authorizeScript[1:]
		return decision, nil
	}
	return f.authorize, nil
}

func (f *compiledRunStoreFake) AuthorizeAndConsumeTaskRunLLMQuotaV1(
	_ context.Context,
	identity types.RunIdentity,
	_ types.RunSnapshotRef,
	_ runtimepolicy.QuotaBucketV1,
	_ float64,
) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.authorizeIdentities = append(f.authorizeIdentities, identity)
	if f.authorizeErr != nil {
		return f.authorizeErr
	}
	if f.quotaErr != nil {
		return f.quotaErr
	}
	if !f.authorize {
		return store.ErrQuotaExceeded
	}
	return nil
}

type compiledQuotaScorerFake struct{ calls atomic.Int32 }

func (*compiledQuotaScorerFake) Score(
	context.Context, int64, types.ContentItem, string, string,
) (float64, error) {
	return 0, errors.New("legacy scorer must not be called")
}

func (f *compiledQuotaScorerFake) ScoreWithPolicyV1(
	ctx context.Context,
	_ int64,
	_ int64,
	_ types.ContentItem,
	_ string,
	_ string,
	_ scorerpkg.PolicyV1,
	beforeSpend func(context.Context, float64) error,
) (float64, error) {
	f.calls.Add(1)
	if err := beforeSpend(ctx, 64); err != nil {
		if errors.Is(err, store.ErrQuotaExceeded) {
			return 0, types.NewAppError(types.CodeQuotaExceeded, "compiled score quota exhausted", err)
		}
		return 0, err
	}
	return 88, nil
}

type compiledQuotaCardGenFake struct{ calls atomic.Int32 }

func (*compiledQuotaCardGenFake) Generate(
	context.Context, int64, types.ScoredItem, string, string,
) (string, error) {
	return "", errors.New("legacy card generator must not be called")
}

func (f *compiledQuotaCardGenFake) GenerateWithPolicyV1(
	ctx context.Context,
	_ int64,
	_ int64,
	_ types.ScoredItem,
	_ string,
	_ string,
	_ cardgenpkg.PolicyV1,
	beforeSpend func(context.Context, float64) error,
) (string, error) {
	f.calls.Add(1)
	if err := beforeSpend(ctx, 64); err != nil {
		if errors.Is(err, store.ErrQuotaExceeded) {
			return "", types.NewAppError(types.CodeQuotaExceeded, "compiled card quota exhausted", err)
		}
		return "", err
	}
	return "body", nil
}

func (f *compiledRunStoreFake) EvolveProfileForTaskRunV1(
	context.Context, types.RunIdentity, types.RunSnapshotRef,
	string, []string, int64, time.Time, int64,
) error {
	return nil
}

func (f *compiledRunStoreFake) AdvanceProfileCursorForTaskRunV1(
	context.Context, types.RunIdentity, types.RunSnapshotRef,
	int64, time.Time, int64,
) error {
	return nil
}

func (f *compiledRunStoreFake) UpsertContentItemForTaskRunV1(
	_ context.Context,
	_ types.RunIdentity,
	_ types.RunSnapshotRef,
	_ int64,
	item *types.ContentItem,
) (int64, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.authorizeErr != nil {
		return 0, false, f.authorizeErr
	}
	if !f.authorize {
		return 0, false, types.NewAppError(types.CodeNotFound,
			"compiled write revoked", nil)
	}
	f.fetchUpserts++
	if item.ID > 0 {
		return item.ID, true, nil
	}
	return int64(f.fetchUpserts), true, nil
}

func (f *compiledRunStoreFake) UpdateSourceFetchStateForTaskRunV1(
	_ context.Context,
	_ types.RunIdentity,
	_ types.RunSnapshotRef,
	_ int64,
	_ time.Time,
	_ time.Time,
	_ int,
) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.authorizeErr != nil {
		return false, f.authorizeErr
	}
	if !f.authorize {
		return false, types.NewAppError(types.CodeNotFound,
			"compiled write revoked", nil)
	}
	f.fetchStateWrites++
	return true, nil
}

func (f *compiledRunStoreFake) DisableSourceIfActiveForTaskRunV1(
	_ context.Context,
	_ types.RunIdentity,
	_ types.RunSnapshotRef,
	_ int64,
) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.authorizeErr != nil {
		return false, f.authorizeErr
	}
	if !f.authorize {
		return false, types.NewAppError(types.CodeNotFound,
			"compiled write revoked", nil)
	}
	f.fetchDisables++
	return true, nil
}

func (f *compiledRunStoreFake) ListRecentSimhashesForTaskRunV1(
	_ context.Context,
	_ types.RunIdentity,
	_ types.RunSnapshotRef,
	_ time.Time,
	_ []int64,
) ([]int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.authorizeErr != nil {
		return nil, f.authorizeErr
	}
	if !f.authorize {
		return nil, types.NewAppError(types.CodeNotFound,
			"compiled read revoked", nil)
	}
	return []int64{}, nil
}

func (f *compiledRunStoreFake) CreatePushBatchForTaskRunV1(
	ctx context.Context,
	identity types.RunIdentity,
	_ types.RunSnapshotRef,
	_ string,
) (int64, error) {
	authorized, err := f.AuthorizeTaskRunSideEffect(ctx, identity, types.RunSnapshotRef{})
	if err != nil {
		return 0, err
	}
	if !authorized {
		return 0, types.NewAppError(types.CodeNotFound, "compiled write revoked", nil)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.batchWrites++
	return 101, nil
}

func (f *compiledRunStoreFake) CreateOrRecoverPushBatchForTaskRunV1(
	ctx context.Context,
	identity types.RunIdentity,
	ref types.RunSnapshotRef,
	idempotencyKey string,
) (int64, bool, error) {
	f.mu.Lock()
	if f.recoveryOnly {
		f.recoveryCalls++
		f.mu.Unlock()
		return 101, true, nil
	}
	f.mu.Unlock()
	id, err := f.CreatePushBatchForTaskRunV1(ctx, identity, ref, idempotencyKey)
	return id, false, err
}

func (f *compiledRunStoreFake) ClaimPushBatchDeliveryAuthority(
	_ context.Context,
	scope types.PushBatchScope,
	desired types.PushBatchDeliveryAuthority,
) (types.PushBatchDeliveryAuthority, error) {
	f.mu.Lock()
	f.authorityCalls++
	f.authorityScopes = append(f.authorityScopes, scope)
	f.authorityDesired = append(f.authorityDesired, desired)
	winner, err, order := f.authorityWinner, f.authorityErr, f.authorityOrder
	f.mu.Unlock()
	if err != nil {
		return "", err
	}
	if winner == "" {
		winner = types.PushBatchDeliveryAuthorityLegacy
	}
	if order != nil {
		order.claimed.Store(true)
	}
	return winner, nil
}

func (f *compiledRunStoreFake) RecordEmptyPushBatchForTaskRunV1(
	ctx context.Context,
	identity types.RunIdentity,
	_ types.RunSnapshotRef,
	_ string,
	_ types.BatchExitGate,
	_ types.PipelineCounts,
) (int64, bool, error) {
	authorized, err := f.AuthorizeTaskRunSideEffect(ctx, identity, types.RunSnapshotRef{})
	if err != nil {
		return 0, false, err
	}
	if !authorized {
		return 0, false, types.NewAppError(types.CodeNotFound, "compiled write revoked", nil)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.batchWrites++
	return 101, false, nil
}

func (f *compiledRunStoreFake) InsertDeliveryForTaskRunV1(
	ctx context.Context,
	identity types.RunIdentity,
	_ types.RunSnapshotRef,
	_ string,
	_ *types.Delivery,
) (int64, bool, bool, error) {
	if f.authorityOrder != nil && !f.authorityOrder.claimed.Load() {
		f.authorityOrder.deliveryBefore.Store(true)
	}
	authorized, err := f.AuthorizeTaskRunSideEffect(ctx, identity, types.RunSnapshotRef{})
	if err != nil {
		return 0, false, false, err
	}
	if !authorized {
		return 0, false, false,
			types.NewAppError(types.CodeNotFound, "compiled write revoked", nil)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deliveryWrites++
	if len(f.deliveryIDSequence) > 0 {
		id := f.deliveryIDSequence[0]
		f.deliveryIDSequence = f.deliveryIDSequence[1:]
		return id, false, false, nil
	}
	return 201, false, false, nil
}

func (f *compiledRunStoreFake) UpdatePushBatchStatusForTaskRunV1(
	ctx context.Context,
	identity types.RunIdentity,
	_ types.RunSnapshotRef,
	_ string,
	_ int64,
	_ types.BatchStatus,
) error {
	authorized, err := f.AuthorizeTaskRunSideEffect(ctx, identity, types.RunSnapshotRef{})
	if err != nil {
		return err
	}
	if !authorized {
		return types.NewAppError(types.CodeNotFound, "compiled write revoked", nil)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.batchStatuses++
	return nil
}

func (f *compiledRunStoreFake) MarkPushBatchDoneReceiptV1(
	_ context.Context,
	_ types.RunIdentity,
	_ types.RunSnapshotRef,
	_ string,
	_ int64,
) error {
	if f.authorityOrder != nil && !f.authorityOrder.claimed.Load() {
		f.authorityOrder.batchDoneBefore.Store(true)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.batchStatuses++
	return nil
}

func (f *compiledRunStoreFake) MarkDeliverySentForTaskRunV1(
	_ context.Context,
	_ types.RunIdentity,
	_ types.RunSnapshotRef,
	_ string,
	_ int64,
	_ int64,
	_ string,
	_ json.RawMessage,
	_ time.Time,
) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deliveryReceipts++
	return f.deliveryReceiptErr
}

func (f *compiledRunStoreFake) ListDueSourcesByIDs(_ context.Context, ids []int64) ([]types.Source, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.dueSourceIDs = append(f.dueSourceIDs, append([]int64(nil), ids...))
	return append([]types.Source(nil), f.dueSources...), nil
}

func (f *compiledRunStoreFake) ListUnpushedForTaskRunV1(
	_ context.Context,
	_ types.RunIdentity,
	_ types.RunSnapshotRef,
	ids []int64,
	_, _ int,
) ([]types.ContentItem, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.candidateSourceID = append(f.candidateSourceID, append([]int64(nil), ids...))
	return append([]types.ContentItem(nil), f.candidates...), nil
}

func (f *compiledRunStoreFake) SourceForContentFromIDs(
	_ context.Context,
	_ int64,
	ids []int64,
) (int64, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.attributionIDs = append(f.attributionIDs, append([]int64(nil), ids...))
	return f.attributionID, f.attributionOK, nil
}

func (f *compiledRunStoreFake) counts() (loadRef, create, loadSnapshot, authorize int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.loadRefIdentities), len(f.createIdentities), len(f.loadSnapshotIdentities), len(f.authorizeIdentities)
}

func cloneCompiledSnapshot(in runcontext.CompiledSnapshotV1) runcontext.CompiledSnapshotV1 {
	out := in
	out.Definition.ScopeJSON = append(json.RawMessage(nil), in.Definition.ScopeJSON...)
	out.Definition.SpecJSON = append(json.RawMessage(nil), in.Definition.SpecJSON...)
	out.Definition.FetchPlan = append(json.RawMessage(nil), in.Definition.FetchPlan...)
	out.Definition.Sources = append([]runcontext.SourceV1(nil), in.Definition.Sources...)
	for i := range out.Definition.Sources {
		out.Definition.Sources[i].Config = append(json.RawMessage(nil), in.Definition.Sources[i].Config...)
	}
	return out
}

func mustCompiledRunRef(identity types.RunIdentity, snapshotID int64) types.RunSnapshotRef {
	ref, err := (types.RunSnapshotRef{
		SchemaVersion:      types.RunSnapshotSchemaVersion,
		SnapshotID:         snapshotID,
		TemporalWorkflowID: identity.TemporalWorkflowID,
		TemporalRunID:      identity.TemporalRunID,
		RunKind:            identity.RunKind,
		TenantID:           identity.TenantID,
		UserID:             identity.UserID,
		TaskID:             identity.TaskID,
		Mode:               types.ExecutionModeCompiled,
		DefinitionDigest:   workflowSnapshotTestDigest,
		PlanDigest:         workflowSnapshotTestDigest,
		Policy: types.RuntimePolicyDigests{
			CapabilityCatalogDigest: workflowSnapshotTestDigest,
			ToolPolicyDigest:        workflowSnapshotTestDigest,
			PromptPolicyDigest:      workflowSnapshotTestDigest,
			ModelPolicyDigest:       workflowSnapshotTestDigest,
			QuotaPolicyDigest:       workflowSnapshotTestDigest,
		},
		PayloadDigest: workflowSnapshotTestDigest,
	}).Seal()
	if err != nil {
		panic(err)
	}
	return ref
}

func testActivityIdentity(tenantID, userID int64, taskID string) types.RunIdentity {
	return types.RunIdentity{
		TemporalWorkflowID: testActivityWorkflowID,
		TemporalRunID:      testActivityRunID,
		RunKind:            types.RunSnapshotKindScheduled,
		TenantID:           tenantID,
		UserID:             userID,
		TaskID:             taskID,
	}
}

func executePrepareRun(
	t *testing.T,
	env *testsuite.TestActivityEnvironment,
	a *Activities,
	p PushParams,
) (PrepareRunResult, error) {
	t.Helper()
	encoded, err := env.ExecuteActivity(a.PrepareRun, p)
	if err != nil {
		return PrepareRunResult{}, err
	}
	var result PrepareRunResult
	if err := encoded.Get(&result); err != nil {
		t.Fatalf("decode PrepareRun result: %v", err)
	}
	return result, nil
}

func exaCompiledCapability(capability types.Capability, generation int64) runtimepolicy.CapabilityV1 {
	kind := types.KindArticle
	if capability == types.CapContents {
		kind = types.KindPageContent
	}
	return runtimepolicy.CapabilityV1{
		Platform: string(types.PlatformWeb), Capability: string(capability), Kind: string(kind),
		ImplementationVersion: runtimepolicy.CapabilityImplementationExaV1,
		CredentialRef: runtimepolicy.CredentialRefV1{
			ID: runtimepolicy.CredentialIDExaPrimaryV1, Generation: generation,
		},
	}
}

func rssCompiledCapability(exaGeneration int64) runtimepolicy.CapabilityV1 {
	return runtimepolicy.CapabilityV1{
		Platform: string(types.PlatformWeb), Capability: string(types.CapFeed), Kind: string(types.KindArticle),
		ImplementationVersion: runtimepolicy.CapabilityImplementationRSSV1,
		DependencyCredentialRefs: []runtimepolicy.CredentialRefV1{{
			ID: runtimepolicy.CredentialIDExaPrimaryV1, Generation: exaGeneration,
		}},
	}
}

func bindingCompiledCapability(
	platform types.Platform,
	capability types.Capability,
	generation int64,
) runtimepolicy.CapabilityV1 {
	return runtimepolicy.CapabilityV1{
		Platform: string(platform), Capability: string(capability), Kind: string(types.KindArticle),
		ImplementationVersion: runtimepolicy.CapabilityImplementationBindingV1,
		CredentialRef: runtimepolicy.CredentialRefV1{
			ID: runtimepolicy.CredentialIDTikHubPrimaryV1, Generation: generation,
		},
	}
}

func compiledRouteSnapshot(
	identity types.RunIdentity,
	ref types.RunSnapshotRef,
	sources []runcontext.SourceV1,
	capabilities []runtimepolicy.CapabilityV1,
) runcontext.CompiledSnapshotV1 {
	return runcontext.CompiledSnapshotV1{
		Ref: ref, Mode: types.ExecutionModeCompiled,
		Definition: runcontext.DefinitionV1{
			TaskID: identity.TaskID, TenantID: identity.TenantID, UserID: identity.UserID,
			ScopeJSON: json.RawMessage(`{}`), Sources: sources,
		},
		Policy: runtimepolicy.BundleV1{
			CapabilityCatalog: runtimepolicy.CapabilityCatalogV1{
				SchemaVersion: runtimepolicy.CapabilityCatalogSchemaVersionV1,
				Allowed:       capabilities,
			},
		},
	}
}

func TestPrepareRun_CreatesThenRecoversExactReferenceBeforeCurrentState(t *testing.T) {
	identity := testActivityIdentity(7, 9, "task-compiled")
	compiledStore := &compiledRunStoreFake{authorize: true}
	resolver := new(compiledModelResolverFake)
	var builderCalls atomic.Int32
	var builderErr atomic.Pointer[error]
	builder := func(context.Context, int64, bool) (runtimepolicy.BundleV1, error) {
		builderCalls.Add(1)
		if ptr := builderErr.Load(); ptr != nil {
			return runtimepolicy.BundleV1{}, *ptr
		}
		return runtimepolicy.BundleV1{SchemaVersion: "first-policy"}, nil
	}
	a := NewActivities(new(compiledRouteFetcherFake), fakeScorer{}, fakeCardGen{}, &fakePusher{}, &fakeStore{}, fakeFeishu{}, nil, nil, nil, nil,
		WithCompiledRuntimeV1(compiledStore, builder, resolver),
		WithObservationRuntime(nil, nil, identity.TaskID, ""))
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	env.RegisterActivity(a.PrepareRun)
	p := PushParams{TenantID: identity.TenantID, UserID: identity.UserID,
		RunKind: PushRunKindScheduled, ExecutionMode: types.ExecutionModeCompiled,
		RuntimeVersion: CompiledRuntimeSnapshotV1, ScheduleID: identity.TaskID}

	first, err := executePrepareRun(t, env, a, p)
	if err != nil {
		t.Fatalf("initial PrepareRun failed: %v", err)
	}
	if !first.Authorized || first.Snapshot.Identity() != identity {
		t.Fatalf("initial result = %+v, want authorized exact identity %+v", first, identity)
	}
	loadRef, create, loadSnapshot, authorize := compiledStore.counts()
	if loadRef != 1 || create != 1 || loadSnapshot != 1 || authorize != 1 || builderCalls.Load() != 1 {
		t.Fatalf("initial call sequence loadRef=%d create=%d load=%d auth=%d build=%d", loadRef, create, loadSnapshot, authorize, builderCalls.Load())
	}
	compiledStore.mu.Lock()
	if len(compiledStore.createRollouts) != 1 ||
		compiledStore.createRollouts[0] != observation.RolloutShadow ||
		compiledStore.snapshot.ObservationRollout != observation.RolloutShadow {
		t.Fatalf("initial rollout was not frozen as shadow: create=%v snapshot=%q",
			compiledStore.createRollouts,
			compiledStore.snapshot.ObservationRollout)
	}
	compiledStore.mu.Unlock()

	// Simulate both current task deletion and an invalid new deployment policy:
	// Create would now fail and the builder is unavailable. Recovery must load
	// the exact already-committed ref before consulting either current input.
	compiledStore.mu.Lock()
	compiledStore.createErr = errors.New("task no longer exists")
	compiledStore.mu.Unlock()
	currentErr := errors.New("current policy is broken")
	builderErr.Store(&currentErr)
	// Simulate worker restart/config toggle to authority. Recovery must not
	// recalculate rollout for this already-committed Temporal run.
	a.observationShadowCanaryTaskID = ""
	a.observationAuthorityCanaryTaskID = identity.TaskID

	recovered, err := executePrepareRun(t, env, a, p)
	if err != nil {
		t.Fatalf("exact-ref recovery failed after current state disappeared: %v", err)
	}
	if recovered != first {
		t.Fatalf("recovered result drifted\nfirst:     %+v\nrecovered: %+v", first, recovered)
	}
	loadRef, create, loadSnapshot, authorize = compiledStore.counts()
	if loadRef != 2 || create != 1 || loadSnapshot != 2 || authorize != 2 || builderCalls.Load() != 1 {
		t.Fatalf("recovery consulted current state: loadRef=%d create=%d load=%d auth=%d build=%d", loadRef, create, loadSnapshot, authorize, builderCalls.Load())
	}
	compiledStore.mu.Lock()
	defer compiledStore.mu.Unlock()
	if compiledStore.snapshot.ObservationRollout != observation.RolloutShadow ||
		len(compiledStore.createRollouts) != 1 {
		t.Fatalf("recovery changed frozen rollout: snapshot=%q creates=%v",
			compiledStore.snapshot.ObservationRollout,
			compiledStore.createRollouts)
	}
}

func TestQualifyEvents_ShadowUsesFrozenRolloutCallsOnceAndReturnsCandidates(
	t *testing.T,
) {
	const (
		taskID     = "task-observation-shadow"
		workflowID = "wf-task-observation-shadow-2026-07-25T00:00:00Z"
	)
	var upstreamCalls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(
		w http.ResponseWriter, _ *http.Request,
	) {
		upstreamCalls.Add(1)
		_, _ = w.Write([]byte(`{
			"id":"shadow-call",
			"model":"shadow-model",
			"choices":[{"message":{"content":"{\"outcome\":\"no_match\",\"events\":[]}"}}],
			"usage":{"prompt_tokens":10,"completion_tokens":4}
		}`))
	}))
	t.Cleanup(srv.Close)
	modelClient := llm.New(config.LLMConfig{
		Provider: "deepseek", BaseURL: srv.URL,
		APIKey: "test", MaxConcurrent: 1,
	})

	policy, err := observation.Compile(observation.PolicySpecV1{
		Schema: observation.SchemaV1,
		Mode:   observation.ModeEvent,
		Window: observation.WindowSpecV1{
			Kind:                   observation.WindowRollingDuration,
			RollingDurationSeconds: 86_400,
		},
		LatePolicy: observation.LateStrict,
		Evidence: observation.EvidencePolicyV1{
			Requirement:     observation.EvidenceOfficialRequired,
			OfficialDomains: []string{"openai.com"},
		},
		UnknownTime: observation.UnknownTimeReject,
		Event: &observation.EventPolicyV1{
			Subject: "OpenAI models", EventKind: "model_release",
			Qualification: observation.QualificationGeneralAvailability,
		},
		QualifierPrompt: observation.QualifierPromptV1,
	}, time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	scopeJSON, err := json.Marshal(struct {
		Observation observation.PolicyV1 `json:"observation"`
	}{Observation: policy})
	if err != nil {
		t.Fatal(err)
	}
	modelPolicy := runtimepolicy.ModelPolicyV1{
		Calls: []runtimepolicy.ModelCallV1{{
			Stage: runtimepolicy.ModelStageCardGen,
			Model: "shadow-model", MaxTokens: 256, DisableThinking: true,
		}},
	}
	quotaPolicy := runtimepolicy.QuotaPolicyV1{
		Buckets: []runtimepolicy.QuotaBucketV1{{
			Name: "llm_tokens", Financial: true,
			EnforcementVersion: runtimepolicy.QuotaEnforcementLLMPrechargeV1,
		}},
	}
	compiledStore := &compiledRunStoreFake{
		snapshot: runcontext.CompiledSnapshotV1{
			Mode:               types.ExecutionModeCompiled,
			ObservationRollout: observation.RolloutShadow,
			Definition: runcontext.DefinitionV1{
				TaskID: taskID, TenantID: 7, UserID: 9,
				SpecJSON:  json.RawMessage(`{"every_seconds":86400,"anchor_at":"2026-07-01T00:00:00Z","tz":"UTC"}`),
				ScopeJSON: scopeJSON,
			},
			Policy: runtimepolicy.BundleV1{
				ModelPolicy: modelPolicy,
				QuotaPolicy: quotaPolicy,
			},
		},
	}
	observationStore := new(observationRuntimeStoreFake)
	resolver := &compiledModelResolverFake{client: modelClient}
	activities := NewActivities(
		new(compiledRouteFetcherFake), fakeScorer{}, fakeCardGen{},
		&fakePusher{}, &fakeStore{}, fakeFeishu{}, nil, nil, nil, nil,
		WithCompiledRuntimeV1(compiledStore, nil, resolver),
		// Live config has advanced to authority. The existing run remains
		// shadow because QualifyEvents reads only the immutable snapshot.
		WithObservationRuntime(
			observationStore, eventqualifier.New(nil), taskID, taskID),
	)

	published := time.Date(2026, 7, 24, 23, 0, 0, 0, time.UTC)
	candidates := []types.ContentItem{{
		ID: 77, Title: "candidate", URL: "https://openai.com/index/release",
		Content: "OpenAI announced availability.", PublishedAt: &published,
	}}
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: workflowID})
	env.RegisterWorkflow(qualifyTwiceTestWorkflow)
	env.RegisterActivity(activities.QualifyEvents)
	env.ExecuteWorkflow(qualifyTwiceTestWorkflow, QualifyEventsIn{
		UserID: 9, TraceID: "shadow-trace", ScheduleID: taskID,
		Items: candidates,
		Run:   &CompiledRunInputV1{TenantID: 7, TaskID: taskID},
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("shadow qualification workflow: %v", err)
	}
	var result qualifyTwiceWorkflowResult
	if err := env.GetWorkflowResult(&result); err != nil {
		t.Fatal(err)
	}
	for _, got := range []QualifyEventsResult{result.First, result.Second} {
		if !reflect.DeepEqual(got.Items, candidates) ||
			got.Outcome != "shadow_no_match" {
			t.Fatalf("shadow result=%+v, want original candidates/no_match", got)
		}
	}
	if upstreamCalls.Load() != 1 {
		t.Fatalf("shadow qualifier upstream calls=%d, want 1 across retry",
			upstreamCalls.Load())
	}
	observationStore.mu.Lock()
	defer observationStore.mu.Unlock()
	if observationStore.spendCalls != 1 ||
		observationStore.completeCalls != 1 ||
		len(observationStore.spendRollout) != 1 ||
		observationStore.spendRollout[0] != observation.RolloutShadow ||
		!observationStore.spendRuleNil[0] {
		t.Fatalf("shadow spend fence drifted: %+v", observationStore)
	}
}

func TestQualifyEventCandidates_InvalidSemanticResultIsPersistedUncertain(
	t *testing.T,
) {
	const taskID = "task-observation-invalid-semantic"
	var upstreamCalls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(
		w http.ResponseWriter, _ *http.Request,
	) {
		upstreamCalls.Add(1)
		_, _ = w.Write([]byte(`{
			"id":"invalid-semantic-call",
			"model":"shadow-model",
			"choices":[{"message":{"content":"{\"outcome\":\"match\",\"events\":[{\"event_type\":\"model_release\",\"subject\":\"OpenAI models\",\"release_identifier\":\"gpt-invalid\",\"occurred_at\":\"2026-07-24T23:00:00Z\",\"qualification\":\"general_availability\",\"evidence_content_ids\":[999],\"reason\":\"not a supplied candidate\"}]}"}}],
			"usage":{"prompt_tokens":10,"completion_tokens":4}
		}`))
	}))
	t.Cleanup(srv.Close)
	modelClient := llm.New(config.LLMConfig{
		Provider: "deepseek", BaseURL: srv.URL,
		APIKey: "test", MaxConcurrent: 1,
	})
	policy, err := observation.Compile(observation.PolicySpecV1{
		Schema: observation.SchemaV1,
		Mode:   observation.ModeEvent,
		Window: observation.WindowSpecV1{
			Kind:                   observation.WindowRollingDuration,
			RollingDurationSeconds: 86_400,
		},
		LatePolicy: observation.LateStrict,
		Evidence: observation.EvidencePolicyV1{
			Requirement:     observation.EvidenceOfficialRequired,
			OfficialDomains: []string{"openai.com"},
		},
		UnknownTime: observation.UnknownTimeReject,
		Event: &observation.EventPolicyV1{
			Subject: "OpenAI models", EventKind: "model_release",
			Qualification: observation.QualificationGeneralAvailability,
		},
		QualifierPrompt: observation.QualifierPromptV1,
	}, time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	identity := types.RunIdentity{
		TemporalWorkflowID: "wf-" + taskID + "-2026-07-25T00:00:00Z",
		TemporalRunID:      "run-invalid-semantic",
		RunKind:            types.RunSnapshotKindScheduled,
		TenantID:           7,
		UserID:             9,
		TaskID:             taskID,
	}
	ref := mustCompiledRunRef(identity, 501)
	observationStore := new(observationRuntimeStoreFake)
	activities := NewActivities(
		new(compiledRouteFetcherFake), fakeScorer{}, fakeCardGen{},
		&fakePusher{}, &fakeStore{}, fakeFeishu{}, nil, nil, nil, nil,
		WithCompiledRuntimeV1(nil, nil,
			&compiledModelResolverFake{client: modelClient}),
		WithObservationRuntime(
			observationStore, eventqualifier.New(nil), taskID, ""),
	)
	published := time.Date(2026, 7, 24, 23, 0, 0, 0, time.UTC)
	items := []types.ContentItem{{
		ID: 77, Title: "candidate", URL: "https://openai.com/index/release",
		Content: "OpenAI announced availability.", PublishedAt: &published,
	}}
	got, outcome, err := activities.qualifyEventCandidates(
		t.Context(),
		identity,
		runcontext.CompiledSnapshotV1{
			Ref: ref, Mode: types.ExecutionModeCompiled,
			ObservationRollout: observation.RolloutShadow,
			Policy: runtimepolicy.BundleV1{
				ModelPolicy: runtimepolicy.ModelPolicyV1{
					Calls: []runtimepolicy.ModelCallV1{{
						Stage: runtimepolicy.ModelStageCardGen,
						Model: "shadow-model", MaxTokens: 256,
						DisableThinking: true,
					}},
				},
			},
		},
		QualifyEventsIn{
			UserID: 9, TraceID: "invalid-semantic-trace",
			ScheduleID: taskID, Items: items,
			Run: &CompiledRunInputV1{
				TenantID: 7, TaskID: taskID, Snapshot: ref,
			},
		},
		observation.RolloutShadow,
		policy,
		observation.Window{
			Start: time.Date(2026, 7, 24, 0, 0, 0, 0, time.UTC),
			End:   time.Date(2026, 7, 25, 0, 0, 0, 0, time.UTC),
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if got != nil || outcome != "uncertain" {
		t.Fatalf("invalid semantic result=(%v,%q), want nil/uncertain",
			got, outcome)
	}
	observationStore.mu.Lock()
	defer observationStore.mu.Unlock()
	if upstreamCalls.Load() != 1 ||
		observationStore.spendCalls != 1 ||
		observationStore.completeCalls != 0 ||
		observationStore.uncertainCalls != 1 ||
		observationStore.status != store.ObservationStepUncertain {
		t.Fatalf("invalid semantic audit drifted: upstream=%d store=%+v",
			upstreamCalls.Load(), observationStore)
	}
}

func TestQualifyEvents_ContentShadowUsesSnapshotWhenLiveConfigIsOff(
	t *testing.T,
) {
	const (
		taskID     = "task-observation-content"
		workflowID = "wf-task-observation-content-2026-07-25T00:00:00Z"
	)
	policy, err := observation.Compile(observation.PolicySpecV1{
		Schema: observation.SchemaV1,
		Mode:   observation.ModeContent,
		Window: observation.WindowSpecV1{
			Kind:                   observation.WindowRollingDuration,
			RollingDurationSeconds: 3_600,
		},
		LatePolicy: observation.LateStrict,
		Evidence: observation.EvidencePolicyV1{
			Requirement: observation.EvidenceTrustedAllowed,
		},
		UnknownTime: observation.UnknownTimeReject,
	}, time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	scopeJSON, err := json.Marshal(struct {
		Observation observation.PolicyV1 `json:"observation"`
	}{Observation: policy})
	if err != nil {
		t.Fatal(err)
	}
	compiledStore := &compiledRunStoreFake{
		snapshot: runcontext.CompiledSnapshotV1{
			Mode:               types.ExecutionModeCompiled,
			ObservationRollout: observation.RolloutShadow,
			Definition: runcontext.DefinitionV1{
				TaskID: taskID, TenantID: 7, UserID: 9,
				SpecJSON:  json.RawMessage(`{"every_seconds":3600,"anchor_at":"2026-07-01T00:00:00Z","tz":"UTC"}`),
				ScopeJSON: scopeJSON,
			},
		},
	}
	observationStore := new(observationRuntimeStoreFake)
	activities := NewActivities(
		new(compiledRouteFetcherFake), fakeScorer{}, fakeCardGen{},
		&fakePusher{}, &fakeStore{}, fakeFeishu{}, nil, nil, nil, nil,
		WithCompiledRuntimeV1(compiledStore, nil, nil),
		// Current worker rollout is fully off.
		WithObservationRuntime(observationStore, nil, "", ""),
	)
	old := time.Date(2026, 7, 20, 0, 0, 0, 0, time.UTC)
	candidates := []types.ContentItem{{
		ID: 88, Title: "old candidate",
		URL: "https://example.com/old", PublishedAt: &old,
	}}
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: workflowID})
	env.RegisterWorkflow(qualifyTwiceTestWorkflow)
	env.RegisterActivity(activities.QualifyEvents)
	env.ExecuteWorkflow(qualifyTwiceTestWorkflow, QualifyEventsIn{
		UserID: 9, TraceID: "content-shadow", ScheduleID: taskID,
		Items: candidates,
		Run:   &CompiledRunInputV1{TenantID: 7, TaskID: taskID},
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("content shadow workflow: %v", err)
	}
	var result qualifyTwiceWorkflowResult
	if err := env.GetWorkflowResult(&result); err != nil {
		t.Fatal(err)
	}
	for _, got := range []QualifyEventsResult{result.First, result.Second} {
		if !reflect.DeepEqual(got.Items, candidates) ||
			got.Outcome != "shadow_no_match" {
			t.Fatalf("content shadow result=%+v", got)
		}
	}
}

func TestPrepareRun_SnapshotV2ShadowUsesExactCanaryAndV1Wire(t *testing.T) {
	identity := testActivityIdentity(7, 9, "task-shadow-canary")
	compiledStore := &compiledRunStoreFake{
		authorize: true,
		auditErr:  errors.New("injected observation failure"),
	}
	resolver := new(compiledModelResolverFake)
	builder := func(context.Context, int64, bool) (runtimepolicy.BundleV1, error) {
		return runtimepolicy.BundleV1{SchemaVersion: "shadow-policy"}, nil
	}
	a := NewActivities(new(compiledRouteFetcherFake), fakeScorer{}, fakeCardGen{},
		&fakePusher{}, &fakeStore{}, fakeFeishu{}, nil, nil, nil, nil,
		WithCompiledRuntimeV1(compiledStore, builder, resolver),
		WithSnapshotV2ShadowCanary(compiledStore, identity.TaskID),
		WithSnapshotV2ReadAuditCanary(compiledStore, identity.TaskID))
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	env.RegisterActivity(a.PrepareRun)
	p := PushParams{
		TenantID: identity.TenantID, UserID: identity.UserID,
		RunKind: PushRunKindScheduled, ExecutionMode: types.ExecutionModeCompiled,
		RuntimeVersion: CompiledRuntimeSnapshotV1, ScheduleID: identity.TaskID,
	}
	result, err := executePrepareRun(t, env, a, p)
	if err != nil || !result.Authorized ||
		result.Snapshot.SchemaVersion != types.RunSnapshotSchemaVersion {
		t.Fatalf("shadow PrepareRun = %+v, %v", result, err)
	}
	compiledStore.mu.Lock()
	shadowCreates := compiledStore.shadowCreates
	auditCalls := len(compiledStore.auditIdentities)
	compiledStore.mu.Unlock()
	if shadowCreates != 1 {
		t.Fatalf("shadow creates = %d, want 1", shadowCreates)
	}
	if auditCalls != 1 {
		t.Fatalf("audit calls = %d, want 1", auditCalls)
	}
}

func TestPrepareRun_SnapshotV2ReadAuditIsExactAndAfterV1Authorization(t *testing.T) {
	tests := []struct {
		name      string
		canaryID  string
		authorize bool
		loadErr   error
		wantCalls int
	}{
		{name: "exact authorized", canaryID: "task-read-audit", authorize: true, wantCalls: 1},
		{name: "outside exact task", canaryID: "task-other", authorize: true},
		{name: "unauthorized", canaryID: "task-read-audit"},
		{
			name: "v1 load failed", canaryID: "task-read-audit", authorize: true,
			loadErr: errors.New("injected v1 read failure"),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			identity := testActivityIdentity(7, 9, "task-read-audit")
			compiledStore := &compiledRunStoreFake{
				authorize: test.authorize, loadSnapshotErr: test.loadErr,
				auditResult: store.CompiledRunSnapshotV2AuditResult{
					Status: store.CompiledRunSnapshotV2AuditMatch, TypedEqual: true,
				},
			}
			a := NewActivities(
				new(compiledRouteFetcherFake), fakeScorer{}, fakeCardGen{},
				&fakePusher{}, &fakeStore{}, fakeFeishu{}, nil, nil, nil, nil,
				WithCompiledRuntimeV1(
					compiledStore,
					func(context.Context, int64, bool) (runtimepolicy.BundleV1, error) {
						return runtimepolicy.BundleV1{SchemaVersion: "audit-policy"}, nil
					},
					new(compiledModelResolverFake)),
				WithSnapshotV2ReadAuditCanary(compiledStore, test.canaryID),
			)
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestActivityEnvironment()
			env.RegisterActivity(a.PrepareRun)
			_, _ = executePrepareRun(t, env, a, PushParams{
				TenantID: identity.TenantID, UserID: identity.UserID,
				RunKind: PushRunKindScheduled, ExecutionMode: types.ExecutionModeCompiled,
				RuntimeVersion: CompiledRuntimeSnapshotV1, ScheduleID: identity.TaskID,
			})
			compiledStore.mu.Lock()
			got := len(compiledStore.auditIdentities)
			compiledStore.mu.Unlock()
			if got != test.wantCalls {
				t.Fatalf("audit calls = %d, want %d", got, test.wantCalls)
			}
		})
	}
}

func TestPrepareRun_SnapshotV2ReadAuditTimeoutKeepsV1Result(t *testing.T) {
	identity := testActivityIdentity(7, 9, "task-read-audit-timeout")
	compiledStore := &compiledRunStoreFake{
		authorize: true, auditBlock: true,
	}
	a := NewActivities(
		new(compiledRouteFetcherFake), fakeScorer{}, fakeCardGen{},
		&fakePusher{}, &fakeStore{}, fakeFeishu{}, nil, nil, nil, nil,
		WithCompiledRuntimeV1(
			compiledStore,
			func(context.Context, int64, bool) (runtimepolicy.BundleV1, error) {
				return runtimepolicy.BundleV1{SchemaVersion: "audit-timeout-policy"}, nil
			},
			new(compiledModelResolverFake)),
		WithSnapshotV2ReadAuditCanary(compiledStore, identity.TaskID),
	)
	a.snapshotV2ReadAuditTimeout = 20 * time.Millisecond
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	env.RegisterActivity(a.PrepareRun)
	started := time.Now()
	result, err := executePrepareRun(t, env, a, PushParams{
		TenantID: identity.TenantID, UserID: identity.UserID,
		RunKind: PushRunKindScheduled, ExecutionMode: types.ExecutionModeCompiled,
		RuntimeVersion: CompiledRuntimeSnapshotV1, ScheduleID: identity.TaskID,
	})
	if err != nil || !result.Authorized {
		t.Fatalf("v1 result changed by audit timeout: %+v, %v", result, err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("audit timeout was not bounded: %v", elapsed)
	}
}

func TestPrepareRun_IdentityAndAuthorizationFailClosed(t *testing.T) {
	t.Run("revoked before first snapshot is a normal denial", func(t *testing.T) {
		compiledStore := &compiledRunStoreFake{
			createErr: types.NewAppError(types.CodeNotFound,
				"compiled task is no longer live", nil),
		}
		resolver := new(compiledModelResolverFake)
		a := NewActivities(new(compiledRouteFetcherFake), fakeScorer{}, fakeCardGen{}, &fakePusher{}, &fakeStore{}, fakeFeishu{}, nil, nil, nil, nil,
			WithCompiledRuntimeV1(compiledStore,
				func(context.Context, int64, bool) (runtimepolicy.BundleV1, error) {
					return runtimepolicy.BundleV1{SchemaVersion: "first-policy"}, nil
				}, resolver))
		var suite testsuite.WorkflowTestSuite
		env := suite.NewTestActivityEnvironment()
		env.RegisterActivity(a.PrepareRun)
		result, err := executePrepareRun(t, env, a, PushParams{
			TenantID: 7, UserID: 9, RunKind: PushRunKindScheduled,
			ExecutionMode: types.ExecutionModeCompiled, RuntimeVersion: CompiledRuntimeSnapshotV1,
			ScheduleID: "task-revoked-before-snapshot",
		})
		if err != nil {
			t.Fatalf("revocation before first snapshot should be a normal terminal: %v", err)
		}
		if result != (PrepareRunResult{}) {
			t.Fatalf("denied result leaked snapshot material: %+v", result)
		}
		loadRef, create, loadSnapshot, authorize := compiledStore.counts()
		if loadRef != 1 || create != 1 || loadSnapshot != 0 || authorize != 0 {
			t.Fatalf("revoked call sequence loadRef=%d create=%d load=%d auth=%d", loadRef, create, loadSnapshot, authorize)
		}
	})

	t.Run("authorization denial exposes no reusable reference", func(t *testing.T) {
		identity := testActivityIdentity(7, 9, "task-denied")
		ref := mustCompiledRunRef(identity, 71)
		compiledStore := &compiledRunStoreFake{
			ref: ref, found: true, authorize: false,
			snapshot: runcontext.CompiledSnapshotV1{Ref: ref, Mode: types.ExecutionModeCompiled},
		}
		a := NewActivities(new(compiledRouteFetcherFake), fakeScorer{}, fakeCardGen{}, &fakePusher{}, &fakeStore{}, fakeFeishu{}, nil, nil, nil, nil,
			WithCompiledRuntimeV1(compiledStore,
				func(context.Context, int64, bool) (runtimepolicy.BundleV1, error) {
					t.Fatal("existing ref must bypass current builder")
					return runtimepolicy.BundleV1{}, nil
				}, new(compiledModelResolverFake)))
		var suite testsuite.WorkflowTestSuite
		env := suite.NewTestActivityEnvironment()
		env.RegisterActivity(a.PrepareRun)
		result, err := executePrepareRun(t, env, a, PushParams{
			TenantID: 7, UserID: 9, RunKind: PushRunKindScheduled,
			ExecutionMode: types.ExecutionModeCompiled, RuntimeVersion: CompiledRuntimeSnapshotV1,
			ScheduleID: "task-denied",
		})
		if err != nil {
			t.Fatalf("authorization denial should be a normal terminal: %v", err)
		}
		if result != (PrepareRunResult{}) {
			t.Fatalf("denied result leaked snapshot material: %+v", result)
		}
	})

	t.Run("reference identity mismatch is rejected", func(t *testing.T) {
		expected := testActivityIdentity(7, 9, "task-expected")
		wrong := expected
		wrong.TaskID = "task-other"
		ref := mustCompiledRunRef(wrong, 72)
		compiledStore := &compiledRunStoreFake{
			ref: ref, found: true, authorize: true,
			snapshot: runcontext.CompiledSnapshotV1{Ref: ref, Mode: types.ExecutionModeCompiled},
		}
		a := NewActivities(new(compiledRouteFetcherFake), fakeScorer{}, fakeCardGen{}, &fakePusher{}, &fakeStore{}, fakeFeishu{}, nil, nil, nil, nil,
			WithCompiledRuntimeV1(compiledStore,
				func(context.Context, int64, bool) (runtimepolicy.BundleV1, error) {
					return runtimepolicy.BundleV1{}, nil
				},
				new(compiledModelResolverFake)))
		var suite testsuite.WorkflowTestSuite
		env := suite.NewTestActivityEnvironment()
		env.RegisterActivity(a.PrepareRun)
		_, err := executePrepareRun(t, env, a, PushParams{
			TenantID: expected.TenantID, UserID: expected.UserID, RunKind: PushRunKindScheduled,
			ExecutionMode: types.ExecutionModeCompiled, RuntimeVersion: CompiledRuntimeSnapshotV1,
			ScheduleID: expected.TaskID,
		})
		if err == nil {
			t.Fatal("mismatched snapshot identity must fail closed")
		}
	})
}

func TestPrepareRun_MissingCompiledFetchResolverFailsBeforeStateOrEffects(t *testing.T) {
	compiledStore := &compiledRunStoreFake{authorize: true}
	resolver := new(compiledModelResolverFake)
	var builderCalls atomic.Int32
	a := NewActivities(fakeFetcher{}, fakeScorer{}, fakeCardGen{}, &fakePusher{}, &fakeStore{}, fakeFeishu{}, nil, nil, nil, nil,
		WithCompiledRuntimeV1(compiledStore,
			func(context.Context, int64, bool) (runtimepolicy.BundleV1, error) {
				builderCalls.Add(1)
				return runtimepolicy.BundleV1{}, nil
			}, resolver))
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	env.RegisterActivity(a.PrepareRun)

	_, err := executePrepareRun(t, env, a, PushParams{
		TenantID: 7, UserID: 9, RunKind: PushRunKindScheduled,
		ExecutionMode: types.ExecutionModeCompiled, RuntimeVersion: CompiledRuntimeSnapshotV1,
		ScheduleID: "task-missing-fetch-resolver",
	})
	if err == nil {
		t.Fatal("missing compiled fetch resolver must fail closed")
	}
	loadRef, create, loadSnapshot, authorize := compiledStore.counts()
	if loadRef != 0 || create != 0 || loadSnapshot != 0 || authorize != 0 ||
		builderCalls.Load() != 0 || resolver.calls.Load() != 0 {
		t.Fatalf("missing resolver crossed preflight: loadRef=%d create=%d load=%d auth=%d build=%d model=%d",
			loadRef, create, loadSnapshot, authorize, builderCalls.Load(), resolver.calls.Load())
	}
}

func TestPrepareRun_MissingExactFetchRoutesFailBeforeAuthorizationOrEffects(t *testing.T) {
	tests := []struct {
		name              string
		sources           []runcontext.SourceV1
		capabilities      []runtimepolicy.CapabilityV1
		wantValidateCalls int32
	}{
		{
			name: "missing primary credential generation",
			sources: []runcontext.SourceV1{{
				SourceID: 1, Platform: types.PlatformWeb, Capability: types.CapSearch,
			}},
			capabilities:      []runtimepolicy.CapabilityV1{exaCompiledCapability(types.CapSearch, 1)},
			wantValidateCalls: 1,
		},
		{
			name: "missing RSS enrichment dependency generation",
			sources: []runcontext.SourceV1{{
				SourceID: 1, Platform: types.PlatformWeb, Capability: types.CapFeed,
			}},
			capabilities:      []runtimepolicy.CapabilityV1{rssCompiledCapability(1)},
			wantValidateCalls: 1,
		},
		{
			name: "every frozen source is checked",
			sources: []runcontext.SourceV1{
				{SourceID: 1, Platform: types.PlatformWeb, Capability: types.CapSearch},
				{SourceID: 2, Platform: types.PlatformXHS, Capability: types.CapSearch},
				{SourceID: 3, Platform: types.PlatformWeb, Capability: types.CapContents},
			},
			capabilities: []runtimepolicy.CapabilityV1{
				exaCompiledCapability(types.CapSearch, 2),
				bindingCompiledCapability(types.PlatformXHS, types.CapSearch, 2),
				exaCompiledCapability(types.CapContents, 1),
			},
			wantValidateCalls: 3,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			identity := testActivityIdentity(7, 9, "task-route-preflight")
			ref := mustCompiledRunRef(identity, 101)
			compiledStore := &compiledRunStoreFake{
				ref: ref, found: true, authorize: true,
				snapshot: compiledRouteSnapshot(identity, ref, test.sources, test.capabilities),
			}
			multi := vaneFetcher.NewMulti(config.FetchConfig{
				ExaAPIKey: "exa-generation-2", TikhubAPIKey: "tikhub-generation-2",
				CompiledExaCredentialGeneration: 2, CompiledTikHubCredentialGeneration: 2,
			}, nil, nil)
			fetcher := &countingCompiledFetcher{inner: multi}
			resolver := new(compiledModelResolverFake)
			var builderCalls atomic.Int32
			a := NewActivities(fetcher, fakeScorer{}, fakeCardGen{}, &fakePusher{}, &fakeStore{}, fakeFeishu{}, nil, nil, nil, nil,
				WithCompiledRuntimeV1(compiledStore,
					func(context.Context, int64, bool) (runtimepolicy.BundleV1, error) {
						builderCalls.Add(1)
						return runtimepolicy.BundleV1{}, nil
					}, resolver))
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestActivityEnvironment()
			env.RegisterActivity(a.PrepareRun)

			_, err := executePrepareRun(t, env, a, PushParams{
				TenantID: identity.TenantID, UserID: identity.UserID, RunKind: PushRunKindScheduled,
				ExecutionMode: types.ExecutionModeCompiled, RuntimeVersion: CompiledRuntimeSnapshotV1,
				ScheduleID: identity.TaskID,
			})
			if err == nil {
				t.Fatal("missing exact retained route must fail closed")
			}
			loadRef, create, loadSnapshot, authorize := compiledStore.counts()
			if loadRef != 1 || create != 0 || loadSnapshot != 1 || authorize != 0 ||
				builderCalls.Load() != 0 || resolver.calls.Load() != 0 ||
				fetcher.validateCalls.Load() != test.wantValidateCalls || fetcher.fetchCalls.Load() != 0 {
				t.Fatalf("route preflight escaped: loadRef=%d create=%d load=%d auth=%d build=%d model=%d validate=%d fetch=%d",
					loadRef, create, loadSnapshot, authorize, builderCalls.Load(), resolver.calls.Load(),
					fetcher.validateCalls.Load(), fetcher.fetchCalls.Load())
			}
		})
	}
}

func TestPrepareRun_RetainedFetchRoutesAuthorizeWithoutNetwork(t *testing.T) {
	oldRoutes, err := vaneFetcher.NewRuntimeFetchRoutesV1(config.FetchConfig{
		ExaAPIKey: "exa-generation-1", TikhubAPIKey: "tikhub-generation-1",
		CompiledExaCredentialGeneration: 1, CompiledTikHubCredentialGeneration: 1,
	}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	multi, err := vaneFetcher.NewMultiWithRuntimeRoutesV1(config.FetchConfig{
		ExaAPIKey: "exa-generation-2", TikhubAPIKey: "tikhub-generation-2",
		CompiledExaCredentialGeneration: 2, CompiledTikHubCredentialGeneration: 2,
	}, nil, nil, oldRoutes...)
	if err != nil {
		t.Fatal(err)
	}
	identity := testActivityIdentity(7, 9, "task-retained-routes")
	ref := mustCompiledRunRef(identity, 102)
	sources := []runcontext.SourceV1{
		{SourceID: 1, Platform: types.PlatformWeb, Capability: types.CapFeed},
		{SourceID: 2, Platform: types.PlatformWeb, Capability: types.CapSearch},
		{SourceID: 3, Platform: types.PlatformXHS, Capability: types.CapSearch},
	}
	capabilities := []runtimepolicy.CapabilityV1{
		rssCompiledCapability(1),
		exaCompiledCapability(types.CapSearch, 1),
		bindingCompiledCapability(types.PlatformXHS, types.CapSearch, 1),
	}
	compiledStore := &compiledRunStoreFake{
		ref: ref, found: true, authorize: true,
		snapshot: compiledRouteSnapshot(identity, ref, sources, capabilities),
	}
	fetcher := &countingCompiledFetcher{inner: multi}
	resolver := new(compiledModelResolverFake)
	var builderCalls atomic.Int32
	a := NewActivities(fetcher, fakeScorer{}, fakeCardGen{}, &fakePusher{}, &fakeStore{}, fakeFeishu{}, nil, nil, nil, nil,
		WithCompiledRuntimeV1(compiledStore,
			func(context.Context, int64, bool) (runtimepolicy.BundleV1, error) {
				builderCalls.Add(1)
				return runtimepolicy.BundleV1{}, nil
			}, resolver))
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	env.RegisterActivity(a.PrepareRun)

	result, err := executePrepareRun(t, env, a, PushParams{
		TenantID: identity.TenantID, UserID: identity.UserID, RunKind: PushRunKindScheduled,
		ExecutionMode: types.ExecutionModeCompiled, RuntimeVersion: CompiledRuntimeSnapshotV1,
		ScheduleID: identity.TaskID,
	})
	if err != nil {
		t.Fatalf("retained exact routes must remain available: %v", err)
	}
	if !result.Authorized || result.Snapshot != ref {
		t.Fatalf("PrepareRun result = %+v, want authorized ref %+v", result, ref)
	}
	loadRef, create, loadSnapshot, authorize := compiledStore.counts()
	if loadRef != 1 || create != 0 || loadSnapshot != 1 || authorize != 1 ||
		builderCalls.Load() != 0 || resolver.calls.Load() != 1 ||
		fetcher.validateCalls.Load() != int32(len(sources)) || fetcher.fetchCalls.Load() != 0 {
		t.Fatalf("retained route path: loadRef=%d create=%d load=%d auth=%d build=%d model=%d validate=%d fetch=%d",
			loadRef, create, loadSnapshot, authorize, builderCalls.Load(), resolver.calls.Load(),
			fetcher.validateCalls.Load(), fetcher.fetchCalls.Load())
	}
}

type compiledWorkflowCapture struct {
	mu sync.Mutex

	prepareCalls   int
	authorizeCalls int
	begin          []RunOutcomeBeginIn
	finalize       []RunOutcomeFinalizeIn
	evolve         []EvolveIn
	fetch          []PushParams
	dedup          []DedupIn
	score          []ScoreIn
	selectIn       []SelectIn
	cardGen        []CardGenIn
	push           []PushIn
	record         []RecordEmptyIn
	notify         []NotifyEmptyIn
	order          []string

	selectEmpty bool
}

func (c *compiledWorkflowCapture) register(env *testsuite.TestWorkflowEnvironment) {
	reg := func(name string, fn any) { env.RegisterActivityWithOptions(fn, activity.RegisterOptions{Name: name}) }
	reg("PrepareRun", func(ctx context.Context, in PushParams) (PrepareRunResult, error) {
		c.mu.Lock()
		c.prepareCalls++
		c.mu.Unlock()
		info := activity.GetInfo(ctx).WorkflowExecution
		identity := types.RunIdentity{
			TemporalWorkflowID: info.ID,
			TemporalRunID:      info.RunID,
			RunKind:            types.RunSnapshotKindScheduled,
			TenantID:           in.TenantID,
			UserID:             in.UserID,
			TaskID:             in.ScheduleID,
		}
		return PrepareRunResult{Authorized: true, Snapshot: mustCompiledRunRef(identity, 81)}, nil
	})
	reg("AuthorizeRun", func(context.Context, PushParams) (bool, error) {
		c.mu.Lock()
		c.authorizeCalls++
		c.mu.Unlock()
		return true, nil
	})
	reg("BeginRunOutcomeV1", func(_ context.Context, in RunOutcomeBeginIn) (types.RunOutcomeMarkerV1, error) {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.begin = append(c.begin, in)
		c.order = append(c.order, "begin")
		return types.RunOutcomeMarkerV1{
			ID: 91, SchemaVersion: types.RunOutcomeSchemaVersionV1,
			RunSnapshotID: in.Run.Snapshot.SnapshotID,
			TenantID:      in.Run.TenantID, UserID: in.UserID,
			TaskID: in.Run.TaskID,
		}, nil
	})
	reg("FinalizeRunOutcomeV1", func(_ context.Context, in RunOutcomeFinalizeIn) (types.RunOutcomeV1, error) {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.finalize = append(c.finalize, in)
		return in.Claim.SealAt(time.Now())
	})
	reg("EvolveProfile", func(_ context.Context, in EvolveIn) error {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.evolve = append(c.evolve, in)
		c.order = append(c.order, "evolve")
		return nil
	})
	reg("Fetch", func(_ context.Context, in PushParams) ([]types.ContentItem, error) {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.fetch = append(c.fetch, in)
		c.order = append(c.order, "fetch")
		return items(1), nil
	})
	reg("FetchOutcomeV1", func(_ context.Context, in PushParams) (FetchOutcomeResult, error) {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.fetch = append(c.fetch, in)
		c.order = append(c.order, "fetch")
		return FetchOutcomeResult{
			Items: items(1), SourceCoverage: types.RunCompletenessComplete,
		}, nil
	})
	reg("Dedup", func(_ context.Context, in DedupIn) ([]types.ContentItem, error) {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.dedup = append(c.dedup, in)
		return in.Items, nil
	})
	reg("QualifyEvents", func(_ context.Context, in QualifyEventsIn) (QualifyEventsResult, error) {
		return QualifyEventsResult{Items: in.Items, Outcome: "not_configured"}, nil
	})
	reg("Score", func(_ context.Context, in ScoreIn) ([]types.ScoredItem, error) {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.score = append(c.score, in)
		return scoredItems(1), nil
	})
	reg("ScoreOutcomeV1", func(_ context.Context, in ScoreIn) (ScoreOutcomeResult, error) {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.score = append(c.score, in)
		return ScoreOutcomeResult{
			Items: scoredItems(1), Processing: types.RunCompletenessComplete,
		}, nil
	})
	reg("Select", func(_ context.Context, in SelectIn) ([]types.ScoredItem, error) {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.selectIn = append(c.selectIn, in)
		if c.selectEmpty {
			return nil, nil
		}
		return in.Scored, nil
	})
	reg("CardGen", func(_ context.Context, in CardGenIn) ([]GeneratedCard, error) {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.cardGen = append(c.cardGen, in)
		return cardsOf(1), nil
	})
	reg("CardGenOutcomeV1", func(_ context.Context, in CardGenIn) (CardGenOutcomeResult, error) {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.cardGen = append(c.cardGen, in)
		return CardGenOutcomeResult{
			Cards: cardsOf(1), Processing: types.RunCompletenessComplete,
		}, nil
	})
	reg("Push", func(_ context.Context, in PushIn) error {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.push = append(c.push, in)
		return nil
	})
	reg("RecordEmptyBatch", func(_ context.Context, in RecordEmptyIn) error {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.record = append(c.record, in)
		return nil
	})
	reg("NotifyEmptyResult", func(_ context.Context, in NotifyEmptyIn) error {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.notify = append(c.notify, in)
		return nil
	})
}

func (c *compiledWorkflowCapture) assertRunRef(t *testing.T, want types.RunSnapshotRef, run *CompiledRunInputV1, stage string) {
	t.Helper()
	if run == nil {
		t.Fatalf("%s did not receive compiled run input", stage)
	}
	if run.TenantID != want.TenantID || run.TaskID != want.TaskID || run.Snapshot != want {
		t.Fatalf("%s run input drifted: %+v, want ref %+v", stage, run, want)
	}
}

func TestPushPipelineWorkflow_CompiledRunUsesPrepareAndPropagatesOneReference(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: "wf-task-compiled"})
	capture := new(compiledWorkflowCapture)
	capture.register(env)

	env.ExecuteWorkflow(PushPipelineWorkflow, PushParams{
		TenantID: 7, UserID: 9, RunKind: PushRunKindScheduled,
		ExecutionMode: types.ExecutionModeCompiled, RuntimeVersion: CompiledRuntimeSnapshotV1,
		ScheduleID: "task-compiled", NLDesc: "mutable title",
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("compiled workflow failed: %v", err)
	}

	capture.mu.Lock()
	defer capture.mu.Unlock()
	if capture.prepareCalls != 1 || capture.authorizeCalls != 0 {
		t.Fatalf("gate calls PrepareRun=%d AuthorizeRun=%d, want 1/0", capture.prepareCalls, capture.authorizeCalls)
	}
	if len(capture.fetch) != 1 || capture.fetch[0].Snapshot == nil {
		t.Fatalf("Fetch did not receive sealed snapshot ref: %+v", capture.fetch)
	}
	want := *capture.fetch[0].Snapshot
	if capture.fetch[0].TenantID != 7 || capture.fetch[0].ScheduleID != "task-compiled" {
		t.Fatalf("Fetch trusted scope drifted: %+v", capture.fetch[0])
	}
	if len(capture.evolve) != 1 || len(capture.dedup) != 1 || len(capture.score) != 1 ||
		len(capture.selectIn) != 1 || len(capture.cardGen) != 1 || len(capture.push) != 1 {
		t.Fatalf("full pipeline calls evolve=%d dedup=%d score=%d select=%d card=%d push=%d",
			len(capture.evolve), len(capture.dedup), len(capture.score), len(capture.selectIn), len(capture.cardGen), len(capture.push))
	}
	capture.assertRunRef(t, want, capture.evolve[0].Run, "EvolveProfile")
	capture.assertRunRef(t, want, capture.dedup[0].Run, "Dedup")
	capture.assertRunRef(t, want, capture.score[0].Run, "Score")
	capture.assertRunRef(t, want, capture.selectIn[0].Run, "Select")
	capture.assertRunRef(t, want, capture.cardGen[0].Run, "CardGen")
	capture.assertRunRef(t, want, capture.push[0].Run, "Push")
}

func TestPushPipelineWorkflow_RunOutcomeBeginsBeforeFirstSideEffectAndFinalizes(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetStartWorkflowOptions(client.StartWorkflowOptions{
		ID: "wf-task-run-outcome",
	})
	capture := new(compiledWorkflowCapture)
	capture.register(env)

	env.ExecuteWorkflow(PushPipelineWorkflow, PushParams{
		TenantID: 7, UserID: 9, RunKind: PushRunKindScheduled,
		ExecutionMode:  types.ExecutionModeCompiled,
		RuntimeVersion: CompiledRuntimeRunOutcomeV1,
		ScheduleID:     "task-run-outcome",
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("run outcome workflow failed: %v", err)
	}
	capture.mu.Lock()
	defer capture.mu.Unlock()
	if len(capture.begin) != 1 || len(capture.finalize) != 1 {
		t.Fatalf("lifecycle calls begin=%d finalize=%d",
			len(capture.begin), len(capture.finalize))
	}
	if len(capture.order) < 3 ||
		capture.order[0] != "begin" ||
		capture.order[1] != "evolve" ||
		capture.order[2] != "fetch" {
		t.Fatalf("first-side-effect ordering = %v", capture.order)
	}
	claim := capture.finalize[0].Claim
	if claim.Result != types.RunResultContent ||
		claim.SourceCoverage != types.RunCompletenessComplete ||
		claim.Processing != types.RunCompletenessComplete {
		t.Fatalf("content outcome claim = %+v", claim)
	}
}

func TestPushPipelineWorkflow_PreP1BCompiledDoesNotCreateOutcomeMarker(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	capture := new(compiledWorkflowCapture)
	capture.register(env)
	env.ExecuteWorkflow(PushPipelineWorkflow, PushParams{
		TenantID: 7, UserID: 9, RunKind: PushRunKindScheduled,
		ExecutionMode:  types.ExecutionModeCompiled,
		RuntimeVersion: CompiledRuntimeSnapshotV1,
		ScheduleID:     "task-pre-p1b",
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	capture.mu.Lock()
	defer capture.mu.Unlock()
	if len(capture.begin) != 0 || len(capture.finalize) != 0 {
		t.Fatalf("pre-P1-B lifecycle calls begin=%d finalize=%d",
			len(capture.begin), len(capture.finalize))
	}
}

func TestPushPipelineWorkflow_CompiledEmptyExitPropagatesReferenceToRecordAndNotify(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: "wf-task-compiled"})
	capture := &compiledWorkflowCapture{selectEmpty: true}
	capture.register(env)

	env.ExecuteWorkflow(PushPipelineWorkflow, PushParams{
		TenantID: 7, UserID: 9, RunKind: PushRunKindScheduled,
		ExecutionMode: types.ExecutionModeCompiled, RuntimeVersion: CompiledRuntimeSnapshotV1,
		ScheduleID: "task-compiled",
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("compiled empty workflow failed: %v", err)
	}
	capture.mu.Lock()
	defer capture.mu.Unlock()
	if capture.prepareCalls != 1 || capture.authorizeCalls != 0 || len(capture.fetch) != 1 || capture.fetch[0].Snapshot == nil {
		t.Fatalf("compiled gate/capture mismatch: prepare=%d authorize=%d fetch=%+v", capture.prepareCalls, capture.authorizeCalls, capture.fetch)
	}
	want := *capture.fetch[0].Snapshot
	if len(capture.record) != 1 || len(capture.notify) != 1 {
		t.Fatalf("select empty exit should record and notify once: record=%d notify=%d", len(capture.record), len(capture.notify))
	}
	capture.assertRunRef(t, want, capture.record[0].Run, "RecordEmptyBatch")
	capture.assertRunRef(t, want, capture.notify[0].Run, "NotifyEmptyResult")
}

func TestPushPipelineWorkflow_LegacyAndAdHocKeepAuthorizationPath(t *testing.T) {
	for _, tc := range []struct {
		name   string
		params PushParams
	}{
		{name: "scheduled fixed pipeline stays legacy runtime", params: PushParams{
			TenantID: 7, UserID: 9, RunKind: PushRunKindScheduled,
			ExecutionMode: types.ExecutionModeCompiled, ScheduleID: "legacy-task",
		}},
		{name: "pre-C1b scheduled Action stays on the authorized legacy path", params: PushParams{
			UserID: 9, RunKind: PushRunKindScheduled, ScheduleID: "legacy-task",
		}},
		{name: "ad hoc stays legacy", params: PushParams{UserID: 9, RunKind: PushRunKindAdHoc}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()
			capture := new(compiledWorkflowCapture)
			capture.register(env)
			env.ExecuteWorkflow(PushPipelineWorkflow, tc.params)
			if err := env.GetWorkflowError(); err != nil {
				t.Fatalf("legacy workflow failed: %v", err)
			}
			capture.mu.Lock()
			defer capture.mu.Unlock()
			if capture.prepareCalls != 0 || capture.authorizeCalls != 1 {
				t.Fatalf("gate calls PrepareRun=%d AuthorizeRun=%d, want 0/1", capture.prepareCalls, capture.authorizeCalls)
			}
			if len(capture.fetch) != 1 || capture.fetch[0].Snapshot != nil {
				t.Fatalf("legacy Fetch must not receive compiled ref: %+v", capture.fetch)
			}
		})
	}
}

func TestPushPipelineWorkflow_PartialOrUnknownRuntimeEnvelopeFailsBeforeActivity(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*PushParams)
	}{
		{name: "tenant only", mutate: func(p *PushParams) { p.TenantID = 7 }},
		{name: "mode only", mutate: func(p *PushParams) { p.ExecutionMode = types.ExecutionModeCompiled }},
		{name: "runtime only", mutate: func(p *PushParams) { p.RuntimeVersion = CompiledRuntimeSnapshotV1 }},
		{name: "unknown mode sentinel", mutate: func(p *PushParams) {
			p.ExecutionMode = types.ExecutionModeUnknown
		}},
		{name: "discover mode", mutate: func(p *PushParams) {
			p.TenantID = 7
			p.ExecutionMode = types.ExecutionModeDiscoverAtRun
		}},
		{name: "unknown runtime", mutate: func(p *PushParams) {
			p.TenantID = 7
			p.ExecutionMode = types.ExecutionModeCompiled
			p.RuntimeVersion = "compiled-snapshot/v999"
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()
			capture := new(compiledWorkflowCapture)
			capture.register(env)
			params := PushParams{
				UserID: 9, RunKind: PushRunKindScheduled, ScheduleID: "task-envelope",
			}
			tc.mutate(&params)
			env.ExecuteWorkflow(PushPipelineWorkflow, params)
			if err := env.GetWorkflowError(); err == nil {
				t.Fatal("partial or unknown runtime envelope must fail closed")
			}
			capture.mu.Lock()
			defer capture.mu.Unlock()
			if capture.prepareCalls != 0 || capture.authorizeCalls != 0 || len(capture.fetch) != 0 {
				t.Fatalf("invalid envelope reached an Activity: prepare=%d authorize=%d fetch=%d",
					capture.prepareCalls, capture.authorizeCalls, len(capture.fetch))
			}
		})
	}
}

type effectCountingStore struct {
	*fakeStore
	muEffects sync.Mutex
	upserts   int
	updates   int
	batches   int
	inserts   int
	marks     int
	statuses  int
	getSource int
}

func (s *effectCountingStore) UpsertContentItem(ctx context.Context, item *types.ContentItem) (int64, bool, error) {
	s.muEffects.Lock()
	s.upserts++
	s.muEffects.Unlock()
	return s.fakeStore.UpsertContentItem(ctx, item)
}

func (s *effectCountingStore) UpdateSourceFetchState(ctx context.Context, id int64, last, next time.Time, failCount int) error {
	s.muEffects.Lock()
	s.updates++
	s.muEffects.Unlock()
	return s.fakeStore.UpdateSourceFetchState(ctx, id, last, next, failCount)
}

func (s *effectCountingStore) CreatePushBatchIdempotent(ctx context.Context, userID int64, key, taskID string) (int64, error) {
	s.muEffects.Lock()
	s.batches++
	s.muEffects.Unlock()
	return s.fakeStore.CreatePushBatchIdempotent(ctx, userID, key, taskID)
}

func (s *effectCountingStore) InsertDeliveryIdempotent(ctx context.Context, d *types.Delivery) (int64, bool, bool, error) {
	s.muEffects.Lock()
	s.inserts++
	s.muEffects.Unlock()
	return s.fakeStore.InsertDeliveryIdempotent(ctx, d)
}

func (s *effectCountingStore) MarkDeliverySent(ctx context.Context, id int64, msgID string, card json.RawMessage, at time.Time) error {
	s.muEffects.Lock()
	s.marks++
	s.muEffects.Unlock()
	return s.fakeStore.MarkDeliverySent(ctx, id, msgID, card, at)
}

func (s *effectCountingStore) UpdatePushBatchStatus(ctx context.Context, id int64, status types.BatchStatus) error {
	s.muEffects.Lock()
	s.statuses++
	s.muEffects.Unlock()
	return s.fakeStore.UpdatePushBatchStatus(ctx, id, status)
}

func (s *effectCountingStore) GetSource(ctx context.Context, id int64) (*types.Source, error) {
	s.muEffects.Lock()
	s.getSource++
	s.muEffects.Unlock()
	return s.fakeStore.GetSource(ctx, id)
}

func (s *effectCountingStore) effectCounts() (upserts, updates, batches, inserts, marks, statuses, getSource int) {
	s.muEffects.Lock()
	defer s.muEffects.Unlock()
	return s.upserts, s.updates, s.batches, s.inserts, s.marks, s.statuses, s.getSource
}

type sourceCaptureFetcher struct {
	mu              sync.Mutex
	legacySources   []types.Source
	compiledSources []types.Source
	capabilities    []runtimepolicy.CapabilityV1
	legacyRuns      []capturedBindingRunAttribution
	compiledRuns    []capturedBindingRunAttribution
	items           []types.ContentItem
}

type capturedBindingRunAttribution struct {
	traceID            string
	tenantID, userID   int64
	hasTenant, hasUser bool
}

type gatedSequenceFetcher struct {
	effects atomic.Int32
}

func (*gatedSequenceFetcher) ValidateRuntimeFetchRouteV1(
	runtimepolicy.CapabilityV1,
	types.Source,
) error {
	return nil
}

func (*gatedSequenceFetcher) Fetch(context.Context, types.Source) ([]types.ContentItem, error) {
	return nil, errors.New("legacy fetch must not be called")
}

func (f *gatedSequenceFetcher) FetchWithPolicyV1(
	ctx context.Context,
	_ types.Source,
	_ runtimepolicy.CapabilityV1,
	beforeEffect func(context.Context) error,
) ([]types.ContentItem, error) {
	if err := beforeEffect(ctx); err != nil {
		return nil, err
	}
	f.effects.Add(1)
	if err := beforeEffect(ctx); err != nil {
		return nil, err
	}
	f.effects.Add(1)
	return nil, nil
}

func (f *sourceCaptureFetcher) Fetch(ctx context.Context, source types.Source) ([]types.ContentItem, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.legacySources = append(f.legacySources, source)
	f.legacyRuns = append(f.legacyRuns, captureBindingRunAttribution(ctx))
	return append([]types.ContentItem(nil), f.items...), nil
}

func (f *sourceCaptureFetcher) FetchWithPolicyV1(
	ctx context.Context,
	source types.Source,
	capability runtimepolicy.CapabilityV1,
	_ func(context.Context) error,
) ([]types.ContentItem, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.compiledSources = append(f.compiledSources, source)
	f.capabilities = append(f.capabilities, capability)
	f.compiledRuns = append(f.compiledRuns, captureBindingRunAttribution(ctx))
	return append([]types.ContentItem(nil), f.items...), nil
}

func captureBindingRunAttribution(ctx context.Context) capturedBindingRunAttribution {
	traceID, tenantID, hasTenant, userID, hasUser := vaneFetcher.BindingRunAttributionFromContext(ctx)
	return capturedBindingRunAttribution{
		traceID: traceID, tenantID: tenantID, hasTenant: hasTenant,
		userID: userID, hasUser: hasUser,
	}
}

func (*sourceCaptureFetcher) ValidateRuntimeFetchRouteV1(
	runtimepolicy.CapabilityV1,
	types.Source,
) error {
	return nil
}

func (f *sourceCaptureFetcher) snapshot() (legacy, compiled []types.Source, capabilities []runtimepolicy.CapabilityV1) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]types.Source(nil), f.legacySources...),
		append([]types.Source(nil), f.compiledSources...),
		append([]runtimepolicy.CapabilityV1(nil), f.capabilities...)
}

func (f *sourceCaptureFetcher) runAttributions() (legacy, compiled []capturedBindingRunAttribution) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]capturedBindingRunAttribution(nil), f.legacyRuns...),
		append([]capturedBindingRunAttribution(nil), f.compiledRuns...)
}

func compiledActivityFixture(taskTitle string) (types.RunIdentity, types.RunSnapshotRef, runcontext.CompiledSnapshotV1) {
	identity := testActivityIdentity(7, 9, "task-frozen")
	ref := mustCompiledRunRef(identity, 91)
	snapshot := runcontext.CompiledSnapshotV1{
		Ref:  ref,
		Mode: types.ExecutionModeCompiled,
		Definition: runcontext.DefinitionV1{
			TaskID: identity.TaskID, TenantID: identity.TenantID, UserID: identity.UserID,
			NLDescription: taskTitle,
			ScopeJSON:     json.RawMessage(`{"source_ids":[10],"top_n":2}`),
			SourceScope:   runcontext.SourceScopeApprovedPlan,
			Sources: []runcontext.SourceV1{{
				SourceID: 10, Platform: types.PlatformWeb, Capability: types.CapFeed,
				Title: "Frozen Source", URL: "https://frozen.example/feed", Config: json.RawMessage(`{"frozen":true}`),
			}},
		},
		Policy: runtimepolicy.BundleV1{
			CapabilityCatalog: runtimepolicy.CapabilityCatalogV1{
				Allowed: []runtimepolicy.CapabilityV1{{
					Platform: string(types.PlatformWeb), Capability: string(types.CapFeed), Kind: string(types.KindArticle),
					ImplementationVersion: runtimepolicy.CapabilityImplementationRSSV1,
					DependencyCredentialRefs: []runtimepolicy.CredentialRefV1{{
						ID: runtimepolicy.CredentialIDExaPrimaryV1, Generation: 1,
					}},
				}},
			},
		},
	}
	return identity, ref, snapshot
}

func executeFetchActivity(t *testing.T, env *testsuite.TestActivityEnvironment, a *Activities, p PushParams) ([]types.ContentItem, error) {
	t.Helper()
	encoded, err := env.ExecuteActivity(a.Fetch, p)
	if err != nil {
		return nil, err
	}
	var items []types.ContentItem
	if err := encoded.Get(&items); err != nil {
		t.Fatalf("decode Fetch result: %v", err)
	}
	return items, nil
}

func TestFetch_CompiledRunUsesFrozenSourcesAndCandidates(t *testing.T) {
	identity, ref, snapshot := compiledActivityFixture("Frozen Task")
	compiledStore := &compiledRunStoreFake{
		snapshot:  snapshot,
		authorize: true,
		dueSources: []types.Source{{
			ID: 10, Platform: types.PlatformXHS, Capability: types.CapSearch,
			Title: "MUTATED SOURCE", URL: "https://mutated.invalid", Config: json.RawMessage(`{"mutated":true}`),
			FetchIntervalSeconds: 60,
		}},
		candidates: []types.ContentItem{{ID: 501, SourceID: 10, Title: "frozen candidate"}},
	}
	legacyStore := &effectCountingStore{fakeStore: &fakeStore{
		dueSources: []types.Source{{ID: 99, Title: "LEGACY CANARY"}},
		unpushed:   []types.ContentItem{{ID: 999, Title: "legacy candidate"}},
	}}
	fetcher := &sourceCaptureFetcher{items: []types.ContentItem{{
		SourceID: 10, CanonicalKey: "https://frozen.example/item",
	}}}
	a := NewActivities(fetcher, fakeScorer{}, fakeCardGen{}, &fakePusher{}, legacyStore, fakeFeishu{}, nil, nil, nil, nil,
		WithCompiledRuntimeV1(compiledStore,
			func(context.Context, int64, bool) (runtimepolicy.BundleV1, error) {
				return runtimepolicy.BundleV1{}, nil
			},
			new(compiledModelResolverFake)))
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	env.RegisterActivity(a.Fetch)
	got, err := executeFetchActivity(t, env, a, PushParams{
		TenantID: identity.TenantID, UserID: identity.UserID, RunKind: PushRunKindScheduled,
		ScheduleID: identity.TaskID, Scope: PushScope{SourceIDs: []int64{99}}, Snapshot: &ref,
	})
	if err != nil {
		t.Fatalf("compiled Fetch failed: %v", err)
	}
	if !reflect.DeepEqual(got, compiledStore.candidates) {
		t.Fatalf("candidate query escaped frozen scope: got %+v, want %+v", got, compiledStore.candidates)
	}
	legacy, seen, capabilities := fetcher.snapshot()
	if len(legacy) != 0 {
		t.Fatalf("compiled Fetch called mutable legacy dispatcher: %+v", legacy)
	}
	if len(seen) != 1 {
		t.Fatalf("fetch calls = %d, want 1", len(seen))
	}
	want := snapshot.Definition.Sources[0]
	if seen[0].ID != want.SourceID || seen[0].Platform != want.Platform || seen[0].Capability != want.Capability ||
		seen[0].Title != want.Title || seen[0].URL != want.URL || !reflect.DeepEqual(seen[0].Config, want.Config) {
		t.Fatalf("Fetch observed mutable source metadata: got %+v, frozen %+v", seen[0], want)
	}
	if len(capabilities) != 1 || !reflect.DeepEqual(capabilities[0], snapshot.Policy.CapabilityCatalog.Allowed[0]) {
		t.Fatalf("compiled fetch capability = %+v, want frozen %+v", capabilities, snapshot.Policy.CapabilityCatalog.Allowed)
	}
	legacyRuns, compiledRuns := fetcher.runAttributions()
	if len(legacyRuns) != 0 || len(compiledRuns) != 1 {
		t.Fatalf("fetch run attributions legacy=%+v compiled=%+v", legacyRuns, compiledRuns)
	}
	gotRun := compiledRuns[0]
	if gotRun.traceID != testActivityWorkflowID || !gotRun.hasTenant || gotRun.tenantID != identity.TenantID ||
		!gotRun.hasUser || gotRun.userID != identity.UserID {
		t.Fatalf("compiled fetch lost immutable run attribution: %+v", gotRun)
	}
	compiledStore.mu.Lock()
	dueIDs := append([][]int64(nil), compiledStore.dueSourceIDs...)
	candidateIDs := append([][]int64(nil), compiledStore.candidateSourceID...)
	exactUpserts := compiledStore.fetchUpserts
	exactStateWrites := compiledStore.fetchStateWrites
	compiledStore.mu.Unlock()
	if len(dueIDs) != 1 || len(candidateIDs) != 1 || !reflect.DeepEqual(dueIDs[0], []int64{10}) || !reflect.DeepEqual(candidateIDs[0], []int64{10}) {
		t.Fatalf("compiled source scope calls due=%v candidates=%v", dueIDs, candidateIDs)
	}
	if exactUpserts != 1 || exactStateWrites != 1 {
		t.Fatalf("compiled Fetch exact writes: content=%d state=%d, want 1/1",
			exactUpserts, exactStateWrites)
	}
	legacyUpserts, legacyUpdates, _, _, _, _, _ := legacyStore.effectCounts()
	if legacyUpserts != 0 || legacyUpdates != 0 {
		t.Fatalf("compiled Fetch escaped to legacy writes: content=%d state=%d",
			legacyUpserts, legacyUpdates)
	}
}

func TestFetch_LegacyRunAttributionDoesNotInventTenant(t *testing.T) {
	legacyStore := &fakeStore{dueSources: []types.Source{{
		ID: 17, Platform: types.PlatformWeb, Capability: types.CapSearch,
		FetchIntervalSeconds: 60,
	}}}
	fetcher := new(sourceCaptureFetcher)
	a := NewActivities(fetcher, fakeScorer{}, fakeCardGen{}, &fakePusher{}, legacyStore, fakeFeishu{}, nil, nil, nil, nil)
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	env.RegisterActivity(a.Fetch)

	if _, err := executeFetchActivity(t, env, a, PushParams{
		UserID: 9, RunKind: PushRunKindAdHoc,
	}); err != nil {
		t.Fatalf("legacy Fetch failed: %v", err)
	}
	legacyRuns, compiledRuns := fetcher.runAttributions()
	if len(legacyRuns) != 1 || len(compiledRuns) != 0 {
		t.Fatalf("fetch run attributions legacy=%+v compiled=%+v", legacyRuns, compiledRuns)
	}
	gotRun := legacyRuns[0]
	if gotRun.traceID != testActivityWorkflowID || gotRun.hasTenant || gotRun.tenantID != 0 || gotRun.hasUser {
		t.Fatalf("legacy fetch invented immutable run attribution: %+v", gotRun)
	}
}

func TestFetch_CompiledRevocationStopsBeforePaidFetchAndWrites(t *testing.T) {
	identity, ref, snapshot := compiledActivityFixture("Frozen Task")
	compiledStore := &compiledRunStoreFake{
		snapshot:  snapshot,
		authorize: false,
		dueSources: []types.Source{{
			ID: 10, FetchIntervalSeconds: 60,
		}},
	}
	legacyStore := &effectCountingStore{fakeStore: new(fakeStore)}
	fetcher := new(sourceCaptureFetcher)
	a := NewActivities(fetcher, fakeScorer{}, fakeCardGen{}, &fakePusher{}, legacyStore, fakeFeishu{}, nil, nil, nil, nil,
		WithCompiledRuntimeV1(compiledStore,
			func(context.Context, int64, bool) (runtimepolicy.BundleV1, error) {
				return runtimepolicy.BundleV1{}, nil
			},
			new(compiledModelResolverFake)))
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	env.RegisterActivity(a.Fetch)
	_, err := executeFetchActivity(t, env, a, PushParams{
		TenantID: identity.TenantID, UserID: identity.UserID, RunKind: PushRunKindScheduled,
		ScheduleID: identity.TaskID, Snapshot: &ref,
	})
	if err == nil {
		t.Fatal("revoked compiled Fetch must fail closed")
	}
	legacy, compiled, _ := fetcher.snapshot()
	if calls := len(legacy) + len(compiled); calls != 0 {
		t.Fatalf("revoked run made %d paid/network fetch calls", calls)
	}
	upserts, updates, _, _, _, _, _ := legacyStore.effectCounts()
	if upserts != 0 || updates != 0 {
		t.Fatalf("revoked run wrote fetch state: upserts=%d updates=%d", upserts, updates)
	}
}

func TestFetch_CompiledRevocationBetweenFetcherEffectsPropagates(t *testing.T) {
	identity, ref, snapshot := compiledActivityFixture("Frozen Task")
	compiledStore := &compiledRunStoreFake{
		snapshot: snapshot,
		// Source-loop precheck, first upstream call, then revoke before the
		// fetcher's second upstream call.
		authorizeScript: []bool{true, true, false},
		dueSources: []types.Source{{
			ID: 10, Platform: types.PlatformWeb, Capability: types.CapFeed,
			FetchIntervalSeconds: 60,
		}},
	}
	legacyStore := &effectCountingStore{fakeStore: new(fakeStore)}
	fetcher := new(gatedSequenceFetcher)
	a := NewActivities(fetcher, fakeScorer{}, fakeCardGen{}, &fakePusher{}, legacyStore, fakeFeishu{}, nil, nil, nil, nil,
		WithCompiledRuntimeV1(compiledStore,
			func(context.Context, int64, bool) (runtimepolicy.BundleV1, error) {
				return runtimepolicy.BundleV1{}, nil
			},
			new(compiledModelResolverFake)))
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	env.RegisterActivity(a.Fetch)

	_, err := executeFetchActivity(t, env, a, PushParams{
		TenantID: identity.TenantID, UserID: identity.UserID, RunKind: PushRunKindScheduled,
		ScheduleID: identity.TaskID, Snapshot: &ref,
	})
	if err == nil {
		t.Fatal("revocation between fetch effects must fail the Activity")
	}
	if got := fetcher.effects.Load(); got != 1 {
		t.Fatalf("upstream effects = %d, want only the pre-revocation call", got)
	}
	upserts, updates, _, _, _, _, _ := legacyStore.effectCounts()
	if upserts != 0 || updates != 0 {
		t.Fatalf("authorization failure was swallowed into fetch bookkeeping: upserts=%d updates=%d", upserts, updates)
	}
}

func TestCompiledLLMActivities_MapQuotaExhaustionToNonRetryableQuota(t *testing.T) {
	identity, ref, snapshot := compiledActivityFixture("Frozen Task")
	policy, err := runtimeconfig.BuildCurrentCompiledV1(runtimeconfig.CurrentCompiledV1Input{
		Model: "deepseek-chat", TaskInstructionEnabled: true,
		ModelEndpointGeneration: 1, ModelCredentialGeneration: 1,
		ExaCredentialGeneration: 1, TikHubCredentialGeneration: 1,
	})
	if err != nil {
		t.Fatalf("build compiled policy fixture: %v", err)
	}
	snapshot.Policy = policy
	compiledStore := &compiledRunStoreFake{
		snapshot: snapshot, authorize: true, quotaErr: store.ErrQuotaExceeded,
	}
	score := new(compiledQuotaScorerFake)
	card := new(compiledQuotaCardGenFake)
	a := NewActivities(fakeFetcher{}, score, card, &fakePusher{}, new(fakeStore),
		fakeFeishu{}, nil, nil, nil, nil,
		WithCompiledRuntimeV1(compiledStore,
			func(context.Context, int64, bool) (runtimepolicy.BundleV1, error) {
				return runtimepolicy.BundleV1{}, nil
			},
			new(compiledModelResolverFake)))
	run := &CompiledRunInputV1{
		TenantID: identity.TenantID, TaskID: identity.TaskID, Snapshot: ref,
	}
	assertQuota := func(t *testing.T, err error) {
		t.Helper()
		if err == nil {
			t.Fatal("quota exhaustion must fail the Activity")
		}
		var appErr *temporal.ApplicationError
		if !errors.As(err, &appErr) {
			t.Fatalf("quota error type = %T, want Temporal ApplicationError: %v", err, err)
		}
		if appErr.Type() != string(types.CodeQuotaExceeded) || !appErr.NonRetryable() {
			t.Fatalf("quota error type=%q non_retryable=%v, want %q/true",
				appErr.Type(), appErr.NonRetryable(), types.CodeQuotaExceeded)
		}
	}

	t.Run("score", func(t *testing.T) {
		var suite testsuite.WorkflowTestSuite
		env := suite.NewTestActivityEnvironment()
		env.RegisterActivity(a.Score)
		_, err := env.ExecuteActivity(a.Score, ScoreIn{
			UserID: identity.UserID, TraceID: "trace-score-quota", Run: run,
			Items: []types.ContentItem{{ID: 501, Title: "item"}},
		})
		assertQuota(t, err)
		if got := score.calls.Load(); got != 1 {
			t.Fatalf("compiled scorer calls = %d, want 1", got)
		}
	})

	t.Run("cardgen", func(t *testing.T) {
		var suite testsuite.WorkflowTestSuite
		env := suite.NewTestActivityEnvironment()
		env.RegisterActivity(a.CardGen)
		_, err := env.ExecuteActivity(a.CardGen, CardGenIn{
			UserID: identity.UserID, TraceID: "trace-card-quota", Run: run,
			Items: []types.ScoredItem{{
				Item: types.ContentItem{ID: 501, Title: "item"}, Score: 88,
			}},
		})
		assertQuota(t, err)
		if got := card.calls.Load(); got != 1 {
			t.Fatalf("compiled card generator calls = %d, want 1", got)
		}
	})
}

func executePushActivity(t *testing.T, env *testsuite.TestActivityEnvironment, a *Activities, in PushIn) error {
	t.Helper()
	_, err := env.ExecuteActivity(a.Push, in)
	return err
}

type authorityCheckingPusher struct {
	order *pushAuthorityOrder
	calls atomic.Int32
}

func (p *authorityCheckingPusher) Push(
	context.Context,
	string,
	string,
) (string, error) {
	if !p.order.claimed.Load() {
		p.order.providerBefore.Store(true)
	}
	p.calls.Add(1)
	return "om-authority-ordered", nil
}

func TestPush_CompiledRunUsesFrozenTaskTitleAndSourceAttribution(t *testing.T) {
	identity, ref, snapshot := compiledActivityFixture("Frozen Task Title")
	compiledStore := &compiledRunStoreFake{
		snapshot: snapshot, authorize: true, attributionID: 10, attributionOK: true,
	}
	legacyStore := &effectCountingStore{fakeStore: new(fakeStore)}
	pusher := &fakePusher{msgID: "om-frozen"}
	var captured feedback.AggregateCardInput
	var headerTask string
	a := NewActivities(fakeFetcher{}, fakeScorer{}, fakeCardGen{}, pusher, legacyStore, fakeFeishu{}, nil, nil,
		func(in feedback.AggregateCardInput) string {
			captured = in
			return `{}`
		},
		func(task string, n int) (string, string) {
			headerTask = task
			return task, "blue"
		},
		WithCompiledRuntimeV1(compiledStore,
			func(context.Context, int64, bool) (runtimepolicy.BundleV1, error) {
				return runtimepolicy.BundleV1{}, nil
			},
			new(compiledModelResolverFake)))
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	env.RegisterActivity(a.Push)
	err := executePushActivity(t, env, a, PushIn{
		UserID: identity.UserID, ScheduleID: identity.TaskID, TraceID: "trace-frozen", TaskTitle: "MUTATED TASK TITLE",
		Run: &CompiledRunInputV1{TenantID: identity.TenantID, TaskID: identity.TaskID, Snapshot: ref},
		Cards: []GeneratedCard{{
			Scored: types.ScoredItem{Item: types.ContentItem{ID: 501, SourceID: 99, Title: "item", URL: "https://item"}, Score: 88},
			BodyMD: "body",
		}},
	})
	if err != nil {
		t.Fatalf("compiled Push failed: %v", err)
	}
	if headerTask != "Frozen Task Title" {
		t.Fatalf("header used mutable task title %q", headerTask)
	}
	if len(captured.Items) != 1 || captured.Items[0].SourceTitle != "Frozen Source" || captured.Items[0].Platform != types.PlatformWeb {
		t.Fatalf("card source attribution escaped frozen definition: %+v", captured.Items)
	}
	if len(pusher.sentCards()) != 1 {
		t.Fatalf("push calls = %d, want 1", len(pusher.sentCards()))
	}
	_, _, _, _, _, _, getSource := legacyStore.effectCounts()
	if getSource != 0 {
		t.Fatalf("compiled Push consulted mutable GetSource %d times", getSource)
	}
	compiledStore.mu.Lock()
	attributionIDs := append([][]int64(nil), compiledStore.attributionIDs...)
	compiledStore.mu.Unlock()
	if len(attributionIDs) != 1 || !reflect.DeepEqual(attributionIDs[0], []int64{10}) {
		t.Fatalf("source attribution scope = %v, want [[10]]", attributionIDs)
	}
}

func TestPush_CompiledClaimsLegacyAuthorityBeforeAllDeliveryEffects(t *testing.T) {
	identity, ref, snapshot := compiledActivityFixture("Frozen Task")
	order := new(pushAuthorityOrder)
	compiledStore := &compiledRunStoreFake{
		snapshot: snapshot, authorize: true, authorityOrder: order,
	}
	observationStore := &observationRuntimeStoreFake{authorityOrder: order}
	pusher := &authorityCheckingPusher{order: order}
	a := NewActivities(
		fakeFetcher{},
		fakeScorer{},
		fakeCardGen{},
		pusher,
		new(fakeStore),
		fakeFeishu{},
		nil,
		nil,
		func(feedback.AggregateCardInput) string { return `{}` },
		func(string, int) (string, string) { return "title", "blue" },
		WithCompiledRuntimeV1(
			compiledStore,
			func(context.Context, int64, bool) (runtimepolicy.BundleV1, error) {
				return runtimepolicy.BundleV1{}, nil
			},
			new(compiledModelResolverFake),
		),
		WithObservationRuntime(observationStore, nil, "", ""),
	)
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	env.RegisterActivity(a.Push)
	err := executePushActivity(t, env, a, PushIn{
		UserID: identity.UserID, ScheduleID: identity.TaskID, TraceID: "trace-authority-order",
		Run: &CompiledRunInputV1{
			TenantID: identity.TenantID,
			TaskID:   identity.TaskID,
			Snapshot: ref,
		},
		Cards: []GeneratedCard{{
			Scored: types.ScoredItem{
				Item: types.ContentItem{
					ID:                      501,
					SourceID:                10,
					Title:                   "item",
					ObservationPolicyDigest: "policy-digest",
					ObservationEventKey:     "event-key",
					ObservationEventJSON: json.RawMessage(
						`{"event_type":"release","subject":"vane","occurred_at":"2026-07-25T12:00:00Z"}`,
					),
				},
				Score: 88,
			},
			BodyMD: "body",
		}},
	})
	if err != nil {
		t.Fatalf("compiled Push failed: %v", err)
	}
	if order.reserveBefore.Load() ||
		order.deliveryBefore.Load() ||
		order.providerBefore.Load() ||
		order.batchDoneBefore.Load() {
		t.Fatalf(
			"delivery effect preceded authority claim: reserve=%t delivery=%t provider=%t batch_done=%t",
			order.reserveBefore.Load(),
			order.deliveryBefore.Load(),
			order.providerBefore.Load(),
			order.batchDoneBefore.Load(),
		)
	}
	compiledStore.mu.Lock()
	authorityCalls := compiledStore.authorityCalls
	authorityScopes := append([]types.PushBatchScope(nil), compiledStore.authorityScopes...)
	authorityDesired := append(
		[]types.PushBatchDeliveryAuthority(nil),
		compiledStore.authorityDesired...,
	)
	deliveryWrites := compiledStore.deliveryWrites
	compiledStore.mu.Unlock()
	if authorityCalls != 1 {
		t.Fatalf("authority claims = %d, want exactly 1", authorityCalls)
	}
	wantScope := types.PushBatchScope{
		TenantID: identity.TenantID,
		UserID:   identity.UserID,
		BatchID:  101,
	}
	if !reflect.DeepEqual(authorityScopes, []types.PushBatchScope{wantScope}) {
		t.Fatalf("authority scopes = %+v, want %+v", authorityScopes, wantScope)
	}
	if !reflect.DeepEqual(
		authorityDesired,
		[]types.PushBatchDeliveryAuthority{types.PushBatchDeliveryAuthorityLegacy},
	) {
		t.Fatalf("desired authority = %v, want legacy", authorityDesired)
	}
	observationStore.mu.Lock()
	reserveCalls := observationStore.reserveCalls
	reserveBatchIDs := append(
		[]int64(nil),
		observationStore.reserveBatchIDs...,
	)
	bindBatchIDs := append(
		[]int64(nil),
		observationStore.bindBatchIDs...,
	)
	bindDeliveryIDs := append(
		[]int64(nil),
		observationStore.bindDeliveryIDs...,
	)
	observationStore.mu.Unlock()
	if reserveCalls != 1 || deliveryWrites != 1 || pusher.calls.Load() != 1 {
		t.Fatalf(
			"delivery effects = reserve:%d insert:%d provider:%d, want 1/1/1",
			reserveCalls,
			deliveryWrites,
			pusher.calls.Load(),
		)
	}
	if !reflect.DeepEqual(reserveBatchIDs, []int64{101}) ||
		!reflect.DeepEqual(bindBatchIDs, []int64{101}) ||
		!reflect.DeepEqual(bindDeliveryIDs, []int64{201}) {
		t.Fatalf(
			"observation batch binding reserve=%v bind=%v deliveries=%v, want [101]/[101]/[201]",
			reserveBatchIDs,
			bindBatchIDs,
			bindDeliveryIDs,
		)
	}
}

func TestPush_CompiledEffectAuthorityWinnerStopsAllDeliveryEffects(t *testing.T) {
	identity, ref, snapshot := compiledActivityFixture("Frozen Task")
	compiledStore := &compiledRunStoreFake{
		snapshot:        snapshot,
		authorize:       true,
		authorityWinner: types.PushBatchDeliveryAuthorityEffect,
	}
	observationStore := new(observationRuntimeStoreFake)
	legacyStore := &effectCountingStore{fakeStore: new(fakeStore)}
	pusher := &fakePusher{msgID: "must-not-send"}
	a := NewActivities(
		fakeFetcher{},
		fakeScorer{},
		fakeCardGen{},
		pusher,
		legacyStore,
		fakeFeishu{},
		nil,
		nil,
		func(feedback.AggregateCardInput) string { return `{}` },
		func(string, int) (string, string) { return "title", "blue" },
		WithCompiledRuntimeV1(
			compiledStore,
			func(context.Context, int64, bool) (runtimepolicy.BundleV1, error) {
				return runtimepolicy.BundleV1{}, nil
			},
			new(compiledModelResolverFake),
		),
		WithObservationRuntime(observationStore, nil, "", ""),
	)
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	env.RegisterActivity(a.Push)
	err := executePushActivity(t, env, a, PushIn{
		UserID: identity.UserID, ScheduleID: identity.TaskID, TraceID: "trace-effect-winner",
		Run: &CompiledRunInputV1{
			TenantID: identity.TenantID,
			TaskID:   identity.TaskID,
			Snapshot: ref,
		},
		Cards: []GeneratedCard{{
			Scored: types.ScoredItem{
				Item: types.ContentItem{
					ID:                      501,
					SourceID:                10,
					Title:                   "item",
					ObservationPolicyDigest: "policy-digest",
					ObservationEventKey:     "event-key",
					ObservationEventJSON: json.RawMessage(
						`{"event_type":"release","subject":"vane","occurred_at":"2026-07-25T12:00:00Z"}`,
					),
				},
				Score: 88,
			},
			BodyMD: "body",
		}},
	})
	if err == nil {
		t.Fatal("effect authority winner must fail the legacy delivery path closed")
	}
	var appErr *temporal.ApplicationError
	if !errors.As(err, &appErr) || !appErr.NonRetryable() {
		t.Fatalf("effect authority conflict must be non-retryable: %T %v", err, err)
	}
	observationStore.mu.Lock()
	reserveCalls := observationStore.reserveCalls
	observationStore.mu.Unlock()
	compiledStore.mu.Lock()
	claims := compiledStore.authorityCalls
	batches := compiledStore.batchWrites
	inserts := compiledStore.deliveryWrites
	receipts := compiledStore.deliveryReceipts
	statuses := compiledStore.batchStatuses
	compiledStore.mu.Unlock()
	if claims != 1 || batches != 1 {
		t.Fatalf("authority path not reached exactly once: claims=%d batches=%d", claims, batches)
	}
	if reserveCalls != 0 || inserts != 0 || len(pusher.sentCards()) != 0 ||
		receipts != 0 || statuses != 0 {
		t.Fatalf(
			"effect winner leaked legacy effects: reserve=%d insert=%d provider=%d delivery_receipt=%d batch_receipt=%d",
			reserveCalls,
			inserts,
			len(pusher.sentCards()),
			receipts,
			statuses,
		)
	}
	_, _, legacyBatches, legacyInserts, legacyMarks, legacyStatuses, _ := legacyStore.effectCounts()
	if legacyBatches != 0 || legacyInserts != 0 || legacyMarks != 0 || legacyStatuses != 0 {
		t.Fatalf(
			"effect winner escaped to legacy store: batches=%d inserts=%d marks=%d statuses=%d",
			legacyBatches,
			legacyInserts,
			legacyMarks,
			legacyStatuses,
		)
	}
}

func TestPush_CompiledAuthorityClaimFailureStopsAllDeliveryEffects(t *testing.T) {
	identity, ref, snapshot := compiledActivityFixture("Frozen Task")
	compiledStore := &compiledRunStoreFake{
		snapshot:  snapshot,
		authorize: true,
		authorityErr: types.NewAppError(
			types.CodeDatabase,
			"claim push batch delivery authority",
			errors.New("database unavailable"),
		),
	}
	legacyStore := &effectCountingStore{fakeStore: new(fakeStore)}
	pusher := &fakePusher{msgID: "must-not-send"}
	a := NewActivities(
		fakeFetcher{},
		fakeScorer{},
		fakeCardGen{},
		pusher,
		legacyStore,
		fakeFeishu{},
		nil,
		nil,
		func(feedback.AggregateCardInput) string { return `{}` },
		func(string, int) (string, string) { return "title", "blue" },
		WithCompiledRuntimeV1(
			compiledStore,
			func(context.Context, int64, bool) (runtimepolicy.BundleV1, error) {
				return runtimepolicy.BundleV1{}, nil
			},
			new(compiledModelResolverFake),
		),
	)
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	env.RegisterActivity(a.Push)
	err := executePushActivity(t, env, a, PushIn{
		UserID: identity.UserID, ScheduleID: identity.TaskID, TraceID: "trace-authority-error",
		Run: &CompiledRunInputV1{
			TenantID: identity.TenantID,
			TaskID:   identity.TaskID,
			Snapshot: ref,
		},
		Cards: []GeneratedCard{{
			Scored: types.ScoredItem{
				Item:  types.ContentItem{ID: 501, SourceID: 10, Title: "item"},
				Score: 88,
			},
			BodyMD: "body",
		}},
	})
	if err == nil {
		t.Fatal("authority claim error must fail the legacy delivery path closed")
	}
	var appErr *temporal.ApplicationError
	if !errors.As(err, &appErr) || appErr.NonRetryable() {
		t.Fatalf("database authority failure must remain retryable: %T %v", err, err)
	}
	compiledStore.mu.Lock()
	claims := compiledStore.authorityCalls
	batches := compiledStore.batchWrites
	inserts := compiledStore.deliveryWrites
	receipts := compiledStore.deliveryReceipts
	statuses := compiledStore.batchStatuses
	compiledStore.mu.Unlock()
	if claims != 1 || batches != 1 {
		t.Fatalf("authority error path not reached exactly once: claims=%d batches=%d", claims, batches)
	}
	if inserts != 0 || len(pusher.sentCards()) != 0 || receipts != 0 || statuses != 0 {
		t.Fatalf(
			"authority error leaked delivery effects: inserts=%d provider=%d delivery_receipts=%d batch_receipts=%d",
			inserts,
			len(pusher.sentCards()),
			receipts,
			statuses,
		)
	}
	_, _, legacyBatches, legacyInserts, legacyMarks, legacyStatuses, _ := legacyStore.effectCounts()
	if legacyBatches != 0 || legacyInserts != 0 || legacyMarks != 0 || legacyStatuses != 0 {
		t.Fatalf(
			"authority error escaped to legacy store: batches=%d inserts=%d marks=%d statuses=%d",
			legacyBatches,
			legacyInserts,
			legacyMarks,
			legacyStatuses,
		)
	}
}

func TestPush_CompiledRevocationImmediatelyBeforeExternalSendStopsPush(t *testing.T) {
	identity, ref, snapshot := compiledActivityFixture("Frozen Task")
	compiledStore := &compiledRunStoreFake{
		snapshot: snapshot,
		// Authorize batch creation and delivery insertion, then revoke at the
		// final check immediately before the external Feishu send.
		authorizeScript: []bool{true, true, false},
	}
	legacyStore := &effectCountingStore{fakeStore: new(fakeStore)}
	pusher := &fakePusher{msgID: "must-not-send"}
	a := NewActivities(fakeFetcher{}, fakeScorer{}, fakeCardGen{}, pusher, legacyStore, fakeFeishu{}, nil, nil,
		func(feedback.AggregateCardInput) string { return `{}` },
		func(string, int) (string, string) { return "title", "blue" },
		WithCompiledRuntimeV1(compiledStore,
			func(context.Context, int64, bool) (runtimepolicy.BundleV1, error) {
				return runtimepolicy.BundleV1{}, nil
			},
			new(compiledModelResolverFake)))
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	env.RegisterActivity(a.Push)
	err := executePushActivity(t, env, a, PushIn{
		UserID: identity.UserID, ScheduleID: identity.TaskID, TraceID: "trace-revoked",
		Run: &CompiledRunInputV1{TenantID: identity.TenantID, TaskID: identity.TaskID, Snapshot: ref},
		Cards: []GeneratedCard{{
			Scored: types.ScoredItem{Item: types.ContentItem{ID: 501, SourceID: 10, Title: "item"}, Score: 88},
			BodyMD: "body",
		}},
	})
	if err == nil {
		t.Fatal("revoked compiled Push must fail closed")
	}
	if got := len(pusher.sentCards()); got != 0 {
		t.Fatalf("revoked run sent %d external cards", got)
	}
	_, _, legacyBatches, legacyInserts, legacyMarks, legacyStatuses, _ := legacyStore.effectCounts()
	if legacyBatches != 0 || legacyInserts != 0 || legacyMarks != 0 || legacyStatuses != 0 {
		t.Fatalf("compiled push escaped to legacy writes: batches=%d inserts=%d marks=%d statuses=%d",
			legacyBatches, legacyInserts, legacyMarks, legacyStatuses)
	}
	compiledStore.mu.Lock()
	batches, inserts := compiledStore.batchWrites, compiledStore.deliveryWrites
	marks, statuses := compiledStore.deliveryReceipts, compiledStore.batchStatuses
	compiledStore.mu.Unlock()
	if batches != 1 || inserts != 1 {
		t.Fatalf("test never reached final pre-send authorization: batches=%d inserts=%d", batches, inserts)
	}
	if marks != 0 || statuses != 0 {
		t.Fatalf("revoked pre-send run wrote post-send receipts: marks=%d statuses=%d", marks, statuses)
	}
}

func TestNotifyEmptyResult_CompiledBuildsBeforeFinalAuthorization(t *testing.T) {
	identity, ref, snapshot := compiledActivityFixture("Frozen Task")
	compiledStore := &compiledRunStoreFake{snapshot: snapshot, authorize: true}
	pusher := &fakePusher{msgID: "must-not-send"}
	buildCalls := 0
	a := NewActivities(fakeFetcher{}, fakeScorer{}, fakeCardGen{}, pusher,
		new(fakeStore), fakeFeishu{}, nil,
		func(string) string {
			buildCalls++
			// Model revocation in the local preparation window. Correct order is
			// build -> live authorization (denied) -> zero external sends. If the
			// authorization moves above the builder, this mutation sends a card.
			compiledStore.mu.Lock()
			compiledStore.authorize = false
			compiledStore.mu.Unlock()
			return `{}`
		}, nil, nil,
		WithCompiledRuntimeV1(compiledStore,
			func(context.Context, int64, bool) (runtimepolicy.BundleV1, error) {
				return runtimepolicy.BundleV1{}, nil
			},
			new(compiledModelResolverFake)))
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	env.RegisterActivity(a.NotifyEmptyResult)
	_, err := env.ExecuteActivity(a.NotifyEmptyResult, NotifyEmptyIn{
		UserID: identity.UserID, TraceID: "trace-empty-revoked", Gate: types.BatchExitGateFetch,
		Run: &CompiledRunInputV1{TenantID: identity.TenantID, TaskID: identity.TaskID, Snapshot: ref},
	})
	if err == nil {
		t.Fatal("revocation after card preparation must fail closed")
	}
	if buildCalls != 1 {
		t.Fatalf("notice build calls = %d, want 1 before authorization", buildCalls)
	}
	if got := len(pusher.sentCards()); got != 0 {
		t.Fatalf("revoked empty-result notification sent %d cards", got)
	}
}

func TestPush_CompiledDeliveryReceiptFailurePreventsBatchDone(t *testing.T) {
	identity, ref, snapshot := compiledActivityFixture("Frozen Task")
	compiledStore := &compiledRunStoreFake{
		snapshot:  snapshot,
		authorize: true,
		deliveryReceiptErr: types.NewAppError(
			types.CodeDatabase, "record compiled delivery receipt", errors.New("connection reset")),
	}
	legacyStore := &effectCountingStore{fakeStore: new(fakeStore)}
	pusher := &fakePusher{msgID: "om-receipt-failed"}
	a := NewActivities(fakeFetcher{}, fakeScorer{}, fakeCardGen{}, pusher, legacyStore, fakeFeishu{}, nil, nil,
		func(feedback.AggregateCardInput) string { return `{}` },
		func(string, int) (string, string) { return "title", "blue" },
		WithCompiledRuntimeV1(compiledStore,
			func(context.Context, int64, bool) (runtimepolicy.BundleV1, error) {
				return runtimepolicy.BundleV1{}, nil
			},
			new(compiledModelResolverFake)))
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	env.RegisterActivity(a.Push)
	err := executePushActivity(t, env, a, PushIn{
		UserID: identity.UserID, ScheduleID: identity.TaskID, TraceID: "trace-receipt-failed",
		Run: &CompiledRunInputV1{TenantID: identity.TenantID, TaskID: identity.TaskID, Snapshot: ref},
		Cards: []GeneratedCard{{
			Scored: types.ScoredItem{Item: types.ContentItem{
				ID: 501, SourceID: 10, Title: "item",
			}, Score: 88},
			BodyMD: "body",
		}},
	})
	if err == nil {
		t.Fatal("compiled delivery receipt failure must fail the Activity")
	}
	var appErr *temporal.ApplicationError
	if !errors.As(err, &appErr) {
		t.Fatalf("database receipt failure must cross Activity boundary as ApplicationError: %T %v", err, err)
	}
	if appErr.NonRetryable() {
		t.Fatalf("database receipt failure must remain retryable: %v", err)
	}
	if got := len(pusher.sentCards()); got != 1 {
		t.Fatalf("external pushes = %d, want the one send preceding receipt failure", got)
	}
	compiledStore.mu.Lock()
	batches, inserts := compiledStore.batchWrites, compiledStore.deliveryWrites
	receipts, statuses := compiledStore.deliveryReceipts, compiledStore.batchStatuses
	compiledStore.mu.Unlock()
	if batches != 1 || inserts != 1 || receipts != 1 {
		t.Fatalf("compiled write path not exercised: batches=%d inserts=%d receipts=%d",
			batches, inserts, receipts)
	}
	if statuses != 0 {
		t.Fatalf("receipt failure incorrectly finalized %d batches", statuses)
	}
	_, _, legacyBatches, legacyInserts, legacyMarks, legacyStatuses, _ := legacyStore.effectCounts()
	if legacyBatches != 0 || legacyInserts != 0 || legacyMarks != 0 || legacyStatuses != 0 {
		t.Fatalf("compiled receipt failure escaped to legacy writes: batches=%d inserts=%d marks=%d statuses=%d",
			legacyBatches, legacyInserts, legacyMarks, legacyStatuses)
	}
}

func TestPush_CompiledRecoveryOnlyAfterRevocationMarksDoneWithoutResend(t *testing.T) {
	identity, ref, snapshot := compiledActivityFixture("Frozen Task")
	order := new(pushAuthorityOrder)
	compiledStore := &compiledRunStoreFake{
		snapshot: snapshot, recoveryOnly: true, authorize: false,
		authorityOrder: order,
	}
	legacyStore := &effectCountingStore{fakeStore: new(fakeStore)}
	pusher := &fakePusher{msgID: "must-not-resend"}
	a := NewActivities(fakeFetcher{}, fakeScorer{}, fakeCardGen{}, pusher, legacyStore, noOwnerFeishu{}, nil, nil,
		func(feedback.AggregateCardInput) string { return `{}` },
		func(string, int) (string, string) { return "title", "blue" },
		WithCompiledRuntimeV1(compiledStore,
			func(context.Context, int64, bool) (runtimepolicy.BundleV1, error) {
				return runtimepolicy.BundleV1{}, nil
			},
			new(compiledModelResolverFake)))
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	env.RegisterActivity(a.Push)
	err := executePushActivity(t, env, a, PushIn{
		UserID: identity.UserID, ScheduleID: identity.TaskID, TraceID: "trace-recovery-only",
		Run: &CompiledRunInputV1{TenantID: identity.TenantID, TaskID: identity.TaskID, Snapshot: ref},
		Cards: []GeneratedCard{{
			Scored: types.ScoredItem{Item: types.ContentItem{
				ID: 501, SourceID: 10, Title: "item",
			}, Score: 88},
			BodyMD: "body",
		}},
	})
	if err != nil {
		t.Fatalf("recovery-only compiled Push failed: %v", err)
	}
	if got := len(pusher.sentCards()); got != 0 {
		t.Fatalf("recovery-only retry resent %d external cards", got)
	}
	compiledStore.mu.Lock()
	recoveries, authorizations := compiledStore.recoveryCalls, len(compiledStore.authorizeIdentities)
	authorityCalls := compiledStore.authorityCalls
	batches, inserts := compiledStore.batchWrites, compiledStore.deliveryWrites
	receipts, statuses := compiledStore.deliveryReceipts, compiledStore.batchStatuses
	compiledStore.mu.Unlock()
	if recoveries != 1 || authorizations != 0 {
		t.Fatalf("recovery path calls = %d, live authorizations = %d; want 1 and 0",
			recoveries, authorizations)
	}
	if authorityCalls != 1 || order.batchDoneBefore.Load() {
		t.Fatalf(
			"recovery authority order: claims=%d batch_done_before_claim=%t, want 1/false",
			authorityCalls,
			order.batchDoneBefore.Load(),
		)
	}
	if batches != 0 || inserts != 0 || receipts != 0 || statuses != 1 {
		t.Fatalf("recovery-only effects: batches=%d inserts=%d receipts=%d statuses=%d",
			batches, inserts, receipts, statuses)
	}
	_, _, legacyBatches, legacyInserts, legacyMarks, legacyStatuses, _ := legacyStore.effectCounts()
	if legacyBatches != 0 || legacyInserts != 0 || legacyMarks != 0 || legacyStatuses != 0 {
		t.Fatalf("compiled recovery escaped to legacy writes: batches=%d inserts=%d marks=%d statuses=%d",
			legacyBatches, legacyInserts, legacyMarks, legacyStatuses)
	}
}
