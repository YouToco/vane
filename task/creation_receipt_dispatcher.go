package task

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/YouToco/vane/internal/strictjson"
	"github.com/YouToco/vane/types"
)

const (
	// AgentAutoReceiptProvider identifies a durable operation authorized by the
	// owner's current natural-language request. Completion is recorded in the
	// Agent session; there is no confirmation UI or external message mutation.
	AgentAutoReceiptProvider = "agent_auto/v1"

	creationReceiptPayloadVersion = "vane.task-creation-session-receipt/v2"
	creationReceiptPollInterval   = 2 * time.Second
	// Session checkpoint uses a non-blocking attempt on Agent's per-user mutex;
	// a busy conversation releases the receipt for retry instead of holding the
	// global scan. One minute covers the bounded DB checkpoints while keeping
	// crash takeover latency short.
	creationReceiptLeaseDuration  = time.Minute
	creationReceiptStoreTimeout   = 5 * time.Second
	creationReceiptTenantLimit    = 100
	creationReceiptPerTenantLimit = 64
	creationReceiptConcurrency    = 4
)

func AgentAutoReceiptTarget(actionID string) CreationReceiptTarget {
	return CreationReceiptTarget{
		Provider: AgentAutoReceiptProvider,
		Target:   actionID,
	}
}

func validAgentAutoReceiptTarget(provider, target, actionID string) bool {
	return provider == AgentAutoReceiptProvider &&
		strings.TrimSpace(target) != "" &&
		target == strings.TrimSpace(target) &&
		strings.TrimSpace(actionID) != "" &&
		actionID == strings.TrimSpace(actionID)
}

// CreationReceiptStore is the durable A6 outbox boundary. Every mutable method
// is fenced by the receipt lease; a process may die after any method and a
// later owner can safely continue from the persisted checkpoint.
type CreationReceiptStore interface {
	ListDueTaskCreationReceiptTenantIDs(
		ctx context.Context, before time.Time, afterTenantID int64, limit int,
	) ([]int64, error)
	ListDueTaskCreationReceipts(
		ctx context.Context, tenantID int64, before time.Time, limit int,
	) ([]types.TaskCreationReceipt, error)
	AcquireTaskCreationReceipt(
		ctx context.Context, p types.AcquireTaskCreationReceiptParams,
	) (*types.TaskCreationReceipt, error)
	CheckpointTaskCreationReceiptPayload(
		ctx context.Context, lease types.TaskCreationReceiptLease,
		payload []byte, digest string,
	) error
	MarkTaskCreationReceiptSent(
		ctx context.Context, lease types.TaskCreationReceiptLease,
		providerMessageID string,
	) error
	RecordTaskCreationReceiptSendFailure(
		ctx context.Context, p types.RecordTaskCreationReceiptSendFailureParams,
	) error
}

// CreationReceiptSessionRecorder serializes the outbox append with Agent's
// per-user load/modify/save lock. Its store transaction appends the message and
// marks session_recorded_at together, making a lost database response replayable.
type CreationReceiptSessionRecorder interface {
	RecordCreationReceiptSession(
		ctx context.Context,
		receipt types.TaskCreationReceipt,
		messages json.RawMessage,
	) error
}

type CreationReceiptDispatcherDeps struct {
	Store    CreationReceiptStore
	Sessions CreationReceiptSessionRecorder
	Logger   *slog.Logger
}

// CreationReceiptDispatcher drains terminal task-creation receipts. It owns no
// business mutation: terminal rows are produced atomically by the A5 saga;
// this component only freezes and records the terminal conversation fact.
type CreationReceiptDispatcher struct {
	store    CreationReceiptStore
	sessions CreationReceiptSessionRecorder
	logger   *slog.Logger

	// A pass is serialized and advances a tenant keyset cursor. When it reaches
	// the end it wraps to zero, so a permanently busy low-ID tenant cannot starve
	// later shards and concurrent manual/recovery passes cannot duplicate scans.
	dispatchMu sync.Mutex
	cursor     int64
}

func NewCreationReceiptDispatcher(
	d CreationReceiptDispatcherDeps,
) (*CreationReceiptDispatcher, error) {
	if d.Store == nil || d.Sessions == nil {
		return nil, errors.New("task: creation receipt dispatcher dependencies are incomplete")
	}
	if d.Logger == nil {
		d.Logger = slog.Default()
	}
	return &CreationReceiptDispatcher{
		store: d.Store, sessions: d.Sessions, logger: d.Logger,
	}, nil
}

// Run performs an immediate recovery pass, then keeps polling until ctx is
// cancelled. Each pass is bounded and completes before the next one starts.
func (d *CreationReceiptDispatcher) Run(ctx context.Context) {
	if d == nil {
		return
	}
	d.dispatchAndLog(ctx)
	ticker := time.NewTicker(creationReceiptPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			d.dispatchAndLog(ctx)
		}
	}
}

func (d *CreationReceiptDispatcher) dispatchAndLog(ctx context.Context) {
	if err := d.DispatchOnce(ctx); err != nil && !errors.Is(err, context.Canceled) {
		d.logger.ErrorContext(ctx, "task creation receipt dispatch pass failed", "err", err)
	}
}

// DispatchOnce drains one bounded snapshot. The caller may invoke it directly
// in crash/replay tests; production uses Run.
func (d *CreationReceiptDispatcher) DispatchOnce(ctx context.Context) error {
	if d == nil || d.store == nil || d.sessions == nil {
		return errors.New("task: creation receipt dispatcher dependencies are incomplete")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	d.dispatchMu.Lock()
	defer d.dispatchMu.Unlock()

	// Store clamps this future boundary to clock_timestamp(). Passing a future
	// value prevents a slow process clock from hiding already-due database work.
	boundary := time.Now().Add(24 * time.Hour)
	tenantIDs, err := d.store.ListDueTaskCreationReceiptTenantIDs(
		ctx, boundary, d.cursor, creationReceiptTenantLimit,
	)
	if err != nil {
		return fmt.Errorf("list receipt tenant shards: %w", err)
	}
	if len(tenantIDs) == 0 && d.cursor > 0 {
		tenantIDs, err = d.store.ListDueTaskCreationReceiptTenantIDs(
			ctx, boundary, 0, creationReceiptTenantLimit,
		)
		if err != nil {
			return fmt.Errorf("wrap receipt tenant shards: %w", err)
		}
	}
	if len(tenantIDs) > 0 {
		d.cursor = tenantIDs[len(tenantIDs)-1]
	}

	sem := make(chan struct{}, creationReceiptConcurrency)
	var wg sync.WaitGroup
	var errsMu sync.Mutex
	var errs []error
	appendErr := func(err error) {
		errsMu.Lock()
		errs = append(errs, err)
		errsMu.Unlock()
	}
	for _, tenantID := range tenantIDs {
		receipts, listErr := d.store.ListDueTaskCreationReceipts(
			ctx, tenantID, boundary, creationReceiptPerTenantLimit,
		)
		if listErr != nil {
			appendErr(fmt.Errorf("list tenant %d receipts: %w", tenantID, listErr))
			continue
		}
		for i := range receipts {
			receipt := receipts[i]
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				wg.Wait()
				appendErr(ctx.Err())
				return errors.Join(errs...)
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() { <-sem }()
				if dispatchErr := d.dispatchReceipt(ctx, receipt); dispatchErr != nil &&
					!errors.Is(dispatchErr, types.ErrTaskCreationReceiptBusy) &&
					!errors.Is(dispatchErr, types.ErrTaskCreationReceiptTerminal) {
					appendErr(fmt.Errorf("receipt %d: %w", receipt.ID, dispatchErr))
				}
			}()
		}
	}
	wg.Wait()
	return errors.Join(errs...)
}

func (d *CreationReceiptDispatcher) dispatchReceipt(
	ctx context.Context,
	listed types.TaskCreationReceipt,
) error {
	owner := "receipt-" + uuid.NewString()
	receipt, err := d.store.AcquireTaskCreationReceipt(ctx,
		types.AcquireTaskCreationReceiptParams{
			ID: listed.ID, TenantID: listed.TenantID, UserID: listed.UserID,
			LeaseOwner: owner, LeaseDuration: creationReceiptLeaseDuration,
		})
	if err != nil {
		return err
	}
	lease := receipt.Lease()

	payload, err := d.loadOrCheckpointPayload(ctx, receipt)
	if err != nil {
		return d.finishFailure(ctx, lease, err, false)
	}

	if receipt.SessionRecordedAt == nil {
		if err := d.sessions.RecordCreationReceiptSession(
			ctx, *receipt, payload.SessionMessages,
		); err != nil {
			return d.finishFailure(ctx, lease, err, false)
		}
	}

	if !validAgentAutoReceiptTarget(
		receipt.Provider, receipt.Target, receipt.OperationID,
	) {
		return d.finishFailure(
			ctx,
			lease,
			types.NewAppError(
				types.CodeValidation,
				"task creation receipt provider is retired",
				nil,
			),
			false,
		)
	}

	if err := d.store.MarkTaskCreationReceiptSent(ctx, lease, receipt.Target); err != nil {
		return err
	}
	return nil
}

type creationUserReceiptPayload struct {
	Version         string          `json:"version"`
	SessionMessages json.RawMessage `json:"session_messages"`
}

func (d *CreationReceiptDispatcher) loadOrCheckpointPayload(
	ctx context.Context,
	receipt *types.TaskCreationReceipt,
) (creationUserReceiptPayload, error) {
	if len(receipt.Payload) != 0 {
		return decodeCreationUserReceiptPayload(receipt.Payload, receipt.PayloadDigest)
	}
	_, history, err := renderCreationUserReceipt(*receipt)
	if err != nil {
		return creationUserReceiptPayload{}, err
	}
	messages, err := json.Marshal([]struct {
		Role    string `json:"role"`
		Content string `json:"content"`
	}{{Role: "user", Content: history}})
	if err != nil {
		return creationUserReceiptPayload{}, fmt.Errorf("marshal receipt session message: %w", err)
	}
	payload := creationUserReceiptPayload{
		Version:         creationReceiptPayloadVersion,
		SessionMessages: messages,
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return creationUserReceiptPayload{}, fmt.Errorf("marshal creation receipt payload: %w", err)
	}
	sum := sha256.Sum256(raw)
	digest := hex.EncodeToString(sum[:])
	if err := d.store.CheckpointTaskCreationReceiptPayload(
		ctx, receipt.Lease(), raw, digest,
	); err != nil {
		return creationUserReceiptPayload{}, err
	}
	receipt.Payload = raw
	receipt.PayloadDigest = digest
	return payload, nil
}

func decodeCreationUserReceiptPayload(
	raw []byte,
	digest string,
) (creationUserReceiptPayload, error) {
	sum := sha256.Sum256(raw)
	if digest == "" || hex.EncodeToString(sum[:]) != digest {
		return creationUserReceiptPayload{},
			types.NewAppError(types.CodeValidation, "task creation receipt payload digest differs", nil)
	}
	var payload creationUserReceiptPayload
	if err := strictjson.Decode(raw, &payload); err != nil {
		return creationUserReceiptPayload{},
			types.NewAppError(types.CodeValidation, "task creation receipt payload is invalid", err)
	}
	if payload.Version != creationReceiptPayloadVersion ||
		len(payload.SessionMessages) == 0 {
		return creationUserReceiptPayload{},
			types.NewAppError(types.CodeValidation, "task creation receipt payload fields are invalid", nil)
	}
	var messages []json.RawMessage
	if err := strictjson.Decode(payload.SessionMessages, &messages); err != nil || len(messages) != 1 {
		return creationUserReceiptPayload{},
			types.NewAppError(types.CodeValidation, "task creation receipt session payload is invalid", err)
	}
	return payload, nil
}

func renderCreationUserReceipt(
	receipt types.TaskCreationReceipt,
) (display string, history string, err error) {
	expectedPhase := map[types.TaskOperationStatus]types.TaskCreationPhase{
		types.TaskOperationStatusExecuted:  types.TaskCreationPhaseCompleted,
		types.TaskOperationStatusCancelled: types.TaskCreationPhaseCancelled,
		types.TaskOperationStatusExpired:   types.TaskCreationPhaseExpired,
		types.TaskOperationStatusBlocked:   types.TaskCreationPhaseBlocked,
		types.TaskOperationStatusFailed:    types.TaskCreationPhaseFailed,
	}[receipt.OperationStatus]
	if expectedPhase == "" || receipt.OperationPhase != expectedPhase {
		return "", "", types.NewAppError(types.CodeValidation,
			"task creation receipt references a non-terminal operation", nil)
	}

	switch receipt.OperationStatus {
	case types.TaskOperationStatusExecuted:
		var checkpoint creationSuccessCheckpoint
		if err := decodeStrictJSON(receipt.Result, &checkpoint); err != nil ||
			checkpoint.Version != creationResultVersion ||
			strings.TrimSpace(receipt.TaskID) == "" || checkpoint.TaskID != receipt.TaskID ||
			strings.TrimSpace(checkpoint.Message) == "" {
			return "", "", types.NewAppError(types.CodeValidation,
				"task creation receipt success checkpoint is invalid", err)
		}
		display = strings.TrimSpace(checkpoint.Message)
		// Session history is a privileged input on the next Agent turn. Never
		// persist operation summary, target title/URL/config, or errors here:
		// those fields may contain external content. Record only the fixed state.
		history = "[Agent执行] 用户已在当前消息中明确要求，任务已成功创建。"
	case types.TaskOperationStatusCancelled:
		display = "已取消本次任务创建。"
		history = "[Agent执行] 任务创建操作已取消。"
	case types.TaskOperationStatusExpired:
		display = "任务创建操作已过期，请重新描述需求。"
		history = "[Agent执行] 任务创建操作已过期，任务未创建。"
	case types.TaskOperationStatusBlocked, types.TaskOperationStatusFailed:
		display = strings.TrimSpace(receipt.ErrorMessage)
		if strings.TrimSpace(receipt.ErrorCode) == "" || display == "" {
			return "", "", types.NewAppError(types.CodeValidation,
				"task creation receipt failure checkpoint is invalid", nil)
		}
		history = "[Agent执行] 用户已在当前消息中明确要求，但任务创建已安全停止，任务未创建。"
	}
	return display, history, nil
}

func (d *CreationReceiptDispatcher) finishFailure(
	ctx context.Context,
	lease types.TaskCreationReceiptLease,
	cause error,
	_ bool,
) error {
	class := types.TaskCreationReceiptFailureRetryable
	retryAfter := creationReceiptBackoff(1)
	var appErr *types.AppError
	if errors.As(cause, &appErr) && !appErr.Retryable {
		class = types.TaskCreationReceiptFailurePermanent
		retryAfter = 0
	}
	if lease.Fence > 0 {
		// Fence starts at one and attempt advances with every acquisition. The
		// current types intentionally keep attempt off Lease, so use fence as the
		// monotonic backoff exponent; stale workers cannot checkpoint it anyway.
		retryAfter = creationReceiptBackoff(lease.Fence)
		if class == types.TaskCreationReceiptFailurePermanent {
			retryAfter = 0
		}
	}

	checkpointCtx, cancel := context.WithTimeout(
		context.WithoutCancel(ctx), creationReceiptStoreTimeout,
	)
	defer cancel()
	checkpointErr := d.store.RecordTaskCreationReceiptSendFailure(
		checkpointCtx,
		types.RecordTaskCreationReceiptSendFailureParams{
			Lease: lease, Class: class, RetryAfter: retryAfter,
		},
	)
	if checkpointErr != nil {
		return errors.Join(cause, fmt.Errorf("checkpoint receipt failure: %w", checkpointErr))
	}
	return cause
}

func creationReceiptBackoff(attempt int64) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	if attempt > 9 {
		attempt = 9
	}
	backoff := 5 * time.Second * time.Duration(1<<(attempt-1))
	if backoff > 15*time.Minute {
		return 15 * time.Minute
	}
	return backoff
}
