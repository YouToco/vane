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
	// FeishuCardPatchReceiptProvider identifies the only A6 delivery adapter.
	// The original confirmation message is replaced in place, so replaying an
	// ambiguous PATCH cannot create a second user-visible message.
	FeishuCardPatchReceiptProvider = "feishu_card_patch"

	creationReceiptPayloadVersion = "vane.task-creation-user-receipt/v1"
	creationReceiptPollInterval   = 2 * time.Second
	// Session checkpoint uses a non-blocking attempt on Agent's per-user mutex;
	// a busy conversation releases the receipt for retry instead of holding the
	// global scan. One minute covers the bounded provider call and DB checkpoints
	// while keeping crash takeover latency short.
	creationReceiptLeaseDuration  = time.Minute
	creationReceiptSendTimeout    = 10 * time.Second
	creationReceiptStoreTimeout   = 5 * time.Second
	creationReceiptTenantLimit    = 100
	creationReceiptPerTenantLimit = 64
	creationReceiptConcurrency    = 4
	maxCreationReceiptCardBytes   = 30 << 10
)

// FeishuCardPatchReceiptProviderForApp binds a receipt to the Feishu app that
// emitted the original card without persisting the app ID itself. Secret
// rotation under the same app remains compatible; switching apps cannot make a
// new credential mutate an old app's message resource.
func FeishuCardPatchReceiptProviderForApp(appID string) string {
	appID = strings.TrimSpace(appID)
	if appID == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(appID))
	return FeishuCardPatchReceiptProvider + ":" + hex.EncodeToString(sum[:16])
}

func validFeishuCardPatchReceiptProvider(provider string) bool {
	prefix := FeishuCardPatchReceiptProvider + ":"
	if !strings.HasPrefix(provider, prefix) || len(provider) != len(prefix)+32 {
		return false
	}
	_, err := hex.DecodeString(strings.TrimPrefix(provider, prefix))
	return err == nil
}

// IsFeishuCardPatchReceiptProvider reports whether provider is a structurally
// valid app-bound Feishu Patch adapter identity.
func IsFeishuCardPatchReceiptProvider(provider string) bool {
	return validFeishuCardPatchReceiptProvider(provider)
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

// CreationReceiptSender applies one immutable terminal card to an existing
// provider resource. Implementations must be replay-safe for the same target
// and bytes; the Feishu adapter uses Message.Patch rather than Message.Create.
type CreationReceiptSender interface {
	SendCreationReceipt(ctx context.Context, provider, messageID, cardJSON string) error
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

type CreationReceiptCardBuilder func(markdown string) string

type CreationReceiptDispatcherDeps struct {
	Store     CreationReceiptStore
	Sender    CreationReceiptSender
	Sessions  CreationReceiptSessionRecorder
	BuildCard CreationReceiptCardBuilder
	Logger    *slog.Logger
}

// CreationReceiptDispatcher drains terminal task-creation receipts. It owns no
// business mutation: terminal rows are produced atomically by the A5 saga;
// this component only freezes presentation, records conversation history, and
// converges the provider card to that frozen terminal state.
type CreationReceiptDispatcher struct {
	store     CreationReceiptStore
	sender    CreationReceiptSender
	sessions  CreationReceiptSessionRecorder
	buildCard CreationReceiptCardBuilder
	logger    *slog.Logger

	// A pass is serialized and advances a tenant keyset cursor. When it reaches
	// the end it wraps to zero, so a permanently busy low-ID tenant cannot starve
	// later shards and concurrent manual/recovery passes cannot duplicate scans.
	dispatchMu sync.Mutex
	cursor     int64
}

func NewCreationReceiptDispatcher(
	d CreationReceiptDispatcherDeps,
) (*CreationReceiptDispatcher, error) {
	if d.Store == nil || d.Sender == nil || d.Sessions == nil || d.BuildCard == nil {
		return nil, errors.New("task: creation receipt dispatcher dependencies are incomplete")
	}
	if d.Logger == nil {
		d.Logger = slog.Default()
	}
	return &CreationReceiptDispatcher{
		store: d.Store, sender: d.Sender, sessions: d.Sessions,
		buildCard: d.BuildCard, logger: d.Logger,
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
	if d == nil || d.store == nil || d.sender == nil || d.sessions == nil || d.buildCard == nil {
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

	sendCtx, cancel := context.WithTimeout(ctx, creationReceiptSendTimeout)
	err = d.sender.SendCreationReceipt(
		sendCtx, receipt.Provider, receipt.Target, payload.CardJSON,
	)
	cancel()
	if err != nil {
		return d.finishFailure(ctx, lease, err, true)
	}

	// Message.Patch returns no provider message ID. The existing immutable target
	// is the resource identity and therefore the sent checkpoint value.
	if err := d.store.MarkTaskCreationReceiptSent(ctx, lease, receipt.Target); err != nil {
		// A patch may already be visible while this DB response is lost. Leave the
		// row replayable: the next owner applies the same bytes to the same message.
		return err
	}
	return nil
}

type creationUserReceiptPayload struct {
	Version         string          `json:"version"`
	CardJSON        string          `json:"card_json"`
	SessionMessages json.RawMessage `json:"session_messages"`
}

func (d *CreationReceiptDispatcher) loadOrCheckpointPayload(
	ctx context.Context,
	receipt *types.TaskCreationReceipt,
) (creationUserReceiptPayload, error) {
	if len(receipt.Payload) != 0 {
		return decodeCreationUserReceiptPayload(receipt.Payload, receipt.PayloadDigest)
	}
	display, history, err := renderCreationUserReceipt(*receipt)
	if err != nil {
		return creationUserReceiptPayload{}, err
	}
	cardJSON := d.buildCard(display)
	if strings.TrimSpace(cardJSON) == "" || len(cardJSON) > maxCreationReceiptCardBytes ||
		!json.Valid([]byte(cardJSON)) {
		return creationUserReceiptPayload{},
			types.NewAppError(types.CodeValidation, "task creation receipt card is invalid", nil)
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
		CardJSON:        cardJSON,
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
		strings.TrimSpace(payload.CardJSON) == "" ||
		len(payload.CardJSON) > maxCreationReceiptCardBytes ||
		!json.Valid([]byte(payload.CardJSON)) || len(payload.SessionMessages) == 0 {
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
	expectedPhase := map[types.PendingActionStatus]types.TaskCreationPhase{
		types.PendingActionStatusExecuted:  types.TaskCreationPhaseCompleted,
		types.PendingActionStatusCancelled: types.TaskCreationPhaseCancelled,
		types.PendingActionStatusExpired:   types.TaskCreationPhaseExpired,
		types.PendingActionStatusBlocked:   types.TaskCreationPhaseBlocked,
		types.PendingActionStatusFailed:    types.TaskCreationPhaseFailed,
	}[receipt.OperationStatus]
	if expectedPhase == "" || receipt.OperationPhase != expectedPhase {
		return "", "", types.NewAppError(types.CodeValidation,
			"task creation receipt references a non-terminal operation", nil)
	}

	switch receipt.OperationStatus {
	case types.PendingActionStatusExecuted:
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
		// persist operation summary, source title/URL/config, provider errors, or
		// rendered card text here: those fields may contain external content even
		// when the user approved the task. The durable callback records only the
		// fixed state transition; detail remains in the card and audit tables.
		history = "[卡片回调] 用户已点击「确认」，任务已成功创建。"
	case types.PendingActionStatusCancelled:
		display = "已取消本次任务创建。"
		history = "[卡片回调] 用户已点击「取消」，任务创建已取消。"
	case types.PendingActionStatusExpired:
		display = "这张任务确认已过期，请重新描述需求。"
		history = "[卡片回调] 用户已点击「确认」，但任务确认已过期，任务未创建。"
	case types.PendingActionStatusBlocked, types.PendingActionStatusFailed:
		display = strings.TrimSpace(receipt.ErrorMessage)
		if strings.TrimSpace(receipt.ErrorCode) == "" || display == "" {
			return "", "", types.NewAppError(types.CodeValidation,
				"task creation receipt failure checkpoint is invalid", nil)
		}
		history = "[卡片回调] 用户已点击「确认」，但任务创建已安全停止，任务未创建。"
	}
	return display, history, nil
}

func (d *CreationReceiptDispatcher) finishFailure(
	ctx context.Context,
	lease types.TaskCreationReceiptLease,
	cause error,
	providerCall bool,
) error {
	class := types.TaskCreationReceiptFailureRetryable
	retryAfter := creationReceiptBackoff(1)
	var appErr *types.AppError
	if errors.As(cause, &appErr) && !appErr.Retryable {
		class = types.TaskCreationReceiptFailurePermanent
		retryAfter = 0
	} else if providerCall {
		// A transport error or timeout cannot prove whether Feishu applied PATCH.
		// It is nevertheless safe to retry because target and bytes are frozen.
		class = types.TaskCreationReceiptFailureAmbiguous
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
