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

	"github.com/YouToco/vane/server/internal/strictjson"
	"github.com/YouToco/vane/server/types"
)

const definitionEditReceiptPayloadVersion = "vane.task-definition-edit-session-receipt/v2"

// DefinitionEditReceiptStore is the complete and only Store surface consumed
// by the C2b3-2d receipt dispatcher. It cannot mutate edit operations or
// reconstruct their terminal state.
type DefinitionEditReceiptStore interface {
	ListRecoveryTenantCatalogPage(
		context.Context, int64, int,
	) ([]int64, error)
	ListDueTaskDefinitionEditReceipts(
		context.Context, int64, time.Time, int,
	) ([]types.TaskDefinitionEditReceipt, error)
	AcquireTaskDefinitionEditReceipt(
		context.Context,
		types.AcquireTaskDefinitionEditReceiptParams,
	) (*types.TaskDefinitionEditReceipt, error)
	CheckpointTaskDefinitionEditReceiptPayload(
		context.Context,
		types.TaskDefinitionEditReceiptLease,
		[]byte,
		string,
	) error
	MarkTaskDefinitionEditReceiptSent(
		context.Context,
		types.TaskDefinitionEditReceiptLease,
		string,
	) error
	RecordTaskDefinitionEditReceiptSendFailure(
		context.Context,
		types.RecordTaskDefinitionEditReceiptSendFailureParams,
	) error
}

// DefinitionEditReceiptSessionRecorder serializes the fixed terminal fact
// with Agent's per-user conversation lock. Store performs append+checkpoint in
// one transaction, so a lost response is an exact replay.
type DefinitionEditReceiptSessionRecorder interface {
	RecordDefinitionEditReceiptSession(
		context.Context,
		types.TaskDefinitionEditReceipt,
		json.RawMessage,
	) error
}

type DefinitionEditReceiptDispatcherDeps struct {
	Store    DefinitionEditReceiptStore
	Sessions DefinitionEditReceiptSessionRecorder
	Logger   *slog.Logger
}

// DefinitionEditReceiptDispatcher consumes only terminal outbox rows. The
// immutable payload is checkpointed before the session effect.
type DefinitionEditReceiptDispatcher struct {
	store    DefinitionEditReceiptStore
	sessions DefinitionEditReceiptSessionRecorder
	logger   *slog.Logger

	dispatchMu sync.Mutex
	cursor     int64
}

func NewDefinitionEditReceiptDispatcher(
	deps DefinitionEditReceiptDispatcherDeps,
) (*DefinitionEditReceiptDispatcher, error) {
	if deps.Store == nil || deps.Sessions == nil {
		return nil, errors.New(
			"task: definition edit receipt dispatcher dependencies are incomplete",
		)
	}
	if deps.Logger == nil {
		deps.Logger = slog.Default()
	}
	return &DefinitionEditReceiptDispatcher{
		store: deps.Store, sessions: deps.Sessions, logger: deps.Logger,
	}, nil
}

func (d *DefinitionEditReceiptDispatcher) Run(ctx context.Context) {
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

func (d *DefinitionEditReceiptDispatcher) dispatchAndLog(
	ctx context.Context,
) {
	if err := d.DispatchOnce(ctx); err != nil &&
		!errors.Is(err, context.Canceled) {
		d.logger.ErrorContext(
			ctx, "task definition edit receipt dispatch pass failed",
			"err", err,
		)
	}
}

func (d *DefinitionEditReceiptDispatcher) DispatchOnce(
	ctx context.Context,
) error {
	if d == nil || d.store == nil || d.sessions == nil {
		return errors.New(
			"task: definition edit receipt dispatcher dependencies are incomplete",
		)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	d.dispatchMu.Lock()
	defer d.dispatchMu.Unlock()

	boundary := time.Now().Add(24 * time.Hour)
	tenantIDs, err := d.store.ListRecoveryTenantCatalogPage(
		ctx, d.cursor, creationReceiptTenantLimit,
	)
	if err != nil {
		return fmt.Errorf("list definition edit receipt tenant shards: %w", err)
	}
	if len(tenantIDs) == 0 && d.cursor > 0 {
		tenantIDs, err =
			d.store.ListRecoveryTenantCatalogPage(
				ctx, 0, creationReceiptTenantLimit,
			)
		if err != nil {
			return fmt.Errorf(
				"wrap definition edit receipt tenant shards: %w", err,
			)
		}
	}
	if len(tenantIDs) > 0 {
		d.cursor = tenantIDs[len(tenantIDs)-1]
	}

	semaphore := make(chan struct{}, creationReceiptConcurrency)
	var wait sync.WaitGroup
	var errorsMu sync.Mutex
	var dispatchErrors []error
	appendError := func(err error) {
		errorsMu.Lock()
		dispatchErrors = append(dispatchErrors, err)
		errorsMu.Unlock()
	}
	for _, tenantID := range tenantIDs {
		receipts, listErr :=
			d.store.ListDueTaskDefinitionEditReceipts(
				ctx, tenantID, boundary,
				creationReceiptPerTenantLimit,
			)
		if listErr != nil {
			appendError(fmt.Errorf(
				"list tenant %d definition edit receipts: %w",
				tenantID, listErr,
			))
			continue
		}
		for i := range receipts {
			receipt := receipts[i]
			select {
			case semaphore <- struct{}{}:
			case <-ctx.Done():
				wait.Wait()
				appendError(ctx.Err())
				return errors.Join(dispatchErrors...)
			}
			wait.Add(1)
			go func() {
				defer wait.Done()
				defer func() { <-semaphore }()
				dispatchErr := d.dispatchReceipt(ctx, receipt)
				if dispatchErr == nil ||
					errors.Is(
						dispatchErr,
						types.ErrTaskDefinitionEditReceiptBusy,
					) ||
					errors.Is(
						dispatchErr,
						types.ErrTaskDefinitionEditReceiptTerminal,
					) {
					return
				}
				appendError(fmt.Errorf(
					"definition edit receipt %d: %w",
					receipt.ID, dispatchErr,
				))
			}()
		}
	}
	wait.Wait()
	return errors.Join(dispatchErrors...)
}

func (d *DefinitionEditReceiptDispatcher) dispatchReceipt(
	ctx context.Context,
	listed types.TaskDefinitionEditReceipt,
) error {
	owner := "definition-edit-receipt-" + uuid.NewString()
	receipt, err := d.store.AcquireTaskDefinitionEditReceipt(
		ctx,
		types.AcquireTaskDefinitionEditReceiptParams{
			ID: listed.ID, TenantID: listed.TenantID,
			UserID: listed.UserID, LeaseOwner: owner,
			LeaseDuration: creationReceiptLeaseDuration,
		},
	)
	if err != nil {
		return err
	}
	lease := receipt.Lease()
	payload, err := d.loadOrCheckpointPayload(ctx, receipt)
	if err != nil {
		return d.finishFailure(ctx, lease, err, false)
	}
	if receipt.SessionRecordedAt == nil {
		if err := d.sessions.RecordDefinitionEditReceiptSession(
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
				"task definition edit receipt provider is retired",
				nil,
			),
			false,
		)
	}
	if err := d.store.MarkTaskDefinitionEditReceiptSent(
		ctx, lease, receipt.Target,
	); err != nil {
		return err
	}
	return nil
}

type definitionEditUserReceiptPayload struct {
	Version         string          `json:"version"`
	SessionMessages json.RawMessage `json:"session_messages"`
}

func (d *DefinitionEditReceiptDispatcher) loadOrCheckpointPayload(
	ctx context.Context,
	receipt *types.TaskDefinitionEditReceipt,
) (definitionEditUserReceiptPayload, error) {
	if len(receipt.Payload) != 0 {
		return decodeDefinitionEditUserReceiptPayload(
			receipt.Payload, receipt.PayloadDigest,
		)
	}
	_, history, err := renderDefinitionEditUserReceipt(*receipt)
	if err != nil {
		return definitionEditUserReceiptPayload{}, err
	}
	messages, err := json.Marshal([]struct {
		Role    string `json:"role"`
		Content string `json:"content"`
	}{{Role: "user", Content: history}})
	if err != nil {
		return definitionEditUserReceiptPayload{},
			fmt.Errorf("marshal definition edit session fact: %w", err)
	}
	payload := definitionEditUserReceiptPayload{
		Version: definitionEditReceiptPayloadVersion, SessionMessages: messages,
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return definitionEditUserReceiptPayload{},
			fmt.Errorf("marshal definition edit receipt payload: %w", err)
	}
	sum := sha256.Sum256(raw)
	digest := hex.EncodeToString(sum[:])
	if err := d.store.CheckpointTaskDefinitionEditReceiptPayload(
		ctx, receipt.Lease(), raw, digest,
	); err != nil {
		return definitionEditUserReceiptPayload{}, err
	}
	receipt.Payload = raw
	receipt.PayloadDigest = digest
	return payload, nil
}

func decodeDefinitionEditUserReceiptPayload(
	raw []byte,
	digest string,
) (definitionEditUserReceiptPayload, error) {
	sum := sha256.Sum256(raw)
	if digest == "" || hex.EncodeToString(sum[:]) != digest {
		return definitionEditUserReceiptPayload{},
			types.NewAppError(
				types.CodeValidation,
				"task definition edit receipt payload digest differs", nil,
			)
	}
	var payload definitionEditUserReceiptPayload
	if err := strictjson.DecodeExact(raw, &payload); err != nil {
		return definitionEditUserReceiptPayload{},
			types.NewAppError(
				types.CodeValidation,
				"task definition edit receipt payload is invalid", err,
			)
	}
	if payload.Version != definitionEditReceiptPayloadVersion ||
		len(payload.SessionMessages) == 0 {
		return definitionEditUserReceiptPayload{},
			types.NewAppError(
				types.CodeValidation,
				"task definition edit receipt payload fields are invalid",
				nil,
			)
	}
	var messages []struct {
		Role    string `json:"role"`
		Content string `json:"content"`
	}
	if err := strictjson.DecodeExact(
		payload.SessionMessages, &messages,
	); err != nil || len(messages) != 1 ||
		messages[0].Role != "user" ||
		strings.TrimSpace(messages[0].Content) == "" {
		return definitionEditUserReceiptPayload{},
			types.NewAppError(
				types.CodeValidation,
				"task definition edit receipt session payload is invalid",
				err,
			)
	}
	return payload, nil
}

func renderDefinitionEditUserReceipt(
	receipt types.TaskDefinitionEditReceipt,
) (display string, history string, err error) {
	if strings.TrimSpace(receipt.TaskID) == "" ||
		strings.TrimSpace(receipt.OperationID) == "" {
		return "", "", definitionEditReceiptRenderError(
			"terminal operation identity is invalid", nil,
		)
	}
	switch receipt.OperationStatus {
	case types.TaskDefinitionEditOperationStatusCompleted:
		if receipt.OperationPhase !=
			types.TaskDefinitionEditPhaseTemporalTargetRestored {
			return "", "", definitionEditReceiptRenderError(
				"completed operation phase is invalid", nil,
			)
		}
		var checkpoint taskDefinitionEditSuccessV1
		if err := strictjson.DecodeExact(
			receipt.Result, &checkpoint,
		); err != nil ||
			checkpoint.Version != taskDefinitionEditResultVersion ||
			checkpoint.TaskID != receipt.TaskID ||
			checkpoint.DefinitionVersion <= 0 ||
			!validLowerSHA256(checkpoint.DefinitionDigest) {
			return "", "", definitionEditReceiptRenderError(
				"success checkpoint is invalid", err,
			)
		}
		display = fmt.Sprintf(
			"任务编辑已完成（id=%s，定义版本 v%d）。",
			checkpoint.TaskID, checkpoint.DefinitionVersion,
		)
		history = "[Agent执行] 用户已在当前消息中明确要求，任务定义编辑已完成。"
	case types.TaskDefinitionEditOperationStatusCancelled:
		if receipt.OperationPhase !=
			types.TaskDefinitionEditPhaseProposalSealed {
			return "", "", definitionEditReceiptRenderError(
				"cancelled operation phase is invalid", nil,
			)
		}
		display = "已取消本次任务编辑。"
		history = "[Agent执行] 任务定义编辑操作已取消。"
	case types.TaskDefinitionEditOperationStatusExpired:
		if receipt.OperationPhase !=
			types.TaskDefinitionEditPhaseProposalSealed {
			return "", "", definitionEditReceiptRenderError(
				"expired operation phase is invalid", nil,
			)
		}
		display = "任务编辑操作已过期，请重新描述需要修改的内容。"
		history = "[Agent执行] 任务定义编辑操作已过期，变更未执行。"
	case types.TaskDefinitionEditOperationStatusBlocked:
		if !validDefinitionEditReceiptProgressPhase(
			receipt.OperationPhase,
		) || !validDefinitionEditReceiptBlockCode(
			receipt.ErrorCode,
		) {
			return "", "", definitionEditReceiptRenderError(
				"blocked operation checkpoint is invalid", nil,
			)
		}
		display = "任务编辑已安全停止，任务保持在受保护状态，请稍后重试或联系管理员。"
		history = "[Agent执行] 用户已在当前消息中明确要求，但任务定义编辑已安全停止。"
	case types.TaskDefinitionEditOperationStatusSuperseded:
		if !validDefinitionEditReceiptProgressPhase(
			receipt.OperationPhase,
		) || receipt.ErrorCode != "definition_superseded" {
			return "", "", definitionEditReceiptRenderError(
				"superseded operation checkpoint is invalid", nil,
			)
		}
		display = "任务定义已发生更新，本次旧编辑方案未执行，请重新发起编辑。"
		history = "[Agent执行] 用户已在当前消息中明确要求，但任务定义已更新，旧编辑方案未执行。"
	default:
		return "", "", definitionEditReceiptRenderError(
			"operation is not terminal", nil,
		)
	}
	return display, history, nil
}

func validDefinitionEditReceiptProgressPhase(
	phase types.TaskDefinitionEditPhase,
) bool {
	switch phase {
	case types.TaskDefinitionEditPhaseProposalSealed,
		types.TaskDefinitionEditPhaseDBQuiesced,
		types.TaskDefinitionEditPhaseTemporalBasePaused,
		types.TaskDefinitionEditPhaseDefinitionCommitted,
		types.TaskDefinitionEditPhaseTemporalTargetApplied,
		types.TaskDefinitionEditPhaseTemporalTargetRestored:
		return true
	default:
		return false
	}
}

func validDefinitionEditReceiptBlockCode(code string) bool {
	switch types.TaskDefinitionEditBlockReason(code) {
	case types.TaskDefinitionEditBlockScheduleDeleted,
		types.TaskDefinitionEditBlockTemporalNotFound,
		types.TaskDefinitionEditBlockUnsafeRemoteState,
		types.TaskDefinitionEditBlockCheckpointInvalid:
		return true
	default:
		return false
	}
}

func definitionEditReceiptRenderError(
	message string,
	cause error,
) error {
	return types.NewAppError(
		types.CodeValidation,
		"task definition edit receipt "+message, cause,
	)
}

func (d *DefinitionEditReceiptDispatcher) finishFailure(
	ctx context.Context,
	lease types.TaskDefinitionEditReceiptLease,
	cause error,
	_ bool,
) error {
	class := types.TaskDefinitionEditReceiptFailureRetryable
	retryAfter := creationReceiptBackoff(lease.Fence)
	var appError *types.AppError
	if errors.As(cause, &appError) && !appError.Retryable {
		class = types.TaskDefinitionEditReceiptFailurePermanent
		retryAfter = 0
	}
	checkpointCtx, cancel := context.WithTimeout(
		context.WithoutCancel(ctx), creationReceiptStoreTimeout,
	)
	defer cancel()
	checkpointErr :=
		d.store.RecordTaskDefinitionEditReceiptSendFailure(
			checkpointCtx,
			types.RecordTaskDefinitionEditReceiptSendFailureParams{
				Lease: lease, Class: class, RetryAfter: retryAfter,
			},
		)
	if checkpointErr != nil {
		return errors.Join(
			cause,
			fmt.Errorf(
				"checkpoint definition edit receipt failure: %w",
				checkpointErr,
			),
		)
	}
	return cause
}
