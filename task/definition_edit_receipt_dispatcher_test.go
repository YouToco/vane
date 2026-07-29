package task

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/YouToco/vane/types"
)

type definitionEditReceiptFakeStore struct {
	mu sync.Mutex
	r  types.TaskDefinitionEditReceipt

	markFailures int
}

func newDefinitionEditReceiptFakeStore(
	status types.TaskDefinitionEditOperationStatus,
) *definitionEditReceiptFakeStore {
	phase := types.TaskDefinitionEditPhaseProposalSealed
	r := types.TaskDefinitionEditReceipt{
		ID: 1, OperationID: "edit-operation-1", TenantID: 7, UserID: 11,
		SessionID: 19,
		Provider:  FeishuCardPatchReceiptProviderForApp("edit_dispatcher_test"),
		Target:    "om_original_edit_card", ProviderKey: "provider-key",
		Status:          types.TaskDefinitionEditReceiptStatusPending,
		NextAttemptAt:   time.Now().Add(-time.Minute),
		OperationStatus: status, OperationPhase: phase, TaskID: "task-edit-1",
	}
	switch status {
	case types.TaskDefinitionEditOperationStatusCompleted:
		r.OperationPhase = types.TaskDefinitionEditPhaseTemporalTargetRestored
		r.Result = json.RawMessage(
			`{"version":"vane.task-definition-edit-result/v1",` +
				`"task_id":"task-edit-1","definition_version":2,` +
				`"definition_digest":"` + strings.Repeat("a", 64) + `"}`,
		)
	case types.TaskDefinitionEditOperationStatusBlocked:
		r.OperationPhase = types.TaskDefinitionEditPhaseDBQuiesced
		r.ErrorCode = string(types.TaskDefinitionEditBlockUnsafeRemoteState)
		r.ErrorMessage = "UNTRUSTED_INTERNAL_DETAIL"
	case types.TaskDefinitionEditOperationStatusSuperseded:
		r.OperationPhase = types.TaskDefinitionEditPhaseDBQuiesced
		r.ErrorCode = "definition_superseded"
		r.ErrorMessage = "UNTRUSTED_INTERNAL_DETAIL"
	}
	return &definitionEditReceiptFakeStore{r: r}
}

func (s *definitionEditReceiptFakeStore) ListDueTaskDefinitionEditReceiptTenantIDs(
	_ context.Context, _ time.Time, afterTenantID int64, _ int,
) ([]int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.dueLocked() && s.r.TenantID > afterTenantID {
		return []int64{s.r.TenantID}, nil
	}
	return nil, nil
}

func (s *definitionEditReceiptFakeStore) ListDueTaskDefinitionEditReceipts(
	_ context.Context, tenantID int64, _ time.Time, _ int,
) ([]types.TaskDefinitionEditReceipt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.dueLocked() && s.r.TenantID == tenantID {
		return []types.TaskDefinitionEditReceipt{s.cloneLocked()}, nil
	}
	return nil, nil
}

func (s *definitionEditReceiptFakeStore) AcquireTaskDefinitionEditReceipt(
	_ context.Context,
	p types.AcquireTaskDefinitionEditReceiptParams,
) (*types.TaskDefinitionEditReceipt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if p.ID != s.r.ID || p.TenantID != s.r.TenantID ||
		p.UserID != s.r.UserID {
		return nil, types.ErrNotFound
	}
	if s.r.Status != types.TaskDefinitionEditReceiptStatusPending {
		return nil, types.ErrTaskDefinitionEditReceiptTerminal
	}
	if s.r.LeaseOwner != "" {
		return nil, types.ErrTaskDefinitionEditReceiptBusy
	}
	s.r.LeaseOwner = p.LeaseOwner
	s.r.Fence++
	s.r.Attempt++
	until := time.Now().Add(p.LeaseDuration)
	s.r.LeaseUntil = &until
	takeover := until.Add(time.Second)
	s.r.TakeoverNotBefore = &takeover
	clone := s.cloneLocked()
	return &clone, nil
}

func (s *definitionEditReceiptFakeStore) CheckpointTaskDefinitionEditReceiptPayload(
	_ context.Context,
	lease types.TaskDefinitionEditReceiptLease,
	payload []byte,
	digest string,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkLeaseLocked(lease); err != nil {
		return err
	}
	if len(s.r.Payload) != 0 {
		if bytes.Equal(s.r.Payload, payload) && s.r.PayloadDigest == digest {
			return nil
		}
		return types.ErrConflict
	}
	s.r.Payload = bytes.Clone(payload)
	s.r.PayloadDigest = digest
	return nil
}

func (s *definitionEditReceiptFakeStore) MarkTaskDefinitionEditReceiptSent(
	_ context.Context,
	lease types.TaskDefinitionEditReceiptLease,
	providerMessageID string,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkLeaseLocked(lease); err != nil {
		return err
	}
	if s.markFailures > 0 {
		s.markFailures--
		return errors.New("sent checkpoint response lost")
	}
	s.r.Status = types.TaskDefinitionEditReceiptStatusSent
	s.r.ProviderMessageID = providerMessageID
	now := time.Now()
	s.r.SentAt = &now
	s.clearLeaseLocked()
	return nil
}

func (s *definitionEditReceiptFakeStore) RecordTaskDefinitionEditReceiptSendFailure(
	_ context.Context,
	p types.RecordTaskDefinitionEditReceiptSendFailureParams,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkLeaseLocked(p.Lease); err != nil {
		return err
	}
	s.r.FailureClass = p.Class
	if p.RetryAfter > 0 {
		s.r.NextAttemptAt = time.Now().Add(p.RetryAfter)
		s.clearLeaseLocked()
		return nil
	}
	s.r.Status = types.TaskDefinitionEditReceiptStatusBlocked
	now := time.Now()
	s.r.BlockedAt = &now
	s.clearLeaseLocked()
	return nil
}

func (s *definitionEditReceiptFakeStore) dueLocked() bool {
	return s.r.Status == types.TaskDefinitionEditReceiptStatusPending &&
		s.r.LeaseOwner == "" && !s.r.NextAttemptAt.After(time.Now())
}

func (s *definitionEditReceiptFakeStore) checkLeaseLocked(
	lease types.TaskDefinitionEditReceiptLease,
) error {
	if lease.ID != s.r.ID || lease.TenantID != s.r.TenantID ||
		lease.UserID != s.r.UserID || lease.LeaseOwner != s.r.LeaseOwner ||
		lease.Fence != s.r.Fence || s.r.LeaseOwner == "" {
		return types.ErrTaskDefinitionEditReceiptLeaseLost
	}
	return nil
}

func (s *definitionEditReceiptFakeStore) clearLeaseLocked() {
	s.r.LeaseOwner = ""
	s.r.LeaseUntil = nil
	s.r.TakeoverNotBefore = nil
}

func (s *definitionEditReceiptFakeStore) cloneLocked() types.TaskDefinitionEditReceipt {
	receipt := s.r
	receipt.Payload = bytes.Clone(s.r.Payload)
	receipt.Result = bytes.Clone(s.r.Result)
	return receipt
}

func (s *definitionEditReceiptFakeStore) snapshot() types.TaskDefinitionEditReceipt {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cloneLocked()
}

func (s *definitionEditReceiptFakeStore) makeDue() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clearLeaseLocked()
	s.r.NextAttemptAt = time.Now().Add(-time.Second)
}

type definitionEditReceiptFakeSessions struct {
	mu sync.Mutex

	store          *definitionEditReceiptFakeStore
	appends        int
	failAfterApply int
}

func (s *definitionEditReceiptFakeSessions) RecordDefinitionEditReceiptSession(
	_ context.Context,
	receipt types.TaskDefinitionEditReceipt,
	messages json.RawMessage,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	sum := sha256.Sum256(messages)
	digest := hex.EncodeToString(sum[:])
	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	if err := s.store.checkLeaseLocked(receipt.Lease()); err != nil {
		return err
	}
	if s.store.r.SessionRecordedAt == nil {
		now := time.Now()
		s.store.r.SessionRecordedAt = &now
		s.store.r.SessionMessagesDigest = digest
		s.appends++
	} else if s.store.r.SessionMessagesDigest != digest {
		return types.ErrConflict
	}
	if s.failAfterApply > 0 {
		s.failAfterApply--
		return errors.New("session checkpoint response lost")
	}
	return nil
}

func (s *definitionEditReceiptFakeSessions) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.appends
}

type definitionEditReceiptFakeSender struct {
	mu sync.Mutex

	resources      map[string]string
	calls          int
	failAfterApply int
	err            error
}

func (s *definitionEditReceiptFakeSender) SendDefinitionEditReceipt(
	_ context.Context,
	provider string,
	target string,
	cardJSON string,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.resources == nil {
		s.resources = make(map[string]string)
	}
	s.calls++
	if provider == "" || target == "" {
		return errors.New("missing immutable target")
	}
	if s.err != nil {
		return s.err
	}
	s.resources[target] = cardJSON
	if s.failAfterApply > 0 {
		s.failAfterApply--
		return types.NewAppError(
			types.CodePushFailed, "provider response lost",
			context.DeadlineExceeded,
		)
	}
	return nil
}

func (s *definitionEditReceiptFakeSender) snapshot() (
	int,
	map[string]string,
) {
	s.mu.Lock()
	defer s.mu.Unlock()
	resources := make(map[string]string, len(s.resources))
	for key, value := range s.resources {
		resources[key] = value
	}
	return s.calls, resources
}

func newDefinitionEditReceiptDispatcherForTest(
	t *testing.T,
	store *definitionEditReceiptFakeStore,
	sessions *definitionEditReceiptFakeSessions,
	sender *definitionEditReceiptFakeSender,
) *DefinitionEditReceiptDispatcher {
	t.Helper()
	sessions.store = store
	dispatcher, err := NewDefinitionEditReceiptDispatcher(
		DefinitionEditReceiptDispatcherDeps{
			Store: store, Sender: sender, Sessions: sessions,
			BuildCard: func(markdown string) string {
				raw, _ := json.Marshal(map[string]any{
					"schema": "2.0",
					"config": map[string]any{"update_multi": true},
					"body":   map[string]any{"text": markdown},
				})
				return string(raw)
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	return dispatcher
}

func TestDefinitionEditReceiptDispatcher_AmbiguousPatchReplayKeepsOneResource(
	t *testing.T,
) {
	store := newDefinitionEditReceiptFakeStore(
		types.TaskDefinitionEditOperationStatusCancelled,
	)
	sessions := &definitionEditReceiptFakeSessions{}
	sender := &definitionEditReceiptFakeSender{failAfterApply: 1}
	dispatcher := newDefinitionEditReceiptDispatcherForTest(
		t, store, sessions, sender,
	)

	if err := dispatcher.DispatchOnce(t.Context()); err == nil {
		t.Fatal("ambiguous provider response must be observable")
	}
	first := store.snapshot()
	if first.Status != types.TaskDefinitionEditReceiptStatusPending ||
		first.FailureClass !=
			types.TaskDefinitionEditReceiptFailureAmbiguous ||
		first.SessionRecordedAt == nil || len(first.Payload) == 0 ||
		sessions.count() != 1 {
		t.Fatalf("first checkpoint mismatch: %+v sessions=%d",
			first, sessions.count())
	}
	store.makeDue()
	if err := dispatcher.DispatchOnce(t.Context()); err != nil {
		t.Fatal(err)
	}
	final := store.snapshot()
	calls, resources := sender.snapshot()
	if final.Status != types.TaskDefinitionEditReceiptStatusSent ||
		calls != 2 || len(resources) != 1 ||
		resources["om_original_edit_card"] == "" || sessions.count() != 1 {
		t.Fatalf("final=%+v calls=%d resources=%v sessions=%d",
			final, calls, resources, sessions.count())
	}
}

func TestDefinitionEditReceiptDispatcher_LocalReceiptsSkipExternalSender(
	t *testing.T,
) {
	for _, provider := range []string{
		WebActionReceiptProvider,
		AgentAutoReceiptProvider,
	} {
		t.Run(provider, func(t *testing.T) {
			st := newDefinitionEditReceiptFakeStore(
				types.TaskDefinitionEditOperationStatusCompleted,
			)
			st.r.Provider = provider
			st.r.Target = st.r.OperationID
			sessions := &definitionEditReceiptFakeSessions{}
			sender := &definitionEditReceiptFakeSender{}
			d := newDefinitionEditReceiptDispatcherForTest(
				t, st, sessions, sender,
			)

			if err := d.DispatchOnce(t.Context()); err != nil {
				t.Fatal(err)
			}
			final := st.snapshot()
			calls, resources := sender.snapshot()
			if final.Status != types.TaskDefinitionEditReceiptStatusSent ||
				final.ProviderMessageID != final.OperationID ||
				calls != 0 || len(resources) != 0 || sessions.count() != 1 {
				t.Fatalf(
					"final=%+v calls=%d resources=%v sessions=%d",
					final, calls, resources, sessions.count(),
				)
			}
		})
	}
}

func TestDefinitionEditReceiptDispatcher_ResponseLossReplaysExactCheckpoints(
	t *testing.T,
) {
	t.Run("session response loss", func(t *testing.T) {
		store := newDefinitionEditReceiptFakeStore(
			types.TaskDefinitionEditOperationStatusCancelled,
		)
		sessions := &definitionEditReceiptFakeSessions{failAfterApply: 1}
		sender := &definitionEditReceiptFakeSender{}
		dispatcher := newDefinitionEditReceiptDispatcherForTest(
			t, store, sessions, sender,
		)
		if err := dispatcher.DispatchOnce(t.Context()); err == nil {
			t.Fatal("lost session response must retry")
		}
		if calls, _ := sender.snapshot(); calls != 0 {
			t.Fatalf("provider called before session convergence: %d", calls)
		}
		store.makeDue()
		if err := dispatcher.DispatchOnce(t.Context()); err != nil {
			t.Fatal(err)
		}
		if sessions.count() != 1 {
			t.Fatalf("session append duplicated: %d", sessions.count())
		}
	})

	t.Run("sent response loss", func(t *testing.T) {
		store := newDefinitionEditReceiptFakeStore(
			types.TaskDefinitionEditOperationStatusCancelled,
		)
		store.markFailures = 1
		sessions := &definitionEditReceiptFakeSessions{}
		sender := &definitionEditReceiptFakeSender{}
		dispatcher := newDefinitionEditReceiptDispatcherForTest(
			t, store, sessions, sender,
		)
		if err := dispatcher.DispatchOnce(t.Context()); err == nil {
			t.Fatal("lost sent response must be observable")
		}
		store.makeDue()
		if err := dispatcher.DispatchOnce(t.Context()); err != nil {
			t.Fatal(err)
		}
		calls, resources := sender.snapshot()
		if calls != 2 || len(resources) != 1 || sessions.count() != 1 {
			t.Fatalf("calls=%d resources=%v sessions=%d",
				calls, resources, sessions.count())
		}
	})
}

func TestDefinitionEditReceiptDispatcher_PermanentPatchFailureBlocks(t *testing.T) {
	store := newDefinitionEditReceiptFakeStore(
		types.TaskDefinitionEditOperationStatusCancelled,
	)
	sessions := &definitionEditReceiptFakeSessions{}
	permanent := types.NewAppError(
		types.CodePushFailed, "message recalled", nil,
	)
	permanent.Retryable = false
	sender := &definitionEditReceiptFakeSender{err: permanent}
	dispatcher := newDefinitionEditReceiptDispatcherForTest(
		t, store, sessions, sender,
	)
	if err := dispatcher.DispatchOnce(t.Context()); err == nil {
		t.Fatal("permanent provider error must be observable")
	}
	receipt := store.snapshot()
	if receipt.Status != types.TaskDefinitionEditReceiptStatusBlocked ||
		receipt.FailureClass !=
			types.TaskDefinitionEditReceiptFailurePermanent ||
		receipt.BlockedAt == nil {
		t.Fatalf("receipt was not blocked: %+v", receipt)
	}
}

func TestRenderDefinitionEditUserReceipt_UsesOnlyFrozenTerminalFacts(
	t *testing.T,
) {
	tests := []struct {
		name        string
		status      types.TaskDefinitionEditOperationStatus
		wantDisplay string
		wantHistory string
	}{
		{
			name:        "completed",
			status:      types.TaskDefinitionEditOperationStatusCompleted,
			wantDisplay: "任务编辑已完成",
			wantHistory: "任务定义编辑已完成",
		},
		{
			name:        "cancelled",
			status:      types.TaskDefinitionEditOperationStatusCancelled,
			wantDisplay: "已取消本次任务编辑",
			wantHistory: "任务定义编辑已取消",
		},
		{
			name:        "expired",
			status:      types.TaskDefinitionEditOperationStatusExpired,
			wantDisplay: "任务编辑确认已过期",
			wantHistory: "任务定义编辑确认已过期",
		},
		{
			name:        "blocked",
			status:      types.TaskDefinitionEditOperationStatusBlocked,
			wantDisplay: "任务编辑已安全停止",
			wantHistory: "任务定义编辑已安全停止",
		},
		{
			name:        "superseded",
			status:      types.TaskDefinitionEditOperationStatusSuperseded,
			wantDisplay: "任务定义已发生更新",
			wantHistory: "旧编辑方案未执行",
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			receipt := newDefinitionEditReceiptFakeStore(
				testCase.status,
			).snapshot()
			display, history, err := renderDefinitionEditUserReceipt(receipt)
			if err != nil ||
				!strings.Contains(display, testCase.wantDisplay) ||
				!strings.Contains(history, testCase.wantHistory) {
				t.Fatalf("display=%q history=%q err=%v",
					display, history, err)
			}
			for _, forbidden := range []string{
				"UNTRUSTED_INTERNAL_DETAIL",
				receipt.Provider, receipt.Target, receipt.OperationID,
			} {
				if forbidden != "" && strings.Contains(history, forbidden) {
					t.Fatalf("session leaked %q: %s", forbidden, history)
				}
			}
		})
	}

	t.Run("agent auto authorization records no fictional card click", func(t *testing.T) {
		receipt := newDefinitionEditReceiptFakeStore(
			types.TaskDefinitionEditOperationStatusCompleted,
		).snapshot()
		receipt.Provider = AgentAutoReceiptProvider
		receipt.Target = receipt.OperationID
		_, history, err := renderDefinitionEditUserReceipt(receipt)
		if err != nil || !strings.HasPrefix(history, "[Agent执行]") ||
			strings.Contains(history, "点击") ||
			strings.Contains(history, "确认卡") {
			t.Fatalf("history=%q err=%v", history, err)
		}
	})
}

func TestDecodeDefinitionEditUserReceiptPayloadRejectsMutation(t *testing.T) {
	raw := []byte(
		`{"version":"vane.task-definition-edit-user-receipt/v1",` +
			`"card_json":"{}",` +
			`"session_messages":[{"role":"user","content":"done"}]}`,
	)
	sum := sha256.Sum256(raw)
	digest := hex.EncodeToString(sum[:])
	if _, err := decodeDefinitionEditUserReceiptPayload(raw, digest); err != nil {
		t.Fatal(err)
	}
	mutated := append(bytes.Clone(raw), ' ')
	if _, err := decodeDefinitionEditUserReceiptPayload(
		mutated, digest,
	); err == nil {
		t.Fatal("payload mutation with old digest must fail")
	}
}
