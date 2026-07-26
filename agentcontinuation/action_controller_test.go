package agentcontinuation

import (
	"context"
	"errors"
	"testing"

	"github.com/YouToco/vane/store"
)

type fakeActionControllerStore struct {
	confirmResult store.AgentActionConfirmation
	confirmErr    error
	cancelResult  store.AgentActionConfirmation
	cancelErr     error
	confirmCalls  int
	cancelCalls   int
	userID        int64
	actionID      string
}

func (f *fakeActionControllerStore) ConfirmAgentActionContinuation(
	_ context.Context,
	userID int64,
	actionID string,
) (store.AgentActionConfirmation, error) {
	f.confirmCalls++
	f.userID = userID
	f.actionID = actionID
	return f.confirmResult, f.confirmErr
}

func (f *fakeActionControllerStore) CancelAgentActionContinuation(
	_ context.Context,
	userID int64,
	actionID string,
) (store.AgentActionConfirmation, error) {
	f.cancelCalls++
	f.userID = userID
	f.actionID = actionID
	return f.cancelResult, f.cancelErr
}

func TestActionController_Confirm(t *testing.T) {
	tests := []struct {
		name       string
		result     store.AgentActionConfirmation
		err        error
		wantText   string
		wantStatus string
		wantReplay bool
		wantErr    error
	}{
		{
			name: "accepts durable action",
			result: store.AgentActionConfirmation{
				Handled: true, Accepted: true,
				Status: store.AgentActionStatusConfirmed,
			},
			wantText:   "已确认，系统将可靠继续执行，无需重复点击。",
			wantStatus: store.AgentActionStatusConfirmed,
		},
		{
			name: "renders terminal replay",
			result: store.AgentActionConfirmation{
				Handled: true, Accepted: true, Replayed: true,
				Status: store.AgentActionStatusCompleted,
			},
			wantText:   "该操作此前已经完成，无需重复确认。",
			wantStatus: store.AgentActionStatusCompleted,
			wantReplay: true,
		},
		{
			name:    "maps only exact not routed",
			err:     store.ErrAgentActionNotRouted,
			wantErr: ErrNotRouted,
		},
		{
			name:    "preserves database error",
			err:     errors.New("database unavailable"),
			wantErr: errTestActionDatabase,
		},
		{
			name: "rejects missing handled proof",
			result: store.AgentActionConfirmation{
				Status: store.AgentActionStatusConfirmed,
			},
			wantErr: errTestActionProtocol,
		},
		{
			name: "rejects unknown status",
			result: store.AgentActionConfirmation{
				Handled: true, Status: "surprise",
			},
			wantErr: errTestActionProtocol,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := &fakeActionControllerStore{
				confirmResult: tt.result, confirmErr: tt.err,
			}
			controller, err := NewActionController(st)
			if err != nil {
				t.Fatal(err)
			}
			got, err := controller.Confirm(t.Context(), 7, "action")
			assertActionControllerResult(t, got, err, tt.wantText,
				tt.wantStatus, tt.wantReplay, tt.wantErr)
			if st.confirmCalls != 1 || st.cancelCalls != 0 ||
				st.userID != 7 || st.actionID != "action" {
				t.Fatalf("call routing drifted: %+v", st)
			}
		})
	}
}

func TestActionController_Cancel(t *testing.T) {
	tests := []struct {
		name       string
		status     string
		wantText   string
		wantReplay bool
	}{
		{
			name:     "cancels pristine action",
			status:   store.AgentActionStatusCancelled,
			wantText: "已取消，本次操作不会执行。",
		},
		{
			name:       "does not claim confirmed action was cancelled",
			status:     store.AgentActionStatusConfirmed,
			wantText:   "该操作已经确认，系统将可靠继续执行，无法再取消。",
			wantReplay: true,
		},
		{
			name:       "does not claim completed action was cancelled",
			status:     store.AgentActionStatusCompleted,
			wantText:   "该操作此前已经完成，无法再取消。",
			wantReplay: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := &fakeActionControllerStore{cancelResult: store.AgentActionConfirmation{
				Handled: true, Replayed: tt.wantReplay,
				Status: tt.status,
			}}
			controller, err := NewActionController(st)
			if err != nil {
				t.Fatal(err)
			}
			got, err := controller.Cancel(t.Context(), 7, "action")
			assertActionControllerResult(t, got, err, tt.wantText,
				tt.status, tt.wantReplay, nil)
			if st.cancelCalls != 1 || st.confirmCalls != 0 {
				t.Fatalf("call routing drifted: %+v", st)
			}
		})
	}
}

var (
	errTestActionDatabase = errors.New("test expects a database error")
	errTestActionProtocol = errors.New("test expects a protocol error")
)

func assertActionControllerResult(
	t *testing.T,
	got ActionOutcome,
	err error,
	wantText, wantStatus string,
	wantReplay bool,
	wantErr error,
) {
	t.Helper()
	switch wantErr {
	case nil:
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	case ErrNotRouted:
		if !errors.Is(err, ErrNotRouted) {
			t.Fatalf("error = %v, want ErrNotRouted", err)
		}
	case errTestActionDatabase:
		if err == nil || errors.Is(err, ErrNotRouted) {
			t.Fatalf("error = %v, want non-routing database error", err)
		}
	case errTestActionProtocol:
		if err == nil {
			t.Fatal("expected protocol error")
		}
	default:
		t.Fatalf("unsupported test expectation %v", wantErr)
	}
	if got.Text != wantText || got.Status != wantStatus ||
		got.Replayed != wantReplay {
		t.Fatalf("outcome = %+v, want text=%q status=%q replay=%t",
			got, wantText, wantStatus, wantReplay)
	}
}
