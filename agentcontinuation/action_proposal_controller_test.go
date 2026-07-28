package agentcontinuation

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/YouToco/vane/types"
)

type fakeActionProposalStore struct {
	action *types.PendingAction
	err    error
	errs   []error
	calls  int
}

func (f *fakeActionProposalStore) ProposeAgentActionContinuation(
	_ context.Context,
	action *types.PendingAction,
) error {
	f.calls++
	f.action = action
	if len(f.errs) > 0 {
		err := f.errs[0]
		f.errs = f.errs[1:]
		return err
	}
	return f.err
}

func TestActionProposalController_Propose(t *testing.T) {
	expiresAt := time.Now().Add(time.Hour)
	st := &fakeActionProposalStore{}
	controller, err := NewActionProposalController(st)
	if err != nil {
		t.Fatal(err)
	}
	got, err := controller.Propose(t.Context(), ActionProposalInput{
		ActionID: "action", UserID: 7, SessionID: 9,
		ToolName: DurableActionToolName,
		RawArgs:  []byte(`{"source_id":11}`),
		Summary:  "重新启用信源（id=11）", ExpiresAt: expiresAt,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != "action" || got.Summary != "重新启用信源（id=11）" ||
		st.calls != 1 || st.action == nil ||
		st.action.SessionID == nil || *st.action.SessionID != 9 ||
		st.action.Status != types.PendingActionStatusPending ||
		!st.action.ExpiresAt.Equal(time.UnixMicro(expiresAt.UnixMicro())) {
		t.Fatalf("proposal=%+v store=%+v", got, st)
	}
}

func TestActionProposalController_ProposesRemoveSource(t *testing.T) {
	st := &fakeActionProposalStore{}
	controller, err := NewActionProposalController(st)
	if err != nil {
		t.Fatal(err)
	}
	expiresAt := time.Now().Add(time.Hour)
	got, err := controller.Propose(t.Context(), ActionProposalInput{
		ActionID: "remove-action", UserID: 7, SessionID: 9,
		ToolName:  DurableActionRemoveSourceToolName,
		RawArgs:   []byte(`{"source_ids":[11,12]}`),
		Summary:   "取消订阅 2 个信源（id=11、12）",
		ExpiresAt: expiresAt,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != "remove-action" ||
		st.action.ToolName != DurableActionRemoveSourceToolName ||
		string(st.action.Args) != `{"source_ids":[11,12]}` {
		t.Fatalf("proposal=%+v action=%+v", got, st.action)
	}
}

func TestActionProposalController_FailsClosed(t *testing.T) {
	sentinel := errors.New("proposal failed")
	st := &fakeActionProposalStore{err: sentinel}
	controller, err := NewActionProposalController(st)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := controller.Propose(t.Context(), ActionProposalInput{
		ActionID: "action", UserID: 7, SessionID: 9,
		ToolName:  DurableActionToolName,
		RawArgs:   []byte(`{"source_id":11}`),
		Summary:   "重新启用信源（id=11）",
		ExpiresAt: time.Now().Add(time.Hour),
	}); !errors.Is(err, sentinel) {
		t.Fatalf("error=%v, want sentinel", err)
	}
	if _, err := controller.Propose(t.Context(), ActionProposalInput{
		ActionID: "other", UserID: 7, SessionID: 9,
		ToolName: "update_profile", Summary: "update",
		ExpiresAt: time.Now().Add(time.Hour),
	}); err == nil {
		t.Fatal("non-durable tool reached proposal Store")
	}
	if st.calls != 1 {
		t.Fatalf("store calls=%d, want 1", st.calls)
	}
}

func TestActionProposalController_AdoptsExactDatabaseResponseLoss(
	t *testing.T,
) {
	st := &fakeActionProposalStore{errs: []error{
		types.NewAppError(
			types.CodeDatabase,
			"commit response lost",
			errors.New("network reset"),
		),
		types.NewAppError(
			types.CodeDatabase,
			"replay connection unavailable",
			errors.New("connection reset"),
		),
		nil,
	}}
	controller, err := NewActionProposalController(st)
	if err != nil {
		t.Fatal(err)
	}
	got, err := controller.Propose(t.Context(), ActionProposalInput{
		ActionID: "stable-action", UserID: 7, SessionID: 9,
		ToolName:  DurableActionToolName,
		RawArgs:   []byte(`{"source_id":11}`),
		Summary:   "重新启用信源（id=11）",
		ExpiresAt: time.Now().Add(time.Hour),
	})
	if err != nil || got.ID != "stable-action" || st.calls != 3 {
		t.Fatalf("proposal=%+v calls=%d err=%v", got, st.calls, err)
	}
}

func TestActionProposalController_SurfacesDeterministicReplayFailure(
	t *testing.T,
) {
	integrityCause := errors.New("partial durable evidence")
	st := &fakeActionProposalStore{errs: []error{
		types.NewAppError(
			types.CodeDatabase,
			"commit response lost",
			errors.New("network reset"),
		),
		types.NewAppError(
			types.CodeInternal,
			"durable proposal evidence is inconsistent",
			integrityCause,
		),
	}}
	controller, err := NewActionProposalController(st)
	if err != nil {
		t.Fatal(err)
	}
	_, err = controller.Propose(t.Context(), ActionProposalInput{
		ActionID: "stable-action", UserID: 7, SessionID: 9,
		ToolName:  DurableActionToolName,
		RawArgs:   []byte(`{"source_id":11}`),
		Summary:   "重新启用信源（id=11）",
		ExpiresAt: time.Now().Add(time.Hour),
	})
	if !errors.Is(err, integrityCause) || st.calls != 2 {
		t.Fatalf("calls=%d error=%v", st.calls, err)
	}
}
