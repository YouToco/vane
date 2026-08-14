package store

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/YouToco/vane/server/types"
)

func TestAuthorizeNewScheduleCommandAllowsPausedOneOffRun(t *testing.T) {
	command := &types.ScheduleCommand{Kind: types.ScheduleCommandRun}
	schedule := &types.Schedule{Status: types.ScheduleStatusPaused}
	if err := authorizeNewScheduleCommand(command, schedule, true); err != nil {
		t.Fatalf("paused one-off run authorization: %v", err)
	}
	if schedule.Status != types.ScheduleStatusPaused {
		t.Fatalf("authorization changed recurring status to %q", schedule.Status)
	}
}

func TestScheduleCommands_PostgreSQLLifecycleAndSharedLock(t *testing.T) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("DATABASE_URL 未设置，跳过任务命令真库测试")
	}
	ctx := t.Context()
	if err := Migrate(ctx, dbURL); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	st, err := New(ctx, dbURL)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	registerStoreClose(t, st)
	if err := st.ValidateScheduleCommandRuntimeRole(ctx); err != nil {
		t.Fatalf("restricted role gate: %v", err)
	}

	user, err := st.UpsertUserByOpenID(
		ctx, "schedule-command-"+uuid.NewString(), "schedule command test",
	)
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	attachTenant(t, st, user.ID)
	taskID := "schedule-command-" + uuid.NewString()
	if err := st.InsertSchedule(ctx, &types.Schedule{
		ID: taskID, UserID: user.ID, NLDescription: "command integration",
		Status: types.ScheduleStatusActive,
	}); err != nil {
		t.Fatalf("insert schedule: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(
			cleanupCtx, t, st,
			`DELETE FROM schedule_commands WHERE user_id=$1`, user.ID,
		)
		cleanupExec(
			cleanupCtx, t, st,
			`DELETE FROM schedules WHERE user_id=$1`, user.ID,
		)
		cleanupExec(
			cleanupCtx, t, st,
			`DELETE FROM memberships WHERE user_id=$1`, user.ID,
		)
		cleanupExec(
			cleanupCtx, t, st,
			`DELETE FROM users WHERE id=$1`, user.ID,
		)
	})

	const (
		runKey     = "store-run-key"
		pauseKey   = "store-pause-key"
		runPayload = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		runRequest = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	)
	run, err := st.CreateOrLoadScheduleCommand(
		ctx, 1, user.ID, taskID, runKey, types.ScheduleCommandRun,
		runPayload, runRequest,
	)
	if err != nil {
		t.Fatalf("create run intent: %v", err)
	}
	replayed, err := st.CreateOrLoadScheduleCommand(
		ctx, 1, user.ID, taskID, runKey, types.ScheduleCommandRun,
		runPayload, runRequest,
	)
	if err != nil || replayed.ID != run.ID {
		t.Fatalf("exact replay=%+v err=%v, want id %s", replayed, err, run.ID)
	}
	if _, err := st.CreateOrLoadScheduleCommand(
		ctx, 1, user.ID, taskID, runKey, types.ScheduleCommandPause,
		runPayload, runRequest,
	); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("cross-kind key reuse error=%v, want conflict", err)
	}
	if _, err := st.CreateOrLoadScheduleCommand(
		ctx, 1, user.ID, taskID, "different-key",
		types.ScheduleCommandPause, runPayload, runRequest,
	); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("parallel task command error=%v, want conflict", err)
	}

	command, _, complete, _, rollback, err :=
		st.BeginScheduleCommandAttempt(ctx, 1, user.ID, runKey)
	if err != nil {
		t.Fatalf("begin run attempt: %v", err)
	}
	if command.ID != run.ID {
		t.Fatalf("attempt command=%s want=%s", command.ID, run.ID)
	}
	if err := complete(ctx); err != nil {
		t.Fatalf("complete run: %v", err)
	}
	if err := rollback(ctx); err != nil {
		t.Fatalf("release completed run: %v", err)
	}

	pause, err := st.CreateOrLoadScheduleCommand(
		ctx, 1, user.ID, taskID, pauseKey, types.ScheduleCommandPause,
		runPayload, "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
	)
	if err != nil {
		t.Fatalf("create pause intent: %v", err)
	}
	command, _, complete, _, rollback, err =
		st.BeginScheduleCommandAttempt(ctx, 1, user.ID, pauseKey)
	if err != nil {
		t.Fatalf("begin pause attempt: %v", err)
	}

	// The command attempt holds the exact same PostgreSQL advisory lock as
	// definition-edit/reconcile. A second process cannot enter its mutation
	// window while Temporal I/O is in flight.
	lockCtx, cancelLock := context.WithTimeout(ctx, 100*time.Millisecond)
	_, release, lockErr := st.AcquireScheduleReconcile(lockCtx, 1, taskID)
	cancelLock()
	if !errors.Is(lockErr, context.DeadlineExceeded) {
		if release != nil {
			_ = release(ctx)
		}
		t.Fatalf("shared mutation lock error=%v, want deadline", lockErr)
	}

	if err := complete(ctx); err != nil {
		t.Fatalf("complete pause: %v", err)
	}
	if err := rollback(ctx); err != nil {
		t.Fatalf("release completed pause: %v", err)
	}
	stored, err := st.GetSchedule(ctx, taskID, user.ID)
	if err != nil || stored.Status != types.ScheduleStatusPaused {
		t.Fatalf("paused mirror=%+v err=%v", stored, err)
	}
	pauseReplay, err := st.LoadScheduleCommand(ctx, 1, user.ID, pauseKey)
	if err != nil ||
		pauseReplay.Status != types.ScheduleCommandCompleted ||
		pauseReplay.Phase != types.ScheduleCommandCompletedPhase {
		t.Fatalf("pause checkpoint=%+v err=%v", pauseReplay, err)
	}
	if pauseReplay.ID != pause.ID {
		t.Fatalf("pause replay id=%s want=%s", pauseReplay.ID, pause.ID)
	}

	pausedRun, err := st.CreateOrLoadScheduleCommand(
		ctx, 1, user.ID, taskID, "store-paused-run-key",
		types.ScheduleCommandRun, runPayload,
		"dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
	)
	if err != nil {
		t.Fatalf("create paused one-off run intent: %v", err)
	}
	command, _, complete, _, rollback, err =
		st.BeginScheduleCommandAttempt(
			ctx, 1, user.ID, pausedRun.IdempotencyKey,
		)
	if err != nil {
		t.Fatalf("begin paused one-off run: %v", err)
	}
	if command.ID != pausedRun.ID {
		t.Fatalf("paused run command=%s want=%s", command.ID, pausedRun.ID)
	}
	if err := complete(ctx); err != nil {
		t.Fatalf("complete paused one-off run: %v", err)
	}
	if err := rollback(ctx); err != nil {
		t.Fatalf("release paused one-off run: %v", err)
	}
	stored, err = st.GetSchedule(ctx, taskID, user.ID)
	if err != nil || stored.Status != types.ScheduleStatusPaused {
		t.Fatalf("one-off run changed paused mirror=%+v err=%v", stored, err)
	}

	if _, err := st.LoadScheduleCommand(
		ctx, 2, user.ID, pauseKey,
	); !errors.Is(err, types.ErrNotFound) {
		t.Fatalf("cross-tenant load error=%v, want not found", err)
	}
}
