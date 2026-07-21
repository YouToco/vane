package a2a

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv/taskstore"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	vanestore "github.com/YouToco/vane/store"
	"github.com/YouToco/vane/types"
)

func newTask(id string, state a2a.TaskState) *a2a.Task {
	return &a2a.Task{ID: a2a.TaskID(id), ContextID: "ctx-1", Status: a2a.TaskStatus{State: state}}
}

// TestSentinelMapping 契约 §5.9 哨兵映射表驱动：三行各一用例，按方法翻译。
func TestSentinelMapping(t *testing.T) {
	t.Run("Create已存在→ErrTaskAlreadyExists", func(t *testing.T) {
		st := newFakeTaskStorage()
		ad := newTaskStore(st)
		if _, err := ad.Create(t.Context(), newTask("t1", a2a.TaskStateSubmitted)); err != nil {
			t.Fatalf("首建应成功: %v", err)
		}
		_, err := ad.Create(t.Context(), newTask("t1", a2a.TaskStateSubmitted))
		if !errors.Is(err, taskstore.ErrTaskAlreadyExists) {
			t.Fatalf("应译为 taskstore.ErrTaskAlreadyExists，实际 %v", err)
		}
	})
	t.Run("Get无行→a2a.ErrTaskNotFound", func(t *testing.T) {
		ad := newTaskStore(newFakeTaskStorage())
		_, err := ad.Get(t.Context(), "missing")
		if !errors.Is(err, a2a.ErrTaskNotFound) {
			t.Fatalf("应译为 a2a.ErrTaskNotFound，实际 %v", err)
		}
	})
	t.Run("Update无行→a2a.ErrTaskNotFound", func(t *testing.T) {
		ad := newTaskStore(newFakeTaskStorage())
		_, err := ad.Update(t.Context(), &taskstore.UpdateRequest{
			Task: newTask("missing", a2a.TaskStateWorking), PrevVersion: 1,
		})
		if !errors.Is(err, a2a.ErrTaskNotFound) {
			t.Fatalf("应译为 a2a.ErrTaskNotFound，实际 %v", err)
		}
	})
	t.Run("Update版本前进→ErrConcurrentModification", func(t *testing.T) {
		st := newFakeTaskStorage()
		ad := newTaskStore(st)
		if _, err := ad.Create(t.Context(), newTask("t2", a2a.TaskStateSubmitted)); err != nil {
			t.Fatal(err)
		}
		// 拿过期版本更新。
		if _, err := ad.Update(t.Context(), &taskstore.UpdateRequest{
			Task: newTask("t2", a2a.TaskStateWorking), PrevVersion: 1,
		}); err != nil {
			t.Fatalf("正确版本更新应成功: %v", err)
		}
		_, err := ad.Update(t.Context(), &taskstore.UpdateRequest{
			Task: newTask("t2", a2a.TaskStateWorking), PrevVersion: 1,
		})
		if !errors.Is(err, taskstore.ErrConcurrentModification) {
			t.Fatalf("应译为 taskstore.ErrConcurrentModification，实际 %v", err)
		}
	})
}

// TestVersionSemantics Create=TaskVersion(1)、Update=PrevVersion+1（契约 §5.9）。
func TestVersionSemantics(t *testing.T) {
	ad := newTaskStore(newFakeTaskStorage())
	v, err := ad.Create(t.Context(), newTask("t1", a2a.TaskStateSubmitted))
	if err != nil || v != taskstore.TaskVersion(1) {
		t.Fatalf("Create 应返回版本 1: v=%d err=%v", v, err)
	}
	v, err = ad.Update(t.Context(), &taskstore.UpdateRequest{
		Task: newTask("t1", a2a.TaskStateWorking), PrevVersion: 1,
	})
	if err != nil || v != taskstore.TaskVersion(2) {
		t.Fatalf("Update 应返回 PrevVersion+1=2: v=%d err=%v", v, err)
	}
	// PrevVersion==TaskVersionMissing：回读当前版本再更新（InMemory 同语义）。
	v, err = ad.Update(t.Context(), &taskstore.UpdateRequest{
		Task: newTask("t1", a2a.TaskStateCompleted), PrevVersion: taskstore.TaskVersionMissing,
	})
	if err != nil || v != taskstore.TaskVersion(3) {
		t.Fatalf("Missing 版本更新应回读后成功: v=%d err=%v", v, err)
	}
}

// TestGetRoundTripAndIsolation Get 反序列化产出全新对象树（深拷贝隔离）。
func TestGetRoundTripAndIsolation(t *testing.T) {
	ad := newTaskStore(newFakeTaskStorage())
	orig := newTask("t1", a2a.TaskStateSubmitted)
	orig.History = []*a2a.Message{a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("hi"))}
	if _, err := ad.Create(t.Context(), orig); err != nil {
		t.Fatal(err)
	}
	got1, err := ad.Get(t.Context(), "t1")
	if err != nil {
		t.Fatal(err)
	}
	got1.Task.Status.State = a2a.TaskStateFailed // 污染副本
	got2, err := ad.Get(t.Context(), "t1")
	if err != nil {
		t.Fatal(err)
	}
	if got2.Task.Status.State != a2a.TaskStateSubmitted {
		t.Error("Get 返回值应相互隔离（深拷贝语义）")
	}
	if got2.Task.History[0].Parts[0].Text() != "hi" {
		t.Error("history 往返丢失")
	}
}

// TestListTrimming HistoryLength/IncludeArtifacts 的 JSONB 裁剪归适配层（契约 §5.9）。
func TestListTrimming(t *testing.T) {
	st := newFakeTaskStorage()
	ad := newTaskStore(st)
	task := newTask("t1", a2a.TaskStateCompleted)
	for i := 0; i < 5; i++ {
		task.History = append(task.History, a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("m")))
	}
	task.Artifacts = []*a2a.Artifact{{ID: a2a.NewArtifactID(), Parts: a2a.ContentParts{a2a.NewTextPart("art")}}}
	if _, err := ad.Create(t.Context(), task); err != nil {
		t.Fatal(err)
	}

	two := 2
	resp, err := ad.List(t.Context(), &a2a.ListTasksRequest{HistoryLength: &two})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Tasks) != 1 || len(resp.Tasks[0].History) != 2 {
		t.Fatalf("HistoryLength=2 应截尾 2 条，实际 %d", len(resp.Tasks[0].History))
	}
	if resp.Tasks[0].Artifacts != nil {
		t.Error("IncludeArtifacts=false 应剥除 artifacts")
	}
	if resp.TotalSize != 1 {
		t.Errorf("TotalSize 应 1，实际 %d", resp.TotalSize)
	}

	resp, err = ad.List(t.Context(), &a2a.ListTasksRequest{IncludeArtifacts: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Tasks[0].Artifacts) != 1 {
		t.Error("IncludeArtifacts=true 应保留 artifacts")
	}
	zero := 0
	resp, err = ad.List(t.Context(), &a2a.ListTasksRequest{HistoryLength: &zero})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Tasks[0].History) != 0 {
		t.Error("HistoryLength=0 应置空 history")
	}
}

// TestListQueryMapping StatusTimestampAfter 指针→零值映射、Status/ContextID 直传。
func TestListQueryMapping(t *testing.T) {
	st := newFakeTaskStorage()
	ad := newTaskStore(st)
	if _, err := ad.Create(t.Context(), newTask("t1", a2a.TaskStateCompleted)); err != nil {
		t.Fatal(err)
	}
	after := time.Now().Add(-time.Hour)
	resp, err := ad.List(t.Context(), &a2a.ListTasksRequest{
		ContextID:            "ctx-1",
		Status:               a2a.TaskStateCompleted,
		StatusTimestampAfter: &after,
		PageSize:             7,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Tasks) != 1 || resp.PageSize != 7 {
		t.Fatalf("过滤/PageSize 回填不符: tasks=%d pageSize=%d", len(resp.Tasks), resp.PageSize)
	}
	// PageSize 钳制口径复算（<=0→50）。
	resp, err = ad.List(t.Context(), &a2a.ListTasksRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if resp.PageSize != 50 {
		t.Errorf("缺省 PageSize 应回填 50，实际 %d", resp.PageSize)
	}
}

// TestRecoveredTaskStatusVisibleThroughAdapter catches a two-ledger split that
// row-level store tests cannot see: List filters on the extracted status column,
// while Get/List responses are deserialized from task JSONB.
func TestRecoveredTaskStatusVisibleThroughAdapter(t *testing.T) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("DATABASE_URL 未设置，跳过 A2A 恢复适配层真库测试")
	}
	ctx := t.Context()
	if err := vanestore.Migrate(ctx, dbURL); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	st, err := vanestore.New(ctx, dbURL)
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}
	t.Cleanup(st.Close)
	rawDB, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	t.Cleanup(rawDB.Close)

	id := "test_a2a_recovery_" + uuid.NewString()
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := rawDB.Exec(cleanupCtx, `DELETE FROM a2a_tasks WHERE id=$1`, id); err != nil {
			t.Errorf("cleanup A2A task: %v", err)
		}
	})
	task := newTask(id, a2a.TaskStateWorking)
	payload, err := json.Marshal(task)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.CreateA2ATask(ctx, &types.A2ATask{
		ID: id, ContextID: task.ContextID, Status: string(task.Status.State), Task: payload,
	}); err != nil {
		t.Fatalf("CreateA2ATask: %v", err)
	}
	ancientUpdatedAt := time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)
	cutoff := ancientUpdatedAt.Add(time.Hour)
	if _, err := rawDB.Exec(ctx, `UPDATE a2a_tasks SET updated_at=$2 WHERE id=$1`, id, ancientUpdatedAt); err != nil {
		t.Fatalf("age task: %v", err)
	}
	// Use an ancient cutoff so this package cannot sweep contemporary WORKING
	// fixtures owned by a concurrently running store test process.
	if n, err := st.FailStaleA2ATasks(ctx, cutoff); err != nil || n < 1 {
		t.Fatalf("FailStaleA2ATasks n=%d err=%v", n, err)
	}

	adapter := newTaskStore(st)
	got, err := adapter.Get(ctx, a2a.TaskID(id))
	if err != nil {
		t.Fatalf("adapter.Get: %v", err)
	}
	if got.Task.Status.State != a2a.TaskStateFailed || got.Task.Status.Timestamp == nil {
		t.Fatalf("Get public status = %+v, want FAILED with timestamp", got.Task.Status)
	}
	listed, err := adapter.List(ctx, &a2a.ListTasksRequest{Status: a2a.TaskStateFailed, PageSize: 200})
	if err != nil {
		t.Fatalf("adapter.List: %v", err)
	}
	found := false
	for _, listedTask := range listed.Tasks {
		if listedTask.ID == a2a.TaskID(id) {
			found = true
			if listedTask.Status.State != a2a.TaskStateFailed {
				t.Fatalf("List filtered FAILED but returned embedded state %s", listedTask.Status.State)
			}
		}
	}
	if !found {
		t.Fatal("List(Status=FAILED) did not return recovered task")
	}
}
