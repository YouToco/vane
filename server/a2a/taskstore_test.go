package a2a

import (
	"errors"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv/taskstore"
)

func newTask(id string, state a2a.TaskState) *a2a.Task {
	return &a2a.Task{ID: a2a.TaskID(id), ContextID: "ctx-1", Status: a2a.TaskStatus{State: state}}
}

// TestSentinelMapping 契约 §5.9 哨兵映射表驱动：三行各一用例，按方法翻译。
func TestSentinelMapping(t *testing.T) {
	t.Run("Create已存在→ErrTaskAlreadyExists", func(t *testing.T) {
		st := newFakeTaskStorage()
		ad := newTaskStore(st)
		if _, err := ad.Create(testA2AContext(t.Context()), newTask("t1", a2a.TaskStateSubmitted)); err != nil {
			t.Fatalf("首建应成功: %v", err)
		}
		_, err := ad.Create(testA2AContext(t.Context()), newTask("t1", a2a.TaskStateSubmitted))
		if !errors.Is(err, taskstore.ErrTaskAlreadyExists) {
			t.Fatalf("应译为 taskstore.ErrTaskAlreadyExists，实际 %v", err)
		}
	})
	t.Run("Get无行→a2a.ErrTaskNotFound", func(t *testing.T) {
		ad := newTaskStore(newFakeTaskStorage())
		_, err := ad.Get(testA2AContext(t.Context()), "missing")
		if !errors.Is(err, a2a.ErrTaskNotFound) {
			t.Fatalf("应译为 a2a.ErrTaskNotFound，实际 %v", err)
		}
	})
	t.Run("Update无行→a2a.ErrTaskNotFound", func(t *testing.T) {
		ad := newTaskStore(newFakeTaskStorage())
		_, err := ad.Update(testA2AContext(t.Context()), &taskstore.UpdateRequest{
			Task: newTask("missing", a2a.TaskStateWorking), PrevVersion: 1,
		})
		if !errors.Is(err, a2a.ErrTaskNotFound) {
			t.Fatalf("应译为 a2a.ErrTaskNotFound，实际 %v", err)
		}
	})
	t.Run("Update版本前进→ErrConcurrentModification", func(t *testing.T) {
		st := newFakeTaskStorage()
		ad := newTaskStore(st)
		if _, err := ad.Create(testA2AContext(t.Context()), newTask("t2", a2a.TaskStateSubmitted)); err != nil {
			t.Fatal(err)
		}
		// 拿过期版本更新。
		if _, err := ad.Update(testA2AContext(t.Context()), &taskstore.UpdateRequest{
			Task: newTask("t2", a2a.TaskStateWorking), PrevVersion: 1,
		}); err != nil {
			t.Fatalf("正确版本更新应成功: %v", err)
		}
		_, err := ad.Update(testA2AContext(t.Context()), &taskstore.UpdateRequest{
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
	v, err := ad.Create(testA2AContext(t.Context()), newTask("t1", a2a.TaskStateSubmitted))
	if err != nil || v != taskstore.TaskVersion(1) {
		t.Fatalf("Create 应返回版本 1: v=%d err=%v", v, err)
	}
	v, err = ad.Update(testA2AContext(t.Context()), &taskstore.UpdateRequest{
		Task: newTask("t1", a2a.TaskStateWorking), PrevVersion: 1,
	})
	if err != nil || v != taskstore.TaskVersion(2) {
		t.Fatalf("Update 应返回 PrevVersion+1=2: v=%d err=%v", v, err)
	}
	// PrevVersion==TaskVersionMissing：回读当前版本再更新（InMemory 同语义）。
	v, err = ad.Update(testA2AContext(t.Context()), &taskstore.UpdateRequest{
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
	if _, err := ad.Create(testA2AContext(t.Context()), orig); err != nil {
		t.Fatal(err)
	}
	got1, err := ad.Get(testA2AContext(t.Context()), "t1")
	if err != nil {
		t.Fatal(err)
	}
	got1.Task.Status.State = a2a.TaskStateFailed // 污染副本
	got2, err := ad.Get(testA2AContext(t.Context()), "t1")
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
	if _, err := ad.Create(testA2AContext(t.Context()), task); err != nil {
		t.Fatal(err)
	}

	two := 2
	resp, err := ad.List(testA2AContext(t.Context()), &a2a.ListTasksRequest{HistoryLength: &two})
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

	resp, err = ad.List(testA2AContext(t.Context()), &a2a.ListTasksRequest{IncludeArtifacts: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Tasks[0].Artifacts) != 1 {
		t.Error("IncludeArtifacts=true 应保留 artifacts")
	}
	zero := 0
	resp, err = ad.List(testA2AContext(t.Context()), &a2a.ListTasksRequest{HistoryLength: &zero})
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
	if _, err := ad.Create(testA2AContext(t.Context()), newTask("t1", a2a.TaskStateCompleted)); err != nil {
		t.Fatal(err)
	}
	after := time.Now().Add(-time.Hour)
	resp, err := ad.List(testA2AContext(t.Context()), &a2a.ListTasksRequest{
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
	resp, err = ad.List(testA2AContext(t.Context()), &a2a.ListTasksRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if resp.PageSize != 50 {
		t.Errorf("缺省 PageSize 应回填 50，实际 %d", resp.PageSize)
	}
}
