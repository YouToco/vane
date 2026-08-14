package a2a

import (
	"context"
	"encoding/json"
	"sort"
	"sync"
	"time"

	"github.com/YouToco/vane/server/types"
)

// fakeContent 是 ContentStore 替身（仿 agent loop_test.go fakeStore：错误注入位 + 调用留痕）。
type fakeContent struct {
	items []types.ContentItem
	err   error

	mu         sync.Mutex
	gotKeyword string
	gotSince   time.Time
	gotLimit   int
	calls      int
}

func (f *fakeContent) SearchContentItems(_ context.Context, keyword string, since time.Time, limit int) ([]types.ContentItem, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.gotKeyword, f.gotSince, f.gotLimit = keyword, since, limit
	if f.err != nil {
		return nil, f.err
	}
	return f.items, nil
}

// fakeTaskStorage 是 TaskStorage 替身：内存 map + 与 store 层相同的版本语义
// （Create 版本 1；Update 条件版本匹配否则 CodeConflict；无行 CodeNotFound），
// 让 SDK handler 的完整任务生命周期能在 httptest 层走通。
type fakeTaskStorage struct {
	mu   sync.Mutex
	rows map[string]*types.A2ATask
	// 错误注入位：非 nil 时对应方法直接返回该错误。
	createErr, getErr, updateErr, listErr error
}

func newFakeTaskStorage() *fakeTaskStorage {
	return &fakeTaskStorage{rows: make(map[string]*types.A2ATask)}
}

func (f *fakeTaskStorage) CreateA2ATask(_ context.Context, t *types.A2ATask) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.createErr != nil {
		return f.createErr
	}
	if _, ok := f.rows[t.ID]; ok {
		return types.NewAppError(types.CodeConflict, "任务已存在", nil)
	}
	cp := *t
	cp.Version = 1
	now := time.Now()
	cp.CreatedAt, cp.UpdatedAt = now, now
	f.rows[t.ID] = &cp
	return nil
}

func (f *fakeTaskStorage) GetA2ATask(_ context.Context, id string) (*types.A2ATask, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.getErr != nil {
		return nil, f.getErr
	}
	row, ok := f.rows[id]
	if !ok {
		return nil, types.NewAppError(types.CodeNotFound, "任务不存在", nil)
	}
	cp := *row
	cp.Task = append(json.RawMessage(nil), row.Task...)
	return &cp, nil
}

func (f *fakeTaskStorage) UpdateA2ATask(_ context.Context, id string, expectedVersion int64, status string, task json.RawMessage) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.updateErr != nil {
		return f.updateErr
	}
	row, ok := f.rows[id]
	if !ok {
		return types.NewAppError(types.CodeNotFound, "任务不存在", nil)
	}
	if row.Version != expectedVersion {
		return types.NewAppError(types.CodeConflict, "版本已前进", nil)
	}
	row.Version++
	row.Status = status
	row.Task = append(json.RawMessage(nil), task...)
	row.UpdatedAt = time.Now()
	return nil
}

func (f *fakeTaskStorage) ListA2ATasks(_ context.Context, q types.A2ATaskQuery) ([]types.A2ATask, int64, string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.listErr != nil {
		return nil, 0, "", f.listErr
	}
	var out []types.A2ATask
	for _, row := range f.rows {
		if q.ContextID != "" && row.ContextID != q.ContextID {
			continue
		}
		if q.Status != "" && row.Status != q.Status {
			continue
		}
		cp := *row
		out = append(out, cp)
	}
	// 对齐 store 契约的 ORDER BY created_at DESC, id DESC（chatHistory 依赖此序）。
	sort.Slice(out, func(i, j int) bool {
		if !out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].CreatedAt.After(out[j].CreatedAt)
		}
		return out[i].ID > out[j].ID
	})
	return out, int64(len(out)), "", nil
}
