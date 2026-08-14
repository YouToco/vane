// SDK taskstore.Store → TaskStorage 适配（契约 §5.9）：SDK 触点与哨兵错误的唯一翻译层。
package a2a

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv/taskstore"

	"github.com/YouToco/vane/server/types"
)

// storeAdapter 把 store 层（types.A2ATask + 哨兵 AppError）适配到 SDK taskstore.Store。
// a2a.Task 与 []byte 的互转用标准库 encoding/json——SDK v2.3.1 实测：a2a.Task 是带
// json tag 的普通 struct，SDK 自身传输层即 json.Marshal（契约 §2 的"ProtoJSON"指
// status 枚举的 TASK_STATE_* 字符串形态，序列化器不是 protojson，PR 描述已记勘误）。
type storeAdapter struct {
	st TaskStorage
}

func newTaskStore(st TaskStorage) *storeAdapter { return &storeAdapter{st: st} }

var _ taskstore.Store = (*storeAdapter)(nil)

// storeErr 是适配层非哨兵错误的唯一出口（契约 §8.1 红线，审查 HIGH 实证）：
// SDK 的 toJSONRPCError 把 taskstore 错误的 Error() 文本逐字写进 JSON-RPC
// error.message——裸返回 pgx 错误链等于把 DB 细节送上公网。原始错误落 slog，
// 对外只有 ErrInternalError 哨兵（SDK 映射 -32603）+ sanitize 文案。
func storeErr(op, taskID string, err error) error {
	slog.Error("a2a: taskstore "+op+" 失败", "task_id", taskID, "err", err)
	return fmt.Errorf("%w: %s", a2a.ErrInternalError, sanitize(err))
}

// opCtx 给每个适配层 DB 操作套请求级超时（审查 CONFIRMED：SDK v2.3.1 的执行跑在
// context.WithoutCancel 的后台 goroutine 里，taskstore 写库不随请求/关停取消——
// 无界的库调用会让 pgxpool.Close 无限阻塞、或把任务永久滞留 WORKING。
// 契约 §5.2 的"无自有后台 goroutine"只对 executor 成立，SDK 侧不成立，勘误见 §5.8）。
func opCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, dbQueryTimeout)
}

// marshalTask 序列化 a2a.Task 为 JSONB 权威载荷。
func marshalTask(task *a2a.Task) (json.RawMessage, error) {
	raw, err := json.Marshal(task)
	if err != nil {
		return nil, fmt.Errorf("序列化 a2a.Task（id=%s）: %w", task.ID, err)
	}
	return raw, nil
}

// unmarshalTask 从 JSONB 载荷还原 a2a.Task。json.Unmarshal 产出全新对象树，
// 天然满足 SDK Get 的深拷贝隔离要求。
func unmarshalTask(t *types.A2ATask) (*a2a.Task, error) {
	var task a2a.Task
	if err := json.Unmarshal(t.Task, &task); err != nil {
		return nil, fmt.Errorf("反序列化 a2a_tasks.task（id=%s）: %w", t.ID, err)
	}
	return &task, nil
}

// Create 落新任务。id 已存在 → taskstore.ErrTaskAlreadyExists（接口文档 "should return"）；
// 成功返回 TaskVersion(1)（表默认值，契约 §5.9）。
func (a *storeAdapter) Create(ctx context.Context, task *a2a.Task) (taskstore.TaskVersion, error) {
	ctx, cancel := opCtx(ctx)
	defer cancel()
	raw, err := marshalTask(task)
	if err != nil {
		return taskstore.TaskVersionMissing, storeErr("Create/marshal", string(task.ID), err)
	}
	err = a.st.CreateA2ATask(ctx, &types.A2ATask{
		ID:        string(task.ID),
		ContextID: task.ContextID,
		Status:    string(task.Status.State),
		Task:      raw,
	})
	if err != nil {
		if errors.Is(err, types.ErrConflict) {
			return taskstore.TaskVersionMissing, taskstore.ErrTaskAlreadyExists
		}
		return taskstore.TaskVersionMissing, storeErr("Create", string(task.ID), err)
	}
	return taskstore.TaskVersion(1), nil
}

// Get 按 id 取任务。无行 → a2a.ErrTaskNotFound（§9.2 的 -32001 断言依赖本行）。
func (a *storeAdapter) Get(ctx context.Context, taskID a2a.TaskID) (*taskstore.StoredTask, error) {
	ctx, cancel := opCtx(ctx)
	defer cancel()
	row, err := a.st.GetA2ATask(ctx, string(taskID))
	if err != nil {
		if errors.Is(err, types.ErrNotFound) {
			return nil, a2a.ErrTaskNotFound
		}
		return nil, storeErr("Get", string(taskID), err)
	}
	task, err := unmarshalTask(row)
	if err != nil {
		return nil, storeErr("Get/unmarshal", string(taskID), err)
	}
	return &taskstore.StoredTask{Task: task, Version: taskstore.TaskVersion(row.Version)}, nil
}

// Update 乐观并发更新。无行 → a2a.ErrTaskNotFound；版本已前进 →
// taskstore.ErrConcurrentModification（接口文档 MUST）。成功返回 PrevVersion+1
// （store UPDATE 语句保证恒成立，契约 §4.1）。
// PrevVersion==TaskVersionMissing（SDK "不跟踪版本"哨兵）时先回读当前版本再条件更新
// （InMemory 同语义：跳过乐观锁检查；回读窗口内的并发前进会得 Conflict，语义仍正确）。
func (a *storeAdapter) Update(ctx context.Context, update *taskstore.UpdateRequest) (taskstore.TaskVersion, error) {
	ctx, cancel := opCtx(ctx)
	defer cancel()
	raw, err := marshalTask(update.Task)
	if err != nil {
		return taskstore.TaskVersionMissing, storeErr("Update/marshal", string(update.Task.ID), err)
	}
	prev := update.PrevVersion
	if prev == taskstore.TaskVersionMissing {
		row, err := a.st.GetA2ATask(ctx, string(update.Task.ID))
		if err != nil {
			if errors.Is(err, types.ErrNotFound) {
				return taskstore.TaskVersionMissing, a2a.ErrTaskNotFound
			}
			return taskstore.TaskVersionMissing, storeErr("Update/get-version", string(update.Task.ID), err)
		}
		prev = taskstore.TaskVersion(row.Version)
	}
	err = a.st.UpdateA2ATask(ctx, string(update.Task.ID), int64(prev),
		string(update.Task.Status.State), raw)
	if err != nil {
		switch {
		case errors.Is(err, types.ErrNotFound):
			return taskstore.TaskVersionMissing, a2a.ErrTaskNotFound
		case errors.Is(err, types.ErrConflict):
			return taskstore.TaskVersionMissing, taskstore.ErrConcurrentModification
		}
		return taskstore.TaskVersionMissing, storeErr("Update", string(update.Task.ID), err)
	}
	return prev + 1, nil
}

// List 检索任务列表。Status/ContextID/StatusTimestampAfter/PageSize/PageToken 直传
// store（types.A2ATaskQuery，契约 §3）；TotalSize/NextPageToken 取 store 返回值；
// HistoryLength/IncludeArtifacts 是 task JSONB 裁剪语义，在本层处理，store 不感知。
// Tenant 单租户恒空不映射（契约 §3）。
func (a *storeAdapter) List(ctx context.Context, req *a2a.ListTasksRequest) (*a2a.ListTasksResponse, error) {
	q := types.A2ATaskQuery{
		ContextID: req.ContextID,
		Status:    string(req.Status),
		PageSize:  req.PageSize,
		PageToken: req.PageToken,
	}
	if req.StatusTimestampAfter != nil {
		q.StatusTimestampAfter = *req.StatusTimestampAfter
	}
	ctx, cancel := opCtx(ctx)
	defer cancel()
	rows, total, next, err := a.st.ListA2ATasks(ctx, q)
	if err != nil {
		return nil, storeErr("List", "", err)
	}
	tasks := make([]*a2a.Task, 0, len(rows))
	for i := range rows {
		task, err := unmarshalTask(&rows[i])
		if err != nil {
			return nil, storeErr("List/unmarshal", rows[i].ID, err)
		}
		trimHistory(task, req.HistoryLength)
		if !req.IncludeArtifacts {
			task.Artifacts = nil
		}
		tasks = append(tasks, task)
	}
	return &a2a.ListTasksResponse{
		Tasks:         tasks,
		TotalSize:     int(total),
		PageSize:      effectivePageSize(req.PageSize),
		NextPageToken: next,
	}, nil
}

// defaultHistoryLimit 是 HistoryLength 未给时的历史截断上限（SDK InMemory 同值）。
const defaultHistoryLimit = 100

// trimHistory 按 SDK InMemory 语义裁剪 history 尾部：nil → 默认上限 100；0 → 置空。
func trimHistory(task *a2a.Task, historyLength *int) {
	limit := defaultHistoryLimit
	if historyLength != nil {
		limit = *historyLength
	}
	if limit <= 0 {
		task.History = nil
		return
	}
	if len(task.History) > limit {
		task.History = task.History[len(task.History)-limit:]
	}
}

// effectivePageSize 复算 store 的钳制口径（<=0 → 50、上限 200，契约 §3），
// 供 ListTasksResponse.PageSize 回填。
func effectivePageSize(requested int) int {
	if requested <= 0 {
		return 50
	}
	if requested > 200 {
		return 200
	}
	return requested
}
