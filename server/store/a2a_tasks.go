package store

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/YouToco/vane/types"
)

// 本文件实现 a2a_tasks 表（migration 013）的五方法，契约 a2a-contract §4.1。
// SDK 类型不进本包：调用方（a2a/taskstore.go 适配层）只见 types.A2ATask，
// CodeNotFound/CodeConflict 由适配层按契约 §5.9 翻译为 SDK 哨兵错误。

// a2aTaskColumns 全列常量，SELECT 与 scanA2ATask 一一对应。
const a2aTaskColumns = `id, context_id, status, task, version, created_at, updated_at`

// scanA2ATask 把一行 a2a_tasks 扫进 types.A2ATask（复用于单行与 RETURNING）。
func scanA2ATask(row pgx.Row, t *types.A2ATask) error {
	return row.Scan(
		&t.ID, &t.ContextID, &t.Status, &t.Task,
		&t.Version, &t.CreatedAt, &t.UpdatedAt,
	)
}

// isUniqueViolation 判断错误链中是否有 Postgres 唯一约束冲突（SQLSTATE 23505）。
// pgconn 是 pgx 模块的子包，不构成新依赖。
func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

// CreateA2ATask 落新任务（version 走表默认 1）。id 冲突返回 CodeConflict。
// RETURNING 全列回填入参：调用方拿到的实体恒为数据库当前真实状态
// （version/created_at/updated_at 均由表默认值产生，同 CreateAgentSession 先例）。
func (s *Store) CreateA2ATask(ctx context.Context, t *types.A2ATask) error {
	err := scanA2ATask(s.pool.QueryRow(ctx,
		`INSERT INTO a2a_tasks (id, context_id, status, task)
		 VALUES ($1, $2, $3, $4)
		 RETURNING `+a2aTaskColumns,
		t.ID, t.ContextID, t.Status, t.Task), t)
	if err != nil {
		if isUniqueViolation(err) {
			return types.NewAppError(types.CodeConflict,
				fmt.Sprintf("A2A 任务 id=%s 已存在", t.ID), err)
		}
		return types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("插入 A2A 任务（id=%s）", t.ID), err)
	}
	return nil
}

// GetA2ATask 按 id 取任务；无行返回 CodeNotFound。JSONB 反序列化天然是深拷贝，
// 满足 SDK Get 的隔离要求（契约 §4.1）。
func (s *Store) GetA2ATask(ctx context.Context, id string) (*types.A2ATask, error) {
	var t types.A2ATask
	err := scanA2ATask(s.pool.QueryRow(ctx,
		`SELECT `+a2aTaskColumns+` FROM a2a_tasks WHERE id = $1`, id), &t)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, types.NewAppError(types.CodeNotFound,
				fmt.Sprintf("A2A 任务 id=%s 不存在", id), err)
		}
		return nil, types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("查询 A2A 任务（id=%s）", id), err)
	}
	return &t, nil
}

// UpdateA2ATask 乐观并发条件更新（契约 §4.1）：
//
//	UPDATE a2a_tasks SET status=$3, task=$4, version=version+1, updated_at=now()
//	WHERE id=$1 AND version=$2
//
// RowsAffected==0 时回查 id 区分：无行 → CodeNotFound；有行但版本已前进 → CodeConflict
// （a2a/taskstore.go 按契约 §5.9 完整哨兵映射表翻译）。成功后新版本恒 = expectedVersion+1。
func (s *Store) UpdateA2ATask(ctx context.Context, id string, expectedVersion int64, status string, task json.RawMessage) error {
	tag, err := s.pool.Exec(ctx,
		`UPDATE a2a_tasks
		 SET status = $3, task = $4, version = version + 1, updated_at = now()
		 WHERE id = $1 AND version = $2`,
		id, expectedVersion, status, task)
	if err != nil {
		return types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("更新 A2A 任务（id=%s, version=%d）", id, expectedVersion), err)
	}
	if tag.RowsAffected() == 0 {
		var exists bool
		if qerr := s.pool.QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM a2a_tasks WHERE id = $1)`, id).Scan(&exists); qerr != nil {
			return types.NewAppError(types.CodeDatabase,
				fmt.Sprintf("回查 A2A 任务（id=%s）", id), qerr)
		}
		if !exists {
			return types.NewAppError(types.CodeNotFound,
				fmt.Sprintf("A2A 任务 id=%s 不存在", id), nil)
		}
		return types.NewAppError(types.CodeConflict,
			fmt.Sprintf("A2A 任务 id=%s 版本已前进（期望 %d）", id, expectedVersion), nil)
	}
	return nil
}

// ListA2ATasks 服务 SDK taskstore.Store 强制的 List（契约 §4.1，签名即终版）：
//
//	WHERE 谓词 = ContextID（空串不过滤）AND status（空串不过滤）
//	             AND updated_at > StatusTimestampAfter（零值不过滤）
//	ORDER BY created_at DESC, id DESC；PageSize 钳制后截断
//	翻页：PageToken 为 (created_at,id) 键集游标（本包私有编解码）；items 满页时
//	      next = 末行键集编码，否则空串
//	total = 同谓词 COUNT(*)（供 SDK ListTasksResponse.TotalSize；不含游标谓词——
//	        游标是翻页位置，不是过滤条件，每页返回的都是全集大小）
//
// HistoryLength/IncludeArtifacts 的 task JSONB 裁剪归 a2a/taskstore.go 适配层（§5.9）。
func (s *Store) ListA2ATasks(ctx context.Context, q types.A2ATaskQuery) (items []types.A2ATask, total int64, next string, err error) {
	pageSize := clampA2APageSize(q.PageSize)

	// 三谓词动态 WHERE，全部参数化。
	var conds []string
	var args []any
	if q.ContextID != "" {
		args = append(args, q.ContextID)
		conds = append(conds, fmt.Sprintf("context_id = $%d", len(args)))
	}
	if q.Status != "" {
		args = append(args, q.Status)
		conds = append(conds, fmt.Sprintf("status = $%d", len(args)))
	}
	if !q.StatusTimestampAfter.IsZero() {
		args = append(args, q.StatusTimestampAfter)
		conds = append(conds, fmt.Sprintf("updated_at > $%d", len(args)))
	}
	whereOf := func(cs []string) string {
		if len(cs) == 0 {
			return ""
		}
		return " WHERE " + strings.Join(cs, " AND ")
	}

	// total：同谓词（不含游标）计数。先算 total 再查页，两条语句非同一快照，
	// 并发写入下可能有 ±1 级别的偏差——分页游标语义不受影响，可接受。
	if err := s.pool.QueryRow(ctx,
		`SELECT count(*) FROM a2a_tasks`+whereOf(conds), args...).Scan(&total); err != nil {
		return nil, 0, "", types.NewAppError(types.CodeDatabase, "统计 A2A 任务（List）", err)
	}

	// 游标谓词只进列表查询：ORDER BY 是 (created_at, id) 双降序，续页取键集之后的行。
	if q.PageToken != "" {
		cursorAt, cursorID, derr := decodeA2ACursor(q.PageToken)
		if derr != nil {
			return nil, 0, "", derr
		}
		args = append(args, cursorAt, cursorID)
		conds = append(conds, fmt.Sprintf("(created_at, id) < ($%d, $%d)", len(args)-1, len(args)))
	}
	args = append(args, pageSize)
	rows, err := s.pool.Query(ctx,
		`SELECT `+a2aTaskColumns+` FROM a2a_tasks`+whereOf(conds)+
			fmt.Sprintf(` ORDER BY created_at DESC, id DESC LIMIT $%d`, len(args)),
		args...)
	if err != nil {
		return nil, 0, "", types.NewAppError(types.CodeDatabase, "查询 A2A 任务列表", err)
	}
	defer rows.Close()

	for rows.Next() {
		var t types.A2ATask
		if err := scanA2ATask(rows, &t); err != nil {
			return nil, 0, "", types.NewAppError(types.CodeDatabase, "扫描 A2A 任务行", err)
		}
		items = append(items, t)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, "", types.NewAppError(types.CodeDatabase, "遍历 A2A 任务结果集", err)
	}

	// 满页才给 next：末页恰好满页时会多发一次空页请求，语义仍正确（不重不漏）。
	if len(items) == pageSize {
		last := items[len(items)-1]
		next = encodeA2ACursor(last.CreatedAt, last.ID)
	}
	return items, total, next, nil
}

// CountA2ATasks 只读计数，供 Gate 探针 P-A2A（契约 §10）：probe.Store 接口追加此一行
// 归 PR-3（probe/ 不在本 PR 范围），本方法先行就位。
func (s *Store) CountA2ATasks(ctx context.Context) (int64, error) {
	var n int64
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM a2a_tasks`).Scan(&n); err != nil {
		return 0, types.NewAppError(types.CodeDatabase, "统计 A2A 任务总数", err)
	}
	return n, nil
}

// FailStaleA2ATasks 把非终态（SUBMITTED/WORKING）且 updated_at 早于 olderThan 的任务
// 批量置为 FAILED，返回受影响行数（对抗审查 A-1）。
//
// 为什么需要：assistant.chat 的执行跑在 SDK 后台 goroutine（不随 HTTP Shutdown 取消），
// 进程重启/被 SIGKILL 时在飞任务被硬杀，DB 里永久停在 WORKING——轮询终态的对端 agent
// 永久挂起。启动时调用一次即可清账（当前单实例进程刚起、无活任务，任何非终态都是
// 上次的遗留）。提取列和 JSONB 权威任务必须在同一 UPDATE 里一起变为 FAILED：SDK
// Get/List 从 JSONB 反序列化，二者漂移会让筛选命中 FAILED 却向对端返回 WORKING。
// olderThan 仍由调用方决定；单实例启动传当前时刻，多实例部署前须改成 execution owner/generation。
func (s *Store) FailStaleA2ATasks(ctx context.Context, olderThan time.Time) (int64, error) {
	tag, err := s.pool.Exec(ctx,
		`UPDATE a2a_tasks
		 SET status='TASK_STATE_FAILED',
		     task=jsonb_set(
		       task,
		       '{status}',
		       (CASE WHEN jsonb_typeof(task->'status')='object' THEN task->'status' ELSE '{}'::jsonb END)
		         || jsonb_build_object('state','TASK_STATE_FAILED','timestamp',now()),
		       true
		     ),
		     version=version+1,
		     updated_at=now()
		 WHERE status IN ('TASK_STATE_SUBMITTED','TASK_STATE_WORKING') AND updated_at < $1`,
		olderThan)
	if err != nil {
		return 0, types.NewAppError(types.CodeDatabase, "清理滞留的 A2A 任务", err)
	}
	return tag.RowsAffected(), nil
}

// clampA2APageSize 钳制 ListA2ATasks 页大小到 [1,200]：<=0 → 50（契约 §3 缺省）。
func clampA2APageSize(n int) int {
	switch {
	case n <= 0:
		return 50
	case n > 200:
		return 200
	default:
		return n
	}
}

// encodeA2ACursor 把 (created_at, id) 键集位置编码为分页游标。格式是本包实现细节
// （可逆、对调用方不透明，契约 §4.1）：base64url(unixMicro "|" id)。
// timestamptz 精度是微秒，UnixMicro 往返无损；id（TEXT）不含 "|" 之外的约束——
// 解码按第一个 "|" 切分，id 自身含 "|" 也不破坏可逆性。
func encodeA2ACursor(createdAt time.Time, id string) string {
	raw := strconv.FormatInt(createdAt.UnixMicro(), 10) + "|" + id
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

// decodeA2ACursor 解码 encodeA2ACursor 的产物；非法游标返回 CodeValidation
// （入站 PageToken 不可信，坏游标是调用方错误而非数据库故障）。
func decodeA2ACursor(token string) (time.Time, string, error) {
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return time.Time{}, "", types.NewAppError(types.CodeValidation,
			"无效的 A2A 分页游标", err)
	}
	micros, id, ok := strings.Cut(string(raw), "|")
	if !ok {
		return time.Time{}, "", types.NewAppError(types.CodeValidation,
			"无效的 A2A 分页游标", nil)
	}
	us, err := strconv.ParseInt(micros, 10, 64)
	if err != nil {
		return time.Time{}, "", types.NewAppError(types.CodeValidation,
			"无效的 A2A 分页游标", err)
	}
	return time.UnixMicro(us), id, nil
}
