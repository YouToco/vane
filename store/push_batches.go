package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/types"
)

// CreatePushBatch 新建一个推送批次并返回 batch_id。
// status 走 001 的 DB 默认 'pending'；scheduled_at 留 NULL（即时批次无预定时间，
// 由 Temporal Schedule 触发时刻决定）。
func (s *Store) CreatePushBatch(ctx context.Context, userID int64) (int64, error) {
	var id int64
	err := s.pool.QueryRow(ctx,
		`INSERT INTO push_batches (tenant_id, user_id) VALUES (`+tenantOfUser+`$1), $1) RETURNING id`, userID).Scan(&id)
	if err != nil {
		return 0, types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("创建推送批次（user=%d）", userID), err)
	}
	return id, nil
}

// CreatePushBatchIdempotent 按幂等键创建/复用批次：同一 idempKey 重复调用返回同一 batch_id。
// idempKey 用 workflow 的确定性 traceID——Temporal 重试 Push Activity 时复用同一批次，
// 是"重试不重复发卡"的地基。
//
// 为什么用 ON CONFLICT DO UPDATE 而非 DO NOTHING：DO NOTHING 在命中冲突时不返回行，
// RETURNING id 会拿到 pgx.ErrNoRows；改用 DO UPDATE（把 user_id 写成它本来的值，等价空更新）
// 保证冲突路径也有行返回、RETURNING id 恒能拿到既有批次 id。
// ON CONFLICT 的 WHERE 谓词必须与 004 的部分唯一索引 uq_push_batches_idem 一致，
// Postgres 才能推断到该索引作为 arbiter。idempKey 恒非空，故一定命中该部分索引。
//
// DO UPDATE 里的 `exit_gate = ”, stage_counts = '{}'` 是 009 加的**反向复位**，
// 与 RecordEmptyPushBatch 的防覆写护栏成镜像，两者缺一不可：
//
//	护栏挡的是「空批次盖掉真实批次」（empty 写在 done 之后）；
//	本复位挡的是「真实批次继承空批次的判词」（done 写在 empty 之后）。
//
// 后者的形状是：同一 traceID 先在 fetch 闸门记了 status='empty' exit_gate='fetch'，
// 随后 Temporal reset 重跑、这次 Fetch 抓到了内容一路走到 Push——Push 复用同一行，
// 收尾只改 status，于是留下一条 status='done'、挂着已发投递、却写着"没抓到任何
// 新内容"的嵌合行。它和护栏要防的那条一样自相矛盾，只是从另一个方向到达。
// 而这个方向**更容易发生**："今早怎么没推？修好信源 reset 重跑一次"正是本 PR
// 制造出的可见性会引出来的运维动作。由合并前的双怀疑者审查实测发现。
//
// 复位不动幂等地基：arbiter、WHERE 谓词、RETURNING id 一字未改，新增的只是
// SET 里两个赋值。正常 Push 重试时这两列本就是空值，写空是等值空更新。
// scheduleID 记录触发本批的定时任务（P1b，可空："" → NULL，即 push_now/老任务的用户级语义）。
// 冲突路径（Temporal 重试复用批次）刻意不更新 schedule_id：首次插入已定，归属不因重试改变。
func (s *Store) CreatePushBatchIdempotent(ctx context.Context, userID int64, idempKey, scheduleID string) (int64, error) {
	var id int64
	err := s.pool.QueryRow(ctx,
		`INSERT INTO push_batches (tenant_id, user_id, idempotency_key, schedule_id) VALUES (`+tenantOfUser+`$1), $1, $2, $3)
		 ON CONFLICT (idempotency_key) WHERE idempotency_key <> ''
		 DO UPDATE SET user_id = EXCLUDED.user_id, exit_gate = '', stage_counts = '{}'
		 RETURNING id`, userID, idempKey, nullableText(scheduleID)).Scan(&id)
	if err != nil {
		return 0, types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("幂等创建批次（user=%d, key=%s）", userID, idempKey), err)
	}
	return id, nil
}

// nullableText 把空串归一为 SQL NULL：pgx 写 Go "" 是空串而非 NULL，而可空列
// （如 push_batches.schedule_id）要用 NULL 表达"无归属任务"（push_now/老任务）。
func nullableText(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// RecordEmptyPushBatch 记录一次"跑完了但没东西可推"的空批次（009 / 契约 §16
// 修订记录「空批次缺口」）。返回 batch_id；返回 (0, nil) 表示**刻意跳过**（见下）。
//
// 为什么新开一个方法而不扩 CreatePushBatchIdempotent 的参数：后者是 Push 活动
// "重试不重复发卡"的地基（修 #1 CRITICAL），改它的签名要动 workflow 的 Store
// 窄接口与全部测试替身，而这条路径与本功能**没有任何交集**——空批次与真实推送
// 在同一次运行里互斥（五处闸门全在 Push 之前 return）。为一个正交的新语义去动
// 核心幂等路径，是拿地基换便利。
//
// 幂等同样按 idempotency_key = traceID，复用 004 的 uq_push_batches_idem 部分唯一
// 索引：Temporal 重试本活动时命中冲突走 DO UPDATE 覆写同值，不会长出第二行。
// ON CONFLICT 的 WHERE 谓词必须与 004:12 的索引谓词逐字一致，Postgres 才推断得到
// 该索引作 arbiter；idempKey 恒非空（VALIDATION 已挡在前面），故必定命中它。
//
// DO UPDATE 上的 `WHERE push_batches.status = 'empty'` 是一道**防覆写护栏**：
// 只有空批次记录能被空批次记录覆盖。触发场景真实存在——Temporal reset 一个已完成的
// 推送运行时，traceID 由 SideEffect 从历史重放为同一个值（见 PushPipelineWorkflow 开头
// 生成 traceID 的 workflow.SideEffect），
// 而重放的这一趟因为内容已投递过会在 fetch 闸门空退，若无护栏就会把一条
// status='done'、底下挂着 5 条 deliveries 的真实批次改写成 'empty'——库里从此
// 存着一条"没推任何东西却有 5 条投递"的自相矛盾行。
//
// 护栏拦下时返回 skipped=true 而**不是**错误：该 traceID 已有真实批次（done/failed，
// 或 pending——那说明 Push 起跑过），本就不该记空批次，拦对了。
//
// 为什么要有 skipped 这个返回值、而不是让调用方看 id==0 或者干脆不管：
// 这是一个"记录悄悄没写成"的路径，而本 PR 的全部意义就是消灭静默。护栏在正常路径上
// 不可能命中（空批次与真实推送在一次运行里互斥），所以它一旦命中就是**有意思的事**——
// 要么有人 reset 了运行，要么出现了第三个写 push_batches 的路径。调用方据此打一条
// 日志，让它响一声。多返回值表达幂等结局是本包既有惯例（见 InsertDeliveryIdempotent
// 的 existed/sentAlready）。
func (s *Store) RecordEmptyPushBatch(ctx context.Context, userID int64, idempKey, scheduleID string,
	gate types.BatchExitGate, counts types.PipelineCounts) (id int64, skipped bool, err error) {
	// 闸门必须有值：status='empty' 且 exit_gate='' 的行说的是"没推，但不知道为什么"，
	// 正是本功能要消灭的那种记录。宁可不写（调用方 Warn 后照常走正常终态），
	// 也不写一行假装自己有信息的行。VALIDATION 不可重试，重试只是重复失败。
	if gate == "" {
		return 0, false, types.NewAppError(types.CodeValidation, "空批次必须带退出闸门", nil)
	}
	if idempKey == "" {
		// 空键会落到 004 部分唯一索引之外（WHERE idempotency_key <> ''），
		// 于是每次重试都新插一行——幂等静默失效。挡在这里而不是让它悄悄长行。
		return 0, false, types.NewAppError(types.CodeValidation, "空批次必须带幂等键", nil)
	}

	countsJSON, err := json.Marshal(counts)
	if err != nil {
		return 0, false, types.NewAppError(types.CodeInternal, "序列化 pipeline 漏斗计数", err)
	}

	err = s.pool.QueryRow(ctx,
		`INSERT INTO push_batches (tenant_id, user_id, status, exit_gate, stage_counts, idempotency_key, schedule_id)
		 VALUES (`+tenantOfUser+`$1), $1, $2, $3, $4, $5, $6)
		 ON CONFLICT (idempotency_key) WHERE idempotency_key <> ''
		 DO UPDATE SET exit_gate = EXCLUDED.exit_gate, stage_counts = EXCLUDED.stage_counts
		 WHERE push_batches.status = $2
		 RETURNING id`,
		userID, types.BatchStatusEmpty, gate, countsJSON, idempKey, nullableText(scheduleID)).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, true, nil // 护栏拦下：该 traceID 已有真实批次，不覆写。
	}
	if err != nil {
		return 0, false, types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("记录空批次（user=%d, gate=%s）", userID, gate), err)
	}
	return id, false, nil
}

// UpdatePushBatchStatus 推进批次状态。有内容可推时的生命周期是 pending→done|failed：
// 没有中间态，Push 活动跑完才一次性落终态。无内容可推的 empty 是终态，由
// RecordEmptyPushBatch 一次写成，不经过本方法。
//
// push_batches 无 updated_at 列（001），故只改 status。
func (s *Store) UpdatePushBatchStatus(ctx context.Context, batchID int64, status types.BatchStatus) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE push_batches SET status = $2 WHERE id = $1`, batchID, status)
	if err != nil {
		return types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("更新批次状态（id=%d）", batchID), err)
	}
	return nil
}
