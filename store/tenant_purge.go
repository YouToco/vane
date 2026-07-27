package store

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/types"
)

// ============================================================
// 租户硬删（D9 保留期到期后的清理）
// ============================================================
//
// 这是全仓**唯一不可逆**的批量删除路径，红线是契约 I-A3：
//
//	硬删只删「租户所有表」。sources / content_items / content_sources
//	是**跨租户客观事实**——同一篇文章可能被多个租户的信源指向，删了会伤到别人。
//
// 三道结构性保证（不是纪律要求）：
//
//  1. **清单是白名单，不是黑名单**。只删列在 purgeOrder 里的表；漏加一张新表的后果是
//     「数据没删干净」（可修复），而黑名单漏加一张共享表的后果是「删了别人的数据」（不可逆）。
//     两种错误的代价不对称，所以必须用白名单。
//  2. **清单与 schema 对账**（见 tenant_purge_test.go）：任何新增带 tenant_id 的表、
//     或新增引用租户表的外键，都会让守卫测试变红——而不是等到某次清理静默漏下数据。
//  3. **dry-run 用「真删 + 回滚」而非计数**：它真正执行 DELETE，因此会验证外键顺序、
//     约束、权限。只数行数的 dry-run 会漏掉「顺序错了删不动」这类问题，
//     而那正是这条路径最可能出错的地方。

// purgeStep 是硬删的一步：一张表 + 它的租户归属判据。
//
// where 里的 $1 是 tenant_id。**不能统一写成 `tenant_id = $1`**：
// schedule_playbooks / schedule_sources 没有 tenant_id 列，只能经 schedules 反查——
// 而它们同样是租户数据，漏删会在 tenants 行被删时撞外键（失败），或残留孤儿行。
type purgeStep struct {
	table string
	where string
}

// purgeOrder 是删除顺序，**子表在前、父表在后**——外键不允许先删被引用的行。
//
// 顺序由 information_schema 的外键关系推导（2026-07-19 实测）：
//
//	schedule_playbooks / schedule_sources → schedules
//	feedbacks                             → deliveries
//	task_creation_receipts               → pending_actions → agent_sessions
//	agent_turn_context_snapshots / agent_session_fact_outbox /
//	agent_session_projection_authority_events /
//	agent_events → agent_sessions
//	push_effects                        → deliveries / push_batches / task_run_snapshots
//	brief_snapshots                    → task_run_outcomes / push_batches / task_run_snapshots
//	task_run_outcomes                  → task_run_snapshots
//	task_observed_events               → deliveries / task_run_snapshots
//	task_event_qualification_steps     → task_run_snapshots
//	deliveries                            → push_batches
//	deliveries → push_batches             → task_run_snapshots
//	task_run_snapshot_v2_shadows          → task_run_snapshots
//	task_run_snapshots                    → task_run_snapshot_v2_cutover_events
//	schedules(current cutover pointer)     → task_run_snapshot_v2_cutover_events
//	task_definition_edit_receipts         → task_definition_edit_operations
//	schedules(definition-edit marker)     → task_definition_edit_operations
//	schedule_commands                     → tenants / users
//	task_adaptive_states                  → task_approved_definition_versions → schedules
//	profile_edit_receipts                 → profile_edit_revisions
//	其余                                   → tenants / users
//
// 排错了不会静默——FK 约束会让整个事务失败，而 dry-run 就是为了在真删之前撞出这个。
var purgeOrder = []purgeStep{
	// Edit receipts are children of the durable operation. Deleting the operation
	// clears a surviving schedule's operation/fence marker through its scoped FK;
	// both audit tables still need explicit rows in the purge report.
	{"task_definition_edit_receipts", "tenant_id = $1"},
	{"task_definition_edit_operations", "tenant_id = $1"},
	// Durable schedule command audit has no schedule FK so completed delete
	// tombstones survive normal task deletion. Tenant hard-delete explicitly
	// removes it before the tenant/user parents.
	{"schedule_commands", "tenant_id = $1"},
	// 无 tenant_id、经 schedules 反查的两张（正因为没有 tenant_id，
	// 「按 tenant_id 列对账」的守卫看不见它们，必须靠外键可达性那条守卫兜住）。
	{"schedule_playbooks", "schedule_id IN (SELECT id FROM schedules WHERE tenant_id = $1)"},
	{"schedule_sources", "schedule_id IN (SELECT id FROM schedules WHERE tenant_id = $1)"},
	// Adaptive 的 last-known-good 外键指向 immutable definition，因此必须先删。
	{"task_adaptive_states", "tenant_id = $1"},
	// 删除当前 definition 会由 FK 把 schedules 收敛为 compiled/headless，随后
	// schedules 自己在父表位置删除；动态任务也不会短暂留下无 definition 的模式。
	{"task_approved_definition_versions", "tenant_id = $1"},

	{"feedback_freshness_triage", "tenant_id = $1"},
	{"feedbacks", "tenant_id = $1"},
	{"task_creation_receipts", "tenant_id = $1"},
	{"agent_action_continuation_authority_events", "tenant_id = $1"},
	{"agent_action_continuations", "tenant_id = $1"},
	{"pending_actions", "tenant_id = $1"},
	{"agent_turn_context_snapshots", "tenant_id = $1"},
	{"agent_session_fact_outbox", "tenant_id = $1"},
	// Projection authority and semantic events reference agent_sessions by the
	// complete tenant/user/session scope and therefore must be deleted before
	// the session projection. Authority is locked/read after the root session
	// by writers, but purge already pre-locks session roots before child deletes.
	{"agent_session_projection_authority_events", "tenant_id = $1"},
	{"agent_events", "tenant_id = $1"},
	// External effect checkpoints bind exact delivery, batch, and immutable run
	// identities, so they must be removed before all three parent aggregates.
	{"push_effects", "tenant_id = $1"},
	// Canonical Briefs bind the finalized outcome, exact batch and immutable
	// run. Delete the whole snapshot first, then its outcome marker, before any
	// of the three retained parents.
	{"brief_snapshots", "tenant_id = $1"},
	{"task_run_outcomes", "tenant_id = $1"},
	// Observation admission and paid-step checkpoints bind immutable task/run
	// identities. Events may additionally bind a delivery after it is created.
	{"task_observed_events", "tenant_id = $1"},
	{"task_event_qualification_steps", "tenant_id = $1"},
	{"deliveries", "tenant_id = $1"},
	{"push_batches", "tenant_id = $1"},
	// Compiled push batches retain the immutable run snapshot through migration
	// 031, so batches must be gone before either the marked parent or its
	// sidecar. A marked run also points at its immutable cutover event; parents
	// must be gone before that event.
	{"task_run_snapshot_v2_shadows", "tenant_id = $1"},
	{"task_run_snapshots", "tenant_id = $1"},
	{"agent_sessions", "tenant_id = $1"},
	{"subscriptions", "tenant_id = $1"},
	// HTTP response-loss receipts reference the append-only revision. Both are
	// tenant evidence and must be removed before the profile/membership roots.
	{"profile_edit_receipts", "tenant_id = $1"},
	{"profile_edit_revisions", "tenant_id = $1"},
	{"profiles", "tenant_id = $1"},
	{"schedules", "tenant_id = $1"},
	// schedules may point at the current event. Deleting the task removes that
	// pointer row while preserving event evidence; tenant purge then reports
	// and deletes the now-unreferenced immutable events explicitly.
	{"task_run_snapshot_v2_cutover_events", "tenant_id = $1"},
	{"llm_calls", "tenant_id = $1"},
	{"tool_calls", "tenant_id = $1"},
	{"user_sessions", "tenant_id = $1"},

	// tenant_quota 与 memberships 一样是纯租户所有的行，无子表引用它，位置随意；
	// 放在 memberships 前只是为了让"归属类"的几张挨在一起。
	{"tenant_quota", "tenant_id = $1"},
	{"memberships", "tenant_id = $1"},
	{"invites", "consumed_by_tenant = $1"},
}

// purgeSharedTables 是**绝不能出现在 purgeOrder 里**的跨租户客观事实表（红线 I-A3）。
// 单列出来是为了让守卫测试能直接断言，而不是靠 review 时肉眼比对。
var purgeSharedTables = []string{"sources", "content_items", "content_sources"}

// PurgeReport 是一次清理的结果。dry-run 与真删返回同样的结构，
// 便于运维先看一眼「会删掉什么」再决定要不要执行。
type PurgeReport struct {
	TenantID int64
	Rows     map[string]int64 // 表名 → 删除行数
	Total    int64
	DryRun   bool
}

// ListPurgeableTenants 返回已过保留期、可以硬删的租户 id。
func (s *Store) ListPurgeableTenants(ctx context.Context) ([]int64, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id FROM tenants
		  WHERE status = 'deleting' AND purge_after IS NOT NULL AND purge_after <= now()
		  ORDER BY id`)
	if err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "查询待清理租户", err)
	}
	defer rows.Close()
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, types.NewAppError(types.CodeDatabase, "扫描待清理租户", err)
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// PurgeTenant 硬删一个租户的全部数据。
//
// dryRun=true 时**照常执行全部 DELETE，最后回滚**——这样返回的行数是真实的，
// 且外键顺序、约束、权限都被真正验证过。只数行数的 dry-run 验不了这些，
// 而"顺序错了删不动"恰恰是这条路径最可能出的错。
//
// 调用方必须自行确认该租户确实已过保留期（用 ListPurgeableTenants），
// 本方法不重复判定——它是一把刀，不是闸门；闸门在调用方。
func (s *Store) PurgeTenant(ctx context.Context, tenantID int64, dryRun bool) (*PurgeReport, error) {
	tx, err := s.beginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "开启清理事务", err)
	}
	// 回滚兜底：dry-run 走下方显式 Rollback，真删走 Commit；两者之后这里都是 no-op。
	defer func() { _ = tx.Rollback(ctx) }()
	if err := lockPushEffectSchemaWriter(ctx, tx); err != nil {
		return nil, types.NewAppError(
			types.CodeDatabase, "锁定推送效果 schema 准入", err)
	}
	if _, err := tx.Exec(ctx,
		`SELECT set_config('app.tenant_id', $1, true)`,
		fmt.Sprintf("%d", tenantID)); err != nil {
		return nil, types.NewAppError(
			types.CodeDatabase, "设置租户清理上下文", err)
	}

	// Join the same per-tenant admission protocol as effect preparation before
	// taking any child row lock. This is an advisory root rather than a tenants
	// row lock so the schedule-first definition-edit order below stays acyclic.
	if _, err := lockTenantAdmissionRoot(ctx, tx, tenantID); err != nil {
		return nil, types.NewAppError(
			types.CodeDatabase, "锁定租户清理准入根", err)
	}

	// Across the purge/coordinator/receipt/session conflict set, every path
	// follows this shared partial order (including FK-acquired parent locks):
	//
	//   task-creation operation -> membership/user capacity -> schedule
	//   task-creation operation -> task-creation receipt -> session
	//   schedule -> definition-edit operation -> definition-edit receipt -> session
	//   session -> agent event -> tenant (event FK)
	//   every tenant-owned child -> tenant delete
	//
	// Purge deliberately does not pre-lock tenant, membership, or the shared
	// user capacity row. Tenant is the FK parent reached last by normal turns
	// and definition-edit creation, while users can span tenants and must never
	// be pulled into a tenant-wide purge. Existing Task Creation sagas use their
	// pending_action as the root before any identity/capacity or aggregate lock.
	//
	// The two receipt dispatchers really lock their receipt before updating the
	// session. Definition-edit coordinators lock schedule -> operation before
	// inserting/verifying the receipt; task-creation coordinators lock the
	// pending action before inserting its receipt. Purge must therefore lock
	// both complete parent/receipt chains before any session. Its old
	// session-first prelock formed receipt -> session -> receipt with either
	// dispatcher and could raise PostgreSQL 40P01.
	//
	// These are tenant-scoped row locks, drained in stable primary-key order;
	// no table-wide or cross-tenant lock is used. They precede the FK-safe
	// child-first DELETE sequence, whose statement order need not equal lock
	// order once every conflicting row is already fenced.
	purgeLocks := []struct {
		name  string
		query string
	}{
		{
			name: "push_batches",
			query: `SELECT id FROM push_batches
			         WHERE tenant_id = $1
			         ORDER BY id
			         FOR UPDATE /* tenant purge push batch lock order */`,
		},
		{
			name: "push_effects",
			query: `SELECT id FROM push_effects
			         WHERE tenant_id = $1
			         ORDER BY batch_id,chunk_index,id
			         FOR UPDATE /* tenant purge push effect lock order */`,
		},
		{
			name: "deliveries",
			query: `SELECT id FROM deliveries
			         WHERE tenant_id = $1
			         ORDER BY batch_id,id
			         FOR UPDATE /* tenant purge delivery lock order */`,
		},
		{
			name: "task_observed_events",
			query: `SELECT id FROM task_observed_events
			         WHERE tenant_id = $1
			         ORDER BY delivery_id NULLS FIRST,id
			         FOR UPDATE /* tenant purge observed event lock order */`,
		},
		{
			name: "pending_actions",
			query: `SELECT id FROM pending_actions
			         WHERE tenant_id = $1
			         ORDER BY id
			         FOR UPDATE /* tenant purge task-creation lock order */`,
		},
		{
			name: "agent_action_continuations",
			query: `SELECT action_id FROM agent_action_continuations
			         WHERE tenant_id = $1
			         ORDER BY action_id
			         FOR UPDATE /* tenant purge Agent action lock order */`,
		},
		{
			name: "schedules",
			query: `SELECT id FROM schedules
			         WHERE tenant_id = $1
			         ORDER BY id
			         FOR UPDATE /* tenant purge definition-edit lock order */`,
		},
		{
			name: "schedule_commands",
			query: `SELECT id FROM schedule_commands
			         WHERE tenant_id = $1
			         ORDER BY task_id,id
			         FOR UPDATE /* tenant purge schedule-command lock order */`,
		},
		{
			name: "task_definition_edit_operations",
			query: `SELECT id FROM task_definition_edit_operations
			         WHERE tenant_id = $1
			         ORDER BY id
			         FOR UPDATE /* tenant purge definition-edit lock order */`,
		},
		{
			name: "task_definition_edit_receipts",
			query: `SELECT id FROM task_definition_edit_receipts
			         WHERE tenant_id = $1
			         ORDER BY id
			         FOR UPDATE /* tenant purge definition-edit receipt lock order */`,
		},
		{
			name: "task_creation_receipts",
			query: `SELECT id FROM task_creation_receipts
			         WHERE tenant_id = $1
			         ORDER BY id
			         FOR UPDATE /* tenant purge task-creation receipt lock order */`,
		},
		{
			name: "agent_session_fact_outbox",
			query: `SELECT id FROM agent_session_fact_outbox
			         WHERE tenant_id = $1
			         ORDER BY id
			         FOR UPDATE /* tenant purge continuation fact lock order */`,
		},
		{
			name: "agent_sessions",
			query: `SELECT id FROM agent_sessions
			         WHERE tenant_id = $1
			         ORDER BY id
			         FOR UPDATE /* tenant purge agent-session lock order */`,
		},
	}
	for _, lock := range purgeLocks {
		rows, lockErr := tx.Query(ctx, lock.query, tenantID)
		if lockErr != nil {
			return nil, types.NewAppError(types.CodeDatabase,
				fmt.Sprintf("锁定租户 %d 的 %s", tenantID, lock.name), lockErr)
		}
		for rows.Next() {
		}
		lockErr = rows.Err()
		rows.Close()
		if lockErr != nil {
			return nil, types.NewAppError(types.CodeDatabase,
				fmt.Sprintf("扫描租户 %d 的 %s 锁", tenantID, lock.name), lockErr)
		}
	}

	rep := &PurgeReport{TenantID: tenantID, Rows: map[string]int64{}, DryRun: dryRun}
	for _, st := range purgeOrder {
		// #nosec G201 -- table 与 where 都来自本文件的常量表，不含任何外部输入；
		// tenant_id 走参数化的 $1。
		tag, err := tx.Exec(ctx, fmt.Sprintf("DELETE FROM %s WHERE %s", st.table, st.where), tenantID)
		if err != nil {
			return nil, types.NewAppError(types.CodeDatabase,
				fmt.Sprintf("清理租户 %d 的 %s", tenantID, st.table), err)
		}
		if n := tag.RowsAffected(); n > 0 {
			rep.Rows[st.table] = n
			rep.Total += n
		}
	}

	// 租户行最后删：前面所有表都引用它。
	tag, err := tx.Exec(ctx, `DELETE FROM tenants WHERE id = $1`, tenantID)
	if err != nil {
		return nil, types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("删除租户 %d", tenantID), err)
	}
	if n := tag.RowsAffected(); n > 0 {
		rep.Rows["tenants"] = n
		rep.Total += n
	}

	if dryRun {
		if err := tx.Rollback(ctx); err != nil {
			return nil, types.NewAppError(types.CodeDatabase, "回滚试运行事务", err)
		}
		return rep, nil
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "提交清理事务", err)
	}
	return rep, nil
}
