package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/internal/strictjson"
	"github.com/YouToco/vane/types"
)

const (
	compiledTaskRollbackTimeout     = 5 * time.Second
	maxCompiledTaskJSONBytes        = 256 << 10
	maxCompiledTaskPlaybookBytes    = 256 << 10
	maxCompiledTaskDescriptionBytes = 16 << 10
	maxCompiledTaskTargets          = 64
	maxCompiledTaskTargetURLBytes   = 4096
	maxCompiledTaskTargetTextBytes  = 4096
)

// compiledFetchPlan 是 schedule_playbooks.fetch_plan 在数据边界上的最小稳定形态。
// agent 包拥有编译过程；store 不反向依赖 agent，只消费其持久化 wire contract。
type compiledFetchPlan struct {
	Targets []compiledPlanTarget `json:"targets"`
}

type compiledPlanTarget struct {
	Platform   string          `json:"platform"`
	Capability string          `json:"capability"`
	Title      string          `json:"title,omitempty"`
	URL        string          `json:"url"`
	Config     json.RawMessage `json:"config,omitempty"`
	ToolName   string          `json:"tool_name,omitempty"`
	ToolArgs   json.RawMessage `json:"tool_arguments,omitempty"`
}

// InsertPausedCompiledTaskDefinition 原子写入一份已编译的稳定监控任务。
//
// Temporal 前置条件：同 TaskID 的 Schedule 已存在，且调用方刚通过 Describe 确认它仍
// paused。A-2 刻意不接 Temporal，也不接受一个可伪造的 paused 布尔值；A-3 将提供创建、
// Describe 与指纹核对原语。这里把 Postgres 镜像状态硬编码为 paused，绝不激活任务。
//
// fetch_plan 是任务抓取目标集合的唯一真相源：本方法从计划材料化全局 fetch_targets，再写出精确相等的
// task_fetch_targets 集合并在提交前反向核对。命中既有 URL 只引用原行，绝不覆盖其元数据或
// 抓取状态。TaskID 冲突返回 CodeConflict，且不会采用或改写
// 原聚合；digest/adopt/任务上限及生产接线属于后续 saga。
//
// Commit 返回错误时本方法会用独立短 context 尝试 Rollback 并把错误上抛。若错误来自网络
// 断连，服务端是否已经提交可能不可判定；该“ambiguous commit”只能由 A-3～A-5 的确定性
// TaskID、冲突采用与 reconcile 收敛，A-2 不做无法兑现的跨系统确定性承诺。
func (s *Store) InsertPausedCompiledTaskDefinition(
	ctx context.Context,
	def types.PausedCompiledTaskDefinition,
) error {
	if s.legacyAdmissionIsClosed() {
		return legacyAdmissionClosed("compiled task definition v1")
	}
	plan, err := validatePausedCompiledTaskDefinition(def)
	if err != nil {
		return err
	}

	tx, err := s.beginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("开始写入编译任务事务（task_id=%s）", def.TaskID), err)
	}
	defer rollbackCompiledTaskTx(ctx, tx)

	if err := lockValidMembership(ctx, tx, def.TenantID, def.UserID); err != nil {
		return err
	}
	if err := insertPausedCompiledTaskDefinitionTx(ctx, tx, def, plan); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return compiledTaskDatabaseError("提交编译任务事务", def.TaskID, err)
	}
	return nil
}

// insertPausedCompiledTaskDefinitionTx is the A2 aggregate writer without
// transaction ownership. A2 calls it after a membership lock; A5 calls it
// while holding the exact creation lease plus the stronger per-user lock, so
// the aggregate and the saga phase can commit atomically.
func insertPausedCompiledTaskDefinitionTx(
	ctx context.Context,
	tx pgx.Tx,
	def types.PausedCompiledTaskDefinition,
	plan *compiledFetchPlan,
) error {

	if _, err := tx.Exec(ctx,
		`INSERT INTO schedules
			(id, tenant_id, user_id, nl_description, spec_json, scope_json, status,
			 push_strictness, execution_mode)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		def.TaskID, def.TenantID, def.UserID, def.NLDescription,
		[]byte(def.SpecJSON), []byte(def.ScopeJSON),
		types.ScheduleStatusPaused, nullableStrictness(def.Strictness),
		types.ExecutionModeCompiled); err != nil {
		if isUniqueViolation(err) {
			return types.NewAppError(types.CodeConflict,
				fmt.Sprintf("任务 id=%s 已存在", def.TaskID), err)
		}
		return compiledTaskDatabaseError("写入 paused 调度镜像", def.TaskID, err)
	}

	if _, err := tx.Exec(ctx,
		`INSERT INTO schedule_playbooks (schedule_id, content, fetch_plan)
		 VALUES ($1, $2, $3)`,
		def.TaskID, def.PlaybookContent, []byte(def.FetchPlan)); err != nil {
		return compiledTaskDatabaseError("写入任务手册与抓取计划", def.TaskID, err)
	}

	// Global source URLs are unique across every tenant. Different users can
	// commit inverse plan orders concurrently, so always acquire those unique
	// index/row locks in canonical URL order. The stored fetch_plan itself keeps
	// its approved order; task_fetch_targets is a set.
	materializationTargets := append([]compiledPlanTarget(nil), plan.Targets...)
	sort.Slice(materializationTargets, func(i, j int) bool {
		return materializationTargets[i].URL < materializationTargets[j].URL
	})
	targetIDs := make([]int64, 0, len(materializationTargets))
	planURLs := make([]string, 0, len(materializationTargets))
	for _, planned := range materializationTargets {
		targetID, err := getOrCreateCompiledPlanTarget(ctx, tx, planned)
		if err != nil {
			if errors.Is(err, types.ErrConflict) {
				return err
			}
			return compiledTaskDatabaseError(
				fmt.Sprintf("材料化计划抓取目标（url=%s）", planned.URL), def.TaskID, err)
		}
		targetIDs = append(targetIDs, targetID)
		planURLs = append(planURLs, planned.URL)
	}

	for _, targetID := range targetIDs {
		if _, err := tx.Exec(ctx,
			`INSERT INTO task_fetch_targets (schedule_id, fetch_target_id) VALUES ($1, $2)`,
			def.TaskID, targetID); err != nil {
			return compiledTaskDatabaseError("写入任务抓取目标链接", def.TaskID, err)
		}
	}

	exact, err := compiledPlanLinksExact(ctx, tx, def.TaskID, planURLs)
	if err != nil {
		return compiledTaskDatabaseError("核对抓取计划与任务抓取目标链接", def.TaskID, err)
	}
	if !exact {
		return types.NewAppError(types.CodeInternal,
			fmt.Sprintf("抓取计划与任务抓取目标链接不一致（task_id=%s）", def.TaskID), nil)
	}
	return nil
}

func validatePausedCompiledTaskDefinition(
	def types.PausedCompiledTaskDefinition,
) (*compiledFetchPlan, error) {
	if strings.TrimSpace(def.TaskID) == "" || strings.TrimSpace(def.TaskID) != def.TaskID ||
		len(def.TaskID) > 255 || !utf8.ValidString(def.TaskID) {
		return nil, compiledTaskValidationError("task_id 必须是无首尾空白的非空字符串", nil)
	}
	if def.TenantID <= 0 {
		return nil, compiledTaskValidationError("tenant_id 必须为正整数", nil)
	}
	if def.UserID <= 0 {
		return nil, compiledTaskValidationError("user_id 必须为正整数", nil)
	}
	if len(def.NLDescription) > maxCompiledTaskDescriptionBytes ||
		!utf8.ValidString(def.NLDescription) {
		return nil, compiledTaskValidationError("nl_description 大小或编码非法", nil)
	}
	if len(def.PlaybookContent) > maxCompiledTaskPlaybookBytes ||
		!utf8.ValidString(def.PlaybookContent) {
		return nil, compiledTaskValidationError("playbook_content 大小或编码非法", nil)
	}
	if err := validateJSONObject(def.SpecJSON, "spec_json"); err != nil {
		return nil, err
	}
	if err := validateJSONObject(def.ScopeJSON, "scope_json"); err != nil {
		return nil, err
	}
	if def.Strictness != "" && !def.Strictness.Valid() {
		return nil, compiledTaskValidationError(
			"push_strictness 必须为空（未设置）或 loose、normal、strict", nil)
	}

	raw := bytes.TrimSpace(def.FetchPlan)
	if len(raw) == 0 || len(raw) > maxCompiledTaskJSONBytes ||
		bytes.Equal(raw, []byte("null")) {
		return nil, compiledTaskValidationError("fetch_plan 不得缺失或为 null", nil)
	}
	var plan *compiledFetchPlan
	if err := strictjson.Decode(raw, &plan); err != nil {
		return nil, compiledTaskValidationError("fetch_plan 必须是合法 JSON 对象", err)
	}
	if plan == nil || len(plan.Targets) == 0 {
		return nil, compiledTaskValidationError("fetch_plan.targets 不得缺失、为 null 或为空", nil)
	}
	if len(plan.Targets) > maxCompiledTaskTargets {
		return nil, compiledTaskValidationError(
			fmt.Sprintf("fetch_plan.targets 不得超过 %d 个", maxCompiledTaskTargets), nil)
	}

	seenURLs := make(map[string]struct{}, len(plan.Targets))
	for i := range plan.Targets {
		target := &plan.Targets[i]
		if len(target.Platform) > maxCompiledTaskTargetTextBytes ||
			len(target.Capability) > maxCompiledTaskTargetTextBytes ||
			len(target.Title) > maxCompiledTaskTargetTextBytes ||
			!utf8.ValidString(target.Platform) || !utf8.ValidString(target.Capability) ||
			!utf8.ValidString(target.Title) || !utf8.ValidString(target.URL) {
			return nil, compiledTaskValidationError(
				fmt.Sprintf("fetch_plan.targets[%d] 文本大小或编码非法", i), nil)
		}
		if strings.TrimSpace(target.Platform) == "" || strings.TrimSpace(target.Platform) != target.Platform {
			return nil, compiledTaskValidationError(
				fmt.Sprintf("fetch_plan.targets[%d].platform 必须是无首尾空白的非空字符串", i), nil)
		}
		if strings.TrimSpace(target.Capability) == "" || strings.TrimSpace(target.Capability) != target.Capability {
			return nil, compiledTaskValidationError(
				fmt.Sprintf("fetch_plan.targets[%d].capability 必须是无首尾空白的非空字符串", i), nil)
		}
		if strings.TrimSpace(target.URL) == "" || strings.TrimSpace(target.URL) != target.URL ||
			len(target.URL) > maxCompiledTaskTargetURLBytes {
			return nil, compiledTaskValidationError(
				fmt.Sprintf("fetch_plan.targets[%d].url 必须是无首尾空白的非空字符串", i), nil)
		}
		if (target.ToolName == "") != (len(bytes.TrimSpace(target.ToolArgs)) == 0) {
			return nil, compiledTaskValidationError(
				fmt.Sprintf(
					"fetch_plan.targets[%d] 的 tool_name 与 tool_arguments 必须同时存在或同时缺失",
					i,
				), nil,
			)
		}
		if target.ToolName != "" {
			if strings.TrimSpace(target.ToolName) != target.ToolName ||
				len(target.ToolName) > maxCompiledTaskTargetTextBytes ||
				!utf8.ValidString(target.ToolName) {
				return nil, compiledTaskValidationError(
					fmt.Sprintf("fetch_plan.targets[%d].tool_name 非法", i), nil,
				)
			}
			if err := validateJSONObject(target.ToolArgs,
				fmt.Sprintf("fetch_plan.targets[%d].tool_arguments", i)); err != nil {
				return nil, err
			}
		}
		if _, duplicate := seenURLs[target.URL]; duplicate {
			return nil, compiledTaskValidationError(
				fmt.Sprintf("fetch_plan 含重复 URL：%s", target.URL), nil)
		}
		seenURLs[target.URL] = struct{}{}

		if len(bytes.TrimSpace(target.Config)) == 0 {
			target.Config = json.RawMessage("{}")
			continue
		}
		if err := validateJSONObject(target.Config,
			fmt.Sprintf("fetch_plan.targets[%d].config", i)); err != nil {
			return nil, err
		}
	}
	return plan, nil
}

func validateJSONObject(raw json.RawMessage, field string) error {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || len(raw) > maxCompiledTaskJSONBytes ||
		bytes.Equal(raw, []byte("null")) {
		return compiledTaskValidationError(field+" 必须是非 null JSON 对象", nil)
	}
	var object map[string]json.RawMessage
	if err := strictjson.Decode(raw, &object); err != nil {
		return compiledTaskValidationError(field+" 必须是合法 JSON 对象", err)
	}
	if object == nil {
		return compiledTaskValidationError(field+" 必须是非 null JSON 对象", nil)
	}
	return nil
}

func lockValidMembership(ctx context.Context, tx pgx.Tx, tenantID, userID int64) error {
	var valid bool
	err := tx.QueryRow(ctx,
		`SELECT true
		   FROM memberships m
		   JOIN tenants t ON t.id = m.tenant_id
		  WHERE m.tenant_id = $1
		    AND m.user_id = $2
		    AND t.status = 'active'
		    AND t.deleted_at IS NULL
		  FOR SHARE OF m, t`,
		tenantID, userID).Scan(&valid)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return compiledTaskValidationError(
				fmt.Sprintf("tenant_id=%d 与 user_id=%d 不构成有效成员关系", tenantID, userID), nil)
		}
		return types.NewAppError(types.CodeDatabase, "锁定并校验租户成员关系", err)
	}
	if !valid {
		return compiledTaskValidationError(
			fmt.Sprintf("tenant_id=%d 与 user_id=%d 不构成有效成员关系", tenantID, userID), nil)
	}
	return nil
}

func getOrCreateCompiledPlanTarget(
	ctx context.Context,
	tx pgx.Tx,
	src compiledPlanTarget,
) (int64, error) {
	var id int64
	err := tx.QueryRow(ctx,
		`INSERT INTO fetch_targets (platform, capability, url, title, config)
		 VALUES ($1, $2, $3, $4, $5)
		 ON CONFLICT (url) DO NOTHING
		 RETURNING id`,
		src.Platform, src.Capability, src.URL, src.Title, []byte(src.Config)).Scan(&id)
	if err == nil {
		return id, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return 0, err
	}

	// URL is the global dedup key. Reuse is allowed only when acquisition
	// semantics match; title is display-only and is deliberately excluded.
	var platform, capability string
	var config json.RawMessage
	if err := tx.QueryRow(ctx,
		`SELECT id, platform, capability, config
		   FROM fetch_targets WHERE url = $1
		   FOR KEY SHARE`,
		src.URL,
	).Scan(&id, &platform, &capability, &config); err != nil {
		return 0, err
	}
	if platform != src.Platform || capability != src.Capability ||
		!taskCreationJSONEqual(config, src.Config) {
		return 0, types.NewAppError(types.CodeConflict,
			fmt.Sprintf("同一 URL 已存在不同抓取语义（url=%s）", src.URL), nil)
	}
	return id, nil
}

func compiledPlanLinksExact(
	ctx context.Context,
	tx pgx.Tx,
	taskID string,
	planURLs []string,
) (bool, error) {
	var exact bool
	err := tx.QueryRow(ctx,
		`SELECT
		    (SELECT count(*) FROM task_fetch_targets WHERE schedule_id = $1)
		      = cardinality($2::text[])
		    AND NOT EXISTS (
		        (SELECT planned_url FROM unnest($2::text[]) AS planned(planned_url))
		        EXCEPT
		        (SELECT s.url
		           FROM task_fetch_targets ss
		           JOIN fetch_targets s ON s.id = ss.fetch_target_id
		          WHERE ss.schedule_id = $1)
		    )
		    AND NOT EXISTS (
		        (SELECT s.url
		           FROM task_fetch_targets ss
		           JOIN fetch_targets s ON s.id = ss.fetch_target_id
		          WHERE ss.schedule_id = $1)
		        EXCEPT
		        (SELECT planned_url FROM unnest($2::text[]) AS planned(planned_url))
		    )`,
		taskID, planURLs).Scan(&exact)
	return exact, err
}

func rollbackCompiledTaskTx(parent context.Context, tx pgx.Tx) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), compiledTaskRollbackTimeout)
	defer cancel()
	_ = tx.Rollback(ctx)
}

func nullableStrictness(strictness types.PushStrictness) *string {
	if strictness == "" {
		return nil
	}
	value := string(strictness)
	return &value
}

func compiledTaskValidationError(message string, cause error) error {
	return types.NewAppError(types.CodeValidation, message, cause)
}

func compiledTaskDatabaseError(action, taskID string, cause error) error {
	return types.NewAppError(types.CodeDatabase,
		fmt.Sprintf("%s（task_id=%s）", action, taskID), cause)
}
