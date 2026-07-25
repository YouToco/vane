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
	maxCompiledTaskSources          = 64
	maxCompiledTaskSourceURLBytes   = 4096
	maxCompiledTaskSourceTextBytes  = 4096
)

// compiledFetchPlan 是 schedule_playbooks.fetch_plan 在数据边界上的最小稳定形态。
// agent 包拥有编译过程；store 不反向依赖 agent，只消费其持久化 wire contract。
type compiledFetchPlan struct {
	Sources []compiledPlanSource `json:"sources"`
}

type compiledPlanSource struct {
	Platform   string          `json:"platform"`
	Capability string          `json:"capability"`
	Title      string          `json:"title,omitempty"`
	URL        string          `json:"url"`
	Config     json.RawMessage `json:"config,omitempty"`
}

// InsertPausedCompiledTaskDefinition 原子写入一份已编译的稳定监控任务。
//
// Temporal 前置条件：同 TaskID 的 Schedule 已存在，且调用方刚通过 Describe 确认它仍
// paused。A-2 刻意不接 Temporal，也不接受一个可伪造的 paused 布尔值；A-3 将提供创建、
// Describe 与指纹核对原语。这里把 Postgres 镜像状态硬编码为 paused，绝不激活任务。
//
// fetch_plan 是任务源集合的唯一真相源：本方法从计划材料化全局 sources，再写出精确相等的
// schedule_sources 集合并在提交前反向核对。命中既有 URL 只引用原行，绝不覆盖其元数据或
// 抓取状态，也不会创建 subscriptions。TaskID 冲突返回 CodeConflict，且不会采用或改写
// 原聚合；digest/adopt/任务上限及生产接线属于后续 saga。
//
// Commit 返回错误时本方法会用独立短 context 尝试 Rollback 并把错误上抛。若错误来自网络
// 断连，服务端是否已经提交可能不可判定；该“ambiguous commit”只能由 A-3～A-5 的确定性
// TaskID、冲突采用与 reconcile 收敛，A-2 不做无法兑现的跨系统确定性承诺。
func (s *Store) InsertPausedCompiledTaskDefinition(
	ctx context.Context,
	def types.PausedCompiledTaskDefinition,
) error {
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
	// its approved order; schedule_sources is a set.
	materializationSources := append([]compiledPlanSource(nil), plan.Sources...)
	sort.Slice(materializationSources, func(i, j int) bool {
		return materializationSources[i].URL < materializationSources[j].URL
	})
	sourceIDs := make([]int64, 0, len(materializationSources))
	planURLs := make([]string, 0, len(materializationSources))
	for _, planned := range materializationSources {
		sourceID, err := getOrCreateCompiledPlanSource(ctx, tx, planned)
		if err != nil {
			return compiledTaskDatabaseError(
				fmt.Sprintf("材料化计划信源（url=%s）", planned.URL), def.TaskID, err)
		}
		sourceIDs = append(sourceIDs, sourceID)
		planURLs = append(planURLs, planned.URL)
	}

	for _, sourceID := range sourceIDs {
		if _, err := tx.Exec(ctx,
			`INSERT INTO schedule_sources (schedule_id, source_id) VALUES ($1, $2)`,
			def.TaskID, sourceID); err != nil {
			return compiledTaskDatabaseError("写入任务信源链接", def.TaskID, err)
		}
	}

	exact, err := compiledPlanLinksExact(ctx, tx, def.TaskID, planURLs)
	if err != nil {
		return compiledTaskDatabaseError("核对抓取计划与任务信源链接", def.TaskID, err)
	}
	if !exact {
		return types.NewAppError(types.CodeInternal,
			fmt.Sprintf("抓取计划与任务信源链接不一致（task_id=%s）", def.TaskID), nil)
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
	if plan == nil || len(plan.Sources) == 0 {
		return nil, compiledTaskValidationError("fetch_plan.sources 不得缺失、为 null 或为空", nil)
	}
	if len(plan.Sources) > maxCompiledTaskSources {
		return nil, compiledTaskValidationError(
			fmt.Sprintf("fetch_plan.sources 不得超过 %d 个", maxCompiledTaskSources), nil)
	}

	seenURLs := make(map[string]struct{}, len(plan.Sources))
	for i := range plan.Sources {
		src := &plan.Sources[i]
		if len(src.Platform) > maxCompiledTaskSourceTextBytes ||
			len(src.Capability) > maxCompiledTaskSourceTextBytes ||
			len(src.Title) > maxCompiledTaskSourceTextBytes ||
			!utf8.ValidString(src.Platform) || !utf8.ValidString(src.Capability) ||
			!utf8.ValidString(src.Title) || !utf8.ValidString(src.URL) {
			return nil, compiledTaskValidationError(
				fmt.Sprintf("fetch_plan.sources[%d] 文本大小或编码非法", i), nil)
		}
		if strings.TrimSpace(src.Platform) == "" || strings.TrimSpace(src.Platform) != src.Platform {
			return nil, compiledTaskValidationError(
				fmt.Sprintf("fetch_plan.sources[%d].platform 必须是无首尾空白的非空字符串", i), nil)
		}
		if strings.TrimSpace(src.Capability) == "" || strings.TrimSpace(src.Capability) != src.Capability {
			return nil, compiledTaskValidationError(
				fmt.Sprintf("fetch_plan.sources[%d].capability 必须是无首尾空白的非空字符串", i), nil)
		}
		if strings.TrimSpace(src.URL) == "" || strings.TrimSpace(src.URL) != src.URL ||
			len(src.URL) > maxCompiledTaskSourceURLBytes {
			return nil, compiledTaskValidationError(
				fmt.Sprintf("fetch_plan.sources[%d].url 必须是无首尾空白的非空字符串", i), nil)
		}
		if _, duplicate := seenURLs[src.URL]; duplicate {
			return nil, compiledTaskValidationError(
				fmt.Sprintf("fetch_plan 含重复 URL：%s", src.URL), nil)
		}
		seenURLs[src.URL] = struct{}{}

		if len(bytes.TrimSpace(src.Config)) == 0 {
			src.Config = json.RawMessage("{}")
			continue
		}
		if err := validateJSONObject(src.Config,
			fmt.Sprintf("fetch_plan.sources[%d].config", i)); err != nil {
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
		   FROM tenants
		  WHERE id = $1 AND status = 'active' AND deleted_at IS NULL
		  FOR SHARE /* membership tenant root lock order */`,
		tenantID).Scan(&valid)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return compiledTaskValidationError(
				fmt.Sprintf("tenant_id=%d 与 user_id=%d 不构成有效成员关系", tenantID, userID), nil)
		}
		return types.NewAppError(types.CodeDatabase, "锁定并校验租户成员关系", err)
	}
	err = tx.QueryRow(ctx,
		`SELECT true
		   FROM memberships m
		  WHERE m.tenant_id = $1 AND m.user_id = $2
		  FOR SHARE /* membership row lock order */`,
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

func getOrCreateCompiledPlanSource(
	ctx context.Context,
	tx pgx.Tx,
	src compiledPlanSource,
) (int64, error) {
	var id int64
	err := tx.QueryRow(ctx,
		`INSERT INTO sources (platform, capability, url, title, config)
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

	// 同 URL 已存在：只取 id，不更新任何全局源字段或抓取状态。
	if err := tx.QueryRow(ctx, `SELECT id FROM sources WHERE url = $1`, src.URL).Scan(&id); err != nil {
		return 0, err
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
		    (SELECT count(*) FROM schedule_sources WHERE schedule_id = $1)
		      = cardinality($2::text[])
		    AND NOT EXISTS (
		        (SELECT planned_url FROM unnest($2::text[]) AS planned(planned_url))
		        EXCEPT
		        (SELECT s.url
		           FROM schedule_sources ss
		           JOIN sources s ON s.id = ss.source_id
		          WHERE ss.schedule_id = $1)
		    )
		    AND NOT EXISTS (
		        (SELECT s.url
		           FROM schedule_sources ss
		           JOIN sources s ON s.id = ss.source_id
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
