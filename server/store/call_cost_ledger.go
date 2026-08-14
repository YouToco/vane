package store

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"strings"
	"time"

	"github.com/YouToco/vane/types"
)

const (
	CallCostKindLLM  = "llm"
	CallCostKindTool = "tool"
)

// CallCostLedgerQuery is the platform-owner view over the immutable LLM and
// tool call receipts. PageToken binds every filter so a cursor cannot silently
// be replayed against a different query.
type CallCostLedgerQuery struct {
	PageSize      int
	PageToken     string
	Kind          string
	Provider      string
	PricingStatus string
	TaskID        string
}

// CallCostLLMUsage contains only accounting inputs. Prompts, completions and
// provider error text deliberately never cross this admin API boundary.
type CallCostLLMUsage struct {
	PromptTokens          int  `json:"prompt_tokens"`
	PromptCacheHitTokens  *int `json:"prompt_cache_hit_tokens,omitempty"`
	PromptCacheMissTokens *int `json:"prompt_cache_miss_tokens,omitempty"`
	CompletionTokens      int  `json:"completion_tokens"`
	ReasoningTokens       *int `json:"reasoning_tokens,omitempty"`
}

// CallCostToolUsage contains the metered quantity and controlled HTTP/tool
// metadata. Arguments and result bodies are intentionally excluded.
type CallCostToolUsage struct {
	ToolName      string  `json:"tool_name"`
	ToolKind      string  `json:"tool_kind"`
	EndpointPath  string  `json:"endpoint_path,omitempty"`
	UsageQuantity float64 `json:"usage_quantity"`
	HTTPStatus    *int    `json:"http_status,omitempty"`
}

// CallCostLedgerItem is one immutable receipt projected into a unified ledger.
// PricingRule is the exact version used for local calculation; it is nil when
// the provider reported the amount directly or no rule matched.
type CallCostLedgerItem struct {
	Kind          string             `json:"kind"`
	ID            int64              `json:"id"`
	CreatedAt     time.Time          `json:"created_at"`
	Provider      string             `json:"provider"`
	Resource      string             `json:"resource"`
	Meter         string             `json:"meter"`
	PricingStatus string             `json:"pricing_status"`
	CostAmount    *float64           `json:"cost_amount,omitempty"`
	CostCurrency  *string            `json:"cost_currency,omitempty"`
	PricingRule   *ProviderPriceRule `json:"pricing_rule,omitempty"`
	LLMUsage      *CallCostLLMUsage  `json:"llm_usage,omitempty"`
	ToolUsage     *CallCostToolUsage `json:"tool_usage,omitempty"`
	TraceID       string             `json:"trace_id"`
	TaskID        string             `json:"task_id,omitempty"`
	TaskTitle     string             `json:"task_title,omitempty"`
	SpanName      string             `json:"span_name,omitempty"`
	DurationMs    int                `json:"duration_ms"`
	Failed        bool               `json:"failed"`
	ErrorType     string             `json:"error_type,omitempty"`
}

type callCostLedgerCursor struct {
	Version       int       `json:"v"`
	BeforeAt      time.Time `json:"before_at"`
	BeforeKind    string    `json:"before_kind"`
	BeforeID      int64     `json:"before_id"`
	PageSize      int       `json:"page_size"`
	Kind          string    `json:"kind,omitempty"`
	Provider      string    `json:"provider,omitempty"`
	PricingStatus string    `json:"pricing_status,omitempty"`
	TaskID        string    `json:"task_id,omitempty"`
}

func normalizeCallCostLedgerQuery(q CallCostLedgerQuery) (CallCostLedgerQuery, error) {
	if q.PageSize == 0 {
		q.PageSize = 50
	}
	if q.PageSize < 1 || q.PageSize > 100 {
		return q, types.NewAppError(types.CodeValidation, "page_size 必须是 1 到 100 之间的整数", types.ErrValidation)
	}
	q.Kind = strings.ToLower(strings.TrimSpace(q.Kind))
	q.Provider = strings.ToLower(strings.TrimSpace(q.Provider))
	q.PricingStatus = strings.ToLower(strings.TrimSpace(q.PricingStatus))
	q.TaskID = strings.TrimSpace(q.TaskID)
	if q.Kind != "" && q.Kind != CallCostKindLLM && q.Kind != CallCostKindTool {
		return q, types.NewAppError(types.CodeValidation, "kind 仅支持 llm 或 tool", types.ErrValidation)
	}
	switch q.PricingStatus {
	case "", "legacy", "provider_reported", "calculated", "estimated", "unpriced":
	default:
		return q, types.NewAppError(types.CodeValidation, "不支持的 pricing_status", types.ErrValidation)
	}
	if len(q.Provider) > 64 || len(q.TaskID) > 255 {
		return q, types.NewAppError(types.CodeValidation, "调用明细筛选条件过长", types.ErrValidation)
	}
	return q, nil
}

// ListCallCostLedger merges LLM and tool receipts without exposing their
// sensitive payload columns. The three-part keyset is stable even when rows
// from both tables share a timestamp and numeric id.
func (s *Store) ListCallCostLedger(
	ctx context.Context,
	query CallCostLedgerQuery,
) ([]CallCostLedgerItem, string, error) {
	q, err := normalizeCallCostLedgerQuery(query)
	if err != nil {
		return nil, "", err
	}
	var beforeAt *time.Time
	var beforeKind string
	var beforeID int64
	if q.PageToken != "" {
		cursor, err := decodeCallCostLedgerCursor(q.PageToken)
		if err != nil {
			return nil, "", err
		}
		if cursor.PageSize != q.PageSize ||
			cursor.Kind != q.Kind ||
			cursor.Provider != q.Provider ||
			cursor.PricingStatus != q.PricingStatus ||
			cursor.TaskID != q.TaskID {
			return nil, "", types.NewAppError(
				types.CodeValidation,
				"分页游标与当前筛选条件不匹配",
				types.ErrValidation,
			)
		}
		beforeAt = &cursor.BeforeAt
		beforeKind = cursor.BeforeKind
		beforeID = cursor.BeforeID
	}

	rows, err := s.pool.Query(ctx, `
WITH calls AS (
	SELECT
		'llm'::text AS kind,
		lc.id,
		lc.created_at,
		lower(lc.provider) AS provider,
		lc.model AS resource,
		'llm_tokens'::text AS meter,
		lc.pricing_status,
		lc.cost_amount::float8,
		lc.cost_currency,
		lc.pricing_rule_id,
		lc.prompt_tokens,
		lc.prompt_cache_hit_tokens,
		lc.prompt_cache_miss_tokens,
		lc.completion_tokens,
		lc.reasoning_tokens,
		NULL::text AS tool_name,
		NULL::text AS tool_kind,
		NULL::text AS endpoint_path,
		NULL::float8 AS usage_quantity,
		NULL::int AS http_status,
		lc.trace_id,
		COALESCE(pb.schedule_id, '') AS task_id,
		COALESCE(s.nl_description, '') AS task_title,
		lc.span_name,
		lc.latency_ms AS duration_ms,
		(lc.error <> '') AS failed,
		''::text AS error_type
	FROM llm_calls lc
	LEFT JOIN push_batches pb
		ON pb.idempotency_key <> ''
		AND pb.idempotency_key = lc.trace_id
	LEFT JOIN schedules s ON s.id = pb.schedule_id

	UNION ALL

	SELECT
		'tool'::text AS kind,
		tc.id,
		tc.created_at,
		lower(tc.provider) AS provider,
		CASE WHEN tc.endpoint_path <> '' THEN tc.endpoint_path ELSE tc.tool_name END AS resource,
		'request'::text AS meter,
		tc.pricing_status,
		tc.cost_amount::float8,
		tc.cost_currency,
		tc.pricing_rule_id,
		NULL::int AS prompt_tokens,
		NULL::int AS prompt_cache_hit_tokens,
		NULL::int AS prompt_cache_miss_tokens,
		NULL::int AS completion_tokens,
		NULL::int AS reasoning_tokens,
		tc.tool_name,
		tc.tool_kind,
		tc.endpoint_path,
		tc.usage_quantity::float8,
		tc.http_status,
		tc.trace_id,
		COALESCE(run.task_id, '') AS task_id,
		COALESCE(run.task_title, '') AS task_title,
		''::text AS span_name,
		tc.duration_ms,
		(tc.error_type <> '' OR tc.error <> '') AS failed,
		tc.error_type
	FROM tool_calls tc
	LEFT JOIN LATERAL (
		SELECT trs.task_id, COALESCE(s.nl_description, '') AS task_title
		FROM task_run_snapshots trs
		LEFT JOIN schedules s ON s.id = trs.task_id
		WHERE trs.temporal_workflow_id = tc.trace_id
		  AND (tc.tenant_id IS NULL OR trs.tenant_id = tc.tenant_id)
		  AND (tc.user_id IS NULL OR trs.user_id = tc.user_id)
		ORDER BY trs.created_at DESC, trs.id DESC
		LIMIT 1
	) run ON true
)
SELECT
	c.kind, c.id, c.created_at, c.provider, c.resource, c.meter,
	c.pricing_status, c.cost_amount, c.cost_currency, c.pricing_rule_id,
	c.prompt_tokens, c.prompt_cache_hit_tokens, c.prompt_cache_miss_tokens,
	c.completion_tokens, c.reasoning_tokens,
	c.tool_name, c.tool_kind, c.endpoint_path, c.usage_quantity, c.http_status,
	c.trace_id, c.task_id, c.task_title, c.span_name, c.duration_ms,
	c.failed, c.error_type,
	p.id, COALESCE(p.provider, ''), COALESCE(p.resource, ''),
	COALESCE(p.meter, ''), COALESCE(p.currency, ''),
	p.input_cache_hit_per_million::float8,
	p.input_cache_miss_per_million::float8,
	p.output_per_million::float8,
	p.request_unit_price::float8,
	p.request_included_quantity::float8,
	p.request_additional_unit_price::float8,
	p.effective_from, p.effective_to, COALESCE(p.source_url, ''),
	COALESCE(p.note, ''), p.created_by, p.created_at
FROM calls c
LEFT JOIN provider_price_rules p ON p.id = c.pricing_rule_id
WHERE ($1 = '' OR c.kind = $1)
  AND ($2 = '' OR c.provider = $2)
  AND ($3 = '' OR c.pricing_status = $3)
  AND ($4 = '' OR c.task_id = $4)
  AND ($5::timestamptz IS NULL OR (c.created_at, c.kind, c.id) < ($5, $6, $7))
ORDER BY c.created_at DESC, c.kind DESC, c.id DESC
LIMIT $8
`,
		q.Kind, q.Provider, q.PricingStatus, q.TaskID,
		beforeAt, beforeKind, beforeID, q.PageSize+1,
	)
	if err != nil {
		return nil, "", types.NewAppError(types.CodeDatabase, "查询逐笔调用账单", err)
	}
	defer rows.Close()

	items := make([]CallCostLedgerItem, 0, q.PageSize+1)
	for rows.Next() {
		var item CallCostLedgerItem
		var pricingRuleID *int64
		var promptTokens, completionTokens *int
		var cacheHitTokens, cacheMissTokens, reasoningTokens *int
		var toolName, toolKind, endpointPath *string
		var usageQuantity *float64
		var httpStatus *int
		var rule ProviderPriceRule
		var ruleID *int64
		var ruleEffectiveFrom, ruleCreatedAt *time.Time
		if err := rows.Scan(
			&item.Kind, &item.ID, &item.CreatedAt, &item.Provider, &item.Resource,
			&item.Meter, &item.PricingStatus, &item.CostAmount, &item.CostCurrency,
			&pricingRuleID,
			&promptTokens, &cacheHitTokens, &cacheMissTokens,
			&completionTokens, &reasoningTokens,
			&toolName, &toolKind, &endpointPath, &usageQuantity, &httpStatus,
			&item.TraceID, &item.TaskID, &item.TaskTitle, &item.SpanName,
			&item.DurationMs, &item.Failed, &item.ErrorType,
			&ruleID, &rule.Provider, &rule.Resource, &rule.Meter, &rule.Currency,
			&rule.InputCacheHitPerMillion, &rule.InputCacheMissPerMillion,
			&rule.OutputPerMillion, &rule.RequestUnitPrice,
			&rule.RequestIncludedQuantity, &rule.RequestAdditionalUnitPrice,
			&ruleEffectiveFrom, &rule.EffectiveTo, &rule.SourceURL, &rule.Note,
			&rule.CreatedBy, &ruleCreatedAt,
		); err != nil {
			return nil, "", types.NewAppError(types.CodeDatabase, "扫描逐笔调用账单", err)
		}
		if pricingRuleID != nil && ruleID != nil {
			rule.ID = *ruleID
			if ruleEffectiveFrom != nil {
				rule.EffectiveFrom = ruleEffectiveFrom.UTC()
			}
			if ruleCreatedAt != nil {
				rule.CreatedAt = ruleCreatedAt.UTC()
			}
			item.PricingRule = &rule
		}
		switch item.Kind {
		case CallCostKindLLM:
			item.LLMUsage = &CallCostLLMUsage{
				PromptTokens:          valueOrZero(promptTokens),
				PromptCacheHitTokens:  cacheHitTokens,
				PromptCacheMissTokens: cacheMissTokens,
				CompletionTokens:      valueOrZero(completionTokens),
				ReasoningTokens:       reasoningTokens,
			}
		case CallCostKindTool:
			item.ToolUsage = &CallCostToolUsage{
				ToolName:      stringOrEmpty(toolName),
				ToolKind:      stringOrEmpty(toolKind),
				EndpointPath:  stringOrEmpty(endpointPath),
				UsageQuantity: floatOrZero(usageQuantity),
				HTTPStatus:    httpStatus,
			}
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, "", types.NewAppError(types.CodeDatabase, "遍历逐笔调用账单", err)
	}

	var next string
	if len(items) > q.PageSize {
		items = items[:q.PageSize]
		last := items[len(items)-1]
		next, err = encodeCallCostLedgerCursor(callCostLedgerCursor{
			Version: 1, BeforeAt: last.CreatedAt.UTC(), BeforeKind: last.Kind,
			BeforeID: last.ID, PageSize: q.PageSize, Kind: q.Kind,
			Provider: q.Provider, PricingStatus: q.PricingStatus, TaskID: q.TaskID,
		})
		if err != nil {
			return nil, "", err
		}
	}
	return items, next, nil
}

func encodeCallCostLedgerCursor(cursor callCostLedgerCursor) (string, error) {
	raw, err := json.Marshal(cursor)
	if err != nil {
		return "", types.NewAppError(types.CodeInternal, "生成逐笔调用账单游标", err)
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

func decodeCallCostLedgerCursor(token string) (callCostLedgerCursor, error) {
	var cursor callCostLedgerCursor
	if len(token) > 4096 {
		return cursor, types.NewAppError(types.CodeValidation, "无效的分页游标", types.ErrValidation)
	}
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return cursor, types.NewAppError(types.CodeValidation, "无效的分页游标", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&cursor); err != nil {
		return cursor, types.NewAppError(types.CodeValidation, "无效的分页游标", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return cursor, types.NewAppError(types.CodeValidation, "无效的分页游标", types.ErrValidation)
	}
	if cursor.Version != 1 || cursor.BeforeAt.IsZero() ||
		(cursor.BeforeKind != CallCostKindLLM && cursor.BeforeKind != CallCostKindTool) ||
		cursor.BeforeID <= 0 {
		return cursor, types.NewAppError(types.CodeValidation, "无效的分页游标", types.ErrValidation)
	}
	normalized, err := normalizeCallCostLedgerQuery(CallCostLedgerQuery{
		PageSize: cursor.PageSize, Kind: cursor.Kind, Provider: cursor.Provider,
		PricingStatus: cursor.PricingStatus, TaskID: cursor.TaskID,
	})
	if err != nil ||
		normalized.Kind != cursor.Kind ||
		normalized.Provider != cursor.Provider ||
		normalized.PricingStatus != cursor.PricingStatus ||
		normalized.TaskID != cursor.TaskID {
		return cursor, types.NewAppError(types.CodeValidation, "无效的分页游标", types.ErrValidation)
	}
	cursor.BeforeAt = cursor.BeforeAt.UTC()
	return cursor, nil
}

func valueOrZero(value *int) int {
	if value == nil {
		return 0
	}
	return *value
}

func stringOrEmpty(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func floatOrZero(value *float64) float64 {
	if value == nil {
		return 0
	}
	return *value
}
