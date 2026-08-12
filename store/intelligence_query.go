package store

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/YouToco/vane/types"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const (
	// V1/V2 remain historical result markers in sealed AgentToolEvidence. V3
	// unifies retained V1 artifacts with native Research V3 evidence and Briefs.
	IntelligenceCatalogVersion = "vane.intelligence-catalog/v3"
	maxIntelligenceRows        = 100
	maxIntelligenceBytes       = 64 * 1024
	intelligenceQueryBudget    = 2 * time.Second
)

type IntelligenceDataset string

const (
	IntelligenceTasks        IntelligenceDataset = "tasks"
	IntelligenceRuns         IntelligenceDataset = "runs"
	IntelligenceObservations IntelligenceDataset = "observations"
	IntelligenceBriefs       IntelligenceDataset = "briefs"
	IntelligenceAgentTurns   IntelligenceDataset = "agent_turns"
	IntelligenceToolCalls    IntelligenceDataset = "tool_calls"
	IntelligenceProfile      IntelligenceDataset = "profile"
	IntelligenceFeedbacks    IntelligenceDataset = "feedbacks"
)

// IntelligenceScope is injected from authenticated runtime state. It is not
// part of the model-visible query schema.
type IntelligenceScope struct {
	TenantID  int64
	UserID    int64
	SessionID *int64
	TaskID    string // scheduled-run callers are fenced to this exact task
}

type IntelligenceQuery struct {
	Dataset IntelligenceDataset  `json:"dataset"`
	Select  []string             `json:"select,omitempty"`
	Filters []IntelligenceFilter `json:"filters,omitempty"`
	GroupBy []string             `json:"group_by,omitempty"`
	Metrics []IntelligenceMetric `json:"metrics,omitempty"`
	OrderBy []IntelligenceOrder  `json:"order_by,omitempty"`
	Limit   int                  `json:"limit,omitempty"`
	Cursor  string               `json:"cursor,omitempty"`
}

type IntelligenceFilter struct {
	Field string          `json:"field"`
	Op    string          `json:"op"`
	Value json.RawMessage `json:"value"`
}

type IntelligenceMetric struct {
	Function string `json:"function"`
	Field    string `json:"field,omitempty"`
	As       string `json:"as"`
}

type IntelligenceOrder struct {
	Field     string `json:"field"`
	Direction string `json:"direction,omitempty"`
}

type IntelligenceColumn struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type IntelligenceCoverage struct {
	Status string `json:"status"`
	Note   string `json:"note,omitempty"`
}

type IntelligenceQueryResult struct {
	CatalogVersion string               `json:"catalog_version"`
	Dataset        IntelligenceDataset  `json:"dataset"`
	Columns        []IntelligenceColumn `json:"columns"`
	Rows           []map[string]any     `json:"rows"`
	Coverage       IntelligenceCoverage `json:"coverage"`
	Truncated      bool                 `json:"truncated"`
	NextCursor     string               `json:"next_cursor,omitempty"`
}

type intelligenceColumnSpec struct {
	name string
	typ  string
}

type intelligenceDatasetSpec struct {
	base             string
	columns          map[string]intelligenceColumnSpec
	filterEnums      map[string][]string
	defaults         []string
	defaultOrder     []IntelligenceOrder
	stableOrder      map[string]bool
	coverage         IntelligenceCoverage
	relativeTimeZone bool
}

var intelligenceCatalog = map[IntelligenceDataset]intelligenceDatasetSpec{
	IntelligenceTasks: {
		base: `SELECT s.tenant_id,s.user_id,s.id AS record_id,s.id AS task_ref,
		              s.nl_description AS task_name,COALESCE(p.content,'') AS playbook,
		              s.status,s.spec_json AS schedule,s.execution_mode,
		              s.created_at,s.updated_at
		         FROM schedules s
		         LEFT JOIN schedule_playbooks p ON p.schedule_id=s.id`,
		columns: intelligenceColumns(
			"record_id:text", "task_ref:text", "task_name:text", "playbook:text",
			"status:text", "schedule:json", "execution_mode:text",
			"created_at:time", "updated_at:time"),
		defaults:     []string{"task_ref", "task_name", "playbook", "status", "schedule", "updated_at"},
		defaultOrder: []IntelligenceOrder{{Field: "created_at", Direction: "desc"}, {Field: "record_id", Direction: "desc"}},
		stableOrder:  intelligenceStableOrder("created_at", "record_id"),
		coverage: IntelligenceCoverage{
			Status: "complete",
			Note:   "仅完整覆盖当前任务定义；不覆盖任务运行、Brief 或 Observation，不能据此判断时间窗内有无新情报。",
		},
		relativeTimeZone: true,
	},
	IntelligenceRuns: {
		base: `SELECT rs.tenant_id,rs.user_id,rs.id::text AS record_id,
		              rs.task_id AS task_ref,rs.id::text AS run_snapshot_id,
		              rs.temporal_workflow_id,rs.temporal_run_id,rs.run_kind,
		              rs.execution_mode,
		              CASE WHEN rs.reference_schema_version='vane.research-run-snapshot-ref/v3'
		                   THEN 'research_v3' ELSE 'legacy' END AS runtime_generation,
		              CASE WHEN rs.reference_schema_version='vane.research-run-snapshot-ref/v3'
		                   THEN COALESCE(CASE WHEN rb.status IN ('prepared','spending') THEN 'pending'
		                                           ELSE rb.status END,'unavailable')
		                   ELSE COALESCE(o.status,'unavailable') END AS outcome_status,
		              CASE WHEN rs.reference_schema_version='vane.research-run-snapshot-ref/v3'
		                   THEN CASE WHEN rb.status='finalized' AND rb.decision='deliver' THEN 'content'
		                                  WHEN rb.status='finalized' AND rb.decision='quiet' THEN 'quiet'
		                                  WHEN rb.status='failed' THEN 'failed'
		                                  WHEN rb.status='ambiguous' THEN 'interrupted' END
		                   ELSE o.result END AS result,
		              CASE WHEN rs.reference_schema_version='vane.research-run-snapshot-ref/v3'
		                   THEN 'unavailable' ELSE o.source_coverage END AS source_coverage,
		              CASE WHEN rs.reference_schema_version='vane.research-run-snapshot-ref/v3'
		                   THEN 'unavailable' ELSE o.processing END AS processing,
		              CASE WHEN rs.reference_schema_version='vane.research-run-snapshot-ref/v3'
		                   THEN COALESCE(rb.failure_code,'') ELSE COALESCE(o.failure_code,'') END AS failure_code,
		              CASE WHEN rs.reference_schema_version='vane.research-run-snapshot-ref/v3'
		                   THEN '' ELSE COALESCE(o.failure_message,'') END AS failure_message,
		              CASE WHEN rs.reference_schema_version='vane.research-run-snapshot-ref/v3'
		                   THEN rb.finalized_at ELSE o.finalized_at END AS finalized_at,rs.created_at
		         FROM task_run_snapshots rs
		         LEFT JOIN task_run_outcomes o
		           ON o.tenant_id=rs.tenant_id AND o.user_id=rs.user_id
		          AND o.task_id=rs.task_id AND o.run_snapshot_id=rs.id
		         LEFT JOIN research_brief_syntheses rb
		           ON rb.tenant_id=rs.tenant_id AND rb.user_id=rs.user_id
		          AND rb.task_id=rs.task_id AND rb.run_snapshot_id=rs.id`,
		columns: intelligenceColumns(
			"record_id:text", "task_ref:text", "run_snapshot_id:text",
			"temporal_workflow_id:text", "temporal_run_id:text", "run_kind:text",
			"execution_mode:text", "runtime_generation:text", "outcome_status:text", "result:text",
			"source_coverage:text", "processing:text", "failure_code:text",
			"failure_message:text", "finalized_at:time", "created_at:time"),
		filterEnums: map[string][]string{
			"outcome_status": {"pending", "finalized", "ambiguous", "failed", "unavailable"},
			"result":         {"content", "quiet", "failed", "interrupted"},
		},
		defaults:     []string{"task_ref", "run_snapshot_id", "run_kind", "runtime_generation", "outcome_status", "result", "source_coverage", "processing", "finalized_at", "created_at"},
		defaultOrder: []IntelligenceOrder{{Field: "created_at", Direction: "desc"}, {Field: "record_id", Direction: "desc"}},
		stableOrder:  intelligenceStableOrder("created_at", "record_id"),
		coverage: IntelligenceCoverage{
			Status: "mixed",
			Note:   "运行快照自 migration 030 完整；061 前的运行没有 RunOutcome，逐行标记 outcome_status=unavailable。outcome_status 只表示运行记录状态，可取 pending/finalized/ambiguous/failed/unavailable，不存在 success；finalized 只表示已结算，是否产出情报必须同时读取 result=content/quiet/failed/interrupted。比较最近一次与上一次运行时不要先按 outcome_status 筛选，应按 created_at 倒序读取至少两行，并明确任何被跳过的失败或缺失运行。",
		},
		relativeTimeZone: true,
	},
	IntelligenceObservations: {
		base: `SELECT p.tenant_id,p.user_id,
		              ('legacy:'||lpad(p.run_snapshot_id::text,20,'0')||':'||
		               p.invocation_digest||':'||lpad(chunk_index::text,6,'0')) AS record_id,
		              'legacy_observation_v1'::text AS lineage,
		              p.task_id AS task_ref,p.run_snapshot_id::text AS run_snapshot_id,
		              p.invocation_digest AS invocation_ref,NULL::text AS tool_name,
		              substring(payload_text FROM chunk_index*8192+1 FOR 8192) AS model_visible_result,
		              p.observation_digest AS result_digest,
		              octet_length(p.observation_payload) AS stored_size,NULL::integer AS original_size,
		              NULL::boolean AS source_truncated,
		              CASE WHEN char_length(payload_text)>8192
		                   THEN 'window' ELSE 'full' END::text AS payload_coverage,
		              'legacy_exact'::text AS evidence_coverage,
		              'legacy_external'::text AS trust_type,
		              chunk_index*8192 AS payload_offset,
		              char_length(payload_text) AS payload_total_chars,
		              (chunk_index*8192+8192>=char_length(payload_text)) AS payload_complete,
		              cardinality(p.content_item_ids) AS content_count,p.created_at
		         FROM task_run_content_provenance p
		        CROSS JOIN LATERAL (
		              SELECT convert_from(p.observation_payload,'UTF8') AS payload_text
		        ) payload
		        CROSS JOIN LATERAL generate_series(
		              0,GREATEST((char_length(payload_text)-1)/8192,0)
		        ) chunk_index
		        WHERE NOT EXISTS (
		              SELECT 1 FROM research_run_plans rp_legacy
		               WHERE rp_legacy.tenant_id=p.tenant_id
		                 AND rp_legacy.user_id=p.user_id
		                 AND rp_legacy.task_id=p.task_id
		                 AND rp_legacy.run_snapshot_id=p.run_snapshot_id
		        )
		        UNION ALL
		       SELECT e.tenant_id,e.user_id,
		              ('v3:'||lpad(e.id::text,20,'0')||':'||lpad(chunk_index::text,6,'0')) AS record_id,
		              'research_tool_evidence_v3'::text AS lineage,
		              e.task_id AS task_ref,rp.run_snapshot_id::text AS run_snapshot_id,
		              e.invocation_id AS invocation_ref,e.tool_name,
		              substring(payload_text FROM chunk_index*8192+1 FOR 8192) AS model_visible_result,
		              e.result_digest,octet_length(e.result_bytes) AS stored_size,e.original_size,
		              e.truncated AS source_truncated,
		              CASE WHEN char_length(payload_text)>8192
		                   THEN 'window' ELSE 'full' END::text AS payload_coverage,
		              'exact'::text AS evidence_coverage,e.trust_type,
		              chunk_index*8192 AS payload_offset,
		              char_length(payload_text) AS payload_total_chars,
		              (chunk_index*8192+8192>=char_length(payload_text)) AS payload_complete,
		              NULL::integer AS content_count,e.created_at
		         FROM research_run_evidence e
		         JOIN research_run_plans rp
		           ON rp.id=e.plan_id AND rp.tenant_id=e.tenant_id
		          AND rp.user_id=e.user_id AND rp.task_id=e.task_id
		          AND rp.temporal_run_id=e.temporal_run_id
		          AND rp.plan_digest=e.plan_digest
		        CROSS JOIN LATERAL (
		              SELECT convert_from(e.result_bytes,'UTF8') AS payload_text
		        ) payload
		        CROSS JOIN LATERAL generate_series(
		              0,GREATEST((char_length(payload_text)-1)/8192,0)
		        ) chunk_index`,
		columns: intelligenceColumns(
			"record_id:text", "lineage:text", "task_ref:text", "run_snapshot_id:text",
			"invocation_ref:text", "tool_name:text", "model_visible_result:text",
			"result_digest:text", "stored_size:integer", "original_size:integer",
			"source_truncated:boolean", "payload_coverage:text", "evidence_coverage:text",
			"trust_type:text", "payload_offset:integer", "payload_total_chars:integer",
			"payload_complete:boolean", "content_count:integer", "created_at:time"),
		defaults:     []string{"lineage", "task_ref", "run_snapshot_id", "invocation_ref", "tool_name", "model_visible_result", "result_digest", "stored_size", "original_size", "source_truncated", "payload_coverage", "evidence_coverage", "trust_type", "payload_offset", "payload_total_chars", "payload_complete", "content_count", "created_at"},
		defaultOrder: []IntelligenceOrder{{Field: "created_at", Direction: "desc"}, {Field: "record_id", Direction: "asc"}},
		stableOrder:  intelligenceStableOrder("created_at", "record_id"),
		coverage:     IntelligenceCoverage{Status: "mixed", Note: "076 前缺少 legacy exact Observation；V3 自 087 起保存 exact model-visible Evidence。大结果按 8192 字符不可变窗口分页，逐页 payload_offset/total/complete 明确，不猜测或丢弃尾部；source_truncated 单独表示上游模型当时是否已截断。"}, relativeTimeZone: true,
	},
	IntelligenceBriefs: {
		base: `SELECT b.tenant_id,b.user_id,
		              ('legacy:'||lpad(b.id::text,20,'0')||':'||lpad(chunk_index::text,6,'0')) AS record_id,
		              'legacy_brief_v1'::text AS lineage,
		              b.task_id AS task_ref,b.run_snapshot_id::text AS run_snapshot_id,
		              b.run_outcome_id::text AS run_outcome_id,
		              CASE WHEN char_length(payload_text)<=8192 THEN payload_text::jsonb END AS brief,
		              substring(payload_text FROM chunk_index*8192+1 FOR 8192) AS brief_preview,
		              b.payload_digest AS brief_digest,'finalized'::text AS status,
		              NULL::text AS significance,NULL::text AS decision,
		              NULL::boolean AS delivery_required,''::text AS failure_code,
		              NULL::text AS delivery_status,NULL::timestamptz AS sent_at,
		              'legacy_exact'::text AS truth_coverage,
		              CASE WHEN char_length(payload_text)>8192
		                   THEN 'window' ELSE 'full' END::text AS payload_coverage,
		              chunk_index*8192 AS payload_offset,
		              char_length(payload_text) AS payload_total_chars,
		              octet_length(b.payload) AS payload_total_bytes,
		              (chunk_index*8192+8192>=char_length(payload_text)) AS payload_complete,
		              b.insight_count,b.generated_at,b.created_at
		         FROM brief_snapshots b
		        CROSS JOIN LATERAL (
		              SELECT convert_from(b.payload,'UTF8') AS payload_text
		        ) payload
		        CROSS JOIN LATERAL generate_series(
		              0,GREATEST((char_length(payload_text)-1)/8192,0)
		        ) chunk_index
		        WHERE NOT EXISTS (
		              SELECT 1 FROM research_brief_syntheses rb_legacy
		               WHERE rb_legacy.tenant_id=b.tenant_id
		                 AND rb_legacy.user_id=b.user_id
		                 AND rb_legacy.task_id=b.task_id
		                 AND rb_legacy.run_snapshot_id=b.run_snapshot_id
		        )
		        UNION ALL
		       SELECT rb.tenant_id,rb.user_id,
		              ('v3:'||lpad(rb.id::text,20,'0')||':'||lpad(chunk_index::text,6,'0')) AS record_id,
		              'research_brief_v3'::text AS lineage,
		              rb.task_id AS task_ref,rb.run_snapshot_id::text AS run_snapshot_id,
		              NULL::text AS run_outcome_id,
		              CASE WHEN rb.status='finalized' AND char_length(payload_text)<=8192
		                   THEN payload_text::jsonb END AS brief,
		              CASE WHEN rb.status='finalized' THEN
		                   substring(payload_text FROM chunk_index*8192+1 FOR 8192) END AS brief_preview,
		              rb.brief_digest,rb.status,rb.significance,rb.decision,
		              rb.delivery_required,rb.failure_code,
		              COALESCE(rd.status,
		                CASE WHEN rb.delivery_required=false THEN 'not_required'
		                     ELSE 'unavailable' END) AS delivery_status,
		              rd.sent_at,
		              CASE WHEN rb.status='finalized' THEN 'exact'
		                   ELSE 'unavailable' END::text AS truth_coverage,
		              CASE WHEN rb.status<>'finalized' THEN 'unavailable'
		                   WHEN char_length(payload_text)>8192
		                     THEN 'window' ELSE 'full' END::text AS payload_coverage,
		              chunk_index*8192 AS payload_offset,
		              char_length(COALESCE(payload_text,'')) AS payload_total_chars,
		              CASE WHEN rb.status='finalized' THEN octet_length(rb.brief_payload)
		                   ELSE 0 END AS payload_total_bytes,
		              (chunk_index*8192+8192>=char_length(COALESCE(payload_text,''))) AS payload_complete,
		              NULL::integer AS insight_count,rb.finalized_at AS generated_at,rb.created_at
		         FROM research_brief_syntheses rb
		         LEFT JOIN research_brief_deliveries rd
		           ON rd.brief_id=rb.id AND rd.tenant_id=rb.tenant_id
		          AND rd.user_id=rb.user_id AND rd.task_id=rb.task_id
		          AND rd.run_snapshot_id=rb.run_snapshot_id AND rd.plan_id=rb.plan_id
		        CROSS JOIN LATERAL (
		              SELECT CASE WHEN rb.status='finalized'
		                          THEN convert_from(rb.brief_payload,'UTF8') END AS payload_text
		        ) payload
		        CROSS JOIN LATERAL generate_series(
		              0,GREATEST((char_length(COALESCE(payload_text,''))-1)/8192,0)
		        ) chunk_index
		        WHERE rb.status IN ('finalized','ambiguous','failed')`,
		columns: intelligenceColumns(
			"record_id:text", "lineage:text", "task_ref:text", "run_snapshot_id:text",
			"run_outcome_id:text", "brief:json", "brief_preview:text", "brief_digest:text",
			"status:text", "significance:text", "decision:text", "delivery_required:boolean",
			"failure_code:text", "delivery_status:text", "sent_at:time",
			"truth_coverage:text", "payload_coverage:text", "payload_offset:integer",
			"payload_total_chars:integer", "payload_total_bytes:integer",
			"payload_complete:boolean", "insight_count:integer",
			"generated_at:time", "created_at:time"),
		defaults:     []string{"lineage", "task_ref", "run_snapshot_id", "brief_preview", "brief_digest", "status", "significance", "decision", "delivery_required", "failure_code", "delivery_status", "truth_coverage", "payload_coverage", "payload_offset", "payload_total_chars", "payload_total_bytes", "payload_complete", "generated_at", "created_at"},
		defaultOrder: []IntelligenceOrder{{Field: "generated_at", Direction: "desc"}, {Field: "record_id", Direction: "asc"}},
		stableOrder:  intelligenceStableOrder("generated_at", "record_id"),
		coverage:     IntelligenceCoverage{Status: "mixed", Note: "061 前没有不可变 legacy Brief；V3 finalized 为 exact，ambiguous/failed 只保留 unavailable 失败事实，不猜测结论。大 Brief 按 8192 字符不可变窗口分页，尾部可由同一工具 next_cursor 续读。"}, relativeTimeZone: true,
	},
	IntelligenceAgentTurns: {
		base: `SELECT t.tenant_id,t.user_id,t.id::text AS record_id,t.session_id::text AS session_id,
		              t.turn_id,t.trace_id,t.user_message,t.assistant_message,
		              t.tool_invocation_ids,t.action_receipts,t.created_at
		         FROM agent_turn_records t`,
		columns: intelligenceColumns(
			"record_id:text", "session_id:text", "turn_id:text", "trace_id:text",
			"user_message:text", "assistant_message:text", "tool_invocation_ids:array",
			"action_receipts:json", "created_at:time"),
		defaults:         []string{"turn_id", "user_message", "assistant_message", "tool_invocation_ids", "action_receipts", "created_at"},
		defaultOrder:     []IntelligenceOrder{{Field: "created_at", Direction: "desc"}, {Field: "record_id", Direction: "desc"}},
		stableOrder:      intelligenceStableOrder("created_at", "record_id"),
		coverage:         IntelligenceCoverage{Status: "partial", Note: "AgentTurnRecordV1 上线前的旧交互无法可靠关联回复与证据，标记 unavailable，不猜测回填。"},
		relativeTimeZone: true,
	},
	IntelligenceToolCalls: {
		base: `SELECT e.tenant_id,e.user_id,('exact:'||e.id::text) AS record_id,
		              e.session_id::text AS session_id,e.trace_id,e.invocation_id,e.tool_name,e.arguments,
		              convert_from(e.result_bytes,'UTF8') AS model_visible_result,
		              e.original_size AS result_size,e.truncated,e.trust_type,
		              'exact'::text AS evidence_coverage,e.created_at
		         FROM agent_tool_evidence e
		        UNION ALL
		       SELECT c.tenant_id,c.user_id,('legacy:'||c.id::text) AS record_id,
		              c.session_id::text AS session_id,c.trace_id,('legacy-'||c.id::text) AS invocation_id,
		              c.tool_name,COALESCE(c.arguments,'{}'::jsonb),c.result_preview,
		              c.result_size,(c.result_size>octet_length(c.result_preview)),
		              CASE WHEN c.tool_name IN (
		                       'list_schedules','create_schedule','remove_schedule',
		                       'run_task_now','view_profile','update_profile',
		                       'view_task_playbook','view_task_latest_run',
		                       'edit_task_definition','search_endpoints','tool_search'
		                   ) THEN 'local' ELSE 'external' END,
		              'legacy_preview'::text,c.created_at
		         FROM tool_calls c
		        WHERE c.tenant_id IS NOT NULL AND c.user_id IS NOT NULL
		          AND NOT EXISTS (
		              SELECT 1 FROM agent_tool_evidence e
		               WHERE e.tool_call_id=c.id AND e.tenant_id=c.tenant_id
		                 AND e.user_id=c.user_id
		          )`,
		columns: intelligenceColumns(
			"record_id:text", "session_id:text", "trace_id:text", "invocation_id:text",
			"tool_name:text", "arguments:json", "model_visible_result:text",
			"result_size:integer", "truncated:boolean", "trust_type:text",
			"evidence_coverage:text", "created_at:time"),
		defaults:         []string{"trace_id", "invocation_id", "tool_name", "arguments", "model_visible_result", "truncated", "trust_type", "evidence_coverage", "created_at"},
		defaultOrder:     []IntelligenceOrder{{Field: "created_at", Direction: "desc"}, {Field: "record_id", Direction: "desc"}},
		stableOrder:      intelligenceStableOrder("created_at", "record_id"),
		coverage:         IntelligenceCoverage{Status: "mixed", Note: "新调用为 exact；仅有旧 8 KiB 预览的调用明确标记 legacy_preview。"},
		relativeTimeZone: true,
	},
	IntelligenceProfile: {
		base: `SELECT p.tenant_id,p.user_id,p.id::text AS record_id,p.industry,
		              p.occupation,p.tags,p.removed_tags,p.summary,
		              p.token_budget_daily,p.tokens_used_today,p.updated_at,p.created_at
		         FROM profiles p`,
		columns: intelligenceColumns(
			"record_id:text", "industry:text", "occupation:text", "tags:array",
			"removed_tags:array", "summary:text", "token_budget_daily:integer",
			"tokens_used_today:integer", "updated_at:time", "created_at:time"),
		defaults:         []string{"industry", "occupation", "tags", "summary", "updated_at"},
		defaultOrder:     []IntelligenceOrder{{Field: "updated_at", Direction: "desc"}},
		stableOrder:      intelligenceStableOrder("record_id"),
		coverage:         IntelligenceCoverage{Status: "current", Note: "画像数据集呈现当前来源化画像；历史画像变化由 Agent 证据解释。"},
		relativeTimeZone: true,
	},
	IntelligenceFeedbacks: {
		base: `SELECT f.tenant_id,f.user_id,f.id::text AS record_id,
		              s.id AS task_ref,rs.id::text AS run_snapshot_id,
		              COALESCE(left(d.body_md,2000),'') AS delivered_summary,f.action,
		              COALESCE(f.reason_code,'') AS reason_code,f.detail,
		              CASE
		                WHEN f.action NOT IN ('interested','not_interested')
		                  THEN NULL::boolean
		                WHEN f.profile_epoch IS DISTINCT FROM
		                  CASE WHEN p.user_id IS NULL THEN 0
		                       ELSE COALESCE(pcs.active_epoch,-1) END
		                  THEN false
		                ELSE NOT EXISTS (
		                  SELECT 1
		                    FROM feedbacks newer
		                   WHERE newer.tenant_id=f.tenant_id
		                     AND newer.user_id=f.user_id
		                     AND newer.delivery_id=f.delivery_id
		                     AND newer.profile_epoch=f.profile_epoch
		                     AND (
		                       (
		                         newer.action IN ('interested','not_interested')
		                         AND (newer.created_at,newer.id)>(f.created_at,f.id)
		                       )
		                       OR (
		                         f.action='not_interested'
		                         AND btrim(f.detail)=''
		                         AND newer.action='misjudged'
		                         AND newer.reason_code IS NOT NULL
		                         AND newer.id>f.id
		                       )
		                     )
		                )
		              END AS is_effective_attitude,
		              f.created_at
		         FROM feedbacks f
		         JOIN deliveries d
		           ON d.id=f.delivery_id AND d.tenant_id=f.tenant_id
		          AND d.user_id=f.user_id
		         JOIN push_batches b
		           ON b.id=d.batch_id AND b.tenant_id=d.tenant_id
		          AND b.user_id=d.user_id
		         LEFT JOIN schedules s
		           ON s.id=b.schedule_id AND s.tenant_id=b.tenant_id
		          AND s.user_id=b.user_id
		         LEFT JOIN task_run_snapshots rs
		           ON rs.id=b.run_snapshot_id AND rs.tenant_id=b.tenant_id
		          AND rs.user_id=b.user_id AND rs.task_id=s.id
		         LEFT JOIN profiles p
		           ON p.tenant_id=f.tenant_id AND p.user_id=f.user_id
		         LEFT JOIN profile_claim_states pcs
		           ON pcs.tenant_id=f.tenant_id AND pcs.user_id=f.user_id`,
		columns: intelligenceColumns(
			"record_id:text", "task_ref:text", "run_snapshot_id:text", "delivered_summary:text",
			"action:text", "reason_code:text", "detail:text",
			"is_effective_attitude:boolean", "created_at:time"),
		defaults: []string{
			"task_ref", "run_snapshot_id", "delivered_summary", "action", "reason_code",
			"detail", "is_effective_attitude", "created_at",
		},
		defaultOrder: []IntelligenceOrder{{Field: "created_at", Direction: "desc"}, {Field: "record_id", Direction: "desc"}},
		stableOrder:  intelligenceStableOrder("created_at", "record_id"),
		coverage: IntelligenceCoverage{
			Status: "mixed",
			Note:   "反馈事件完整保留；旧 push-now 投递可能没有 task_ref/run_snapshot_id，定时任务身份只读取绑定到当前任务的反馈。",
		},
		relativeTimeZone: true,
	},
}

// Migration/replay tests intentionally run the current Store against retained
// pre-115 schemas. These fixed projections are the last catalog-v2 shapes for
// the three datasets whose v3 SQL names native Research tables. Production
// reaches the v3 map only after migration 115 is durably applied; no arbitrary
// relation name or model input participates in this selection.
var intelligenceCatalogPreV3 = map[IntelligenceDataset]intelligenceDatasetSpec{
	IntelligenceRuns: {
		base: `SELECT rs.tenant_id,rs.user_id,rs.id::text AS record_id,
		              rs.task_id AS task_ref,rs.id::text AS run_snapshot_id,
		              rs.temporal_workflow_id,rs.temporal_run_id,rs.run_kind,
		              rs.execution_mode,COALESCE(o.status,'unavailable') AS outcome_status,
		              o.result,o.source_coverage,o.processing,o.failure_code,
		              o.failure_message,o.finalized_at,rs.created_at
		         FROM task_run_snapshots rs
		         LEFT JOIN task_run_outcomes o
		           ON o.tenant_id=rs.tenant_id AND o.user_id=rs.user_id
		          AND o.run_snapshot_id=rs.id`,
		columns: intelligenceColumns(
			"record_id:text", "task_ref:text", "run_snapshot_id:text",
			"temporal_workflow_id:text", "temporal_run_id:text", "run_kind:text",
			"execution_mode:text", "outcome_status:text", "result:text",
			"source_coverage:text", "processing:text", "failure_code:text",
			"failure_message:text", "finalized_at:time", "created_at:time"),
		filterEnums: map[string][]string{
			"outcome_status": {"pending", "finalized", "unavailable"},
			"result":         {"content", "quiet", "failed", "interrupted"},
		},
		defaults:     []string{"task_ref", "run_snapshot_id", "run_kind", "outcome_status", "result", "source_coverage", "processing", "finalized_at", "created_at"},
		defaultOrder: []IntelligenceOrder{{Field: "created_at", Direction: "desc"}, {Field: "record_id", Direction: "desc"}},
		stableOrder:  intelligenceStableOrder("created_at", "record_id"),
		coverage: IntelligenceCoverage{
			Status: "mixed",
			Note:   "运行快照自 migration 030 完整；061 前的运行没有 RunOutcome，逐行标记 outcome_status=unavailable。outcome_status 只表示运行记录状态，可取 pending/finalized/unavailable，不存在 success；finalized 只表示已结算，是否产出情报必须同时读取 result=content/quiet/failed/interrupted。比较最近一次与上一次运行时不要先按 outcome_status 筛选，应按 created_at 倒序读取至少两行。",
		},
		relativeTimeZone: true,
	},
	IntelligenceObservations: {
		base: `SELECT p.tenant_id,p.user_id,
		              (p.run_snapshot_id::text||':'||p.invocation_digest) AS record_id,
		              p.task_id AS task_ref,p.run_snapshot_id::text AS run_snapshot_id,p.invocation_digest,
		              convert_from(p.observation_payload,'UTF8')::jsonb AS observation,
		              p.observation_digest,cardinality(p.content_item_ids) AS content_count,
		              p.created_at
		         FROM task_run_content_provenance p`,
		columns: intelligenceColumns(
			"record_id:text", "task_ref:text", "run_snapshot_id:text",
			"invocation_digest:text", "observation:json", "observation_digest:text",
			"content_count:integer", "created_at:time"),
		defaults:     []string{"task_ref", "run_snapshot_id", "observation", "content_count", "created_at"},
		defaultOrder: []IntelligenceOrder{{Field: "created_at", Direction: "desc"}, {Field: "record_id", Direction: "desc"}},
		stableOrder:  intelligenceStableOrder("created_at", "record_id"),
		coverage:     IntelligenceCoverage{Status: "partial", Note: "076 前没有 exact Observation provenance；缺失历史保持 unavailable，不猜测回填。"}, relativeTimeZone: true,
	},
	IntelligenceBriefs: {
		base: `SELECT b.tenant_id,b.user_id,b.id::text AS record_id,
		              b.task_id AS task_ref,b.run_snapshot_id::text AS run_snapshot_id,
		              b.run_outcome_id::text AS run_outcome_id,
		              convert_from(b.payload,'UTF8')::jsonb AS brief,
		              b.insight_count,b.generated_at,b.created_at
		         FROM brief_snapshots b`,
		columns: intelligenceColumns(
			"record_id:text", "task_ref:text", "run_snapshot_id:text",
			"run_outcome_id:text", "brief:json", "insight_count:integer",
			"generated_at:time", "created_at:time"),
		defaults:     []string{"task_ref", "run_snapshot_id", "brief", "insight_count", "generated_at"},
		defaultOrder: []IntelligenceOrder{{Field: "generated_at", Direction: "desc"}, {Field: "record_id", Direction: "desc"}},
		stableOrder:  intelligenceStableOrder("generated_at", "record_id"),
		coverage:     IntelligenceCoverage{Status: "partial", Note: "061 前没有不可变 BriefV1 snapshot；缺失历史保持 unavailable，不猜测回填。"}, relativeTimeZone: true,
	},
}

func intelligenceColumns(items ...string) map[string]intelligenceColumnSpec {
	out := make(map[string]intelligenceColumnSpec, len(items))
	for _, item := range items {
		parts := strings.SplitN(item, ":", 2)
		out[parts[0]] = intelligenceColumnSpec{name: parts[0], typ: parts[1]}
	}
	return out
}

func intelligenceStableOrder(fields ...string) map[string]bool {
	out := make(map[string]bool, len(fields))
	for _, field := range fields {
		out[field] = true
	}
	return out
}

// QueryMyIntelligence executes one relation query over one fixed semantic
// dataset. No SQL, identity, table or expression is accepted from the model.
func (s *Store) QueryMyIntelligence(
	ctx context.Context,
	scope IntelligenceScope,
	query IntelligenceQuery,
) (_ *IntelligenceQueryResult, retErr error) {
	started := time.Now()
	status := "failed"
	rowCount := 0
	truncated := false
	denialAudited := false
	digest, summary := intelligenceQueryAuditMaterial(query)
	defer func() {
		if retErr != nil {
			if errors.Is(retErr, context.DeadlineExceeded) || isStatementTimeout(retErr) {
				status = "timeout"
			} else if errors.Is(retErr, types.ErrValidation) {
				status = "rejected"
			}
		}
		if scope.TenantID > 0 && scope.UserID > 0 && !denialAudited {
			auditErr := s.insertIntelligenceQueryAudit(
				context.WithoutCancel(ctx), scope, query.Dataset, digest, summary,
				status, rowCount, time.Since(started), truncated,
			)
			if auditErr != nil && retErr == nil {
				retErr = types.NewAppError(
					types.CodeDatabase, "提交用户情报查询审计", auditErr)
			}
		}
	}()

	if scope.TenantID <= 0 || scope.UserID <= 0 {
		return nil, types.NewAppError(types.CodeValidation,
			"情报查询认证范围无效", types.ErrValidation)
	}
	queryBytes, _ := json.Marshal(query)
	if len(queryBytes) > 32768 {
		return nil, types.NewAppError(types.CodeValidation,
			"情报查询请求超过 32 KiB", types.ErrValidation)
	}
	spec, ok := intelligenceCatalog[query.Dataset]
	if !ok {
		return nil, types.NewAppError(types.CodeValidation,
			"未知情报数据集", types.ErrValidation)
	}
	queryCtx, cancel := context.WithTimeout(ctx, intelligenceQueryBudget)
	defer cancel()
	if err := s.ensureIntelligenceCursorKeys(queryCtx); err != nil {
		return nil, types.NewAppError(types.CodeDatabase,
			"加载用户情报游标签名材料", err)
	}
	tx, err := s.beginTx(queryCtx, pgx.TxOptions{
		IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "开启用户情报查询事务", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(queryCtx)) }()
	if _, err := tx.Exec(queryCtx,
		`SET LOCAL search_path = pg_catalog, public, pg_temp`); err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "固定用户情报查询路径", err)
	}
	if _, err := tx.Exec(queryCtx,
		`SELECT set_config('app.tenant_id',$1,true),
		        set_config('app.user_id',$2,true),
		        set_config('statement_timeout','2000',true)`,
		strconv.FormatInt(scope.TenantID, 10), strconv.FormatInt(scope.UserID, 10)); err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "设置用户情报查询范围", err)
	}
	var membershipExists bool
	if err := tx.QueryRow(queryCtx,
		`SELECT EXISTS (
		     SELECT 1 FROM memberships WHERE tenant_id=$1 AND user_id=$2
		 )`, scope.TenantID, scope.UserID).Scan(&membershipExists); err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "验证用户情报查询成员身份", err)
	}
	if !membershipExists {
		// Release the semantic-read connection before opening the owner-only
		// denial ledger transaction. Holding one connection per rejected request
		// while each waits for a second connection can starve a full pool.
		if err := tx.Rollback(queryCtx); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
			return nil, types.NewAppError(types.CodeDatabase,
				"释放越权用户情报查询事务", err)
		}
		if err := s.insertIntelligenceAccessDenial(
			context.WithoutCancel(ctx), scope, query.Dataset, digest,
			"membership_mismatch",
		); err != nil {
			return nil, types.NewAppError(types.CodeDatabase,
				"提交用户情报越权拒绝审计", err)
		}
		denialAudited = true
		return nil, types.NewAppError(types.CodeValidation,
			"情报查询认证身份与租户不匹配", types.ErrValidation)
	}
	var catalogV3Applied bool
	// Detect the deployed semantic capability, not owner-only migration state.
	// vane_server_runtime intentionally cannot read goose_db_version; PostgreSQL
	// privilege introspection is public, fixed, and matches the exact column
	// grant installed atomically by migration 115.
	if err := tx.QueryRow(queryCtx, `SELECT has_column_privilege(
		'vane_intelligence_reader','public.task_run_snapshots',
		'reference_schema_version','SELECT'
	)`).Scan(&catalogV3Applied); err != nil {
		return nil, types.NewAppError(types.CodeDatabase,
			"检测用户情报目录能力", err)
	}
	if !catalogV3Applied {
		if historical, exists := intelligenceCatalogPreV3[query.Dataset]; exists {
			spec = historical
		}
	}
	if _, err := tx.Exec(queryCtx, `SET LOCAL ROLE vane_intelligence_reader`); err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "进入用户情报只读角色", err)
	}
	compiled, err := s.compileIntelligenceQuery(queryCtx, tx, scope, query, spec)
	if err != nil {
		return nil, err
	}
	rows, err := tx.Query(queryCtx, compiled.sql, compiled.args...)
	if err != nil {
		return nil, mapIntelligenceQueryError(err)
	}
	defer rows.Close()

	result := &IntelligenceQueryResult{
		CatalogVersion: IntelligenceCatalogVersion,
		Dataset:        query.Dataset,
		Columns:        compiled.columns,
		Rows:           make([]map[string]any, 0, compiled.limit),
		Coverage:       spec.coverage,
	}
	rowCursors := make([][]json.RawMessage, 0, compiled.limit)
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return nil, types.NewAppError(types.CodeDatabase, "扫描用户情报查询结果", err)
		}
		if len(result.Rows) == compiled.limit {
			truncated = true
			break
		}
		var item map[string]any
		decoder := json.NewDecoder(strings.NewReader(string(raw)))
		decoder.UseNumber()
		if err := decoder.Decode(&item); err != nil {
			return nil, types.NewAppError(types.CodeInternal, "解码用户情报查询结果", err)
		}
		cursorValues, err := extractIntelligenceCursorValues(item, compiled.order)
		if err != nil {
			return nil, err
		}
		result.Rows = append(result.Rows, item)
		rowCursors = append(rowCursors, cursorValues)
		encoded, _ := json.Marshal(result)
		if len(encoded) > maxIntelligenceBytes {
			result.Rows = result.Rows[:len(result.Rows)-1]
			rowCursors = rowCursors[:len(rowCursors)-1]
			if len(result.Rows) == 0 {
				return nil, types.NewAppError(types.CodeValidation,
					"情报查询单行超过 64 KiB，请减少 select 字段", types.ErrValidation)
			}
			truncated = true
			break
		}
	}
	if err := rows.Err(); err != nil {
		return nil, mapIntelligenceQueryError(err)
	}
	rows.Close()
	if truncated && !compiled.paginationStable {
		return nil, types.NewAppError(types.CodeValidation,
			"该排序字段会随任务编辑而变化；分页请改用不可变时间与记录引用排序", types.ErrValidation)
	}
	if err := tx.Commit(queryCtx); err != nil {
		return nil, mapIntelligenceQueryError(err)
	}
	rowCount = len(result.Rows)
	result.Truncated = truncated
	if truncated {
		result.NextCursor = s.signIntelligenceCursor(
			scope, compiled.queryDigest, rowCursors[len(rowCursors)-1], compiled.asOf,
		)
	}
	// Account for cursor/metadata bytes too. A pathological column list can
	// only reduce rows; it can never exceed the public 64 KiB contract.
	for {
		encoded, _ := json.Marshal(result)
		if len(encoded) <= maxIntelligenceBytes || len(result.Rows) == 0 {
			break
		}
		if !compiled.paginationStable {
			return nil, types.NewAppError(types.CodeValidation,
				"该排序字段会随任务编辑而变化；分页请改用不可变时间与记录引用排序", types.ErrValidation)
		}
		result.Rows = result.Rows[:len(result.Rows)-1]
		rowCursors = rowCursors[:len(rowCursors)-1]
		rowCount = len(result.Rows)
		if len(result.Rows) == 0 {
			return nil, types.NewAppError(types.CodeValidation,
				"情报查询单行与分页元数据超过 64 KiB，请减少 select 字段", types.ErrValidation)
		}
		result.Truncated = true
		truncated = true
		result.NextCursor = s.signIntelligenceCursor(
			scope, compiled.queryDigest, rowCursors[len(rowCursors)-1], compiled.asOf,
		)
	}
	status = "completed"
	return result, nil
}

func extractIntelligenceCursorValues(
	row map[string]any,
	order []IntelligenceOrder,
) ([]json.RawMessage, error) {
	values := make([]json.RawMessage, len(order))
	for i := range order {
		key := fmt.Sprintf("__cursor_%d", i)
		value, ok := row[key]
		delete(row, key)
		if !ok || value == nil {
			return nil, types.NewAppError(types.CodeInternal,
				"用户情报分页键缺失", nil)
		}
		raw, err := json.Marshal(value)
		if err != nil || len(raw) == 0 || len(raw) > 4096 {
			return nil, types.NewAppError(types.CodeInternal,
				"编码用户情报分页键", err)
		}
		values[i] = raw
	}
	return values, nil
}

type compiledIntelligenceQuery struct {
	sql              string
	args             []any
	columns          []IntelligenceColumn
	limit            int
	order            []IntelligenceOrder
	queryDigest      string
	asOf             time.Time
	paginationStable bool
}

func (s *Store) compileIntelligenceQuery(
	ctx context.Context,
	querier interface {
		Query(context.Context, string, ...any) (pgx.Rows, error)
	},
	scope IntelligenceScope,
	query IntelligenceQuery,
	spec intelligenceDatasetSpec,
) (*compiledIntelligenceQuery, error) {
	limit := query.Limit
	if limit == 0 {
		limit = 20
	}
	if limit < 1 || limit > maxIntelligenceRows {
		return nil, types.NewAppError(types.CodeValidation,
			"情报查询 limit 必须在 1–100", types.ErrValidation)
	}
	queryDigest := intelligenceQueryDigest(query, false)
	var cursorAfter []json.RawMessage
	asOf := time.Now().UTC()
	if query.Cursor != "" {
		var err error
		cursorAfter, asOf, err = s.verifyIntelligenceCursor(ctx, scope, queryDigest, query.Cursor)
		if err != nil {
			return nil, err
		}
	}

	selected := append([]string(nil), query.Select...)
	if len(selected) == 0 {
		selected = append(selected, spec.defaults...)
	}
	if len(selected) > 32 || len(query.Filters) > 16 || len(query.GroupBy) > 8 ||
		len(query.Metrics) > 8 || len(query.OrderBy) > 8 {
		return nil, types.NewAppError(types.CodeValidation,
			"情报查询字段数量超过上限", types.ErrValidation)
	}
	if len(query.GroupBy) > 0 || len(query.Metrics) > 0 {
		if len(query.Select) > 0 {
			return nil, types.NewAppError(types.CodeValidation,
				"聚合查询请使用 group_by 与 metrics，不同时提交 select", types.ErrValidation)
		}
		selected = append([]string(nil), query.GroupBy...)
	}
	columns := make([]IntelligenceColumn, 0, len(selected)+len(query.Metrics))
	selectSQL := make([]string, 0, len(selected)+len(query.Metrics))
	seenOutput := map[string]struct{}{}
	for _, field := range selected {
		col, exists := spec.columns[field]
		if !exists {
			return nil, invalidIntelligenceField(field)
		}
		if _, duplicate := seenOutput[field]; duplicate {
			return nil, invalidIntelligenceField(field)
		}
		seenOutput[field] = struct{}{}
		selectSQL = append(selectSQL, quoteIntelligenceIdent(field))
		columns = append(columns, IntelligenceColumn{Name: col.name, Type: col.typ})
	}
	metricSQL := make([]string, 0, len(query.Metrics))
	for _, metric := range query.Metrics {
		if !validIntelligenceIdent(metric.As) {
			return nil, types.NewAppError(types.CodeValidation,
				"情报查询 metric.as 无效", types.ErrValidation)
		}
		if _, duplicate := seenOutput[metric.As]; duplicate {
			return nil, invalidIntelligenceField(metric.As)
		}
		fn := strings.ToLower(metric.Function)
		if fn != "count" && fn != "sum" && fn != "avg" && fn != "min" && fn != "max" {
			return nil, types.NewAppError(types.CodeValidation,
				"情报查询 metric.function 无效", types.ErrValidation)
		}
		fieldSQL := "*"
		typ := "number"
		if metric.Field != "" {
			col, exists := spec.columns[metric.Field]
			if !exists {
				return nil, invalidIntelligenceField(metric.Field)
			}
			if (fn == "sum" || fn == "avg") && col.typ != "number" && col.typ != "integer" {
				return nil, types.NewAppError(types.CodeValidation,
					"sum/avg 只接受数值字段", types.ErrValidation)
			}
			fieldSQL = quoteIntelligenceIdent(metric.Field)
			if fn == "min" || fn == "max" {
				typ = col.typ
			}
		} else if fn != "count" {
			return nil, types.NewAppError(types.CodeValidation,
				"仅 count metric 可省略 field", types.ErrValidation)
		}
		metricSQL = append(metricSQL,
			fmt.Sprintf("%s(%s) AS %s", strings.ToUpper(fn), fieldSQL, quoteIntelligenceIdent(metric.As)))
		columns = append(columns, IntelligenceColumn{Name: metric.As, Type: typ})
		seenOutput[metric.As] = struct{}{}
	}
	selectSQL = append(selectSQL, metricSQL...)
	if len(selectSQL) == 0 {
		return nil, types.NewAppError(types.CodeValidation,
			"情报查询没有输出字段", types.ErrValidation)
	}

	args := []any{scope.TenantID, scope.UserID}
	where := []string{"tenant_id=$1", "user_id=$2"}
	if scope.TaskID != "" && query.Dataset != IntelligenceProfile {
		if _, hasTask := spec.columns["task_ref"]; !hasTask {
			return nil, types.NewAppError(types.CodeValidation,
				"定时任务身份无权读取该数据集", types.ErrValidation)
		}
		args = append(args, scope.TaskID)
		where = append(where, fmt.Sprintf("task_ref=$%d", len(args)))
	}
	if _, hasCreatedAt := spec.columns["created_at"]; hasCreatedAt {
		args = append(args, asOf)
		where = append(where, fmt.Sprintf("created_at <= $%d", len(args)))
	}
	location, err := resolveIntelligenceLocation(ctx, querier, scope, query, spec)
	if err != nil {
		return nil, err
	}
	for _, filter := range query.Filters {
		clause, values, err := compileIntelligenceFilter(filter, spec, location, asOf, len(args)+1)
		if err != nil {
			return nil, err
		}
		where = append(where, clause)
		args = append(args, values...)
	}

	groupSQL := ""
	if len(query.GroupBy) > 0 {
		parts := make([]string, 0, len(query.GroupBy))
		for _, field := range query.GroupBy {
			if _, exists := spec.columns[field]; !exists {
				return nil, invalidIntelligenceField(field)
			}
			parts = append(parts, quoteIntelligenceIdent(field))
		}
		groupSQL = " GROUP BY " + strings.Join(parts, ",")
	}
	order := append([]IntelligenceOrder(nil), query.OrderBy...)
	if len(order) == 0 {
		if len(query.GroupBy) > 0 || len(query.Metrics) > 0 {
			for _, field := range query.GroupBy {
				order = append(order, IntelligenceOrder{Field: field, Direction: "asc"})
			}
		} else {
			order = append([]IntelligenceOrder(nil), spec.defaultOrder...)
		}
	}
	if len(query.GroupBy) == 0 && len(query.Metrics) == 0 {
		if _, hasRecordID := spec.columns["record_id"]; hasRecordID {
			hasTieBreak := false
			for _, item := range order {
				if item.Field == "record_id" {
					hasTieBreak = true
					break
				}
			}
			if !hasTieBreak {
				order = append(order, IntelligenceOrder{Field: "record_id", Direction: "asc"})
			}
		}
	} else if len(query.GroupBy) > 0 {
		// A keyset must order by the complete group identity. A caller may
		// prioritize one group field, but every remaining group key is appended
		// as a deterministic tie-breaker so equal leading values cannot vanish
		// between pages.
		for _, field := range query.GroupBy {
			found := false
			for _, item := range order {
				if item.Field == field {
					found = true
					break
				}
			}
			if !found {
				order = append(order, IntelligenceOrder{Field: field, Direction: "asc"})
			}
		}
	}
	orderParts := make([]string, 0, len(order))
	paginationStable := true
	groupFields := make(map[string]bool, len(query.GroupBy))
	seenOrder := make(map[string]bool, len(order))
	for _, field := range query.GroupBy {
		groupFields[field] = true
	}
	for i, item := range order {
		if seenOrder[item.Field] {
			return nil, types.NewAppError(types.CodeValidation,
				"情报查询 order_by 字段不能重复", types.ErrValidation)
		}
		seenOrder[item.Field] = true
		if _, exists := seenOutput[item.Field]; !exists {
			// Non-aggregate queries may order by a catalog field not selected.
			if len(query.GroupBy) > 0 || len(query.Metrics) > 0 {
				return nil, invalidIntelligenceField(item.Field)
			}
			if _, exists := spec.columns[item.Field]; !exists {
				return nil, invalidIntelligenceField(item.Field)
			}
			selectSQL = append(selectSQL, quoteIntelligenceIdent(item.Field))
		}
		direction := strings.ToUpper(item.Direction)
		if direction == "" {
			direction = "ASC"
		}
		if direction != "ASC" && direction != "DESC" {
			return nil, types.NewAppError(types.CodeValidation,
				"情报查询 order_by.direction 无效", types.ErrValidation)
		}
		if !spec.stableOrder[item.Field] {
			paginationStable = false
		}
		if (len(query.GroupBy) > 0 || len(query.Metrics) > 0) && !groupFields[item.Field] {
			paginationStable = false
		}
		order[i].Direction = strings.ToLower(direction)
		orderParts = append(orderParts, quoteIntelligenceIdent(item.Field)+" "+direction+" NULLS LAST")
	}
	if query.Cursor != "" && !paginationStable {
		return nil, types.NewAppError(types.CodeValidation,
			"游标不能用于会随编辑变化的排序；请使用不可变时间与记录引用排序", types.ErrValidation)
	}
	if query.Cursor != "" && len(cursorAfter) != len(order) {
		return nil, invalidIntelligenceCursor()
	}

	pairs := make([]string, 0, (len(columns)+len(order))*2)
	for _, column := range columns {
		pairs = append(pairs, quoteSQLLiteral(column.Name), quoteIntelligenceIdent(column.Name))
	}
	for i, item := range order {
		pairs = append(pairs, quoteSQLLiteral(fmt.Sprintf("__cursor_%d", i)),
			quoteIntelligenceIdent(item.Field))
	}
	inner := `SELECT ` + strings.Join(selectSQL, ",") +
		` FROM (` + spec.base + `) intelligence_base WHERE ` + strings.Join(where, " AND ") + groupSQL
	pageSQL := ""
	if len(cursorAfter) > 0 {
		clause, values, err := compileIntelligenceKeyset(order, spec, cursorAfter, len(args)+1)
		if err != nil {
			return nil, err
		}
		pageSQL = " WHERE " + clause
		args = append(args, values...)
	}
	orderSQL := ""
	if len(orderParts) > 0 {
		orderSQL = " ORDER BY " + strings.Join(orderParts, ",")
	}
	args = append(args, limit+1)
	sql := `SELECT jsonb_build_object(` + strings.Join(pairs, ",") + `)
	          FROM (` + inner + `) intelligence_result` + pageSQL + orderSQL +
		fmt.Sprintf(" LIMIT $%d", len(args))
	return &compiledIntelligenceQuery{
		sql: sql, args: args, columns: columns, limit: limit,
		order: order, queryDigest: queryDigest,
		asOf: asOf, paginationStable: paginationStable,
	}, nil
}

func compileIntelligenceKeyset(
	order []IntelligenceOrder,
	spec intelligenceDatasetSpec,
	after []json.RawMessage,
	firstParam int,
) (string, []any, error) {
	if len(order) == 0 || len(order) != len(after) {
		return "", nil, invalidIntelligenceCursor()
	}
	values := make([]any, len(order))
	for i, item := range order {
		column, ok := spec.columns[item.Field]
		if !ok || !spec.stableOrder[item.Field] {
			return "", nil, invalidIntelligenceCursor()
		}
		value, err := decodeIntelligenceValue(after[i], column.typ)
		if err != nil {
			return "", nil, invalidIntelligenceCursor()
		}
		values[i] = value
	}
	branches := make([]string, 0, len(order))
	for i, item := range order {
		terms := make([]string, 0, i+1)
		for prior := 0; prior < i; prior++ {
			terms = append(terms, fmt.Sprintf("%s = $%d",
				quoteIntelligenceIdent(order[prior].Field), firstParam+prior))
		}
		operator := ">"
		if strings.EqualFold(item.Direction, "desc") {
			operator = "<"
		}
		terms = append(terms, fmt.Sprintf("%s %s $%d",
			quoteIntelligenceIdent(item.Field), operator, firstParam+i))
		branches = append(branches, "("+strings.Join(terms, " AND ")+")")
	}
	return "(" + strings.Join(branches, " OR ") + ")", values, nil
}

func compileIntelligenceFilter(
	filter IntelligenceFilter,
	spec intelligenceDatasetSpec,
	location *time.Location,
	asOf time.Time,
	firstParam int,
) (string, []any, error) {
	col, exists := spec.columns[filter.Field]
	if !exists {
		return "", nil, invalidIntelligenceField(filter.Field)
	}
	field := quoteIntelligenceIdent(filter.Field)
	op := strings.ToLower(filter.Op)
	allowedValues := spec.filterEnums[filter.Field]
	if op == "within" {
		if col.typ != "time" || location == nil {
			return "", nil, types.NewAppError(types.CodeValidation,
				"within 仅接受具有明确任务/用户时区的时间字段", types.ErrValidation)
		}
		var token string
		if err := json.Unmarshal(filter.Value, &token); err != nil {
			return "", nil, invalidIntelligenceFilterValue()
		}
		start, end, err := resolveRelativeWindow(asOf, location, token)
		if err != nil {
			return "", nil, err
		}
		return fmt.Sprintf("%s >= $%d AND %s < $%d", field, firstParam, field, firstParam+1), []any{start, end}, nil
	}
	if op == "in" {
		if col.typ != "text" {
			return "", nil, types.NewAppError(types.CodeValidation,
				"in 当前只接受文本字段", types.ErrValidation)
		}
		var values []string
		if err := json.Unmarshal(filter.Value, &values); err != nil || len(values) == 0 || len(values) > 50 {
			return "", nil, invalidIntelligenceFilterValue()
		}
		if err := validateIntelligenceFilterEnum(filter.Field, values, allowedValues); err != nil {
			return "", nil, err
		}
		return fmt.Sprintf("%s = ANY($%d::text[])", field, firstParam), []any{values}, nil
	}
	value, err := decodeIntelligenceValue(filter.Value, col.typ)
	if err != nil {
		return "", nil, err
	}
	if len(allowedValues) > 0 {
		text, ok := value.(string)
		if !ok || (op != "eq" && op != "neq") {
			return "", nil, invalidIntelligenceEnumFilter(filter.Field, allowedValues)
		}
		if err := validateIntelligenceFilterEnum(filter.Field, []string{text}, allowedValues); err != nil {
			return "", nil, err
		}
	}
	switch op {
	case "eq":
		return fmt.Sprintf("%s = $%d", field, firstParam), []any{value}, nil
	case "neq":
		return fmt.Sprintf("%s IS DISTINCT FROM $%d", field, firstParam), []any{value}, nil
	case "gt", "gte", "lt", "lte":
		if col.typ != "number" && col.typ != "integer" && col.typ != "time" && col.typ != "text" {
			return "", nil, invalidIntelligenceFilterValue()
		}
		operator := map[string]string{"gt": ">", "gte": ">=", "lt": "<", "lte": "<="}[op]
		return fmt.Sprintf("%s %s $%d", field, operator, firstParam), []any{value}, nil
	case "contains":
		if col.typ != "text" {
			return "", nil, invalidIntelligenceFilterValue()
		}
		text, ok := value.(string)
		if !ok || len(text) == 0 || len(text) > 1024 {
			return "", nil, invalidIntelligenceFilterValue()
		}
		text = strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`).Replace(text)
		return fmt.Sprintf("%s ILIKE '%%' || $%d || '%%' ESCAPE '\\'", field, firstParam), []any{text}, nil
	default:
		return "", nil, types.NewAppError(types.CodeValidation,
			"情报查询 filter.op 无效", types.ErrValidation)
	}
}

func validateIntelligenceFilterEnum(field string, values, allowed []string) error {
	if len(allowed) == 0 {
		return nil
	}
	valid := make(map[string]struct{}, len(allowed))
	for _, value := range allowed {
		valid[value] = struct{}{}
	}
	for _, value := range values {
		if _, ok := valid[value]; !ok {
			return invalidIntelligenceEnumFilter(field, allowed)
		}
	}
	return nil
}

func invalidIntelligenceEnumFilter(field string, allowed []string) error {
	detail := fmt.Sprintf("情报查询字段 %s 只接受 %s", field, strings.Join(allowed, ", "))
	if field == "outcome_status" {
		detail += "；不存在 success，运行是否产出情报需同时读取 result"
	}
	return types.NewAppError(types.CodeValidation, detail, types.ErrValidation)
}

func decodeIntelligenceValue(raw json.RawMessage, typ string) (any, error) {
	switch typ {
	case "text":
		var value string
		if err := json.Unmarshal(raw, &value); err != nil || len(value) > 4096 {
			return nil, invalidIntelligenceFilterValue()
		}
		return value, nil
	case "number":
		var value json.Number
		decoder := json.NewDecoder(strings.NewReader(string(raw)))
		decoder.UseNumber()
		if err := decoder.Decode(&value); err != nil {
			return nil, invalidIntelligenceFilterValue()
		}
		parsed, err := value.Float64()
		if err != nil {
			return nil, invalidIntelligenceFilterValue()
		}
		return parsed, nil
	case "integer":
		var value json.Number
		decoder := json.NewDecoder(strings.NewReader(string(raw)))
		decoder.UseNumber()
		if err := decoder.Decode(&value); err != nil {
			return nil, invalidIntelligenceFilterValue()
		}
		parsed, err := value.Int64()
		if err != nil {
			return nil, invalidIntelligenceFilterValue()
		}
		return parsed, nil
	case "boolean":
		var value bool
		if err := json.Unmarshal(raw, &value); err != nil {
			return nil, invalidIntelligenceFilterValue()
		}
		return value, nil
	case "time":
		var value string
		if err := json.Unmarshal(raw, &value); err != nil {
			return nil, invalidIntelligenceFilterValue()
		}
		parsed, err := time.Parse(time.RFC3339, value)
		if err != nil {
			return nil, invalidIntelligenceFilterValue()
		}
		return parsed, nil
	case "json":
		if !json.Valid(raw) {
			return nil, invalidIntelligenceFilterValue()
		}
		return raw, nil
	default:
		return nil, invalidIntelligenceFilterValue()
	}
}

func resolveIntelligenceLocation(
	ctx context.Context,
	querier interface {
		Query(context.Context, string, ...any) (pgx.Rows, error)
	},
	scope IntelligenceScope,
	query IntelligenceQuery,
	spec intelligenceDatasetSpec,
) (*time.Location, error) {
	needsRelative := false
	for _, filter := range query.Filters {
		if strings.EqualFold(filter.Op, "within") {
			needsRelative = true
			break
		}
	}
	if !needsRelative {
		return nil, nil
	}
	if !spec.relativeTimeZone {
		return nil, types.NewAppError(types.CodeValidation,
			"该数据集没有可验证的业务时区", types.ErrValidation)
	}
	taskID := scope.TaskID
	if taskID == "" {
		for _, filter := range query.Filters {
			if filter.Field == "task_ref" && strings.EqualFold(filter.Op, "eq") {
				_ = json.Unmarshal(filter.Value, &taskID)
				break
			}
		}
	}
	args := []any{scope.TenantID, scope.UserID}
	taskPredicate := ""
	if taskID != "" {
		args = append(args, taskID)
		taskPredicate = " AND id=$3"
	}
	rows, err := querier.Query(ctx,
		`SELECT DISTINCT COALESCE(NULLIF(spec_json->>'timezone',''),NULLIF(spec_json->>'tz',''))
		   FROM schedules
		  WHERE tenant_id=$1 AND user_id=$2`+taskPredicate,
		args...)
	if err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "解析用户情报业务时区", err)
	}
	defer rows.Close()
	var zones []string
	for rows.Next() {
		var zone *string
		if err := rows.Scan(&zone); err != nil {
			return nil, types.NewAppError(types.CodeDatabase, "扫描用户情报业务时区", err)
		}
		if zone != nil {
			zones = append(zones, *zone)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "遍历用户情报业务时区", err)
	}
	if len(zones) != 1 {
		return nil, types.NewAppError(types.CodeValidation,
			"相对时间需要先定位一个具有明确时区的任务", types.ErrValidation)
	}
	location, err := time.LoadLocation(zones[0])
	if err != nil {
		return nil, types.NewAppError(types.CodeValidation,
			"任务时区无效，无法解析相对时间", types.ErrValidation)
	}
	return location, nil
}

func resolveRelativeWindow(now time.Time, location *time.Location, token string) (time.Time, time.Time, error) {
	local := now.In(location)
	today := time.Date(local.Year(), local.Month(), local.Day(), 0, 0, 0, 0, location)
	switch strings.ToLower(strings.TrimSpace(token)) {
	case "today":
		return today, today.AddDate(0, 0, 1), nil
	case "yesterday":
		return today.AddDate(0, 0, -1), today, nil
	case "last_7_days":
		return today.AddDate(0, 0, -6), today.AddDate(0, 0, 1), nil
	default:
		return time.Time{}, time.Time{}, types.NewAppError(types.CodeValidation,
			"相对时间仅支持 today、yesterday、last_7_days", types.ErrValidation)
	}
}

func intelligenceQueryDigest(query IntelligenceQuery, includeCursor bool) string {
	copyQuery := query
	if !includeCursor {
		copyQuery.Cursor = ""
	}
	raw, _ := json.Marshal(struct {
		CatalogVersion string            `json:"catalog_version"`
		Query          IntelligenceQuery `json:"query"`
	}{IntelligenceCatalogVersion, copyQuery})
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

func intelligenceQueryAuditMaterial(query IntelligenceQuery) (string, json.RawMessage) {
	type filterSummary struct{ Field, Op string }
	summary := struct {
		Dataset   IntelligenceDataset  `json:"dataset"`
		Select    []string             `json:"select,omitempty"`
		Filters   []filterSummary      `json:"filters,omitempty"`
		GroupBy   []string             `json:"group_by,omitempty"`
		Metrics   []IntelligenceMetric `json:"metrics,omitempty"`
		OrderBy   []IntelligenceOrder  `json:"order_by,omitempty"`
		Limit     int                  `json:"limit,omitempty"`
		HasCursor bool                 `json:"has_cursor"`
	}{query.Dataset, boundedAuditStrings(query.Select, 32), nil,
		boundedAuditStrings(query.GroupBy, 8), nil, nil,
		query.Limit, query.Cursor != ""}
	for _, metric := range query.Metrics[:min(len(query.Metrics), 8)] {
		metric.Function = boundedAuditString(metric.Function)
		metric.Field = boundedAuditString(metric.Field)
		metric.As = boundedAuditString(metric.As)
		summary.Metrics = append(summary.Metrics, metric)
	}
	for _, order := range query.OrderBy[:min(len(query.OrderBy), 8)] {
		order.Field = boundedAuditString(order.Field)
		order.Direction = boundedAuditString(order.Direction)
		summary.OrderBy = append(summary.OrderBy, order)
	}
	for _, filter := range query.Filters[:min(len(query.Filters), 16)] {
		summary.Filters = append(summary.Filters, filterSummary{
			boundedAuditString(filter.Field), boundedAuditString(filter.Op)})
	}
	raw, _ := json.Marshal(summary)
	return intelligenceQueryDigest(query, true), raw
}

func boundedAuditStrings(values []string, limit int) []string {
	values = values[:min(len(values), limit)]
	out := make([]string, 0, len(values))
	for _, value := range values {
		out = append(out, boundedAuditString(value))
	}
	return out
}

func boundedAuditString(value string) string {
	const limit = 128
	if len(value) <= limit {
		return value
	}
	return value[:limit]
}

type intelligenceCursor struct {
	Version      int               `json:"v"`
	Catalog      string            `json:"catalog"`
	KeyVersion   int               `json:"k"`
	TenantID     int64             `json:"t"`
	UserID       int64             `json:"u"`
	TaskID       string            `json:"task,omitempty"`
	QueryDigest  string            `json:"q"`
	After        []json.RawMessage `json:"after"`
	AsOfUnixNano int64             `json:"a"`
}

func (s *Store) signIntelligenceCursor(scope IntelligenceScope, queryDigest string, after []json.RawMessage, asOf time.Time) string {
	state := s.intelligenceCursorState
	state.Lock()
	defer state.Unlock()
	keyVersion := state.activeKey
	payload, _ := json.Marshal(intelligenceCursor{
		Version: 2, Catalog: IntelligenceCatalogVersion, KeyVersion: keyVersion,
		TenantID: scope.TenantID, UserID: scope.UserID,
		TaskID: scope.TaskID, QueryDigest: queryDigest, After: after,
		AsOfUnixNano: asOf.UnixNano(),
	})
	mac := hmac.New(sha256.New, state.keys[keyVersion])
	_, _ = mac.Write(payload)
	signature := mac.Sum(nil)
	return base64.RawURLEncoding.EncodeToString(payload) + "." +
		base64.RawURLEncoding.EncodeToString(signature)
}

func (s *Store) verifyIntelligenceCursor(ctx context.Context, scope IntelligenceScope, queryDigest, encoded string) ([]json.RawMessage, time.Time, error) {
	parts := strings.Split(encoded, ".")
	if len(parts) != 2 {
		return nil, time.Time{}, invalidIntelligenceCursor()
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil || len(payload) > 8192 {
		return nil, time.Time{}, invalidIntelligenceCursor()
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, time.Time{}, invalidIntelligenceCursor()
	}
	var cursor intelligenceCursor
	if err := json.Unmarshal(payload, &cursor); err != nil || cursor.Version != 2 ||
		cursor.Catalog != IntelligenceCatalogVersion ||
		len(cursor.After) == 0 || len(cursor.After) > 9 {
		return nil, time.Time{}, invalidIntelligenceCursor()
	}
	state := s.intelligenceCursorState
	state.Lock()
	key, exists := state.keys[cursor.KeyVersion]
	key = append([]byte(nil), key...)
	state.Unlock()
	if !exists {
		if err := s.reloadIntelligenceCursorKeys(ctx); err != nil {
			return nil, time.Time{}, invalidIntelligenceCursor()
		}
		state.Lock()
		key, exists = state.keys[cursor.KeyVersion]
		key = append([]byte(nil), key...)
		state.Unlock()
		if !exists {
			return nil, time.Time{}, invalidIntelligenceCursor()
		}
	}
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write(payload)
	if !hmac.Equal(signature, mac.Sum(nil)) {
		return nil, time.Time{}, invalidIntelligenceCursor()
	}
	asOf := time.Unix(0, cursor.AsOfUnixNano).UTC()
	if cursor.TenantID != scope.TenantID ||
		cursor.UserID != scope.UserID ||
		cursor.TaskID != scope.TaskID || cursor.QueryDigest != queryDigest ||
		cursor.AsOfUnixNano <= 0 || asOf.After(time.Now().Add(time.Minute)) {
		return nil, time.Time{}, invalidIntelligenceCursor()
	}
	for _, raw := range cursor.After {
		if len(raw) == 0 || len(raw) > 4096 || !json.Valid(raw) || bytes.Equal(raw, []byte("null")) {
			return nil, time.Time{}, invalidIntelligenceCursor()
		}
	}
	return cursor.After, asOf, nil
}

func (s *Store) insertIntelligenceQueryAudit(
	ctx context.Context,
	scope IntelligenceScope,
	dataset IntelligenceDataset,
	digest string,
	summary json.RawMessage,
	status string,
	rowCount int,
	duration time.Duration,
	truncated bool,
) error {
	if _, valid := intelligenceCatalog[dataset]; !valid {
		dataset = "invalid"
	}
	auditCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	tx, err := s.beginTx(auditCtx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(auditCtx)) }()
	if _, err := tx.Exec(auditCtx,
		`SET LOCAL search_path = pg_catalog, public, pg_temp`); err != nil {
		return err
	}
	if _, err := tx.Exec(auditCtx,
		`SELECT set_config('app.tenant_id',$1,true),
		        set_config('app.user_id',$2,true)`,
		strconv.FormatInt(scope.TenantID, 10), strconv.FormatInt(scope.UserID, 10)); err != nil {
		return err
	}
	if _, err := tx.Exec(auditCtx, `SET LOCAL ROLE vane_app`); err != nil {
		return err
	}
	durationMS := duration.Milliseconds()
	if durationMS < 0 {
		durationMS = 0
	}
	if durationMS > 2147483647 {
		durationMS = 2147483647
	}
	_, err = tx.Exec(auditCtx,
		`INSERT INTO agent_intelligence_query_audits (
		     tenant_id,user_id,session_id,dataset,query_digest,query_summary,
		     status,row_count,duration_ms,truncated
		 ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
		scope.TenantID, scope.UserID, scope.SessionID, dataset, digest, summary,
		status, rowCount, durationMS, truncated)
	if err != nil {
		return err
	}
	return tx.Commit(auditCtx)
}

func (s *Store) insertIntelligenceAccessDenial(
	ctx context.Context,
	scope IntelligenceScope,
	dataset IntelligenceDataset,
	digest, reason string,
) error {
	auditCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	tx, err := s.beginTx(auditCtx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(auditCtx)) }()
	if _, err := tx.Exec(auditCtx,
		`SET LOCAL search_path = pg_catalog, public, pg_temp`); err != nil {
		return err
	}
	datasetName := string(dataset)
	if _, valid := intelligenceCatalog[dataset]; !valid {
		datasetName = "invalid"
	}
	if _, err := tx.Exec(auditCtx,
		`INSERT INTO agent_intelligence_access_denials (
		     presented_tenant_id,presented_user_id,dataset,query_digest,reason
		 ) VALUES ($1,$2,$3,$4,$5)`,
		scope.TenantID, scope.UserID, datasetName, digest, reason); err != nil {
		return err
	}
	return tx.Commit(auditCtx)
}

func validIntelligenceIdent(value string) bool {
	if len(value) == 0 || len(value) > 63 {
		return false
	}
	for i, r := range value {
		if !(r == '_' || r >= 'a' && r <= 'z' || i > 0 && r >= '0' && r <= '9') {
			return false
		}
	}
	return true
}

func quoteIntelligenceIdent(value string) string { return `"` + value + `"` }
func quoteSQLLiteral(value string) string        { return `'` + strings.ReplaceAll(value, `'`, `''`) + `'` }

func invalidIntelligenceField(field string) error {
	return types.NewAppError(types.CodeValidation,
		fmt.Sprintf("情报查询字段 %q 不存在或不可读", field), types.ErrValidation)
}

func invalidIntelligenceFilterValue() error {
	return types.NewAppError(types.CodeValidation,
		"情报查询 filter.value 类型无效", types.ErrValidation)
}

func invalidIntelligenceCursor() error {
	return types.NewAppError(types.CodeValidation,
		"情报查询游标无效、已过期或被篡改", types.ErrValidation)
}

func mapIntelligenceQueryError(err error) error {
	if errors.Is(err, context.DeadlineExceeded) || isStatementTimeout(err) {
		return types.NewAppError(types.CodeDatabase, "用户情报查询超时", context.DeadlineExceeded)
	}
	return types.NewAppError(types.CodeDatabase, "执行用户情报查询", err)
}

func isStatementTimeout(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "57014"
}
