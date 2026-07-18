-- 015: agent 工具调用记录表 + 会话激活端点列表（TikHub 端点注册表契约 §5/§6）
--
-- tool_calls 是 agent 全部工具调用的记账表（Boss 拍板 2026-07-18：全量工具都记，
-- 不只 TikHub 元工具）。定位与 llm_calls 同构：旁路可观测性，记账失败不放大成
-- 业务失败；字段语义对齐 OTel GenAI 的 execute_tool span 约定（gen_ai.tool.name /
-- error.type 低基数 / duration），未来接任何 OTel 兼容后端零翻译。
--
-- 三个「注册表场景特有」字段的存在理由：
--   retrieval_query + candidate_tools：search_endpoints 每次检索记下「用什么词搜到
--   哪些候选」——这是事后评估检索召回、决定要不要升级 embedding 检索的唯一数据源
--   （没有它，「搜索质量好不好」永远只能靠感觉）。
--   result_preview 截断存（8KB）而非全文：上游响应动辄 100KB+ 且可随时重取，
--   不是本库资产；行式存储塞大 blob 是 Langfuse 迁 ClickHouse 的直接教训。
--   result_size + http_status 保全量元数据——「元数据 100% 记录，内容截断」。

-- +goose Up

CREATE TABLE tool_calls (
    id              BIGSERIAL   PRIMARY KEY,
    trace_id        TEXT        NOT NULL DEFAULT '',      -- 与 llm_calls.trace_id 同源，可 JOIN 回放整条消息链路
    user_id         BIGINT,                               -- 归属用户；与 llm_calls 一致刻意不建 FK
    session_id      BIGINT,                               -- 来源 agent 会话；确认卡回调等无会话路径为 NULL
    tool_name       TEXT        NOT NULL,                 -- 工具名（静态工具名或端点名，即 gen_ai.tool.name）
    tool_kind       TEXT        NOT NULL DEFAULT 'static',-- static / tikhub_search / tikhub_endpoint
    endpoint_path   TEXT        NOT NULL DEFAULT '',      -- tikhub_endpoint 专用：上游 path
    arguments       JSONB,                                -- 模型产出的调用参数原文
    result_preview  TEXT        NOT NULL DEFAULT '',      -- 结果截断版（8K rune，rune 边界安全）
    result_size     INT         NOT NULL DEFAULT 0,       -- 截断前结果字节数
    http_status     INT,                                  -- tikhub_endpoint 专用：上游 HTTP 状态；非 HTTP 工具为 NULL
    error_type      TEXT        NOT NULL DEFAULT '',      -- 低基数错误分类（timeout/http_error/invalid_args/budget_exceeded/internal），成功为空串
    error           TEXT        NOT NULL DEFAULT '',      -- 错误详情，成功为空串
    duration_ms     INT         NOT NULL DEFAULT 0,
    retrieval_query TEXT        NOT NULL DEFAULT '',      -- tikhub_search 专用：检索词
    candidate_tools TEXT[]      NOT NULL DEFAULT '{}',    -- tikhub_search 专用：本次返回的候选端点名
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 按时间窗口统计调用量（每日限额 COUNT 与成本分析共用）
CREATE INDEX idx_tool_calls_kind_created ON tool_calls (tool_kind, created_at);
-- per-tool 错误率/延迟视图（坏端点发现）
CREATE INDEX idx_tool_calls_tool_created ON tool_calls (tool_name, created_at);
-- 链路回放：与 llm_calls 同款 trace 聚合
CREATE INDEX idx_tool_calls_trace ON tool_calls (trace_id, created_at);

-- 会话激活端点列表（契约 §4）：search_endpoints 命中的端点在会话内被"激活"为一等
-- FC 工具，激活集必须随会话持久化——30 分钟 TTL 内跨消息有效，进程重启不丢。
-- JSONB 数组存端点名（激活顺序即注入顺序，append-only + FIFO 逐出保 DeepSeek 缓存前缀）。
ALTER TABLE agent_sessions ADD COLUMN activated_tools JSONB NOT NULL DEFAULT '[]';

-- +goose Down

ALTER TABLE agent_sessions DROP COLUMN activated_tools;
DROP TABLE tool_calls;
