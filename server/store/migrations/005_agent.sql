-- 005_agent.sql — M4 最小 Agent Loop：会话表 + 待确认动作表（契约 §1）
--
-- 设计要点：
--   1. agent_sessions 单 owner MVP：messages 整列存 OpenAI 兼容消息数组 JSONB
--      （含 tool_calls），覆盖写而非行级建模——会话消息是应用层截断的小数组
--      （上限 60 条，契约 §10），不值得拆表。system 消息不入库，调用时动态前置。
--   2. 会话过期是惰性翻转：GetActiveAgentSession 读取时把 updated_at 早于 TTL
--      窗口的 active 会话顺带置 expired，不引入后台清理任务。
--   3. pending_actions 主键 TEXT 存 uuid：确认卡按钮 value 只携带此 id，
--      工具参数以库中 args 为准，杜绝客户端篡改（契约 §10 安全红线）。
--   4. 状态枚举（active/expired/closed、pending/executed/cancelled/expired）
--      由应用层校验，未建 CHECK（沿用 001 约定）；updated_at 由应用层负责
--      更新（001 约定不建触发器）。

-- +goose Up

-- agent 会话：单 owner MVP，messages 存 OpenAI 兼容消息数组（含 tool_calls）
CREATE TABLE agent_sessions (
    id         BIGSERIAL   PRIMARY KEY,
    user_id    BIGINT      NOT NULL REFERENCES users (id),
    status     TEXT        NOT NULL DEFAULT 'active',  -- active / expired / closed（应用层校验）
    messages   JSONB       NOT NULL DEFAULT '[]',
    turn_count INT         NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_agent_sessions_user_status ON agent_sessions (user_id, status, updated_at DESC);

-- 待确认动作：确认卡按钮 value 只带 id，参数服务端存取（防客户端篡改参数）
CREATE TABLE pending_actions (
    id          TEXT        PRIMARY KEY,               -- uuid
    user_id     BIGINT      NOT NULL REFERENCES users (id),
    session_id  BIGINT      REFERENCES agent_sessions (id),
    tool_name   TEXT        NOT NULL,
    args        JSONB       NOT NULL DEFAULT '{}',
    summary     TEXT        NOT NULL DEFAULT '',       -- 卡片上展示过的人类可读摘要
    status      TEXT        NOT NULL DEFAULT 'pending', -- pending / executed / cancelled / expired
    expires_at  TIMESTAMPTZ NOT NULL,
    executed_at TIMESTAMPTZ,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_pending_actions_user_status ON pending_actions (user_id, status);

-- +goose Down

-- 按 FK 依赖逆序删除
DROP TABLE IF EXISTS pending_actions;
DROP TABLE IF EXISTS agent_sessions;
