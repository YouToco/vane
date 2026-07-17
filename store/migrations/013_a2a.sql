-- 013: A2A server 任务持久化（a2a-contract §2）

-- +goose Up

CREATE TABLE a2a_tasks (
    id         TEXT PRIMARY KEY,           -- 服务端生成 taskId（SDK uuid），不可由客户端指定
    context_id TEXT NOT NULL,              -- 会话轴：同 contextId 的任务构成一段对话
    status     TEXT NOT NULL,              -- TASK_STATE_* 原文（提取列：List 过滤与探针用）
    task       JSONB NOT NULL,             -- 完整 a2a.Task（ProtoJSON），权威载荷
    version    BIGINT NOT NULL DEFAULT 1,  -- 乐观并发（SDK TaskVersion 语义）
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_a2a_tasks_context ON a2a_tasks (context_id, created_at DESC);

-- status 单列/复合索引刻意不建：验证窗口才打开的功能没有该查询量，ListA2ATasks 的
-- status 过滤（§4.1）走上述索引或顺序扫描后过滤即可；有真实查询量再补 status 维度。

-- +goose Down

DROP TABLE a2a_tasks;
