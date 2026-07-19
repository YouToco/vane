-- 025: per-tenant 配额桶（契约 §2.7，D3 下配额是财务护栏而非可选优化）
--
-- D3 拍板「第三方 API 成本平台全垫付」，于是每个租户的调用量直接等于平台的账单。
-- D4（邀请制）靠"发出的邀请数"给敞口封顶，是第一道；本表是第二道——
-- **邀请出去的人也可能（有意或无意地）把额度跑光**，而在 #91 把邀请码签发做出来之后，
-- 陌生人真的能注册了，这道闸门才从"将来要做"变成"现在就得有"。
--
-- ---- 为什么是 Postgres 单行 token bucket，不是 Redis ----
--
-- vane 栈里没有 Redis。为限流引入一个新的有状态组件，意味着新的部署面、新的故障模式、
-- 以及"Redis 挂了配额怎么办"这个必须回答却没有好答案的问题。
-- Postgres 的单行原子 UPDATE 就够：取用与补充在同一条语句里完成，天然无竞态。
--
-- ---- 桶的划分：财务面 vs DoS 面 ----
--
-- 契约原文列的是 llm_tokens | push | fetch | tikhub_calls，**漏了 Exa**——
-- 而真正按次计费的是 LLM / TikHub / Exa 三家（2026-07-19 Boss 纠正）。
-- 这里按用途分成两类，因为它们的失败语义不同：
--
--   财务面（llm_tokens / tikhub_calls / exa_calls）：超限意味着"再花就超预算"，
--     必须硬拦。
--   DoS 面（push / fetch）：超限意味着"这个租户太吵"，拦的是资源占用而非钱。
--
-- 分开也让将来调参有的放矢：财务面的速率该跟着预算走，DoS 面该跟着容量走。

-- +goose Up

CREATE TABLE tenant_quota (
    tenant_id  BIGINT           NOT NULL REFERENCES tenants (id),
    bucket     TEXT             NOT NULL,
    -- tokens 用 DOUBLE PRECISION 而非整数：补充是按经过秒数连续计算的
    -- （rate * 秒数），整数会在高频小额取用时把小数部分反复截断掉，
    -- 实际速率被系统性地压低——而且压低的幅度取决于调用频率，查都查不出来。
    tokens     DOUBLE PRECISION NOT NULL,
    rate       DOUBLE PRECISION NOT NULL,  -- 每秒补充量
    burst      DOUBLE PRECISION NOT NULL,  -- 桶容量（也是单次可取的上限）
    updated_at TIMESTAMPTZ      NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, bucket),
    CONSTRAINT ck_tenant_quota_nonneg CHECK (tokens >= 0 AND rate >= 0 AND burst > 0)
);

COMMENT ON TABLE tenant_quota IS
    'per-tenant token bucket（契约 §2.7）。取用与补充由单条原子 UPDATE 完成，无需 Redis。';

-- +goose Down
DROP TABLE IF EXISTS tenant_quota;
