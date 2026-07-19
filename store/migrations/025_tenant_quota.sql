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
    -- tokens **允许为负**（刻意不加 tokens >= 0）。
    --
    -- 负数就是"欠账"：LLM 的实际用量要调用完才知道，事前只能按估算预扣。
    -- 若禁止负数，事后对账在"实际 > 估算"时就扣不动，于是超出的那部分
    -- 被永久丢弃——桶会显示还有余额，而钱已经花掉了。
    --
    -- 2026-07-19 审查实测过这个失败模式的威力：桶余额一旦低于单次用量，
    -- 事后扣减每次都失败、只有事前那点预扣生效，补充速率反超消耗速率，
    -- **桶不降反升**，实测放行 4.9 倍日额度且无上界。允许记负债正是修法：
    -- 欠账被如实记下，下一次事前检查就过不了，直到时间把它补回正数。
    CONSTRAINT ck_tenant_quota_rates CHECK (rate >= 0 AND burst > 0)
);

COMMENT ON TABLE tenant_quota IS
    'per-tenant token bucket（契约 §2.7）。取用与补充由单条原子 UPDATE 完成，无需 Redis。tokens 可为负=欠账。';

-- 存量租户回填。**没有这一段，本迁移上线即锁死生产**：
--
-- 018_tenants.sql 显式建了 tenant id=1（生产租户本人）并回填了 owner 的
-- membership，而 SeedTenantQuota 只在"新建租户"路径上被调用（注册 / 邀请码兑换）。
-- 于是 025 上线后 tenant 1 一行配额都没有，配合代码里"缺行即拒绝"的失败方向，
-- 它的每一次 LLM 调用都被拒 —— 而下游把额度用尽当作**正常终态**处理：
-- 推送 100% 停摆、Temporal 里一片绿、零告警，用户只收到一句
-- 「额度会随时间自动恢复」。没有配额行就没有 rate，永远不会恢复。
--
-- 参数与 store/tenant_quota.go 的 defaultQuotas 一致；两处必须同步，
-- 由 TestInvariant_MigrationBackfillMatchesDefaults 守住。
-- ON CONFLICT DO NOTHING：与 SeedTenantQuota 同样幂等，重跑不会把用掉的额度刷回满格。
INSERT INTO tenant_quota (tenant_id, bucket, tokens, rate, burst)
SELECT t.id, q.bucket, q.burst, q.rate, q.burst
  FROM tenants t
 CROSS JOIN (VALUES
     ('llm_tokens',   2000000.0 / 86400, 2000000.0),
     ('exa_calls',        500.0 / 86400,     500.0),
     ('tikhub_calls',     500.0 / 86400,     500.0),
     ('push',             200.0 / 86400,     200.0),
     ('fetch',           2000.0 / 86400,    2000.0)
 ) AS q(bucket, rate, burst)
ON CONFLICT (tenant_id, bucket) DO NOTHING;

-- +goose Down
DROP TABLE IF EXISTS tenant_quota;
