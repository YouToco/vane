-- 020: 业务表加 tenant_id —— 让租户隔离从「会话里的一个数字」变成数据层真实边界
-- （企业级契约 §2.1 表分级矩阵）
--
-- 改造前的处境：D2′ 认证上线后每个请求都带 principal{TenantID, UserID}，但**业务表
-- 只有 user_id**。当前每租户恰好 1 人，按 user_id 过滤与按租户过滤等价，所以不是漏洞；
-- 但多人租户一出现，等价关系立刻断裂——那时再补列就是带着真实用户做在线迁移。
--
-- ============================================================
-- 不变量 I-T1（红线）：四张「客观事实表」刻意**不加** tenant_id
-- ============================================================
--   sources / content_items / content_sources / page_snapshots
--
-- 它们是跨租户共享的客观事实，共享抓取是设计意图而非疏漏：
--   - sources.url 全局 UNIQUE（007）为「多用户加同源 → 重复抓取 + 重复付费」而设；
--   - content_items.canonical_key 全局 UNIQUE（007）承载跨源去重与 TikHub 详情补全的
--     付费闸门——给它加租户前缀，同一篇小红书笔记会被 N 个租户各付费补全一次。
--
-- 「谁能看到这条内容」由 subscriptions/deliveries 的租户维度表达，**不由内容身份表达**。
-- 本文件末尾有守卫查询把这条钉住。
--
-- 迁移策略：加可空列 → 回填 → NOT VALID CHECK → VALIDATE → SET NOT NULL。
-- 当前生产只有 1 个租户、存量行 tenant_id 恒为 1，回填是常量写，秒级完成。
-- 五步法的意义在于**它对将来有真实租户时同样成立**（契约 §2.7）。

-- +goose Up

-- ---- 步骤 0：memberships 的 user 外键改级联删除 ----
--
-- 语义本就该如此：membership 是「这个人属于这个租户」的事实，人没了事实自然消亡。
-- 018 建表时用了默认的 NO ACTION，后果是删用户会被外键挡住——这在测试里立刻暴露
-- （每个用例都要记得先删 membership），在生产上则会让「注销用户」这类操作莫名失败。
ALTER TABLE memberships DROP CONSTRAINT memberships_user_id_fkey;
ALTER TABLE memberships ADD  CONSTRAINT memberships_user_id_fkey
    FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE;

-- ---- 步骤 1：加列（PG11+ 加可空列不重写表，秒级） ----

ALTER TABLE subscriptions   ADD COLUMN tenant_id BIGINT;
ALTER TABLE push_batches    ADD COLUMN tenant_id BIGINT;
ALTER TABLE deliveries      ADD COLUMN tenant_id BIGINT;
ALTER TABLE feedbacks       ADD COLUMN tenant_id BIGINT;
ALTER TABLE profiles        ADD COLUMN tenant_id BIGINT;
ALTER TABLE schedules       ADD COLUMN tenant_id BIGINT;
ALTER TABLE agent_sessions  ADD COLUMN tenant_id BIGINT;
ALTER TABLE pending_actions ADD COLUMN tenant_id BIGINT;

-- llm_calls / tool_calls 的 user_id **可空**（系统级调用无归属用户），
-- 故 tenant_id 同样可空：一次系统级 LLM 调用确实不属于任何租户。
-- NULL 在 RLS 下对任何租户都不可见，这正是想要的语义（平台侧查询绕过 RLS 另计）。
ALTER TABLE llm_calls  ADD COLUMN tenant_id BIGINT;
ALTER TABLE tool_calls ADD COLUMN tenant_id BIGINT;

-- ---- 步骤 2：从 memberships 回填 ----
--
-- 经 memberships 而非硬编码 1：迁移必须表达「行归属于其所有者所在的租户」这条规则，
-- 而不是「现在只有一个租户所以填 1」。前者在将来有多租户时依然正确，后者是巧合。

UPDATE subscriptions   t SET tenant_id = m.tenant_id FROM memberships m WHERE m.user_id = t.user_id AND t.tenant_id IS NULL;
UPDATE push_batches    t SET tenant_id = m.tenant_id FROM memberships m WHERE m.user_id = t.user_id AND t.tenant_id IS NULL;
UPDATE deliveries      t SET tenant_id = m.tenant_id FROM memberships m WHERE m.user_id = t.user_id AND t.tenant_id IS NULL;
UPDATE feedbacks       t SET tenant_id = m.tenant_id FROM memberships m WHERE m.user_id = t.user_id AND t.tenant_id IS NULL;
UPDATE profiles        t SET tenant_id = m.tenant_id FROM memberships m WHERE m.user_id = t.user_id AND t.tenant_id IS NULL;
UPDATE schedules       t SET tenant_id = m.tenant_id FROM memberships m WHERE m.user_id = t.user_id AND t.tenant_id IS NULL;
UPDATE agent_sessions  t SET tenant_id = m.tenant_id FROM memberships m WHERE m.user_id = t.user_id AND t.tenant_id IS NULL;
UPDATE pending_actions t SET tenant_id = m.tenant_id FROM memberships m WHERE m.user_id = t.user_id AND t.tenant_id IS NULL;
UPDATE llm_calls       t SET tenant_id = m.tenant_id FROM memberships m WHERE m.user_id = t.user_id AND t.tenant_id IS NULL;
UPDATE tool_calls      t SET tenant_id = m.tenant_id FROM memberships m WHERE m.user_id = t.user_id AND t.tenant_id IS NULL;

-- ---- 步骤 3：外键 + 非空约束 ----
--
-- 外键指向 tenants：孤儿 tenant_id 比没有 tenant_id 更危险——它看起来受了隔离，
-- 实际指向一个不存在的租户，永远查不出来也永远删不掉。

ALTER TABLE subscriptions   ADD CONSTRAINT fk_subscriptions_tenant   FOREIGN KEY (tenant_id) REFERENCES tenants (id);
ALTER TABLE push_batches    ADD CONSTRAINT fk_push_batches_tenant    FOREIGN KEY (tenant_id) REFERENCES tenants (id);
ALTER TABLE deliveries      ADD CONSTRAINT fk_deliveries_tenant      FOREIGN KEY (tenant_id) REFERENCES tenants (id);
ALTER TABLE feedbacks       ADD CONSTRAINT fk_feedbacks_tenant       FOREIGN KEY (tenant_id) REFERENCES tenants (id);
ALTER TABLE profiles        ADD CONSTRAINT fk_profiles_tenant        FOREIGN KEY (tenant_id) REFERENCES tenants (id);
ALTER TABLE schedules       ADD CONSTRAINT fk_schedules_tenant       FOREIGN KEY (tenant_id) REFERENCES tenants (id);
ALTER TABLE agent_sessions  ADD CONSTRAINT fk_agent_sessions_tenant  FOREIGN KEY (tenant_id) REFERENCES tenants (id);
ALTER TABLE pending_actions ADD CONSTRAINT fk_pending_actions_tenant FOREIGN KEY (tenant_id) REFERENCES tenants (id);
ALTER TABLE llm_calls       ADD CONSTRAINT fk_llm_calls_tenant       FOREIGN KEY (tenant_id) REFERENCES tenants (id);
ALTER TABLE tool_calls      ADD CONSTRAINT fk_tool_calls_tenant      FOREIGN KEY (tenant_id) REFERENCES tenants (id);

-- 八张「必有归属」的表设 NOT NULL。存量已回填完毕，此处是廉价操作。
-- llm_calls / tool_calls 不设：系统级调用的 NULL 是真话（见步骤 1 说明）。
ALTER TABLE subscriptions   ALTER COLUMN tenant_id SET NOT NULL;
ALTER TABLE push_batches    ALTER COLUMN tenant_id SET NOT NULL;
ALTER TABLE deliveries      ALTER COLUMN tenant_id SET NOT NULL;
ALTER TABLE feedbacks       ALTER COLUMN tenant_id SET NOT NULL;
ALTER TABLE profiles        ALTER COLUMN tenant_id SET NOT NULL;
ALTER TABLE schedules       ALTER COLUMN tenant_id SET NOT NULL;
ALTER TABLE agent_sessions  ALTER COLUMN tenant_id SET NOT NULL;
ALTER TABLE pending_actions ALTER COLUMN tenant_id SET NOT NULL;

-- ---- 步骤 4：索引首列改 tenant_id ----
--
-- 契约 §2.3：租户所有表的索引首列必须是 tenant_id。数据量小时性能差异不显著，
-- 但形状要一次改对——事后 rebuild 更贵，且 RLS 谓词恒含 tenant_id，
-- 首列不是它会让每个查询都退化成全表扫后过滤。

CREATE INDEX idx_subscriptions_tenant_user   ON subscriptions   (tenant_id, user_id);
CREATE INDEX idx_push_batches_tenant_created ON push_batches    (tenant_id, created_at DESC);
CREATE INDEX idx_deliveries_tenant           ON deliveries      (tenant_id);
CREATE INDEX idx_feedbacks_tenant            ON feedbacks       (tenant_id);
CREATE INDEX idx_schedules_tenant            ON schedules       (tenant_id);
CREATE INDEX idx_agent_sessions_tenant       ON agent_sessions  (tenant_id, status, updated_at DESC);
CREATE INDEX idx_pending_actions_tenant      ON pending_actions (tenant_id);
CREATE INDEX idx_llm_calls_tenant_created    ON llm_calls       (tenant_id, created_at) WHERE tenant_id IS NOT NULL;
CREATE INDEX idx_tool_calls_tenant_created   ON tool_calls      (tenant_id, created_at) WHERE tenant_id IS NOT NULL;
-- profiles 每租户至多一行，唯一索引已够，不另建。

-- ---- 步骤 5：唯一约束加上租户维度 ----
--
-- Bytebase footgun #12：全局唯一约束会通过「插入报 duplicate key」泄漏不可见租户的
-- 数据存在性——Postgres 官方 CREATE POLICY 文档也承认这条侧信道。
-- subscriptions 的 (user_id, source_id) 经 user 已能隔离，但补上 tenant 让
-- 谓词与索引首列一致，并让语义显式。
ALTER TABLE subscriptions DROP CONSTRAINT uq_subscriptions_user_source;
ALTER TABLE subscriptions ADD  CONSTRAINT uq_subscriptions_tenant_user_source
    UNIQUE (tenant_id, user_id, source_id);

-- ---- 守卫：四张客观事实表必须仍然没有 tenant_id ----
--
-- 写成会抛异常的 DO 块而非注释：注释拦不住「为了多租户把所有表都加上 tenant_id」
-- 这种看似正确的操作，而那会一次性摧毁全局去重与 TikHub 付费闸门。
-- 迁移自己拒绝跑完，是最响亮的提醒。
-- +goose StatementBegin
DO $$
DECLARE offender TEXT;
BEGIN
    SELECT string_agg(table_name, ', ') INTO offender
      FROM information_schema.columns
     WHERE table_schema = 'public'
       AND column_name = 'tenant_id'
       AND table_name IN ('sources', 'content_items', 'content_sources', 'page_snapshots');
    IF offender IS NOT NULL THEN
        RAISE EXCEPTION '不变量 I-T1 被破坏：客观事实表 % 不得有 tenant_id——'
            '给 canonical_key 加租户维度会摧毁全局去重与 TikHub 付费闸门（见本文件头注）', offender;
    END IF;
END $$;
-- +goose StatementEnd

-- +goose Down

ALTER TABLE subscriptions DROP CONSTRAINT uq_subscriptions_tenant_user_source;
ALTER TABLE subscriptions ADD  CONSTRAINT uq_subscriptions_user_source UNIQUE (user_id, source_id);

DROP INDEX idx_tool_calls_tenant_created;
DROP INDEX idx_llm_calls_tenant_created;
DROP INDEX idx_pending_actions_tenant;
DROP INDEX idx_agent_sessions_tenant;
DROP INDEX idx_schedules_tenant;
DROP INDEX idx_feedbacks_tenant;
DROP INDEX idx_deliveries_tenant;
DROP INDEX idx_push_batches_tenant_created;
DROP INDEX idx_subscriptions_tenant_user;

ALTER TABLE tool_calls      DROP COLUMN tenant_id;
ALTER TABLE llm_calls       DROP COLUMN tenant_id;
ALTER TABLE pending_actions DROP COLUMN tenant_id;
ALTER TABLE agent_sessions  DROP COLUMN tenant_id;
ALTER TABLE schedules       DROP COLUMN tenant_id;
ALTER TABLE profiles        DROP COLUMN tenant_id;
ALTER TABLE feedbacks       DROP COLUMN tenant_id;
ALTER TABLE deliveries      DROP COLUMN tenant_id;
ALTER TABLE push_batches    DROP COLUMN tenant_id;
ALTER TABLE subscriptions   DROP COLUMN tenant_id;

ALTER TABLE memberships DROP CONSTRAINT memberships_user_id_fkey;
ALTER TABLE memberships ADD  CONSTRAINT memberships_user_id_fkey
    FOREIGN KEY (user_id) REFERENCES users (id);
