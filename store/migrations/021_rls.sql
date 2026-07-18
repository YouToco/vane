-- 021: 行级安全（RLS）—— 租户隔离的**兜底层**（企业级契约 §2.2）
--
-- 前面几步保证的是「写对」：tenant_id 由 memberships 推导、三处路径一致地拒绝猜测。
-- 但读取仍靠应用层记得加过滤条件，而**「忘了加 WHERE」是这类系统最典型的漏数据方式**
-- ——它不报错、不崩溃，只是安静地多返回几行别人的数据。
--
-- RLS 把这条防线下沉到数据库：即使 Go 代码漏了谓词，数据库也不交出别的租户的行。
--
-- ============================================================
-- 本迁移**不激活**到生产路径，只是把策略建好并使其可被证明
-- ============================================================
-- 生产以角色 vane 连接，而 vane 是这些表的 owner —— Postgres 的 owner 默认**绕过**
-- RLS（除非显式 FORCE）。因此本迁移对现有生产行为零影响。
--
-- 这是刻意的分步：RLS 一旦对当前连接生效、而租户上下文没设，**所有查询会返回零行**
-- ——整个服务安静地变成空壳。所以顺序必须是「先建好并证明有效 → 再切换连接角色」，
-- 而不是一次做完。切换是下一步的事（需要 tenantdb 的事务内 set_config 全面铺开）。
--
-- 策略的正确性由 store/rls_test.go 证明：它 SET LOCAL ROLE 到受限角色，
-- 在真库上验证跨租户读写确实被拦。

-- +goose Up

-- ---- 受限角色 ----
--
-- 不给 LOGIN：连接仍由 owner 建立，业务代码在事务内 `SET LOCAL ROLE vane_app`
-- 切进来。好处是**不用管理第二套数据库密码**，且 SET LOCAL 随事务自动还原，
-- 不会像连接级设置那样污染连接池里的连接（pgx 作者对 AfterConnect 的裁定同理）。
--
-- NOBYPASSRLS 是显式写出来的：默认值就是它，但这一条是整个机制的地基，
-- 值得写明而不是依赖默认。
-- +goose StatementBegin
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'vane_app') THEN
        CREATE ROLE vane_app NOLOGIN NOBYPASSRLS;
    END IF;
END $$;
-- +goose StatementEnd

-- 受限角色需要能读写业务表；DDL 与迁移仍归 owner。
GRANT USAGE ON SCHEMA public TO vane_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO vane_app;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO vane_app;

-- ---- 策略 ----
--
-- 每张租户所有表两条策略，缺一不可：
--
--   1. PERMISSIVE `tenant_visible`：授予访问。Postgres 规定**只有 RESTRICTIVE 策略
--      时没有任何行可访问**（restrictive 只做 AND 收窄，不授予），所以必须有它。
--   2. RESTRICTIVE `tenant_isolation`：租户谓词。RESTRICTIVE 语义是 AND——
--      这意味着**将来任何人新增 PERMISSIVE 策略都无法放宽它**。若只写一条 permissive
--      的租户策略，后人加一条 `USING (true)` 的策略就会把隔离整个抹掉，且看起来像是
--      在「加一个新功能的可见性」。
--
-- 三个签名级细节（缺任何一个都会让策略静默失效或退化）：
--
--   a. `current_setting('app.tenant_id', true)` 的第二参 missing_ok=true：
--      未设置该 GUC 时返回 NULL 而不是抛错。
--
--   a'. 比较用 `IS NOT DISTINCT FROM` 而非 `=`。这一条是「想清楚怎么激活」时才发现的：
--      llm_calls / tool_calls 的 tenant_id 可空（系统级调用无归属租户），而 `=` 对 NULL
--      恒为非真——于是**系统级调用的记账写入会被 WITH CHECK 拒绝**，一旦激活 RLS，
--      后台任务的记账会全线失败（已在真库复现：ERROR new row violates row-level
--      security policy）。IS NOT DISTINCT FROM 让 NULL 匹配 NULL，语义正好对上：
--        · 无租户上下文 + 行无租户 → 可见可写（系统路径正常）
--        · 无租户上下文 + 行有租户 → 不可见（fail-closed 不变）
--        · 有租户上下文 + 行是本租户 → 可见
--        · 有租户上下文 + 行无租户 → 不可见（租户看不到系统级记录）
--      对 8 张 NOT NULL 的表行为不变：无上下文时 `非空 IS NOT DISTINCT FROM NULL`
--      恒为假，依然 fail-closed。
--
--   b. `(SELECT ...)` 包一层：不包的话表达式被当作 SubPlan **逐行求值**；
--      包了会被 planner 提升为 InitPlan，整个查询只求值一次。
--      PlanetScale 实测 cost 34,828 → 10,095、latency 1.96s → 102ms。
--      注意：即使函数标了 STABLE 也仍然需要包。
--
--   c. `WITH CHECK` 必须显式写：FOR ALL 时缺 WITH CHECK 会用 USING 顶替，
--      看似等价——但一旦将来拆成 FOR INSERT 单独策略，INSERT **不认 USING**，
--      漏写就等于允许租户写入标着别人 tenant_id 的行。写全了不会有这个坑。

-- +goose StatementBegin
DO $$
DECLARE t TEXT;
BEGIN
    FOREACH t IN ARRAY ARRAY[
        'subscriptions', 'push_batches', 'deliveries', 'feedbacks',
        'profiles', 'schedules', 'agent_sessions', 'pending_actions'
    ] LOOP
        EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY', t);
        EXECUTE format(
            'CREATE POLICY tenant_visible ON %I FOR ALL USING (true) WITH CHECK (true)', t);
        EXECUTE format(
            'CREATE POLICY tenant_isolation ON %I AS RESTRICTIVE FOR ALL '
            'USING (tenant_id IS NOT DISTINCT FROM (SELECT current_setting(''app.tenant_id'', true))::bigint) '
            'WITH CHECK (tenant_id IS NOT DISTINCT FROM (SELECT current_setting(''app.tenant_id'', true))::bigint)', t);
    END LOOP;
END $$;
-- +goose StatementEnd

-- llm_calls / tool_calls 的 tenant_id 可空（系统级调用无归属租户）。
-- 正因如此才必须用 IS NOT DISTINCT FROM（见上方细节 a′）——用 = 的话，
-- 系统级记账在 RLS 下会全线写入失败。
-- +goose StatementBegin
DO $$
DECLARE t TEXT;
BEGIN
    FOREACH t IN ARRAY ARRAY['llm_calls', 'tool_calls'] LOOP
        EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY', t);
        EXECUTE format(
            'CREATE POLICY tenant_visible ON %I FOR ALL USING (true) WITH CHECK (true)', t);
        EXECUTE format(
            'CREATE POLICY tenant_isolation ON %I AS RESTRICTIVE FOR ALL '
            'USING (tenant_id IS NOT DISTINCT FROM (SELECT current_setting(''app.tenant_id'', true))::bigint) '
            'WITH CHECK (tenant_id IS NOT DISTINCT FROM (SELECT current_setting(''app.tenant_id'', true))::bigint)', t);
    END LOOP;
END $$;
-- +goose StatementEnd

-- 共享事实表（sources / content_items / content_sources / page_snapshots）**不加 RLS**：
-- 它们本就是跨租户共享的客观事实（不变量 I-T1）。给它们加租户策略等于让每个租户
-- 只看得见自己抓来的内容，那会一次性摧毁全局去重与 TikHub 付费闸门。

-- +goose Down

-- +goose StatementBegin
DO $$
DECLARE t TEXT;
BEGIN
    FOREACH t IN ARRAY ARRAY[
        'subscriptions', 'push_batches', 'deliveries', 'feedbacks',
        'profiles', 'schedules', 'agent_sessions', 'pending_actions',
        'llm_calls', 'tool_calls'
    ] LOOP
        EXECUTE format('DROP POLICY IF EXISTS tenant_isolation ON %I', t);
        EXECUTE format('DROP POLICY IF EXISTS tenant_visible ON %I', t);
        EXECUTE format('ALTER TABLE %I DISABLE ROW LEVEL SECURITY', t);
    END LOOP;
END $$;
-- +goose StatementEnd

REVOKE ALL ON ALL SEQUENCES IN SCHEMA public FROM vane_app;
REVOKE ALL ON ALL TABLES IN SCHEMA public FROM vane_app;
REVOKE USAGE ON SCHEMA public FROM vane_app;
