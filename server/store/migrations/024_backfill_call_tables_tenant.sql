-- 024: 补回填 llm_calls / tool_calls 的 tenant_id —— 收拾 021 留下的那道缝
--
-- 021 给这两张表加了 tenant_id 并回填了当时的存量，但**没改 INSERT**（vane#87 修）。
-- 于是 021 的回填成了一次性快照：从 021 部署（2026-07-18 18:15）到 #87 部署
-- （2026-07-19 06:54）之间新写入的每一行，tenant_id 都是 NULL。
--
-- 本迁移只做一件事：把那段窗口里漏掉的行补上。用的是和 021 步骤 2 逐字相同的语句
-- ——不是复制粘贴省事，而是**归属规则只能有一处定义**：行归属于其所有者所在的租户。
-- 若将来规则变了（比如一个用户可属多个租户），这两处必须一起改，形状相同才看得出来。
--
-- 为什么不是一次性手工 SQL：手工跑过的东西在别的环境里不存在，也没人知道跑没跑过。
-- 迁移是有版本、可重放、能在 goose_db_version 里查证的。新库上它是无害的空操作
-- （没有 NULL 行可补），这正是幂等该有的样子。
--
-- user_id 为空的行**不在回填范围**：那是真的系统级调用（021 步骤 1 的说明），
-- 它们的 NULL 是真话，不是缺失。填成任何租户都是在编造归属。

-- +goose Up

UPDATE llm_calls  t SET tenant_id = m.tenant_id FROM memberships m WHERE m.user_id = t.user_id AND t.tenant_id IS NULL;
UPDATE tool_calls t SET tenant_id = m.tenant_id FROM memberships m WHERE m.user_id = t.user_id AND t.tenant_id IS NULL;

-- +goose Down

-- 不可逆：回填后无法区分「本迁移填的」与「021 填的」与「#87 之后 INSERT 写的」。
-- 把它们一律清回 NULL 会连带毁掉正确数据，比不回滚危险得多。
-- 真要退，退的是代码（#87），不是这些行。
SELECT 1;
