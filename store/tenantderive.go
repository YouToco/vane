package store

// tenantOfUser 是「一行数据归属于其所有者所在的租户」这条规则的 SQL 表达。
//
// 为什么在 SQL 里推导，而不是给每个写入函数加一个 tenantID 形参：
//
//  1. **不可能传错组合**。加形参意味着每个调用点都要自己取 tenant，而取错、
//     取成别人的、或从过时的上下文里取——都是编译期看不出来的错。
//     从 memberships 推导则让 (tenant, user) 恒一致，错配无法表达。
//  2. **规则只写一处**。migration 021 的回填用的是同一条规则；若写入侧另写一份，
//     两份迟早漂移，而漂移的表现是「新行的租户归属和历史行不一样」——极难发现。
//  3. **将来会在正确的时刻响亮失败**。一个用户加入多个租户后，本子查询会因
//     「more than one row returned by a subquery」而报错，正好逼出「此处必须显式
//     指定租户」的改造。静默取第一行才是灾难。
//
// 用法：把它拼进 INSERT 的 VALUES 里，参数位与 user_id 用同一个占位符。
const tenantOfUser = `(SELECT m.tenant_id FROM memberships m WHERE m.user_id = `
