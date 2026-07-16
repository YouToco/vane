-- 009_empty_batch_exit.sql — 空批次可见化（M5 契约 §16 修订记录「空批次缺口（归 PR2）」）
--
-- 背景：pipeline 有五处提前退出（见 workflow/workflow.go 里五处 recordEmpty 调用——无新内容 /
-- 去重后空 / 打分后空 / 择优后空 / 卡片生成后空）全部在 Push 之前 `return nil`；而在 009 之前，
-- 生产路径上写 push_batches 的只有 Push 活动内的 store.CreatePushBatchIdempotent 一处
-- （store.CreatePushBatch 那条 INSERT 彼时与现在都只有测试在用）。后果是五种语义完全不同的
-- 结局塌缩成同一个"库里什么都没有"：Boss 问"今早为什么没推送"时，那件事**在库里根本不存在**，
-- 不是查询查不到。本迁移给这些结局一个落脚点，并自己成为第三处 INSERT
-- （store.RecordEmptyPushBatch）。
--
-- 本文件引用代码一律用符号锚点（函数名/调用名）而非行号：迁移落地后永不再改，是仓库里最长寿的
-- 文档，而它引用的代码会一直漂。009 初稿正是照着改动前的行号写的，PR 自身插入代码后当场失效。
-- 指向迁移文件自身的行号（001/004/006）不在此列——那些文件同样冻结，坐标不会漂。
--
-- 两列而非一列：
--   exit_gate    = 从哪个闸门退出（主判别键，回答"为什么没推"）
--   stage_counts = 各阶段跑完后还剩几条（展开细节，回答"差在哪一步、差多少"）
-- 前者定性、后者定量。只有 exit_gate 就分不出"抓到 20 条全被去重"与"抓到 1 条被去重"；
-- 只有 stage_counts 就得让读者自己从漏斗里反推闸门，而反推是会错的（见 types 注释）。
--
-- 不加 CHECK、不加触发器：沿用 001 的约定（001:13-15、006:8）——枚举值由应用层校验。
-- status='empty' 的合法性、exit_gate 非空、stage_counts 的形状统一由
-- store.RecordEmptyPushBatch 一处守卫（写入口本就只有它一个）。
--
-- 不加索引：唯一的查询形态是"某用户最近 N 天的批次"（store.ListPushBatchSummaries），
-- 已被 001 的 idx_push_batches_user_created (user_id, created_at DESC) 完全覆盖；
-- 按 exit_gate 分组是在那个结果集（单用户、14 天、几十行）上做的，再建索引是给
-- 优化器添堵。等真出现跨用户按闸门扫全表的查询再说。
--
-- +goose Up

-- 退出闸门。DEFAULT '' 的语义是"没有提前退出（跑到了 Push）"——009 之前的历史行
-- **全部**如此（它们能存在就是因为 Push 建了行），故默认值恰好就是它们的真实语义，
-- 无需任何回填。这是刻意挑的默认值，不是图省事：若把 '' 定义成"未知"，历史行就会
-- 从"已知推过"退化成"不知道"，凭空制造一片查不清的灰色地带。
ALTER TABLE push_batches ADD COLUMN exit_gate TEXT NOT NULL DEFAULT '';

-- 漏斗计数。DEFAULT '{}' = 各阶段计数缺席 = "没记录"，对历史行同样是真话。
-- NOT NULL + '{}' 而非可空：读侧 json.Unmarshal 不必先判 NULL，空对象直接解成
-- 全 nil 的 PipelineCounts，与"这些阶段没记录"同义，一条路径通吃。
ALTER TABLE push_batches ADD COLUMN stage_counts JSONB NOT NULL DEFAULT '{}';

-- +goose Down
--
-- **回滚只还原 schema，不还原数据——回滚后的库比 007 更糟，不是等于 007。**
-- 实测 down/re-up 循环确认：DROP COLUMN 删的是两个列，而 009 期间写下的
-- status='empty' 那些**行留在表里**，退化成"status 是个旧代码不认识的值、
-- 且说不出为什么没推"的孤儿——恰恰是本迁移立项要消灭的那种记录。
-- 红线 5（数据是资产不清理）在这里的含义是：这些行不该被删，所以本 Down 也不删它们；
-- 代价就是回滚不干净。这与 007 Down 的处境同源（见 007 的 Down 段）。
--
-- 若真要回滚，先决定这些行怎么办（改回 pending？导出留档？），再执行本段。
-- 别指望 goose down 一把回到干净状态——它做不到，本注释就是为了让你别这么指望。
ALTER TABLE push_batches DROP COLUMN IF EXISTS stage_counts;
ALTER TABLE push_batches DROP COLUMN IF EXISTS exit_gate;
