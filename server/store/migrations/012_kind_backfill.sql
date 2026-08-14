-- 012_kind_backfill.sql — 回填 content_items.kind 的空串污染（M6 契约 §3.3.1 / §7.2(b)）
--
-- 编号取 012 而非紧随事故源头 008：011（page_snapshots）已先合入 main 并随
-- push-to-main 自动部署应用到生产，goose provider 默认拒绝乱序迁移（低于库内
-- 已应用最大版本号的新迁移会让启动迁移直接报错），故只能顺延取空号。
--
-- 背景：008 给 content_items 加了 kind 列（NOT NULL DEFAULT 'article'），同时
-- store.UpsertContentItem 的 INSERT 显式写入 item.Kind——但四个 article 抓取器
-- （rss / exa / tikhub_xhs / x）都没给 Kind 赋过值，Go 零值 "" 被显式写入、覆盖了
-- 列默认值。自 008 在生产应用起，这四类新抓内容的 kind 都是空串，且按红线 5
-- （数据是资产不清理）在永久累积。空串对下游的语义等同于"未知"：workflow Dedup
-- 的 change 豁免（item.Kind == KindChange）判不出 article，将来任何按 kind 分派
-- 的读路径都会漏掉这些行。
--
-- 回填为 'article' 是无损且唯一正确的选择：kind='' 的行只可能来自上述四个 article
-- 抓取器——change 的唯一产出方 page_watch（011 同期上线）在构造 item 处就显式赋
-- KindChange，其行落库即为 'change'，不会产生空串行。回填不会把真 change 错标。
--
-- 写入侧的根治在同一 PR 的 Go 代码里：四个抓取器构造 item 处显式赋 KindArticle，
-- fetcher.finalize 对空 Kind 一律拒绝。本迁移只管已经落库的存量。
-- 不加 CHECK 约束：沿用 001/009 的约定，枚举值由应用层校验（finalize 即守卫）。

-- +goose Up
UPDATE content_items SET kind = 'article' WHERE kind = '';

-- +goose Down
-- 纯数据回填、无 schema 变更，Down 无事可做：把 kind 改回空串只是重新制造本迁移
-- 要消灭的污染，且回填后已无法区分"被回填的行"与"回填后新写入的 article 行"。
-- 与 009 Down 的处境同源（见 009）：回滚不还原数据。
