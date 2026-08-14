-- 007_content_identity.sql — 内容身份全局化（契约 docs/content-identity-contract.md）
--
-- 设计要点：
--   1. 身份是 canonical_key（跨源唯一），不是 (source_id, external_id)：实测生产库 219 行
--      → 205 个 content_hash，13 组冗余全是 url 相同 / external_id 各不同（BBC 更新文章换
--      guid）；小红书恰好相反（url 带每次搜索新发的 xsec_token，note_id 稳定）。
--      没有单一字段能通吃，故按源类型分派构造。
--   2. 三个语义分层：content_items.canonical_key = 内容身份（全局一份）；
--      content_items.source_id = **首发源**（谁先发现）；content_sources = 出现在哪些源、各自何时。
--      首发源 + 出现时间差 = 信源质量分析的地基（功能清单 V4「自动信源溯源」要的数据）。
--   3. 不做 TTL（Boss 决策 2026-07-15）：数据是资产，留存做需求挖掘与信源质量评估。
--      001 里那两处 TTL 注释同步作废，索引保留改作分析查询用。
--
-- 语句顺序不可换：先搬 content_sources、再搬 deliveries、最后删行、最后建 UNIQUE。
-- 提前删行会丢掉"这个源见过这条内容"的证据；提前建 UNIQUE 会撞上尚未合并的重复行。

-- +goose Up

-- ① sources.url 唯一：多用户并发添加同源时，应用层"查-再插"挡不住竞态
-- （UpsertSource 的 SELECT-then-INSERT），结果是重复源 → 内容双份 + 每轮重复
-- 抓取重复付费。约束加在库里才是真的。实测当前零重复 url，加约束安全。
ALTER TABLE sources ADD CONSTRAINT uq_sources_url UNIQUE (url);

-- ② content_items 身份列
ALTER TABLE content_items ADD COLUMN canonical_key TEXT NOT NULL DEFAULT '';

-- 回填：按源类型分派（见契约 §1；没有单一字段能通吃）。
-- **TRIM 不可省**：运行时 fetcher.CanonicalKey 对两个分支都做 TrimSpace，回填若不做，
-- 存量里任何带首尾空白的 url/external_id 都会算出与运行时不同的键 → 该内容被当新的
-- 再落一份 → 正是本迁移要消灭的重复。当前生产库实测 0 行需要 trim（`url <> TRIM(url)`），
-- 但这条对齐是**契约级约束**：改 fetcher 的归一化必须同步改这里，反之亦然。
UPDATE content_items ci SET canonical_key = CASE
    WHEN s.type = 'tikhub_xhs' THEN TRIM(ci.external_id)
    ELSE TRIM(ci.url)
END
FROM sources s WHERE s.id = ci.source_id;

-- 兜底：键为空的历史行（trim 后为空，或 url/external_id 本就是空串）用 id 兜底。
-- 不兜底的话，多行空串键会在下面的 UNIQUE 上互撞——把两条毫不相干的内容当成
-- 同一条合并掉，且不可逆。
UPDATE content_items SET canonical_key = 'legacy://' || id WHERE canonical_key = '';

-- ③ 建 content_sources 并从现有数据回填（每行一条 appearance）
CREATE TABLE content_sources (
    content_item_id BIGINT      NOT NULL,
    source_id       BIGINT      NOT NULL,
    -- 该源内的 id 与 url：BBC 的 guid 会变、xhs 的 url 带临时 token，
    -- 都不适合当身份，但作为溯源证据保留。
    external_id     TEXT        NOT NULL,
    url             TEXT        NOT NULL DEFAULT '',
    -- 该源**首次**出现这条内容的时刻。与 content_items.fetched_at 的差
    -- 就是该源相对源头的滞后量——信源质量分析的核心指标。
    first_seen_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (content_item_id, source_id),
    CONSTRAINT fk_content_sources_item   FOREIGN KEY (content_item_id) REFERENCES content_items (id) ON DELETE CASCADE,
    CONSTRAINT fk_content_sources_source FOREIGN KEY (source_id)       REFERENCES sources (id)
);
-- 按源反查其承载的内容（信源质量分析 + ListUnpushedByUser 的 EXISTS 子查询）
CREATE INDEX idx_content_sources_source ON content_sources (source_id, first_seen_at DESC);

INSERT INTO content_sources (content_item_id, source_id, external_id, url, first_seen_at)
SELECT id, source_id, external_id, url, fetched_at FROM content_items
ON CONFLICT DO NOTHING;

-- ④ 合并重复内容：同 canonical_key 保留 id 最小的一行（= 最早入库 = 首发）。
--
-- 顺序不可换，且每一步都踩过一个真实的坑（均在 postgres:18-alpine 上复现过）：
--   1. 先把非 keep 行的 appearance 并进 keep —— **必须 INSERT…SELECT 而不是 UPDATE**。
--      原写法 `UPDATE content_sources SET content_item_id=keep WHERE NOT EXISTS(keep 已有该源)`
--      的 NOT EXISTS 读的是**语句开始时的快照**，看不见同一条语句里正在被搬的兄弟行：
--      同一 canonical 组里有 ≥2 个非 keep 行来自同一个源时，两行都通过 NOT EXISTS、
--      双双 SET 成 (keep_id, 同一 source) → 撞 PRIMARY KEY → 整个迁移回滚。
--      DISTINCT ON 在语句内先收敛到每 (keep, source) 一行，ON CONFLICT 再挡 keep 已有的。
--   2. 再把 deliveries / llm_calls 的引用指过去（各有各的坑，见下）。
--   3. 最后删非 keep 行 —— 它们残留的 appearance 由 content_sources 的
--      ON DELETE CASCADE 自动清掉，不必显式 DELETE。
WITH dup AS (
    SELECT id, first_value(id) OVER (PARTITION BY canonical_key ORDER BY id) AS keep_id
    FROM content_items
)
INSERT INTO content_sources (content_item_id, source_id, external_id, url, first_seen_at)
SELECT DISTINCT ON (d.keep_id, cs.source_id)
       d.keep_id, cs.source_id, cs.external_id, cs.url, cs.first_seen_at
FROM content_sources cs
JOIN dup d ON d.id = cs.content_item_id
WHERE d.id <> d.keep_id
-- 同 (keep, source) 多条时取 first_seen_at 最早的：该源"第一次见到这条内容"
-- 才是信源滞后量要的时刻。
ORDER BY d.keep_id, cs.source_id, cs.first_seen_at
ON CONFLICT (content_item_id, source_id) DO NOTHING;

-- deliveries 重指向。**不能无条件 UPDATE**：004 建了
-- `uq_deliveries_batch_content (batch_id, content_item_id) WHERE content_item_id IS NOT NULL`。
-- 旧 schema 下同一批次投递两条重复内容完全合法（它们是两个不同的 content_item_id），
-- 合并后双双变成 (batch_id, keep_id) → 撞唯一索引 → 迁移回滚。
-- 触发条件真实：BBC 改稿重发换 guid，改动大到 simhash 距离超阈值时两条同批存活同批投递。
-- 处置：每个 (batch, keep) 只留最早一条指向 keep，其余置 NULL —— **不删行**
-- （数据是资产；deliveries 的 card_json/body_md 仍保留着当时推了什么，
-- 且 content_item_id 可空是既有语义，代码已有"原文已过期清理"分支）。
WITH dup AS (
    SELECT id, first_value(id) OVER (PARTITION BY canonical_key ORDER BY id) AS keep_id
    FROM content_items
),
-- 注意：dup 覆盖**全部**行（keep 自身的 keep_id 就是自己），所以 keep 原有的
-- 投递也进入排名，不会出现"新搬来的撞上 keep 自己那条"。
ranked AS (
    SELECT dl.id AS delivery_id, d.keep_id,
           row_number() OVER (PARTITION BY dl.batch_id, d.keep_id ORDER BY dl.id) AS rn
    FROM deliveries dl
    JOIN dup d ON d.id = dl.content_item_id
)
UPDATE deliveries dl
SET content_item_id = CASE WHEN r.rn = 1 THEN r.keep_id ELSE NULL END
FROM ranked r WHERE dl.id = r.delivery_id;

-- 合并前先把"最长的正文"抬到 keep 行上：keep 是 id 最小者（首发），但首发不等于
-- 正文最全——同一篇 BBC 文章首发时是摘要、改稿重发时正文更长；小红书更常见：
-- 某个源那条补全成功（2000 字）、另一个源那条卡在 60 字残句，而 keep 可能恰是后者。
-- 直接删非 keep 行会把库里已有的、花过钱的最好版本丢掉。
-- 与 UpsertContentItem 的"更长的正文赢"同一条规则，迁移与运行时必须一致。
-- content_hash/simhash 跟着正文一起换：它们是正文的派生指纹，只换正文会让精确去重
-- 与近似去重都按旧版本判、静默失准。
WITH dup AS (
    SELECT id, canonical_key,
           first_value(id) OVER (PARTITION BY canonical_key ORDER BY id) AS keep_id
    FROM content_items
),
best AS (
    SELECT DISTINCT ON (d.keep_id)
           d.keep_id, ci.content, ci.content_hash, ci.simhash
    FROM dup d JOIN content_items ci ON ci.id = d.id
    ORDER BY d.keep_id, char_length(ci.content) DESC, ci.id
)
UPDATE content_items k
SET content = b.content, content_hash = b.content_hash, simhash = b.simhash
FROM best b
WHERE k.id = b.keep_id AND char_length(k.content) < char_length(b.content);

-- llm_calls 重指向。它是 content_items.id 的**第三个引用者**，且刻意不建 FK
-- （001：ref_type + ref_id 多态关联）——正因为没有 FK，"grep 外键"排查必然漏掉它，
-- 删行时 DB 既不报错也不级联，失效完全静默。活跃写入路径：scorer / cardgen /
-- feedback.deepdive 都以 RefType=content_item + RefID=content_items.id 记账。
-- 无唯一约束，直接搬即可。
WITH dup AS (
    SELECT id, first_value(id) OVER (PARTITION BY canonical_key ORDER BY id) AS keep_id
    FROM content_items
)
UPDATE llm_calls lc SET ref_id = d.keep_id
FROM dup d
WHERE lc.ref_type = 'content_item' AND lc.ref_id = d.id AND d.id <> d.keep_id;

DELETE FROM content_items ci USING (
    SELECT id, first_value(id) OVER (PARTITION BY canonical_key ORDER BY id) AS keep_id
    FROM content_items
) d WHERE ci.id = d.id AND d.id <> d.keep_id;

-- ⑤ 身份唯一（必须在去重之后）
CREATE UNIQUE INDEX uq_content_items_canonical ON content_items (canonical_key);

-- 旧的 per-source 唯一约束退役：它把"源内 id"当身份，正是本次要修的错。
-- 源内 id 的唯一性移交 content_sources 的 PK。
ALTER TABLE content_items DROP CONSTRAINT uq_content_items_source_external;

-- +goose Down
--
-- **回滚本质上不可靠，Down 只服务开发期的 down/up 循环。**
-- 007 之后 (source_id, external_id) 的唯一性再无人维护（UpsertContentItem 只按
-- canonical_key 收敛），新代码写过数据后旧约束必然建不回来（实测
-- ERROR: Key (source_id, external_id)=(1, guid-x) is duplicated）——而旧代码的
-- ON CONFLICT (source_id, external_id) 又必须有它。生产回滚请用备份恢复。
-- 被合并的重复行也不还原（数据合并不可逆）。
DROP INDEX IF EXISTS uq_content_items_canonical;
DROP TABLE IF EXISTS content_sources;
ALTER TABLE content_items DROP COLUMN IF EXISTS canonical_key;
ALTER TABLE sources DROP CONSTRAINT IF EXISTS uq_sources_url;
-- best-effort 还原旧约束：数据仍满足旧唯一性时（刚迁完就回滚）能成功，
-- 否则告警跳过而不是让 Down 整体失败——失败会让库卡在半迁状态，比缺个约束更糟。
DO $$ BEGIN
    ALTER TABLE content_items ADD CONSTRAINT uq_content_items_source_external UNIQUE (source_id, external_id);
EXCEPTION WHEN OTHERS THEN
    RAISE WARNING '(source_id, external_id) 已不唯一，跳过还原旧约束；旧代码将无法写入，请用备份恢复';
END $$;
