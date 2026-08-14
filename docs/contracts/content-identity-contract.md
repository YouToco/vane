# 内容身份重构契约（migration 007）

> 事实基准：生产库实测 2026-07-15。设计动机见 Boss 决策：**数据是资产、一律不清理**，
> 留存做①深层需求挖掘 ②信源质量评估。数据模型必须为分析服务——同一篇内容存 N 份，
> 统计口径就全错。

## 0. 为什么要改

### 实测证据（生产库 219 行）

| 现象 | 数据 | 结论 |
|---|---|---|
| 同一篇内容存多份 | 219 行 → 205 个不同 content_hash，**13 组冗余**（1 篇存了 3 份） | `UNIQUE(source_id, external_id)` 没挡住 |
| 冗余组的特征 | **url 全部相同、external_id 各不相同** | BBC 文章更新后 guid 变 → RSS 的稳定身份是 **url** |
| 小红书反过来 | url 带 `xsec_token`（每次搜索新发的临时票据），note_id 稳定 | xhs 的身份是 **note_id** |
| sources.url | 无 UNIQUE 约束，UpsertSource 是 SELECT-then-INSERT | 多用户并发添加同源必产生重复行 |

**没有单一字段能通吃**：用 url 统一会在小红书失败，用 external_id 统一会在 RSS 失败。

### 多用户放大

用户 A 订「小红书: AI编程」、B 订「AI工具」，同一篇笔记命中两个源 →
per-source 唯一挡不住 → 存两份 + **详情补全被付两次钱**（$0.01/次）。

## 1. 身份模型

```
canonical_key = 内容的跨源唯一身份
  rss / exa    → url
  tikhub_xhs   → external_id（note_id）
```

由 **fetcher 构造**（只有它知道源类型），落在 `types.ContentItem.CanonicalKey`。

### 三个语义分层（这是本次重构的核心）

| 层 | 承载 | 语义 |
|---|---|---|
| `content_items.canonical_key` | UNIQUE | 内容身份，全局一份 |
| `content_items.source_id` | 保留 | **首发源**——谁先发现这条内容 |
| `content_sources` | 新表 | 这条内容**出现在哪些源、各自何时** |

**首发源 + 出现时间差 = 信源质量分析的地基**：源 A 比源 B 早 3 小时发同一条内容
→ A 是源头、B 是二手。这正是功能清单 V4「自动信源溯源与升级建议」要的数据。

## 2. migration 007

```sql
-- +goose Up

-- ① sources.url 唯一：多用户并发添加同源时，应用层"查-再插"挡不住竞态
-- （UpsertSource 的 SELECT-then-INSERT），结果是重复源 → 内容双份 + 每轮重复
-- 抓取重复付费。约束加在库里才是真的。实测当前零重复 url，加约束安全。
ALTER TABLE sources ADD CONSTRAINT uq_sources_url UNIQUE (url);

-- ② content_items 身份列
ALTER TABLE content_items ADD COLUMN canonical_key TEXT NOT NULL DEFAULT '';

-- 回填：按源类型分派（见 §1；没有单一字段能通吃）
UPDATE content_items ci SET canonical_key = CASE
    WHEN s.type = 'tikhub_xhs' THEN ci.external_id
    ELSE ci.url
END
FROM sources s WHERE s.id = ci.source_id;

-- 兜底：url 与 external_id 都为空的历史行（理论不存在，NOT NULL DEFAULT '' 下
-- 仍可能）用 id 兜底，避免空串在 UNIQUE 上互撞把它们判成同一条内容。
UPDATE content_items SET canonical_key = 'legacy://' || id WHERE canonical_key = '';

-- ③ 建 content_sources 并从现有数据回填（每行一条 appearance）
CREATE TABLE content_sources (
    content_item_id BIGINT      NOT NULL,
    source_id       BIGINT      NOT NULL,
    -- 该源内的 id 与 url：BBC 的 guid 会变、xhs 的 url 带临时 token，
    -- 都不适合当身份，但作为溯源证据保留。
    external_id     TEXT        NOT NULL,
    url             TEXT        NOT NULL DEFAULT '',
    -- 该源**首次**出现这条内容的时刻。与 content_items.first_fetched_at 的差
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

-- ④ 合并重复内容：同 canonical_key 保留最早一行（id 最小 = 首发），
-- 其余行的 appearance 与投递记录改指向它，然后删除。
-- 顺序不可换：先搬 content_sources、再搬 deliveries、最后删行。
WITH dup AS (
    SELECT id,
           first_value(id) OVER (PARTITION BY canonical_key ORDER BY id) AS keep_id
    FROM content_items
)
UPDATE content_sources cs SET content_item_id = d.keep_id
FROM dup d WHERE cs.content_item_id = d.id AND d.id <> d.keep_id
  -- 目标行已有该源的 appearance 时不搬（PK 冲突），下一步直接删
  AND NOT EXISTS (
      SELECT 1 FROM content_sources x
      WHERE x.content_item_id = d.keep_id AND x.source_id = cs.source_id
  );

DELETE FROM content_sources cs USING (
    SELECT id, first_value(id) OVER (PARTITION BY canonical_key ORDER BY id) AS keep_id
    FROM content_items
) d WHERE cs.content_item_id = d.id AND d.id <> d.keep_id;

WITH dup AS (
    SELECT id, first_value(id) OVER (PARTITION BY canonical_key ORDER BY id) AS keep_id
    FROM content_items
)
UPDATE deliveries dl SET content_item_id = d.keep_id
FROM dup d WHERE dl.content_item_id = d.id AND d.id <> d.keep_id;

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
ALTER TABLE content_items ADD CONSTRAINT uq_content_items_source_external UNIQUE (source_id, external_id);
DROP INDEX IF EXISTS uq_content_items_canonical;
DROP TABLE IF EXISTS content_sources;
ALTER TABLE content_items DROP COLUMN IF EXISTS canonical_key;
ALTER TABLE sources DROP CONSTRAINT IF EXISTS uq_sources_url;
```

**注意 Down 不还原被合并的重复行**——数据合并不可逆，这是刻意的（回滚只需让旧代码能跑）。

## 3. types

```go
// entities.go：ContentItem 追加（ExternalID 之后）
// CanonicalKey 是内容的跨源唯一身份，由 fetcher 按源类型构造（rss/exa=url、
// tikhub_xhs=note_id）。不能用单一字段通吃：BBC 更新文章会换 guid（url 稳定）、
// 小红书 url 带每次刷新的 xsec_token（note_id 稳定），两者恰好相反。
CanonicalKey string `json:"canonical_key"`
```

## 4. store

```go
// UpsertSource 改为 INSERT ... ON CONFLICT (url) DO UPDATE：
// 原 SELECT-then-INSERT 在多用户并发下会建出重复源（约束加了也会直接报错而非
// 静默重复，但报错会让用户添加失败）——ON CONFLICT 让并发添加同源天然收敛到一行。
// DO UPDATE 保持原语义：刷 type/title(COALESCE NULLIF)/config/updated_at，
// 不动抓取状态（next_fetch_at/last_fetched_at/fail_count）与 created_at。
func (s *Store) UpsertSource(ctx context.Context, src *types.Source) (int64, error)

// UpsertContentItem 取代 InsertContentItemIfNew（改名以反映新语义）：
// 按 canonical_key 落内容（全局一份），再登记本次出现的源。
//   INSERT INTO content_items (...) ON CONFLICT (canonical_key) DO NOTHING RETURNING id
//   冲突 → SELECT id FROM content_items WHERE canonical_key = $1
//   然后 INSERT INTO content_sources (...) ON CONFLICT (content_item_id, source_id) DO NOTHING
// isNew 语义不变（内容首次入库=true），调用方据此决定是否补全/记账。
// content_items.source_id 只在首次插入时写=首发源，之后不再改。
func (s *Store) UpsertContentItem(ctx context.Context, item *types.ContentItem) (id int64, isNew bool, err error)

// EnrichedCanonicalKeys 取代 EnrichedExternalIDs：**不再需要 source_id**。
// 净收益：内容全局一份 → 用户 A 的源补全过的笔记，用户 B 的源不用再付 $0.01。
// 判据仍是正文长度而非"行存在"（补全会失败，失败的笔记以 60 字落库，
// 按"存在"跳过会让它终身 60 字——见原 EnrichedExternalIDs 注释）。
func (s *Store) EnrichedCanonicalKeys(ctx context.Context, keys []string, minRunes int) (map[string]struct{}, error)

// ListUnpushedByUser：内容不再直接挂在订阅源下，改经 content_sources 反查。
//   WHERE EXISTS (SELECT 1 FROM content_sources cs
//                 JOIN subscriptions sub ON sub.source_id = cs.source_id
//                 WHERE cs.content_item_id = ci.id AND sub.user_id=$1 AND sub.status='active')
// perSourceCap 仍按 ci.source_id（**首发源**）分区——取舍写进注释：一条内容
// 出现在多个订阅源时只占首发源的配额，语义清晰且不会重复计数；副作用是
// 首发源可能不是用户订的那个（用户仍能收到，配额记在别处），单用户下不可见。
// **必须避免一条内容返回多行**（它可能命中多个订阅源）——用 EXISTS 而非 JOIN。
func (s *Store) ListUnpushedByUser(ctx context.Context, userID int64, limit, perSourceCap int) ([]types.ContentItem, error)

// ListRecentSimhashesByUser：同样改 EXISTS 反查（原按 source_id 直连）。
```

## 5. fetcher

- `mapItems`（rss）/ exa：`CanonicalKey = url`
- `mapTikhubNotes`：`CanonicalKey = n.ID`（note_id）
- **canonical_key 为空时必须跳过该条**（宁可丢一条也不能让空串在 UNIQUE 上互撞、
  把两条无关内容判成同一条）——记 Warn。
- `enrichDescs` 的 seen 查询改 `EnrichedCanonicalKeys(keys, tikhubDetailMinRunes)`，
  keys 用 note_id（= canonical_key）。

## 6. 不做 TTL（Boss 决策 2026-07-15）

001 注释里的「content_items 30 天 TTL」「llm_calls 90 天 TTL」**作废**——数据是资产。
为 TTL 建的两个索引（`idx_deliveries_content_item_id`、`idx_llm_calls_created_at`）
保留但语义改为**分析查询用**。`deliveries.content_item_id` 的 ON DELETE SET NULL
与代码里的"原文已过期清理"分支实际不会触发，保留作防御。
**本次顺带修正 001 里那两处会误导后人的 TTL 注释。**

## 7. 测试要求

- store（DB 门控）：UpsertSource 并发同 url 只产生一行（真并发跑 goroutine）；
  UpsertContentItem 同 canonical_key 跨源只存一份且 content_sources 两行；
  isNew 语义；首发源不被后来的源覆盖；EnrichedCanonicalKeys 的长度判据与跨源命中；
  ListUnpushedByUser 一条内容命中多个订阅源时**只返回一行**、perSourceCap 生效。
- migration：007 在有重复数据的库上跑完后 canonical_key 唯一、deliveries 无悬挂、
  content_sources 覆盖全部 appearance（DB 门控）。
- fetcher：三类源的 canonical_key 构造；空 key 跳过。
