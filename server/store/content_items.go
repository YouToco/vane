package store

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/server/types"
)

// UpsertContentItem 按 canonical_key 全局去重地落内容，并登记本次出现的源。
// 返回 (id, isNew, err)：内容**首次入库**返回新 id 且 isNew=true；已存在（含被别的源
// 先发现过）返回其 id 且 isNew=false。调用方据 isNew 决定是否补全/记账。
//
// 取代 InsertContentItemIfNew：旧的 (source_id, external_id) 唯一把"源内 id"当身份，
// 而实测两个方向都挡不住——BBC 更新文章换 guid（同一篇存 3 份）、同一篇笔记命中
// 不同任务目标（存两份且详情补全被付两次钱）。身份改由 canonical_key 承载。
//
// content_items.source_id 是 legacy **首发源**。V2 Source-free 可能先以 NULL 发现内容；
// 第一个 retained V1 appearance 会用 COALESCE 原子补上正数，之后不再覆盖。后来的源
// 只在 content_sources 里追加 appearance。
//
// first_seen_at / fetched_at 都不由 Go 传入而走 DB 默认 now()：两者的差是信源滞后量
// （信源质量分析的核心指标），必须同一个时钟，混入应用侧时间会让差值含机器间时钟偏移。
//
// 两条 INSERT 刻意不包在事务里（与本包其它方法一致，全包无 tx）：中途失败最坏是内容
// 已落库但本次 appearance 未登记，下一轮抓取会 ON CONFLICT DO NOTHING 地补上，可自愈。
func (s *Store) UpsertContentItem(ctx context.Context, item *types.ContentItem) (id int64, isNew bool, err error) {
	isNew = true
	err = s.pool.QueryRow(ctx,
		`INSERT INTO content_items (
			source_id, external_id, canonical_key, url, title, content, author,
			published_at, content_hash, simhash, kind
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		ON CONFLICT (canonical_key) DO NOTHING
		RETURNING id`,
		item.SourceID, item.ExternalID, item.CanonicalKey, item.URL, item.Title, item.Content,
		item.Author, item.PublishedAt, item.ContentHash, item.Simhash, item.Kind,
	).Scan(&id)
	if err != nil {
		if !errors.Is(err, pgx.ErrNoRows) {
			return 0, false, types.NewAppError(types.CodeDatabase,
				fmt.Sprintf("插入内容条目（canonical_key=%s）", item.CanonicalKey), err)
		}
		// 命中身份唯一：内容已存在（可能是别的源先发现的），补查其 id，isNew=false。
		isNew = false
		if qerr := s.pool.QueryRow(ctx,
			`SELECT id FROM content_items WHERE canonical_key = $1`,
			item.CanonicalKey).Scan(&id); qerr != nil {
			return 0, false, types.NewAppError(types.CodeDatabase,
				fmt.Sprintf("回查既有内容条目（canonical_key=%s）", item.CanonicalKey), qerr)
		}
	}

	// 更长的正文赢：ON CONFLICT DO NOTHING 只保住身份与首发源，代价是正文被
	// **第一次写入的版本永久冻结**——而全仓没有任何别的 UPDATE content 路径。
	// 这会打穿 EnrichedCanonicalKeys 承诺的自愈：源 A 补全 429 失败 → 60 字落库 →
	// 下轮某源再抓到、闸门判"未补全"→ 再付 $0.01 → 这次拿到 2000 字全文 →
	// DO NOTHING 丢弃 → 库里仍是 60 字 → 永远循环、每源每轮重复付费。
	// 007 把这从"per-source 可自限"恶化成"全局不可修复"，故必须补这一步。
	//
	// 只在**新正文更长**时覆盖：长度是"信息量"的可靠代理（截断残句必短于全文），
	// 且天然幂等——重复抓到同一版本时 char_length 相等、不触发写。
	// content_hash / simhash 必须跟着一起换：它们是正文的派生指纹，只换正文会让
	// 精确去重与近似去重都按旧版本判，静默失准。
	// 不动 source_id（首发源）、不动 title（标题来自搜索结果，详情未必更好）。
	if !isNew {
		if _, uerr := s.pool.Exec(ctx,
			`UPDATE content_items
			    SET source_id=COALESCE(source_id,$2)
			  WHERE id=$1`,
			id, item.SourceID); uerr != nil {
			return 0, false, types.NewAppError(types.CodeDatabase,
				fmt.Sprintf("固化内容首个 legacy 信源（content_item=%d）", id), uerr)
		}
		if _, uerr := s.pool.Exec(ctx,
			`UPDATE content_items
			 SET content = $2, content_hash = $3, simhash = $4
			 WHERE id = $1 AND char_length(content) < char_length($2)`,
			id, item.Content, item.ContentHash, item.Simhash); uerr != nil {
			// 正文补写失败不该让整条入库失败：身份已落、appearance 待登记，
			// 内容留在旧版本下轮还能再补（闸门按长度判，见 EnrichedCanonicalKeys）。
			slog.Warn("store: 内容正文补写失败，保留既有版本",
				"content_item_id", id, "canonical_key", item.CanonicalKey, "err", uerr)
		}
	}

	// 登记 appearance：无论内容是不是新的都要写——"这个源见过这条内容"正是跨源
	// 场景下任务能收到它的唯一凭据（经 content_sources 反查）。
	// 同源重复抓到时 DO NOTHING，保住首次出现的 first_seen_at 不被刷新。
	if _, cerr := s.pool.Exec(ctx,
		`INSERT INTO content_sources (content_item_id, source_id, external_id, url)
		 VALUES ($1, $2, $3, $4)
		 ON CONFLICT (content_item_id, source_id) DO NOTHING`,
		id, item.SourceID, item.ExternalID, item.URL); cerr != nil {
		return 0, false, types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("登记内容出现的源（content_item=%d, source=%d）", id, item.SourceID), cerr)
	}
	return id, isNew, nil
}

// EnrichedCanonicalKeys 返回 keys 里"已入库**且正文长于 minRunes**"的那部分。
//
// 存在的理由是成本：抓取器据此跳过不必再花钱的笔记——上游详情接口按次计费
// （$0.01/次），把已经拿到全文的老笔记每轮再补一遍纯属白烧。
//
// 取代 EnrichedExternalIDs 的 **不再需要 source_id**：内容按 canonical_key 全局一份，
// 用户 A 的源补全过的笔记，用户 B 的源查同一个 key 就能命中、不用再付 $0.01。
// 旧签名按 source_id 隔离，跨源时必然 miss、必然重复付费。
//
// 判据是**正文长度**而不是"行是否存在"，这个区别是刻意的：详情补全会失败
// （429/网络抖动/上游风控），失败时笔记仍以搜索摘要（≤60 rune 的半句话）落库。
// 若按"行存在"跳过，一次瞬时 429 就让这条笔记**终身停在 60 字**，且系统再无自愈
// 路径——而下游此时已把它判死（scorer 压到 0-20、deep_dive 闸门直接拒）。
// 按长度判，则下一轮自然重试；代价是极少数"真实全文恰好 ≤minRunes"的笔记会在它
// 还出现在搜索结果里的一两天内被重复补全（每次 $0.01），量级可忽略。
//
// 去重本身仍由 UNIQUE(canonical_key) 在 UpsertContentItem 里兜底，
// 本方法漏判/多判都不会造成脏数据，最坏只是多花/少花几分钱。
//
// 空入参直接返回空 map 不查库：抓取器每轮都调，空批次不该白跑一次 DB 往返。
func (s *Store) EnrichedCanonicalKeys(ctx context.Context, keys []string, minRunes int) (map[string]struct{}, error) {
	out := make(map[string]struct{}, len(keys))
	if len(keys) == 0 {
		return out, nil
	}
	// char_length 数的是字符不是字节，与调用方的 utf8.RuneCountInString 同口径。
	rows, err := s.pool.Query(ctx,
		`SELECT canonical_key FROM content_items
		 WHERE canonical_key = ANY($1) AND char_length(content) > $2`,
		keys, minRunes)
	if err != nil {
		return nil, types.NewAppError(types.CodeDatabase,
			"查询已补全的 canonical_key", err)
	}
	defer rows.Close()

	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return nil, types.NewAppError(types.CodeDatabase, "扫描 canonical_key 行", err)
		}
		out[key] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "遍历 canonical_key 结果集", err)
	}
	return out, nil
}

// GetContentItem 按 id 取内容条目（deep_dive / 追问取原文用）。无行返回 CodeNotFound。
func (s *Store) GetContentItem(ctx context.Context, id int64) (*types.ContentItem, error) {
	var ci types.ContentItem
	err := s.pool.QueryRow(ctx,
		`SELECT id, COALESCE(source_id, 0), external_id, canonical_key, url, title, content, author,
		        published_at, content_hash, simhash, fetched_at, created_at, kind
		 FROM content_items WHERE id = $1`, id).Scan(
		&ci.ID, &ci.SourceID, &ci.ExternalID, &ci.CanonicalKey, &ci.URL, &ci.Title, &ci.Content,
		&ci.Author, &ci.PublishedAt, &ci.ContentHash, &ci.Simhash, &ci.FetchedAt, &ci.CreatedAt, &ci.Kind,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, types.NewAppError(types.CodeNotFound,
				fmt.Sprintf("内容条目 id=%d 不存在", id), err)
		}
		return nil, types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("查询内容条目（id=%d）", id), err)
	}
	return &ci, nil
}

// ListUnpushedBySchedule 只在本任务的 fetch target 见过的内容里挑，
// 取材范围始终按任务隔离。
// **去重不隔离**：NOT EXISTS 排除该用户**已投递过的全部内容**（任意批次/
// 任意状态），而非只排除本任务自己投过的。
//
// 为什么去重用用户级而非 per-schedule 账本（决策 A，2026-07-19 Boss 拍板改）：早期用
// per-schedule 账本（deliveries JOIN push_batches WHERE schedule_id）追求「自包含互不干扰」，
// 但任务从用户级转隔离时该账本为空 → 本任务源里用户**早已在全局推送里看过**的存量积压全被
// 当成「没推过」重推一遍（实测：转隔离首日 47 条候选里 40 条是用户已读）。改成用户级去重后，
// 隔离任务永不把用户看过的内容再推一遍（无论经本任务、别的任务还是全局）。代价（已知、接受）：
// 两个任务共享同一源时，同一内容只会被先触发的那个推给用户一次——轻微弱化「任务自包含」，
// 换来「同一条永不重复轰炸用户」，对单用户产品是更优权衡。
//
// perSourceCap 仍按 source_id 分区（同用户级版，防高产源饿死低产源）。scheduleID 由 PushParams
// 经 scheduler 可信下传，无注入面；用户由 scheduleID 反查（schedule 属主唯一）。
func (s *Store) ListUnpushedBySchedule(ctx context.Context, scheduleID string, limit, perSourceCap int) ([]types.ContentItem, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id, source_id, external_id, canonical_key, url, title, content, author,
		        published_at, content_hash, simhash, fetched_at, created_at, kind
		 FROM (
		     SELECT ci.id, COALESCE(ci.source_id, matched.source_id) AS source_id,
		            ci.external_id, ci.canonical_key, ci.url, ci.title,
		            ci.content, ci.author, ci.published_at, ci.content_hash, ci.simhash,
		            ci.fetched_at, ci.created_at, ci.kind,
		            ROW_NUMBER() OVER (
		                PARTITION BY matched.source_id
		                ORDER BY ci.fetched_at DESC, ci.id DESC
		            ) AS rn
		     FROM content_items ci
		     JOIN LATERAL (
		         SELECT MIN(cs.source_id) AS source_id
		           FROM content_sources cs
		           JOIN task_fetch_targets ss
		             ON ss.fetch_target_id = cs.source_id
		          WHERE cs.content_item_id = ci.id
		            AND ss.schedule_id = $1
		     ) matched ON matched.source_id IS NOT NULL
		       AND NOT EXISTS (
		           SELECT 1 FROM deliveries d
		           WHERE d.content_item_id = ci.id
		             AND d.user_id = (SELECT user_id FROM schedules WHERE id = $1)
		       )
		 ) t
		 WHERE t.rn <= $3
		 ORDER BY fetched_at DESC
		 LIMIT $2`,
		scheduleID, limit, perSourceCap)
	if err != nil {
		return nil, types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("查询任务 %s 未投递内容", scheduleID), err)
	}
	defer rows.Close()

	var out []types.ContentItem
	for rows.Next() {
		var ci types.ContentItem
		if err := rows.Scan(
			&ci.ID, &ci.SourceID, &ci.ExternalID, &ci.CanonicalKey, &ci.URL, &ci.Title, &ci.Content,
			&ci.Author, &ci.PublishedAt, &ci.ContentHash, &ci.Simhash, &ci.FetchedAt, &ci.CreatedAt,
			&ci.Kind,
		); err != nil {
			return nil, types.NewAppError(types.CodeDatabase, "扫描 content_item 行", err)
		}
		out = append(out, ci)
	}
	if err := rows.Err(); err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "遍历 content_item 结果集", err)
	}
	return out, nil
}

// SearchContentItems 按关键词 + 时间窗检索内容，content.query 的唯一数据面
// （a2a-contract §4.2）。语义：
//
//	(title ILIKE '%kw%' OR content ILIKE '%kw%')     -- kw 空串时省略该谓词
//	AND COALESCE(published_at, fetched_at) >= $since -- published_at 可空（001:76），NULL 回退
//	                                                 -- fetched_at，不静默丢无发布时间的整类源
//	ORDER BY COALESCE(published_at, fetched_at) DESC, id DESC LIMIT $limit
//
// keyword 中的 %、_、\ 经 escapeLike 转义后再拼 '%..%'：入站文本不可信，裸拼会让外部
// 输入携带 LIKE 通配符打穿检索语义（参数化仍守住 SQL 注入，被劫持的是语义与性能）。
// limit 由调用方（executor）钳制后传入，本方法防御性处理 limit<=0 → 20。
// 第一期刻意 ILIKE 不引分词：中文 tsvector 无效、pg_jieba/zhparser 不在默认镜像
// （拍板 §13.8）；真实库量基准慢了再补表达式索引或 pg_trgm。
func (s *Store) SearchContentItems(ctx context.Context, keyword string, since time.Time, limit int) ([]types.ContentItem, error) {
	if limit <= 0 {
		limit = 20
	}
	query := `SELECT id, COALESCE(source_id, 0), external_id, canonical_key, url, title, content, author,
	                 published_at, content_hash, simhash, fetched_at, created_at, kind
	          FROM content_items
	          WHERE COALESCE(published_at, fetched_at) >= $1`
	args := []any{since}
	if keyword != "" {
		args = append(args, "%"+escapeLike(keyword)+"%")
		query += ` AND (title ILIKE $2 OR content ILIKE $2)`
	}
	args = append(args, limit)
	query += fmt.Sprintf(` ORDER BY COALESCE(published_at, fetched_at) DESC, id DESC LIMIT $%d`, len(args))

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, types.NewAppError(types.CodeDatabase,
			fmt.Sprintf("检索内容条目（keyword=%q）", keyword), err)
	}
	defer rows.Close()

	var out []types.ContentItem
	for rows.Next() {
		var ci types.ContentItem
		if err := rows.Scan(
			&ci.ID, &ci.SourceID, &ci.ExternalID, &ci.CanonicalKey, &ci.URL, &ci.Title, &ci.Content,
			&ci.Author, &ci.PublishedAt, &ci.ContentHash, &ci.Simhash, &ci.FetchedAt, &ci.CreatedAt,
			&ci.Kind,
		); err != nil {
			return nil, types.NewAppError(types.CodeDatabase, "扫描 content_item 行", err)
		}
		out = append(out, ci)
	}
	if err := rows.Err(); err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "遍历 content_item 结果集", err)
	}
	return out, nil
}

// SearchContentItemsForA2A projects globally de-duplicated content through the
// current workspace's task fetch targets. content_items/content_sources remain
// shared facts; this query is the authorization edge that decides which facts
// an authenticated workspace principal may observe.
func (s *Store) SearchContentItemsForA2A(
	ctx context.Context,
	scope types.A2AExecutionScope,
	keyword string,
	since time.Time,
	limit int,
) ([]types.ContentItem, error) {
	hasContentScope := false
	for _, granted := range scope.Scopes {
		if granted == types.A2AScopeContentQuery {
			hasContentScope = true
			break
		}
	}
	if !hasContentScope {
		return nil, types.NewAppError(types.CodeForbidden,
			"A2A credential does not grant content.query", nil)
	}
	if limit <= 0 {
		limit = 20
	}
	tx, err := s.beginA2APrincipalTx(ctx, scope)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	query := `SELECT ci.id,COALESCE(ci.source_id,0),ci.external_id,ci.canonical_key,
	                 ci.url,ci.title,ci.content,ci.author,ci.published_at,ci.content_hash,
	                 ci.simhash,ci.fetched_at,ci.created_at,ci.kind
	          FROM content_items ci
	          WHERE COALESCE(ci.published_at,ci.fetched_at)>=$1
	            AND EXISTS (
	                SELECT 1
	                FROM content_sources appearance
	                JOIN task_fetch_targets binding
	                  ON binding.fetch_target_id=appearance.source_id
	                JOIN schedules task ON task.id=binding.schedule_id
	                JOIN tenants workspace ON workspace.id=task.tenant_id
	                WHERE appearance.content_item_id=ci.id
	                  AND task.tenant_id=$2
	                  AND (
	                    (workspace.workspace_kind='team' AND task.task_visibility='workspace') OR
	                    (workspace.workspace_kind='personal' AND task.task_visibility='personal'
	                     AND workspace.personal_owner_user_id=$3)
	                  )
	            )`
	args := []any{since, scope.TenantID, scope.UserID}
	if keyword != "" {
		args = append(args, "%"+escapeLike(keyword)+"%")
		query += fmt.Sprintf(` AND (ci.title ILIKE $%d OR ci.content ILIKE $%d)`, len(args), len(args))
	}
	args = append(args, limit)
	query += fmt.Sprintf(` ORDER BY COALESCE(ci.published_at,ci.fetched_at) DESC,ci.id DESC LIMIT $%d`, len(args))
	rows, err := tx.Query(ctx, query, args...)
	if err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "search workspace-scoped A2A content", err)
	}
	defer rows.Close()
	out := make([]types.ContentItem, 0)
	for rows.Next() {
		var item types.ContentItem
		if err := rows.Scan(&item.ID, &item.SourceID, &item.ExternalID, &item.CanonicalKey,
			&item.URL, &item.Title, &item.Content, &item.Author, &item.PublishedAt,
			&item.ContentHash, &item.Simhash, &item.FetchedAt, &item.CreatedAt, &item.Kind); err != nil {
			return nil, types.NewAppError(types.CodeDatabase, "scan workspace-scoped A2A content", err)
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "iterate workspace-scoped A2A content", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, types.NewAppError(types.CodeDatabase, "commit workspace-scoped A2A content search", err)
	}
	return out, nil
}

// escapeLike 把 s 中的 \、%、_ 依序替换为 \\、\%、\_ 后返回（a2a-contract §4.2）。
// SQL 侧依赖 Postgres 默认转义符 ESCAPE '\'（标准默认行为，不显式写 ESCAPE 子句）。
// 替换顺序必须 \ 在先：先转义通配符再转义反斜杠会把刚产出的转义符再翻一遍。
func escapeLike(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `%`, `\%`)
	s = strings.ReplaceAll(s, `_`, `\_`)
	return s
}
