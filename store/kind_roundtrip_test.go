package store

import (
	"database/sql"
	"io/fs"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/pressly/goose/v3"

	"github.com/YouToco/vane/types"
)

// TestKindRoundtrip 是 M6 契约 §16 的端到端往返用例（🔴，§3.3.1 头号致命缺陷的守卫）：
// UpsertContentItem(Kind=change) → ListUnpushedByUser → 读回的 Kind 必须还是 change。
//
// 它抓的是"契约漏掉了 store 层 SQL"这件事本身——INSERT 列清单漏 kind 则 DB 落
// DEFAULT 'article'，SELECT/Scan 漏 kind 则 Go 读回零值 ""；任一方向漏掉，下游
// Dedup 的 change 豁免就恒不触发、页面变化被 simhash 静默吞掉，而查 content_items
// 的探针照样是绿的（那些行在 Dedup 之前就已写入）。两个方向本用例都判红。
// DB 门控（无 DATABASE_URL 跳过，CI 带 postgres 真跑）。
func TestKindRoundtrip(t *testing.T) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("未设置 DATABASE_URL，跳过 kind 往返集成测试")
	}
	ctx := t.Context()

	if err := Migrate(ctx, dbURL); err != nil {
		t.Fatalf("Migrate() 执行失败: %v", err)
	}
	st, err := New(ctx, dbURL)
	if err != nil {
		t.Fatalf("New() 建池失败: %v", err)
	}
	registerStoreClose(t, st)

	u, err := st.UpsertUserByOpenID(ctx, "test_kind_"+uuid.NewString(), "kind-test")
	if err != nil {
		t.Fatalf("UpsertUserByOpenID() 失败: %v", err)
	}
	srcID, _, err := st.UpsertSource(ctx, &types.Source{
		Platform:   types.PlatformWeb,
		Capability: types.CapFeed,
		URL:        "https://example.com/test-kind-" + uuid.NewString(),
		Title:      "kind-roundtrip-source",
	})
	if err != nil {
		t.Fatalf("UpsertSource() 失败: %v", err)
	}

	t.Cleanup(func() {
		ctx, cancel := cleanupContext()
		defer cancel()
		// FK 逆序，同 TestPipelineStore 的清理（PR#26/#28 的显性化模式）。
		cleanupExec(ctx, t, st, `DELETE FROM content_sources WHERE source_id = $1`, srcID)
		cleanupExec(ctx, t, st, `DELETE FROM content_items WHERE source_id = $1`, srcID)
		cleanupExec(ctx, t, st, `DELETE FROM subscriptions WHERE user_id = $1`, u.ID)
		cleanupExec(ctx, t, st, `DELETE FROM sources WHERE id = $1`, srcID)
		cleanupExec(ctx, t, st, `DELETE FROM users WHERE id = $1`, u.ID)
	})

	if err := st.AddSubscription(ctx, u.ID, srcID); err != nil {
		t.Fatalf("AddSubscription() 失败: %v", err)
	}

	// change 条目（I4 的主角）+ article 对照组：两种值都得原样活着走完往返，
	// 只测 change 挡不住"读侧把 kind 写死成常量"这类退化。
	changeKey := "watch://example.com/pricing#h1->h2-" + uuid.NewString()
	change := &types.ContentItem{
		SourceID:     srcID,
		ExternalID:   "chg-" + uuid.NewString(),
		CanonicalKey: changeKey,
		URL:          "https://example.com/pricing",
		Title:        "页面变化事件",
		Content:      "价格行从 302 变为 298",
		ContentHash:  "khash-" + uuid.NewString(),
		Kind:         types.KindChange,
	}
	changeID, isNew, err := st.UpsertContentItem(ctx, change)
	if err != nil {
		t.Fatalf("UpsertContentItem(change) 失败: %v", err)
	}
	if !isNew {
		t.Fatal("change 条目首插应 isNew=true（fixture 撞了库里已有身份，用例失去意义）")
	}
	articleKey := "https://example.com/article-" + uuid.NewString()
	article := &types.ContentItem{
		SourceID:     srcID,
		ExternalID:   "art-" + uuid.NewString(),
		CanonicalKey: articleKey,
		URL:          articleKey,
		Title:        "一篇文章",
		Content:      "正文",
		ContentHash:  "khash-" + uuid.NewString(),
		Kind:         types.KindArticle,
	}
	articleID, _, err := st.UpsertContentItem(ctx, article)
	if err != nil {
		t.Fatalf("UpsertContentItem(article) 失败: %v", err)
	}

	// 写侧真值：DB 列里存的必须是 'change' 本身，不是 DEFAULT 兜出来的 'article'
	// 也不是空串。核对行数只认 count(*)（红线 7）。
	var stored int
	if err := st.pool.QueryRow(ctx,
		`SELECT count(*) FROM content_items WHERE canonical_key = $1 AND kind = 'change'`,
		changeKey).Scan(&stored); err != nil {
		t.Fatalf("核对 change 行的 kind 列失败: %v", err)
	}
	if stored != 1 {
		t.Errorf("INSERT 后 DB 里 kind='change' 的该行应恰好 1 行，实际 %d（INSERT 漏写 kind 列或值被改写）", stored)
	}

	// 读侧往返：workflow.Fetch 返回的正是 ListUnpushedByUser 的结果（不是本次抓到的
	// items），所以 Dedup 看到的 Kind 只能来自这条 SELECT——它漏 kind，豁免就死。
	got, err := st.ListUnpushedByUser(ctx, u.ID, 100, 100)
	if err != nil {
		t.Fatalf("ListUnpushedByUser() 失败: %v", err)
	}
	kinds := map[int64]types.Kind{}
	for _, ci := range got {
		kinds[ci.ID] = ci.Kind
	}
	gotChange, ok := kinds[changeID]
	if !ok {
		t.Fatalf("change 条目 id=%d 未出现在未投递列表里（共 %d 条）", changeID, len(got))
	}
	if gotChange != types.KindChange {
		t.Errorf("change 条目读回 Kind 应为 %q，实际 %q（SELECT/Scan 漏了 kind 列）",
			types.KindChange, gotChange)
	}
	gotArticle, ok := kinds[articleID]
	if !ok {
		t.Fatalf("article 条目 id=%d 未出现在未投递列表里（共 %d 条）", articleID, len(got))
	}
	if gotArticle != types.KindArticle {
		t.Errorf("article 条目读回 Kind 应为 %q，实际 %q", types.KindArticle, gotArticle)
	}
}

// TestMigration012KindBackfill 验证 012 在**有空串污染**的库上的回填结果。
//
// 与 TestMigration007ContentIdentity 同理另建一次性库：必须先停在 012 之前才能
// 人为造出 kind='' 的行（复刻生产污染的形态——008 之后 INSERT 显式写入 Go 零值
// ""，覆盖列 DEFAULT），在共享测试库上做这件事会污染并行包的用例。
//
// 顺带钉死两件事：
//   - WHERE kind='' 的选择性：非空值（change/article）一行都不许动；
//   - 012 的 Down 段真的可执行——它是刻意的空段（纯数据回填无从回滚），而 008
//     恰恰因 goose 注解问题炸过部署，"空 Down 段能被 goose 解析并执行"必须真跑
//     一次 down 来证明，不能停留在纸面。
func TestMigration012KindBackfill(t *testing.T) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("未设置 DATABASE_URL，跳过 012 迁移集成测试")
	}
	ctx := t.Context()

	migURL, drop := createScratchDB(ctx, t, dbURL)
	defer drop()

	db, err := sql.Open("pgx", migURL)
	if err != nil {
		t.Fatalf("打开一次性库连接失败: %v", err)
	}
	defer db.Close()

	dir, err := fs.Sub(migrationsFS, "migrations")
	if err != nil {
		t.Fatalf("定位迁移目录失败: %v", err)
	}
	provider, err := goose.NewProvider(goose.DialectPostgres, db, dir)
	if err != nil {
		t.Fatalf("初始化 goose provider 失败: %v", err)
	}

	// 停在 011（page_snapshots，012 的前一号）：kind 列已存在（008 建的）、012 未跑，
	// 可以造污染。
	if _, err := provider.UpTo(ctx, 11); err != nil {
		t.Fatalf("迁移到 011 失败: %v", err)
	}

	var srcID int64
	mustQueryRow(ctx, t, db,
		`INSERT INTO sources (platform, capability, url) VALUES ('web', 'feed', 'https://mig012.example/feed') RETURNING id`,
		&srcID)
	// 三行三种形态：空串（生产污染）、change、article（各自必须原样保留）。
	mustExec(ctx, t, db,
		`INSERT INTO content_items (source_id, external_id, canonical_key, url, title, content_hash, kind)
		 VALUES ($1, 'e-empty',   'mig012-key-empty',   'u1', '污染行', 'h1', ''),
		        ($1, 'e-change',  'mig012-key-change',  'u2', '变化行', 'h2', 'change'),
		        ($1, 'e-article', 'mig012-key-article', 'u3', '文章行', 'h3', 'article')`,
		srcID)

	if _, err := provider.UpTo(ctx, 12); err != nil {
		t.Fatalf("执行 012 迁移失败: %v", err)
	}

	// 核对行数只认 count(*)（红线 7）。
	var empties, changes, articles int
	mustQueryRow(ctx, t, db, `SELECT count(*) FROM content_items WHERE kind = ''`, &empties)
	if empties != 0 {
		t.Errorf("012 后不应残留 kind='' 的行，实际 %d 行", empties)
	}
	mustQueryRow(ctx, t, db,
		`SELECT count(*) FROM content_items WHERE canonical_key = 'mig012-key-change' AND kind = 'change'`, &changes)
	if changes != 1 {
		t.Errorf("kind='change' 的行不许被回填误伤，实际匹配 %d 行", changes)
	}
	mustQueryRow(ctx, t, db, `SELECT count(*) FROM content_items WHERE kind = 'article'`, &articles)
	if articles != 2 {
		t.Errorf("回填后应恰好 2 行 article（污染行被回填 + 原本的 article 行），实际 %d 行", articles)
	}

	// Down 可执行且不还原数据（空段语义）：回填过的行保持 article。
	if _, err := provider.DownTo(ctx, 11); err != nil {
		t.Fatalf("012 down 失败（空 Down 段应可被 goose 解析并执行）: %v", err)
	}
	mustQueryRow(ctx, t, db, `SELECT count(*) FROM content_items WHERE kind = ''`, &empties)
	if empties != 0 {
		t.Errorf("down 不应把数据改回空串，实际出现 %d 行", empties)
	}
	// 再 up 回来：幂等（没有 '' 行时 UPDATE 空转，不报错）。
	if _, err := provider.UpTo(ctx, 12); err != nil {
		t.Fatalf("012 down 后重新 up 失败（应幂等）: %v", err)
	}
}
