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

// TestKindRoundtrip 是 M6 契约 §16 Kind 往返用例的收敛版：page_watch/change 下线后
// kind 只剩 'article' 一种，无法再用"change vs article"验值区分，但仍守着一件事——
// kind 列必须活着走完 DB 往返（SELECT/Scan 漏了 kind 则 Go 读回零值 "" ≠ 'article'，
// 本用例判红）。将来若再引入非 article 的内容种类，应把当年的"两值对照"版本找回来
// （git history: refactor/drop-page-watch 之前），以恢复对"读侧把 kind 写死成常量"的守护。
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
	attachTenant(t, st, u.ID)
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
	articleID, isNew, err := st.UpsertContentItem(ctx, article)
	if err != nil {
		t.Fatalf("UpsertContentItem(article) 失败: %v", err)
	}
	if !isNew {
		t.Fatal("article 条目首插应 isNew=true（fixture 撞了库里已有身份，用例失去意义）")
	}

	// 写侧真值：DB 列里存的必须是 'article' 本身（不是空串）。核对行数只认 count(*)（红线 7）。
	var stored int
	if err := st.pool.QueryRow(ctx,
		`SELECT count(*) FROM content_items WHERE canonical_key = $1 AND kind = 'article'`,
		articleKey).Scan(&stored); err != nil {
		t.Fatalf("核对 article 行的 kind 列失败: %v", err)
	}
	if stored != 1 {
		t.Errorf("INSERT 后 DB 里 kind='article' 的该行应恰好 1 行，实际 %d（INSERT 漏写 kind 列或值被改写）", stored)
	}

	// 读侧往返：SELECT/Scan 漏了 kind 列则读回零值 "" ≠ 'article'，本断言判红。
	got, err := st.ListUnpushedByUser(ctx, u.ID, 100, 100)
	if err != nil {
		t.Fatalf("ListUnpushedByUser() 失败: %v", err)
	}
	kinds := map[int64]types.Kind{}
	for _, ci := range got {
		kinds[ci.ID] = ci.Kind
	}
	gotArticle, ok := kinds[articleID]
	if !ok {
		t.Fatalf("article 条目 id=%d 未出现在未投递列表里（共 %d 条）", articleID, len(got))
	}
	if gotArticle != types.KindArticle {
		t.Errorf("article 条目读回 Kind 应为 %q，实际 %q（SELECT/Scan 漏了 kind 列）", types.KindArticle, gotArticle)
	}
}

// TestMigration012KindBackfill 验证 012 在**有空串污染**的库上的回填结果。
//
// 与 TestMigration007ContentIdentity 同理另建一次性库：必须先停在 012 之前才能
// 人为造出 kind=” 的行（复刻生产污染的形态——008 之后 INSERT 显式写入 Go 零值
// ""，覆盖列 DEFAULT），在共享测试库上做这件事会污染并行包的用例。
//
// 顺带钉死两件事：
//   - WHERE kind=” 的选择性：非空值（change/article）一行都不许动；
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
