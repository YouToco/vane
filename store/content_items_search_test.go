package store

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/YouToco/vane/types"
)

// TestEscapeLike 纯单测（无 DB）：\、%、_ 依序转义（a2a-contract §4.2）。
func TestEscapeLike(t *testing.T) {
	cases := []struct {
		name, in, want string
	}{
		{"空串", "", ""},
		{"无特殊字符", "anthropic 新模型", "anthropic 新模型"},
		{"百分号", "100%", `100\%`},
		{"下划线", "snake_case", `snake\_case`},
		{"反斜杠", `C:\dir`, `C:\\dir`},
		{"反斜杠在先不被二次转义", `\%`, `\\\%`},
		{"三种混合", `a\b%c_d`, `a\\b\%c\_d`},
		{"连续通配符", "%%__", `\%\%\_\_`},
		{"已转义形态再转义", `\\`, `\\\\`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := escapeLike(c.in); got != c.want {
				t.Errorf("escapeLike(%q) = %q，期望 %q", c.in, got, c.want)
			}
		})
	}
}

// TestSearchContentItems 是 DATABASE_URL 门控的集成测试（无则跳过，契约 §9.3）：
// 关键词命中 title/content 两路、时间窗（含 published_at NULL 回退 fetched_at）、
// limit 与防御缺省、escapeLike 防裸通配误伤、基准计时留档。
func TestSearchContentItems(t *testing.T) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("未设置 DATABASE_URL，跳过 SearchContentItems 集成测试")
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

	srcID, _, err := st.UpsertSource(ctx, &types.Source{
		Platform:   types.PlatformWeb,
		Capability: types.CapFeed,
		URL:        "https://example.com/test-search-" + uuid.NewString(),
		Title:      "search-test-source",
	})
	if err != nil {
		t.Fatalf("UpsertSource() 失败: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := cleanupContext()
		defer cancel()
		// FK 逆序：content_sources → content_items → sources。
		cleanupExec(ctx, t, st, `DELETE FROM content_sources WHERE source_id = $1`, srcID)
		cleanupExec(ctx, t, st, `DELETE FROM content_items WHERE source_id = $1`, srcID)
		cleanupExec(ctx, t, st, `DELETE FROM sources WHERE id = $1`, srcID)
	})

	// 关键词带 uuid 后缀：检索打的是共享库全表，唯一标记保证谓词只命中本测试的行。
	mk := "a2akw" + strings.ReplaceAll(uuid.NewString(), "-", "")
	mkLike := "a2alike" + strings.ReplaceAll(uuid.NewString(), "-", "")
	now := time.Now()
	tp := func(t time.Time) *time.Time { return &t }
	// 早于全部插入时刻、晚于任何回填的 published_at；留 5s 余量吸收应用与 DB 的时钟偏差
	//（fetched_at 是 DB now()，winStart 是应用侧时钟，journalctl 时区教训的近亲）。
	winStart := now.Add(-5 * time.Second)

	insert := func(name, title, content string, publishedAt *time.Time) int64 {
		t.Helper()
		id, isNew, err := st.UpsertContentItem(ctx, &types.ContentItem{
			SourceID:     srcID,
			ExternalID:   "ext-" + uuid.NewString(),
			CanonicalKey: "https://example.com/search-" + uuid.NewString(),
			Kind:         types.KindArticle,
			URL:          "https://example.com/search-item",
			Title:        title,
			Content:      content,
			PublishedAt:  publishedAt,
			ContentHash:  "hash-" + uuid.NewString(),
		})
		if err != nil {
			t.Fatalf("插入测试内容 %s 失败: %v", name, err)
		}
		if !isNew {
			t.Fatalf("测试内容 %s 撞了既有 canonical_key（uuid 冲突？）", name)
		}
		return id
	}

	titleHit := insert("titleHit", "标题命中 "+mk+" 的行", "无关正文", tp(now.Add(-time.Hour)))
	contentHit := insert("contentHit", "无关标题", "正文命中 "+mk+" 的行", tp(now.Add(-2*time.Hour)))
	nullPub := insert("nullPub", "无发布时间", "正文也命中 "+mk+" 的行", nil) // 时间锚点回退 fetched_at≈now
	oldPub := insert("oldPub", "旧文命中 "+mk, "老正文", tp(now.Add(-72*time.Hour)))
	likeA := insert("likeA", mkLike+"_100%_off", "含通配字面量的正文", nil)
	likeB := insert("likeB", mkLike+"Z100ZZoff", "不含通配字面量的正文", nil)

	idsOf := func(items []types.ContentItem) []int64 {
		out := make([]int64, len(items))
		for i, it := range items {
			out[i] = it.ID
		}
		return out
	}

	t.Run("关键词两路命中与时间窗排序", func(t *testing.T) {
		start := time.Now()
		items, err := st.SearchContentItems(ctx, mk, now.Add(-24*time.Hour), 50)
		elapsed := time.Since(start)
		if err != nil {
			t.Fatalf("SearchContentItems() 失败: %v", err)
		}
		// 基准计时留档（契约 §4.2：本地小库仅记录耗时，真实库量基准在 VPS 阶段补）。
		t.Logf("SearchContentItems(keyword+24h 窗) 耗时 %v", elapsed)

		// 恰好 3 条：title 路、content 路、NULL published_at 回退路；72h 前的旧文被
		// COALESCE 用 published_at 挡在窗外（即便它 fetched_at 是刚刚）。
		got := idsOf(items)
		want := []int64{nullPub, titleHit, contentHit} // COALESCE desc：now ≈ fetched > -1h > -2h
		if len(got) != 3 || got[0] != want[0] || got[1] != want[1] || got[2] != want[2] {
			t.Errorf("期望 %v（按时间倒序），实际 %v", want, got)
		}
		for _, it := range items {
			if it.ID == oldPub {
				t.Error("published_at 在窗外的旧文不应命中（COALESCE 优先 published_at）")
			}
			// Kind 强制语义（types/enums.go）：SELECT 必须带 kind，回读不得是零值。
			if it.Kind != types.KindArticle {
				t.Errorf("回读 kind 应为 article，实际 %q（id=%d）", it.Kind, it.ID)
			}
		}
	})

	t.Run("放宽时间窗包含旧文", func(t *testing.T) {
		items, err := st.SearchContentItems(ctx, mk, now.Add(-96*time.Hour), 50)
		if err != nil {
			t.Fatalf("SearchContentItems() 失败: %v", err)
		}
		if got := idsOf(items); len(got) != 4 || got[3] != oldPub {
			t.Errorf("96h 窗应 4 条且旧文垫底，实际 %v", got)
		}
	})

	t.Run("limit截断与防御缺省", func(t *testing.T) {
		items, err := st.SearchContentItems(ctx, mk, now.Add(-24*time.Hour), 2)
		if err != nil {
			t.Fatalf("SearchContentItems(limit=2) 失败: %v", err)
		}
		if got := idsOf(items); len(got) != 2 || got[0] != nullPub || got[1] != titleHit {
			t.Errorf("limit=2 应取时间倒序前两条 [%d %d]，实际 %v", nullPub, titleHit, got)
		}
		// limit<=0 → 防御性 20：不报错、不空转（本用例 3 条全回）。
		items, err = st.SearchContentItems(ctx, mk, now.Add(-24*time.Hour), 0)
		if err != nil {
			t.Fatalf("SearchContentItems(limit=0) 失败: %v", err)
		}
		if len(items) != 3 {
			t.Errorf("limit=0 应走缺省 20 返回全部 3 条，实际 %d", len(items))
		}
	})

	t.Run("空关键词纯时间窗", func(t *testing.T) {
		// kw 空省略 ILIKE 谓词 = 纯时间窗浏览。窗口从 winStart 起：只有 published_at
		// 为 NULL（锚点回退 fetched_at≈插入时刻）的三条落在窗内；共享库可能有别人的
		// 行，只做包含性断言。
		items, err := st.SearchContentItems(ctx, "", winStart, 200)
		if err != nil {
			t.Fatalf("SearchContentItems(空关键词) 失败: %v", err)
		}
		got := map[int64]bool{}
		for _, it := range items {
			got[it.ID] = true
		}
		for _, want := range []int64{nullPub, likeA, likeB} {
			if !got[want] {
				t.Errorf("空关键词时间窗应包含 id=%d（fetched_at 在窗内）", want)
			}
		}
		for _, notWant := range []int64{titleHit, contentHit, oldPub} {
			if got[notWant] {
				t.Errorf("空关键词时间窗不应包含 id=%d（published_at 在窗外）", notWant)
			}
		}
	})

	t.Run("escapeLike防裸通配误伤", func(t *testing.T) {
		// 关键词含字面 %、_：转义后只命中真含这些字符的行。若不转义，模式
		// '%…_100%_off%' 里的 _ / % 会当通配符用，likeB（Z100ZZoff）也会被打中。
		items, err := st.SearchContentItems(ctx, mkLike+"_100%_off", now.Add(-24*time.Hour), 50)
		if err != nil {
			t.Fatalf("SearchContentItems(通配字面量) 失败: %v", err)
		}
		if got := idsOf(items); len(got) != 1 || got[0] != likeA {
			t.Errorf("通配字面量关键词应恰好命中 likeA=%d，实际 %v（含 %d 即裸通配打穿）",
				likeA, got, likeB)
		}
	})

	t.Run("全无命中返回空", func(t *testing.T) {
		items, err := st.SearchContentItems(ctx, "绝无此词"+uuid.NewString(), now.Add(-24*time.Hour), 50)
		if err != nil {
			t.Fatalf("SearchContentItems(无命中) 失败: %v", err)
		}
		// 空结果不是错误：err 已在上面断言为 nil，此处只验证空集形态。
		if len(items) != 0 {
			t.Errorf("无命中应返回空集，实际 %d 条", len(items))
		}
	})
}
