package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io/fs"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pressly/goose/v3"

	"github.com/YouToco/vane/types"
)

// containsInt64 判断切片是否含某值（测试辅助）。
func containsInt64(s []int64, v int64) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}

// TestPipelineStore 是 DATABASE_URL 门控的集成测试（无则跳过），覆盖 M3 store
// 扩展的关键往返：UpsertSource 按 url 幂等、加订阅→列订阅、
// UpsertContentItem 按 canonical_key 全局去重、schedule 增查删。
func TestPipelineStore(t *testing.T) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("未设置 DATABASE_URL，跳过 pipeline store 集成测试")
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

	// 测试数据用固定前缀 + uuid 后缀，结束时按 FK 逆序清理，避免污染共享测试库。
	u, err := st.UpsertUserByOpenID(ctx, "test_pipeline_"+uuid.NewString(), "pipeline-test")
	if err != nil {
		t.Fatalf("UpsertUserByOpenID() 失败: %v", err)
	}

	srcURL := "https://example.com/test-pipeline-" + uuid.NewString()
	srcID, err := st.UpsertSource(ctx, &types.Source{
		Platform:   types.PlatformWeb,
		Capability: types.CapFeed,
		URL:        srcURL,
		Title:      "pipeline-test-source",
	})
	if err != nil {
		t.Fatalf("UpsertSource() 失败: %v", err)
	}
	// 第二个源：007 的跨源用例（同一条内容命中两个源）都要它。放在 Cleanup 注册前
	// 建好，否则子测试里新建的源会漏出清理范围、污染共享测试库。
	src2ID, err := st.UpsertSource(ctx, &types.Source{
		Platform:   types.PlatformWeb,
		Capability: types.CapFeed,
		URL:        "https://example.com/test-pipeline-2nd-" + uuid.NewString(),
		Title:      "pipeline-test-source-2",
	})
	if err != nil {
		t.Fatalf("UpsertSource() 建第二个源失败: %v", err)
	}
	srcIDs := []int64{srcID, src2ID}

	t.Cleanup(func() {
		ctx, cancel := cleanupContext()
		defer cancel()
		// FK 逆序：deliveries→push_batches→content_sources→content_items→
		// subscriptions/schedules→sources→users。
		cleanupExec(ctx, t, st, `DELETE FROM deliveries WHERE user_id = $1`, u.ID)
		cleanupExec(ctx, t, st, `DELETE FROM push_batches WHERE user_id = $1`, u.ID)
		// content_sources 必须显式先删：它对 content_items 的 FK 是 ON DELETE CASCADE，
		// 但对 sources 的不是——按 source_id 删内容会漏掉"以另一个源首发"的那些行，
		// 残留任意一行都会让下面删 sources 撞 FK。
		cleanupExec(ctx, t, st, `DELETE FROM content_sources WHERE source_id = ANY($1)`, srcIDs)
		cleanupExec(ctx, t, st, `DELETE FROM content_items WHERE source_id = ANY($1)`, srcIDs)
		cleanupExec(ctx, t, st, `DELETE FROM subscriptions WHERE user_id = $1`, u.ID)
		cleanupExec(ctx, t, st, `DELETE FROM schedules WHERE user_id = $1`, u.ID)
		cleanupExec(ctx, t, st, `DELETE FROM sources WHERE id = ANY($1)`, srcIDs)
		cleanupExec(ctx, t, st, `DELETE FROM users WHERE id = $1`, u.ID)
	})

	t.Run("UpsertSource按url幂等", func(t *testing.T) {
		again, err := st.UpsertSource(ctx, &types.Source{
			Platform:   types.PlatformWeb,
			Capability: types.CapFeed,
			URL:        srcURL,
			Title:      "pipeline-test-source-renamed",
		})
		if err != nil {
			t.Fatalf("UpsertSource() 二次调用失败: %v", err)
		}
		if again != srcID {
			t.Errorf("同 url 重复 upsert 应返回同 id：首次 %d，二次 %d", srcID, again)
		}
	})

	t.Run("UpsertSource并发同url只产生一行", func(t *testing.T) {
		// 007 的核心动机之一，必须真起 goroutine 打真库：这个竞态是 SELECT-then-INSERT
		// 写法的必然产物（N 个事务同时查空、同时插入），单线程顺序调用永远测不出来。
		// 漏掉它的代价是重复信源 → 同一内容存两份 → 每轮重复抓取重复付费。
		concURL := "https://example.com/test-pipeline-conc-" + uuid.NewString()
		const n = 8

		var wg sync.WaitGroup
		ids := make([]int64, n)
		errs := make([]error, n)
		start := make(chan struct{}) // 同时放行，尽量把 n 个 upsert 挤进同一竞态窗口
		for i := range n {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				ids[i], errs[i] = st.UpsertSource(ctx, &types.Source{
					Platform:   types.PlatformWeb,
					Capability: types.CapFeed,
					URL:        concURL,
					Title:      "conc-source",
				})
			}()
		}
		close(start)
		wg.Wait()

		t.Cleanup(func() {
			ctx, cancel := cleanupContext()
			defer cancel()
			cleanupExec(ctx, t, st, `DELETE FROM sources WHERE url = $1`, concURL)
		})

		for i, err := range errs {
			if err != nil {
				t.Fatalf("并发 UpsertSource() 第 %d 个失败: %v", i, err)
			}
		}
		// 全部返回同一个 id，且库里只有一行——两者都要查：只查 id 相同挡不住
		// "多插了一行但恰好都返回第一行 id"，只查行数挡不住返回错 id。
		for i, id := range ids {
			if id != ids[0] {
				t.Errorf("并发 upsert 同 url 应收敛到同一 id：第 0 个 %d，第 %d 个 %d", ids[0], i, id)
			}
		}
		var rows int
		if err := st.pool.QueryRow(ctx,
			`SELECT count(*) FROM sources WHERE url = $1`, concURL).Scan(&rows); err != nil {
			t.Fatalf("统计并发 upsert 后的 sources 行数失败: %v", err)
		}
		if rows != 1 {
			t.Errorf("并发 upsert 同 url 应只产生 1 行，实际 %d 行", rows)
		}
	})

	t.Run("加订阅→列订阅", func(t *testing.T) {
		if err := st.AddSubscription(ctx, u.ID, srcID); err != nil {
			t.Fatalf("AddSubscription() 失败: %v", err)
		}
		// 幂等：ON CONFLICT DO NOTHING，重复加不报错。
		if err := st.AddSubscription(ctx, u.ID, srcID); err != nil {
			t.Fatalf("AddSubscription() 重复调用应幂等，实际报错: %v", err)
		}
		subs, err := st.ListSubscriptionsByUser(ctx, u.ID)
		if err != nil {
			t.Fatalf("ListSubscriptionsByUser() 失败: %v", err)
		}
		var found int
		for _, sub := range subs {
			if sub.SourceID == srcID {
				found++
			}
		}
		if found != 1 {
			t.Errorf("期望恰好 1 条对 source %d 的订阅，实际 %d 条（共 %d）", srcID, found, len(subs))
		}
	})

	t.Run("ListSubscribedSourcesByUser含非active", func(t *testing.T) {
		// 依赖前一个子测试已建立 u→srcID 的订阅关系。把 source 置为 disabled，
		// 验证 ListSubscribedSourcesByUser 仍返回它（状态灯可达），而抓取用的
		// ListActiveSourcesByUser 则将其排除。测试结束恢复为 active。
		if _, err := st.pool.Exec(ctx,
			`UPDATE sources SET status = $2 WHERE id = $1`, srcID, types.SourceStatusDisabled); err != nil {
			t.Fatalf("置 source 为 disabled 失败: %v", err)
		}
		defer func() {
			// 恢复失败会让后续子测试面对一个 disabled 的源，不能吞。
			ctx, cancel := cleanupContext()
			defer cancel()
			cleanupExec(ctx, t, st, `UPDATE sources SET status = $2 WHERE id = $1`, srcID, types.SourceStatusActive)
		}()

		all, err := st.ListSubscribedSourcesByUser(ctx, u.ID)
		if err != nil {
			t.Fatalf("ListSubscribedSourcesByUser() 失败: %v", err)
		}
		var foundDisabled bool
		for _, s := range all {
			if s.ID == srcID {
				foundDisabled = true
				if s.Status != types.SourceStatusDisabled {
					t.Errorf("期望回读 status=disabled，实际 %q", s.Status)
				}
			}
		}
		if !foundDisabled {
			t.Errorf("ListSubscribedSourcesByUser 应包含 disabled 的源 %d，实际未包含（共 %d）", srcID, len(all))
		}

		active, err := st.ListActiveSourcesByUser(ctx, u.ID)
		if err != nil {
			t.Fatalf("ListActiveSourcesByUser() 失败: %v", err)
		}
		for _, s := range active {
			if s.ID == srcID {
				t.Errorf("ListActiveSourcesByUser 不应包含 disabled 的源 %d", srcID)
			}
		}
	})

	t.Run("UpsertContentItem按canonical_key去重", func(t *testing.T) {
		item := &types.ContentItem{
			SourceID:     srcID,
			ExternalID:   "ext-" + uuid.NewString(),
			CanonicalKey: "https://example.com/item-" + uuid.NewString(),
			URL:          "https://example.com/item",
			Title:        "标题",
			ContentHash:  "hash-" + uuid.NewString(),
		}
		id1, isNew1, err := st.UpsertContentItem(ctx, item)
		if err != nil {
			t.Fatalf("UpsertContentItem() 首插失败: %v", err)
		}
		if !isNew1 {
			t.Error("首次插入应 isNew=true")
		}
		// 同 canonical_key 第二次：isNew=false，返回同 id。
		id2, isNew2, err := st.UpsertContentItem(ctx, item)
		if err != nil {
			t.Fatalf("UpsertContentItem() 二插失败: %v", err)
		}
		if isNew2 {
			t.Error("重复插入应 isNew=false")
		}
		if id2 != id1 {
			t.Errorf("重复插入应返回同 id：首次 %d，二次 %d", id1, id2)
		}
	})

	t.Run("UpsertContentItem跨源只存一份且登记两条appearance", func(t *testing.T) {
		// 007 要根治的跨源重复：用户 A 订「AI编程」、B 订「AI工具」，同一篇笔记命中
		// 两个源。旧的 per-source 唯一会存两份 → 详情补全被付两次钱。
		// 两个源给的 external_id / url 刻意不同（xhs 的 url 带每次刷新的 xsec_token），
		// 正因如此身份只能是 canonical_key。
		key := "https://example.com/cross-source-" + uuid.NewString()
		first := &types.ContentItem{
			SourceID:     srcID,
			ExternalID:   "note-a-" + uuid.NewString(),
			CanonicalKey: key,
			URL:          "https://example.com/cross?xsec_token=aaa",
			Title:        "跨源内容",
			ContentHash:  "hash-x-" + uuid.NewString(),
		}
		id1, isNew1, err := st.UpsertContentItem(ctx, first)
		if err != nil {
			t.Fatalf("UpsertContentItem() 首发源插入失败: %v", err)
		}
		if !isNew1 {
			t.Error("内容首次入库应 isNew=true")
		}

		second := &types.ContentItem{
			SourceID:     src2ID,
			ExternalID:   "note-b-" + uuid.NewString(),
			CanonicalKey: key,
			URL:          "https://example.com/cross?xsec_token=bbb",
			Title:        "跨源内容",
			ContentHash:  "hash-x-" + uuid.NewString(),
		}
		id2, isNew2, err := st.UpsertContentItem(ctx, second)
		if err != nil {
			t.Fatalf("UpsertContentItem() 第二个源插入失败: %v", err)
		}
		if id2 != id1 {
			t.Errorf("同 canonical_key 跨源应复用同一条内容：首发 %d，第二源 %d", id1, id2)
		}
		// isNew=false 正是省钱的闸门：第二个源据此知道不必再为这篇付 $0.01。
		if isNew2 {
			t.Error("内容已被别的源收录过，第二个源应 isNew=false")
		}

		var items int
		if err := st.pool.QueryRow(ctx,
			`SELECT count(*) FROM content_items WHERE canonical_key = $1`, key).Scan(&items); err != nil {
			t.Fatalf("统计 content_items 失败: %v", err)
		}
		if items != 1 {
			t.Errorf("同 canonical_key 应全局只存 1 份，实际 %d 份", items)
		}

		// 两条 appearance：这是"哪些源见过这条内容"的唯一记录，也是第二个源的
		// 订阅者能收到它的唯一凭据（ListUnpushedByUser 经 content_sources 反查）。
		var appearances int
		if err := st.pool.QueryRow(ctx,
			`SELECT count(*) FROM content_sources WHERE content_item_id = $1`, id1).Scan(&appearances); err != nil {
			t.Fatalf("统计 content_sources 失败: %v", err)
		}
		if appearances != 2 {
			t.Errorf("两个源各见过一次，应有 2 条 appearance，实际 %d 条", appearances)
		}
		// 各源的 external_id 随链接存：源内 id 不同正是它不能当身份的原因。
		var extB string
		if err := st.pool.QueryRow(ctx,
			`SELECT external_id FROM content_sources WHERE content_item_id = $1 AND source_id = $2`,
			id1, src2ID).Scan(&extB); err != nil {
			t.Fatalf("查第二个源的 appearance 失败: %v", err)
		}
		if extB != second.ExternalID {
			t.Errorf("appearance 应记该源自己的 external_id：期望 %q，实际 %q", second.ExternalID, extB)
		}

		// 首发源不被后来的源覆盖：content_items.source_id 是"谁先发现"，
		// 信源质量分析（谁是源头、谁是二手）整个建在这个语义上，被覆盖就全错。
		var gotSourceID int64
		var gotExternalID string
		if err := st.pool.QueryRow(ctx,
			`SELECT source_id, external_id FROM content_items WHERE id = $1`, id1).
			Scan(&gotSourceID, &gotExternalID); err != nil {
			t.Fatalf("回读内容首发源失败: %v", err)
		}
		if gotSourceID != srcID {
			t.Errorf("首发源应保持为 %d（先发现者），实际被改成 %d", srcID, gotSourceID)
		}
		if gotExternalID != first.ExternalID {
			t.Errorf("首发源的 external_id 应保持为 %q，实际 %q", first.ExternalID, gotExternalID)
		}
	})

	t.Run("EnrichedCanonicalKeys只报已补全的", func(t *testing.T) {
		// 抓取器靠它决定"这条笔记还要不要花 $0.01 补全"。判错的代价是钱
		// （多判→为已补全的笔记重复付费）或内容质量（漏判→笔记永远停在 60 字残句），
		// 所以必须真库往返验证 char_length + ANY($1) 的行为。
		const minRunes = 60
		long := "key-long-" + uuid.NewString()   // 已补全：正文远长于阈值
		short := "key-short-" + uuid.NewString() // 补全失败过：只有搜索摘要
		absent := "key-absent-" + uuid.NewString()

		mk := func(key, content string) {
			t.Helper()
			if _, _, err := st.UpsertContentItem(ctx, &types.ContentItem{
				SourceID:     srcID,
				ExternalID:   "ext-" + key,
				CanonicalKey: key,
				URL:          "https://example.com/" + key,
				Title:        "条目",
				Content:      content,
				ContentHash:  "ehash-" + uuid.NewString(),
			}); err != nil {
				t.Fatalf("准备条目 %s 失败: %v", key, err)
			}
		}
		// 中文正文：char_length 数字符、len() 数字节，两者差三倍——SQL 用错函数
		// 会让每条中文笔记都被误判为"未补全"而每轮重复付费。
		mk(long, strings.Repeat("正", 200))
		mk(short, strings.Repeat("残", minRunes)) // 恰好 60：截断残句的典型长度

		got, err := st.EnrichedCanonicalKeys(ctx, []string{long, short, absent}, minRunes)
		if err != nil {
			t.Fatalf("EnrichedCanonicalKeys() 失败: %v", err)
		}
		if _, ok := got[long]; !ok {
			t.Errorf("正文已补全的 %q 应被报告（跳过付费），实际 %v", long, got)
		}
		// 边界是 > 而非 >=：恰好 60 rune 正是上游截断的长度，必须判为"未补全"
		// 让它下轮重试——否则一次瞬时 429 就把这条笔记终身钉在 60 字。
		if _, ok := got[short]; ok {
			t.Errorf("正文恰好 %d rune（截断残句）不该被判为已补全，实际 %v", minRunes, got)
		}
		if _, ok := got[absent]; ok {
			t.Errorf("未入库的 %q 不该被报告", absent)
		}
		if len(got) != 1 {
			t.Errorf("期望恰好 1 个命中，实际 %d", len(got))
		}

		// 空入参：直接返回空 map，不该查库更不该报错。
		empty, err := st.EnrichedCanonicalKeys(ctx, nil, minRunes)
		if err != nil {
			t.Fatalf("空入参不该报错: %v", err)
		}
		if len(empty) != 0 {
			t.Errorf("空入参应返回空 map，实际 %v", empty)
		}

		// 跨源命中：这正是 007 的净收益，也是签名去掉 source_id 的全部理由——
		// 别的源补全过的内容，本源查同一个 key 就该命中、不必再付一次钱。
		// 旧的 EnrichedExternalIDs 按 source_id 隔离，这里必然 miss、必然重复付费。
		crossKey := "key-cross-" + uuid.NewString()
		if _, _, err := st.UpsertContentItem(ctx, &types.ContentItem{
			SourceID:     src2ID, // 由**另一个**源补全并入库
			ExternalID:   "ext-" + crossKey,
			CanonicalKey: crossKey,
			URL:          "https://example.com/" + crossKey,
			Title:        "别的源补全过的内容",
			Content:      strings.Repeat("全", 200),
			ContentHash:  "ehash-" + uuid.NewString(),
		}); err != nil {
			t.Fatalf("准备跨源已补全条目失败: %v", err)
		}
		cross, err := st.EnrichedCanonicalKeys(ctx, []string{crossKey}, minRunes)
		if err != nil {
			t.Fatalf("EnrichedCanonicalKeys() 跨源查询失败: %v", err)
		}
		if _, ok := cross[crossKey]; !ok {
			t.Errorf("别的源已补全的 %q 应命中（省下 $0.01），实际 %v", crossKey, cross)
		}
	})

	t.Run("ListRecentSimhashesByUser排除本批ID_防自撞", func(t *testing.T) {
		// 回归测试：Fetch 在抓取时已把 simhash 写入 content_items，Dedup 随后查历史。
		// 若不排除本批刚入库的 ID，每条内容都会查到自己的 simhash 而被判近重复、整批删光，
		// pipeline "去重后无内容" 早退、永远推不出卡片（真实线上故障，2026-07-14 定位）。
		// 历史按 user 维度（跨订阅源）查询——依赖前面子测试建立的 u→srcID 订阅关系。
		var sh int64 = 0x0123456789abcdef
		item := &types.ContentItem{
			SourceID:     srcID,
			ExternalID:   "sim-" + uuid.NewString(),
			CanonicalKey: "https://example.com/sim-" + uuid.NewString(),
			URL:          "https://example.com/sim",
			Title:        "simhash 内容",
			ContentHash:  "shash-" + uuid.NewString(),
			Simhash:      &sh,
		}
		id, _, err := st.UpsertContentItem(ctx, item)
		if err != nil {
			t.Fatalf("插入带 simhash 内容失败: %v", err)
		}
		since := time.Now().Add(-time.Hour)

		// 不排除：应能查到刚入库的 simhash（经 user→subscription→source 关联）。
		got, err := st.ListRecentSimhashesByUser(ctx, u.ID, since, nil)
		if err != nil {
			t.Fatalf("ListRecentSimhashesByUser(不排除) 失败: %v", err)
		}
		if !containsInt64(got, sh) {
			t.Errorf("不排除时应包含刚入库的 simhash %x，实际 %v", sh, got)
		}

		// 排除本条 ID：绝不能再查到它自己的 simhash（否则 Dedup 自撞、整批删光）。
		got2, err := st.ListRecentSimhashesByUser(ctx, u.ID, since, []int64{id})
		if err != nil {
			t.Fatalf("ListRecentSimhashesByUser(排除本批) 失败: %v", err)
		}
		if containsInt64(got2, sh) {
			t.Errorf("排除本批 ID 后不应再包含它自己的 simhash %x，实际 %v", sh, got2)
		}
	})

	t.Run("ListUnpushedByUser内容命中多个订阅源只返回一行", func(t *testing.T) {
		// 007 的 EXISTS 反查（而非 JOIN content_sources）的回归测试。JOIN 会为每个
		// 命中的订阅源各返回一行 → 同一条内容被重复打分（多花钱）、重复推送（用户
		// 收到多张一模一样的卡）。这是多用户下的必然场景：A 订「AI编程」、B 订
		// 「AI工具」，同一篇笔记两个源都抓到；单用户订两个重叠的源也一样触发。
		if err := st.AddSubscription(ctx, u.ID, src2ID); err != nil {
			t.Fatalf("AddSubscription(第二个源) 失败: %v", err)
		}
		key := "https://example.com/multi-hit-" + uuid.NewString()
		id, _, err := st.UpsertContentItem(ctx, &types.ContentItem{
			SourceID:     srcID,
			ExternalID:   "multi-a-" + uuid.NewString(),
			CanonicalKey: key,
			URL:          "https://example.com/multi-hit",
			Title:        "命中两个订阅源的内容",
			ContentHash:  "mhash-" + uuid.NewString(),
		})
		if err != nil {
			t.Fatalf("插入多源命中内容失败: %v", err)
		}
		// 第二个源也见到同一条内容：content_items 仍一份，content_sources 两行。
		if _, _, err := st.UpsertContentItem(ctx, &types.ContentItem{
			SourceID:     src2ID,
			ExternalID:   "multi-b-" + uuid.NewString(),
			CanonicalKey: key,
			URL:          "https://example.com/multi-hit?from=b",
			Title:        "命中两个订阅源的内容",
			ContentHash:  "mhash-" + uuid.NewString(),
		}); err != nil {
			t.Fatalf("登记第二个源的 appearance 失败: %v", err)
		}

		got, err := st.ListUnpushedByUser(ctx, u.ID, 100, 100)
		if err != nil {
			t.Fatalf("ListUnpushedByUser() 失败: %v", err)
		}
		var n int
		for _, ci := range got {
			if ci.ID == id {
				n++
			}
		}
		if n != 1 {
			t.Errorf("一条内容命中 2 个订阅源时应只返回 1 行，实际 %d 行（用 JOIN 而非 EXISTS 就会这样）", n)
		}
	})

	t.Run("ListUnpushedByUser按首发源限额", func(t *testing.T) {
		// 依赖前面子测试已在 srcID 下堆了多条未投递内容。perSourceCap 按 ci.source_id
		// （首发源）分区，防高产源把先抓的源永远挤出候选窗口。
		capped, err := st.ListUnpushedByUser(ctx, u.ID, 100, 1)
		if err != nil {
			t.Fatalf("ListUnpushedByUser(cap=1) 失败: %v", err)
		}
		counts := map[int64]int{}
		for _, ci := range capped {
			counts[ci.SourceID]++
		}
		for sid, c := range counts {
			if c > 1 {
				t.Errorf("perSourceCap=1 时源 %d 应最多 1 条，实际 %d 条", sid, c)
			}
		}
		// 限额必须真的在限：srcID 下未投递内容远多于 1 条，若放开限额还是只回 1 条，
		// 说明上面的"未超限"是假象（比如候选本来就空）。
		loose, err := st.ListUnpushedByUser(ctx, u.ID, 100, 100)
		if err != nil {
			t.Fatalf("ListUnpushedByUser(cap=100) 失败: %v", err)
		}
		var looseSrc1 int
		for _, ci := range loose {
			if ci.SourceID == srcID {
				looseSrc1++
			}
		}
		if looseSrc1 <= counts[srcID] {
			t.Errorf("放开限额后源 %d 应返回更多条目（cap=1 时 %d 条，cap=100 时 %d 条），限额似乎未生效",
				srcID, counts[srcID], looseSrc1)
		}
	})

	t.Run("schedule Insert→List→Get→Delete", func(t *testing.T) {
		schedID := "push-" + uuid.NewString()
		sc := &types.Schedule{
			ID:            schedID,
			UserID:        u.ID,
			NLDescription: "每天早8点推科技",
			SpecJSON:      json.RawMessage(`{"cron":"0 8 * * *","tz":"Asia/Shanghai"}`),
			ScopeJSON:     json.RawMessage(`{}`),
			Status:        types.ScheduleStatusActive,
		}
		if err := st.InsertSchedule(ctx, sc); err != nil {
			t.Fatalf("InsertSchedule() 失败: %v", err)
		}

		list, err := st.ListSchedulesByUser(ctx, u.ID)
		if err != nil {
			t.Fatalf("ListSchedulesByUser() 失败: %v", err)
		}
		var inList bool
		for _, got := range list {
			if got.ID == schedID {
				inList = true
			}
		}
		if !inList {
			t.Errorf("新建的调度 %s 未出现在列表中（共 %d 条）", schedID, len(list))
		}

		got, err := st.GetSchedule(ctx, schedID)
		if err != nil {
			t.Fatalf("GetSchedule() 失败: %v", err)
		}
		if got.UserID != u.ID || got.NLDescription != sc.NLDescription {
			t.Errorf("GetSchedule() 回读不一致：%+v", got)
		}

		if err := st.DeleteSchedule(ctx, schedID); err != nil {
			t.Fatalf("DeleteSchedule() 失败: %v", err)
		}
		_, err = st.GetSchedule(ctx, schedID)
		if err == nil {
			t.Fatal("删除后 GetSchedule() 应返回错误")
		}
		if !errors.Is(err, types.ErrNotFound) {
			t.Errorf("删除后错误应满足 errors.Is(err, types.ErrNotFound)，实际: %v", err)
		}
	})

	t.Run("推送幂等CreatePushBatchIdempotent复用批次", func(t *testing.T) {
		// 004 幂等地基核心行为一：同一 idempKey（= workflow traceID）两次调用返回同一 batch_id，
		// 使 Temporal 重试 Push Activity 时复用同一批次而非重复建批。
		idempKey := "trace-" + uuid.NewString()
		batchID1, err := st.CreatePushBatchIdempotent(ctx, u.ID, idempKey)
		if err != nil {
			t.Fatalf("CreatePushBatchIdempotent() 首次失败: %v", err)
		}
		batchID2, err := st.CreatePushBatchIdempotent(ctx, u.ID, idempKey)
		if err != nil {
			t.Fatalf("CreatePushBatchIdempotent() 二次失败: %v", err)
		}
		if batchID2 != batchID1 {
			t.Errorf("同 idempKey 应复用同一 batch_id：首次 %d，二次 %d", batchID1, batchID2)
		}

		// 004 幂等地基核心行为二：同一 (batch_id, content_item_id) 两次 InsertDeliveryIdempotent，
		// 第二次 existed=true，避免重试时重复投递同一条内容。
		// 先建一条内容条目拿到合法的 content_item_id（deliveries.content_item_id 有 FK）。
		ci := &types.ContentItem{
			SourceID:     srcID,
			ExternalID:   "ext-idem-" + uuid.NewString(),
			CanonicalKey: "https://example.com/idem-item-" + uuid.NewString(),
			URL:          "https://example.com/idem-item",
			Title:        "幂等测试内容",
			ContentHash:  "hash-idem-" + uuid.NewString(),
		}
		ciID, _, err := st.UpsertContentItem(ctx, ci)
		if err != nil {
			t.Fatalf("UpsertContentItem() 建内容失败: %v", err)
		}

		d := &types.Delivery{
			BatchID:       batchID1,
			UserID:        u.ID,
			ContentItemID: &ciID,
			Score:         42,
			CardJSON:      json.RawMessage(`{"k":"v"}`),
			Status:        types.DeliveryStatusPending,
		}
		id1, existed1, sentAlready1, err := st.InsertDeliveryIdempotent(ctx, d)
		if err != nil {
			t.Fatalf("InsertDeliveryIdempotent() 首插失败: %v", err)
		}
		if existed1 {
			t.Error("首次插入应 existed=false")
		}
		if sentAlready1 {
			t.Error("首次插入 status=pending，应 sentAlready=false")
		}

		id2, existed2, _, err := st.InsertDeliveryIdempotent(ctx, d)
		if err != nil {
			t.Fatalf("InsertDeliveryIdempotent() 二插失败: %v", err)
		}
		if !existed2 {
			t.Error("同 (batch_id, content_item_id) 二次插入应 existed=true")
		}
		if id2 != id1 {
			t.Errorf("重复投递应返回同 id：首次 %d，二次 %d", id1, id2)
		}
	})
}

// TestMigration007ContentIdentity 验证 007 在**有重复数据**的库上的合并结果
// （契约 §7 migration 段）。回填与合并是这次重构风险最高的部分：算错就是不可逆的
// 数据损坏（Down 明确不还原被合并的行），而它只在真 Postgres 上跑得出来。
//
// 为什么另建一个库而不是用 DATABASE_URL 指的那个：本测试必须先把 schema 降到 006
// 才能造出"重复数据"（007 之后 UNIQUE(canonical_key) 就插不进重复行了）。而 CI 是
// `go test ./...`——store / evolver 等包**并行**跑、共用同一个 vane_test 库，在共享库上
// down 到 006 会把并行包的 schema 抽掉。故本测试全程在自己的一次性库里进行，
// 结束即 DROP，对共享库零影响。
func TestMigration007ContentIdentity(t *testing.T) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("未设置 DATABASE_URL，跳过 007 迁移集成测试")
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

	// 先到 006：此刻 content_items 还是 UNIQUE(source_id, external_id)，能造重复。
	if _, err := provider.UpTo(ctx, 6); err != nil {
		t.Fatalf("迁移到 006 失败: %v", err)
	}

	// 造一份生产库的缩影（实测形态，见契约 §0）：
	//   - BBC 同一篇文章发了两个 guid（url 相同）→ 006 的 per-source 唯一挡不住；
	//   - 同一篇小红书笔记命中两个不同的源（url 带各自的 xsec_token，note_id 相同）；
	//   - 两条重复内容各自被投递过 → 合并后 deliveries 必须无悬挂。
	var userID, rssSrcID, xhsSrc1ID, xhsSrc2ID int64
	mustQueryRow(ctx, t, db, `INSERT INTO users (feishu_open_id, name) VALUES ('ou_mig_007', 'mig') RETURNING id`, &userID)
	mustQueryRow(ctx, t, db, `INSERT INTO sources (type, url) VALUES ('rss', 'https://bbc.com/feed') RETURNING id`, &rssSrcID)
	mustQueryRow(ctx, t, db, `INSERT INTO sources (type, url) VALUES ('tikhub_xhs', 'tikhub://xhs/search?keyword=AI编程') RETURNING id`, &xhsSrc1ID)
	mustQueryRow(ctx, t, db, `INSERT INTO sources (type, url) VALUES ('tikhub_xhs', 'tikhub://xhs/search?keyword=AI工具') RETURNING id`, &xhsSrc2ID)

	const bbcURL = "https://bbc.com/news/article-1"
	const noteID = "note_abc123"
	var bbcOldID, bbcNewID, xhsAID, xhsBID int64
	// BBC 同一篇、两个 guid：id 小者是首发，007 后应只剩它。
	mustQueryRow(ctx, t, db,
		`INSERT INTO content_items (source_id, external_id, url, title, content_hash)
		 VALUES ($1, 'guid-old', $2, 'BBC 文章', 'h1') RETURNING id`, &bbcOldID, rssSrcID, bbcURL)
	mustQueryRow(ctx, t, db,
		`INSERT INTO content_items (source_id, external_id, url, title, content_hash)
		 VALUES ($1, 'guid-new', $2, 'BBC 文章（更新）', 'h2') RETURNING id`, &bbcNewID, rssSrcID, bbcURL)
	// 同一篇笔记命中两个源：note_id 相同、url 各带不同的 xsec_token。
	mustQueryRow(ctx, t, db,
		`INSERT INTO content_items (source_id, external_id, url, title, content_hash)
		 VALUES ($1, $2, 'https://www.xiaohongshu.com/explore/x?xsec_token=aaa', '笔记', 'h3') RETURNING id`,
		&xhsAID, xhsSrc1ID, noteID)
	mustQueryRow(ctx, t, db,
		`INSERT INTO content_items (source_id, external_id, url, title, content_hash)
		 VALUES ($1, $2, 'https://www.xiaohongshu.com/explore/x?xsec_token=bbb', '笔记', 'h4') RETURNING id`,
		&xhsBID, xhsSrc2ID, noteID)

	// 两条 BBC 副本分属不同批次投递过：合并后两条投递都应指向幸存者，不留悬挂。
	var batch1, batch2 int64
	mustQueryRow(ctx, t, db, `INSERT INTO push_batches (user_id) VALUES ($1) RETURNING id`, &batch1, userID)
	mustQueryRow(ctx, t, db, `INSERT INTO push_batches (user_id) VALUES ($1) RETURNING id`, &batch2, userID)
	mustExec(ctx, t, db, `INSERT INTO deliveries (batch_id, user_id, content_item_id) VALUES ($1, $2, $3)`, batch1, userID, bbcOldID)
	mustExec(ctx, t, db, `INSERT INTO deliveries (batch_id, user_id, content_item_id) VALUES ($1, $2, $3)`, batch2, userID, bbcNewID)

	// ── 以下三种形态都曾把 007 跑炸过（真库复现），逐一钉住，防日后改回去 ──

	// (a) keep 来自源 A、≥2 个非 keep 来自同一个源 B。
	// 原写法用 `UPDATE content_sources … WHERE NOT EXISTS(keep 已有该源)` 搬 appearance，
	// 而 NOT EXISTS 读的是语句开始时的快照、看不见同语句里正在搬的兄弟行 →
	// 两行双双 SET 成 (keep, 同一源) → 撞 PK → 整个迁移回滚。
	const sharedURL = "https://news.example/shared"
	var exaSrcID, exaFirstID, rssDup1ID, rssDup2ID int64
	mustQueryRow(ctx, t, db, `INSERT INTO sources (type, url) VALUES ('exa', 'exa://search?q=news') RETURNING id`, &exaSrcID)
	mustQueryRow(ctx, t, db,
		`INSERT INTO content_items (source_id, external_id, url, title, content, content_hash)
		 VALUES ($1, 'exa-shared', $2, 'S', '短', 'hs1') RETURNING id`, &exaFirstID, exaSrcID, sharedURL)
	mustQueryRow(ctx, t, db,
		`INSERT INTO content_items (source_id, external_id, url, title, content, content_hash)
		 VALUES ($1, 'guid-s1', $2, 'S', '中等长度的正文', 'hs2') RETURNING id`, &rssDup1ID, rssSrcID, sharedURL)
	mustQueryRow(ctx, t, db,
		`INSERT INTO content_items (source_id, external_id, url, title, content, content_hash)
		 VALUES ($1, 'guid-s2', $2, 'S', '改稿之后明显更长的正文内容', 'hs3') RETURNING id`, &rssDup2ID, rssSrcID, sharedURL)

	// (b) 同一批次投递过两条后来被判为同一 canonical_key 的内容。
	// 004 建了 uq_deliveries_batch_content (batch_id, content_item_id)，无条件把两条
	// 都重指向 keep 会撞唯一索引 → 迁移回滚。触发条件真实：BBC 改稿重发时正文有变，
	// simhash 距离可能超阈值 → 两条同批存活、同批投递。
	var batch3 int64
	mustQueryRow(ctx, t, db, `INSERT INTO push_batches (user_id) VALUES ($1) RETURNING id`, &batch3, userID)
	mustExec(ctx, t, db, `INSERT INTO deliveries (batch_id, user_id, content_item_id) VALUES ($1, $2, $3)`, batch3, userID, rssDup1ID)
	mustExec(ctx, t, db, `INSERT INTO deliveries (batch_id, user_id, content_item_id) VALUES ($1, $2, $3)`, batch3, userID, rssDup2ID)

	// (c) llm_calls 是 content_items.id 的第三个引用者且**刻意不建 FK**（多态关联）——
	// 按外键排查必然漏掉它，删行时 DB 既不报错也不级联，悬挂完全静默。
	mustExec(ctx, t, db,
		`INSERT INTO llm_calls (trace_id, span_name, model, ref_type, ref_id, user_id)
		 VALUES ('mig-t1', 'score', 'm', 'content_item', $1, $2),
		        ('mig-t2', 'cardgen', 'm', 'content_item', $3, $2)`,
		rssDup1ID, userID, rssDup2ID)

	// 跑 007。
	if _, err := provider.UpTo(ctx, 7); err != nil {
		t.Fatalf("执行 007 迁移失败: %v", err)
	}

	// ① canonical_key 唯一且按源类型回填正确（rss 认 url、xhs 认 note_id）。
	var dupKeys int
	mustQueryRow(ctx, t, db,
		`SELECT count(*) FROM (SELECT canonical_key FROM content_items
		 GROUP BY canonical_key HAVING count(*) > 1) x`, &dupKeys)
	if dupKeys != 0 {
		t.Errorf("007 后 canonical_key 应全局唯一，实际有 %d 组重复", dupKeys)
	}
	var bbcKey, xhsKey string
	mustQueryRow(ctx, t, db, `SELECT canonical_key FROM content_items WHERE id = $1`, &bbcKey, bbcOldID)
	if bbcKey != bbcURL {
		t.Errorf("rss 的 canonical_key 应回填为 url：期望 %q，实际 %q", bbcURL, bbcKey)
	}
	mustQueryRow(ctx, t, db, `SELECT canonical_key FROM content_items WHERE id = $1`, &xhsKey, xhsAID)
	if xhsKey != noteID {
		t.Errorf("tikhub_xhs 的 canonical_key 应回填为 external_id(note_id)：期望 %q，实际 %q", noteID, xhsKey)
	}

	// ② 重复行被合并，保留 id 最小者（= 首发）。
	for _, tc := range []struct {
		name         string
		keep, merged int64
	}{
		{"BBC guid 漂移", bbcOldID, bbcNewID},
		{"同一笔记跨源命中", xhsAID, xhsBID},
	} {
		var kept, gone int
		mustQueryRow(ctx, t, db, `SELECT count(*) FROM content_items WHERE id = $1`, &kept, tc.keep)
		mustQueryRow(ctx, t, db, `SELECT count(*) FROM content_items WHERE id = $1`, &gone, tc.merged)
		if kept != 1 {
			t.Errorf("%s：应保留首发行 id=%d，实际未保留", tc.name, tc.keep)
		}
		if gone != 0 {
			t.Errorf("%s：重复行 id=%d 应被合并删除，实际仍在", tc.name, tc.merged)
		}
	}

	// ③ deliveries 零悬挂：指向的内容行必须存在。这是合并顺序（先搬后删）的核心
	// 保障——先删内容行的话 FK 的 ON DELETE SET NULL 会把投递的原文指针抹成 NULL，
	// 用户点"阅读原文"落空、信源质量分析断链。
	var dangling int
	mustQueryRow(ctx, t, db,
		`SELECT count(*) FROM deliveries d WHERE d.content_item_id IS NOT NULL
		    AND NOT EXISTS (SELECT 1 FROM content_items ci WHERE ci.id = d.content_item_id)`, &dangling)
	if dangling != 0 {
		t.Errorf("007 后 deliveries 不应有悬挂的 content_item_id，实际 %d 条", dangling)
	}
	// 跨批次的重复投递：两条都改指幸存者（不同批次不受 004 唯一索引约束）。
	var pointingToKeep int
	mustQueryRow(ctx, t, db,
		`SELECT count(*) FROM deliveries WHERE content_item_id = $1`, &pointingToKeep, bbcOldID)
	if pointingToKeep != 2 {
		t.Errorf("两条 BBC 投递都应改指幸存者 id=%d，实际 %d 条", bbcOldID, pointingToKeep)
	}

	// ③' 同批次的重复投递（种子 (b)）：004 的 uq_deliveries_batch_content 不允许
	// (batch, keep) 出现两次，故只保留最早一条指向 keep、另一条置 NULL。
	// **行不能删**——数据是资产，且 card_json 仍记录着当时推了什么；
	// content_item_id 可空是既有语义（代码有"原文已过期清理"分支）。
	var inBatch3, nulledInBatch3 int
	mustQueryRow(ctx, t, db,
		`SELECT count(*) FROM deliveries WHERE batch_id = $1`, &inBatch3, batch3)
	if inBatch3 != 2 {
		t.Errorf("同批两条投递都应保留（数据是资产，不删行），实际 %d 条", inBatch3)
	}
	mustQueryRow(ctx, t, db,
		`SELECT count(*) FROM deliveries WHERE batch_id = $1 AND content_item_id IS NULL`, &nulledInBatch3, batch3)
	if nulledInBatch3 != 1 {
		t.Errorf("同批重复投递应恰好 1 条被置 NULL（另一条指向 keep），实际 %d 条", nulledInBatch3)
	}

	// ④' 最长的正文赢：keep 是 id 最小者（首发），但首发不等于正文最全——
	// 种子 (a) 里 keep 的正文最短、被合并行的正文最长。直接删非 keep 行会把
	// 库里已有的、可能花过钱补全的最好版本丢掉。与 UpsertContentItem 同规则。
	var keptContent, keptHash string
	mustQueryRow(ctx, t, db,
		`SELECT content FROM content_items WHERE id = $1`, &keptContent, exaFirstID)
	mustQueryRow(ctx, t, db,
		`SELECT content_hash FROM content_items WHERE id = $1`, &keptHash, exaFirstID)
	if keptContent != "改稿之后明显更长的正文内容" {
		t.Errorf("合并应保留最长的正文，实际 %q", keptContent)
	}
	// 指纹必须跟着正文一起换：只换正文会让精确/近似去重都按旧版本判、静默失准。
	if keptHash != "hs3" {
		t.Errorf("content_hash 应随正文一起更新为 hs3，实际 %q", keptHash)
	}
	// 首发源不被覆盖：正文换了，但"谁先发现"仍是 Exa 源。
	var keptSourceID int64
	mustQueryRow(ctx, t, db, `SELECT source_id FROM content_items WHERE id = $1`, &keptSourceID, exaFirstID)
	if keptSourceID != exaSrcID {
		t.Errorf("首发源应保持为 Exa 源 %d，实际 %d", exaSrcID, keptSourceID)
	}

	// ⑤' llm_calls 的多态引用零悬挂（无 FK，DB 不会替我们发现）。
	var danglingLLM int
	mustQueryRow(ctx, t, db,
		`SELECT count(*) FROM llm_calls lc WHERE lc.ref_type = 'content_item'
		    AND NOT EXISTS (SELECT 1 FROM content_items ci WHERE ci.id = lc.ref_id)`, &danglingLLM)
	if danglingLLM != 0 {
		t.Errorf("llm_calls 的 content_item 引用不应悬挂，实际 %d 条", danglingLLM)
	}

	// ④ content_sources 覆盖全部 appearance：合并前每条内容行都是一次"某源见过它"，
	// 一条都不能丢——丢了就等于那个源的订阅者再也看不到这条内容。
	// 笔记两个源各一条；BBC 同源两行合并后按 PK 只余一条。
	var xhsAppearances int
	mustQueryRow(ctx, t, db,
		`SELECT count(*) FROM content_sources WHERE content_item_id = $1`, &xhsAppearances, xhsAID)
	if xhsAppearances != 2 {
		t.Errorf("笔记被 2 个源见过，应有 2 条 appearance，实际 %d 条", xhsAppearances)
	}
	for _, sid := range []int64{xhsSrc1ID, xhsSrc2ID} {
		var n int
		mustQueryRow(ctx, t, db,
			`SELECT count(*) FROM content_sources WHERE content_item_id = $1 AND source_id = $2`,
			&n, xhsAID, sid)
		if n != 1 {
			t.Errorf("源 %d 见过这条笔记，appearance 应存在（实际 %d 条）", sid, n)
		}
	}
	// appearance 不能指向已被删除的内容行。
	var orphanAppearances int
	mustQueryRow(ctx, t, db,
		`SELECT count(*) FROM content_sources cs
		 WHERE NOT EXISTS (SELECT 1 FROM content_items ci WHERE ci.id = cs.content_item_id)`, &orphanAppearances)
	if orphanAppearances != 0 {
		t.Errorf("content_sources 不应指向已删除的内容行，实际 %d 条", orphanAppearances)
	}

	// ⑤ 身份唯一约束真的生效（前面的"无重复"可能只是数据恰好不重复）。
	_, err = db.ExecContext(ctx,
		`INSERT INTO content_items (source_id, external_id, canonical_key, url, title, content_hash)
		 VALUES ($1, 'guid-dup', $2, 'x', 'x', 'h9')`, rssSrcID, bbcURL)
	if err == nil {
		t.Error("插入重复 canonical_key 应被 uq_content_items_canonical 拒绝，实际成功了")
	}
}

// createScratchDB 在 dbURL 所在实例上建一个一次性库，返回它的连接串与清理函数。
// 库名带 uuid：CI 里同一实例可能同时跑多个 job。
func createScratchDB(ctx context.Context, t *testing.T, dbURL string) (string, func()) {
	t.Helper()
	admin, err := sql.Open("pgx", dbURL)
	if err != nil {
		t.Fatalf("打开管理连接失败: %v", err)
	}
	defer admin.Close()

	name := "vane_mig007_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	// CREATE/DROP DATABASE 不能在事务里跑，用裸 Exec。
	if _, err := admin.ExecContext(ctx, `CREATE DATABASE `+pgQuoteIdent(name)); err != nil {
		t.Skipf("无法创建一次性库（当前账号可能无 CREATEDB 权限），跳过 007 迁移测试: %v", err)
	}

	u, err := url.Parse(dbURL)
	if err != nil {
		t.Fatalf("解析 DATABASE_URL 失败: %v", err)
	}
	u.Path = "/" + name

	return u.String(), func() {
		cleanup, err := sql.Open("pgx", dbURL)
		if err != nil {
			t.Logf("清理一次性库 %s：打开连接失败: %v", name, err)
			return
		}
		defer cleanup.Close()
		// WITH (FORCE) 踢掉残留连接，否则 DROP 会因"数据库正被访问"失败。
		if _, err := cleanup.ExecContext(context.WithoutCancel(ctx),
			`DROP DATABASE IF EXISTS `+pgQuoteIdent(name)+` WITH (FORCE)`); err != nil {
			t.Logf("清理一次性库 %s 失败（需手工删）: %v", name, err)
		}
	}
}

// pgQuoteIdent 给标识符加双引号并转义内部双引号。库名由本测试用 uuid 生成、
// 不含用户输入，这里只是不留下"拼接标识符"的坏示范。
func pgQuoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

// mustQueryRow 查一行一列到 dst，失败即 Fatal（迁移测试里任何一步查不出来都没法继续）。
func mustQueryRow(ctx context.Context, t *testing.T, db *sql.DB, query string, dst any, args ...any) {
	t.Helper()
	if err := db.QueryRowContext(ctx, query, args...).Scan(dst); err != nil {
		t.Fatalf("查询失败 %q: %v", query, err)
	}
}

// mustExec 执行一条写语句，失败即 Fatal。
func mustExec(ctx context.Context, t *testing.T, db *sql.DB, query string, args ...any) {
	t.Helper()
	if _, err := db.ExecContext(ctx, query, args...); err != nil {
		t.Fatalf("执行失败 %q: %v", query, err)
	}
}
