package store

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/google/uuid"

	"github.com/YouToco/vane/types"
)

// TestUpsertSourceConfigNotClobbered 锁死 UpsertSource 的 config 语义：**既有值胜出，
// 只补缺键**（`EXCLUDED.config || sources.config`）。
//
// 这里防的是一处真实的数据损坏面。原实现是 `config = EXCLUDED.config`（整个替换），
// 两类受害者：
//
//	① 带外调优字段——lookback_days / num_results 被 fetcher 读取，却从不由 sourcespec
//	  写入（是人工调的）。任何人重新添加同一个源，config 就被一个不含这些键的新对象
//	  整体替换，调优静默回默认。**单用户今天就能踩到**，生产 id=2/9 正带着这样的值。
//	② 跨用户改写——A、B 订阅同一个源时共用一行 source（跨租户共享的客观事实，I-T1），
//	  "后写者赢" 等于把 A 的意图交给 B 支配。
//
// 覆盖不会报错、不会崩溃，只是让抓取行为悄悄变成别人要的样子——所以必须由测试兜住。
func TestUpsertSourceConfigNotClobbered(t *testing.T) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("未设置 DATABASE_URL，跳过 source config 集成测试")
	}
	ctx := t.Context()
	if err := Migrate(ctx, dbURL); err != nil {
		t.Fatalf("Migrate() 失败: %v", err)
	}
	st, err := New(ctx, dbURL)
	if err != nil {
		t.Fatalf("New() 失败: %v", err)
	}
	registerStoreClose(t, st)

	srcURL := "vane://web/search?q=cfg-test-" + uuid.NewString()

	// 第一位写入者：带着人工调过的 lookback_days / num_results（模拟生产 id=9 的形态）。
	firstID, _, err := st.UpsertSource(ctx, &types.Source{
		Platform:   types.PlatformWeb,
		Capability: types.CapSearch,
		URL:        srcURL,
		Title:      "第一位写入者",
		Status:     types.SourceStatusActive,
		Config: json.RawMessage(
			`{"query":"cfg-test","lookback_days":-1,"num_results":15}`),
	})
	if err != nil {
		t.Fatalf("首次 upsert 失败: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(ctx, t, st, `DELETE FROM sources WHERE id = $1`, firstID)
	})

	// 第二位写入者：同一个 url（同一幂等键），config 里没有那两个带外键。
	// 这正是 agent 加源 / API 订阅重复添加时会发生的事——sourcespec 从不产出它们。
	secondID, updated, err := st.UpsertSource(ctx, &types.Source{
		Platform:   types.PlatformWeb,
		Capability: types.CapSearch,
		URL:        srcURL,
		Title:      "第二位写入者",
		Status:     types.SourceStatusActive,
		Config:     json.RawMessage(`{"query":"cfg-test"}`),
	})
	if err != nil {
		t.Fatalf("二次 upsert 失败: %v", err)
	}
	if secondID != firstID {
		t.Fatalf("同一 url 应命中同一行：first=%d second=%d", firstID, secondID)
	}
	if !updated {
		t.Errorf("二次 upsert 应报告命中既有行（updated=true），实得 false")
	}

	var raw []byte
	if err := st.pool.QueryRow(ctx,
		`SELECT config FROM sources WHERE id = $1`, firstID).Scan(&raw); err != nil {
		t.Fatalf("回读 config 失败: %v", err)
	}
	var got struct {
		Query        string `json:"query"`
		LookbackDays *int   `json:"lookback_days"`
		NumResults   *int   `json:"num_results"`
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("解析 config 失败: %v（原文 %s）", err, raw)
	}

	if got.LookbackDays == nil {
		t.Errorf("lookback_days 被抹掉了——这正是被修的 bug：重新添加同一个源不得重置带外调优。config=%s", raw)
	} else if *got.LookbackDays != -1 {
		t.Errorf("lookback_days = %d，期望保持 -1（不限）；config=%s", *got.LookbackDays, raw)
	}
	if got.NumResults == nil {
		t.Errorf("num_results 被抹掉了；config=%s", raw)
	} else if *got.NumResults != 15 {
		t.Errorf("num_results = %d，期望保持 15；config=%s", *got.NumResults, raw)
	}
	if got.Query != "cfg-test" {
		t.Errorf("query = %q，期望 %q；config=%s", got.Query, "cfg-test", raw)
	}
}

// TestUpsertSourceConfigFillsMissingKeys 是上一条的另一半：先到先得**不等于**冻结。
// 既有行缺的键，后来者仍应补上——否则新增一个配置项后，所有存量源都永远拿不到它，
// 而这种"只在老数据上失效"的行为极难在开发期被发现。
func TestUpsertSourceConfigFillsMissingKeys(t *testing.T) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("未设置 DATABASE_URL，跳过 source config 集成测试")
	}
	ctx := t.Context()
	if err := Migrate(ctx, dbURL); err != nil {
		t.Fatalf("Migrate() 失败: %v", err)
	}
	st, err := New(ctx, dbURL)
	if err != nil {
		t.Fatalf("New() 失败: %v", err)
	}
	registerStoreClose(t, st)

	srcURL := "vane://web/search?q=cfg-fill-" + uuid.NewString()

	id, _, err := st.UpsertSource(ctx, &types.Source{
		Platform: types.PlatformWeb, Capability: types.CapSearch, URL: srcURL,
		Title: "补键测试", Status: types.SourceStatusActive,
		Config: json.RawMessage(`{"query":"fill"}`),
	})
	if err != nil {
		t.Fatalf("首次 upsert 失败: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(ctx, t, st, `DELETE FROM sources WHERE id = $1`, id)
	})

	if _, _, err := st.UpsertSource(ctx, &types.Source{
		Platform: types.PlatformWeb, Capability: types.CapSearch, URL: srcURL,
		Title: "补键测试", Status: types.SourceStatusActive,
		Config: json.RawMessage(`{"query":"fill","category":"news"}`),
	}); err != nil {
		t.Fatalf("二次 upsert 失败: %v", err)
	}

	var raw []byte
	if err := st.pool.QueryRow(ctx,
		`SELECT config FROM sources WHERE id = $1`, id).Scan(&raw); err != nil {
		t.Fatalf("回读 config 失败: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("解析 config 失败: %v", err)
	}
	if got["category"] != "news" {
		t.Errorf("既有行缺失的键未被补上：category=%v，期望 news；config=%s", got["category"], raw)
	}
	if got["query"] != "fill" {
		t.Errorf("既有键被改动：query=%v；config=%s", got["query"], raw)
	}
}
