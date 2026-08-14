package store

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/YouToco/vane/types"
)

// TestClampA2APageSize 纯单测：PageSize 钳制 [1,200]、<=0 → 50（契约 §3）。
// 上界 200 在门控测试里要 201 行才观测得到，钳制逻辑抽成函数后在此表驱动钉死。
func TestClampA2APageSize(t *testing.T) {
	cases := []struct {
		in, want int
	}{
		{-1, 50},
		{0, 50},
		{1, 1},
		{50, 50},
		{200, 200},
		{201, 200},
		{100000, 200},
	}
	for _, c := range cases {
		if got := clampA2APageSize(c.in); got != c.want {
			t.Errorf("clampA2APageSize(%d) = %d，期望 %d", c.in, got, c.want)
		}
	}
}

// TestA2ACursorRoundTrip 纯单测：键集游标编解码可逆（契约 §4.1"格式自定但要可逆、不透明"）。
func TestA2ACursorRoundTrip(t *testing.T) {
	cases := []struct {
		name string
		at   time.Time
		id   string
	}{
		{"常规", time.Date(2026, 7, 17, 8, 30, 0, 123456000, time.UTC), "task-" + uuid.NewString()},
		{"零纳秒", time.Unix(1700000000, 0), "t1"},
		{"id 含分隔符", time.Unix(1700000000, 42000), "a|b|c"},
		{"1970 之前", time.Unix(-1, 0), "old"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			token := encodeA2ACursor(c.at, c.id)
			gotAt, gotID, err := decodeA2ACursor(token)
			if err != nil {
				t.Fatalf("decodeA2ACursor(%q) 失败: %v", token, err)
			}
			// timestamptz 只有微秒精度，UnixMicro 口径比较。
			if gotAt.UnixMicro() != c.at.UnixMicro() {
				t.Errorf("时间往返不一致：入 %d，出 %d", c.at.UnixMicro(), gotAt.UnixMicro())
			}
			if gotID != c.id {
				t.Errorf("id 往返不一致：入 %q，出 %q", c.id, gotID)
			}
		})
	}

	// 非法游标必须报 CodeValidation，不能进 SQL：
	// 非法 base64 / 无分隔符 / 微秒段非数字，三种坏法各一。
	bads := []string{
		"!!!非法base64!!!",
		base64.RawURLEncoding.EncodeToString([]byte("no-separator")),
		base64.RawURLEncoding.EncodeToString([]byte("notanumber|x")),
	}
	for _, bad := range bads {
		if _, _, err := decodeA2ACursor(bad); !errors.Is(err, types.ErrValidation) {
			t.Errorf("decodeA2ACursor(%q) 应返回 ErrValidation，实际: %v", bad, err)
		}
	}
}

// TestA2ATaskStore 是 DATABASE_URL 门控的集成测试（无则跳过，契约 §9.3），覆盖
// a2a_tasks 五方法：CRUD 往返、Create 冲突、乐观并发一胜一负、Update 回查区分
// NotFound/Conflict、List 三谓词/排序/钳制/键集游标翻页/total、Count。
func TestA2ATaskStore(t *testing.T) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("未设置 DATABASE_URL，跳过 a2a_tasks store 集成测试")
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

	// 测试数据用固定前缀 + uuid 后缀；a2a_tasks 无 FK，一条语句可清干净。
	prefix := "test_a2a_" + uuid.NewString() + "_"
	newID := func(n string) string { return prefix + n }
	t.Cleanup(func() {
		ctx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(ctx, t, st, `DELETE FROM a2a_tasks WHERE id LIKE $1`, prefix+"%")
	})

	t.Run("CRUD往返与Create冲突", func(t *testing.T) {
		id := newID("crud")
		created := &types.A2ATask{
			ID:        id,
			ContextID: prefix + "ctx-crud",
			Status:    "TASK_STATE_SUBMITTED",
			Task:      json.RawMessage(`{"id":"` + id + `","status":{"state":"TASK_STATE_SUBMITTED"}}`),
		}
		if err := st.CreateA2ATask(ctx, created); err != nil {
			t.Fatalf("CreateA2ATask() 失败: %v", err)
		}
		// RETURNING 回填：version 走表默认 1，时间戳有值。
		if created.Version != 1 {
			t.Errorf("新建任务 version 应为 1，实际 %d", created.Version)
		}
		if created.CreatedAt.IsZero() || created.UpdatedAt.IsZero() {
			t.Error("新建任务 created_at/updated_at 应由表默认值回填")
		}

		got, err := st.GetA2ATask(ctx, id)
		if err != nil {
			t.Fatalf("GetA2ATask() 失败: %v", err)
		}
		if got.ID != id || got.ContextID != created.ContextID || got.Status != "TASK_STATE_SUBMITTED" || got.Version != 1 {
			t.Errorf("Get 回读不一致: %+v", got)
		}
		// JSONB 不保证键序，语义比较。
		var payload map[string]any
		if err := json.Unmarshal(got.Task, &payload); err != nil {
			t.Fatalf("回读 task 解析失败: %v（原文 %s）", err, got.Task)
		}
		if payload["id"] != id {
			t.Errorf("task 载荷回读不一致: %s", got.Task)
		}

		// 同 id 再建 → CodeConflict（契约 §4.1，适配层译 ErrTaskAlreadyExists）。
		dup := &types.A2ATask{ID: id, ContextID: "x", Status: "TASK_STATE_SUBMITTED", Task: json.RawMessage(`{}`)}
		if err := st.CreateA2ATask(ctx, dup); !errors.Is(err, types.ErrConflict) {
			t.Errorf("重复 id 建任务应 ErrConflict，实际: %v", err)
		}

		// 不存在的 id → CodeNotFound。
		if _, err := st.GetA2ATask(ctx, newID("nonexistent")); !errors.Is(err, types.ErrNotFound) {
			t.Errorf("查不存在任务应 ErrNotFound，实际: %v", err)
		}
	})

	t.Run("Update乐观并发语义与回查区分", func(t *testing.T) {
		id := newID("upd")
		task := &types.A2ATask{
			ID:        id,
			ContextID: prefix + "ctx-upd",
			Status:    "TASK_STATE_SUBMITTED",
			Task:      json.RawMessage(`{"v":1}`),
		}
		if err := st.CreateA2ATask(ctx, task); err != nil {
			t.Fatalf("CreateA2ATask() 失败: %v", err)
		}

		if err := st.UpdateA2ATask(ctx, id, 1, "TASK_STATE_WORKING", json.RawMessage(`{"v":2}`)); err != nil {
			t.Fatalf("UpdateA2ATask() 失败: %v", err)
		}
		got, err := st.GetA2ATask(ctx, id)
		if err != nil {
			t.Fatalf("更新后 GetA2ATask() 失败: %v", err)
		}
		// 成功后新版本恒 = expectedVersion+1（契约 §4.1，适配层返回 TaskVersion 依赖它）。
		if got.Version != 2 {
			t.Errorf("更新后 version 应为 2，实际 %d", got.Version)
		}
		if got.Status != "TASK_STATE_WORKING" {
			t.Errorf("更新后 status 应为 WORKING，实际 %q", got.Status)
		}
		if !got.UpdatedAt.After(task.UpdatedAt) {
			t.Errorf("Update 应刷新 updated_at：建 %v，更 %v", task.UpdatedAt, got.UpdatedAt)
		}

		// 拿旧版本再更 → 回查有行 → CodeConflict。
		if err := st.UpdateA2ATask(ctx, id, 1, "TASK_STATE_COMPLETED", json.RawMessage(`{}`)); !errors.Is(err, types.ErrConflict) {
			t.Errorf("旧版本更新应 ErrConflict，实际: %v", err)
		}
		// 不存在的 id → 回查无行 → CodeNotFound。
		if err := st.UpdateA2ATask(ctx, newID("ghost"), 1, "TASK_STATE_COMPLETED", json.RawMessage(`{}`)); !errors.Is(err, types.ErrNotFound) {
			t.Errorf("更新不存在任务应 ErrNotFound，实际: %v", err)
		}
	})

	t.Run("乐观并发两写一胜一负", func(t *testing.T) {
		id := newID("race")
		if err := st.CreateA2ATask(ctx, &types.A2ATask{
			ID:        id,
			ContextID: prefix + "ctx-race",
			Status:    "TASK_STATE_SUBMITTED",
			Task:      json.RawMessage(`{}`),
		}); err != nil {
			t.Fatalf("CreateA2ATask() 失败: %v", err)
		}

		// channel 编排时序：两个 goroutine 同时放行，都以 expectedVersion=1 更新。
		// 行锁保证只有一个 UPDATE 命中 WHERE version=1，另一个必然 RowsAffected==0
		// → 回查有行 → CodeConflict。
		start := make(chan struct{})
		errs := make(chan error, 2)
		for i := range 2 {
			go func() {
				<-start
				errs <- st.UpdateA2ATask(ctx, id, 1, "TASK_STATE_WORKING",
					json.RawMessage(fmt.Sprintf(`{"writer":%d}`, i)))
			}()
		}
		close(start)

		var wins, conflicts int
		for range 2 {
			switch err := <-errs; {
			case err == nil:
				wins++
			case errors.Is(err, types.ErrConflict):
				conflicts++
			default:
				t.Fatalf("并发更新出现非 Conflict 错误: %v", err)
			}
		}
		if wins != 1 || conflicts != 1 {
			t.Errorf("并发更新应一胜一负：实际 %d 胜 %d 冲突", wins, conflicts)
		}
		got, err := st.GetA2ATask(ctx, id)
		if err != nil {
			t.Fatalf("并发更新后 GetA2ATask() 失败: %v", err)
		}
		if got.Version != 2 {
			t.Errorf("并发更新后 version 应为 2（只成功一次），实际 %d", got.Version)
		}
	})

	t.Run("List谓词排序游标与total", func(t *testing.T) {
		ctxL := prefix + "ctx-list"
		ctxM := prefix + "ctx-other"
		base := time.Now().UTC().Truncate(time.Microsecond)

		// 5 条同 context 任务：偶数下标 COMPLETED（3 条）、奇数 SUBMITTED（2 条）。
		// created_at/updated_at 直写为 base - i 分钟，把排序与游标测试变成确定性的。
		ids := make([]string, 5)
		for i := range ids {
			ids[i] = newID(fmt.Sprintf("list-%d", i))
			status := "TASK_STATE_COMPLETED"
			if i%2 == 1 {
				status = "TASK_STATE_SUBMITTED"
			}
			if err := st.CreateA2ATask(ctx, &types.A2ATask{
				ID: ids[i], ContextID: ctxL, Status: status,
				Task: json.RawMessage(fmt.Sprintf(`{"n":%d}`, i)),
			}); err != nil {
				t.Fatalf("建第 %d 条列表任务失败: %v", i, err)
			}
			at := base.Add(-time.Duration(i) * time.Minute)
			if _, err := st.pool.Exec(ctx,
				`UPDATE a2a_tasks SET created_at = $2, updated_at = $2 WHERE id = $1`,
				ids[i], at); err != nil {
				t.Fatalf("固定第 %d 条任务时间失败: %v", i, err)
			}
		}
		// 另一 context 的干扰行：ContextID 谓词必须把它隔离在外。
		if err := st.CreateA2ATask(ctx, &types.A2ATask{
			ID: newID("other"), ContextID: ctxM, Status: "TASK_STATE_COMPLETED",
			Task: json.RawMessage(`{}`),
		}); err != nil {
			t.Fatalf("建干扰任务失败: %v", err)
		}

		idsOf := func(items []types.A2ATask) []string {
			out := make([]string, len(items))
			for i, it := range items {
				out[i] = it.ID
			}
			return out
		}

		// ContextID 过滤 + 排序 + PageSize<=0 缺省 50 + 不满页 next 空。
		items, total, next, err := st.ListA2ATasks(ctx, types.A2ATaskQuery{ContextID: ctxL})
		if err != nil {
			t.Fatalf("ListA2ATasks(ContextID) 失败: %v", err)
		}
		if total != 5 {
			t.Errorf("ContextID 过滤 total 应为 5，实际 %d", total)
		}
		if next != "" {
			t.Errorf("不满页 next 应为空串，实际 %q", next)
		}
		if got := idsOf(items); len(got) != 5 ||
			got[0] != ids[0] || got[1] != ids[1] || got[2] != ids[2] || got[3] != ids[3] || got[4] != ids[4] {
			t.Errorf("排序应为 created_at DESC：期望 %v，实际 %v", ids, got)
		}

		// Status 谓词（与 ContextID 组合）。
		items, total, _, err = st.ListA2ATasks(ctx, types.A2ATaskQuery{
			ContextID: ctxL, Status: "TASK_STATE_SUBMITTED",
		})
		if err != nil {
			t.Fatalf("ListA2ATasks(Status) 失败: %v", err)
		}
		if len(items) != 2 || total != 2 {
			t.Errorf("SUBMITTED 过滤应 2 条 total=2，实际 %d 条 total=%d", len(items), total)
		}
		for _, it := range items {
			if it.Status != "TASK_STATE_SUBMITTED" {
				t.Errorf("Status 过滤漏出 %q（id=%s）", it.Status, it.ID)
			}
		}

		// StatusTimestampAfter 谓词：updated_at > base-90s → 只剩 i=0（base）与 i=1（base-60s）。
		items, total, _, err = st.ListA2ATasks(ctx, types.A2ATaskQuery{
			ContextID: ctxL, StatusTimestampAfter: base.Add(-90 * time.Second),
		})
		if err != nil {
			t.Fatalf("ListA2ATasks(StatusTimestampAfter) 失败: %v", err)
		}
		if got := idsOf(items); len(got) != 2 || total != 2 || got[0] != ids[0] || got[1] != ids[1] {
			t.Errorf("时间谓词应恰好 [%s %s] total=2，实际 %v total=%d", ids[0], ids[1], got, total)
		}

		// 键集游标翻页：PageSize=2 → 2+2+1，不重不漏、末页 next 空、每页 total 都是全集 5。
		var seen []string
		token := ""
		for page := 1; ; page++ {
			items, total, next, err = st.ListA2ATasks(ctx, types.A2ATaskQuery{
				ContextID: ctxL, PageSize: 2, PageToken: token,
			})
			if err != nil {
				t.Fatalf("翻页第 %d 页失败: %v", page, err)
			}
			if total != 5 {
				t.Errorf("第 %d 页 total 应为 5（全集大小与游标无关），实际 %d", page, total)
			}
			seen = append(seen, idsOf(items)...)
			if next == "" {
				if len(items) == 2 {
					t.Errorf("第 %d 页满页却没给 next", page)
				}
				break
			}
			if len(items) != 2 {
				t.Fatalf("给了 next 的第 %d 页应满页 2 条，实际 %d", page, len(items))
			}
			token = next
			if page > 5 {
				t.Fatal("翻页未收敛（游标可能循环）")
			}
		}
		if len(seen) != 5 {
			t.Fatalf("翻页并集应恰好 5 条（不重不漏），实际 %d: %v", len(seen), seen)
		}
		for i, id := range seen {
			if id != ids[i] {
				t.Errorf("翻页第 %d 条应为 %s，实际 %s", i, ids[i], id)
			}
		}

		// 末页恰好满页：COMPLETED 3 条、PageSize=3 → 满页给 next，续查 0 条 next 空。
		items, _, next, err = st.ListA2ATasks(ctx, types.A2ATaskQuery{
			ContextID: ctxL, Status: "TASK_STATE_COMPLETED", PageSize: 3,
		})
		if err != nil {
			t.Fatalf("满页末页首查失败: %v", err)
		}
		if len(items) != 3 || next == "" {
			t.Fatalf("COMPLETED 首页应满页 3 条且给 next，实际 %d 条 next=%q", len(items), next)
		}
		items, total, next, err = st.ListA2ATasks(ctx, types.A2ATaskQuery{
			ContextID: ctxL, Status: "TASK_STATE_COMPLETED", PageSize: 3, PageToken: next,
		})
		if err != nil {
			t.Fatalf("满页末页续查失败: %v", err)
		}
		if len(items) != 0 || next != "" || total != 3 {
			t.Errorf("满页后的续查应 0 条 next 空 total=3，实际 %d 条 next=%q total=%d", len(items), next, total)
		}

		// 非法游标：报 Validation，不进 SQL。
		if _, _, _, err := st.ListA2ATasks(ctx, types.A2ATaskQuery{
			ContextID: ctxL, PageToken: "损坏的游标!!!",
		}); !errors.Is(err, types.ErrValidation) {
			t.Errorf("非法游标应 ErrValidation，实际: %v", err)
		}
	})

	t.Run("Count只读计数", func(t *testing.T) {
		n, err := st.CountA2ATasks(ctx)
		if err != nil {
			t.Fatalf("CountA2ATasks() 失败: %v", err)
		}
		// 共享测试库可能有别人的行，只断言下界：本测试至少建了 9 条。
		var mine int64
		if err := st.pool.QueryRow(ctx,
			`SELECT count(*) FROM a2a_tasks WHERE id LIKE $1`, prefix+"%").Scan(&mine); err != nil {
			t.Fatalf("统计本测试行数失败: %v", err)
		}
		if n < mine {
			t.Errorf("CountA2ATasks()=%d 小于本测试自建的 %d 行", n, mine)
		}
	})

	t.Run("FailStale只清超龄非终态", func(t *testing.T) {
		// 三行：超龄 WORKING（应清）、超龄 SUBMITTED（应清）、超龄 COMPLETED（终态，不动）。
		mk := func(name, status string, age time.Duration) string {
			id := newID(name)
			if _, err := st.pool.Exec(ctx,
				`INSERT INTO a2a_tasks (id, context_id, status, task, updated_at)
				 VALUES ($1,$2,$3,$4, now() - $5::interval)`,
				id, prefix+"ctx-stale", status,
				json.RawMessage(`{"id":"`+id+`","status":{"state":"`+status+`"}}`),
				fmt.Sprintf("%d seconds", int(age.Seconds()))); err != nil {
				t.Fatalf("插入 %s 失败: %v", name, err)
			}
			return id
		}
		working := mk("stale_working", "TASK_STATE_WORKING", 20*time.Minute)
		submitted := mk("stale_submitted", "TASK_STATE_SUBMITTED", 20*time.Minute)
		completed := mk("stale_completed", "TASK_STATE_COMPLETED", 20*time.Minute)
		fresh := mk("fresh_working", "TASK_STATE_WORKING", 1*time.Minute) // 未超龄，不动

		n, err := st.FailStaleA2ATasks(ctx, time.Now().Add(-15*time.Minute))
		if err != nil {
			t.Fatalf("FailStaleA2ATasks() 失败: %v", err)
		}
		if n != 2 {
			t.Errorf("应恰清 2 行（超龄 WORKING+SUBMITTED），实得 %d", n)
		}
		statusOf := func(id string) (string, string, string) {
			g, err := st.GetA2ATask(ctx, id)
			if err != nil {
				t.Fatalf("GetA2ATask(%s) 失败: %v", id, err)
			}
			var payload struct {
				Status struct {
					State     string `json:"state"`
					Timestamp string `json:"timestamp"`
				} `json:"status"`
			}
			if err := json.Unmarshal(g.Task, &payload); err != nil {
				t.Fatalf("解析 task JSONB(%s) 失败: %v", id, err)
			}
			return g.Status, payload.Status.State, payload.Status.Timestamp
		}
		for _, id := range []string{working, submitted} {
			column, embedded, timestamp := statusOf(id)
			if column != "TASK_STATE_FAILED" || embedded != "TASK_STATE_FAILED" || timestamp == "" {
				t.Errorf("超龄任务 %s 应在提取列/JSONB 同时置 FAILED 并带时间，实得 column=%q embedded=%q timestamp=%q",
					id, column, embedded, timestamp)
			}
		}
		completedColumn, completedEmbedded, _ := statusOf(completed)
		if completedColumn != "TASK_STATE_COMPLETED" || completedEmbedded != "TASK_STATE_COMPLETED" {
			t.Error("终态 COMPLETED 不得被动")
		}
		freshColumn, freshEmbedded, _ := statusOf(fresh)
		if freshColumn != "TASK_STATE_WORKING" || freshEmbedded != "TASK_STATE_WORKING" {
			t.Error("未超龄的 WORKING 不得被动（防误杀多实例在飞任务）")
		}
	})
}
