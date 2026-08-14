package store

import (
	"encoding/json"
	"errors"
	"os"
	"testing"

	"github.com/YouToco/vane/types"
)

// TestSettings 是集成测试：依赖真实 Postgres（CI 的 test job 提供 DATABASE_URL）。
// 覆盖契约三点：Put→Get 往返、不存在返回 NotFound、upsert 覆盖。
func TestSettings(t *testing.T) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("未设置 DATABASE_URL，跳过 settings 集成测试")
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

	// key 带测试前缀并在结束时清理，避免污染共享测试库里的真实配置。
	const key = "test_settings_roundtrip"
	t.Cleanup(func() {
		ctx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(ctx, t, st, `DELETE FROM settings WHERE key = $1`, key)
	})

	t.Run("不存在返回NotFound", func(t *testing.T) {
		_, err := st.GetSetting(ctx, "test_settings_no_such_key")
		if err == nil {
			t.Fatal("GetSetting() 对不存在的 key 应返回错误，实际返回 nil")
		}
		if !errors.Is(err, types.ErrNotFound) {
			t.Errorf("错误应满足 errors.Is(err, types.ErrNotFound)，实际: %v", err)
		}
	})

	t.Run("Put→Get往返", func(t *testing.T) {
		want := json.RawMessage(`{"app_id":"cli_test","enabled":true}`)
		if err := st.PutSetting(ctx, key, want); err != nil {
			t.Fatalf("PutSetting() 失败: %v", err)
		}
		got, err := st.GetSetting(ctx, key)
		if err != nil {
			t.Fatalf("GetSetting() 失败: %v", err)
		}
		// JSONB 会规范化空白/键序，逐字节比较会脆断——比较解析后的语义等价。
		assertJSONEqual(t, want, got)
	})

	t.Run("upsert覆盖", func(t *testing.T) {
		updated := json.RawMessage(`{"app_id":"cli_test2","enabled":false}`)
		if err := st.PutSetting(ctx, key, updated); err != nil {
			t.Fatalf("PutSetting() 覆盖写失败: %v", err)
		}
		got, err := st.GetSetting(ctx, key)
		if err != nil {
			t.Fatalf("GetSetting() 失败: %v", err)
		}
		assertJSONEqual(t, updated, got)
	})
}

// assertJSONEqual 按解析后的结构比较两段 JSON 是否语义等价。
func assertJSONEqual(t *testing.T, want, got json.RawMessage) {
	t.Helper()
	var w, g any
	if err := json.Unmarshal(want, &w); err != nil {
		t.Fatalf("解析期望 JSON 失败: %v", err)
	}
	if err := json.Unmarshal(got, &g); err != nil {
		t.Fatalf("解析实际 JSON 失败: %v（原文: %s）", err, got)
	}
	wb, _ := json.Marshal(w)
	gb, _ := json.Marshal(g)
	if string(wb) != string(gb) {
		t.Errorf("JSON 不等价:\n  期望: %s\n  实际: %s", wb, gb)
	}
}
