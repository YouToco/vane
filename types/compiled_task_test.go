package types

import (
	"encoding/json"
	"testing"
)

func TestDigestPausedCompiledTaskDefinition_StableAndComplete(t *testing.T) {
	def := PausedCompiledTaskDefinition{
		TaskID: "push-digest", TenantID: 7, UserID: 9,
		NLDescription:   "监控官方更新",
		SpecJSON:        json.RawMessage(`{"cron":"0 8 * * *","tz":"Asia/Shanghai"}`),
		ScopeJSON:       json.RawMessage(`{"max_items":5}`),
		PlaybookContent: "只看官方来源",
		FetchPlan: json.RawMessage(
			`{"sources":[{"platform":"web","capability":"feed","url":"https://example.com/feed","config":{}}]}`),
		Strictness: StrictnessNormal,
	}
	first, err := DigestPausedCompiledTaskDefinition(def)
	if err != nil {
		t.Fatal(err)
	}
	second, err := DigestPausedCompiledTaskDefinition(def)
	if err != nil || first != second || len(first) != 64 {
		t.Fatalf("digest 不稳定: first=%q second=%q err=%v", first, second, err)
	}
	mutated := def
	mutated.PlaybookContent = "不同手册"
	different, err := DigestPausedCompiledTaskDefinition(mutated)
	if err != nil {
		t.Fatal(err)
	}
	if different == first {
		t.Fatal("定义字段变化必须改变 digest")
	}
	invalid := def
	invalid.FetchPlan = json.RawMessage(`{`)
	if _, err := DigestPausedCompiledTaskDefinition(invalid); err == nil {
		t.Fatal("非法 RawMessage 必须拒绝")
	}
	duplicate := def
	duplicate.FetchPlan = json.RawMessage(
		`{"sources":[{"platform":"web","capability":"feed","url":"https://example.com/feed","config":{"x":1,"x":2}}]}`)
	if _, err := DigestPausedCompiledTaskDefinition(duplicate); err == nil {
		t.Fatal("digest 不得授权 JSONB 会折叠的重复 key")
	}
}
