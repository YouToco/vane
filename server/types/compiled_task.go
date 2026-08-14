package types

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"

	"github.com/YouToco/vane/internal/strictjson"
)

const pausedCompiledTaskDefinitionDigestVersion = "vane.paused-compiled-task-definition/v1"

// PausedCompiledTaskDefinition 是一份已经编译完成、等待持久化的稳定监控任务定义。
//
// 它只服务任务创建 saga 的数据库步骤，不是公开 HTTP/A2A wire contract，也不试图描述
// DiscoverAtRun 等未来任务形态。调用 InsertPausedCompiledTaskDefinition 前，调用方必须已经
// 在 Temporal 中创建同 TaskID 的 Schedule，并通过 Describe 确认它仍处于 paused 状态；
// 该 Temporal 原语及指纹核对属于 A-3，不由本数据结构伪装成一个可自行声明的 bool。
type PausedCompiledTaskDefinition struct {
	TaskID          string
	TenantID        int64
	UserID          int64
	NLDescription   string
	SpecJSON        json.RawMessage
	ScopeJSON       json.RawMessage
	PlaybookContent string
	FetchPlan       json.RawMessage
	Strictness      PushStrictness
}

// DigestPausedCompiledTaskDefinition hashes a stable, explicitly ordered wire
// envelope. It is shared by task preparation and Store commit so a coordinator
// mapping bug cannot pair one immutable compiled checkpoint with a different
// A2 aggregate. Raw JSON fields are embedded as JSON, not quoted strings.
func DigestPausedCompiledTaskDefinition(def PausedCompiledTaskDefinition) (string, error) {
	if strictjson.Validate(def.SpecJSON) != nil ||
		strictjson.Validate(def.ScopeJSON) != nil ||
		strictjson.Validate(def.FetchPlan) != nil {
		return "", errors.New("vane: paused compiled task definition contains invalid JSON")
	}
	envelope := struct {
		Version         string          `json:"version"`
		TaskID          string          `json:"task_id"`
		TenantID        int64           `json:"tenant_id"`
		UserID          int64           `json:"user_id"`
		NLDescription   string          `json:"nl_description"`
		SpecJSON        json.RawMessage `json:"spec_json"`
		ScopeJSON       json.RawMessage `json:"scope_json"`
		PlaybookContent string          `json:"playbook_content"`
		FetchPlan       json.RawMessage `json:"fetch_plan"`
		Strictness      PushStrictness  `json:"strictness"`
	}{
		Version:         pausedCompiledTaskDefinitionDigestVersion,
		TaskID:          def.TaskID,
		TenantID:        def.TenantID,
		UserID:          def.UserID,
		NLDescription:   def.NLDescription,
		SpecJSON:        def.SpecJSON,
		ScopeJSON:       def.ScopeJSON,
		PlaybookContent: def.PlaybookContent,
		FetchPlan:       def.FetchPlan,
		Strictness:      def.Strictness,
	}
	payload, err := json.Marshal(envelope)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}
