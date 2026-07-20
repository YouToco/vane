package types

import "encoding/json"

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
