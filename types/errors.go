// Package types 定义见微 Vane 的共享领域类型：实体、枚举与统一错误体系。
// 本包只依赖标准库，供 store / llm / api / workflow 等所有包引用，自身不引用任何内部包。
package types

import (
	"errors"
	"fmt"
)

// ============================================================
// 哨兵错误（Sentinel）：粗粒度错误类别，用 errors.Is 判断。
// 三层分组：业务层 / 系统层 / 外部依赖层（Step 4 设计）。
// ============================================================

var (
	// 业务层
	ErrNotFound   = errors.New("vane: not found")  // 资源不存在
	ErrConflict   = errors.New("vane: conflict")   // 唯一约束冲突 / 状态冲突
	ErrValidation = errors.New("vane: validation") // 入参 / 业务校验失败

	// 系统层
	ErrDatabase = errors.New("vane: database") // 数据库层错误（含死锁 / 断连 / 约束）
	ErrInternal = errors.New("vane: internal") // 未分类的内部错误

	// 外部依赖层
	ErrLLM   = errors.New("vane: llm")   // LLM API 调用错误
	ErrFetch = errors.New("vane: fetch") // 内容抓取错误（RSS / TikHub）
	ErrPush  = errors.New("vane: push")  // 飞书推送错误
)

// ============================================================
// ErrCode：细分错误码，机器可读（写日志 / API 响应 / Temporal
// NonRetryableErrorTypes 均使用字符串值）。
// ============================================================

// ErrCode 细分错误码类型。
type ErrCode string

const (
	// 业务层
	CodeNotFound   ErrCode = "NOT_FOUND"
	CodeConflict   ErrCode = "CONFLICT"
	CodeValidation ErrCode = "VALIDATION"

	// 系统层
	CodeDatabase ErrCode = "DATABASE"
	CodeInternal ErrCode = "INTERNAL"

	// LLM 细分
	CodeLLMRateLimit   ErrCode = "LLM_RATE_LIMIT"
	CodeLLMBadRequest  ErrCode = "LLM_BAD_REQUEST"
	CodeLLMUnavailable ErrCode = "LLM_UNAVAILABLE"

	// 抓取细分
	CodeFetchTimeout   ErrCode = "FETCH_TIMEOUT"
	CodeFetchRateLimit ErrCode = "FETCH_RATE_LIMIT"

	// 推送
	CodePushFailed ErrCode = "PUSH_FAILED"

	// 数据库细分
	CodeDBDeadlock   ErrCode = "DB_DEADLOCK"
	CodeDBConnLost   ErrCode = "DB_CONN_LOST"
	CodeDBConstraint ErrCode = "DB_CONSTRAINT"
)

// codeSentinel 是 ErrCode → 哨兵错误的映射，供 AppError.Is 使用，
// 使 errors.Is(err, ErrDatabase) 能匹配到所有 DB_* 细分码。
var codeSentinel = map[ErrCode]error{
	CodeNotFound:   ErrNotFound,
	CodeConflict:   ErrConflict,
	CodeValidation: ErrValidation,

	CodeDatabase:     ErrDatabase,
	CodeDBDeadlock:   ErrDatabase,
	CodeDBConnLost:   ErrDatabase,
	CodeDBConstraint: ErrDatabase,

	CodeInternal: ErrInternal,

	CodeLLMRateLimit:   ErrLLM,
	CodeLLMBadRequest:  ErrLLM,
	CodeLLMUnavailable: ErrLLM,

	CodeFetchTimeout:   ErrFetch,
	CodeFetchRateLimit: ErrFetch,

	CodePushFailed: ErrPush,
}

// retryableByDefault 各错误码的默认可重试性。
// 与 Step 4 Temporal RetryPolicy 对齐：LLM_BAD_REQUEST / VALIDATION /
// DB_CONSTRAINT / CONFLICT / NOT_FOUND 等确定性失败不可重试；
// 限流、超时、死锁、断连等瞬态失败可重试。
var retryableByDefault = map[ErrCode]bool{
	CodeNotFound:   false,
	CodeConflict:   false,
	CodeValidation: false,

	CodeDatabase:     true,
	CodeDBDeadlock:   true,
	CodeDBConnLost:   true,
	CodeDBConstraint: false,

	CodeInternal: false,

	CodeLLMRateLimit:   true,
	CodeLLMBadRequest:  false,
	CodeLLMUnavailable: true,

	CodeFetchTimeout:   true,
	CodeFetchRateLimit: true,

	CodePushFailed: true,
}

// ============================================================
// AppError：贯穿全链路的统一错误载体（Step 4 设计）。
// Cause 只存原始错误（pgx error / HTTP error），不做双 %w 包装，
// 通过自定义 Is() 按 Code → 哨兵映射实现 errors.Is 匹配
// （Validator 修正 C-1，避免 Go 1.20 双 %w 的 Unwrap() []error 链歧义）。
// ============================================================

// AppError 统一应用错误。各包在自己的转换点（wrapPgxError /
// wrapHTTPError / wrapFetchError）只包装一次。
type AppError struct {
	Code      ErrCode // 细分错误码
	Message   string  // 面向日志 / 内部调用者的描述
	Cause     error   // 原始错误，可为 nil
	Retryable bool    // 是否可重试（Temporal / 调用方参考）
}

// Error 实现 error 接口，格式：CODE: message: cause。
func (e *AppError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("%s: %s: %v", e.Code, e.Message, e.Cause)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

// Unwrap 返回原始错误，使 errors.Is / errors.As 能继续下钻
// 到 pgx.ErrNoRows、context.DeadlineExceeded 等底层错误。
func (e *AppError) Unwrap() error {
	return e.Cause
}

// Is 自定义匹配规则（由 errors.Is 在每层解包时调用）：
//  1. target 是本 Code 对应的哨兵错误 → 匹配（如 CodeDBDeadlock 匹配 ErrDatabase）；
//  2. target 也是 *AppError 且 Code 相同 → 匹配。
func (e *AppError) Is(target error) bool {
	if sentinel, ok := codeSentinel[e.Code]; ok && target == sentinel {
		return true
	}
	// t != nil 防护：errors.Is(err, (*AppError)(nil)) 这类误用应返回 false 而非 panic。
	if t, ok := target.(*AppError); ok && t != nil {
		return t.Code == e.Code
	}
	return false
}

// NewAppError 构造 AppError，Retryable 按 retryableByDefault 取默认值，
// 调用方如需覆盖可直接对返回值的 Retryable 字段赋值。cause 可为 nil。
func NewAppError(code ErrCode, message string, cause error) *AppError {
	return &AppError{
		Code:      code,
		Message:   message,
		Cause:     cause,
		Retryable: retryableByDefault[code],
	}
}

// CodeOf 提取错误链中最外层 AppError 的错误码；
// 链上没有 AppError 时回退为 CodeInternal（api.writeError 等出口使用）。
func CodeOf(err error) ErrCode {
	var ae *AppError
	if errors.As(err, &ae) {
		return ae.Code
	}
	return CodeInternal
}

// IsRetryable 判断错误链中最外层 AppError 是否可重试；
// 链上没有 AppError 时保守返回 false。
func IsRetryable(err error) bool {
	var ae *AppError
	if errors.As(err, &ae) {
		return ae.Retryable
	}
	return false
}
