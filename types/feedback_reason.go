package types

import "time"

// FeedbackReason is the stable machine-readable cause attached to a
// misjudged feedback event. It is intentionally orthogonal to the user's
// attitude: reporting a stale or incorrect item must not teach the profile
// that the user dislikes the topic.
type FeedbackReason string

const (
	FeedbackReasonOutdated    FeedbackReason = "outdated_or_out_of_window"
	FeedbackReasonNotRelevant FeedbackReason = "not_relevant"
	FeedbackReasonDuplicate   FeedbackReason = "duplicate"
	FeedbackReasonFactWrong   FeedbackReason = "factually_wrong"
	FeedbackReasonPoorSource  FeedbackReason = "poor_source_or_evidence"
	FeedbackReasonOther       FeedbackReason = "other"
)

type TaskPolicySuggestion struct {
	FeedbackID int64
	DeliveryID int64
	TaskID     string
	ClaimToken string
	CreatedAt  time.Time
}

// Valid reports whether r is one of the reasons emitted by current cards.
func (r FeedbackReason) Valid() bool {
	switch r {
	case FeedbackReasonOutdated,
		FeedbackReasonNotRelevant,
		FeedbackReasonDuplicate,
		FeedbackReasonFactWrong,
		FeedbackReasonPoorSource,
		FeedbackReasonOther:
		return true
	default:
		return false
	}
}

type FreshnessFeedbackAuditOutcome string

const (
	FreshnessAuditSystemDefect         FreshnessFeedbackAuditOutcome = "system_defect"
	FreshnessAuditTaskPolicySuggestion FreshnessFeedbackAuditOutcome = "task_policy_suggestion"
	FreshnessAuditPolicySatisfied      FreshnessFeedbackAuditOutcome = "policy_satisfied"
	FreshnessAuditUnverifiable         FreshnessFeedbackAuditOutcome = "unverifiable"
)

// Label returns the user-facing Chinese label used in cards and evolution
// prompts. Keeping the mapping in one package prevents UI and learning
// semantics from drifting apart.
func (r FeedbackReason) Label() string {
	switch r {
	case FeedbackReasonOutdated:
		return "过时或超出任务时间范围"
	case FeedbackReasonNotRelevant:
		return "与任务无关"
	case FeedbackReasonDuplicate:
		return "重复或已经推过"
	case FeedbackReasonFactWrong:
		return "事实或结论错误"
	case FeedbackReasonPoorSource:
		return "来源或证据质量差"
	case FeedbackReasonOther:
		return "其他"
	default:
		return ""
	}
}
