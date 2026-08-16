package types

import "time"

const (
	MemoryActionRemember = "remember"
	MemoryActionCorrect  = "correct"
	MemoryActionForget   = "forget"

	// MemoryEvidenceOwnerExplicitAgentTurn is the only write authority accepted
	// by the memory Store. Model-inferred or implicit conversation text cannot
	// be promoted into active long-term memory.
	MemoryEvidenceOwnerExplicitAgentTurn = "owner_explicit_agent_turn"
)

// MemoryEvidence binds an explicit owner instruction to its trusted server
// trace. SourceID is a canonical UUID string generated outside the model.
type MemoryEvidence struct {
	AuthorizationID     string `json:"authorization_id,omitempty"`
	SourceType          string `json:"source_type"`
	SourceID            string `json:"source_id"`
	OwnerRequest        string `json:"owner_request"`
	AuthorizationDigest string `json:"authorization_digest"`
}

// MemoryAction is one explicit append-only mutation. MemoryID is required for
// correct/forget and must be zero for remember. Text is required for
// remember/correct and must be empty for forget.
type MemoryAction struct {
	Action   string         `json:"action"`
	MemoryID int64          `json:"memory_id,omitempty"`
	Text     string         `json:"text,omitempty"`
	Evidence MemoryEvidence `json:"evidence"`
}

type MemoryRecord struct {
	ID                 int64          `json:"id"`
	CreatorUserID      int64          `json:"creator_user_id,omitempty"`
	Text               string         `json:"text"`
	Evidence           MemoryEvidence `json:"evidence"`
	SupersedesMemoryID int64          `json:"supersedes_memory_id,omitempty"`
	Active             bool           `json:"active"`
	CreatedAt          time.Time      `json:"created_at"`
}

type MemoryEvent struct {
	ID             int64          `json:"id"`
	ActorUserID    int64          `json:"actor_user_id,omitempty"`
	Action         string         `json:"action"`
	TargetMemoryID int64          `json:"target_memory_id,omitempty"`
	ResultMemoryID int64          `json:"result_memory_id,omitempty"`
	Evidence       MemoryEvidence `json:"evidence"`
	CreatedAt      time.Time      `json:"created_at"`
}

// MemoryActionResult is persisted with exact JSON semantics in the idempotency
// receipt. For forget, Memory is the forgotten target with Active=false.
type MemoryActionResult struct {
	Memory MemoryRecord `json:"memory"`
	Event  MemoryEvent  `json:"event"`
}

type MemoryRecallQuery struct {
	Query string `json:"query"`
	Limit int    `json:"limit"`
}

type MemoryRecallItem struct {
	Memory MemoryRecord `json:"memory"`
	Score  float64      `json:"score"`
}

type MemoryRecallResult struct {
	Memories []MemoryRecallItem `json:"memories"`
}
