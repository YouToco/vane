package types

import "time"

type ProfileClaimSource struct {
	State   string `json:"state"`
	RefType string `json:"ref_type,omitempty"`
	Ref     string `json:"ref,omitempty"`
}

type ProfileClaim struct {
	ID           string             `json:"id"`
	Field        string             `json:"field"`
	Value        string             `json:"value"`
	Source       ProfileClaimSource `json:"source"`
	SupersedesID string             `json:"supersedes_id,omitempty"`
	Active       bool               `json:"active"`
	Pinned       bool               `json:"pinned"`
	CreatedAt    time.Time          `json:"created_at"`
}

type ProfileClaimList struct {
	ProfileEpoch     int64               `json:"profile_epoch"`
	Version          int64               `json:"version"`
	Claims           []ProfileClaim      `json:"claims"`
	Events           []ProfileClaimEvent `json:"events"`
	EventsHasMore    bool                `json:"events_has_more"`
	EventsNextCursor string              `json:"events_next_cursor,omitempty"`
}

type ProfileClaimEvent struct {
	ID            string    `json:"id"`
	Kind          string    `json:"kind"`
	TargetClaimID string    `json:"target_claim_id,omitempty"`
	ResultClaimID string    `json:"result_claim_id,omitempty"`
	CreatedAt     time.Time `json:"created_at"`
	Revoked       bool      `json:"revoked"`
	Revocable     bool      `json:"revocable"`
}

type ProfileClaimAction struct {
	ExpectedVersion int64
	Action          string
	ClaimID         int64
	EventID         int64
	Value           string
}

type ProfileClaimActionResult struct {
	// Pointer preserves exact replay for receipts created before epoch support:
	// absent stays absent, while every new response includes even epoch zero.
	ProfileEpoch   *int64         `json:"profile_epoch,omitempty"`
	Version        int64          `json:"version"`
	EventID        string         `json:"event_id"`
	Profile        ProfileView    `json:"profile"`
	Claims         []ProfileClaim `json:"claims"`
	ClaimsComplete bool           `json:"claims_complete"`
}
