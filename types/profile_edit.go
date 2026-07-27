package types

import "time"

type ProfileView struct {
	Industry    string    `json:"industry"`
	Occupation  string    `json:"occupation"`
	Tags        []string  `json:"tags"`
	RemovedTags []string  `json:"removed_tags"`
	Summary     string    `json:"summary"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

// ProfileEditPatch preserves field presence: nil means omitted, while pointers
// to empty strings/slices mean an explicit clear.
type ProfileEditPatch struct {
	Industry   *string
	Occupation *string
	Tags       *[]string
}

type ProfileEditChange struct {
	Field  string `json:"field"`
	Before any    `json:"before"`
	After  any    `json:"after"`
}

type ProfileEditRevision struct {
	ID        string              `json:"id"`
	CreatedAt time.Time           `json:"created_at"`
	Actor     string              `json:"actor"`
	Kind      string              `json:"kind"`
	Changes   []ProfileEditChange `json:"changes"`
	Undoable  bool                `json:"undoable"`
}
