package types

import (
	"errors"
	"time"
)

// ErrResearchV3CutoverDrift distinguishes a verified definition/baseline
// change from an optimistic phase conflict. Coordinators may compensate the
// former, but must reload and resume after the latter.
var ErrResearchV3CutoverDrift = errors.New("vane: research V3 cutover definition drift")

// ResearchV3CutoverPhase is the durable checkpoint of the exact-task
// Schedule Action replacement saga.  The remote Schedule is always described
// again before advancing a recovered checkpoint.
type ResearchV3CutoverPhase string

const (
	ResearchV3CutoverPrepared               ResearchV3CutoverPhase = "prepared"
	ResearchV3CutoverPauseRequested         ResearchV3CutoverPhase = "pause_requested"
	ResearchV3CutoverPaused                 ResearchV3CutoverPhase = "paused"
	ResearchV3CutoverDefinitionPromoted     ResearchV3CutoverPhase = "definition_promoted"
	ResearchV3CutoverActionSwapped          ResearchV3CutoverPhase = "action_swapped"
	ResearchV3CutoverActive                 ResearchV3CutoverPhase = "active"
	ResearchV3CutoverRollbackPauseRequested ResearchV3CutoverPhase = "rollback_pause_requested"
	ResearchV3CutoverRollbackPaused         ResearchV3CutoverPhase = "rollback_paused"
	ResearchV3CutoverDefinitionRestored     ResearchV3CutoverPhase = "definition_restored"
	ResearchV3CutoverRolledBack             ResearchV3CutoverPhase = "rolled_back"
	ResearchV3CutoverAborted                ResearchV3CutoverPhase = "aborted"
	ResearchV3CutoverManualIntervention     ResearchV3CutoverPhase = "manual_intervention"
)

// ResearchV3DefinitionHead is the exact immutable V3 definition bound to a
// cutover generation.
type ResearchV3DefinitionHead struct {
	Version int64
	Digest  string
}

// ResearchV3CutoverOperation contains only durable control-plane evidence.
// Protobuf bytes are opaque to Store and decoded by the scheduler.
type ResearchV3CutoverOperation struct {
	ID                        int64
	TenantID                  int64
	UserID                    int64
	TaskID                    string
	IdempotencyKey            string
	Generation                int64
	Definition                ResearchV3DefinitionHead
	SourceBaselineDigest      string
	OriginalExecutionMode     ExecutionMode
	OriginalDefinition        *ResearchV3DefinitionHead
	FrozenSchedule            []byte
	FrozenScheduleDigest      string
	FrozenConflictToken       []byte
	ConflictTokenDigest       string
	RollbackConflictToken     []byte
	RollbackTokenDigest       string
	TargetAction              []byte
	TargetActionDigest        string
	ActionAuthorizationDigest string
	OriginalPaused            bool
	Phase                     ResearchV3CutoverPhase
	CreatedAt                 time.Time
	UpdatedAt                 time.Time
}

type BeginResearchV3CutoverParams struct {
	TenantID                  int64
	UserID                    int64
	TaskID                    string
	IdempotencyKey            string
	Definition                ResearchV3DefinitionHead
	FrozenSchedule            []byte
	FrozenScheduleDigest      string
	FrozenConflictToken       []byte
	ConflictTokenDigest       string
	TargetAction              []byte
	TargetActionDigest        string
	ActionAuthorizationDigest string
	OriginalPaused            bool
}

// ResearchV3DeliveryAuthority is the durable precondition for every new V3
// provider-effect claim. Revocation blocks new claims but never prevents a
// receipt for an effect that the provider already accepted from being settled.
type ResearchV3DeliveryAuthority struct {
	TenantID                  int64
	UserID                    int64
	TaskID                    string
	Generation                int64
	DefinitionVersion         int64
	DefinitionDigest          string
	TargetActionDigest        string
	ActionAuthorizationDigest string
	Enabled                   bool
	Revoked                   bool
}
