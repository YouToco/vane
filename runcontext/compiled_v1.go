// Package runcontext defines the typed, in-process view of an immutable task
// run snapshot. The full view may contain approved prompts, source URLs and
// source configuration, so it must stay inside Activities and must never be
// returned through a Temporal Workflow payload. Workflows carry only the
// sealed types.RunSnapshotRef.
package runcontext

import (
	"encoding/json"

	"github.com/YouToco/vane/observation"
	"github.com/YouToco/vane/runtimepolicy"
	"github.com/YouToco/vane/types"
)

const (
	// SourceScopeApprovedPlan identifies a task whose exact compiled plan and
	// materialized sources were approved by the user.
	SourceScopeApprovedPlan = "approved_plan"
	// SourceScopeLegacySubscriptions identifies the explicit compatibility
	// snapshot of the subscriptions that existed when the run began.
	SourceScopeLegacySubscriptions = "legacy_subscriptions"
)

// SourceV1 is one materialized, immutable source identity for a compiled run.
// Scheduling health remains adaptive and is deliberately loaded separately.
type SourceV1 struct {
	SourceID   int64
	Platform   types.Platform
	Capability types.Capability
	Title      string
	URL        string
	Config     json.RawMessage
}

// DefinitionV1 is the approved task definition frozen at run start.
type DefinitionV1 struct {
	TaskID          string
	TenantID        int64
	UserID          int64
	NLDescription   string
	SpecJSON        json.RawMessage
	ScopeJSON       json.RawMessage
	PlaybookContent string
	Strictness      types.PushStrictness
	SourceScope     string
	FetchPlan       json.RawMessage
	Sources         []SourceV1
}

// CompiledSnapshotV1 is the complete typed Activity-only view of a compiled
// V1 snapshot. Ref is the only member allowed to cross back into Workflow
// history; Definition and Policy must remain inside the Activity process.
type CompiledSnapshotV1 struct {
	Ref                types.RunSnapshotRef
	Mode               types.ExecutionMode
	AdaptiveVersion    int64
	Budget             types.PlannerBudget
	ObservationRollout observation.RolloutMode
	Definition         DefinitionV1
	Policy             runtimepolicy.BundleV1
}
