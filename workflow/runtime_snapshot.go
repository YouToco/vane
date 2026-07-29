package workflow

import "github.com/YouToco/vane/types"

// Workflow aliases keep the Activity wire contract close to its consumer while
// Store and Workflow share one implementation of validation and hashing.
const RunSnapshotSchemaVersion = types.RunSnapshotSchemaVersion

type RunSnapshotKind = types.RunSnapshotKind
type RunIdentity = types.RunIdentity
type RuntimePolicySnapshot = types.RuntimePolicyDigests
type PlannerBudgetSnapshot = types.PlannerBudget
type RunSnapshotRef = types.RunSnapshotRef
type RunSnapshotRefV2 = types.RunSnapshotRefV2

const RunSnapshotKindScheduled = types.RunSnapshotKindScheduled

// PrepareRunResult is the sole run-start authorization result consumed by the
// Workflow. Unauthorized runs carry an exact zero snapshot, preventing callers
// from accidentally using stale partially populated references.
type PrepareRunResult struct {
	Authorized bool           `json:"authorized"`
	Snapshot   RunSnapshotRef `json:"snapshot"`
}

// Validate rejects both an authorized invalid snapshot and an unauthorized
// result that still carries snapshot material.
func (r PrepareRunResult) Validate() error {
	if !r.Authorized {
		if r.Snapshot != (RunSnapshotRef{}) {
			return types.NewAppError(types.CodeValidation,
				"unauthorized prepare result must carry a zero snapshot", nil)
		}
		return nil
	}
	return r.Snapshot.Validate()
}

// ValidateFor additionally binds an authorized result to the caller-observed
// identity. expected is validated even for an unauthorized result, so missing
// expected scope can never act as a wildcard.
func (r PrepareRunResult) ValidateFor(expected RunIdentity) error {
	if err := expected.Validate(); err != nil {
		return err
	}
	if err := r.Validate(); err != nil {
		return err
	}
	if !r.Authorized {
		return nil
	}
	return r.Snapshot.ValidateFor(expected)
}

// PrepareToolRunV2Result is deliberately distinct from PrepareRunResult so a
// retained Source runtime cannot receive or authorize a Source-free Tool ref.
type PrepareToolRunV2Result struct {
	Authorized bool             `json:"authorized"`
	Snapshot   RunSnapshotRefV2 `json:"snapshot"`
}

// ExecuteToolInvocationV2Input carries only an immutable reference and one
// invocation digest. Tool arguments and materialized requests stay in the
// Store snapshot and never enter Temporal history.
type ExecuteToolInvocationV2Input struct {
	TenantID         int64            `json:"tenant_id"`
	UserID           int64            `json:"user_id"`
	TaskID           string           `json:"task_id"`
	Snapshot         RunSnapshotRefV2 `json:"snapshot"`
	InvocationDigest string           `json:"invocation_digest"`
}

// ToolInvocationReceiptV1 is the small durable Activity result. Content bodies
// remain in the append-only observation evidence row.
type ToolInvocationReceiptV1 struct {
	InvocationDigest  string `json:"invocation_digest"`
	ObservationDigest string `json:"observation_digest"`
	ContentCount      int    `json:"content_count"`
}

func (r PrepareToolRunV2Result) Validate() error {
	if !r.Authorized {
		if r.Snapshot != (RunSnapshotRefV2{}) {
			return types.NewAppError(types.CodeValidation,
				"unauthorized Tool prepare result must carry a zero snapshot", nil)
		}
		return nil
	}
	return r.Snapshot.Validate()
}

func (r PrepareToolRunV2Result) ValidateFor(expected RunIdentity) error {
	if err := expected.Validate(); err != nil {
		return err
	}
	if err := r.Validate(); err != nil {
		return err
	}
	if !r.Authorized {
		return nil
	}
	return r.Snapshot.ValidateFor(expected)
}
