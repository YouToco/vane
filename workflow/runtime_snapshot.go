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
