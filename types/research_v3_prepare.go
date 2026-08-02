package types

import "time"

type ResearchV3DefinitionPreparePhase string

const (
	ResearchV3DefinitionPrepared   ResearchV3DefinitionPreparePhase = "prepared"
	ResearchV3DefinitionRolledBack ResearchV3DefinitionPreparePhase = "rolled_back"
)

// ResearchV3DefinitionPrepareOperation is the immutable audit/recovery record
// for a delivery-dark V3 sidecar head. The primary Schedule head is not
// changed by this operation.
type ResearchV3DefinitionPrepareOperation struct {
	ID                   int64
	TenantID             int64
	UserID               int64
	TaskID               string
	IdempotencyKey       string
	Target               ResearchV3DefinitionHead
	PreviousPreparedHead *ResearchV3DefinitionHead
	SourceBaselineDigest string
	OriginalMode         ExecutionMode
	OriginalHead         *ResearchV3DefinitionHead
	Phase                ResearchV3DefinitionPreparePhase
	CreatedAt            time.Time
	UpdatedAt            time.Time
}
