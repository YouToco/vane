package types

import (
	"encoding/json"
	"errors"
	"time"
)

var (
	ErrTaskDefinitionEditBusy = errors.New(
		"vane: task definition edit busy",
	)
	ErrTaskDefinitionEditTerminal = errors.New(
		"vane: task definition edit terminal",
	)
	ErrTaskDefinitionEditLeaseLost = errors.New(
		"vane: task definition edit lease lost",
	)
	ErrTaskDefinitionEditReceiptBusy = errors.New(
		"vane: task definition edit receipt busy",
	)
	ErrTaskDefinitionEditReceiptTerminal = errors.New(
		"vane: task definition edit receipt terminal",
	)
	ErrTaskDefinitionEditReceiptLeaseLost = errors.New(
		"vane: task definition edit receipt lease lost",
	)
)

// TaskDefinitionEditOperationStatus is the durable lifecycle of one frozen,
// authenticated Approved Definition proposal. Pending and Executing are the
// only non-terminal values; every other value is a permanent tombstone.
type TaskDefinitionEditOperationStatus string

const (
	TaskDefinitionEditOperationStatusPending    TaskDefinitionEditOperationStatus = "pending"
	TaskDefinitionEditOperationStatusExecuting  TaskDefinitionEditOperationStatus = "executing"
	TaskDefinitionEditOperationStatusCompleted  TaskDefinitionEditOperationStatus = "completed"
	TaskDefinitionEditOperationStatusCancelled  TaskDefinitionEditOperationStatus = "cancelled"
	TaskDefinitionEditOperationStatusExpired    TaskDefinitionEditOperationStatus = "expired"
	TaskDefinitionEditOperationStatusBlocked    TaskDefinitionEditOperationStatus = "blocked"
	TaskDefinitionEditOperationStatusSuperseded TaskDefinitionEditOperationStatus = "superseded"
)

// TaskDefinitionEditPhase is the monotonic cross-system progress checkpoint.
// Terminal status never overwrites it, so blocked/cancelled audit retains the
// last proven phase. A Store may replay identical bytes at the same phase, but
// must reject different bytes and every backwards transition.
type TaskDefinitionEditPhase string

const (
	TaskDefinitionEditPhaseProposalSealed         TaskDefinitionEditPhase = "proposal_sealed"
	TaskDefinitionEditPhaseDBQuiesced             TaskDefinitionEditPhase = "db_quiesced"
	TaskDefinitionEditPhaseTemporalBasePaused     TaskDefinitionEditPhase = "temporal_base_paused"
	TaskDefinitionEditPhaseDefinitionCommitted    TaskDefinitionEditPhase = "definition_committed"
	TaskDefinitionEditPhaseTemporalTargetApplied  TaskDefinitionEditPhase = "temporal_target_applied"
	TaskDefinitionEditPhaseTemporalTargetRestored TaskDefinitionEditPhase = "temporal_target_restored"
)

// TaskDefinitionEditLease is the complete authorization and fencing identity
// required by every mutable operation after acquisition. Actor and target are
// intentionally both present even though protocol v1 requires them to match.
type TaskDefinitionEditLease struct {
	ID             string
	TenantID       int64
	UserID         int64
	TargetTenantID int64
	TargetUserID   int64
	TaskID         string
	LeaseOwner     string
	Fence          int64
}

// TaskDefinitionEditScope is the complete immutable identity used before an
// operation has a fencing token. Requiring actor and target fields on every
// lookup keeps a future team-capable protocol from weakening the owner-only V1
// boundary by accident.
type TaskDefinitionEditScope struct {
	ID             string
	TenantID       int64
	UserID         int64
	TargetTenantID int64
	TargetUserID   int64
	TaskID         string
}

// CreateTaskDefinitionEditOperationParams contains only the five canonical
// frozen checkpoints. Store decodes them and derives every identity, digest,
// status, and timestamp; callers cannot separately claim trusted metadata.
type CreateTaskDefinitionEditOperationParams struct {
	CanonicalProposal []byte
	BaseDefinition    []byte
	TargetDefinition  []byte
	PreparedEdit      []byte
	BaseSnapshot      []byte
}

// AcquireTaskDefinitionEditOperationParams starts or recovers one exact
// operation. LeaseOwner is generated once before the call so a response-lost
// replay by the same owner can recover the same active fence. The Store owns
// all database-clock deadlines and the fixed takeover grace.
type AcquireTaskDefinitionEditOperationParams struct {
	Scope           TaskDefinitionEditScope
	LeaseOwner      string
	LeaseDuration   time.Duration
	ReceiptProvider string
	ReceiptTarget   string
}

// ExpireTaskDefinitionEditOperationParams tombstones an expired, unstarted
// proposal. Provider and target may both be empty for an audit-only suppressed
// receipt, or both be present when an authenticated callback supplied the
// original operation resource.
type ExpireTaskDefinitionEditOperationParams struct {
	Scope           TaskDefinitionEditScope
	ReceiptProvider string
	ReceiptTarget   string
}

// TaskDefinitionEditBlockReason is a fixed, non-sensitive terminal reason.
// Raw database, Temporal, or provider errors must never enter the durable
// operation, user card, or Agent session.
type TaskDefinitionEditBlockReason string

const (
	TaskDefinitionEditBlockScheduleDeleted   TaskDefinitionEditBlockReason = "schedule_deleted"
	TaskDefinitionEditBlockTemporalNotFound  TaskDefinitionEditBlockReason = "temporal_not_found"
	TaskDefinitionEditBlockUnsafeRemoteState TaskDefinitionEditBlockReason = "unsafe_remote_state"
	TaskDefinitionEditBlockCheckpointInvalid TaskDefinitionEditBlockReason = "checkpoint_invalid"
)

// TaskDefinitionEditOperation is one durable edit proposal/execution. All
// proposal and Temporal fields are exact PostgreSQL BYTEA checkpoints; their
// sibling digest fields are database-verified SHA-256 values.
type TaskDefinitionEditOperation struct {
	ID                 string
	TenantID           int64
	UserID             int64
	TargetTenantID     int64
	TargetUserID       int64
	TaskID             string
	SessionID          int64
	OperationRef       string
	Status             TaskDefinitionEditOperationStatus
	Phase              TaskDefinitionEditPhase
	ExpiresAt          time.Time
	ExecutionStartedAt *time.Time
	OriginalStatus     ScheduleStatus

	BaseDefinitionVersion   int64
	BaseDefinitionDigest    string
	BaseDefinition          []byte
	TargetDefinitionVersion int64
	TargetDefinitionDigest  string
	TargetDefinition        []byte
	CanonicalProposal       []byte
	ProposalDigest          string
	PreparedEdit            []byte
	PreparedEditDigest      string
	BaseSnapshot            []byte
	BaseSnapshotDigest      string
	PauseSnapshot           []byte
	PauseSnapshotDigest     string
	ApplySnapshot           []byte
	ApplySnapshotDigest     string
	RestoreSnapshot         []byte
	RestoreSnapshotDigest   string

	LeaseOwner        string
	LeaseUntil        *time.Time
	TakeoverNotBefore *time.Time
	Fence             int64
	// Attempt counts lease acquisition generations (initial claim plus stale
	// takeovers). The coordinator's one-remote-phase execution attempt is a
	// separate code-level boundary and does not rotate a live fence.
	Attempt         int
	ReceiptProvider string
	ReceiptTarget   string
	Result          json.RawMessage
	ErrorCode       string
	ErrorMessage    string
	CreatedAt       time.Time
	UpdatedAt       time.Time
	TombstonedAt    *time.Time
}

// Lease returns the complete fencing identity represented by the row.
func (o TaskDefinitionEditOperation) Lease() TaskDefinitionEditLease {
	return TaskDefinitionEditLease{
		ID:             o.ID,
		TenantID:       o.TenantID,
		UserID:         o.UserID,
		TargetTenantID: o.TargetTenantID,
		TargetUserID:   o.TargetUserID,
		TaskID:         o.TaskID,
		LeaseOwner:     o.LeaseOwner,
		Fence:          o.Fence,
	}
}

func (o TaskDefinitionEditOperation) Scope() TaskDefinitionEditScope {
	return TaskDefinitionEditScope{
		ID:             o.ID,
		TenantID:       o.TenantID,
		UserID:         o.UserID,
		TargetTenantID: o.TargetTenantID,
		TargetUserID:   o.TargetUserID,
		TaskID:         o.TaskID,
	}
}

type TaskDefinitionEditReceiptStatus string

const (
	TaskDefinitionEditReceiptStatusPending    TaskDefinitionEditReceiptStatus = "pending"
	TaskDefinitionEditReceiptStatusSent       TaskDefinitionEditReceiptStatus = "sent"
	TaskDefinitionEditReceiptStatusBlocked    TaskDefinitionEditReceiptStatus = "blocked"
	TaskDefinitionEditReceiptStatusSuppressed TaskDefinitionEditReceiptStatus = "suppressed"
)

type TaskDefinitionEditReceiptFailureClass string

const (
	TaskDefinitionEditReceiptFailureRetryable     TaskDefinitionEditReceiptFailureClass = "retryable"
	TaskDefinitionEditReceiptFailureAmbiguous     TaskDefinitionEditReceiptFailureClass = "ambiguous"
	TaskDefinitionEditReceiptFailurePermanent     TaskDefinitionEditReceiptFailureClass = "permanent"
	TaskDefinitionEditReceiptFailureTargetUnbound TaskDefinitionEditReceiptFailureClass = "target_unbound"
)

type TaskDefinitionEditReceiptLease struct {
	ID         int64
	TenantID   int64
	UserID     int64
	LeaseOwner string
	Fence      int64
}

type AcquireTaskDefinitionEditReceiptParams struct {
	ID            int64
	TenantID      int64
	UserID        int64
	LeaseOwner    string
	LeaseDuration time.Duration
}

type RecordTaskDefinitionEditReceiptSendFailureParams struct {
	Lease      TaskDefinitionEditReceiptLease
	Class      TaskDefinitionEditReceiptFailureClass
	RetryAfter time.Duration
}

// TaskDefinitionEditReceipt is one durable session receipt for the operation
// resource. Joined operation fields let a renderer build a fixed terminal card
// without a second, potentially differently scoped lookup.
type TaskDefinitionEditReceipt struct {
	ID          int64
	OperationID string
	TenantID    int64
	UserID      int64
	SessionID   int64
	Provider    string
	Target      string
	ProviderKey string
	Status      TaskDefinitionEditReceiptStatus

	LeaseOwner            string
	LeaseUntil            *time.Time
	TakeoverNotBefore     *time.Time
	Fence                 int64
	Attempt               int
	NextAttemptAt         time.Time
	Payload               []byte
	PayloadDigest         string
	SessionRecordedAt     *time.Time
	SessionMessagesDigest string
	ProviderMessageID     string
	FailureClass          TaskDefinitionEditReceiptFailureClass
	AmbiguousSince        *time.Time
	SentAt                *time.Time
	BlockedAt             *time.Time
	CreatedAt             time.Time
	UpdatedAt             time.Time

	OperationStatus TaskDefinitionEditOperationStatus
	OperationPhase  TaskDefinitionEditPhase
	TaskID          string
	Result          json.RawMessage
	ErrorCode       string
	ErrorMessage    string
}

func (r TaskDefinitionEditReceipt) Lease() TaskDefinitionEditReceiptLease {
	return TaskDefinitionEditReceiptLease{
		ID: r.ID, TenantID: r.TenantID, UserID: r.UserID,
		LeaseOwner: r.LeaseOwner, Fence: r.Fence,
	}
}
