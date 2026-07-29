package types

import (
	"encoding/json"
	"errors"
	"time"
)

var (
	ErrTaskCreationReceiptBusy     = errors.New("vane: task creation receipt busy")
	ErrTaskCreationReceiptTerminal = errors.New(
		"vane: task creation receipt terminal")
	ErrTaskCreationReceiptLeaseLost = errors.New(
		"vane: task creation receipt lease lost")
)

type TaskCreationReceiptStatus string

const (
	TaskCreationReceiptStatusPending    TaskCreationReceiptStatus = "pending"
	TaskCreationReceiptStatusSent       TaskCreationReceiptStatus = "sent"
	TaskCreationReceiptStatusBlocked    TaskCreationReceiptStatus = "blocked"
	TaskCreationReceiptStatusSuppressed TaskCreationReceiptStatus = "suppressed"
)

type TaskCreationReceiptFailureClass string

const (
	TaskCreationReceiptFailureRetryable     TaskCreationReceiptFailureClass = "retryable"
	TaskCreationReceiptFailureAmbiguous     TaskCreationReceiptFailureClass = "ambiguous"
	TaskCreationReceiptFailurePermanent     TaskCreationReceiptFailureClass = "permanent"
	TaskCreationReceiptFailureTargetUnbound TaskCreationReceiptFailureClass = "target_unbound"
)

type TaskCreationReceiptLease struct {
	ID         int64
	TenantID   int64
	UserID     int64
	LeaseOwner string
	Fence      int64
}

type AcquireTaskCreationReceiptParams struct {
	ID            int64
	TenantID      int64
	UserID        int64
	LeaseOwner    string
	LeaseDuration time.Duration
}

type RecordTaskCreationReceiptSendFailureParams struct {
	Lease      TaskCreationReceiptLease
	Class      TaskCreationReceiptFailureClass
	RetryAfter time.Duration
}

// TaskCreationReceipt is one durable notification attempt for a terminal v1
// create_schedule operation. Operation fields are read through an immutable
// join so renderers need no second, potentially differently scoped lookup.
type TaskCreationReceipt struct {
	ID          int64
	OperationID string
	TenantID    int64
	UserID      int64
	SessionID   *int64
	Provider    string
	Target      string
	ProviderKey string
	Status      TaskCreationReceiptStatus

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
	FailureClass          TaskCreationReceiptFailureClass
	AmbiguousSince        *time.Time
	SentAt                *time.Time
	BlockedAt             *time.Time
	CreatedAt             time.Time
	UpdatedAt             time.Time
	OperationSummary      string
	OperationStatus       TaskOperationStatus
	OperationPhase        TaskCreationPhase
	TaskID                string
	Result                json.RawMessage
	ErrorCode             string
	ErrorMessage          string
}

func (r TaskCreationReceipt) Lease() TaskCreationReceiptLease {
	return TaskCreationReceiptLease{
		ID: r.ID, TenantID: r.TenantID, UserID: r.UserID,
		LeaseOwner: r.LeaseOwner, Fence: r.Fence,
	}
}
