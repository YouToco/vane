package types

import "time"

// ScheduleCommandKind is one user-visible mutation of an existing schedule.
// Values are persisted and therefore form a versioned database contract.
type ScheduleCommandKind string

const (
	ScheduleCommandRun    ScheduleCommandKind = "run"
	ScheduleCommandPause  ScheduleCommandKind = "pause"
	ScheduleCommandResume ScheduleCommandKind = "resume"
	ScheduleCommandDelete ScheduleCommandKind = "delete"
)

// ScheduleCommandStatus deliberately has only a non-terminal and terminal
// value. A remote timeout never means failure: recovery retries the exact
// request identity until it can prove the requested fact.
type ScheduleCommandStatus string

const (
	ScheduleCommandPending   ScheduleCommandStatus = "pending"
	ScheduleCommandCompleted ScheduleCommandStatus = "completed"
	ScheduleCommandBlocked   ScheduleCommandStatus = "blocked"
)

// ScheduleCommandPhase is the last durable checkpoint. "intent" means the
// Temporal request may or may not have been applied; deterministic request
// identity (or Delete's NotFound fact) makes retry safe. "completed" means the
// matching PostgreSQL state was committed atomically with this checkpoint.
type ScheduleCommandPhase string

const (
	ScheduleCommandIntent         ScheduleCommandPhase = "intent"
	ScheduleCommandCompletedPhase ScheduleCommandPhase = "completed"
	ScheduleCommandBlockedPhase   ScheduleCommandPhase = "blocked"
)

// ScheduleCommand is the retained idempotency and recovery record for the Web
// task control loop. IdempotencyKey is owner-scoped; PayloadDigest binds that
// key to one exact kind/task payload and RemoteRequestID binds retries to one
// Temporal Schedule patch.
type ScheduleCommand struct {
	ID              string
	TenantID        int64
	UserID          int64
	TaskID          string
	IdempotencyKey  string
	Kind            ScheduleCommandKind
	PayloadDigest   string
	RemoteRequestID string
	Status          ScheduleCommandStatus
	Phase           ScheduleCommandPhase
	CreatedAt       time.Time
	UpdatedAt       time.Time
	CompletedAt     *time.Time
	ErrorCode       string
	ErrorMessage    string
}
