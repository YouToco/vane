package agent

import (
	"context"
	"sync"
)

// SessionAdmissionCoordinator serializes every session-bearing Agent ingress
// for one user across Loop instances. A process may expose several authenticated
// entry points (for example owner chat and the Web Dashboard) while they still
// mutate the same active Agent session. Sharing this coordinator keeps their
// load -> model/tool work -> commit sequence under one context-aware admission
// lock, preventing duplicate active sessions and lost turns.
//
// The coordinator owns admission only. Each Loop retains its own side-write
// lifecycle and DrainSessionWrites boundary. Sessionless RunOnce callers such as
// A2A do not acquire these locks and should not share this dependency.
type SessionAdmissionCoordinator struct {
	users sync.Map // map[int64]*userTurnLock
}

// NewSessionAdmissionCoordinator creates an admission domain. Loops that can
// read or write the same users' Agent sessions must receive the same instance.
func NewSessionAdmissionCoordinator() *SessionAdmissionCoordinator {
	return &SessionAdmissionCoordinator{}
}

func (c *SessionAdmissionCoordinator) lockForUser(userID int64) *userTurnLock {
	if c == nil {
		panic("agent: nil session admission coordinator")
	}
	value, _ := c.users.LoadOrStore(userID, newUserTurnLock())
	return value.(*userTurnLock)
}

// userTurnLock is a context-aware binary semaphore. sync.Mutex cannot abandon
// Lock when an HTTP request is canceled, so a queued grounded ask could outlive
// its response deadline and then start a fresh paid/persistent turn.
type userTurnLock struct {
	token chan struct{}
}

func newUserTurnLock() *userTurnLock {
	lock := &userTurnLock{token: make(chan struct{}, 1)}
	lock.token <- struct{}{}
	return lock
}

func (m *userTurnLock) Lock(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-m.token:
		// Cancellation may race a ready token. Return it before leaving so the
		// canceled waiter cannot enter any database/model work or strand the lock.
		if err := ctx.Err(); err != nil {
			m.Unlock()
			return err
		}
		return nil
	}
}

func (m *userTurnLock) TryLock() bool {
	select {
	case <-m.token:
		return true
	default:
		return false
	}
}

func (m *userTurnLock) Unlock() {
	select {
	case m.token <- struct{}{}:
	default:
		panic("agent: unlock of unlocked user turn lock")
	}
}
