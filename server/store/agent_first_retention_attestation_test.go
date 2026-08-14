package store

import (
	"errors"
	"testing"
	"time"
)

func TestAgentFirstRetentionAdoptionRequiresFreshActiveChain(t *testing.T) {
	now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	baselineDigest := "baseline"
	newerDigest := "newer"
	baseline := &AgentFirstRetentionAttestationEvent{
		ID: 2, Phase: AgentFirstRetentionPhaseBaseline, PayloadDigest: baselineDigest,
		ExpiresAt: now.Add(time.Minute),
	}
	parent := baselineDigest
	prepared := &AgentFirstRetentionAttestationEvent{
		ID: 3, Phase: AgentFirstRetentionPhasePrepared, ParentDigest: &parent,
		ExpiresAt: now.Add(time.Minute),
	}
	for _, tc := range []struct {
		name        string
		event       *AgentFirstRetentionAttestationEvent
		head        *string
		databaseNow time.Time
		cutoff      int64
		parentID    *int64
		wantStale   bool
	}{
		{"fresh baseline", baseline, &baselineDigest, now, 1, nil, false},
		{"prefence baseline", baseline, &baselineDigest, now, 2, nil, true},
		{"expired baseline", baseline, &baselineDigest, now.Add(time.Minute), 1, nil, true},
		{"superseded baseline", baseline, &newerDigest, now, 1, nil, true},
		{"fresh prepared", prepared, &baselineDigest, now, 1, retentionInt64Ptr(2), false},
		{"prefence prepared", prepared, &baselineDigest, now, 2, retentionInt64Ptr(2), true},
		{"expired prepared", prepared, &baselineDigest, now.Add(time.Minute), 1, retentionInt64Ptr(2), true},
		{"prepared parent superseded", prepared, &newerDigest, now, 1, retentionInt64Ptr(2), true},
		{"missing head", prepared, nil, now, 1, retentionInt64Ptr(2), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validateAgentFirstRetentionAdoption(
				tc.event, tc.databaseNow, tc.head, tc.cutoff, tc.parentID)
			if errors.Is(err, ErrAgentFirstRetentionAttestationStale) != tc.wantStale {
				t.Fatalf("error=%v want stale=%t", err, tc.wantStale)
			}
		})
	}
}

func retentionInt64Ptr(value int64) *int64 { return &value }
