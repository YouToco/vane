package store

import (
	"testing"
	"time"

	"github.com/YouToco/vane/pusheffect"
)

func TestValidatePushEffectFailureSeparatesRetryAuthorities(t *testing.T) {
	base := pusheffect.FailureParams{
		Lease: pusheffect.Lease{
			Scope: pusheffect.Scope{
				ID:       "effect-contract",
				TenantID: 1,
				UserID:   1,
			},
			LeaseOwner: "worker-contract",
			Fence:      1,
		},
		Class: "provider_contract",
	}

	if err := validatePushEffectFailure(base, false); err != nil {
		t.Fatalf("zero-backoff ambiguous failure rejected: %v", err)
	}
	ambiguousWithBackoff := base
	ambiguousWithBackoff.RetryAfter = time.Second
	if err := validatePushEffectFailure(
		ambiguousWithBackoff, false,
	); err == nil {
		t.Fatal("ambiguous failure incorrectly accepted a retry schedule")
	}

	if err := validatePushEffectFailure(base, true); err == nil {
		t.Fatal("definite failure incorrectly accepted a zero retry schedule")
	}
	definiteWithBackoff := base
	definiteWithBackoff.RetryAfter = time.Second
	if err := validatePushEffectFailure(
		definiteWithBackoff, true,
	); err != nil {
		t.Fatalf("definite failure backoff rejected: %v", err)
	}
}
