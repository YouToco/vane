package fetcher

import (
	"context"
	"errors"
)

// effectGateError marks a failed live-authorization check performed immediately
// before an upstream effect. The wrapper preserves the original error chain so
// callers can still inspect AppError codes, while best-effort enrichment paths
// can distinguish revocation from an ordinary provider failure and propagate it.
type effectGateError struct {
	cause error
}

func (e *effectGateError) Error() string { return e.cause.Error() }
func (e *effectGateError) Unwrap() error { return e.cause }

func checkEffectGate(ctx context.Context, beforeEffect func(context.Context) error) error {
	if beforeEffect == nil {
		return nil
	}
	if err := beforeEffect(ctx); err != nil {
		return &effectGateError{cause: err}
	}
	return nil
}

func isEffectGateError(err error) bool {
	var denied *effectGateError
	return errors.As(err, &denied)
}
