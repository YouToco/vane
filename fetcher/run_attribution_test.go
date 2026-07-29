package fetcher

import (
	"context"
	"testing"

	"github.com/YouToco/vane/config"
	"github.com/YouToco/vane/tikhubcatalog"
	"github.com/YouToco/vane/types"
)

// TestCompiledFetchRecordersPreserveExactRunAttribution locks the accounting
// boundary shared by every paid compiled fetch path. The upstream effect may
// finish after cancellation or membership revocation, so the detached receipt
// must retain the immutable tenant/user pair instead of deriving a tenant from
// whatever memberships happen to exist when the receipt is written.
func TestCompiledFetchRecordersPreserveExactRunAttribution(t *testing.T) {
	const (
		traceID  = "wf-compiled-attribution"
		tenantID = int64(81)
		userID   = int64(19)
	)
	base := WithBindingRunAttribution(
		context.Background(), traceID, tenantID, userID)
	ctx, cancel := context.WithCancel(base)
	cancel()

	tests := []struct {
		name   string
		record func(*mockRecorder)
	}{
		{
			name: "exa search",
			record: func(rec *mockRecorder) {
				NewExa(config.FetchConfig{}, rec).recordCall(
					ctx, types.FetchTarget{ID: 1}, 200, 0, 0, 0, nil)
			},
		},
		{
			name: "exa contents",
			record: func(rec *mockRecorder) {
				NewExaContents(config.FetchConfig{}, rec).recordCall(
					ctx, types.FetchTarget{ID: 2}, 200, 0, 0, 0, nil)
			},
		},
		{
			name: "tikhub binding",
			record: func(rec *mockRecorder) {
				NewBinding(config.FetchConfig{}, nil, rec).record(
					ctx,
					tikhubcatalog.Entry{Name: "fixture", Path: "/fixture"},
					map[string]any{"query": "safe"}, nil, nil,
					types.FetchTarget{ID: 3},
				)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := &mockRecorder{}
			tt.record(rec)
			got := rec.last()
			if got == nil {
				t.Fatal("recorder did not receive a receipt")
			}
			if got.TraceID != traceID || got.TenantID == nil ||
				*got.TenantID != tenantID || got.UserID == nil ||
				*got.UserID != userID {
				t.Fatalf("receipt lost exact run attribution: %+v", got)
			}
			if ctxErr, hasDeadline := rec.contextState(); ctxErr != nil || !hasDeadline {
				t.Fatalf("receipt context must detach cancellation but stay bounded: err=%v deadline=%v",
					ctxErr, hasDeadline)
			}
		})
	}
}

func TestLegacyFetchAttributionDoesNotInventTenant(t *testing.T) {
	ctx := WithBindingAttribution(context.Background(), "legacy-trace", 7)
	rec := &mockRecorder{}
	NewExa(config.FetchConfig{}, rec).recordCall(
		ctx, types.FetchTarget{ID: 1}, 200, 0, 0, 0, nil)

	got := rec.last()
	if got == nil || got.UserID == nil || *got.UserID != 7 {
		t.Fatalf("legacy user attribution lost: %+v", got)
	}
	if got.TenantID != nil {
		t.Fatalf("legacy fetch must leave tenant derivation to the store: %v", *got.TenantID)
	}
}
