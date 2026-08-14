package store

import (
	"errors"
	"testing"

	"github.com/YouToco/vane/server/types"
)

// TestLLMCalls_ExplicitTenantReceiptSurvivesMembershipChanges proves that a
// paid compiled LLM request remains accounted to the tenant authorized before
// the network effect. Post-effect accounting must not re-derive live
// membership: a user may belong to multiple tenants and may be revoked while
// the request is in flight.
func TestLLMCalls_ExplicitTenantReceiptSurvivesMembershipChanges(t *testing.T) {
	ctx := t.Context()
	st := tenantTestStore(t)
	uid := testUser(t, st)

	var tenantA, tenantB int64
	for _, dst := range []*int64{&tenantA, &tenantB} {
		if err := st.pool.QueryRow(ctx,
			`INSERT INTO tenants (status, plan) VALUES ('active', 'free') RETURNING id`,
		).Scan(dst); err != nil {
			t.Fatalf("create tenant: %v", err)
		}
		if _, err := st.pool.Exec(ctx,
			`INSERT INTO memberships (tenant_id, user_id, role) VALUES ($1, $2, 'owner')`,
			*dst, uid); err != nil {
			t.Fatalf("attach tenant %d: %v", *dst, err)
		}
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(cleanupCtx, t, st, `DELETE FROM llm_calls WHERE user_id = $1`, uid)
		cleanupExec(cleanupCtx, t, st, `DELETE FROM memberships WHERE user_id = $1`, uid)
		cleanupExec(cleanupCtx, t, st, `DELETE FROM tenants WHERE id IN ($1, $2)`, tenantA, tenantB)
		cleanupExec(cleanupCtx, t, st, `DELETE FROM users WHERE id = $1`, uid)
	})

	insert := func(trace string) int64 {
		t.Helper()
		id, err := st.InsertLLMCall(ctx, &types.LLMCall{
			TraceID: trace, SpanName: "score", TenantID: &tenantA, UserID: &uid,
		})
		if err != nil {
			t.Fatalf("insert exact receipt %q: %v", trace, err)
		}
		return id
	}
	assertTenant := func(id int64) {
		t.Helper()
		var got int64
		if err := st.pool.QueryRow(ctx,
			`SELECT tenant_id FROM llm_calls WHERE id = $1`, id).Scan(&got); err != nil {
			t.Fatalf("read exact receipt: %v", err)
		}
		if got != tenantA {
			t.Fatalf("receipt tenant=%d, want frozen tenant %d", got, tenantA)
		}
	}

	// Ambiguous live memberships must not influence the exact receipt.
	assertTenant(insert("compiled-llm-before-revoke"))

	// Revocation after the upstream effect must not erase or move its receipt.
	if _, err := st.pool.Exec(ctx,
		`DELETE FROM memberships WHERE tenant_id = $1 AND user_id = $2`, tenantA, uid); err != nil {
		t.Fatalf("revoke membership: %v", err)
	}
	assertTenant(insert("compiled-llm-after-revoke"))

	if _, err := st.InsertLLMCall(ctx, nil); !errors.Is(err, types.ErrValidation) {
		t.Fatalf("nil call error=%v, want validation", err)
	}
	if _, err := st.InsertLLMCall(ctx, &types.LLMCall{TenantID: &tenantA}); !errors.Is(err, types.ErrValidation) {
		t.Fatalf("tenant without user error=%v, want validation", err)
	}
	zero := int64(0)
	if _, err := st.InsertLLMCall(ctx, &types.LLMCall{TenantID: &zero, UserID: &uid}); !errors.Is(err, types.ErrValidation) {
		t.Fatalf("non-positive tenant error=%v, want validation", err)
	}
	if _, err := st.InsertLLMCall(ctx, &types.LLMCall{TenantID: &tenantA, UserID: &zero}); !errors.Is(err, types.ErrValidation) {
		t.Fatalf("non-positive user error=%v, want validation", err)
	}
}
