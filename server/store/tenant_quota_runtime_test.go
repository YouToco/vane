package store

import (
	"errors"
	"testing"

	"github.com/YouToco/vane/server/types"
)

func setRuntimeQuotaRow(
	t *testing.T,
	st *Store,
	tenantID int64,
	bucket QuotaBucket,
	tokens float64,
	rate float64,
	burst float64,
	ageSeconds float64,
) {
	t.Helper()
	if _, err := st.pool.Exec(t.Context(),
		`UPDATE tenant_quota
		    SET tokens=$3, rate=$4, burst=$5,
		        updated_at=now()-($6 * interval '1 second')
		  WHERE tenant_id=$1 AND bucket=$2`,
		tenantID, string(bucket), tokens, rate, burst, ageSeconds,
	); err != nil {
		t.Fatalf("set runtime quota row: %v", err)
	}
}

func runtimeQuotaTokens(t *testing.T, st *Store, tenantID int64, bucket QuotaBucket) float64 {
	t.Helper()
	var tokens float64
	if err := st.pool.QueryRow(t.Context(),
		`SELECT tokens FROM tenant_quota WHERE tenant_id=$1 AND bucket=$2`,
		tenantID, string(bucket),
	).Scan(&tokens); err != nil {
		t.Fatalf("load runtime quota tokens: %v", err)
	}
	return tokens
}

func TestLoadQuotaRule_ReturnsCurrentTenantRateAndBurst(t *testing.T) {
	st := quotaStore(t)
	tenantID := newQuotaTenant(t, st)
	setBucket(t, st, tenantID, QuotaLLMTokens, 50, 1.25, 101.5)

	got, err := st.LoadQuotaRule(t.Context(), tenantID, QuotaLLMTokens)
	if err != nil {
		t.Fatalf("LoadQuotaRule() error = %v", err)
	}
	want := QuotaRule{Bucket: QuotaLLMTokens, Rate: 1.25, Burst: 101.5}
	if got != want {
		t.Fatalf("LoadQuotaRule() = %+v, want %+v", got, want)
	}
}

func TestAdjustForTenant_UsesExactTenantAndLiveRule(t *testing.T) {
	st := quotaStore(t)
	tenantA := newQuotaTenant(t, st)
	tenantB := newQuotaTenant(t, st)
	setRuntimeQuotaRow(t, st, tenantA, QuotaLLMTokens, 10, 0.000001, 100, 0)
	setRuntimeQuotaRow(t, st, tenantB, QuotaLLMTokens, 80, 0.000001, 100, 0)

	if err := st.AdjustForTenant(t.Context(), tenantA, QuotaLLMTokens, -20); err != nil {
		t.Fatalf("AdjustForTenant() error = %v", err)
	}
	if got := runtimeQuotaTokens(t, st, tenantA, QuotaLLMTokens); got > -9.9 || got < -10.1 {
		t.Errorf("tenant A tokens = %.6f, want debt near -10", got)
	}
	if got := runtimeQuotaTokens(t, st, tenantB, QuotaLLMTokens); got != 80 {
		t.Errorf("tenant B tokens changed to %.6f", got)
	}
}

func TestRuntimeQuotaRule_ErrorClassification(t *testing.T) {
	st := quotaStore(t)
	tenantID := newQuotaTenant(t, st)

	if _, err := st.LoadQuotaRule(t.Context(), 0, QuotaLLMTokens); types.CodeOf(err) != types.CodeValidation {
		t.Errorf("invalid tenant error = %v, want CodeValidation", err)
	}
	if err := st.AdjustForTenant(t.Context(), 0, QuotaLLMTokens, 1); types.CodeOf(err) != types.CodeValidation {
		t.Errorf("invalid adjust tenant = %v, want CodeValidation", err)
	}
	if err := st.AdjustForTenant(t.Context(), tenantID, QuotaBucket("unknown"), 1); types.CodeOf(err) != types.CodeValidation {
		t.Errorf("invalid adjust bucket = %v, want CodeValidation", err)
	}

	if _, err := st.pool.Exec(t.Context(),
		`DELETE FROM tenant_quota WHERE tenant_id=$1 AND bucket=$2`,
		tenantID, string(QuotaLLMTokens)); err != nil {
		t.Fatal(err)
	}
	if _, err := st.LoadQuotaRule(t.Context(), tenantID, QuotaLLMTokens); !errors.Is(err, ErrQuotaExceeded) {
		t.Errorf("LoadQuotaRule missing row = %v, want ErrQuotaExceeded", err)
	}
	if err := st.AdjustForTenant(t.Context(), tenantID, QuotaLLMTokens, 1); !errors.Is(err, ErrQuotaExceeded) {
		t.Errorf("AdjustForTenant missing row = %v, want ErrQuotaExceeded", err)
	}
}
