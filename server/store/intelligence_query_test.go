package store

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/YouToco/vane/types"
)

func TestCompileIntelligenceQueryUsesFixedCatalogAndBoundValues(t *testing.T) {
	st := &Store{}
	query := IntelligenceQuery{
		Dataset: IntelligenceRuns,
		Select:  []string{"task_ref", "result", "created_at"},
		Filters: []IntelligenceFilter{
			{Field: "task_ref", Op: "contains", Value: json.RawMessage(`"Kimi%' OR true --"`)},
		},
		OrderBy: []IntelligenceOrder{{Field: "created_at", Direction: "desc"}},
		Limit:   25,
	}
	compiled, err := st.compileIntelligenceQuery(t.Context(), nil, IntelligenceScope{
		TenantID: 7, UserID: 9,
	}, query, intelligenceCatalog[IntelligenceRuns])
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(compiled.sql, "Kimi") || strings.Contains(compiled.sql, "OR true") {
		t.Fatalf("filter value reached SQL text: %s", compiled.sql)
	}
	if !strings.Contains(compiled.sql, "tenant_id=$1") ||
		!strings.Contains(compiled.sql, "user_id=$2") {
		t.Fatalf("identity predicates missing: %s", compiled.sql)
	}
	if got := compiled.args[3]; got != `Kimi\%' OR true --` {
		t.Fatalf("escaped contains arg=%q", got)
	}
}

func TestFeedbackIntelligenceCatalogV3UsesCanonicalScopedProjection(t *testing.T) {
	if IntelligenceCatalogVersion != "vane.intelligence-catalog/v3" {
		t.Fatalf("catalog version=%q", IntelligenceCatalogVersion)
	}
	spec, ok := intelligenceCatalog[IntelligenceFeedbacks]
	if !ok {
		t.Fatal("feedbacks dataset is absent from the generic catalog")
	}
	st := &Store{}
	compiled, err := st.compileIntelligenceQuery(
		t.Context(), nil,
		IntelligenceScope{TenantID: 7, UserID: 9, TaskID: "task-kimi"},
		IntelligenceQuery{
			Dataset: IntelligenceFeedbacks,
			Select: []string{
				"task_ref", "delivered_summary", "action", "reason_code",
				"detail", "is_effective_attitude", "created_at",
			},
			Filters: []IntelligenceFilter{{
				Field: "action", Op: "eq", Value: json.RawMessage(`"deep_dive"`),
			}},
			Limit: 25,
		}, spec,
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{
		"FROM feedbacks f", "JOIN deliveries d", "JOIN push_batches b",
		"LEFT JOIN schedules s", "LEFT JOIN task_run_snapshots rs",
		"rs.user_id=b.user_id AND rs.task_id=s.id",
		"LEFT JOIN profile_claim_states pcs", "newer.profile_epoch=f.profile_epoch",
		"(newer.created_at,newer.id)>(f.created_at,f.id)",
		"left(d.body_md,2000)", "f.action='not_interested'", "btrim(f.detail)=''",
		"newer.action='misjudged'", "newer.reason_code IS NOT NULL",
		"newer.id>f.id",
		"tenant_id=$1", "user_id=$2", "task_ref=$3",
	} {
		if !strings.Contains(compiled.sql, required) {
			t.Fatalf("feedback projection is missing %q:\n%s", required, compiled.sql)
		}
	}
	if strings.Contains(compiled.sql, "'deep_dive'") {
		t.Fatalf("model filter value reached SQL text: %s", compiled.sql)
	}
	if got := compiled.args[2]; got != "task-kimi" {
		t.Fatalf("task fence arg=%v", got)
	}
	foundAction := false
	for _, arg := range compiled.args {
		if arg == "deep_dive" {
			foundAction = true
			break
		}
	}
	if !foundAction {
		t.Fatalf("feedback action is not parameter-bound: %#v", compiled.args)
	}
	if _, exists := spec.columns["tenant_id"]; exists {
		t.Fatal("tenant identity became model-selectable")
	}
	if _, exists := spec.columns["user_id"]; exists {
		t.Fatal("user identity became model-selectable")
	}
}

func TestIntelligenceCatalogV3UnifiesNativeResearchArtifacts(t *testing.T) {
	for dataset, required := range map[IntelligenceDataset][]string{
		IntelligenceRuns: {
			"LEFT JOIN research_brief_syntheses rb", "rb.run_snapshot_id=rs.id",
			"'research_v3'", "runtime_generation",
		},
		IntelligenceObservations: {
			"FROM task_run_content_provenance p", "UNION ALL",
			"FROM research_run_evidence e", "JOIN research_run_plans rp",
			"rp.id=e.plan_id", "rp.tenant_id=e.tenant_id", "rp.user_id=e.user_id",
			"rp.task_id=e.task_id", "rp.temporal_run_id=e.temporal_run_id",
			"rp.plan_digest=e.plan_digest", "substring(payload_text FROM chunk_index*8192+1 FOR 8192)",
			"generate_series", "payload_coverage", "payload_offset",
			"payload_total_chars", "payload_complete", "source_truncated", "evidence_coverage",
		},
		IntelligenceBriefs: {
			"FROM brief_snapshots b", "UNION ALL",
			"FROM research_brief_syntheses rb", "LEFT JOIN research_brief_deliveries rd",
			"rd.brief_id=rb.id", "rd.tenant_id=rb.tenant_id", "rd.user_id=rb.user_id",
			"rd.task_id=rb.task_id", "rd.run_snapshot_id=rb.run_snapshot_id",
			"rd.plan_id=rb.plan_id", "rb.status IN ('finalized','ambiguous','failed')",
			"truth_coverage", "payload_coverage", "octet_length(b.payload)",
			"octet_length(rb.brief_payload)",
		},
	} {
		spec := intelligenceCatalog[dataset]
		compiled, err := (&Store{}).compileIntelligenceQuery(
			t.Context(), nil, IntelligenceScope{TenantID: 7, UserID: 9, TaskID: "task-kimi"},
			IntelligenceQuery{Dataset: dataset}, spec,
		)
		if err != nil {
			t.Fatalf("compile %s: %v", dataset, err)
		}
		for _, fragment := range required {
			if !strings.Contains(compiled.sql, fragment) {
				t.Fatalf("%s projection is missing %q:\n%s", dataset, fragment, compiled.sql)
			}
		}
		for _, identity := range []string{"tenant_id=$1", "user_id=$2", "task_ref=$3"} {
			if !strings.Contains(compiled.sql, identity) {
				t.Fatalf("%s projection is missing %s", dataset, identity)
			}
		}
	}

	observations := intelligenceCatalog[IntelligenceObservations]
	for _, field := range []string{
		"lineage", "model_visible_result", "stored_size", "original_size",
		"source_truncated", "payload_coverage", "evidence_coverage", "trust_type",
		"payload_offset", "payload_total_chars", "payload_complete",
	} {
		if _, ok := observations.columns[field]; !ok {
			t.Fatalf("observations catalog omitted %q", field)
		}
	}
	briefs := intelligenceCatalog[IntelligenceBriefs]
	for _, field := range []string{
		"lineage", "brief_preview", "status", "truth_coverage",
		"payload_coverage", "payload_offset", "payload_total_chars", "payload_total_bytes",
		"payload_complete", "delivery_status",
	} {
		if _, ok := briefs.columns[field]; !ok {
			t.Fatalf("briefs catalog omitted %q", field)
		}
	}
}

func TestCompileIntelligenceQueryRejectsSQLAndUnknownFields(t *testing.T) {
	st := &Store{}
	for _, field := range []string{"tenant_id", "user_id", "pg_sleep(5)", "system_prompt"} {
		_, err := st.compileIntelligenceQuery(t.Context(), nil, IntelligenceScope{
			TenantID: 1, UserID: 2,
		}, IntelligenceQuery{
			Dataset: IntelligenceRuns,
			Select:  []string{field},
		}, intelligenceCatalog[IntelligenceRuns])
		if !errors.Is(err, types.ErrValidation) {
			t.Fatalf("field %q error=%v", field, err)
		}
	}
}

func TestCompileIntelligenceQueryRejectsInventedRunStatus(t *testing.T) {
	st := &Store{}
	base := IntelligenceQuery{
		Dataset: IntelligenceRuns,
		Filters: []IntelligenceFilter{{
			Field: "outcome_status", Op: "eq", Value: json.RawMessage(`"success"`),
		}},
	}
	_, err := st.compileIntelligenceQuery(
		t.Context(), nil, IntelligenceScope{TenantID: 1, UserID: 2},
		base, intelligenceCatalog[IntelligenceRuns],
	)
	if !errors.Is(err, types.ErrValidation) || !strings.Contains(err.Error(), "不存在 success") ||
		!strings.Contains(err.Error(), "result") {
		t.Fatalf("invented run status error=%v", err)
	}

	base.Filters[0].Value = json.RawMessage(`"finalized"`)
	if _, err := st.compileIntelligenceQuery(
		t.Context(), nil, IntelligenceScope{TenantID: 1, UserID: 2},
		base, intelligenceCatalog[IntelligenceRuns],
	); err != nil {
		t.Fatalf("valid finalized status rejected: %v", err)
	}

	base.Filters[0] = IntelligenceFilter{
		Field: "result", Op: "in", Value: json.RawMessage(`["content","quiet"]`),
	}
	if _, err := st.compileIntelligenceQuery(
		t.Context(), nil, IntelligenceScope{TenantID: 1, UserID: 2},
		base, intelligenceCatalog[IntelligenceRuns],
	); err != nil {
		t.Fatalf("valid result set rejected: %v", err)
	}
}

func TestIntelligenceCursorBindsScopeAndQuery(t *testing.T) {
	st := &Store{}
	st.intelligenceCursorState = &intelligenceCursorState{
		keys: map[int][]byte{1: bytes.Repeat([]byte{0x42}, 32)}, activeKey: 1,
	}
	scope := IntelligenceScope{TenantID: 11, UserID: 12, TaskID: "task-kimi"}
	after := []json.RawMessage{json.RawMessage(`"2026-08-01T00:00:00Z"`), json.RawMessage(`"task-kimi"`)}
	cursor := st.signIntelligenceCursor(scope, strings.Repeat("a", 64), after, time.Now())
	gotAfter, _, err := st.verifyIntelligenceCursor(t.Context(), scope, strings.Repeat("a", 64), cursor)
	if err != nil || len(gotAfter) != 2 || string(gotAfter[1]) != `"task-kimi"` {
		t.Fatalf("verify after=%q err=%v", gotAfter, err)
	}
	tamperedCursor := "A" + cursor[1:]
	if cursor[0] == 'A' {
		tamperedCursor = "B" + cursor[1:]
	}
	for name, candidate := range map[string]struct {
		scope  IntelligenceScope
		digest string
		cursor string
	}{
		"tenant": {IntelligenceScope{TenantID: 99, UserID: 12, TaskID: "task-kimi"}, strings.Repeat("a", 64), cursor},
		"user":   {IntelligenceScope{TenantID: 11, UserID: 99, TaskID: "task-kimi"}, strings.Repeat("a", 64), cursor},
		"task":   {IntelligenceScope{TenantID: 11, UserID: 12, TaskID: "other"}, strings.Repeat("a", 64), cursor},
		"query":  {scope, strings.Repeat("b", 64), cursor},
		"bytes":  {scope, strings.Repeat("a", 64), tamperedCursor},
	} {
		t.Run(name, func(t *testing.T) {
			if _, _, err := st.verifyIntelligenceCursor(t.Context(), candidate.scope, candidate.digest, candidate.cursor); !errors.Is(err, types.ErrValidation) {
				t.Fatalf("tampered cursor error=%v", err)
			}
		})
	}
	legacyPayload, _ := json.Marshal(intelligenceCursor{
		Version: 1, KeyVersion: 1, TenantID: scope.TenantID, UserID: scope.UserID,
		TaskID: scope.TaskID, QueryDigest: strings.Repeat("a", 64), After: after,
		AsOfUnixNano: time.Now().UnixNano(),
	})
	legacyMAC := hmac.New(sha256.New, st.intelligenceCursorState.keys[1])
	_, _ = legacyMAC.Write(legacyPayload)
	legacyCursor := base64.RawURLEncoding.EncodeToString(legacyPayload) + "." +
		base64.RawURLEncoding.EncodeToString(legacyMAC.Sum(nil))
	if _, _, err := st.verifyIntelligenceCursor(
		t.Context(), scope, strings.Repeat("a", 64), legacyCursor,
	); !errors.Is(err, types.ErrValidation) {
		t.Fatalf("signed catalog-v2 cursor was accepted: %v", err)
	}
}

func TestRelativeWindowUsesBusinessTimezone(t *testing.T) {
	location, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		t.Fatal(err)
	}
	now, err := time.Parse(time.RFC3339, "2026-08-01T01:30:00Z")
	if err != nil {
		t.Fatal(err)
	}
	start, end, err := resolveRelativeWindow(now, location, "yesterday")
	if err != nil {
		t.Fatal(err)
	}
	if got := start.Format("2006-01-02T15:04:05Z07:00"); got != "2026-07-31T00:00:00+08:00" {
		t.Fatalf("start=%s", got)
	}
	if got := end.Format("2006-01-02T15:04:05Z07:00"); got != "2026-08-01T00:00:00+08:00" {
		t.Fatalf("end=%s", got)
	}
}

func TestCanonicalJSONPreservesLargeIntegers(t *testing.T) {
	left, err := canonicalJSON([]byte(`{"n":9007199254740992}`))
	if err != nil {
		t.Fatal(err)
	}
	right, err := canonicalJSON([]byte(`{"n":9007199254740993}`))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(left, right) {
		t.Fatalf("large integer canonicalization collided: %s", left)
	}
}

func TestIntelligenceAuditSummaryIsBounded(t *testing.T) {
	query := IntelligenceQuery{Dataset: IntelligenceTasks}
	for i := 0; i < 1000; i++ {
		query.Select = append(query.Select, strings.Repeat("x", 1000))
		query.Filters = append(query.Filters, IntelligenceFilter{
			Field: strings.Repeat("f", 1000), Op: strings.Repeat("o", 1000),
		})
	}
	_, summary := intelligenceQueryAuditMaterial(query)
	if len(summary) > 16384 {
		t.Fatalf("audit summary bytes=%d", len(summary))
	}
}

func TestIntelligenceTasksCoverageStatesDefinitionBoundary(t *testing.T) {
	coverage := intelligenceCatalog[IntelligenceTasks].coverage
	if coverage.Status != "complete" {
		t.Fatalf("tasks coverage status=%q", coverage.Status)
	}
	for _, required := range []string{"当前任务定义", "不覆盖任务运行", "Brief", "Observation", "不能据此判断"} {
		if !strings.Contains(coverage.Note, required) {
			t.Fatalf("tasks coverage note is missing %q: %q", required, coverage.Note)
		}
	}
}

func TestIntelligenceRunsCoveragePublishesStatusSemantics(t *testing.T) {
	coverage := intelligenceCatalog[IntelligenceRuns].coverage
	for _, required := range []string{
		"pending/finalized/ambiguous/failed/unavailable",
		"不存在 success",
		"finalized 只表示已结算",
		"result=content/quiet/failed/interrupted",
		"created_at 倒序读取至少两行",
	} {
		if !strings.Contains(coverage.Note, required) {
			t.Fatalf("runs coverage note is missing %q: %q", required, coverage.Note)
		}
	}
}
