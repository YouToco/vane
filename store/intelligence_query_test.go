package store

import (
	"bytes"
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
	for name, candidate := range map[string]struct {
		scope  IntelligenceScope
		digest string
		cursor string
	}{
		"tenant": {IntelligenceScope{TenantID: 99, UserID: 12, TaskID: "task-kimi"}, strings.Repeat("a", 64), cursor},
		"user":   {IntelligenceScope{TenantID: 11, UserID: 99, TaskID: "task-kimi"}, strings.Repeat("a", 64), cursor},
		"task":   {IntelligenceScope{TenantID: 11, UserID: 12, TaskID: "other"}, strings.Repeat("a", 64), cursor},
		"query":  {scope, strings.Repeat("b", 64), cursor},
		"bytes":  {scope, strings.Repeat("a", 64), cursor[:len(cursor)-1] + "A"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, _, err := st.verifyIntelligenceCursor(t.Context(), candidate.scope, candidate.digest, candidate.cursor); !errors.Is(err, types.ErrValidation) {
				t.Fatalf("tampered cursor error=%v", err)
			}
		})
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
