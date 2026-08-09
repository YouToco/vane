package store

import (
	"database/sql"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pressly/goose/v3"
)

func TestMigration125FreezesOneCorrectionAndOneReverification(t *testing.T) {
	payload, err := os.ReadFile(filepath.Join(
		"migrations", "125_research_grounding_correction.sql"))
	if err != nil {
		t.Fatal(err)
	}
	sql := string(payload)
	for _, required := range []string{
		"CREATE TABLE research_brief_grounding_corrections",
		"uq_research_grounding_correction_synthesis UNIQUE(synthesis_id)",
		"status IN ('prepared','corrected','grounded','rejected')",
		"protect_research_brief_grounding_correction_v1",
		"round_ordinal IN (0,1,2,3)",
		"requested_round_ordinal NOT IN (1,2,3)",
		"requested_round_ordinal=1 AND snapshot_json IS NOT NULL",
		"grounding.status='prepared' AND brief.status='spending'",
		"renderer_version}' IS NULL",
		"correction.status='prepared' AND grounding.status='rejected'",
		"correction.status='corrected' AND brief.status='spending'",
		"correction.correction_prompt=convert_to(requested_user_prompt,'UTF8')",
		"correction.verifier_prompt=convert_to(requested_user_prompt,'UTF8')",
		"research-synthesis.render/v3.6",
		"grounding_corrector",
		"research_brief_grounding_finalization_v36",
		"research_grounding_has_exact_receipt_v125",
		"research_expected_grounding_correction_prompt_v125",
		"research_expected_grounding_verifier_prompt_v125",
		"enforce_research_grounding_insert_v125",
		"enforce_research_grounding_correction_insert_v125",
		"research_correction_has_exact_candidate_receipt_v125",
		"research_grounding_correction_citations_subset_v125",
		"research_brief_matches_completion_v125",
		"research_brief_candidate_valid_v125",
		"research_grounding_verdict_valid_v125",
		"research_text_is_go_trimmed_v125",
		"grounding.id,'rejected'",
		"grounding.id,'grounded'",
		"correction.corrected_brief_payload=NEW.brief_payload",
		"final corrected research Brief differs from its completed correction receipt",
		"reservation.round_ordinal=2",
		"verifier_reservation.round_ordinal=3",
		"correction.verdict_payload,verifier_call.completion",
		"v3.6 correction history exists",
		"FOR EACH ROW EXECUTE FUNCTION enforce_research_scope_window_v33()",
		"FOR EACH ROW EXECUTE FUNCTION enforce_research_run_llm_spend_reservation_v2()",
	} {
		if !strings.Contains(sql, required) {
			t.Fatalf("migration 125 lost guard %q", required)
		}
	}
}

func TestMigration125EmptyDownRestoresV124Postgres(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Skip("DATABASE_URL is required for migration 125 integration tests")
	}
	scratchURL, drop := createScratchDB(t.Context(), t, databaseURL)
	t.Cleanup(drop)
	database, err := sql.Open("pgx", scratchURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	dir, err := fs.Sub(migrationsFS, "migrations")
	if err != nil {
		t.Fatal(err)
	}
	provider, err := goose.NewProvider(goose.DialectPostgres, database, dir,
		goose.WithAllowOutofOrder(true))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := provider.UpTo(t.Context(), 125); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.DownTo(t.Context(), 124); err != nil {
		t.Fatal(err)
	}
	var correctionTable, correctionTrigger, finalizationTrigger, admissionV6 bool
	var groundingReceiptHelper, correctionReceiptHelper bool
	var expectedPromptHelper, expectedVerifierHelper, citationSubsetHelper bool
	var briefCompletionHelper, briefCandidateHelper, groundingVerdictHelper bool
	var goTrimHelper bool
	var correctionInsertTrigger, groundingInsertTrigger bool
	var scopeTriggerFunction, reservationTriggerFunction string
	if err := database.QueryRowContext(t.Context(), `SELECT
		to_regclass('public.research_brief_grounding_corrections') IS NOT NULL,
		EXISTS (SELECT 1 FROM pg_trigger
		         WHERE tgname='protect_research_brief_grounding_correction_v1'),
		EXISTS (SELECT 1 FROM pg_trigger
		         WHERE tgname='research_brief_grounding_finalization_v36'),
		to_regprocedure(
		 'admit_research_run_llm_spend_cap_v6(bigint,bigint,text,bigint,text,integer,bigint,text,text,text,text)'
		) IS NOT NULL,
		to_regprocedure(
		 'research_grounding_has_exact_receipt_v125(bigint,text)'
		) IS NOT NULL,
		to_regprocedure(
		 'research_correction_has_exact_candidate_receipt_v125(bigint)'
		) IS NOT NULL,
		to_regprocedure(
		 'research_expected_grounding_correction_prompt_v125(bigint)'
		) IS NOT NULL,
		to_regprocedure(
		 'research_expected_grounding_verifier_prompt_v125(bigint,bytea,text)'
		) IS NOT NULL,
		to_regprocedure(
		 'research_grounding_correction_citations_subset_v125(bytea,bytea)'
		) IS NOT NULL,
		to_regprocedure(
		 'research_brief_matches_completion_v125(bytea,text)'
		) IS NOT NULL,
		to_regprocedure(
		 'research_brief_candidate_valid_v125(bigint,bytea)'
		) IS NOT NULL,
		to_regprocedure(
		 'research_grounding_verdict_valid_v125(bytea,bytea,text)'
		) IS NOT NULL,
		to_regprocedure(
		 'research_text_is_go_trimmed_v125(text)'
		) IS NOT NULL,
		EXISTS (SELECT 1 FROM pg_trigger
		         WHERE tgname='enforce_research_grounding_correction_insert_v125'),
		EXISTS (SELECT 1 FROM pg_trigger
		         WHERE tgname='enforce_research_grounding_insert_v125'),
		(SELECT proname FROM pg_trigger trigger
		 JOIN pg_proc function ON function.oid=trigger.tgfoid
		 WHERE trigger.tgname='research_scope_window_v33'),
		(SELECT proname FROM pg_trigger trigger
		 JOIN pg_proc function ON function.oid=trigger.tgfoid
		 WHERE trigger.tgname='research_run_llm_spend_reservation_v1')`,
	).Scan(&correctionTable, &correctionTrigger, &finalizationTrigger,
		&admissionV6, &groundingReceiptHelper, &correctionReceiptHelper,
		&expectedPromptHelper, &expectedVerifierHelper, &citationSubsetHelper,
		&briefCompletionHelper, &briefCandidateHelper, &groundingVerdictHelper,
		&goTrimHelper,
		&correctionInsertTrigger, &groundingInsertTrigger,
		&scopeTriggerFunction,
		&reservationTriggerFunction); err != nil {
		t.Fatal(err)
	}
	if correctionTable || correctionTrigger || finalizationTrigger || admissionV6 ||
		groundingReceiptHelper || correctionReceiptHelper || expectedPromptHelper ||
		expectedVerifierHelper || citationSubsetHelper || correctionInsertTrigger ||
		briefCompletionHelper || briefCandidateHelper || groundingVerdictHelper ||
		goTrimHelper ||
		groundingInsertTrigger ||
		scopeTriggerFunction != "enforce_research_scope_window_v33" ||
		reservationTriggerFunction != "enforce_research_run_llm_spend_reservation_v2" {
		t.Fatalf("Down mismatch table=%v correction_trigger=%v final_trigger=%v v6=%v grounding_receipt=%v correction_receipt=%v expected_prompt=%v expected_verifier=%v citation_subset=%v completion_helper=%v candidate_helper=%v verdict_helper=%v go_trim=%v correction_insert=%v grounding_insert=%v scope=%s reservation=%s",
			correctionTable, correctionTrigger, finalizationTrigger, admissionV6,
			groundingReceiptHelper, correctionReceiptHelper, expectedPromptHelper,
			expectedVerifierHelper, citationSubsetHelper, briefCompletionHelper,
			briefCandidateHelper, groundingVerdictHelper, goTrimHelper,
			correctionInsertTrigger,
			groundingInsertTrigger, scopeTriggerFunction, reservationTriggerFunction)
	}
}
