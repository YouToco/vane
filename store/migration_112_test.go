package store

import (
	"encoding/json"
	"io/fs"
	"strconv"
	"strings"
	"testing"

	"github.com/YouToco/vane/types"
)

func TestMigration112GroundedOfficialPartialCoverageFinalizesQuietPostgres(t *testing.T) {
	fixture, synthesis := migrationPreparedBriefFixtureV31(
		t, map[int]bool{0: true}, "official")
	var manifest researchEvidenceManifestV3
	if err := json.Unmarshal(synthesis.EvidenceManifest, &manifest); err != nil ||
		len(manifest.Items) != 1 || len(manifest.ToolFailures) != 1 {
		t.Fatalf("partial manifest=%+v err=%v", manifest, err)
	}
	payload, err := types.EncodeResearchBriefPayloadV3(types.ResearchBriefPayloadV3{
		SchemaVersion: types.ResearchBriefPayloadSchemaV32,
		Assessment:    types.ResearchBriefAssessmentGroundedV31,
		Headline:      "Kimi 套餐仍需预约",
		Summary:       "成功的官方结构化状态表明套餐当前仍不能直接购买。",
		Significance:  types.ResearchBriefSignificanceNoneV3,
		Citations: []types.ResearchBriefCitationV3{{
			Kind: types.ResearchBriefCitationCurrentEvidenceV3,
			Ref:  strconv.FormatInt(manifest.Items[0].EvidenceID, 10),
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	handle, reservation := claimResearchBriefWithPendingReceiptV3(t, fixture, synthesis)
	settleResearchBriefReceiptV3(t, fixture, reservation, synthesis, payload)
	ref, err := fixture.st.FinalizeResearchBriefSynthesisV3(t.Context(),
		FinalizeResearchBriefSynthesisV3Params{
			ClaimResearchBriefSynthesisV3Params: handle,
			BriefPayload:                        payload,
		})
	if err != nil {
		t.Fatal(err)
	}
	if ref.Significance != types.ResearchBriefSignificanceNoneV3 ||
		ref.Decision != types.ResearchBriefDecisionQuietV3 || ref.DeliveryRequired {
		t.Fatalf("grounded partial Brief escaped quiet gate: %+v", ref)
	}
}

func TestMigration112DatabaseRejectsExternalOnlyAndDeliveryForgeryPostgres(t *testing.T) {
	for _, test := range []struct {
		name         string
		trustType    string
		significance string
		decision     string
		deliver      bool
		want         string
	}{
		{
			name: "external-only citation", trustType: "external",
			significance: "none", decision: "quiet", deliver: false,
			want: "112: grounded partial Brief must cite official Evidence and stay quiet",
		},
		{
			name: "official citation with forged delivery", trustType: "official",
			significance: "major", decision: "deliver", deliver: true,
			want: "112: grounded partial Brief must cite official Evidence and stay quiet",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture, synthesis := migrationPreparedBriefFixtureV31(
				t, map[int]bool{0: true}, test.trustType)
			var manifest researchEvidenceManifestV3
			if err := json.Unmarshal(synthesis.EvidenceManifest, &manifest); err != nil {
				t.Fatal(err)
			}
			payload, err := types.EncodeResearchBriefPayloadV3(types.ResearchBriefPayloadV3{
				SchemaVersion: types.ResearchBriefPayloadSchemaV32,
				Assessment:    types.ResearchBriefAssessmentGroundedV31,
				Headline:      "Kimi status is grounded",
				Summary:       "The current evidence reports the purchase status.",
				Significance:  types.ResearchBriefSignificanceNoneV3,
				Citations: []types.ResearchBriefCitationV3{{
					Kind: types.ResearchBriefCitationCurrentEvidenceV3,
					Ref:  strconv.FormatInt(manifest.Items[0].EvidenceID, 10),
				}},
			})
			if err != nil {
				t.Fatal(err)
			}
			_, reservation := claimResearchBriefWithPendingReceiptV3(t, fixture, synthesis)
			settleResearchBriefReceiptV3(t, fixture, reservation, synthesis, payload)

			tx, err := fixture.st.pool.Begin(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback(t.Context())
			if _, err := tx.Exec(t.Context(),
				`SELECT set_config('app.tenant_id',$1,true),
				        set_config('app.user_id',$2,true)`,
				strconv.FormatInt(fixture.tenantID, 10),
				strconv.FormatInt(fixture.userID, 10)); err != nil {
				t.Fatal(err)
			}
			_, err = tx.Exec(t.Context(), `
				UPDATE research_brief_syntheses
				   SET status='finalized',significance=$2,decision=$3,
				       delivery_required=$4,brief_payload=$5,
				       brief_digest=encode(sha256($5),'hex'),
				       finalized_at=clock_timestamp(),updated_at=clock_timestamp()
				 WHERE id=$1`, synthesis.ID, test.significance, test.decision,
				test.deliver, payload)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("database admitted forged grounded Brief or wrong fence: %v", err)
			}
		})
	}
}

func TestMigration112SQLHasVersionedRouteAndRollbackGuard(t *testing.T) {
	payload, err := fs.ReadFile(migrationsFS,
		"migrations/112_research_brief_grounded_partial_coverage.sql")
	if err != nil {
		t.Fatal(err)
	}
	sqlText := string(payload)
	for _, required := range []string{
		"vane.research-brief/v3.2",
		"assessment' IS DISTINCT FROM 'grounded'",
		"item->>'trust_type'='official'",
		"NEW.significance IS DISTINCT FROM 'none'",
		"NEW.decision IS DISTINCT FROM 'quiet'",
		"NEW.delivery_required IS DISTINCT FROM false",
		"grounded partial-coverage Brief evidence exists; restore from backup",
	} {
		if !strings.Contains(sqlText, required) {
			t.Fatalf("112 migration omitted %q", required)
		}
	}
}
