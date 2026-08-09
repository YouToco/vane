package runcontext

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/YouToco/vane/internal/strictjson"
)

const ResearchPlannerToolSearchReceiptSchemaV1 = "vane.research-planner-tool-search-receipt/v1"

const (
	maxResearchPlannerSearchQueryBytes = 512
	maxResearchPlannerSearchMatches    = 8
	maxResearchPlannerSearchPayload    = 256 << 10
)

type ResearchPlannerToolSearchMatchV1 struct {
	Name         string `json:"name"`
	SchemaDigest string `json:"schema_digest"`
	Score        string `json:"score"`
}

// ResearchPlannerToolSearchReceiptV1 is the immutable deterministic output of
// one local BM25 search over the run's frozen authorized tool policy. It never
// carries provider routing metadata, credentials, or executable handlers.
type ResearchPlannerToolSearchReceiptV1 struct {
	SchemaVersion string                             `json:"schema_version"`
	RoundOrdinal  int                                `json:"round_ordinal"`
	CatalogDigest string                             `json:"catalog_digest"`
	Query         string                             `json:"query"`
	Limit         int                                `json:"limit"`
	Matches       []ResearchPlannerToolSearchMatchV1 `json:"matches"`
}

func CanonicalResearchPlannerSearchScoreV1(score float64) (string, error) {
	if math.IsNaN(score) || math.IsInf(score, 0) || score <= 0 {
		return "", invalidResearchPlan("planner tool search score is invalid")
	}
	canonical := strconv.FormatFloat(score, 'f', 9, 64)
	if canonical == "0.000000000" {
		return "", invalidResearchPlan("planner tool search score is below receipt precision")
	}
	return canonical, nil
}

func BuildResearchPlannerToolSearchReceiptV1(
	round int, catalogDigest, query string, limit int,
	matches []ResearchPlannerToolSearchMatchV1,
) (ResearchPlannerToolSearchReceiptV1, error) {
	receipt := ResearchPlannerToolSearchReceiptV1{
		SchemaVersion: ResearchPlannerToolSearchReceiptSchemaV1,
		RoundOrdinal:  round, CatalogDigest: catalogDigest,
		Query: query, Limit: limit,
		Matches: append([]ResearchPlannerToolSearchMatchV1(nil), matches...),
	}
	if receipt.Matches == nil {
		receipt.Matches = []ResearchPlannerToolSearchMatchV1{}
	}
	if err := receipt.Validate(); err != nil {
		return ResearchPlannerToolSearchReceiptV1{}, err
	}
	return receipt, nil
}

func (r ResearchPlannerToolSearchReceiptV1) Validate() error {
	if r.SchemaVersion != ResearchPlannerToolSearchReceiptSchemaV1 ||
		r.RoundOrdinal < 0 || r.RoundOrdinal >= 8 ||
		!validResearchPlannerDigestV1(r.CatalogDigest) ||
		r.Query == "" || len(r.Query) > maxResearchPlannerSearchQueryBytes ||
		!utf8.ValidString(r.Query) || strings.TrimSpace(r.Query) != r.Query ||
		r.Limit < 1 || r.Limit > maxResearchPlannerSearchMatches ||
		r.Matches == nil || len(r.Matches) > r.Limit {
		return invalidResearchPlan("planner tool search receipt is invalid")
	}
	seen := make(map[string]struct{}, len(r.Matches))
	for _, match := range r.Matches {
		if match.Name == "" || len(match.Name) > 255 ||
			!utf8.ValidString(match.Name) || strings.TrimSpace(match.Name) != match.Name ||
			!validResearchPlannerDigestV1(match.SchemaDigest) {
			return invalidResearchPlan("planner tool search match is invalid")
		}
		if _, duplicate := seen[match.Name]; duplicate {
			return invalidResearchPlan("planner tool search match is duplicated")
		}
		seen[match.Name] = struct{}{}
		score, err := strconv.ParseFloat(match.Score, 64)
		canonical, canonicalErr := CanonicalResearchPlannerSearchScoreV1(score)
		if err != nil || canonicalErr != nil || canonical != match.Score {
			return invalidResearchPlan("planner tool search score is not canonical")
		}
	}
	return nil
}

func EncodeResearchPlannerToolSearchReceiptV1(
	receipt ResearchPlannerToolSearchReceiptV1,
) ([]byte, error) {
	if err := receipt.Validate(); err != nil {
		return nil, err
	}
	payload, err := json.Marshal(receipt)
	if err != nil || len(payload) < 2 || len(payload) > maxResearchPlannerSearchPayload {
		return nil, invalidResearchPlan("encoded planner tool search receipt is invalid")
	}
	return payload, nil
}

func DecodeResearchPlannerToolSearchReceiptV1(
	payload []byte,
) (ResearchPlannerToolSearchReceiptV1, error) {
	if len(payload) < 2 || len(payload) > maxResearchPlannerSearchPayload {
		return ResearchPlannerToolSearchReceiptV1{}, invalidResearchPlan("planner tool search receipt JSON is invalid")
	}
	var receipt ResearchPlannerToolSearchReceiptV1
	if strictjson.DecodeExact(payload, &receipt) != nil || receipt.Validate() != nil {
		return ResearchPlannerToolSearchReceiptV1{}, invalidResearchPlan("planner tool search receipt JSON is invalid")
	}
	canonical, err := json.Marshal(receipt)
	if err != nil || !bytes.Equal(canonical, payload) {
		return ResearchPlannerToolSearchReceiptV1{}, invalidResearchPlan("planner tool search receipt JSON is not canonical")
	}
	return receipt, nil
}

func DigestResearchPlannerToolSearchReceiptV1(
	receipt ResearchPlannerToolSearchReceiptV1,
) (string, error) {
	payload, err := EncodeResearchPlannerToolSearchReceiptV1(receipt)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}

func validResearchPlannerDigestV1(value string) bool {
	if len(value) != sha256.Size*2 || strings.ToLower(value) != value {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}
