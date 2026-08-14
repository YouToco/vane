package types

import (
	"bytes"
	"encoding/json"
	"strings"
	"unicode/utf8"

	"github.com/YouToco/vane/internal/strictjson"
)

const ResearchGroundingVerdictSchemaV1 = "vane.research-grounding-verdict/v1"

type ResearchGroundingVerdictV1 string

const (
	ResearchGroundingGroundedV1    ResearchGroundingVerdictV1 = "grounded"
	ResearchGroundingUnsupportedV1 ResearchGroundingVerdictV1 = "unsupported"
)

type ResearchGroundingIssueV1 struct {
	Field  string                    `json:"field"`
	Claim  string                    `json:"claim"`
	Refs   []ResearchBriefCitationV3 `json:"refs"`
	Reason string                    `json:"reason"`
}

// ResearchGroundingVerdictPayloadV1 is an independent no-Tool adjudication.
// CandidateDigest binds the verdict to the exact canonical Brief bytes that
// were reviewed; it cannot be replayed against another candidate.
type ResearchGroundingVerdictPayloadV1 struct {
	SchemaVersion   string                     `json:"schema_version"`
	CandidateDigest string                     `json:"candidate_digest"`
	Verdict         ResearchGroundingVerdictV1 `json:"verdict"`
	Issues          []ResearchGroundingIssueV1 `json:"issues"`
}

func DecodeResearchGroundingVerdictV1(raw []byte) (
	ResearchGroundingVerdictPayloadV1, []byte, error,
) {
	verdict, canonical, err := NormalizeResearchGroundingVerdictV1(raw)
	if err != nil {
		return ResearchGroundingVerdictPayloadV1{}, nil, err
	}
	if !bytes.Equal(raw, canonical) {
		return ResearchGroundingVerdictPayloadV1{}, nil,
			NewAppError(CodeValidation, "research grounding verdict 必须为规范 JSON", ErrValidation)
	}
	return verdict, canonical, nil
}

// NormalizeResearchGroundingVerdictV1 accepts representation-only JSON
// whitespace and key ordering while retaining strict unknown/duplicate-field
// rejection. The provider's exact bytes remain in llm_calls; the canonical
// verdict is the immutable application artifact.
func NormalizeResearchGroundingVerdictV1(raw []byte) (
	ResearchGroundingVerdictPayloadV1, []byte, error,
) {
	if len(raw) < 2 || len(raw) > 64<<10 {
		return ResearchGroundingVerdictPayloadV1{}, nil,
			NewAppError(CodeValidation, "research grounding verdict 无效", ErrValidation)
	}
	var verdict ResearchGroundingVerdictPayloadV1
	if err := strictjson.DecodeExact(raw, &verdict); err != nil {
		return ResearchGroundingVerdictPayloadV1{}, nil,
			NewAppError(CodeValidation, "research grounding verdict 无效", err)
	}
	if err := verdict.Validate(); err != nil {
		return ResearchGroundingVerdictPayloadV1{}, nil, err
	}
	canonical, err := json.Marshal(verdict)
	if err != nil {
		return ResearchGroundingVerdictPayloadV1{}, nil,
			NewAppError(CodeValidation, "research grounding verdict 无效", ErrValidation)
	}
	return verdict, canonical, nil
}

func (v ResearchGroundingVerdictPayloadV1) Validate() error {
	if v.SchemaVersion != ResearchGroundingVerdictSchemaV1 ||
		!validResearchDigestV1(v.CandidateDigest) || len(v.Issues) > 16 {
		return NewAppError(CodeValidation, "research grounding verdict 无效", ErrValidation)
	}
	switch v.Verdict {
	case ResearchGroundingGroundedV1:
		if len(v.Issues) != 0 || v.Issues == nil {
			return NewAppError(CodeValidation, "grounded verdict 不能包含问题", ErrValidation)
		}
	case ResearchGroundingUnsupportedV1:
		if len(v.Issues) == 0 {
			return NewAppError(CodeValidation, "unsupported verdict 必须说明问题", ErrValidation)
		}
	default:
		return NewAppError(CodeValidation, "research grounding verdict 无效", ErrValidation)
	}
	for _, issue := range v.Issues {
		if (issue.Field != "headline" && issue.Field != "summary" &&
			issue.Field != "significance") || !validGroundingTextV1(issue.Claim, 4096) ||
			!validGroundingTextV1(issue.Reason, 4096) || issue.Refs == nil ||
			len(issue.Refs) > 64 {
			return NewAppError(CodeValidation, "research grounding issue 无效", ErrValidation)
		}
		seen := make(map[string]struct{}, len(issue.Refs))
		for _, ref := range issue.Refs {
			if (ref.Kind != ResearchBriefCitationCurrentEvidenceV3 &&
				ref.Kind != ResearchBriefCitationHistoryV3) ||
				!boundedResearchRefText(ref.Ref, 255) {
				return NewAppError(CodeValidation, "research grounding issue 引用无效", ErrValidation)
			}
			key := string(ref.Kind) + "\x00" + ref.Ref
			if _, duplicate := seen[key]; duplicate {
				return NewAppError(CodeValidation, "research grounding issue 引用重复", ErrValidation)
			}
			seen[key] = struct{}{}
		}
	}
	return nil
}

func validGroundingTextV1(value string, max int) bool {
	return value != "" && len(value) <= max && utf8.ValidString(value) &&
		strings.TrimSpace(value) == value && !strings.ContainsRune(value, 0)
}

func validResearchDigestV1(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, r := range value {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
			return false
		}
	}
	return true
}
