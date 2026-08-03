package types

import (
	"bytes"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"strings"
	"unicode/utf8"

	"github.com/YouToco/vane/internal/strictjson"
)

const ResearchBriefRefSchemaV3 = "vane.research-brief-ref/v3"

const ResearchBriefPayloadSchemaV3 = "vane.research-brief/v3"
const ResearchBriefPayloadSchemaV31 = "vane.research-brief/v3.1"

type ResearchBriefAssessmentV31 string

const (
	ResearchBriefAssessmentUnknownV31  ResearchBriefAssessmentV31 = "unknown"
	ResearchBriefAssessmentGroundedV31 ResearchBriefAssessmentV31 = "grounded"
)

type ResearchBriefSignificanceV3 string

const (
	ResearchBriefSignificanceNoneV3      ResearchBriefSignificanceV3 = "none"
	ResearchBriefSignificanceQualifiedV3 ResearchBriefSignificanceV3 = "qualified"
	ResearchBriefSignificanceMajorV3     ResearchBriefSignificanceV3 = "major"
)

func (s ResearchBriefSignificanceV3) Valid() bool {
	switch s {
	case ResearchBriefSignificanceNoneV3,
		ResearchBriefSignificanceQualifiedV3,
		ResearchBriefSignificanceMajorV3:
		return true
	default:
		return false
	}
}

type ResearchBriefDecisionV3 string

const (
	ResearchBriefDecisionQuietV3   ResearchBriefDecisionV3 = "quiet"
	ResearchBriefDecisionDeliverV3 ResearchBriefDecisionV3 = "deliver"
)

func (d ResearchBriefDecisionV3) Valid() bool {
	return d == ResearchBriefDecisionQuietV3 || d == ResearchBriefDecisionDeliverV3
}

type ResearchBriefCitationKindV3 string

const (
	ResearchBriefCitationCurrentEvidenceV3 ResearchBriefCitationKindV3 = "current_evidence"
	ResearchBriefCitationHistoryV3         ResearchBriefCitationKindV3 = "history"
)

// ResearchBriefCitationV3 names an immutable record from the Store-owned
// evidence/history manifests. Ref is the decimal Evidence id for current
// evidence and the opaque manifest record_id for retained history.
type ResearchBriefCitationV3 struct {
	Kind ResearchBriefCitationKindV3 `json:"kind"`
	Ref  string                      `json:"ref"`
}

// ResearchBriefPayloadV3 is the strict model output contract. Significance is
// part of the content-addressed payload so callers cannot submit a different
// out-of-band value to influence notification policy.
type ResearchBriefPayloadV3 struct {
	SchemaVersion string                      `json:"schema_version"`
	Assessment    ResearchBriefAssessmentV31  `json:"assessment,omitempty"`
	Headline      string                      `json:"headline"`
	Summary       string                      `json:"summary"`
	Significance  ResearchBriefSignificanceV3 `json:"significance"`
	Citations     []ResearchBriefCitationV3   `json:"citations"`
}

func DecodeResearchBriefPayloadV3(raw []byte) (ResearchBriefPayloadV3, []byte, error) {
	if len(raw) < 2 || len(raw) > 256<<10 {
		return ResearchBriefPayloadV3{}, nil, NewAppError(CodeValidation, "research Brief payload 无效", ErrValidation)
	}
	var payload ResearchBriefPayloadV3
	if err := strictjson.DecodeExact(raw, &payload); err != nil {
		return ResearchBriefPayloadV3{}, nil, NewAppError(CodeValidation, "research Brief payload 无效", err)
	}
	if err := payload.Validate(); err != nil {
		return ResearchBriefPayloadV3{}, nil, err
	}
	canonical, err := json.Marshal(payload)
	if err != nil || !bytes.Equal(raw, canonical) {
		return ResearchBriefPayloadV3{}, nil, NewAppError(CodeValidation, "research Brief payload 必须为规范 JSON", ErrValidation)
	}
	return payload, canonical, nil
}

func EncodeResearchBriefPayloadV3(payload ResearchBriefPayloadV3) ([]byte, error) {
	if err := payload.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(payload)
}

func (p ResearchBriefPayloadV3) Validate() error {
	if (p.SchemaVersion != ResearchBriefPayloadSchemaV3 &&
		p.SchemaVersion != ResearchBriefPayloadSchemaV31) || !p.Significance.Valid() ||
		!validResearchBriefPayloadTextV3(p.Headline, 1024) ||
		!validResearchBriefPayloadTextV3(p.Summary, 64<<10) ||
		len(p.Citations) > 64 {
		return NewAppError(CodeValidation, "research Brief payload 无效", ErrValidation)
	}
	if p.SchemaVersion == ResearchBriefPayloadSchemaV3 {
		if p.Assessment != "" || len(p.Citations) == 0 {
			return NewAppError(CodeValidation, "research Brief payload 无效", ErrValidation)
		}
	} else {
		switch p.Assessment {
		case ResearchBriefAssessmentUnknownV31:
			if p.Significance != ResearchBriefSignificanceNoneV3 || p.Citations == nil {
				return NewAppError(CodeValidation, "research Brief unknown assessment 必须静默", ErrValidation)
			}
		case ResearchBriefAssessmentGroundedV31:
			if len(p.Citations) == 0 {
				return NewAppError(CodeValidation, "research Brief grounded assessment 必须引用证据", ErrValidation)
			}
		default:
			return NewAppError(CodeValidation, "research Brief assessment 无效", ErrValidation)
		}
	}
	seen := make(map[string]struct{}, len(p.Citations))
	hasCurrent := false
	for _, citation := range p.Citations {
		if (citation.Kind != ResearchBriefCitationCurrentEvidenceV3 &&
			citation.Kind != ResearchBriefCitationHistoryV3) ||
			!boundedResearchRefText(citation.Ref, 255) {
			return NewAppError(CodeValidation, "research Brief citation 无效", ErrValidation)
		}
		key := string(citation.Kind) + "\x00" + citation.Ref
		if _, duplicate := seen[key]; duplicate {
			return NewAppError(CodeValidation, "research Brief citation 重复", ErrValidation)
		}
		seen[key] = struct{}{}
		if citation.Kind == ResearchBriefCitationCurrentEvidenceV3 {
			hasCurrent = true
			if citation.Ref[0] == '0' || strings.IndexFunc(citation.Ref, func(r rune) bool { return r < '0' || r > '9' }) >= 0 {
				return NewAppError(CodeValidation, "research Brief Evidence citation 无效", ErrValidation)
			}
		}
	}
	if !hasCurrent && (p.SchemaVersion == ResearchBriefPayloadSchemaV3 ||
		p.Assessment == ResearchBriefAssessmentGroundedV31) {
		return NewAppError(CodeValidation, "research Brief 必须引用当前 Evidence", ErrValidation)
	}
	return nil
}

func validResearchBriefPayloadTextV3(value string, max int) bool {
	return value != "" && len(value) <= max && utf8.ValidString(value) &&
		strings.TrimSpace(value) == value
}

// ResearchBriefRefV3 is the only synthesized Brief value allowed in Temporal
// history. It binds the frozen inputs and Store-owned notification decision,
// but deliberately excludes Brief/context/evidence bytes.
type ResearchBriefRefV3 struct {
	SchemaVersion         string                      `json:"schema_version"`
	BriefID               int64                       `json:"brief_id"`
	RunSnapshotID         int64                       `json:"run_snapshot_id"`
	PlanID                int64                       `json:"plan_id"`
	TemporalWorkflowID    string                      `json:"temporal_workflow_id"`
	TemporalRunID         string                      `json:"temporal_run_id"`
	TenantID              int64                       `json:"tenant_id"`
	UserID                int64                       `json:"user_id"`
	TaskID                string                      `json:"task_id"`
	DefinitionDigest      string                      `json:"definition_digest"`
	PlanDigest            string                      `json:"plan_digest"`
	RequestDigest         string                      `json:"request_digest"`
	BriefDigest           string                      `json:"brief_digest"`
	EvidenceDigest        string                      `json:"evidence_digest"`
	HistoryDigest         string                      `json:"history_digest"`
	NotificationThreshold string                      `json:"notification_threshold"`
	Significance          ResearchBriefSignificanceV3 `json:"significance"`
	Decision              ResearchBriefDecisionV3     `json:"decision"`
	DeliveryRequired      bool                        `json:"delivery_required"`
	ReferenceDigest       string                      `json:"reference_digest"`
}

type researchBriefRefDigestV3 struct {
	SchemaVersion         string                      `json:"schema_version"`
	BriefID               int64                       `json:"brief_id"`
	RunSnapshotID         int64                       `json:"run_snapshot_id"`
	PlanID                int64                       `json:"plan_id"`
	TemporalWorkflowID    string                      `json:"temporal_workflow_id"`
	TemporalRunID         string                      `json:"temporal_run_id"`
	TenantID              int64                       `json:"tenant_id"`
	UserID                int64                       `json:"user_id"`
	TaskID                string                      `json:"task_id"`
	DefinitionDigest      string                      `json:"definition_digest"`
	PlanDigest            string                      `json:"plan_digest"`
	RequestDigest         string                      `json:"request_digest"`
	BriefDigest           string                      `json:"brief_digest"`
	EvidenceDigest        string                      `json:"evidence_digest"`
	HistoryDigest         string                      `json:"history_digest"`
	NotificationThreshold string                      `json:"notification_threshold"`
	Significance          ResearchBriefSignificanceV3 `json:"significance"`
	Decision              ResearchBriefDecisionV3     `json:"decision"`
	DeliveryRequired      bool                        `json:"delivery_required"`
}

func SealResearchBriefRefV3(ref ResearchBriefRefV3) (ResearchBriefRefV3, error) {
	ref.SchemaVersion = ResearchBriefRefSchemaV3
	ref.ReferenceDigest = ""
	if err := validateResearchBriefRefFieldsV3(ref, false); err != nil {
		return ResearchBriefRefV3{}, err
	}
	digest, err := researchBriefReferenceDigestV3(ref)
	if err != nil {
		return ResearchBriefRefV3{}, err
	}
	ref.ReferenceDigest = digest
	return ref, nil
}

func (r ResearchBriefRefV3) ValidateFor(
	identity RunIdentity, snapshotID, planID int64,
) error {
	if err := validateResearchBriefRefFieldsV3(r, true); err != nil {
		return err
	}
	if identity.RunKind != RunSnapshotKindScheduled || snapshotID <= 0 || planID <= 0 ||
		r.RunSnapshotID != snapshotID || r.PlanID != planID ||
		r.TemporalWorkflowID != identity.TemporalWorkflowID ||
		r.TemporalRunID != identity.TemporalRunID || r.TenantID != identity.TenantID ||
		r.UserID != identity.UserID || r.TaskID != identity.TaskID {
		return NewAppError(CodeValidation, "research Brief 引用范围不匹配", ErrValidation)
	}
	expected, err := researchBriefReferenceDigestV3(r)
	if err != nil || subtle.ConstantTimeCompare([]byte(expected), []byte(r.ReferenceDigest)) != 1 {
		return NewAppError(CodeValidation, "research Brief 引用摘要无效", ErrValidation)
	}
	return nil
}

func validateResearchBriefRefFieldsV3(r ResearchBriefRefV3, requireDigest bool) error {
	if r.SchemaVersion != ResearchBriefRefSchemaV3 || r.BriefID <= 0 ||
		r.RunSnapshotID <= 0 || r.PlanID <= 0 || r.TenantID <= 0 || r.UserID <= 0 ||
		!boundedResearchRefText(r.TaskID, 255) ||
		!boundedResearchRefText(r.TemporalWorkflowID, 512) ||
		!boundedResearchRefText(r.TemporalRunID, 512) ||
		!researchSHA256(r.DefinitionDigest) || !researchSHA256(r.PlanDigest) ||
		!researchSHA256(r.RequestDigest) || !researchSHA256(r.BriefDigest) ||
		!researchSHA256(r.EvidenceDigest) || !researchSHA256(r.HistoryDigest) ||
		(requireDigest && !researchSHA256(r.ReferenceDigest)) ||
		!r.Significance.Valid() || !r.Decision.Valid() ||
		!validResearchBriefNotificationDecisionV3(r.NotificationThreshold,
			r.Significance, r.Decision, r.DeliveryRequired) {
		return NewAppError(CodeValidation, "research Brief 引用无效", ErrValidation)
	}
	return nil
}

func validResearchBriefNotificationDecisionV3(
	threshold string, significance ResearchBriefSignificanceV3,
	decision ResearchBriefDecisionV3, deliveryRequired bool,
) bool {
	deliver := false
	switch threshold {
	case "major_updates_only":
		deliver = significance == ResearchBriefSignificanceMajorV3
	case "all_qualified_updates":
		deliver = significance == ResearchBriefSignificanceQualifiedV3 ||
			significance == ResearchBriefSignificanceMajorV3
	default:
		return false
	}
	return deliveryRequired == deliver &&
		((deliver && decision == ResearchBriefDecisionDeliverV3) ||
			(!deliver && decision == ResearchBriefDecisionQuietV3))
}

func researchBriefReferenceDigestV3(r ResearchBriefRefV3) (string, error) {
	payload, err := json.Marshal(researchBriefRefDigestV3{
		SchemaVersion: r.SchemaVersion, BriefID: r.BriefID,
		RunSnapshotID: r.RunSnapshotID, PlanID: r.PlanID,
		TemporalWorkflowID: r.TemporalWorkflowID, TemporalRunID: r.TemporalRunID,
		TenantID: r.TenantID, UserID: r.UserID, TaskID: r.TaskID,
		DefinitionDigest: r.DefinitionDigest, PlanDigest: r.PlanDigest,
		RequestDigest: r.RequestDigest, BriefDigest: r.BriefDigest,
		EvidenceDigest: r.EvidenceDigest, HistoryDigest: r.HistoryDigest,
		NotificationThreshold: r.NotificationThreshold,
		Significance:          r.Significance, Decision: r.Decision,
		DeliveryRequired: r.DeliveryRequired,
	})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}
