package types

import (
	"errors"
	"reflect"
	"sort"
	"time"
)

const (
	ExecutiveBriefSchemaVersionV1 = "vane.executive-brief/v1"
	PeriodicBriefSchemaVersionV1  = "vane.periodic-brief/v1"

	maxExecutiveHeadlineBytes = 512
	maxExecutiveTextBytes     = 4096
	maxExecutiveSignals       = 5
	maxExecutiveNextSteps     = 3
	maxExecutiveRefs          = 32
	maxPeriodicBriefInputs    = 20
)

type ExecutiveDecisionStateV1 string

const (
	ExecutiveDecisionAct                  ExecutiveDecisionStateV1 = "act"
	ExecutiveDecisionWatch                ExecutiveDecisionStateV1 = "watch"
	ExecutiveDecisionNoAction             ExecutiveDecisionStateV1 = "no_action"
	ExecutiveDecisionInsufficientEvidence ExecutiveDecisionStateV1 = "insufficient_evidence"
)

func (s ExecutiveDecisionStateV1) Valid() bool {
	switch s {
	case ExecutiveDecisionAct, ExecutiveDecisionWatch,
		ExecutiveDecisionNoAction, ExecutiveDecisionInsufficientEvidence:
		return true
	default:
		return false
	}
}

type ExecutiveSignalKindV1 string

const (
	ExecutiveSignalOpportunity ExecutiveSignalKindV1 = "opportunity"
	ExecutiveSignalRisk        ExecutiveSignalKindV1 = "risk"
	ExecutiveSignalChange      ExecutiveSignalKindV1 = "change"
	ExecutiveSignalTrend       ExecutiveSignalKindV1 = "trend"
)

func (k ExecutiveSignalKindV1) Valid() bool {
	switch k {
	case ExecutiveSignalOpportunity, ExecutiveSignalRisk,
		ExecutiveSignalChange, ExecutiveSignalTrend:
		return true
	default:
		return false
	}
}

type ExecutiveNextStepKindV1 string

const (
	ExecutiveNextStepDeepDive   ExecutiveNextStepKindV1 = "deep_dive"
	ExecutiveNextStepMonitor    ExecutiveNextStepKindV1 = "monitor"
	ExecutiveNextStepEditTask   ExecutiveNextStepKindV1 = "edit_task"
	ExecutiveNextStepCreateTask ExecutiveNextStepKindV1 = "create_task"
)

func (k ExecutiveNextStepKindV1) Valid() bool {
	switch k {
	case ExecutiveNextStepDeepDive, ExecutiveNextStepMonitor,
		ExecutiveNextStepEditTask, ExecutiveNextStepCreateTask:
		return true
	default:
		return false
	}
}

type ExecutiveGenerationModeV1 string

const (
	ExecutiveGenerationModel    ExecutiveGenerationModeV1 = "model"
	ExecutiveGenerationFallback ExecutiveGenerationModeV1 = "deterministic_fallback"
)

func (m ExecutiveGenerationModeV1) Valid() bool {
	return m == ExecutiveGenerationModel ||
		m == ExecutiveGenerationFallback
}

// ExecutiveEvidenceRefV1 binds a synthesized statement to one immutable
// Insight and exact claims inside it. BriefID is zero while an issue artifact
// is staged before Brief finalization; periodic reports require it.
type ExecutiveEvidenceRefV1 struct {
	BriefID      int64 `json:"brief_id,omitempty"`
	InsightID    int64 `json:"insight_id"`
	ClaimIndexes []int `json:"claim_indexes"`
}

func (r ExecutiveEvidenceRefV1) validate(periodic bool) error {
	if (periodic && r.BriefID <= 0) || (!periodic && r.BriefID < 0) ||
		r.InsightID <= 0 || len(r.ClaimIndexes) == 0 ||
		len(r.ClaimIndexes) > maxStructuredClaims {
		return errors.New("executive evidence reference is invalid")
	}
	last := -1
	for _, index := range r.ClaimIndexes {
		if index < 0 || index >= maxStructuredClaims || index <= last {
			return errors.New("executive evidence claim indexes are invalid")
		}
		last = index
	}
	return nil
}

type ExecutiveSignalV1 struct {
	Kind         ExecutiveSignalKindV1    `json:"kind"`
	Title        string                   `json:"title"`
	Summary      string                   `json:"summary"`
	EvidenceRefs []ExecutiveEvidenceRefV1 `json:"evidence_refs"`
}

type ExecutiveNextStepV1 struct {
	Kind         ExecutiveNextStepKindV1  `json:"kind"`
	Label        string                   `json:"label"`
	Rationale    string                   `json:"rationale"`
	EvidenceRefs []ExecutiveEvidenceRefV1 `json:"evidence_refs"`
}

// ExecutiveBriefContentV1 is the channel-neutral, evidence-bound synthesis
// shared by issue artifacts and periodic reports.
type ExecutiveBriefContentV1 struct {
	Headline         string                   `json:"headline"`
	ExecutiveSummary string                   `json:"executive_summary"`
	DecisionState    ExecutiveDecisionStateV1 `json:"decision_state"`
	WhyForYou        string                   `json:"why_for_you"`
	Signals          []ExecutiveSignalV1      `json:"signals"`
	NextSteps        []ExecutiveNextStepV1    `json:"next_steps"`
}

func (c ExecutiveBriefContentV1) validate(periodic bool) error {
	if !validBriefText(c.Headline, maxExecutiveHeadlineBytes, false) ||
		!validBriefText(c.ExecutiveSummary, maxExecutiveTextBytes, false) ||
		!c.DecisionState.Valid() ||
		!validBriefText(c.WhyForYou, maxExecutiveTextBytes, false) ||
		len(c.Signals) > maxExecutiveSignals ||
		len(c.NextSteps) > maxExecutiveNextSteps {
		return errors.New("executive brief content is invalid")
	}
	if len(c.Signals) == 0 {
		if !periodic ||
			(c.DecisionState != ExecutiveDecisionNoAction &&
				c.DecisionState != ExecutiveDecisionInsufficientEvidence) ||
			len(c.NextSteps) != 0 {
			return errors.New("executive brief content is empty")
		}
		return nil
	}
	refCount := 0
	for _, signal := range c.Signals {
		if !signal.Kind.Valid() ||
			!validBriefText(signal.Title, maxExecutiveHeadlineBytes, false) ||
			!validBriefText(signal.Summary, maxExecutiveTextBytes, false) ||
			len(signal.EvidenceRefs) == 0 {
			return errors.New("executive brief signal is invalid")
		}
		for _, ref := range signal.EvidenceRefs {
			if err := ref.validate(periodic); err != nil {
				return err
			}
			refCount++
		}
	}
	for _, step := range c.NextSteps {
		if !step.Kind.Valid() ||
			!validBriefText(step.Label, maxExecutiveHeadlineBytes, false) ||
			!validBriefText(step.Rationale, maxExecutiveTextBytes, false) ||
			len(step.EvidenceRefs) == 0 {
			return errors.New("executive brief next step is invalid")
		}
		for _, ref := range step.EvidenceRefs {
			if err := ref.validate(periodic); err != nil {
				return err
			}
			refCount++
		}
	}
	if refCount > maxExecutiveRefs {
		return errors.New("executive brief has too many evidence references")
	}
	return nil
}

// ValidateIssue verifies content for a single-run artifact. Evidence may omit
// BriefID until the canonical Brief is frozen, but it must name exact Insights
// and claim indexes.
func (c ExecutiveBriefContentV1) ValidateIssue() error {
	return c.validate(false)
}

// ValidatePeriodic verifies content for a cross-run report. Every evidence
// reference must include its immutable canonical Brief identity.
func (c ExecutiveBriefContentV1) ValidatePeriodic() error {
	return c.validate(true)
}

// BindBriefID returns a deep copy whose evidence references are attached to
// the immutable canonical Brief. Existing non-zero identities must agree.
func (c ExecutiveBriefContentV1) BindBriefID(
	briefID int64,
) (ExecutiveBriefContentV1, error) {
	if briefID <= 0 || c.ValidateIssue() != nil {
		return ExecutiveBriefContentV1{},
			errors.New("executive brief identity binding is invalid")
	}
	bound := c
	bound.Signals = append([]ExecutiveSignalV1(nil), c.Signals...)
	for index := range bound.Signals {
		bound.Signals[index].EvidenceRefs = append(
			[]ExecutiveEvidenceRefV1(nil),
			c.Signals[index].EvidenceRefs...)
		for refIndex := range bound.Signals[index].EvidenceRefs {
			ref := &bound.Signals[index].EvidenceRefs[refIndex]
			if ref.BriefID != 0 && ref.BriefID != briefID {
				return ExecutiveBriefContentV1{},
					errors.New("executive brief identity already differs")
			}
			ref.BriefID = briefID
		}
	}
	bound.NextSteps = append(
		[]ExecutiveNextStepV1(nil), c.NextSteps...)
	for index := range bound.NextSteps {
		bound.NextSteps[index].EvidenceRefs = append(
			[]ExecutiveEvidenceRefV1(nil),
			c.NextSteps[index].EvidenceRefs...)
		for refIndex := range bound.NextSteps[index].EvidenceRefs {
			ref := &bound.NextSteps[index].EvidenceRefs[refIndex]
			if ref.BriefID != 0 && ref.BriefID != briefID {
				return ExecutiveBriefContentV1{},
					errors.New("executive brief identity already differs")
			}
			ref.BriefID = briefID
		}
	}
	if err := bound.ValidatePeriodic(); err != nil {
		return ExecutiveBriefContentV1{}, err
	}
	return bound, nil
}

type ExecutiveBriefArtifactDraftV1 struct {
	SchemaVersion  string                    `json:"schema_version"`
	RunOutcomeID   int64                     `json:"run_outcome_id"`
	RunSnapshotID  int64                     `json:"run_snapshot_id"`
	PushBatchID    int64                     `json:"push_batch_id"`
	TenantID       int64                     `json:"tenant_id"`
	UserID         int64                     `json:"user_id"`
	TaskID         string                    `json:"task_id"`
	ProfileEpoch   int64                     `json:"profile_epoch"`
	ProfileVersion int64                     `json:"profile_version"`
	ProfileDigest  string                    `json:"profile_digest"`
	InputDigest    string                    `json:"input_digest"`
	GenerationMode ExecutiveGenerationModeV1 `json:"generation_mode"`
	Processing     RunCompletenessV1         `json:"processing"`
	GeneratedAt    time.Time                 `json:"generated_at"`
	Content        ExecutiveBriefContentV1   `json:"content"`
}

func (d ExecutiveBriefArtifactDraftV1) Canonical() (
	ExecutiveBriefArtifactDraftV1, error,
) {
	d.GeneratedAt = canonicalBriefTime(d.GeneratedAt)
	d.Content.Signals = append([]ExecutiveSignalV1(nil), d.Content.Signals...)
	for index := range d.Content.Signals {
		d.Content.Signals[index].EvidenceRefs = append(
			[]ExecutiveEvidenceRefV1(nil),
			d.Content.Signals[index].EvidenceRefs...)
	}
	d.Content.NextSteps = append(
		[]ExecutiveNextStepV1(nil), d.Content.NextSteps...)
	for index := range d.Content.NextSteps {
		d.Content.NextSteps[index].EvidenceRefs = append(
			[]ExecutiveEvidenceRefV1(nil),
			d.Content.NextSteps[index].EvidenceRefs...)
	}
	if err := d.Validate(); err != nil {
		return ExecutiveBriefArtifactDraftV1{}, err
	}
	return d, nil
}

func (d ExecutiveBriefArtifactDraftV1) Validate() error {
	if d.SchemaVersion != ExecutiveBriefSchemaVersionV1 ||
		d.RunOutcomeID <= 0 || d.RunSnapshotID <= 0 || d.PushBatchID <= 0 ||
		d.TenantID <= 0 || d.UserID <= 0 ||
		!validBriefText(d.TaskID, maxBriefTaskIDBytes, false) ||
		d.ProfileEpoch < 0 || d.ProfileVersion < 0 ||
		!validBriefDigest(d.ProfileDigest) ||
		!validBriefDigest(d.InputDigest) ||
		!d.GenerationMode.Valid() || !d.Processing.Valid() ||
		d.GeneratedAt.IsZero() ||
		d.GeneratedAt != canonicalBriefTime(d.GeneratedAt) ||
		d.Content.validate(false) != nil {
		return errors.New("executive brief artifact draft is invalid")
	}
	if d.GenerationMode == ExecutiveGenerationModel &&
		d.Processing != RunCompletenessComplete {
		return errors.New("model executive brief must be complete")
	}
	if d.GenerationMode == ExecutiveGenerationFallback &&
		d.Processing != RunCompletenessPartial {
		return errors.New("fallback executive brief must be partial")
	}
	return nil
}

func (d ExecutiveBriefArtifactDraftV1) RequestDigest() (string, error) {
	canonical, err := d.Canonical()
	if err != nil {
		return "", err
	}
	return digestJSON(canonical)
}

type ExecutiveBriefArtifactV1 struct {
	ID              int64  `json:"id"`
	BriefSnapshotID int64  `json:"brief_snapshot_id"`
	Digest          string `json:"digest"`
	ExecutiveBriefArtifactDraftV1
}

func (d ExecutiveBriefArtifactDraftV1) Seal(
	id int64, briefSnapshotID int64,
) (ExecutiveBriefArtifactV1, error) {
	canonical, err := d.Canonical()
	if err != nil || id <= 0 || briefSnapshotID <= 0 {
		if err != nil {
			return ExecutiveBriefArtifactV1{}, err
		}
		return ExecutiveBriefArtifactV1{},
			errors.New("executive brief artifact identity is invalid")
	}
	artifact := ExecutiveBriefArtifactV1{
		ID: id, BriefSnapshotID: briefSnapshotID,
		ExecutiveBriefArtifactDraftV1: canonical,
	}
	digest, err := digestJSON(struct {
		ID              int64 `json:"id"`
		BriefSnapshotID int64 `json:"brief_snapshot_id"`
		ExecutiveBriefArtifactDraftV1
	}{
		ID: id, BriefSnapshotID: briefSnapshotID,
		ExecutiveBriefArtifactDraftV1: canonical,
	})
	if err != nil {
		return ExecutiveBriefArtifactV1{}, err
	}
	artifact.Digest = digest
	return artifact, nil
}

func (a ExecutiveBriefArtifactV1) Validate() error {
	sealed, err := a.ExecutiveBriefArtifactDraftV1.Seal(
		a.ID, a.BriefSnapshotID)
	if err != nil || !validBriefDigest(a.Digest) ||
		!reflect.DeepEqual(sealed.ExecutiveBriefArtifactDraftV1,
			a.ExecutiveBriefArtifactDraftV1) ||
		!equalBriefDigest(sealed.Digest, a.Digest) {
		if err != nil {
			return err
		}
		return errors.New("executive brief artifact is invalid")
	}
	return nil
}

type PeriodicBriefInputV1 struct {
	BriefID int64  `json:"brief_id"`
	Digest  string `json:"digest"`
}

type PeriodicBriefReportDraftV1 struct {
	SchemaVersion  string                    `json:"schema_version"`
	TenantID       int64                     `json:"tenant_id"`
	UserID         int64                     `json:"user_id"`
	TaskID         string                    `json:"task_id"`
	Cadence        string                    `json:"cadence"`
	Timezone       string                    `json:"timezone"`
	PeriodStart    time.Time                 `json:"period_start"`
	PeriodEnd      time.Time                 `json:"period_end"`
	GeneratedAt    time.Time                 `json:"generated_at"`
	ProfileEpoch   int64                     `json:"profile_epoch"`
	ProfileVersion int64                     `json:"profile_version"`
	ProfileDigest  string                    `json:"profile_digest"`
	InputDigest    string                    `json:"input_digest"`
	Inputs         []PeriodicBriefInputV1    `json:"inputs"`
	GenerationMode ExecutiveGenerationModeV1 `json:"generation_mode"`
	SourceCoverage RunCompletenessV1         `json:"source_coverage"`
	Processing     RunCompletenessV1         `json:"processing"`
	Content        ExecutiveBriefContentV1   `json:"content"`
}

func (d PeriodicBriefReportDraftV1) Canonical() (
	PeriodicBriefReportDraftV1, error,
) {
	d.PeriodStart = canonicalBriefTime(d.PeriodStart)
	d.PeriodEnd = canonicalBriefTime(d.PeriodEnd)
	d.GeneratedAt = canonicalBriefTime(d.GeneratedAt)
	d.Inputs = append([]PeriodicBriefInputV1(nil), d.Inputs...)
	sort.Slice(d.Inputs, func(i, j int) bool {
		return d.Inputs[i].BriefID < d.Inputs[j].BriefID
	})
	if err := d.Validate(); err != nil {
		return PeriodicBriefReportDraftV1{}, err
	}
	return d, nil
}

func (d PeriodicBriefReportDraftV1) Validate() error {
	if d.SchemaVersion != PeriodicBriefSchemaVersionV1 ||
		d.TenantID <= 0 || d.UserID <= 0 ||
		!validBriefText(d.TaskID, maxBriefTaskIDBytes, false) ||
		(d.Cadence != "daily" && d.Cadence != "weekly" &&
			d.Cadence != "monthly") ||
		!validBriefText(d.Timezone, 255, false) ||
		d.PeriodStart.IsZero() || d.PeriodEnd.IsZero() ||
		!d.PeriodStart.Before(d.PeriodEnd) ||
		d.PeriodStart != canonicalBriefTime(d.PeriodStart) ||
		d.PeriodEnd != canonicalBriefTime(d.PeriodEnd) ||
		d.GeneratedAt.IsZero() ||
		d.GeneratedAt != canonicalBriefTime(d.GeneratedAt) ||
		d.ProfileEpoch < 0 || d.ProfileVersion < 0 ||
		!validBriefDigest(d.ProfileDigest) ||
		!validBriefDigest(d.InputDigest) ||
		len(d.Inputs) > maxPeriodicBriefInputs ||
		!d.GenerationMode.Valid() ||
		!d.SourceCoverage.Valid() || !d.Processing.Valid() ||
		d.Content.validate(true) != nil {
		return errors.New("periodic brief report draft is invalid")
	}
	lastID := int64(0)
	for _, input := range d.Inputs {
		if input.BriefID <= lastID || !validBriefDigest(input.Digest) {
			return errors.New("periodic brief inputs are invalid")
		}
		lastID = input.BriefID
	}
	if d.GenerationMode == ExecutiveGenerationModel &&
		d.Processing != RunCompletenessComplete {
		return errors.New("model periodic brief must be complete")
	}
	if d.GenerationMode == ExecutiveGenerationFallback &&
		d.Processing != RunCompletenessPartial &&
		!(len(d.Content.Signals) == 0 &&
			d.Processing == RunCompletenessComplete) {
		return errors.New("fallback periodic brief must be partial")
	}
	return nil
}

func (d PeriodicBriefReportDraftV1) RequestDigest() (string, error) {
	canonical, err := d.Canonical()
	if err != nil {
		return "", err
	}
	return digestJSON(canonical)
}

type PeriodicBriefReportV1 struct {
	ID     int64  `json:"id"`
	Digest string `json:"digest"`
	PeriodicBriefReportDraftV1
}

func (d PeriodicBriefReportDraftV1) Seal(
	id int64,
) (PeriodicBriefReportV1, error) {
	canonical, err := d.Canonical()
	if err != nil || id <= 0 {
		if err != nil {
			return PeriodicBriefReportV1{}, err
		}
		return PeriodicBriefReportV1{},
			errors.New("periodic brief report identity is invalid")
	}
	report := PeriodicBriefReportV1{
		ID: id, PeriodicBriefReportDraftV1: canonical,
	}
	digest, err := digestJSON(struct {
		ID int64 `json:"id"`
		PeriodicBriefReportDraftV1
	}{ID: id, PeriodicBriefReportDraftV1: canonical})
	if err != nil {
		return PeriodicBriefReportV1{}, err
	}
	report.Digest = digest
	return report, nil
}

func (r PeriodicBriefReportV1) Validate() error {
	sealed, err := r.PeriodicBriefReportDraftV1.Seal(r.ID)
	if err != nil || !validBriefDigest(r.Digest) ||
		!reflect.DeepEqual(sealed.PeriodicBriefReportDraftV1,
			r.PeriodicBriefReportDraftV1) ||
		!equalBriefDigest(sealed.Digest, r.Digest) {
		if err != nil {
			return err
		}
		return errors.New("periodic brief report is invalid")
	}
	return nil
}
