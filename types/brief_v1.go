package types

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/url"
	"reflect"
	"sort"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

const (
	RunOutcomeSchemaVersionV1              = "vane.run-outcome/v1"
	BriefSchemaVersionV1                   = "vane.brief/v1"
	StructuredInsightSchemaVersionV1       = "vane.cardgen-insight/v1"
	ObservedEventProvenanceSchemaVersionV1 = "vane.observed-event-provenance/v1"

	maxBriefTaskIDBytes      = 255
	maxBriefFailureCodeBytes = 128
	maxBriefFailureTextBytes = 4096
	maxBriefTitleBytes       = 2048
	maxBriefBodyBytes        = 262144
	maxBriefSourceTitleBytes = 2048
	maxBriefSourceURLBytes   = 8192
	maxBriefInsights         = 100
	maxBriefPayloadBytes     = 32 << 20
	maxStructuredFieldBytes  = 4096
	maxStructuredBodyBytes   = 16 << 10
	maxStructuredClaims      = 16
	maxStructuredRefs        = 8
)

// RunResultV1 separates user-visible content availability from completeness.
// Quiet is only an honest "no important change" when both completeness axes
// are complete; callers must preserve the independent axes in every outcome.
type RunResultV1 string

const (
	RunResultContent     RunResultV1 = "content"
	RunResultQuiet       RunResultV1 = "quiet"
	RunResultFailed      RunResultV1 = "failed"
	RunResultInterrupted RunResultV1 = "interrupted"
)

func (r RunResultV1) Valid() bool {
	switch r {
	case RunResultContent, RunResultQuiet, RunResultFailed, RunResultInterrupted:
		return true
	default:
		return false
	}
}

// RunCompletenessV1 is used independently for source coverage and processing.
type RunCompletenessV1 string

const (
	RunCompletenessComplete RunCompletenessV1 = "complete"
	RunCompletenessPartial  RunCompletenessV1 = "partial"
)

func (c RunCompletenessV1) Valid() bool {
	return c == RunCompletenessComplete || c == RunCompletenessPartial
}

// RunOutcomeMarkerV1 is the durable identity created before a run can
// finalize. It deliberately contains no inferred result.
type RunOutcomeMarkerV1 struct {
	ID            int64  `json:"id"`
	SchemaVersion string `json:"schema_version"`
	RunSnapshotID int64  `json:"run_snapshot_id"`
	TenantID      int64  `json:"tenant_id"`
	UserID        int64  `json:"user_id"`
	TaskID        string `json:"task_id"`
}

func (m RunOutcomeMarkerV1) Validate() error {
	if m.ID <= 0 || m.SchemaVersion != RunOutcomeSchemaVersionV1 ||
		m.RunSnapshotID <= 0 || m.TenantID <= 0 || m.UserID <= 0 ||
		!validBriefText(m.TaskID, maxBriefTaskIDBytes, false) {
		return errors.New("run outcome marker is invalid")
	}
	return nil
}

// RunOutcomeV1 is the immutable finalized result of one exact run snapshot.
// FailureMessage preserves the sanitized application-level failure text; raw
// driver/provider error chains remain outside user content and logs.
type RunOutcomeV1 struct {
	RunOutcomeMarkerV1
	Result         RunResultV1       `json:"result"`
	SourceCoverage RunCompletenessV1 `json:"source_coverage"`
	Processing     RunCompletenessV1 `json:"processing"`
	FailureCode    string            `json:"failure_code"`
	FailureMessage string            `json:"failure_message"`
	FinalizedAt    time.Time         `json:"finalized_at"`
	Digest         string            `json:"digest"`
}

// RunOutcomeClaimV1 is the semantic terminal claim supplied by workflow or
// recovery code. The database owns FinalizedAt, and the Store seals the digest
// only after reading that database time inside the finalization transaction.
type RunOutcomeClaimV1 struct {
	RunOutcomeMarkerV1
	Result         RunResultV1       `json:"result"`
	SourceCoverage RunCompletenessV1 `json:"source_coverage"`
	Processing     RunCompletenessV1 `json:"processing"`
	FailureCode    string            `json:"failure_code"`
	FailureMessage string            `json:"failure_message"`
}

func (c RunOutcomeClaimV1) Validate() error {
	probe := RunOutcomeV1{
		RunOutcomeMarkerV1: c.RunOutcomeMarkerV1,
		Result:             c.Result,
		SourceCoverage:     c.SourceCoverage,
		Processing:         c.Processing,
		FailureCode:        c.FailureCode,
		FailureMessage:     c.FailureMessage,
		FinalizedAt:        time.Unix(1, 0).UTC(),
	}
	return probe.validateUnsealed()
}

// SealAt binds a validated semantic claim to a database-generated timestamp.
func (c RunOutcomeClaimV1) SealAt(finalizedAt time.Time) (RunOutcomeV1, error) {
	return (RunOutcomeV1{
		RunOutcomeMarkerV1: c.RunOutcomeMarkerV1,
		Result:             c.Result,
		SourceCoverage:     c.SourceCoverage,
		Processing:         c.Processing,
		FailureCode:        c.FailureCode,
		FailureMessage:     c.FailureMessage,
		FinalizedAt:        finalizedAt,
	}).Seal()
}

// Matches reports whether a stored immutable outcome represents this exact
// semantic claim, intentionally ignoring the database-owned time and digest.
func (c RunOutcomeClaimV1) Matches(o RunOutcomeV1) bool {
	return c.RunOutcomeMarkerV1 == o.RunOutcomeMarkerV1 &&
		c.Result == o.Result &&
		c.SourceCoverage == o.SourceCoverage &&
		c.Processing == o.Processing &&
		c.FailureCode == o.FailureCode &&
		c.FailureMessage == o.FailureMessage
}

// Seal normalizes the timestamp to PostgreSQL microsecond precision and binds
// the complete finalized outcome with a SHA-256 digest.
func (o RunOutcomeV1) Seal() (RunOutcomeV1, error) {
	o.FinalizedAt = canonicalBriefTime(o.FinalizedAt)
	o.Digest = ""
	if err := o.validateUnsealed(); err != nil {
		return RunOutcomeV1{}, err
	}
	digest, err := digestJSON(runOutcomeDigestEnvelope(o))
	if err != nil {
		return RunOutcomeV1{}, err
	}
	o.Digest = digest
	return o, nil
}

func (o RunOutcomeV1) Validate() error {
	if err := o.validateUnsealed(); err != nil || !validBriefDigest(o.Digest) {
		if err != nil {
			return err
		}
		return errors.New("run outcome digest is invalid")
	}
	expected, err := digestJSON(runOutcomeDigestEnvelope(o))
	if err != nil {
		return err
	}
	if !equalBriefDigest(expected, o.Digest) {
		return errors.New("run outcome digest does not match")
	}
	return nil
}

func (o RunOutcomeV1) validateUnsealed() error {
	if err := o.RunOutcomeMarkerV1.Validate(); err != nil ||
		!o.Result.Valid() || !o.SourceCoverage.Valid() || !o.Processing.Valid() ||
		o.FinalizedAt.IsZero() || o.FinalizedAt != canonicalBriefTime(o.FinalizedAt) ||
		!validBriefText(o.FailureCode, maxBriefFailureCodeBytes, true) ||
		!validBriefText(o.FailureMessage, maxBriefFailureTextBytes, true) {
		return errors.New("run outcome is invalid")
	}
	failed := o.Result == RunResultFailed || o.Result == RunResultInterrupted
	if failed != (o.FailureCode != "") {
		return errors.New("run outcome failure fields do not match result")
	}
	if !failed && o.FailureMessage != "" {
		return errors.New("successful run outcome cannot carry a failure message")
	}
	return nil
}

type runOutcomeDigestV1 struct {
	ID             int64             `json:"id"`
	SchemaVersion  string            `json:"schema_version"`
	RunSnapshotID  int64             `json:"run_snapshot_id"`
	TenantID       int64             `json:"tenant_id"`
	UserID         int64             `json:"user_id"`
	TaskID         string            `json:"task_id"`
	Result         RunResultV1       `json:"result"`
	SourceCoverage RunCompletenessV1 `json:"source_coverage"`
	Processing     RunCompletenessV1 `json:"processing"`
	FailureCode    string            `json:"failure_code"`
	FailureMessage string            `json:"failure_message"`
	FinalizedAt    time.Time         `json:"finalized_at"`
}

func runOutcomeDigestEnvelope(o RunOutcomeV1) runOutcomeDigestV1 {
	return runOutcomeDigestV1{
		ID: o.ID, SchemaVersion: o.SchemaVersion,
		RunSnapshotID: o.RunSnapshotID, TenantID: o.TenantID,
		UserID: o.UserID, TaskID: o.TaskID, Result: o.Result,
		SourceCoverage: o.SourceCoverage, Processing: o.Processing,
		FailureCode: o.FailureCode, FailureMessage: o.FailureMessage,
		FinalizedAt: o.FinalizedAt,
	}
}

// InsightV1 is a frozen channel-neutral projection of one existing delivery.
// ID intentionally remains delivery_id in Phase 1 so feedback keeps one
// identity. RankPosition is persisted and never recomputed from score or ID.
type InsightV1 struct {
	ID           int64                `json:"id"`
	RankPosition int                  `json:"rank_position"`
	Title        string               `json:"title"`
	BodyMD       string               `json:"body_md"`
	SourceTitle  string               `json:"source_title"`
	SourceURL    string               `json:"source_url"`
	PublishedAt  *time.Time           `json:"published_at,omitempty"`
	DiscoveredAt time.Time            `json:"discovered_at"`
	Structured   *StructuredInsightV1 `json:"structured,omitempty"`
}

type StructuredClaimV1 struct {
	Text       string   `json:"text"`
	Excerpt    string   `json:"excerpt"`
	SourceRefs []string `json:"source_refs"`
}

type StructuredInsightV1 struct {
	SchemaVersion    string              `json:"schema_version"`
	BodyMD           string              `json:"body_md"`
	WhatChanged      string              `json:"what_changed"`
	WhyItMatters     string              `json:"why_it_matters"`
	ImportanceReason string              `json:"importance_reason"`
	Claims           []StructuredClaimV1 `json:"claims"`
	EvidenceDigest   string              `json:"evidence_digest,omitempty"`
}

// ObservedEventProvenanceV1 is the immutable, channel-neutral identity of the
// task_observed_events row that owns one future event-backed Insight. Phase
// 2-B0 only exposes the sealed Store result; no workflow, Brief, API or
// renderer consumes it until the later versioned wiring batch.
type ObservedEventProvenanceV1 struct {
	ID             int64     `json:"id"`
	SchemaVersion  string    `json:"schema_version"`
	PolicyDigest   string    `json:"policy_digest"`
	EventKey       string    `json:"event_key"`
	EventType      string    `json:"event_type"`
	Subject        string    `json:"subject"`
	OccurredAt     time.Time `json:"occurred_at"`
	EvidenceDigest string    `json:"evidence_digest"`
}

// SealObservedEventProvenanceV1 binds a reserved observed-event row to the
// canonical JSON value supplied to its evidence_json column. The Store calls
// this only after the reservation transaction has committed.
func SealObservedEventProvenanceV1(
	id int64,
	policyDigest, eventKey, eventType, subject string,
	occurredAt time.Time,
	evidenceJSON json.RawMessage,
) (ObservedEventProvenanceV1, error) {
	evidenceDigest, err := digestCanonicalJSON(evidenceJSON)
	if err != nil {
		return ObservedEventProvenanceV1{},
			errors.New("observed event evidence is invalid")
	}
	provenance := ObservedEventProvenanceV1{
		ID:             id,
		SchemaVersion:  ObservedEventProvenanceSchemaVersionV1,
		PolicyDigest:   policyDigest,
		EventKey:       eventKey,
		EventType:      eventType,
		Subject:        subject,
		OccurredAt:     canonicalBriefTime(occurredAt),
		EvidenceDigest: evidenceDigest,
	}
	if err := provenance.Validate(); err != nil {
		return ObservedEventProvenanceV1{}, err
	}
	return provenance, nil
}

func (p ObservedEventProvenanceV1) Validate() error {
	if p.ID <= 0 ||
		p.SchemaVersion != ObservedEventProvenanceSchemaVersionV1 ||
		!validBriefDigest(p.PolicyDigest) ||
		!validBriefDigest(p.EventKey) ||
		!validBriefText(p.EventType, maxStructuredFieldBytes, false) ||
		!validBriefText(p.Subject, maxStructuredFieldBytes, false) ||
		p.OccurredAt.IsZero() ||
		p.OccurredAt != canonicalBriefTime(p.OccurredAt) ||
		!validBriefDigest(p.EvidenceDigest) {
		return errors.New("observed event provenance is invalid")
	}
	return nil
}

// MatchesEvidenceJSON verifies that a later projection still refers to the
// exact canonical evidence value sealed at reservation time.
func (p ObservedEventProvenanceV1) MatchesEvidenceJSON(
	evidenceJSON json.RawMessage,
) bool {
	if err := p.Validate(); err != nil {
		return false
	}
	digest, err := digestCanonicalJSON(evidenceJSON)
	return err == nil && equalBriefDigest(digest, p.EvidenceDigest)
}

func (s StructuredInsightV1) Validate() error {
	if s.SchemaVersion != StructuredInsightSchemaVersionV1 ||
		!validBriefText(s.BodyMD, maxStructuredBodyBytes, false) ||
		!validBriefText(s.WhatChanged, maxStructuredFieldBytes, true) ||
		!validBriefText(s.WhyItMatters, maxStructuredFieldBytes, true) ||
		!validBriefText(s.ImportanceReason, maxStructuredFieldBytes, true) {
		return errors.New("structured insight is invalid")
	}
	if s.EvidenceDigest != "" && !validBriefDigest(s.EvidenceDigest) {
		return errors.New("structured insight evidence digest is invalid")
	}
	present := 0
	for _, value := range []string{s.WhatChanged, s.WhyItMatters, s.ImportanceReason} {
		if value != "" {
			present++
		}
	}
	if present != 0 && present != 3 ||
		len(s.Claims) > maxStructuredClaims ||
		(len(s.Claims) > 0 && present == 0) {
		return errors.New("structured insight projection is invalid")
	}
	for _, claim := range s.Claims {
		if !validBriefText(claim.Text, maxStructuredFieldBytes, false) ||
			!validBriefText(claim.Excerpt, maxStructuredFieldBytes, false) ||
			len(claim.SourceRefs) == 0 || len(claim.SourceRefs) > maxStructuredRefs {
			return errors.New("structured insight claim is invalid")
		}
		seen := make(map[string]struct{}, len(claim.SourceRefs))
		for _, ref := range claim.SourceRefs {
			if !validBriefText(ref, 255, false) {
				return errors.New("structured insight source reference is invalid")
			}
			if _, exists := seen[ref]; exists {
				return errors.New("structured insight source reference is duplicated")
			}
			seen[ref] = struct{}{}
		}
	}
	return nil
}

type structuredEvidenceSourceDigestV1 struct {
	Ref  string `json:"ref"`
	Text string `json:"text"`
}

type structuredEvidenceDigestV1 struct {
	SchemaVersion string                             `json:"schema_version"`
	Sources       []structuredEvidenceSourceDigestV1 `json:"sources"`
}

// SealStructuredInsightEvidenceV1 binds a structured result to the exact
// sanitized source bytes supplied to CardGen. The digest becomes part of the
// immutable Brief payload; raw source content does not.
func SealStructuredInsightEvidenceV1(
	insight StructuredInsightV1,
	sources map[string]string,
) (StructuredInsightV1, error) {
	insight.EvidenceDigest = ""
	if err := insight.Validate(); err != nil {
		return StructuredInsightV1{}, err
	}
	digest, err := structuredEvidenceDigest(sources)
	if err != nil {
		return StructuredInsightV1{}, err
	}
	insight.EvidenceDigest = digest
	if err := ValidateStructuredInsightEvidenceV1(insight, sources); err != nil {
		return StructuredInsightV1{}, err
	}
	return insight, nil
}

// ValidateStructuredInsightEvidenceV1 independently verifies the durable
// evidence digest and requires every cited ref to contain the exact excerpt.
func ValidateStructuredInsightEvidenceV1(
	insight StructuredInsightV1,
	sources map[string]string,
) error {
	if err := insight.Validate(); err != nil ||
		!validBriefDigest(insight.EvidenceDigest) {
		return errors.New("structured insight evidence is invalid")
	}
	digest, err := structuredEvidenceDigest(sources)
	if err != nil || !equalBriefDigest(digest, insight.EvidenceDigest) {
		return errors.New("structured insight evidence digest does not match")
	}
	for _, claim := range insight.Claims {
		excerpt := normalizeStructuredEvidenceText(claim.Excerpt)
		for _, ref := range claim.SourceRefs {
			source, ok := sources[ref]
			if !ok || !strings.Contains(
				normalizeStructuredEvidenceText(source), excerpt,
			) {
				return errors.New(
					"structured insight excerpt is not present in every cited source")
			}
		}
	}
	return nil
}

func structuredEvidenceDigest(sources map[string]string) (string, error) {
	if len(sources) == 0 {
		return "", errors.New("structured insight evidence sources are empty")
	}
	refs := make([]string, 0, len(sources))
	for ref, text := range sources {
		if !validBriefText(ref, 255, false) ||
			!validBriefText(text, maxBriefBodyBytes, true) {
			return "", errors.New("structured insight evidence source is invalid")
		}
		refs = append(refs, ref)
	}
	sort.Strings(refs)
	envelope := structuredEvidenceDigestV1{
		SchemaVersion: StructuredInsightSchemaVersionV1,
		Sources:       make([]structuredEvidenceSourceDigestV1, len(refs)),
	}
	for index, ref := range refs {
		envelope.Sources[index] = structuredEvidenceSourceDigestV1{
			Ref: ref, Text: sources[ref],
		}
	}
	return digestJSON(envelope)
}

func normalizeStructuredEvidenceText(value string) string {
	return strings.Join(strings.Fields(value), " ")
}

// BriefDraftV1 contains every caller-provided byte that becomes a canonical
// Brief. It has no database-generated ID, so RequestDigest is stable across a
// response-lost retry.
type BriefDraftV1 struct {
	SchemaVersion string      `json:"schema_version"`
	RunOutcomeID  int64       `json:"run_outcome_id"`
	RunSnapshotID int64       `json:"run_snapshot_id"`
	PushBatchID   int64       `json:"push_batch_id"`
	TenantID      int64       `json:"tenant_id"`
	UserID        int64       `json:"user_id"`
	TaskID        string      `json:"task_id"`
	GeneratedAt   time.Time   `json:"generated_at"`
	Insights      []InsightV1 `json:"insights"`
}

func (d BriefDraftV1) Canonical() (BriefDraftV1, error) {
	d.GeneratedAt = canonicalBriefTime(d.GeneratedAt)
	d.Insights = append([]InsightV1(nil), d.Insights...)
	for i := range d.Insights {
		if d.Insights[i].Structured != nil {
			structured := *d.Insights[i].Structured
			structured.Claims = append([]StructuredClaimV1(nil), structured.Claims...)
			for claimIndex := range structured.Claims {
				structured.Claims[claimIndex].SourceRefs = append(
					[]string(nil), structured.Claims[claimIndex].SourceRefs...)
			}
			d.Insights[i].Structured = &structured
		}
		d.Insights[i].DiscoveredAt = canonicalBriefTime(d.Insights[i].DiscoveredAt)
		if d.Insights[i].PublishedAt != nil {
			published := canonicalBriefTime(*d.Insights[i].PublishedAt)
			d.Insights[i].PublishedAt = &published
		}
	}
	if err := d.Validate(); err != nil {
		return BriefDraftV1{}, err
	}
	return d, nil
}

func (d BriefDraftV1) Validate() error {
	if d.SchemaVersion != BriefSchemaVersionV1 || d.RunOutcomeID <= 0 ||
		d.RunSnapshotID <= 0 || d.PushBatchID <= 0 ||
		d.TenantID <= 0 || d.UserID <= 0 ||
		!validBriefText(d.TaskID, maxBriefTaskIDBytes, false) ||
		d.GeneratedAt.IsZero() || d.GeneratedAt != canonicalBriefTime(d.GeneratedAt) ||
		len(d.Insights) == 0 || len(d.Insights) > maxBriefInsights {
		return errors.New("brief draft identity is invalid")
	}
	seen := make(map[int64]struct{}, len(d.Insights))
	for i, insight := range d.Insights {
		if insight.ID <= 0 || insight.RankPosition != i+1 ||
			!validBriefText(insight.Title, maxBriefTitleBytes, false) ||
			!validBriefBody(insight.BodyMD) ||
			!validBriefText(insight.SourceTitle, maxBriefSourceTitleBytes, true) ||
			!validBriefSourceURL(insight.SourceURL) ||
			insight.DiscoveredAt.IsZero() ||
			insight.DiscoveredAt != canonicalBriefTime(insight.DiscoveredAt) {
			return errors.New("brief insight is invalid")
		}
		if insight.Structured != nil {
			if err := insight.Structured.Validate(); err != nil ||
				insight.Structured.BodyMD != insight.BodyMD {
				return errors.New("brief structured insight is invalid")
			}
		}
		if insight.PublishedAt != nil &&
			(insight.PublishedAt.IsZero() ||
				*insight.PublishedAt != canonicalBriefTime(*insight.PublishedAt)) {
			return errors.New("brief insight publication time is invalid")
		}
		if _, exists := seen[insight.ID]; exists {
			return errors.New("brief insight id is duplicated")
		}
		seen[insight.ID] = struct{}{}
	}
	// JSON escaping can make the whole payload much larger than the sum of raw
	// body bytes. Probe with the largest possible sequence ID and a full digest
	// so every domain-valid Brief is also admissible under the database limit.
	payload, err := json.Marshal(BriefV1{
		ID: 1<<63 - 1, Digest: strings.Repeat("f", sha256.Size*2),
		BriefDraftV1: d,
	})
	if err != nil || len(payload) > maxBriefPayloadBytes {
		return errors.New("brief payload is too large")
	}
	return nil
}

func (d BriefDraftV1) RequestDigest() (string, error) {
	canonical, err := d.Canonical()
	if err != nil {
		return "", err
	}
	return digestJSON(canonical)
}

// BriefV1 is an immutable whole-issue snapshot. Digest covers the generated ID
// and complete payload; RequestDigest separately handles idempotent retries.
type BriefV1 struct {
	ID     int64  `json:"id"`
	Digest string `json:"digest"`
	BriefDraftV1
}

func (d BriefDraftV1) Seal(id int64) (BriefV1, error) {
	canonical, err := d.Canonical()
	if err != nil || id <= 0 {
		if err != nil {
			return BriefV1{}, err
		}
		return BriefV1{}, errors.New("brief id is invalid")
	}
	brief := BriefV1{ID: id, BriefDraftV1: canonical}
	digest, err := digestJSON(briefDigestEnvelope(brief))
	if err != nil {
		return BriefV1{}, err
	}
	brief.Digest = digest
	return brief, nil
}

func (b BriefV1) Validate() error {
	if b.ID <= 0 {
		return errors.New("brief id is invalid")
	}
	canonical, err := b.BriefDraftV1.Canonical()
	if err != nil || !reflect.DeepEqual(canonical, b.BriefDraftV1) ||
		!validBriefDigest(b.Digest) {
		if err != nil {
			return err
		}
		return errors.New("brief payload is not canonical")
	}
	expected, err := digestJSON(briefDigestEnvelope(b))
	if err != nil {
		return err
	}
	if !equalBriefDigest(expected, b.Digest) {
		return errors.New("brief digest does not match")
	}
	return nil
}

type briefDigestV1 struct {
	ID int64 `json:"id"`
	BriefDraftV1
}

func briefDigestEnvelope(b BriefV1) briefDigestV1 {
	return briefDigestV1{ID: b.ID, BriefDraftV1: b.BriefDraftV1}
}

func canonicalBriefTime(value time.Time) time.Time {
	return value.Round(0).UTC().Truncate(time.Microsecond)
}

func validBriefText(value string, maxBytes int, allowEmpty bool) bool {
	if (!allowEmpty && value == "") || len(value) > maxBytes ||
		!utf8.ValidString(value) || strings.TrimSpace(value) != value {
		return false
	}
	for _, r := range value {
		if unicode.IsControl(r) && r != '\n' && r != '\r' && r != '\t' {
			return false
		}
	}
	return true
}

func validBriefBody(value string) bool {
	return value != "" && len(value) <= maxBriefBodyBytes &&
		utf8.ValidString(value) && strings.TrimSpace(value) != ""
}

func validBriefSourceURL(value string) bool {
	if !validBriefText(value, maxBriefSourceURLBytes, false) {
		return false
	}
	parsed, err := url.Parse(value)
	return err == nil && parsed.Host != "" &&
		(parsed.Scheme == "https" || parsed.Scheme == "http") &&
		parsed.User == nil
}

func digestJSON(value any) (string, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}

func digestCanonicalJSON(raw json.RawMessage) (string, error) {
	if len(raw) == 0 || !utf8.Valid(raw) {
		return "", errors.New("canonical JSON is empty or invalid UTF-8")
	}
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return "", err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return "", errors.New("canonical JSON has trailing data")
		}
		return "", err
	}
	payload, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}

func validBriefDigest(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && hex.EncodeToString(decoded) == value
}

func equalBriefDigest(a, b string) bool {
	aBytes, aErr := hex.DecodeString(a)
	bBytes, bErr := hex.DecodeString(b)
	return aErr == nil && bErr == nil &&
		subtle.ConstantTimeCompare(aBytes, bBytes) == 1
}
