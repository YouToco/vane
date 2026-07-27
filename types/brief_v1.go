package types

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/url"
	"reflect"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

const (
	RunOutcomeSchemaVersionV1 = "vane.run-outcome/v1"
	BriefSchemaVersionV1      = "vane.brief/v1"

	maxBriefTaskIDBytes      = 255
	maxBriefFailureCodeBytes = 128
	maxBriefFailureTextBytes = 4096
	maxBriefTitleBytes       = 2048
	maxBriefBodyBytes        = 262144
	maxBriefSourceTitleBytes = 2048
	maxBriefSourceURLBytes   = 8192
	maxBriefInsights         = 100
	maxBriefPayloadBytes     = 32 << 20
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
	ID           int64      `json:"id"`
	RankPosition int        `json:"rank_position"`
	Title        string     `json:"title"`
	BodyMD       string     `json:"body_md"`
	SourceTitle  string     `json:"source_title"`
	SourceURL    string     `json:"source_url"`
	PublishedAt  *time.Time `json:"published_at,omitempty"`
	DiscoveredAt time.Time  `json:"discovered_at"`
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
