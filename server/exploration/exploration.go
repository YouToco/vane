// Package exploration defines the bounded, Web-only projection used by
// Vane's "探索视野" surface.
//
// This package deliberately has no delivery, Feishu, workflow, LLM, or store
// dependency. Candidate production and durable storage are separate rollout
// steps; this boundary only decides which already-evidenced candidates are
// safe to show and gives the Web reader a small, deterministic feed.
package exploration

import (
	"encoding/hex"
	"errors"
	"net"
	"net/url"
	"reflect"
	"sort"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	SchemaVersionV1 = "vane.exploration-feed/v1"
	ChannelWebV1    = "web"

	defaultLimitV1 = 3
	maxLimitV1     = 3
	minScoreV1     = 55
)

type BoundaryReasonV1 string

const (
	ReasonChallengesJudgmentV1  BoundaryReasonV1 = "challenges_judgment"
	ReasonAdjacentOpportunityV1 BoundaryReasonV1 = "adjacent_opportunity"
	ReasonNewSourceV1           BoundaryReasonV1 = "new_source"
)

type FeedbackV1 string

const (
	FeedbackInspiringV1     FeedbackV1 = "inspiring"
	FeedbackOffTargetV1     FeedbackV1 = "off_target"
	FeedbackMuteDirectionV1 FeedbackV1 = "mute_direction"
)

func (f FeedbackV1) Validate() error {
	switch f {
	case FeedbackInspiringV1, FeedbackOffTargetV1, FeedbackMuteDirectionV1:
		return nil
	default:
		return errors.New("exploration feedback is invalid")
	}
}

// EvidenceSourceV1 contains public source metadata only. Raw evidence bodies,
// profile text, prompts, and internal provenance never enter this projection.
type EvidenceSourceV1 struct {
	Ref         string     `json:"ref"`
	Title       string     `json:"title"`
	SourceURL   string     `json:"source_url"`
	PublishedAt *time.Time `json:"published_at,omitempty"`
}

// CandidateV1 is prepared by a bounded producer. EvidenceReceiptID and
// EvidenceReceiptDigest identify its exact durable verification receipt;
// SelectV1 still requires an independent verifier and never trusts their shape
// alone.
type CandidateV1 struct {
	ContentItemID         int64              `json:"content_item_id"`
	DirectionKey          string             `json:"direction_key"`
	EvidenceReceiptID     int64              `json:"evidence_receipt_id"`
	EvidenceReceiptDigest string             `json:"evidence_receipt_digest"`
	Title                 string             `json:"title"`
	Summary               string             `json:"summary"`
	SourceTitle           string             `json:"source_title"`
	SourceURL             string             `json:"source_url"`
	PublishedAt           *time.Time         `json:"published_at,omitempty"`
	DiscoveredAt          time.Time          `json:"discovered_at"`
	RelevanceScore        int                `json:"relevance_score"`
	EvidenceSources       []EvidenceSourceV1 `json:"evidence_sources"`
	ChallengesJudgment    bool               `json:"challenges_judgment"`
	AdjacentOpportunity   bool               `json:"adjacent_opportunity"`
	NewSource             bool               `json:"new_source"`
}

// ItemV1 is the public Web projection. DirectionKey is an opaque feedback
// handle, not a profile or evidence digest.
type ItemV1 struct {
	ContentItemID   int64              `json:"content_item_id"`
	DirectionKey    string             `json:"direction_key"`
	Title           string             `json:"title"`
	Summary         string             `json:"summary"`
	SourceTitle     string             `json:"source_title"`
	SourceURL       string             `json:"source_url"`
	PublishedAt     *time.Time         `json:"published_at,omitempty"`
	DiscoveredAt    time.Time          `json:"discovered_at"`
	RelevanceScore  int                `json:"relevance_score"`
	EvidenceSources []EvidenceSourceV1 `json:"evidence_sources"`
	Reason          BoundaryReasonV1   `json:"reason"`
}

type FeedV1 struct {
	SchemaVersion string   `json:"schema_version"`
	Channel       string   `json:"channel"`
	Items         []ItemV1 `json:"items"`
}

type ScopeIdentityV1 struct {
	TenantID int64
	UserID   int64
	TaskID   string
}

// ExclusionSnapshotV1 must be complete even when every set is empty. Nil means
// the caller could not prove the current boundary, so selection fails closed.
type ExclusionSnapshotV1 struct {
	Scope                       ScopeIdentityV1
	SnapshotReceiptID           int64
	SnapshotDigest              string
	CanonicalContentItemIDs     map[int64]struct{}
	RecentlyShownContentItemIDs map[int64]struct{}
	MutedDirectionKeys          map[string]struct{}
}

// AuthorityVerifierV1 re-proves both the exclusion snapshot and each
// candidate's durable evidence receipt against one expected tenant/user/task.
type AuthorityVerifierV1 interface {
	VerifyExplorationScopeV1(ScopeIdentityV1, ExclusionSnapshotV1) bool
	VerifyExplorationEvidenceV1(ScopeIdentityV1, CandidateV1) bool
}

type OptionsV1 struct {
	Limit         int
	ExpectedScope ScopeIdentityV1
	Scope         *ExclusionSnapshotV1
	Verifier      AuthorityVerifierV1
}

// SelectV1 validates, deduplicates, ranks, and diversity-fills a Web-only
// exploration feed. Missing authority is an error with an empty feed. An
// individually invalid or unverified candidate is omitted so exploration
// cannot break the ordinary intelligence feed.
func SelectV1(candidates []CandidateV1, opts OptionsV1) (FeedV1, error) {
	empty := FeedV1{
		SchemaVersion: SchemaVersionV1,
		Channel:       ChannelWebV1,
		Items:         []ItemV1{},
	}
	if opts.Scope == nil ||
		!validScopeIdentityV1(opts.ExpectedScope) ||
		opts.Scope.Scope != opts.ExpectedScope ||
		opts.Scope.SnapshotReceiptID <= 0 ||
		!validDigestV1(opts.Scope.SnapshotDigest) ||
		opts.Scope.CanonicalContentItemIDs == nil ||
		opts.Scope.RecentlyShownContentItemIDs == nil ||
		opts.Scope.MutedDirectionKeys == nil {
		return empty, errors.New("exploration exclusion snapshot is incomplete")
	}
	if opts.Verifier == nil ||
		!opts.Verifier.VerifyExplorationScopeV1(
			opts.ExpectedScope, *opts.Scope) {
		return empty, errors.New("exploration scope receipt is not verified")
	}

	limit := opts.Limit
	if limit <= 0 {
		limit = defaultLimitV1
	}
	if limit > maxLimitV1 {
		limit = maxLimitV1
	}

	type candidateGroupV1 struct {
		item                  ItemV1
		evidenceReceiptID     int64
		evidenceReceiptDigest string
		conflicted            bool
	}
	groups := make(map[int64]candidateGroupV1, len(candidates))
	for _, candidate := range candidates {
		if _, canonical := opts.Scope.CanonicalContentItemIDs[candidate.ContentItemID]; canonical {
			continue
		}
		if _, recent := opts.Scope.RecentlyShownContentItemIDs[candidate.ContentItemID]; recent {
			continue
		}
		group, exists := groups[candidate.ContentItemID]
		if !opts.Verifier.VerifyExplorationEvidenceV1(
			opts.ExpectedScope, candidate) {
			group.conflicted = true
			groups[candidate.ContentItemID] = group
			continue
		}
		item, ok := projectCandidateV1(candidate)
		if !ok {
			group.conflicted = true
			groups[candidate.ContentItemID] = group
			continue
		}
		if !exists {
			groups[candidate.ContentItemID] = candidateGroupV1{
				item:                  item,
				evidenceReceiptID:     candidate.EvidenceReceiptID,
				evidenceReceiptDigest: candidate.EvidenceReceiptDigest,
			}
			continue
		}
		if group.conflicted ||
			group.evidenceReceiptID != candidate.EvidenceReceiptID ||
			group.evidenceReceiptDigest != candidate.EvidenceReceiptDigest ||
			!reflect.DeepEqual(group.item, item) {
			group.conflicted = true
			groups[candidate.ContentItemID] = group
		}
	}
	ranked := make([]ItemV1, 0, len(groups))
	for _, group := range groups {
		if group.conflicted {
			continue
		}
		if _, muted := opts.Scope.MutedDirectionKeys[group.item.DirectionKey]; muted {
			continue
		}
		ranked = append(ranked, group.item)
	}

	sort.Slice(ranked, func(i, j int) bool {
		a, b := ranked[i], ranked[j]
		if a.RelevanceScore != b.RelevanceScore {
			return a.RelevanceScore > b.RelevanceScore
		}
		if len(a.EvidenceSources) != len(b.EvidenceSources) {
			return len(a.EvidenceSources) > len(b.EvidenceSources)
		}
		at, bt := itemTimeV1(a), itemTimeV1(b)
		if !at.Equal(bt) {
			return at.After(bt)
		}
		return a.ContentItemID > b.ContentItemID
	})

	// First pass gives each boundary reason one slot. The second pass fills
	// remaining room by the same deterministic quality order.
	selected := make([]ItemV1, 0, limit)
	selectedIDs := make(map[int64]struct{}, limit)
	selectedReasons := make(map[BoundaryReasonV1]struct{}, 3)
	for _, item := range ranked {
		if len(selected) == limit {
			break
		}
		if _, exists := selectedReasons[item.Reason]; exists {
			continue
		}
		selected = append(selected, item)
		selectedIDs[item.ContentItemID] = struct{}{}
		selectedReasons[item.Reason] = struct{}{}
	}
	for _, item := range ranked {
		if len(selected) == limit {
			break
		}
		if _, exists := selectedIDs[item.ContentItemID]; exists {
			continue
		}
		selected = append(selected, item)
		selectedIDs[item.ContentItemID] = struct{}{}
	}

	return FeedV1{
		SchemaVersion: SchemaVersionV1,
		Channel:       ChannelWebV1,
		Items:         selected,
	}, nil
}

func projectCandidateV1(candidate CandidateV1) (ItemV1, bool) {
	title := strings.TrimSpace(candidate.Title)
	summary := strings.TrimSpace(candidate.Summary)
	sourceTitle := strings.TrimSpace(candidate.SourceTitle)
	if candidate.ContentItemID <= 0 ||
		!validDigestV1(candidate.DirectionKey) ||
		candidate.EvidenceReceiptID <= 0 ||
		!validDigestV1(candidate.EvidenceReceiptDigest) ||
		title == "" || utf8.RuneCountInString(title) > 240 ||
		summary == "" || utf8.RuneCountInString(summary) > 1200 ||
		sourceTitle == "" || utf8.RuneCountInString(sourceTitle) > 240 ||
		candidate.DiscoveredAt.IsZero() ||
		candidate.RelevanceScore < minScoreV1 ||
		candidate.RelevanceScore > 100 {
		return ItemV1{}, false
	}
	sourceURL, ok := canonicalPublicURLV1(candidate.SourceURL)
	if !ok {
		return ItemV1{}, false
	}
	reason, ok := candidateReasonV1(candidate)
	if !ok {
		return ItemV1{}, false
	}
	evidence, ok := validatedEvidenceV1(candidate.EvidenceSources)
	if !ok {
		return ItemV1{}, false
	}
	return ItemV1{
		ContentItemID:   candidate.ContentItemID,
		DirectionKey:    candidate.DirectionKey,
		Title:           title,
		Summary:         summary,
		SourceTitle:     sourceTitle,
		SourceURL:       sourceURL,
		PublishedAt:     canonicalTimePtrV1(candidate.PublishedAt),
		DiscoveredAt:    candidate.DiscoveredAt.Round(0).UTC(),
		RelevanceScore:  candidate.RelevanceScore,
		EvidenceSources: evidence,
		Reason:          reason,
	}, true
}

func candidateReasonV1(candidate CandidateV1) (BoundaryReasonV1, bool) {
	switch {
	case candidate.ChallengesJudgment:
		return ReasonChallengesJudgmentV1, true
	case candidate.AdjacentOpportunity:
		return ReasonAdjacentOpportunityV1, true
	case candidate.NewSource:
		return ReasonNewSourceV1, true
	default:
		return "", false
	}
}

func validatedEvidenceV1(in []EvidenceSourceV1) ([]EvidenceSourceV1, bool) {
	if len(in) == 0 || len(in) > 8 {
		return nil, false
	}
	out := make([]EvidenceSourceV1, 0, len(in))
	refs := make(map[string]struct{}, len(in))
	urls := make(map[string]struct{}, len(in))
	for _, source := range in {
		ref := strings.TrimSpace(source.Ref)
		title := strings.TrimSpace(source.Title)
		if ref == "" || utf8.RuneCountInString(ref) > 64 ||
			title == "" || utf8.RuneCountInString(title) > 240 {
			return nil, false
		}
		sourceURL, ok := canonicalPublicURLV1(source.SourceURL)
		if !ok {
			return nil, false
		}
		if _, exists := refs[ref]; exists {
			return nil, false
		}
		if _, exists := urls[sourceURL]; exists {
			return nil, false
		}
		refs[ref] = struct{}{}
		urls[sourceURL] = struct{}{}
		out = append(out, EvidenceSourceV1{
			Ref:         ref,
			Title:       title,
			SourceURL:   sourceURL,
			PublishedAt: canonicalTimePtrV1(source.PublishedAt),
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].SourceURL != out[j].SourceURL {
			return out[i].SourceURL < out[j].SourceURL
		}
		return out[i].Ref < out[j].Ref
	})
	return out, true
}

func canonicalPublicURLV1(raw string) (string, bool) {
	if raw != strings.TrimSpace(raw) {
		return "", false
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Host == "" || parsed.User != nil ||
		parsed.RawQuery != "" || parsed.ForceQuery ||
		parsed.Fragment != "" || parsed.RawFragment != "" ||
		parsed.RawPath != "" {
		return "", false
	}
	parsed.Scheme = strings.ToLower(parsed.Scheme)
	if parsed.Scheme != "https" && parsed.Scheme != "http" {
		return "", false
	}
	host := strings.ToLower(parsed.Hostname())
	if !publicHostnameV1(host) {
		return "", false
	}
	port := parsed.Port()
	if (parsed.Scheme == "https" && port == "443") ||
		(parsed.Scheme == "http" && port == "80") {
		port = ""
	}
	if port != "" {
		parsed.Host = net.JoinHostPort(host, port)
	} else if strings.Contains(host, ":") {
		parsed.Host = "[" + host + "]"
	} else {
		parsed.Host = host
	}
	return parsed.String(), true
}

func publicHostnameV1(host string) bool {
	if host == "" || host == "localhost" ||
		strings.HasSuffix(host, ".localhost") ||
		strings.HasSuffix(host, ".local") {
		return false
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return strings.Contains(host, ".")
	}
	return !ip.IsUnspecified() &&
		!ip.IsLoopback() &&
		!ip.IsPrivate() &&
		!ip.IsLinkLocalUnicast() &&
		!ip.IsLinkLocalMulticast()
}

func validDigestV1(value string) bool {
	if len(value) != 64 || strings.ToLower(value) != value {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == 32
}

func validScopeIdentityV1(scope ScopeIdentityV1) bool {
	return scope.TenantID > 0 && scope.UserID > 0 &&
		scope.TaskID != "" && scope.TaskID == strings.TrimSpace(scope.TaskID) &&
		utf8.RuneCountInString(scope.TaskID) <= 255
}

func canonicalTimePtrV1(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	canonical := value.Round(0).UTC()
	return &canonical
}

func itemTimeV1(item ItemV1) time.Time {
	if item.PublishedAt != nil {
		return *item.PublishedAt
	}
	return item.DiscoveredAt
}
