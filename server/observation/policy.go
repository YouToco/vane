// Package observation defines the approved, versioned freshness contract for
// compiled monitoring tasks. Agents may propose this contract; deterministic
// code validates it and owns the final admission decision.
package observation

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/YouToco/vane/server/internal/strictjson"
	"github.com/google/uuid"
	"github.com/robfig/cron"
)

const (
	SchemaV1          = "vane.observation-policy/v1"
	QualifierPromptV1 = "vane.qualify-events/v1"
)

type Mode string

const (
	ModeContent Mode = "content"
	ModeEvent   Mode = "event"
)

type WindowKind string

const (
	WindowScheduleInterval WindowKind = "schedule_interval"
	WindowRollingDuration  WindowKind = "rolling_duration"
	WindowCalendarPeriod   WindowKind = "calendar_period"
)

type CalendarPeriod string

const (
	CalendarDay   CalendarPeriod = "day"
	CalendarWeek  CalendarPeriod = "week"
	CalendarMonth CalendarPeriod = "month"
)

type LatePolicy string

const (
	LateStrict  LatePolicy = "strict"
	LateBounded LatePolicy = "bounded"
)

type EvidenceRequirement string

const (
	EvidenceOfficialRequired EvidenceRequirement = "official_required"
	EvidenceTrustedAllowed   EvidenceRequirement = "trusted_allowed"
)

type UnknownTimePolicy string

const (
	UnknownTimeReject       UnknownTimePolicy = "reject"
	UnknownTimeDeprioritize UnknownTimePolicy = "deprioritize"
	UnknownTimeAllow        UnknownTimePolicy = "allow"
)

type Qualification string

const (
	QualificationAnnouncement        Qualification = "official_announcement"
	QualificationGeneralAvailability Qualification = "general_availability"
	QualificationEither              Qualification = "either"
)

// PolicySpecV1 is the exact user-confirmed portion of an observation policy.
// EffectiveAt is deliberately absent: callers inject that trusted boundary
// from the durable creation operation.
type PolicySpecV1 struct {
	Schema              string            `json:"schema"`
	Mode                Mode              `json:"mode"`
	Window              WindowSpecV1      `json:"window"`
	LatePolicy          LatePolicy        `json:"late_policy"`
	AllowedLatenessSecs int64             `json:"allowed_lateness_seconds,omitempty"`
	Evidence            EvidencePolicyV1  `json:"evidence"`
	UnknownTime         UnknownTimePolicy `json:"unknown_time"`
	Event               *EventPolicyV1    `json:"event,omitempty"`
	QualifierPrompt     string            `json:"qualifier_prompt,omitempty"`
}

type WindowSpecV1 struct {
	Kind                   WindowKind     `json:"kind"`
	RollingDurationSeconds int64          `json:"rolling_duration_seconds,omitempty"`
	CalendarPeriod         CalendarPeriod `json:"calendar_period,omitempty"`
}

type EvidencePolicyV1 struct {
	Requirement     EvidenceRequirement `json:"requirement"`
	OfficialDomains []string            `json:"official_domains,omitempty"`
}

type EventPolicyV1 struct {
	Subject       string        `json:"subject"`
	EventKind     string        `json:"event_kind"`
	Qualification Qualification `json:"qualification"`
}

// PolicyV1 is the immutable runtime form stored inside ScopeJSON.
type PolicyV1 struct {
	PolicySpecV1
	EffectiveAt time.Time `json:"effective_at"`
}

type policyWireV1 struct {
	Schema              string            `json:"schema"`
	Mode                Mode              `json:"mode"`
	Window              WindowSpecV1      `json:"window"`
	LatePolicy          LatePolicy        `json:"late_policy"`
	AllowedLatenessSecs int64             `json:"allowed_lateness_seconds,omitempty"`
	Evidence            EvidencePolicyV1  `json:"evidence"`
	UnknownTime         UnknownTimePolicy `json:"unknown_time"`
	Event               *EventPolicyV1    `json:"event,omitempty"`
	QualifierPrompt     string            `json:"qualifier_prompt,omitempty"`
	EffectiveAt         time.Time         `json:"effective_at"`
}

func Compile(spec PolicySpecV1, effectiveAt time.Time) (PolicyV1, error) {
	if err := spec.Validate(); err != nil {
		return PolicyV1{}, err
	}
	if effectiveAt.IsZero() {
		return PolicyV1{}, errors.New("observation: effective_at is required")
	}
	return PolicyV1{
		PolicySpecV1: spec,
		EffectiveAt:  effectiveAt.UTC().Truncate(time.Second),
	}, nil
}

// DecodePolicyV1Exact decodes the flattened PolicyV1 JSON wire without
// weakening exact-key checks for its embedded PolicySpecV1 fields.
func DecodePolicyV1Exact(raw []byte) (PolicyV1, error) {
	var wire policyWireV1
	if err := strictjson.DecodeExact(raw, &wire); err != nil {
		return PolicyV1{}, err
	}
	policy := PolicyV1{
		PolicySpecV1: PolicySpecV1{
			Schema:              wire.Schema,
			Mode:                wire.Mode,
			Window:              wire.Window,
			LatePolicy:          wire.LatePolicy,
			AllowedLatenessSecs: wire.AllowedLatenessSecs,
			Evidence:            wire.Evidence,
			UnknownTime:         wire.UnknownTime,
			Event:               wire.Event,
			QualifierPrompt:     wire.QualifierPrompt,
		},
		EffectiveAt: wire.EffectiveAt,
	}
	if err := policy.Validate(); err != nil {
		return PolicyV1{}, err
	}
	if policy.EffectiveAt.IsZero() {
		return PolicyV1{}, errors.New("observation: effective_at is required")
	}
	return policy, nil
}

func (p PolicySpecV1) Validate() error {
	if p.Schema != SchemaV1 {
		return fmt.Errorf("observation: unsupported schema %q", p.Schema)
	}
	if p.Mode != ModeContent && p.Mode != ModeEvent {
		return fmt.Errorf("observation: invalid mode %q", p.Mode)
	}
	switch p.Window.Kind {
	case WindowScheduleInterval:
		if p.Window.RollingDurationSeconds != 0 || p.Window.CalendarPeriod != "" {
			return errors.New("observation: schedule interval has extraneous fields")
		}
	case WindowRollingDuration:
		if p.Window.RollingDurationSeconds < 3600 ||
			p.Window.RollingDurationSeconds > int64((366*24*time.Hour)/time.Second) ||
			p.Window.CalendarPeriod != "" {
			return errors.New("observation: rolling duration is outside 1h..366d")
		}
	case WindowCalendarPeriod:
		if p.Window.RollingDurationSeconds != 0 ||
			(p.Window.CalendarPeriod != CalendarDay &&
				p.Window.CalendarPeriod != CalendarWeek &&
				p.Window.CalendarPeriod != CalendarMonth) {
			return errors.New("observation: invalid calendar period")
		}
	default:
		return fmt.Errorf("observation: invalid window kind %q", p.Window.Kind)
	}
	if p.LatePolicy != LateStrict && p.LatePolicy != LateBounded {
		return fmt.Errorf("observation: invalid late policy %q", p.LatePolicy)
	}
	if p.LatePolicy == LateStrict && p.AllowedLatenessSecs != 0 {
		return errors.New("observation: strict late policy cannot allow lateness")
	}
	if p.LatePolicy == LateBounded &&
		(p.AllowedLatenessSecs <= 0 || p.AllowedLatenessSecs > int64((30*24*time.Hour)/time.Second)) {
		return errors.New("observation: bounded lateness is outside 1s..30d")
	}
	if p.Evidence.Requirement != EvidenceOfficialRequired &&
		p.Evidence.Requirement != EvidenceTrustedAllowed {
		return fmt.Errorf("observation: invalid evidence requirement %q", p.Evidence.Requirement)
	}
	seen := make(map[string]struct{}, len(p.Evidence.OfficialDomains))
	for _, domain := range p.Evidence.OfficialDomains {
		if !validBareDomain(domain) {
			return fmt.Errorf("observation: invalid official domain %q", domain)
		}
		if _, duplicate := seen[domain]; duplicate {
			return fmt.Errorf("observation: duplicate official domain %q", domain)
		}
		seen[domain] = struct{}{}
	}
	if p.Evidence.Requirement == EvidenceOfficialRequired &&
		len(p.Evidence.OfficialDomains) == 0 {
		return errors.New("observation: official evidence requires domains")
	}
	if p.UnknownTime != UnknownTimeReject &&
		p.UnknownTime != UnknownTimeDeprioritize &&
		p.UnknownTime != UnknownTimeAllow {
		return fmt.Errorf("observation: invalid unknown-time policy %q", p.UnknownTime)
	}
	if p.Mode == ModeEvent {
		if p.Event == nil || strings.TrimSpace(p.Event.Subject) == "" ||
			strings.TrimSpace(p.Event.Subject) != p.Event.Subject ||
			strings.TrimSpace(p.Event.EventKind) == "" ||
			strings.TrimSpace(p.Event.EventKind) != p.Event.EventKind ||
			utf8.RuneCountInString(p.Event.Subject) > 160 ||
			utf8.RuneCountInString(p.Event.EventKind) > 80 {
			return errors.New("observation: event subject or kind is invalid")
		}
		if p.Event.Qualification != QualificationAnnouncement &&
			p.Event.Qualification != QualificationGeneralAvailability &&
			p.Event.Qualification != QualificationEither {
			return fmt.Errorf("observation: invalid event qualification %q",
				p.Event.Qualification)
		}
		if p.QualifierPrompt != QualifierPromptV1 {
			return errors.New("observation: event mode requires the frozen qualifier prompt")
		}
	} else if p.Event != nil || p.QualifierPrompt != "" {
		return errors.New("observation: content mode cannot carry event fields")
	}
	return nil
}

func validBareDomain(domain string) bool {
	if domain == "" || domain != strings.ToLower(domain) ||
		strings.TrimSpace(domain) != domain ||
		strings.ContainsAny(domain, "/:*") || net.ParseIP(domain) != nil ||
		len(domain) > 253 || !strings.Contains(domain, ".") {
		return false
	}
	for _, label := range strings.Split(domain, ".") {
		if label == "" || len(label) > 63 ||
			strings.HasPrefix(label, "-") || strings.HasSuffix(label, "-") {
			return false
		}
		for _, r := range label {
			if (r < 'a' || r > 'z') && (r < '0' || r > '9') && r != '-' {
				return false
			}
		}
	}
	return true
}

// Schedule is the small timing surface needed to derive adjacent nominal
// triggers without importing scheduler (which already imports workflow).
type Schedule struct {
	Cron         string
	EverySeconds int
	AnchorAt     string
	TimeZone     string
}

type Window struct {
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
}

type QualifiedEvent struct {
	PolicyDigest string          `json:"policy_digest"`
	EventKey     string          `json:"event_key"`
	EventType    string          `json:"event_type"`
	Subject      string          `json:"subject"`
	OccurredAt   time.Time       `json:"occurred_at"`
	EvidenceJSON json.RawMessage `json:"evidence_json"`
}

func (w Window) Contains(t time.Time) bool {
	return t.After(w.Start) && (t.Before(w.End) || t.Equal(w.End))
}

func PolicyDigest(policy PolicyV1) (string, error) {
	raw, err := json.Marshal(policy)
	if err != nil {
		return "", fmt.Errorf("observation: marshal policy digest: %w", err)
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

// NominalTrigger parses the exact UTC-second suffix carried by recurring and
// explicit manual workflow identities. Bare retained Action IDs and legacy
// manual IDs remain valid for runtime authorization, but do not carry a
// trustworthy observation window and therefore fail closed here.
func NominalTrigger(taskID, workflowID string) (time.Time, error) {
	const (
		layout       = "2006-01-02T15:04:05Z"
		manualPrefix = "wf-manual-"
		uuidLength   = 36
	)
	if strings.HasPrefix(workflowID, manualPrefix) {
		raw := strings.TrimPrefix(workflowID, manualPrefix)
		if len(raw) != uuidLength+1+len(layout) || raw[uuidLength] != '-' {
			return time.Time{}, errors.New(
				"observation: manual workflow ID has no nominal trigger")
		}
		commandID := raw[:uuidLength]
		parsedID, err := uuid.Parse(commandID)
		if err != nil || parsedID.String() != commandID {
			return time.Time{}, errors.New(
				"observation: manual workflow ID is invalid")
		}
		return parseNominalTrigger(raw[uuidLength+1:], layout)
	}
	base := "wf-" + taskID + "-"
	if !strings.HasPrefix(workflowID, base) {
		return time.Time{}, errors.New("observation: workflow ID has no nominal trigger")
	}
	raw := strings.TrimPrefix(workflowID, base)
	return parseNominalTrigger(raw, layout)
}

func parseNominalTrigger(raw, layout string) (time.Time, error) {
	if len(raw) != len(layout) {
		return time.Time{}, errors.New("observation: nominal trigger has invalid length")
	}
	parsed, err := time.Parse(layout, raw)
	if err != nil || parsed.UTC().Format(layout) != raw {
		return time.Time{}, errors.New("observation: nominal trigger is invalid")
	}
	return parsed.UTC(), nil
}

func OfficialURLAllowed(rawURL string, domains []string) bool {
	parsed, err := url.Parse(rawURL)
	if err != nil || parsed.Scheme != "https" || parsed.User != nil ||
		parsed.Hostname() == "" || parsed.Port() != "" {
		return false
	}
	host := strings.TrimSuffix(strings.ToLower(parsed.Hostname()), ".")
	for _, domain := range domains {
		if host == domain || strings.HasSuffix(host, "."+domain) {
			return true
		}
	}
	return false
}

// WindowForNominal returns the deterministic (start, end] admission window.
// nominal is the planned trigger encoded in the Temporal Workflow ID, never
// the worker's wall clock.
func WindowForNominal(policy PolicyV1, schedule Schedule, nominal time.Time) (Window, error) {
	if err := policy.Validate(); err != nil {
		return Window{}, err
	}
	if nominal.IsZero() {
		return Window{}, errors.New("observation: nominal trigger is required")
	}
	nominal = nominal.UTC().Truncate(time.Second)
	var start time.Time
	switch policy.Window.Kind {
	case WindowScheduleInterval:
		previous, err := previousNominal(schedule, nominal)
		if err != nil {
			return Window{}, err
		}
		start = previous
	case WindowRollingDuration:
		start = nominal.Add(-time.Duration(policy.Window.RollingDurationSeconds) * time.Second)
	case WindowCalendarPeriod:
		location, err := time.LoadLocation(schedule.TimeZone)
		if err != nil {
			return Window{}, fmt.Errorf("observation: load time zone: %w", err)
		}
		local := nominal.In(location)
		switch policy.Window.CalendarPeriod {
		case CalendarDay:
			start = time.Date(local.Year(), local.Month(), local.Day(), 0, 0, 0, 0, location)
		case CalendarWeek:
			day := time.Date(local.Year(), local.Month(), local.Day(), 0, 0, 0, 0, location)
			daysSinceMonday := (int(local.Weekday()) + 6) % 7
			start = day.AddDate(0, 0, -daysSinceMonday)
		case CalendarMonth:
			start = time.Date(local.Year(), local.Month(), 1, 0, 0, 0, 0, location)
		}
		start = start.UTC()
	}
	if policy.EffectiveAt.After(start) {
		start = policy.EffectiveAt.UTC()
	}
	if !start.Before(nominal) {
		return Window{}, errors.New("observation: effective policy leaves an empty window")
	}
	return Window{Start: start, End: nominal}, nil
}

func previousNominal(schedule Schedule, nominal time.Time) (time.Time, error) {
	if schedule.EverySeconds > 0 {
		interval := int64(schedule.EverySeconds)
		anchor := time.Unix(0, 0).UTC()
		if schedule.AnchorAt != "" {
			parsed, err := time.Parse(time.RFC3339, schedule.AnchorAt)
			if err != nil {
				return time.Time{}, fmt.Errorf("observation: parse schedule anchor: %w", err)
			}
			anchor = parsed.UTC()
		}
		delta := nominal.Unix() - anchor.Unix()
		steps := delta / interval
		current := anchor.Add(time.Duration(steps*interval) * time.Second)
		if current.Equal(nominal) {
			return current.Add(-time.Duration(interval) * time.Second), nil
		}
		if current.After(nominal) {
			current = current.Add(-time.Duration(interval) * time.Second)
		}
		return current, nil
	}
	location, err := time.LoadLocation(schedule.TimeZone)
	if err != nil {
		return time.Time{}, fmt.Errorf("observation: load schedule time zone: %w", err)
	}
	cronExpression, err := canonicalCronForPredecessor(schedule.Cron)
	if err != nil {
		return time.Time{}, err
	}
	parsed, err := cron.ParseStandard(cronExpression)
	if err != nil {
		return time.Time{}, fmt.Errorf("observation: parse schedule cron: %w", err)
	}
	spec, ok := parsed.(*cron.SpecSchedule)
	if !ok {
		return time.Time{}, errors.New("observation: cron did not compile to a calendar schedule")
	}
	// Temporal ScheduleCalendarSpec requires every calendar field to match.
	// robfig's Next deliberately implements Unix cron's DOM/DOW OR rule, so it
	// cannot be used here once both fields are restricted. Walk absolute
	// minutes backwards and match the exact bitsets compiled by the scheduler.
	cursor := nominal.UTC().Add(-time.Minute).Truncate(time.Minute)
	earliest := nominal.AddDate(-2, 0, 0)
	for !cursor.Before(earliest) {
		local := cursor.In(location)
		if cronBitMatches(spec.Minute, local.Minute()) &&
			cronBitMatches(spec.Hour, local.Hour()) &&
			cronBitMatches(spec.Dom, local.Day()) &&
			cronBitMatches(spec.Month, int(local.Month())) &&
			cronBitMatches(spec.Dow, int(local.Weekday())) {
			return cursor.UTC(), nil
		}
		cursor = cursor.Add(-time.Minute)
	}
	return time.Time{}, errors.New("observation: previous cron trigger is unavailable")
}

func cronBitMatches(bits uint64, value int) bool {
	return bits&(uint64(1)<<value) != 0
}

// canonicalCronForPredecessor retains the same five-field calendar surface as
// the scheduler. robfig/cron does not accept the standard Sunday alias 7 (nor
// full month names), while Temporal's calendar compiler does. Convert only
// those aliases and leave all other field semantics to robfig's parser.
func canonicalCronForPredecessor(expression string) (string, error) {
	fields := strings.Fields(expression)
	if len(fields) != 5 {
		return "", errors.New("observation: cron must contain five fields")
	}
	dayBits, err := parseCronDayOfWeek(fields[4])
	if err != nil {
		return "", fmt.Errorf("observation: parse cron day-of-week: %w", err)
	}
	days := make([]string, 0, 7)
	for day := 0; day <= 6; day++ {
		if dayBits&(uint64(1)<<day) != 0 {
			days = append(days, strconv.Itoa(day))
		}
	}
	fields[3] = strings.NewReplacer(
		"january", "jan", "february", "feb", "march", "mar", "april", "apr",
		"june", "jun", "july", "jul", "august", "aug", "september", "sep",
		"october", "oct", "november", "nov", "december", "dec",
	).Replace(strings.ToLower(fields[3]))
	fields[4] = strings.Join(days, ",")
	return strings.Join(fields, " "), nil
}

func parseCronDayOfWeek(field string) (uint64, error) {
	field = strings.TrimSpace(strings.ToLower(field))
	if field == "" {
		return 0, errors.New("field is empty")
	}
	aliases := map[string]int{
		"sun": 0, "mon": 1, "tue": 2, "wed": 3,
		"thu": 4, "fri": 5, "sat": 6,
		"sunday": 0, "monday": 1, "tuesday": 2, "wednesday": 3,
		"thursday": 4, "friday": 5, "saturday": 6,
	}
	parseValue := func(value string) (int, error) {
		if alias, ok := aliases[value]; ok {
			return alias, nil
		}
		parsed, err := strconv.Atoi(value)
		if err != nil {
			return 0, fmt.Errorf("invalid value %q", value)
		}
		if parsed < 0 || parsed > 7 {
			return 0, fmt.Errorf("value %d is outside 0..7", parsed)
		}
		return parsed, nil
	}
	var bits uint64
	for token := range strings.SplitSeq(field, ",") {
		if token == "" {
			return 0, errors.New("empty list item")
		}
		parts := strings.Split(token, "/")
		if len(parts) > 2 {
			return 0, fmt.Errorf("invalid step expression %q", token)
		}
		step := 1
		if len(parts) == 2 {
			parsedStep, err := strconv.Atoi(parts[1])
			if err != nil || parsedStep <= 0 {
				return 0, fmt.Errorf("invalid step in %q", token)
			}
			step = parsedStep
		}
		start, end := 0, 6
		switch base := parts[0]; {
		case base == "*" || base == "?":
		case strings.Contains(base, "-"):
			rangeParts := strings.Split(base, "-")
			if len(rangeParts) != 2 {
				return 0, fmt.Errorf("invalid range %q", base)
			}
			var err error
			start, err = parseValue(rangeParts[0])
			if err != nil {
				return 0, err
			}
			end, err = parseValue(rangeParts[1])
			if err != nil {
				return 0, err
			}
			if start > end {
				return 0, fmt.Errorf("range %q descends", base)
			}
		default:
			value, err := parseValue(base)
			if err != nil {
				return 0, err
			}
			start, end = value, value
			if len(parts) == 2 && value < 7 {
				end = 6
			}
		}
		for value := start; ; value += step {
			bits |= uint64(1) << (value % 7)
			if step > end-value {
				break
			}
		}
	}
	if bits == 0 {
		return 0, errors.New("field selects no days")
	}
	return bits, nil
}
