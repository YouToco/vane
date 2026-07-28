package cardgen

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"unicode/utf8"
)

const (
	StructuredInsightSchemaV1  = "vane.cardgen-insight/v1"
	maxStructuredResponseBytes = 64 << 10
	maxStructuredFieldBytes    = 4096
	maxStructuredBodyBytes     = 16 << 10
	maxStructuredClaims        = 16
	maxStructuredRefs          = 8
)

// StructuredClaimV1 binds one displayable factual claim to an exact excerpt
// from one of the opaque source references supplied in the same request.
type StructuredClaimV1 struct {
	Text       string   `json:"text"`
	Excerpt    string   `json:"excerpt"`
	SourceRefs []string `json:"source_refs"`
}

// StructuredInsightV1 is the strict CardGen v2 envelope. BodyMD is the
// presentation-safe fallback when the optional structured projection is
// incomplete; inventory-owned title/URL/time fields never enter this schema.
type StructuredInsightV1 struct {
	SchemaVersion    string              `json:"schema_version"`
	BodyMD           string              `json:"body_md"`
	WhatChanged      string              `json:"what_changed"`
	WhyItMatters     string              `json:"why_it_matters"`
	ImportanceReason string              `json:"importance_reason"`
	Claims           []StructuredClaimV1 `json:"claims"`
}

// ParseStructuredInsightV1 validates one model response against the exact
// request-owned opaque source IDs and normalized source text. It accepts no
// trailing JSON, unknown fields, duplicate object keys, forged references or
// excerpts that cannot be found in a cited source.
func ParseStructuredInsightV1(raw []byte, sources map[string]string) (StructuredInsightV1, error) {
	if len(raw) == 0 || len(raw) > maxStructuredResponseBytes || !utf8.Valid(raw) {
		return StructuredInsightV1{}, errors.New("structured insight response is invalid")
	}
	if err := rejectDuplicateJSONKeys(raw); err != nil {
		return StructuredInsightV1{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var insight StructuredInsightV1
	if err := decoder.Decode(&insight); err != nil {
		return StructuredInsightV1{}, fmt.Errorf("decode structured insight: %w", err)
	}
	if err := requireJSONEOF(decoder); err != nil {
		return StructuredInsightV1{}, err
	}
	if insight.SchemaVersion != StructuredInsightSchemaV1 ||
		!validStructuredText(insight.BodyMD, maxStructuredBodyBytes, false) {
		return StructuredInsightV1{}, errors.New("structured insight envelope is invalid")
	}
	structured := []string{insight.WhatChanged, insight.WhyItMatters, insight.ImportanceReason}
	present := 0
	for _, value := range structured {
		if value != "" {
			present++
		}
		if !validStructuredText(value, maxStructuredFieldBytes, true) {
			return StructuredInsightV1{}, errors.New("structured insight field is invalid")
		}
	}
	if present != 0 && present != len(structured) {
		return StructuredInsightV1{}, errors.New("structured insight projection is incomplete")
	}
	if len(insight.Claims) > maxStructuredClaims || (len(insight.Claims) > 0 && present == 0) {
		return StructuredInsightV1{}, errors.New("structured insight claims are invalid")
	}
	for _, claim := range insight.Claims {
		if !validStructuredText(claim.Text, maxStructuredFieldBytes, false) ||
			!validStructuredText(claim.Excerpt, maxStructuredFieldBytes, false) ||
			len(claim.SourceRefs) == 0 || len(claim.SourceRefs) > maxStructuredRefs {
			return StructuredInsightV1{}, errors.New("structured insight claim is invalid")
		}
		matched := false
		seen := make(map[string]struct{}, len(claim.SourceRefs))
		for _, ref := range claim.SourceRefs {
			source, ok := sources[ref]
			if !ok || !validStructuredText(ref, 255, false) {
				return StructuredInsightV1{}, errors.New("structured insight source reference is invalid")
			}
			if _, exists := seen[ref]; exists {
				return StructuredInsightV1{}, errors.New("structured insight source reference is duplicated")
			}
			seen[ref] = struct{}{}
			if strings.Contains(normalizeEvidenceText(source), normalizeEvidenceText(claim.Excerpt)) {
				matched = true
			}
		}
		if !matched {
			return StructuredInsightV1{}, errors.New("structured insight excerpt is not cited")
		}
	}
	return insight, nil
}

func validStructuredText(value string, maxBytes int, allowEmpty bool) bool {
	return (allowEmpty || value != "") && len(value) <= maxBytes &&
		utf8.ValidString(value) && strings.TrimSpace(value) == value
}

func normalizeEvidenceText(value string) string {
	return strings.Join(strings.Fields(value), " ")
}

func requireJSONEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("structured insight has trailing JSON")
		}
		return fmt.Errorf("decode structured insight trailer: %w", err)
	}
	return nil
}

func rejectDuplicateJSONKeys(raw []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	var walk func() error
	walk = func() error {
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		delim, ok := token.(json.Delim)
		if !ok {
			return nil
		}
		switch delim {
		case '{':
			seen := map[string]struct{}{}
			for decoder.More() {
				keyToken, err := decoder.Token()
				if err != nil {
					return err
				}
				key, ok := keyToken.(string)
				if !ok {
					return errors.New("structured insight object key is invalid")
				}
				if _, exists := seen[key]; exists {
					return errors.New("structured insight object key is duplicated")
				}
				seen[key] = struct{}{}
				if err := walk(); err != nil {
					return err
				}
			}
			_, err = decoder.Token()
			return err
		case '[':
			for decoder.More() {
				if err := walk(); err != nil {
					return err
				}
			}
			_, err = decoder.Token()
			return err
		default:
			return errors.New("structured insight JSON delimiter is invalid")
		}
	}
	return walk()
}
