package toolsearch

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"unicode/utf8"
)

const (
	maxCatalogEntries   = 4096
	maxSchemaBytes      = 64 << 10
	maxSchemaDepth      = 16
	maxSchemaNodes      = 4096
	maxSearchQueryBytes = 512
	maxSearchResults    = 8
)

// Entry is one deferred tool after the caller has applied the trusted local
// authorization policy. Catalog never accepts tenant, credential, runtime
// result, or remote authorization data.
type Entry struct {
	Namespace   string
	Name        string
	Description string
	Parameters  json.RawMessage
	Aliases     []string
	Tags        []string
}

// Match carries the complete model-facing definition for an authorized hit.
// All slices are defensive copies and may be mutated by the caller.
type Match struct {
	Score float64
	Entry Entry
}

// Catalog is an immutable metadata index. Callers must filter authorization
// before construction so excluded names cannot leak through BM25 scores.
type Catalog struct {
	digest  string
	index   *Index
	entries map[string]Entry
}

type canonicalEntry struct {
	Namespace   string          `json:"namespace"`
	Name        string          `json:"name"`
	Description string          `json:"description"`
	Parameters  json.RawMessage `json:"parameters"`
	Aliases     []string        `json:"aliases,omitempty"`
	Tags        []string        `json:"tags,omitempty"`
}

// NewCatalog validates and indexes a complete already-authorized snapshot.
// Input order does not affect its digest or search ranking.
func NewCatalog(entries []Entry) (*Catalog, error) {
	if len(entries) == 0 {
		return nil, errors.New("toolsearch: catalog is empty")
	}
	if len(entries) > maxCatalogEntries {
		return nil, fmt.Errorf("toolsearch: catalog has %d entries, maximum is %d", len(entries), maxCatalogEntries)
	}

	canonical := make([]canonicalEntry, 0, len(entries))
	byName := make(map[string]Entry, len(entries))
	termsByName := make(map[string][]string, len(entries))
	for i, raw := range entries {
		entry, schema, schemaTerms, err := normalizeEntry(raw)
		if err != nil {
			return nil, fmt.Errorf("toolsearch: entry %d: %w", i, err)
		}
		if _, duplicate := byName[entry.Name]; duplicate {
			return nil, fmt.Errorf("toolsearch: duplicate tool name %q", entry.Name)
		}
		entry.Parameters = append(json.RawMessage(nil), schema...)
		byName[entry.Name] = cloneEntry(entry)
		termsByName[entry.Name] = append([]string(nil), schemaTerms...)
		canonical = append(canonical, canonicalEntry{
			Namespace: entry.Namespace, Name: entry.Name,
			Description: entry.Description, Parameters: schema,
			Aliases: entry.Aliases, Tags: entry.Tags,
		})
	}
	sort.Slice(canonical, func(i, j int) bool { return canonical[i].Name < canonical[j].Name })

	documents := make([]Document, 0, len(canonical))
	for _, item := range canonical {
		entry := byName[item.Name]
		searchText := strings.Join([]string{
			entry.Namespace,
			entry.Name,
			strings.ReplaceAll(entry.Name, "_", " "),
			entry.Description,
			strings.Join(entry.Aliases, " "),
			strings.Join(entry.Tags, " "),
			strings.Join(termsByName[item.Name], " "),
		}, " ")
		documents = append(documents, Document{ID: entry.Name, Text: searchText})
	}
	index, err := New(documents)
	if err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(canonical)
	if err != nil {
		return nil, fmt.Errorf("toolsearch: encode catalog digest: %w", err)
	}
	sum := sha256.Sum256(encoded)
	return &Catalog{
		digest:  hex.EncodeToString(sum[:]),
		index:   index,
		entries: byName,
	}, nil
}

// Digest identifies the complete normalized catalog snapshot.
func (c *Catalog) Digest() string {
	if c == nil {
		return ""
	}
	return c.digest
}

// Search returns one to eight authorized hits with complete schemas.
func (c *Catalog) Search(query string, limit int) ([]Match, error) {
	if c == nil || c.index == nil {
		return nil, errors.New("toolsearch: catalog is unavailable")
	}
	query = strings.TrimSpace(query)
	if query == "" {
		return nil, errors.New("toolsearch: query is empty")
	}
	if len(query) > maxSearchQueryBytes || !utf8.ValidString(query) {
		return nil, fmt.Errorf("toolsearch: query exceeds %d UTF-8 bytes", maxSearchQueryBytes)
	}
	if limit < 1 || limit > maxSearchResults {
		return nil, fmt.Errorf("toolsearch: limit must be between 1 and %d", maxSearchResults)
	}
	hits := c.index.Search(query, limit)
	if len(hits) == 0 {
		return nil, nil
	}
	matches := make([]Match, 0, len(hits))
	for _, hit := range hits {
		entry, ok := c.entries[hit.ID]
		if !ok {
			return nil, fmt.Errorf("toolsearch: index references unknown tool %q", hit.ID)
		}
		matches = append(matches, Match{Score: hit.Score, Entry: cloneEntry(entry)})
	}
	return matches, nil
}

// Registry atomically publishes complete catalog snapshots. A failed rebuild
// leaves the last known-good catalog active.
type Registry struct{ current atomic.Pointer[Catalog] }

func NewRegistry(entries []Entry) (*Registry, error) {
	catalog, err := NewCatalog(entries)
	if err != nil {
		return nil, err
	}
	registry := &Registry{}
	registry.current.Store(catalog)
	return registry, nil
}

func (r *Registry) Rebuild(entries []Entry) error {
	if r == nil {
		return errors.New("toolsearch: registry is nil")
	}
	catalog, err := NewCatalog(entries)
	if err != nil {
		return err
	}
	r.current.Store(catalog)
	return nil
}

func (r *Registry) Catalog() *Catalog {
	if r == nil {
		return nil
	}
	return r.current.Load()
}

func normalizeEntry(raw Entry) (Entry, json.RawMessage, []string, error) {
	entry := cloneEntry(raw)
	if entry.Name == "" || strings.TrimSpace(entry.Name) != entry.Name {
		return Entry{}, nil, nil, errors.New("tool name is empty or not canonical")
	}
	if entry.Namespace == "" || strings.TrimSpace(entry.Namespace) != entry.Namespace {
		return Entry{}, nil, nil, fmt.Errorf("tool %q namespace is empty or not canonical", entry.Name)
	}
	if entry.Description == "" || strings.TrimSpace(entry.Description) != entry.Description {
		return Entry{}, nil, nil, fmt.Errorf("tool %q description is empty or not canonical", entry.Name)
	}
	if len(entry.Parameters) == 0 || len(entry.Parameters) > maxSchemaBytes || !utf8.Valid(entry.Parameters) {
		return Entry{}, nil, nil, fmt.Errorf("tool %q schema must be 1..%d UTF-8 bytes", entry.Name, maxSchemaBytes)
	}

	var schema any
	decoder := json.NewDecoder(strings.NewReader(string(entry.Parameters)))
	decoder.UseNumber()
	if err := decoder.Decode(&schema); err != nil {
		return Entry{}, nil, nil, fmt.Errorf("tool %q schema is invalid JSON: %w", entry.Name, err)
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return Entry{}, nil, nil, fmt.Errorf("tool %q schema has trailing JSON", entry.Name)
	}
	root, ok := schema.(map[string]any)
	if !ok || root["type"] != "object" {
		return Entry{}, nil, nil, fmt.Errorf("tool %q schema root must be an object type", entry.Name)
	}
	terms := make([]string, 0, 32)
	nodes := 0
	if err := collectSchemaTerms(schema, 0, &nodes, &terms); err != nil {
		return Entry{}, nil, nil, fmt.Errorf("tool %q schema: %w", entry.Name, err)
	}
	canonicalSchema, err := json.Marshal(schema)
	if err != nil {
		return Entry{}, nil, nil, fmt.Errorf("tool %q canonical schema: %w", entry.Name, err)
	}
	entry.Aliases, err = normalizeLabels(entry.Aliases, "alias")
	if err != nil {
		return Entry{}, nil, nil, fmt.Errorf("tool %q: %w", entry.Name, err)
	}
	entry.Tags, err = normalizeLabels(entry.Tags, "tag")
	if err != nil {
		return Entry{}, nil, nil, fmt.Errorf("tool %q: %w", entry.Name, err)
	}
	return entry, canonicalSchema, terms, nil
}

func normalizeLabels(values []string, kind string) ([]string, error) {
	out := append([]string(nil), values...)
	for i, value := range out {
		if value == "" || strings.TrimSpace(value) != value {
			return nil, fmt.Errorf("%s %d is empty or not canonical", kind, i)
		}
	}
	sort.Strings(out)
	for i := 1; i < len(out); i++ {
		if out[i] == out[i-1] {
			return nil, fmt.Errorf("duplicate %s %q", kind, out[i])
		}
	}
	return out, nil
}

func collectSchemaTerms(value any, depth int, nodes *int, terms *[]string) error {
	if depth > maxSchemaDepth {
		return fmt.Errorf("nesting exceeds %d", maxSchemaDepth)
	}
	(*nodes)++
	if *nodes > maxSchemaNodes {
		return fmt.Errorf("node count exceeds %d", maxSchemaNodes)
	}
	switch typed := value.(type) {
	case map[string]any:
		keys := make([]string, 0, len(typed))
		for key := range typed {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			child := typed[key]
			switch key {
			case "title", "description":
				if text, ok := child.(string); ok {
					*terms = append(*terms, text)
				}
			case "enum", "const", "examples", "default":
				appendScalarTerms(child, terms)
			case "properties", "patternProperties", "$defs", "definitions":
				if children, ok := child.(map[string]any); ok {
					childKeys := make([]string, 0, len(children))
					for childKey := range children {
						childKeys = append(childKeys, childKey)
					}
					sort.Strings(childKeys)
					for _, childKey := range childKeys {
						*terms = append(*terms, childKey, strings.ReplaceAll(childKey, "_", " "))
						if err := collectSchemaTerms(children[childKey], depth+1, nodes, terms); err != nil {
							return err
						}
					}
				}
			case "items", "anyOf", "oneOf", "allOf", "additionalProperties":
				if err := collectSchemaTerms(child, depth+1, nodes, terms); err != nil {
					return err
				}
			}
		}
	case []any:
		for _, child := range typed {
			if err := collectSchemaTerms(child, depth+1, nodes, terms); err != nil {
				return err
			}
		}
	}
	return nil
}

func appendScalarTerms(value any, terms *[]string) {
	switch typed := value.(type) {
	case string:
		*terms = append(*terms, typed)
	case json.Number:
		*terms = append(*terms, typed.String())
	case bool:
		*terms = append(*terms, strconv.FormatBool(typed))
	case []any:
		for _, child := range typed {
			appendScalarTerms(child, terms)
		}
	}
}

func cloneEntry(entry Entry) Entry {
	entry.Parameters = append(json.RawMessage(nil), entry.Parameters...)
	entry.Aliases = append([]string(nil), entry.Aliases...)
	entry.Tags = append([]string(nil), entry.Tags...)
	return entry
}
