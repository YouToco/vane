package toolsearch

import (
	"encoding/json"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
)

func catalogFixture() []Entry {
	return []Entry{
		{
			Namespace: "social/x", Name: "read_creator_posts",
			Description: "Read recent creator posts from a public account",
			Parameters: json.RawMessage(`{
			  "type":"object",
			  "properties":{
			    "creator_name":{"type":"string","description":"博主名称或公开账号"},
			    "window":{"anyOf":[
			      {"type":"string","enum":["yesterday","last_7_days"]},
			      {"type":"object","properties":{"since":{"type":"string","description":"历史对比起点"}}}
			    ]}
			  }
			}`),
			Aliases: []string{"博主动态", "creator feed"},
			Tags:    []string{"history", "public-read"},
		},
		{
			Namespace: "commerce/product", Name: "read_product_status",
			Description: "Read current official purchase availability",
			Parameters:  json.RawMessage(`{"properties":{"product":{"description":"套餐名称","type":"string"},"region":{"items":{"enum":["CN","US"],"type":"string"},"type":"array"}},"type":"object"}`),
			Aliases:     []string{"购买状态"}, Tags: []string{"official", "public-read"},
		},
	}
}

func TestCatalogSearchesFullMetadataAndReturnsSchema(t *testing.T) {
	t.Parallel()
	catalog, err := NewCatalog(catalogFixture())
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		query string
		want  string
	}{
		{query: "博主昨天发了什么", want: "read_creator_posts"},
		{query: "历史对比起点", want: "read_creator_posts"},
		{query: "last_7_days", want: "read_creator_posts"},
		{query: "套餐是否可以购买", want: "read_product_status"},
		{query: "commerce official", want: "read_product_status"},
		{query: "region CN", want: "read_product_status"},
	} {
		test := test
		t.Run(test.query, func(t *testing.T) {
			t.Parallel()
			matches, searchErr := catalog.Search(test.query, 2)
			if searchErr != nil {
				t.Fatal(searchErr)
			}
			if len(matches) == 0 || matches[0].Entry.Name != test.want {
				t.Fatalf("Search(%q) = %#v, want %s first", test.query, matches, test.want)
			}
			var schema map[string]any
			if err := json.Unmarshal(matches[0].Entry.Parameters, &schema); err != nil || schema["type"] != "object" {
				t.Fatalf("returned schema = %s, err=%v", matches[0].Entry.Parameters, err)
			}
		})
	}
}

func TestCatalogDigestIsCanonical(t *testing.T) {
	t.Parallel()
	leftEntries := catalogFixture()
	rightEntries := []Entry{leftEntries[1], leftEntries[0]}
	rightEntries[0].Parameters = json.RawMessage(`{"type":"object","properties":{"region":{"type":"array","items":{"type":"string","enum":["CN","US"]}},"product":{"type":"string","description":"套餐名称"}}}`)
	rightEntries[0].Aliases = []string{"购买状态"}
	rightEntries[0].Tags = []string{"public-read", "official"}
	rightEntries[1].Aliases = []string{"creator feed", "博主动态"}
	rightEntries[1].Tags = []string{"public-read", "history"}
	left, err := NewCatalog(leftEntries)
	if err != nil {
		t.Fatal(err)
	}
	right, err := NewCatalog(rightEntries)
	if err != nil {
		t.Fatal(err)
	}
	if left.Digest() != right.Digest() {
		t.Fatalf("equivalent catalogs have different digests: %s != %s", left.Digest(), right.Digest())
	}
	got, err := left.Search("博主 套餐", 2)
	if err != nil {
		t.Fatal(err)
	}
	want, err := right.Search("博主 套餐", 2)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("equivalent catalogs rank differently: %#v != %#v", got, want)
	}
}

func TestCatalogWithDocumentsBindsStableSearchCorpus(t *testing.T) {
	t.Parallel()
	entries := catalogFixture()
	left, err := NewCatalogWithDocuments(entries, []Document{
		{ID: entries[0].Name, Text: "  legacy   creator ranking  "},
		{ID: entries[1].Name, Text: "legacy product ranking"},
	})
	if err != nil {
		t.Fatal(err)
	}
	right, err := NewCatalogWithDocuments([]Entry{entries[1], entries[0]}, []Document{
		{ID: entries[1].Name, Text: "legacy product ranking"},
		{ID: entries[0].Name, Text: "legacy creator ranking"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if left.Digest() != right.Digest() {
		t.Fatalf("equivalent custom catalogs have different digests: %s != %s", left.Digest(), right.Digest())
	}
	matches, err := left.Search("legacy creator", 2)
	if err != nil || len(matches) != 2 || matches[0].Entry.Name != entries[0].Name {
		t.Fatalf("custom search = %#v, err=%v", matches, err)
	}
	changed, err := NewCatalogWithDocuments(entries, []Document{
		{ID: entries[0].Name, Text: "changed creator ranking"},
		{ID: entries[1].Name, Text: "legacy product ranking"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if changed.Digest() == left.Digest() {
		t.Fatal("search corpus change did not change catalog digest")
	}

	for _, documents := range [][]Document{
		{{ID: entries[0].Name, Text: "missing second"}},
		{{ID: entries[0].Name, Text: "first"}, {ID: entries[0].Name, Text: "duplicate"}},
		{{ID: entries[0].Name, Text: "first"}, {ID: "unknown", Text: "unknown"}},
		{{ID: entries[0].Name, Text: string([]byte{0xff})}, {ID: entries[1].Name, Text: "valid"}},
		{{ID: entries[0].Name, Text: strings.Repeat("x", maxSearchDocBytes+1)}, {ID: entries[1].Name, Text: "valid"}},
	} {
		if _, err := NewCatalogWithDocuments(entries, documents); err == nil {
			t.Fatalf("NewCatalogWithDocuments(%#v) succeeded, want error", documents)
		}
	}
}

func TestCatalogWithDocumentsRejectsOversizeCorpus(t *testing.T) {
	t.Parallel()
	if _, err := NewCatalogWithDocuments(catalogFixture(), make([]Document, maxCatalogEntries+1)); err == nil {
		t.Fatal("oversize document count succeeded, want error")
	}
	documentText := strings.Repeat("x", maxSearchDocBytes)
	documents := make([]Document, maxSearchCorpusBytes/maxSearchDocBytes+1)
	for i := range documents {
		documents[i] = Document{ID: "tool_" + strconv.Itoa(i), Text: documentText}
	}
	if _, err := NewCatalogWithDocuments(catalogFixture(), documents); err == nil {
		t.Fatal("oversize search corpus succeeded, want error")
	}
}

func TestCatalogSearchFilteredPreservesGlobalRanking(t *testing.T) {
	t.Parallel()
	entries := catalogFixture()
	catalog, err := NewCatalogWithDocuments(entries, []Document{
		{ID: entries[0].Name, Text: "shared shared creator"},
		{ID: entries[1].Name, Text: "shared product"},
	})
	if err != nil {
		t.Fatal(err)
	}
	matches, err := catalog.SearchFiltered("shared", 1, func(entry Entry) bool {
		return entry.Name == entries[1].Name
	})
	if err != nil || len(matches) != 1 || matches[0].Entry.Name != entries[1].Name {
		t.Fatalf("filtered search = %#v, err=%v", matches, err)
	}
}

func TestCatalogDefensiveCopies(t *testing.T) {
	t.Parallel()
	entries := catalogFixture()
	catalog, err := NewCatalog(entries)
	if err != nil {
		t.Fatal(err)
	}
	digest := catalog.Digest()
	entries[0].Name = "mutated"
	entries[0].Parameters[0] = '!'
	entries[0].Aliases[0] = "mutated"

	matches, err := catalog.Search("博主动态", 1)
	if err != nil || len(matches) != 1 || matches[0].Entry.Name != "read_creator_posts" {
		t.Fatalf("input mutation changed catalog: matches=%#v err=%v", matches, err)
	}
	matches[0].Entry.Parameters[0] = '!'
	matches[0].Entry.Aliases[0] = "mutated again"
	again, err := catalog.Search("博主动态", 1)
	if err != nil || len(again) != 1 || again[0].Entry.Parameters[0] != '{' || again[0].Entry.Aliases[0] == "mutated again" {
		t.Fatalf("output mutation changed catalog: matches=%#v err=%v", again, err)
	}
	if catalog.Digest() != digest {
		t.Fatalf("catalog digest changed: %s != %s", catalog.Digest(), digest)
	}
}

func TestCatalogRejectsInvalidMetadata(t *testing.T) {
	t.Parallel()
	tooDeep := `{"type":"object","properties":{"x":`
	for i := 0; i < maxSchemaDepth+2; i++ {
		tooDeep += `{"items":`
	}
	tooDeep += `{"type":"string"}` + strings.Repeat("}", maxSchemaDepth+2) + `}}`
	valid := Entry{Namespace: "web", Name: "read_page", Description: "Read page", Parameters: json.RawMessage(`{"type":"object","properties":{}}`)}
	invalidUTF8 := string([]byte{0xff})
	for _, test := range []struct {
		name    string
		entries []Entry
	}{
		{name: "empty catalog"},
		{name: "empty namespace", entries: []Entry{{Name: "read", Description: "read", Parameters: valid.Parameters}}},
		{name: "noncanonical name", entries: []Entry{{Namespace: "web", Name: " read", Description: "read", Parameters: valid.Parameters}}},
		{name: "empty description", entries: []Entry{{Namespace: "web", Name: "read", Parameters: valid.Parameters}}},
		{name: "invalid UTF-8 namespace", entries: []Entry{{Namespace: invalidUTF8, Name: "read", Description: "read", Parameters: valid.Parameters}}},
		{name: "invalid UTF-8 name", entries: []Entry{{Namespace: "web", Name: invalidUTF8, Description: "read", Parameters: valid.Parameters}}},
		{name: "invalid UTF-8 description", entries: []Entry{{Namespace: "web", Name: "read", Description: invalidUTF8, Parameters: valid.Parameters}}},
		{name: "invalid UTF-8 alias", entries: []Entry{{Namespace: "web", Name: "read", Description: "read", Parameters: valid.Parameters, Aliases: []string{invalidUTF8}}}},
		{name: "invalid UTF-8 tag", entries: []Entry{{Namespace: "web", Name: "read", Description: "read", Parameters: valid.Parameters, Tags: []string{invalidUTF8}}}},
		{name: "oversize name", entries: []Entry{{Namespace: "web", Name: strings.Repeat("n", maxToolNameBytes+1), Description: "read", Parameters: valid.Parameters}}},
		{name: "oversize namespace", entries: []Entry{{Namespace: strings.Repeat("n", maxNamespaceBytes+1), Name: "read", Description: "read", Parameters: valid.Parameters}}},
		{name: "oversize description", entries: []Entry{{Namespace: "web", Name: "read", Description: strings.Repeat("d", maxDescriptionBytes+1), Parameters: valid.Parameters}}},
		{name: "too many aliases", entries: []Entry{{Namespace: "web", Name: "read", Description: "read", Parameters: valid.Parameters, Aliases: make([]string, maxLabelsPerKind+1)}}},
		{name: "oversize alias", entries: []Entry{{Namespace: "web", Name: "read", Description: "read", Parameters: valid.Parameters, Aliases: []string{strings.Repeat("a", maxLabelBytes+1)}}}},
		{name: "invalid JSON", entries: []Entry{{Namespace: "web", Name: "read", Description: "read", Parameters: json.RawMessage(`{`)}}},
		{name: "trailing JSON", entries: []Entry{{Namespace: "web", Name: "read", Description: "read", Parameters: json.RawMessage(`{} {}`)}}},
		{name: "wrong root", entries: []Entry{{Namespace: "web", Name: "read", Description: "read", Parameters: json.RawMessage(`{"type":"array"}`)}}},
		{name: "duplicate name", entries: []Entry{valid, valid}},
		{name: "duplicate alias", entries: []Entry{{Namespace: "web", Name: "read", Description: "read", Parameters: valid.Parameters, Aliases: []string{"page", "page"}}}},
		{name: "too deep", entries: []Entry{{Namespace: "web", Name: "read", Description: "read", Parameters: json.RawMessage(tooDeep)}}},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if _, err := NewCatalog(test.entries); err == nil {
				t.Fatalf("NewCatalog(%s) succeeded, want error", test.name)
			}
		})
	}
}

func TestCatalogRejectsHugeMetadataBeforeCustomDocumentProjection(t *testing.T) {
	entry := catalogFixture()[0]
	entry.Description = strings.Repeat("x", 100<<20)
	if _, err := NewCatalogWithDocuments([]Entry{entry}, []Document{{ID: entry.Name, Text: "short valid document"}}); err == nil {
		t.Fatal("100 MiB description bypassed metadata bounds through a short custom document")
	}
	entry = catalogFixture()[0]
	if _, err := NewCatalogWithDocuments([]Entry{entry}, []Document{{
		ID: entry.Name, Text: strings.Repeat(" ", maxSearchDocBytes+1),
	}}); err == nil {
		t.Fatal("oversize raw whitespace document reached normalization")
	}
}

func TestCatalogRejectsOversizeCanonicalMetadataTotal(t *testing.T) {
	t.Parallel()
	description := strings.Repeat("d", maxDescriptionBytes)
	count := maxCatalogMetaBytes/maxDescriptionBytes + 1
	entries := make([]Entry, count)
	for i := range entries {
		entries[i] = Entry{
			Namespace: "test", Name: "tool_" + strconv.Itoa(i), Description: description,
			Parameters: json.RawMessage(`{"type":"object","properties":{}}`),
		}
	}
	if _, err := NewCatalog(entries); err == nil {
		t.Fatal("oversize canonical catalog metadata succeeded")
	}
}

func TestCatalogRejectsInvalidUTF8BeforeDigestWithCustomDocuments(t *testing.T) {
	t.Parallel()
	left, err := json.Marshal(map[string]string{"description": string([]byte{0xff})})
	if err != nil {
		t.Fatal(err)
	}
	right, err := json.Marshal(map[string]string{"description": string([]byte{0xfe})})
	if err != nil {
		t.Fatal(err)
	}
	if string(left) != string(right) {
		t.Fatalf("mutation precondition changed: invalid UTF-8 encodings no longer collide: %s != %s", left, right)
	}
	for _, bad := range []byte{0xff, 0xfe} {
		entry := catalogFixture()[0]
		entry.Description = string([]byte{bad})
		if _, err := NewCatalogWithDocuments([]Entry{entry}, []Document{{ID: entry.Name, Text: "valid search metadata"}}); err == nil {
			t.Fatalf("invalid metadata byte 0x%x reached custom-document digest", bad)
		}
	}
}

func TestCatalogSchemaLimitsCoverOpaqueKeywordTrees(t *testing.T) {
	t.Parallel()
	base := Entry{Namespace: "web", Name: "read", Description: "read", Parameters: json.RawMessage(`{"type":"object"}`)}

	deep := any("leaf")
	for i := 0; i < maxSchemaDepth+1; i++ {
		deep = map[string]any{"nested": deep}
	}
	base.Parameters = mustSchemaJSON(t, map[string]any{
		"type": "object",
		"properties": map[string]any{
			"query": map[string]any{"type": "object", "default": deep},
		},
	})
	if _, err := NewCatalog([]Entry{base}); err == nil {
		t.Fatal("deep default object bypassed schema depth limit")
	}

	many := make([]any, maxSchemaNodes)
	for i := range many {
		many[i] = i
	}
	base.Parameters = mustSchemaJSON(t, map[string]any{
		"type": "object",
		"properties": map[string]any{
			"query": map[string]any{"type": "array", "examples": many},
		},
	})
	if _, err := NewCatalog([]Entry{base}); err == nil {
		t.Fatal("large examples array bypassed schema node limit")
	}
}

func TestCatalogIndexesOnlyScalarsFromNestedSchemaLiterals(t *testing.T) {
	t.Parallel()
	entry := catalogFixture()[0]
	entry.Parameters = mustSchemaJSON(t, map[string]any{
		"type": "object",
		"properties": map[string]any{
			"query": map[string]any{
				"type": "object",
				"default": map[string]any{
					"opaque_key_not_indexed": []any{"deep_default_needle"},
				},
			},
		},
	})
	catalog, err := NewCatalog([]Entry{entry})
	if err != nil {
		t.Fatal(err)
	}
	matches, err := catalog.Search("deep_default_needle", 1)
	if err != nil || len(matches) != 1 {
		t.Fatalf("nested scalar Search() = %#v, err=%v", matches, err)
	}
	matches, err = catalog.Search("opaque_key_not_indexed", 1)
	if err != nil || matches != nil {
		t.Fatalf("nested object key entered search terms: %#v, err=%v", matches, err)
	}
}

func mustSchemaJSON(t *testing.T, value any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestCatalogSearchBounds(t *testing.T) {
	t.Parallel()
	catalog, err := NewCatalog(catalogFixture())
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		query string
		limit int
	}{
		{query: "", limit: 1},
		{query: "posts", limit: 0},
		{query: "posts", limit: maxSearchResults + 1},
		{query: strings.Repeat("a", maxSearchQueryBytes+1), limit: 1},
	} {
		if _, err := catalog.Search(test.query, test.limit); err == nil {
			t.Fatalf("Search(%q, %d) succeeded, want error", test.query, test.limit)
		}
	}
	if matches, err := catalog.Search("not-present", 1); err != nil || matches != nil {
		t.Fatalf("zero match = %#v, err=%v; want nil, nil", matches, err)
	}
}

func TestRegistryPublishesOnlyCompleteCatalogs(t *testing.T) {
	t.Parallel()
	registry, err := NewRegistry(catalogFixture())
	if err != nil {
		t.Fatal(err)
	}
	before := registry.Catalog()
	equivalent := catalogFixture()
	equivalent[0], equivalent[1] = equivalent[1], equivalent[0]
	if err := registry.Rebuild(equivalent); err != nil {
		t.Fatal(err)
	}
	if registry.Catalog() != before {
		t.Fatal("equivalent rebuild published a new catalog pointer")
	}
	if err := registry.Rebuild([]Entry{{Name: "broken"}}); err == nil {
		t.Fatal("invalid rebuild succeeded")
	}
	if registry.Catalog() != before {
		t.Fatal("failed rebuild replaced last known-good catalog")
	}

	next := catalogFixture()
	next = append(next, Entry{
		Namespace: "news", Name: "search_press_releases", Description: "Search official press releases",
		Parameters: json.RawMessage(`{"type":"object","properties":{"query":{"type":"string"}}}`),
	})
	var wait sync.WaitGroup
	for i := 0; i < 16; i++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for j := 0; j < 100; j++ {
				catalog := registry.Catalog()
				if catalog == nil || catalog.Digest() == "" {
					t.Error("reader observed incomplete catalog")
					return
				}
			}
		}()
	}
	if err := registry.Rebuild(next); err != nil {
		t.Fatal(err)
	}
	wait.Wait()
	if registry.Catalog() == before || registry.Catalog().Digest() == before.Digest() {
		t.Fatal("valid rebuild did not publish new catalog")
	}
}

func TestRegistryConcurrentEquivalentRebuildReusesPublishedPointer(t *testing.T) {
	t.Parallel()
	registry, err := NewRegistry(catalogFixture())
	if err != nil {
		t.Fatal(err)
	}
	next := append(catalogFixture(), Entry{
		Namespace: "news", Name: "search_press_releases", Description: "Search official press releases",
		Parameters: json.RawMessage(`{"type":"object","properties":{"query":{"type":"string"}}}`),
	})
	const writers = 16
	var wait sync.WaitGroup
	wait.Add(writers)
	for i := 0; i < writers; i++ {
		go func() {
			defer wait.Done()
			if rebuildErr := registry.Rebuild(next); rebuildErr != nil {
				t.Errorf("Rebuild() error = %v", rebuildErr)
			}
		}()
	}
	wait.Wait()
	published := registry.Catalog()
	if published == nil {
		t.Fatal("concurrent rebuild published nil")
	}
	if err := registry.Rebuild(next); err != nil {
		t.Fatal(err)
	}
	if registry.Catalog() != published {
		t.Fatal("same digest did not reuse concurrently published catalog pointer")
	}
}
