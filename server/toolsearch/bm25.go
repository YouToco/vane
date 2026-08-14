// Package toolsearch provides Vane's native, deterministic lexical retrieval
// core for deferred tools. It has no dependency on Codex or any external
// search service.
package toolsearch

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"unicode"
)

const (
	bm25K1 = 1.2
	bm25B  = 0.75
)

// Document is one already-authorized catalog entry. ID must be the stable,
// canonical tool name. Text is assembled by the caller from reviewed tool
// metadata; credentials and runtime results must never be included.
type Document struct {
	ID   string
	Text string
}

// Hit is one lexical retrieval result.
type Hit struct {
	ID    string
	Score float64
}

type posting struct {
	doc int
	tf  int
}

// Index is immutable after construction and therefore safe for concurrent
// searches. Catalog filtering must happen before New is called so unauthorized
// tool names cannot affect scores or leak through results.
type Index struct {
	ids      []string
	postings map[string][]posting
	docLen   []int
	avgLen   float64
}

// New builds an in-memory BM25 index. Documents are defensively copied into
// immutable index state. Empty or duplicate IDs and empty metadata fail closed.
func New(documents []Document) (*Index, error) {
	idx := &Index{
		ids:      make([]string, len(documents)),
		postings: make(map[string][]posting),
		docLen:   make([]int, len(documents)),
	}
	seen := make(map[string]struct{}, len(documents))
	total := 0
	for i, document := range documents {
		if document.ID == "" || strings.TrimSpace(document.ID) != document.ID {
			return nil, fmt.Errorf("toolsearch: invalid document ID at index %d", i)
		}
		if _, exists := seen[document.ID]; exists {
			return nil, fmt.Errorf("toolsearch: duplicate document ID %q", document.ID)
		}
		if strings.TrimSpace(document.Text) == "" {
			return nil, fmt.Errorf("toolsearch: empty metadata for %q", document.ID)
		}
		seen[document.ID] = struct{}{}
		idx.ids[i] = document.ID

		termFrequency := make(map[string]int)
		for _, token := range Tokenize(document.Text) {
			termFrequency[token]++
			idx.docLen[i]++
		}
		if idx.docLen[i] == 0 {
			return nil, fmt.Errorf("toolsearch: metadata for %q has no searchable terms", document.ID)
		}
		total += idx.docLen[i]
		for token, frequency := range termFrequency {
			idx.postings[token] = append(idx.postings[token], posting{
				doc: i,
				tf:  frequency,
			})
		}
	}
	if len(documents) > 0 {
		idx.avgLen = float64(total) / float64(len(documents))
	}
	return idx, nil
}

// Search returns at most limit results ordered by descending BM25 score, then
// canonical ID. Repeated query tokens do not receive extra weight. A non-
// positive limit, an empty query, or a zero-match query returns nil.
func (idx *Index) Search(query string, limit int) []Hit {
	if idx == nil || limit <= 0 || len(idx.ids) == 0 {
		return nil
	}
	queryTokens := Tokenize(query)
	if len(queryTokens) == 0 {
		return nil
	}

	seen := make(map[string]struct{}, len(queryTokens))
	scores := make(map[int]float64)
	documentCount := float64(len(idx.ids))
	for _, token := range queryTokens {
		if _, duplicate := seen[token]; duplicate {
			continue
		}
		seen[token] = struct{}{}
		postings := idx.postings[token]
		if len(postings) == 0 {
			continue
		}
		idf := math.Log(1 + (documentCount-float64(len(postings))+0.5)/
			(float64(len(postings))+0.5))
		for _, posting := range postings {
			documentLength := float64(idx.docLen[posting.doc])
			termFrequency := float64(posting.tf)
			scores[posting.doc] += idf * termFrequency * (bm25K1 + 1) /
				(termFrequency + bm25K1*(1-bm25B+bm25B*documentLength/idx.avgLen))
		}
	}
	if len(scores) == 0 {
		return nil
	}

	hits := make([]Hit, 0, len(scores))
	for document, score := range scores {
		hits = append(hits, Hit{ID: idx.ids[document], Score: score})
	}
	sort.Slice(hits, func(i, j int) bool {
		if hits[i].Score != hits[j].Score {
			return hits[i].Score > hits[j].Score
		}
		return hits[i].ID < hits[j].ID
	})
	if len(hits) > limit {
		hits = hits[:limit]
	}
	return hits
}

// Tokenize is the stable bilingual tokenizer shared by Vane tool catalogs.
// ASCII letter/digit runs become lowercase terms; contiguous Han text becomes
// overlapping bigrams, with a single Han rune retained as one term.
func Tokenize(text string) []string {
	var tokens []string
	runes := []rune(strings.ToLower(text))
	for i := 0; i < len(runes); {
		r := runes[i]
		switch {
		case isASCIIAlphaNumeric(r):
			j := i
			for j < len(runes) && isASCIIAlphaNumeric(runes[j]) {
				j++
			}
			tokens = append(tokens, string(runes[i:j]))
			i = j
		case unicode.Is(unicode.Han, r):
			j := i
			for j < len(runes) && unicode.Is(unicode.Han, runes[j]) {
				j++
			}
			if j-i == 1 {
				tokens = append(tokens, string(runes[i]))
			}
			for k := i; k+1 < j; k++ {
				tokens = append(tokens, string(runes[k:k+2]))
			}
			i = j
		default:
			i++
		}
	}
	return tokens
}

func isASCIIAlphaNumeric(r rune) bool {
	return r >= 'a' && r <= 'z' || r >= '0' && r <= '9'
}
