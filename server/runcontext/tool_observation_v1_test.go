package runcontext

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/YouToco/vane/types"
)

func TestToolObservationSetV1RoundTripsWithoutSourceIdentity(t *testing.T) {
	published := time.Date(2026, 7, 29, 1, 2, 3, 0, time.UTC)
	fetched := published.Add(time.Minute)
	simhash := int64(42)
	items := []types.ContentItem{{
		ID: 91, ExternalID: "post-1",
		CanonicalKey: "https://example.com/post-1",
		Kind:         types.KindArticle, URL: "https://example.com/post-1",
		Title: "Post", Content: "Body", Author: "Author",
		PublishedAt: &published, ContentHash: "content-hash",
		Simhash: &simhash, FetchedAt: fetched,
	}}
	const digest = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	set, raw, sealed, err := BuildToolObservationSetV1(77, digest, items)
	if err != nil {
		t.Fatal(err)
	}
	decoded, got, decodedSeal, err := DecodeToolObservationSetV1(raw)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded, set) || decodedSeal != sealed || len(got) != 1 ||
		got[0].SourceID != 0 || got[0].ID != 91 ||
		got[0].CanonicalKey != items[0].CanonicalKey {
		t.Fatalf("observation round trip drifted: set=%+v items=%+v", decoded, got)
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(raw, &object); err != nil {
		t.Fatal(err)
	}
	if _, leaked := object["source_id"]; leaked {
		t.Fatal("Source identity leaked into Tool observation set")
	}
}

func TestToolObservationSetV1PreservesEmptyCompletion(t *testing.T) {
	const digest = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	_, raw, _, err := BuildToolObservationSetV1(
		77, digest, []types.ContentItem{})
	if err != nil {
		t.Fatal(err)
	}
	_, items, _, err := DecodeToolObservationSetV1(raw)
	if err != nil || items == nil || len(items) != 0 {
		t.Fatalf("empty observation set = %#v err=%v", items, err)
	}
}
