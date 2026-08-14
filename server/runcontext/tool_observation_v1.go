package runcontext

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/YouToco/vane/internal/strictjson"
	"github.com/YouToco/vane/types"
)

const (
	ToolObservationSetSchemaV1 = "vane.task-run-content-observation-set/v1"
	MaxToolObservationItemsV1  = 256
	MaxToolObservationBytesV1  = 8 << 20
)

// ToolObservedContentV1 is the exact content evidence returned by one frozen
// Tool invocation. SourceID is intentionally absent.
type ToolObservedContentV1 struct {
	ContentItemID int64      `json:"content_item_id"`
	ExternalID    string     `json:"external_id"`
	CanonicalKey  string     `json:"canonical_key"`
	Kind          types.Kind `json:"kind"`
	URL           string     `json:"url"`
	Title         string     `json:"title"`
	Content       string     `json:"content"`
	Author        string     `json:"author"`
	PublishedAt   *time.Time `json:"published_at"`
	ContentHash   string     `json:"content_hash"`
	Simhash       *int64     `json:"simhash"`
	FetchedAt     time.Time  `json:"fetched_at"`
}

// ToolObservationSetV1 is one atomic, retry-recoverable invocation result,
// including an empty result. It is evidence, not a user-managed ToolInvocation
// entity.
type ToolObservationSetV1 struct {
	SchemaVersion    string                  `json:"schema_version"`
	RunSnapshotID    int64                   `json:"run_snapshot_id"`
	InvocationDigest string                  `json:"invocation_digest"`
	Items            []ToolObservedContentV1 `json:"items"`
}

func BuildToolObservationSetV1(
	runSnapshotID int64,
	invocationDigest string,
	items []types.ContentItem,
) (ToolObservationSetV1, []byte, string, error) {
	if runSnapshotID <= 0 || !validDigestV1(invocationDigest) ||
		items == nil || len(items) > MaxToolObservationItemsV1 {
		return ToolObservationSetV1{}, nil, "", errors.New(
			"Tool observation set input is invalid")
	}
	observed := make([]ToolObservedContentV1, len(items))
	seenIDs := make(map[int64]struct{}, len(items))
	for i := range items {
		item := items[i]
		if item.ID <= 0 || item.SourceID != 0 ||
			strings.TrimSpace(item.CanonicalKey) == "" ||
			strings.TrimSpace(item.ExternalID) == "" ||
			strings.TrimSpace(item.ContentHash) == "" ||
			(item.Kind != types.KindArticle &&
				item.Kind != types.KindPageContent) {
			return ToolObservationSetV1{}, nil, "", errors.New(
				"Tool observed content is invalid")
		}
		if _, duplicate := seenIDs[item.ID]; duplicate {
			return ToolObservationSetV1{}, nil, "", errors.New(
				"Tool observed content is duplicated")
		}
		seenIDs[item.ID] = struct{}{}
		observed[i] = ToolObservedContentV1{
			ContentItemID: item.ID, ExternalID: item.ExternalID,
			CanonicalKey: item.CanonicalKey, Kind: item.Kind,
			URL: item.URL, Title: item.Title, Content: item.Content,
			Author: item.Author, PublishedAt: cloneTimeV1(item.PublishedAt),
			ContentHash: item.ContentHash, Simhash: cloneInt64V1(item.Simhash),
			FetchedAt: item.FetchedAt,
		}
	}
	set := ToolObservationSetV1{
		SchemaVersion: ToolObservationSetSchemaV1,
		RunSnapshotID: runSnapshotID, InvocationDigest: invocationDigest,
		Items: observed,
	}
	canonical, err := json.Marshal(set)
	if err != nil || len(canonical) == 0 ||
		len(canonical) > MaxToolObservationBytesV1 {
		return ToolObservationSetV1{}, nil, "", errors.New(
			"Tool observation set cannot be encoded")
	}
	sum := sha256.Sum256(canonical)
	return set, canonical, hex.EncodeToString(sum[:]), nil
}

func DecodeToolObservationSetV1(
	raw []byte,
) (ToolObservationSetV1, []types.ContentItem, string, error) {
	if len(raw) == 0 || len(raw) > MaxToolObservationBytesV1 ||
		strictjson.Validate(raw) != nil {
		return ToolObservationSetV1{}, nil, "", errors.New(
			"Tool observation set payload is invalid")
	}
	var set ToolObservationSetV1
	if err := strictjson.DecodeExact(raw, &set); err != nil ||
		set.SchemaVersion != ToolObservationSetSchemaV1 ||
		set.RunSnapshotID <= 0 ||
		!validDigestV1(set.InvocationDigest) ||
		set.Items == nil ||
		len(set.Items) > MaxToolObservationItemsV1 {
		return ToolObservationSetV1{}, nil, "", errors.New(
			"Tool observation set envelope is invalid")
	}
	items := make([]types.ContentItem, len(set.Items))
	seenIDs := make(map[int64]struct{}, len(set.Items))
	for i := range set.Items {
		item := set.Items[i]
		if item.ContentItemID <= 0 ||
			strings.TrimSpace(item.CanonicalKey) == "" ||
			strings.TrimSpace(item.ExternalID) == "" ||
			strings.TrimSpace(item.ContentHash) == "" ||
			(item.Kind != types.KindArticle &&
				item.Kind != types.KindPageContent) {
			return ToolObservationSetV1{}, nil, "", errors.New(
				"Tool observation set content is invalid")
		}
		if _, duplicate := seenIDs[item.ContentItemID]; duplicate {
			return ToolObservationSetV1{}, nil, "", errors.New(
				"Tool observation set content is duplicated")
		}
		seenIDs[item.ContentItemID] = struct{}{}
		items[i] = types.ContentItem{
			ID: item.ContentItemID, SourceID: 0,
			ExternalID: item.ExternalID, CanonicalKey: item.CanonicalKey,
			Kind: item.Kind, URL: item.URL, Title: item.Title,
			Content: item.Content, Author: item.Author,
			PublishedAt: cloneTimeV1(item.PublishedAt),
			ContentHash: item.ContentHash, Simhash: cloneInt64V1(item.Simhash),
			FetchedAt: item.FetchedAt,
		}
	}
	canonical, err := json.Marshal(set)
	if err != nil || !bytes.Equal(canonical, raw) {
		return ToolObservationSetV1{}, nil, "", errors.New(
			"Tool observation set payload is not canonical")
	}
	sum := sha256.Sum256(canonical)
	return set, items, hex.EncodeToString(sum[:]), nil
}

func validDigestV1(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil && value == strings.ToLower(value)
}

func cloneTimeV1(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func cloneInt64V1(value *int64) *int64 {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}
