package store

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"strconv"

	"github.com/YouToco/vane/types"
)

const (
	defaultProfileClaimEventLimit = 20
	maxProfileClaimEventLimit     = 50
	maxProfileClaimEventCursorLen = 512
	maxPublicActiveProfileClaims  = 514
	maxPublicEventContextClaims   = 100
	maxPublicFirstProfileClaims   = 614
	maxPublicActionProfileClaims  = 516
	profileClaimEventCursorSchema = "vane.profile-claim-event-cursor/v2"
	profileClaimEventCursorKind   = "profile_claim_events"
)

type ProfileClaimEventPageOptions struct {
	Limit  int
	Cursor string
}

type profileClaimEventCursor struct {
	Schema             string `json:"schema"`
	Kind               string `json:"kind"`
	TenantID           int64  `json:"tenant_id"`
	UserID             int64  `json:"user_id"`
	ProfileEpoch       int64  `json:"profile_epoch"`
	SnapshotVersion    int64  `json:"snapshot_version"`
	SnapshotMaxEventID int64  `json:"snapshot_max_event_id"`
	BeforeEventID      int64  `json:"before_event_id"`
	Limit              int    `json:"limit"`
}

func encodeProfileClaimEventCursor(cursor profileClaimEventCursor) (string, error) {
	raw, err := json.Marshal(cursor)
	if err != nil {
		return "", types.NewAppError(
			types.CodeInternal, "编码画像主张事件游标失败", err)
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

func decodeProfileClaimEventCursor(token string) (profileClaimEventCursor, error) {
	var cursor profileClaimEventCursor
	if token == "" || len(token) > maxProfileClaimEventCursorLen {
		return cursor, invalidProfileClaimEventCursor()
	}
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil || len(raw) == 0 || len(raw) > maxProfileClaimEventCursorLen {
		return cursor, invalidProfileClaimEventCursor()
	}
	if err := validateProfileClaimCursorJSON(raw); err != nil {
		return cursor, err
	}
	if err := json.Unmarshal(raw, &cursor); err != nil {
		return cursor, invalidProfileClaimEventCursor()
	}
	if cursor.Schema != profileClaimEventCursorSchema ||
		cursor.Kind != profileClaimEventCursorKind ||
		cursor.TenantID <= 0 || cursor.UserID <= 0 ||
		cursor.ProfileEpoch < 0 ||
		cursor.SnapshotVersion < 0 || cursor.SnapshotMaxEventID <= 0 ||
		cursor.BeforeEventID <= 0 ||
		cursor.BeforeEventID > cursor.SnapshotMaxEventID ||
		cursor.Limit < 1 || cursor.Limit > maxProfileClaimEventLimit {
		return profileClaimEventCursor{}, invalidProfileClaimEventCursor()
	}
	return cursor, nil
}

func validateProfileClaimCursorJSON(raw []byte) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	token, err := dec.Token()
	if err != nil || token != json.Delim('{') {
		return invalidProfileClaimEventCursor()
	}
	allowed := map[string]bool{
		"schema": true, "kind": true, "tenant_id": true, "user_id": true,
		"profile_epoch":    true,
		"snapshot_version": true, "snapshot_max_event_id": true,
		"before_event_id": true, "limit": true,
	}
	seen := make(map[string]bool, len(allowed))
	for dec.More() {
		keyToken, err := dec.Token()
		if err != nil {
			return invalidProfileClaimEventCursor()
		}
		key, ok := keyToken.(string)
		if !ok || !allowed[key] || seen[key] {
			return invalidProfileClaimEventCursor()
		}
		seen[key] = true
		var value json.RawMessage
		if err := dec.Decode(&value); err != nil {
			return invalidProfileClaimEventCursor()
		}
	}
	if _, err := dec.Token(); err != nil || len(seen) != len(allowed) {
		return invalidProfileClaimEventCursor()
	}
	var trailing any
	if err := dec.Decode(&trailing); err != io.EOF {
		return invalidProfileClaimEventCursor()
	}
	return nil
}

func invalidProfileClaimEventCursor() error {
	return types.NewAppError(
		types.CodeValidation, "画像主张事件游标无效", types.ErrValidation)
}

func validateProfileClaimEventPageOptions(
	tenantID, userID int64, options ProfileClaimEventPageOptions,
) (profileClaimEventCursor, bool, error) {
	if options.Limit == 0 {
		options.Limit = defaultProfileClaimEventLimit
	}
	if options.Limit < 1 || options.Limit > maxProfileClaimEventLimit {
		return profileClaimEventCursor{}, false, types.NewAppError(
			types.CodeValidation, "画像主张事件页大小必须是 1 到 50", nil)
	}
	if options.Cursor == "" {
		return profileClaimEventCursor{Limit: options.Limit}, false, nil
	}
	cursor, err := decodeProfileClaimEventCursor(options.Cursor)
	if err != nil {
		return profileClaimEventCursor{}, false, err
	}
	if cursor.TenantID != tenantID || cursor.UserID != userID ||
		cursor.Limit != options.Limit {
		return profileClaimEventCursor{}, false, invalidProfileClaimEventCursor()
	}
	return cursor, true, nil
}

func maxProfileClaimEventID(events []profileClaimEventRow) int64 {
	if len(events) == 0 {
		return 0
	}
	return events[len(events)-1].ID
}

func pageProfileClaimEvents(
	events []profileClaimEventRow, snapshotMaxID, beforeID int64, limit int,
) ([]profileClaimEventRow, bool) {
	page := make([]profileClaimEventRow, 0, limit)
	hasMore := false
	for i := len(events) - 1; i >= 0; i-- {
		event := events[i]
		if event.ID > snapshotMaxID || (beforeID > 0 && event.ID >= beforeID) {
			continue
		}
		if len(page) == limit {
			hasMore = true
			break
		}
		page = append(page, event)
	}
	return page, hasMore
}

func publicProfileClaimEventPage(
	allEvents, page []profileClaimEventRow,
) []types.ProfileClaimEvent {
	revoked, dependent := profileClaimEventAuthority(allEvents)
	out := make([]types.ProfileClaimEvent, 0, len(page))
	for _, event := range page {
		item := types.ProfileClaimEvent{
			ID:        strconv.FormatInt(event.ID, 10),
			Kind:      event.Kind,
			CreatedAt: event.CreatedAt,
			Revoked:   event.Kind != "revoke" && revoked[event.ID],
			Revocable: event.Kind != "revoke" && !revoked[event.ID] &&
				!dependent[event.ID],
		}
		if event.TargetClaimID != nil {
			item.TargetClaimID = strconv.FormatInt(*event.TargetClaimID, 10)
		}
		if event.ResultClaimID != nil {
			item.ResultClaimID = strconv.FormatInt(*event.ResultClaimID, 10)
		}
		out = append(out, item)
	}
	if out == nil {
		out = []types.ProfileClaimEvent{}
	}
	return out
}

func publicProfileClaimsForPage(
	claims []profileClaimRow, projection profileClaimProjection,
	pageEvents []profileClaimEventRow, includeActive bool,
) ([]types.ProfileClaim, error) {
	include := make(map[int64]bool)
	if includeActive {
		for _, claim := range claims {
			if projection.active[claim.ID] {
				include[claim.ID] = true
			}
		}
		if len(include) > maxPublicActiveProfileClaims {
			return nil, types.NewAppError(
				types.CodeInternal, "生效画像主张超过公开读取硬上界", nil)
		}
	}
	addProfileClaimEventContext(include, pageEvents)
	limit := maxPublicEventContextClaims
	if includeActive {
		limit = maxPublicFirstProfileClaims
	}
	if len(include) > limit {
		return nil, types.NewAppError(
			types.CodeInternal, "画像主张事件上下文超过公开读取硬上界", nil)
	}
	return publicProfileClaimSubset(claims, projection, include), nil
}

func publicProfileClaimsForAction(
	claims []profileClaimRow, projection profileClaimProjection,
	contextIDs map[int64]bool,
) ([]types.ProfileClaim, error) {
	include := make(map[int64]bool)
	for _, claim := range claims {
		if projection.active[claim.ID] {
			include[claim.ID] = true
		}
	}
	if len(include) > maxPublicActiveProfileClaims {
		return nil, types.NewAppError(
			types.CodeInternal, "生效画像主张超过 action 响应硬上界", nil)
	}
	for id := range contextIDs {
		if id > 0 {
			include[id] = true
		}
	}
	if len(include) > maxPublicActionProfileClaims {
		return nil, types.NewAppError(
			types.CodeInternal, "画像主张 action 上下文超过响应硬上界", nil)
	}
	return publicProfileClaimSubset(claims, projection, include), nil
}

func addProfileClaimEventContext(
	include map[int64]bool, events []profileClaimEventRow,
) {
	for _, event := range events {
		if event.TargetClaimID != nil {
			include[*event.TargetClaimID] = true
		}
		if event.ResultClaimID != nil {
			include[*event.ResultClaimID] = true
		}
	}
}

func publicProfileClaimSubset(
	claims []profileClaimRow, projection profileClaimProjection,
	include map[int64]bool,
) []types.ProfileClaim {
	out := make([]types.ProfileClaim, 0, len(include))
	for _, claim := range claims {
		if !include[claim.ID] {
			continue
		}
		item := types.ProfileClaim{
			ID:        strconv.FormatInt(claim.ID, 10),
			Field:     claim.Field,
			Value:     claim.Value,
			Source:    types.ProfileClaimSource{State: claim.SourceState},
			Active:    projection.active[claim.ID],
			Pinned:    projection.pinned[claim.ID],
			CreatedAt: claim.CreatedAt,
		}
		if claim.SourceRefType != nil {
			item.Source.RefType = *claim.SourceRefType
		}
		if claim.SourceRef != nil {
			item.Source.Ref = *claim.SourceRef
		}
		if claim.SupersedesID != nil {
			item.SupersedesID = strconv.FormatInt(*claim.SupersedesID, 10)
		}
		out = append(out, item)
	}
	if out == nil {
		out = []types.ProfileClaim{}
	}
	return out
}

func profileClaimCursorConflict() error {
	return types.NewAppError(
		types.CodeConflict, "画像主张版本已变化，请从首屏重新读取", errors.New("stale profile claim event cursor"))
}
