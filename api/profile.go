package api

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/YouToco/vane/auth"
	"github.com/YouToco/vane/store"
	"github.com/YouToco/vane/types"
)

const profileEditBodyLimit = 8 << 10

type patchProfileRequest struct {
	ExpectedUpdatedAt json.RawMessage `json:"expected_updated_at"`
	Industry          json.RawMessage `json:"industry,omitempty"`
	Occupation        json.RawMessage `json:"occupation,omitempty"`
	Tags              json.RawMessage `json:"tags,omitempty"`
}

type undoProfileRequest struct {
	ExpectedUpdatedAt json.RawMessage `json:"expected_updated_at"`
}

type profileClaimActionRequest struct {
	ExpectedVersion int64  `json:"expected_version"`
	Action          string `json:"action"`
	ClaimID         string `json:"claim_id,omitempty"`
	EventID         string `json:"event_id,omitempty"`
	Value           string `json:"value,omitempty"`
}

func (s *server) handleProfile(w http.ResponseWriter, r *http.Request) {
	p, err := auth.PrincipalFromContext(r.Context())
	if err != nil {
		writeAppError(w, err)
		return
	}
	profile, err := s.deps.Store.GetProfileView(
		r.Context(), int64(p.TenantID), p.UserID)
	if err != nil {
		writeAppError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, profile)
}

func (s *server) handlePatchProfile(w http.ResponseWriter, r *http.Request) {
	key, ok := scheduleCommandIdempotencyKey(r)
	if !ok {
		writeError(w, http.StatusBadRequest, "缺少或无效的 Idempotency-Key")
		return
	}
	var req patchProfileRequest
	if !decodeProfileJSON(w, r, &req) {
		return
	}
	expected, valid := parseExpectedUpdatedAt(req.ExpectedUpdatedAt, true)
	if !valid {
		writeError(w, http.StatusBadRequest, "expected_updated_at 必须存在，且为 null 或 RFC3339 时间")
		return
	}
	patch, err := canonicalizeProfilePatch(req)
	if err != nil {
		writeAppError(w, err)
		return
	}
	principal, err := auth.PrincipalFromContext(r.Context())
	if err != nil {
		writeAppError(w, err)
		return
	}
	digest, err := store.ProfileEditRequestDigest(struct {
		Operation         string     `json:"operation"`
		ExpectedUpdatedAt *time.Time `json:"expected_updated_at"`
		Industry          *string    `json:"industry,omitempty"`
		Occupation        *string    `json:"occupation,omitempty"`
		Tags              *[]string  `json:"tags,omitempty"`
	}{
		Operation: "PATCH /api/profile", ExpectedUpdatedAt: expected,
		Industry: patch.Industry, Occupation: patch.Occupation, Tags: patch.Tags,
	})
	if err != nil {
		writeAppError(w, err)
		return
	}
	profile, err := s.deps.Store.PatchProfile(
		r.Context(), int64(principal.TenantID), principal.UserID,
		expected, patch, key, digest)
	if err != nil {
		writeAppError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, profile)
}

func (s *server) handleListProfileEdits(w http.ResponseWriter, r *http.Request) {
	limit := 20
	if raw := r.URL.Query().Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > 50 {
			writeError(w, http.StatusBadRequest, "limit 必须是 1 到 50")
			return
		}
		limit = n
	}
	if len(r.URL.Query()) > 0 {
		for key := range r.URL.Query() {
			if key != "limit" {
				writeError(w, http.StatusBadRequest, "包含未知查询参数")
				return
			}
		}
	}
	principal, err := auth.PrincipalFromContext(r.Context())
	if err != nil {
		writeAppError(w, err)
		return
	}
	edits, err := s.deps.Store.ListProfileEdits(
		r.Context(), int64(principal.TenantID), principal.UserID, limit)
	if err != nil {
		writeAppError(w, err)
		return
	}
	if edits == nil {
		edits = []types.ProfileEditRevision{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"edits": edits})
}

func (s *server) handleUndoProfileEdit(w http.ResponseWriter, r *http.Request) {
	key, ok := scheduleCommandIdempotencyKey(r)
	if !ok {
		writeError(w, http.StatusBadRequest, "缺少或无效的 Idempotency-Key")
		return
	}
	targetID, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || targetID <= 0 {
		writeError(w, http.StatusBadRequest, "画像编辑记录 id 无效")
		return
	}
	var req undoProfileRequest
	if !decodeProfileJSON(w, r, &req) {
		return
	}
	expected, valid := parseExpectedUpdatedAt(req.ExpectedUpdatedAt, false)
	if !valid || expected == nil {
		writeError(w, http.StatusBadRequest, "expected_updated_at 必须是 RFC3339 时间")
		return
	}
	principal, err := auth.PrincipalFromContext(r.Context())
	if err != nil {
		writeAppError(w, err)
		return
	}
	digest, err := store.ProfileEditRequestDigest(struct {
		Operation         string    `json:"operation"`
		TargetRevisionID  int64     `json:"target_revision_id"`
		ExpectedUpdatedAt time.Time `json:"expected_updated_at"`
	}{
		Operation:        "POST /api/profile/edits/{id}/undo",
		TargetRevisionID: targetID, ExpectedUpdatedAt: expected.UTC(),
	})
	if err != nil {
		writeAppError(w, err)
		return
	}
	profile, err := s.deps.Store.UndoProfileEdit(
		r.Context(), int64(principal.TenantID), principal.UserID,
		targetID, *expected, key, digest)
	if err != nil {
		writeAppError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, profile)
}

func (s *server) handleListProfileClaims(w http.ResponseWriter, r *http.Request) {
	if len(r.URL.Query()) != 0 {
		writeError(w, http.StatusBadRequest, "画像主张查询不接受查询参数")
		return
	}
	principal, err := auth.PrincipalFromContext(r.Context())
	if err != nil {
		writeAppError(w, err)
		return
	}
	result, err := s.deps.Store.ListProfileClaims(
		r.Context(), int64(principal.TenantID), principal.UserID)
	if err != nil {
		writeAppError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *server) handleProfileClaimAction(w http.ResponseWriter, r *http.Request) {
	key, ok := scheduleCommandIdempotencyKey(r)
	if !ok {
		writeError(w, http.StatusBadRequest, "缺少或无效的 Idempotency-Key")
		return
	}
	var req profileClaimActionRequest
	if !decodeProfileClaimActionJSON(w, r, &req) {
		return
	}
	action := types.ProfileClaimAction{
		ExpectedVersion: req.ExpectedVersion,
		Action:          strings.TrimSpace(req.Action),
		Value:           strings.TrimSpace(req.Value),
	}
	var err error
	switch action.Action {
	case "correct", "suppress", "pin":
		action.ClaimID, err = strconv.ParseInt(req.ClaimID, 10, 64)
		if err != nil || action.ClaimID <= 0 || req.EventID != "" {
			writeError(w, http.StatusBadRequest, "该操作需要合法 claim_id，且不能提供 event_id")
			return
		}
		if action.Action == "correct" {
			if action.Value == "" || utf8.RuneCountInString(action.Value) > 240 {
				writeError(w, http.StatusBadRequest, "correct 的 value 必须为 1 到 240 字")
				return
			}
		} else if req.Value != "" {
			writeError(w, http.StatusBadRequest, "suppress/pin 不能提供 value")
			return
		}
	case "revoke":
		action.EventID, err = strconv.ParseInt(req.EventID, 10, 64)
		if err != nil || action.EventID <= 0 || req.ClaimID != "" || req.Value != "" {
			writeError(w, http.StatusBadRequest, "revoke 只接受合法 event_id")
			return
		}
	default:
		writeError(w, http.StatusBadRequest, "action 必须是 correct、suppress、pin 或 revoke")
		return
	}
	if action.ExpectedVersion < 0 {
		writeError(w, http.StatusBadRequest, "expected_version 不能为负数")
		return
	}
	principal, err := auth.PrincipalFromContext(r.Context())
	if err != nil {
		writeAppError(w, err)
		return
	}
	digest, err := store.ProfileEditRequestDigest(req)
	if err != nil {
		writeAppError(w, err)
		return
	}
	result, err := s.deps.Store.ApplyProfileClaimAction(
		r.Context(), int64(principal.TenantID), principal.UserID,
		action, key, digest)
	if err != nil {
		writeAppError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func decodeProfileClaimActionJSON(
	w http.ResponseWriter, r *http.Request, dst *profileClaimActionRequest,
) bool {
	r.Body = http.MaxBytesReader(w, r.Body, profileEditBodyLimit)
	data, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "画像主张请求体过大或无法读取")
		return false
	}
	keys, err := topLevelJSONKeys(data)
	if err != nil {
		writeError(w, http.StatusBadRequest, "请求体必须是单一 JSON 对象，且字段不能重复")
		return false
	}
	allowed := map[string]bool{
		"expected_version": true, "action": true, "claim_id": true,
		"event_id": true, "value": true,
	}
	for key := range keys {
		if !allowed[key] {
			writeError(w, http.StatusBadRequest, "请求体包含未知字段")
			return false
		}
	}
	if _, ok := keys["expected_version"]; !ok {
		writeError(w, http.StatusBadRequest, "缺少 expected_version")
		return false
	}
	if _, ok := keys["action"]; !ok {
		writeError(w, http.StatusBadRequest, "缺少 action")
		return false
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		writeError(w, http.StatusBadRequest, "请求体不是合法的画像主张 JSON")
		return false
	}
	var trailing any
	if err := dec.Decode(&trailing); err != io.EOF {
		writeError(w, http.StatusBadRequest, "请求体只能包含一个 JSON 对象")
		return false
	}
	return true
}

func decodeProfileJSON(w http.ResponseWriter, r *http.Request, dst any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, profileEditBodyLimit)
	data, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "画像修改请求体过大或无法读取")
		return false
	}
	keys, err := topLevelJSONKeys(data)
	if err != nil {
		writeError(w, http.StatusBadRequest, "请求体必须是单一 JSON 对象，且字段不能重复")
		return false
	}
	allowed := map[string]bool{"expected_updated_at": true}
	if _, ok := dst.(*patchProfileRequest); ok {
		allowed["industry"] = true
		allowed["occupation"] = true
		allowed["tags"] = true
	}
	for key := range keys {
		if !allowed[key] {
			writeError(w, http.StatusBadRequest, "请求体包含未知字段")
			return false
		}
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		writeError(w, http.StatusBadRequest, "请求体不是合法的画像修改 JSON")
		return false
	}
	var trailing any
	if err := dec.Decode(&trailing); err != io.EOF {
		writeError(w, http.StatusBadRequest, "请求体只能包含一个 JSON 对象")
		return false
	}
	return true
}

func topLevelJSONKeys(data []byte) (map[string]struct{}, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	tok, err := dec.Token()
	if err != nil || tok != json.Delim('{') {
		return nil, types.ErrValidation
	}
	seen := make(map[string]struct{})
	for dec.More() {
		keyTok, err := dec.Token()
		if err != nil {
			return nil, err
		}
		key, ok := keyTok.(string)
		if !ok {
			return nil, types.ErrValidation
		}
		if _, duplicate := seen[key]; duplicate {
			return nil, types.ErrValidation
		}
		seen[key] = struct{}{}
		var value json.RawMessage
		if err := dec.Decode(&value); err != nil {
			return nil, err
		}
	}
	if _, err := dec.Token(); err != nil {
		return nil, err
	}
	var trailing any
	if err := dec.Decode(&trailing); err != io.EOF {
		return nil, types.ErrValidation
	}
	return seen, nil
}

func parseExpectedUpdatedAt(raw json.RawMessage, allowNull bool) (*time.Time, bool) {
	if len(raw) == 0 {
		return nil, false
	}
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, allowNull
	}
	var text string
	if json.Unmarshal(raw, &text) != nil {
		return nil, false
	}
	t, err := time.Parse(time.RFC3339Nano, text)
	if err != nil {
		return nil, false
	}
	t = t.UTC()
	return &t, true
}

func canonicalizeProfilePatch(req patchProfileRequest) (types.ProfileEditPatch, error) {
	if len(req.Industry) == 0 && len(req.Occupation) == 0 && len(req.Tags) == 0 {
		return types.ProfileEditPatch{}, types.NewAppError(
			types.CodeValidation, "至少提供 industry、occupation、tags 中的一项", nil)
	}
	out := types.ProfileEditPatch{}
	if len(req.Industry) > 0 {
		var raw string
		if bytes.Equal(bytes.TrimSpace(req.Industry), []byte("null")) ||
			json.Unmarshal(req.Industry, &raw) != nil {
			return out, types.NewAppError(types.CodeValidation, "industry 必须是字符串，不能为 null", nil)
		}
		value := strings.TrimSpace(raw)
		if utf8.RuneCountInString(value) > 200 {
			return out, types.NewAppError(types.CodeValidation, "industry 不能超过 200 字", nil)
		}
		out.Industry = &value
	}
	if len(req.Occupation) > 0 {
		var raw string
		if bytes.Equal(bytes.TrimSpace(req.Occupation), []byte("null")) ||
			json.Unmarshal(req.Occupation, &raw) != nil {
			return out, types.NewAppError(types.CodeValidation, "occupation 必须是字符串，不能为 null", nil)
		}
		value := strings.TrimSpace(raw)
		if utf8.RuneCountInString(value) > 200 {
			return out, types.NewAppError(types.CodeValidation, "occupation 不能超过 200 字", nil)
		}
		out.Occupation = &value
	}
	if len(req.Tags) > 0 {
		var rawTags []string
		if bytes.Equal(bytes.TrimSpace(req.Tags), []byte("null")) ||
			json.Unmarshal(req.Tags, &rawTags) != nil {
			return out, types.NewAppError(types.CodeValidation, "tags 必须是字符串数组，不能为 null", nil)
		}
		tags := make([]string, 0, len(rawTags))
		seen := make(map[string]struct{}, len(rawTags))
		for _, rawTag := range rawTags {
			tag := strings.TrimSpace(rawTag)
			if tag == "" {
				return out, types.NewAppError(types.CodeValidation, "tag 不能为空", nil)
			}
			if strings.IndexFunc(tag, func(r rune) bool {
				return unicode.IsControl(r) || r == '\u2028' || r == '\u2029'
			}) >= 0 {
				return out, types.NewAppError(types.CodeValidation, "tag 不能包含控制字符", nil)
			}
			if utf8.RuneCountInString(tag) > 20 {
				return out, types.NewAppError(types.CodeValidation, "单个 tag 不能超过 20 字", nil)
			}
			if _, exists := seen[tag]; exists {
				continue
			}
			seen[tag] = struct{}{}
			tags = append(tags, tag)
		}
		if len(tags) > 12 {
			return out, types.NewAppError(types.CodeValidation, "tags 最多 12 个", nil)
		}
		out.Tags = &tags
	}
	return out, nil
}
