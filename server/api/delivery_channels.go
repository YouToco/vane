package api

import (
	"encoding/json"
	"io"
	"net/http"

	"github.com/YouToco/vane/server/auth"
	"github.com/YouToco/vane/server/store"
)

const deliveryChannelPreferenceBodyLimit = 2 << 10

type deliveryChannelPreferencePatch struct {
	Selection       store.DeliveryChannelSelection `json:"selection"`
	TelegramRouteID *int64                         `json:"telegram_route_id"`
}

func (s *server) handleGetDeliveryChannelPreference(
	w http.ResponseWriter, r *http.Request,
) {
	principal, err := auth.PrincipalFromContext(r.Context())
	if err != nil {
		writeAppError(w, err)
		return
	}
	preference, err := s.deps.Store.ResolveDeliveryChannelPreference(
		r.Context(), int64(principal.TenantID), principal.UserID, "")
	if err != nil {
		writeAppError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, preference)
}

func (s *server) handlePatchDeliveryChannelPreference(
	w http.ResponseWriter, r *http.Request,
) {
	if !s.checkOrigin(w, r) {
		return
	}
	body := http.MaxBytesReader(w, r.Body, deliveryChannelPreferenceBodyLimit)
	decoder := json.NewDecoder(body)
	decoder.DisallowUnknownFields()
	var patch deliveryChannelPreferencePatch
	if err := decoder.Decode(&patch); err != nil {
		writeError(w, http.StatusBadRequest, "投递渠道偏好 JSON 无效")
		return
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF || !patch.Selection.Valid() {
		writeError(w, http.StatusBadRequest, "投递渠道偏好无效")
		return
	}
	principal, err := auth.PrincipalFromContext(r.Context())
	if err != nil {
		writeAppError(w, err)
		return
	}
	preference, err := s.deps.Store.PutAccountDeliveryChannelPreference(
		r.Context(), int64(principal.TenantID), principal.UserID,
		patch.Selection, patch.TelegramRouteID)
	if err != nil {
		writeAppError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, preference)
}
