package api

import (
	"net/http"
	"net/url"
	"strconv"

	"github.com/YouToco/vane/store"
)

type callCostLedgerResponse struct {
	Items         []store.CallCostLedgerItem `json:"items"`
	NextPageToken string                     `json:"next_page_token,omitempty"`
}

func parseCallCostLedgerQuery(values url.Values) (store.CallCostLedgerQuery, string) {
	query := store.CallCostLedgerQuery{
		PageToken:     values.Get("page_token"),
		Kind:          values.Get("kind"),
		Provider:      values.Get("provider"),
		PricingStatus: values.Get("pricing_status"),
		TaskID:        values.Get("task_id"),
	}
	if raw := values.Get("page_size"); raw != "" {
		size, err := strconv.Atoi(raw)
		if err != nil || size < 1 || size > 100 {
			return store.CallCostLedgerQuery{}, "page_size 必须是 1 到 100 之间的整数"
		}
		query.PageSize = size
	}
	return query, ""
}

// handleListCallCostLedger exposes only immutable accounting fields to the
// platform owner. Prompt/completion/tool payloads and raw provider errors never
// enter this response type.
func (s *server) handleListCallCostLedger(w http.ResponseWriter, r *http.Request) {
	if !s.requirePlatformOwner(w, r) {
		return
	}
	query, message := parseCallCostLedgerQuery(r.URL.Query())
	if message != "" {
		writeError(w, http.StatusBadRequest, message)
		return
	}
	items, next, err := s.deps.Store.ListCallCostLedger(r.Context(), query)
	if err != nil {
		writeAppError(w, err)
		return
	}
	if items == nil {
		items = []store.CallCostLedgerItem{}
	}
	writeJSON(w, http.StatusOK, callCostLedgerResponse{
		Items: items, NextPageToken: next,
	})
}
