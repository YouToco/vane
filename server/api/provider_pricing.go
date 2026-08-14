package api

import (
	"encoding/json"
	"io"
	"net/http"
	"time"

	"github.com/YouToco/vane/server/auth"
	"github.com/YouToco/vane/server/store"
)

const providerPriceBodyLimit = 16 << 10

type replaceProviderPriceRequest struct {
	Provider                   string     `json:"provider"`
	Resource                   string     `json:"resource"`
	Meter                      string     `json:"meter"`
	Currency                   string     `json:"currency"`
	InputCacheHitPerMillion    *float64   `json:"input_cache_hit_per_million,omitempty"`
	InputCacheMissPerMillion   *float64   `json:"input_cache_miss_per_million,omitempty"`
	OutputPerMillion           *float64   `json:"output_per_million,omitempty"`
	RequestUnitPrice           *float64   `json:"request_unit_price,omitempty"`
	RequestIncludedQuantity    *float64   `json:"request_included_quantity,omitempty"`
	RequestAdditionalUnitPrice *float64   `json:"request_additional_unit_price,omitempty"`
	EffectiveFrom              *time.Time `json:"effective_from,omitempty"`
	SourceURL                  string     `json:"source_url"`
	Note                       string     `json:"note"`
}

func (s *server) handleListProviderPrices(w http.ResponseWriter, r *http.Request) {
	if !s.requirePlatformOwner(w, r) {
		return
	}
	rules, err := s.deps.Store.ListProviderPriceRules(r.Context())
	if err != nil {
		writeAppError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"rules": rules})
}

func (s *server) handleReplaceProviderPrice(w http.ResponseWriter, r *http.Request) {
	if !s.requirePlatformOwner(w, r) {
		return
	}
	changeID, ok := scheduleCommandIdempotencyKey(r)
	if !ok {
		writeError(w, http.StatusBadRequest, "缺少或无效的 Idempotency-Key")
		return
	}
	body := http.MaxBytesReader(w, r.Body, providerPriceBodyLimit)
	dec := json.NewDecoder(body)
	dec.DisallowUnknownFields()
	var req replaceProviderPriceRequest
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "价格配置不是合法 JSON")
		return
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		writeError(w, http.StatusBadRequest, "价格配置只能包含一个 JSON 对象")
		return
	}
	p, err := auth.PrincipalFromContext(r.Context())
	if err != nil {
		writeAppError(w, err)
		return
	}
	var effectiveFrom time.Time
	if req.EffectiveFrom != nil {
		effectiveFrom = req.EffectiveFrom.UTC()
	}
	rule, err := s.deps.Store.ReplaceProviderPriceRule(
		r.Context(),
		store.ReplaceProviderPriceRuleInput{
			Provider: req.Provider, Resource: req.Resource, Meter: req.Meter,
			Currency:                   req.Currency,
			InputCacheHitPerMillion:    req.InputCacheHitPerMillion,
			InputCacheMissPerMillion:   req.InputCacheMissPerMillion,
			OutputPerMillion:           req.OutputPerMillion,
			RequestUnitPrice:           req.RequestUnitPrice,
			RequestIncludedQuantity:    req.RequestIncludedQuantity,
			RequestAdditionalUnitPrice: req.RequestAdditionalUnitPrice,
			EffectiveFrom:              effectiveFrom, SourceURL: req.SourceURL, Note: req.Note,
			CreatedBy: p.UserID, ChangeID: changeID,
		},
	)
	if err != nil {
		writeAppError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, rule)
}
