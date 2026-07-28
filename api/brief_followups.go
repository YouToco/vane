package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/YouToco/vane/agent"
	"github.com/YouToco/vane/auth"
	"github.com/YouToco/vane/feedback"
	"github.com/YouToco/vane/store"
	"github.com/YouToco/vane/types"
)

const briefFollowupBodyLimit = 20 << 10

var errInvalidBriefTarget = errors.New("invalid brief target")

type groundedTaskAgent interface {
	HandleGroundedMessage(
		ctx context.Context,
		userID int64,
		question string,
		grounding string,
	) (agent.Outcome, error)
}

type briefFollowupRequestV1 struct {
	Question string `json:"question"`
}

type briefDeepDiveRequestV1 struct {
	InsightID int64 `json:"insight_id"`
}

func (s *server) loadBriefGrounding(
	r *http.Request,
	kind store.GroundedBriefKindV1,
) (store.GroundedBriefContextV1, error) {
	if !s.executiveBriefTaskEnabled(r.PathValue("id")) {
		return store.GroundedBriefContextV1{},
			types.NewAppError(types.CodeNotFound, "执行简报未启用", nil)
	}
	targetID, err := strconv.ParseInt(r.PathValue("target_id"), 10, 64)
	if err != nil || targetID <= 0 {
		return store.GroundedBriefContextV1{}, errInvalidBriefTarget
	}
	principal, err := auth.PrincipalFromContext(r.Context())
	if err != nil {
		return store.GroundedBriefContextV1{}, err
	}
	return s.deps.Store.LoadGroundedBriefContextV1(
		r.Context(), int64(principal.TenantID), principal.UserID,
		r.PathValue("id"), kind, targetID)
}

func (s *server) handleBriefGrounding(
	w http.ResponseWriter,
	r *http.Request,
	kind store.GroundedBriefKindV1,
) {
	contextValue, err := s.loadBriefGrounding(r, kind)
	if err != nil {
		if errors.Is(err, errInvalidBriefTarget) {
			writeError(w, http.StatusBadRequest, "简报证据目标无效")
			return
		}
		writeAppError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, contextValue)
}

func (s *server) handleIssueBriefGrounding(
	w http.ResponseWriter,
	r *http.Request,
) {
	s.handleBriefGrounding(w, r, store.GroundedBriefIssue)
}

func (s *server) handlePeriodicBriefGrounding(
	w http.ResponseWriter,
	r *http.Request,
) {
	s.handleBriefGrounding(w, r, store.GroundedBriefReport)
}

func (s *server) handleBriefFollowup(
	w http.ResponseWriter,
	r *http.Request,
	kind store.GroundedBriefKindV1,
) {
	targetID, err := strconv.ParseInt(r.PathValue("target_id"), 10, 64)
	if err != nil || targetID <= 0 {
		writeError(w, http.StatusBadRequest, "简报追问目标无效")
		return
	}
	var request briefFollowupRequestV1
	body := http.MaxBytesReader(w, r.Body, briefFollowupBodyLimit)
	decoder := json.NewDecoder(body)
	decoder.DisallowUnknownFields()
	if decoder.Decode(&request) != nil ||
		strings.TrimSpace(request.Question) == "" ||
		len(request.Question) > 16<<10 {
		writeError(w, http.StatusBadRequest, "简报追问内容无效")
		return
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		writeError(w, http.StatusBadRequest, "简报追问内容无效")
		return
	}
	contextValue, err := s.loadBriefGrounding(r, kind)
	if err != nil {
		writeAppError(w, err)
		return
	}
	grounding, err := json.Marshal(contextValue)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "简报追问依据编码失败")
		return
	}
	grounded, ok := s.deps.TaskAgent.(groundedTaskAgent)
	if !ok {
		writeError(w, http.StatusServiceUnavailable, "简报追问能力未启用")
		return
	}
	principal, err := auth.PrincipalFromContext(r.Context())
	if err != nil {
		writeAppError(w, err)
		return
	}
	outcome, err := grounded.HandleGroundedMessage(
		r.Context(), principal.UserID, request.Question, string(grounding))
	if err != nil {
		writeAppError(w, err)
		return
	}
	if outcome.Confirm != nil {
		writeError(w, http.StatusConflict, "简报追问仅支持只读回答")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"reply": outcome.Reply})
}

func (s *server) handleIssueBriefFollowup(
	w http.ResponseWriter,
	r *http.Request,
) {
	s.handleBriefFollowup(w, r, store.GroundedBriefIssue)
}

func (s *server) handlePeriodicBriefFollowup(
	w http.ResponseWriter,
	r *http.Request,
) {
	s.handleBriefFollowup(w, r, store.GroundedBriefReport)
}

func groundedContextAllowsDeepDiveV1(
	contextValue store.GroundedBriefContextV1,
	insightID int64,
) bool {
	if insightID <= 0 {
		return false
	}
	referenced := false
	for _, step := range contextValue.Content.NextSteps {
		if step.Kind != types.ExecutiveNextStepDeepDive {
			continue
		}
		for _, ref := range step.EvidenceRefs {
			if ref.InsightID == insightID {
				referenced = true
				break
			}
		}
	}
	if !referenced {
		return false
	}
	for _, brief := range contextValue.Evidence {
		for _, insight := range brief.Insights {
			if insight.ID == insightID {
				return true
			}
		}
	}
	return false
}

func (s *server) handleBriefDeepDive(
	w http.ResponseWriter,
	r *http.Request,
	kind store.GroundedBriefKindV1,
) {
	var request briefDeepDiveRequestV1
	body := http.MaxBytesReader(w, r.Body, briefFollowupBodyLimit)
	decoder := json.NewDecoder(body)
	decoder.DisallowUnknownFields()
	if decoder.Decode(&request) != nil || request.InsightID <= 0 {
		writeError(w, http.StatusBadRequest, "深入了解目标无效")
		return
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		writeError(w, http.StatusBadRequest, "深入了解目标无效")
		return
	}
	contextValue, err := s.loadBriefGrounding(r, kind)
	if err != nil {
		writeAppError(w, err)
		return
	}
	if !groundedContextAllowsDeepDiveV1(
		contextValue, request.InsightID,
	) {
		writeError(w, http.StatusConflict, "该简报未提供此深入了解动作")
		return
	}
	if s.deps.BriefFeedback == nil {
		writeError(w, http.StatusServiceUnavailable, "深入了解能力未启用")
		return
	}
	principal, err := auth.PrincipalFromContext(r.Context())
	if err != nil {
		writeAppError(w, err)
		return
	}
	result, err := s.deps.BriefFeedback.HandleClick(
		r.Context(), principal.UserID,
		feedback.Click{
			Action:     types.FeedbackActionDeepDive,
			DeliveryID: request.InsightID,
		},
	)
	if err != nil {
		writeAppError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"message": result.Toast, "accepted": result.ToastOK,
	})
}

func (s *server) handleIssueBriefDeepDive(
	w http.ResponseWriter,
	r *http.Request,
) {
	s.handleBriefDeepDive(w, r, store.GroundedBriefIssue)
}

func (s *server) handlePeriodicBriefDeepDive(
	w http.ResponseWriter,
	r *http.Request,
) {
	s.handleBriefDeepDive(w, r, store.GroundedBriefReport)
}
