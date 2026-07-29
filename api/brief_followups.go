package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	neturl "net/url"
	"regexp"
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
	HandleGroundedMessageGuarded(
		ctx context.Context,
		userID int64,
		question string,
		grounding string,
		replyGuard func(string) (string, error),
	) (agent.Outcome, error)
}

type briefFollowupRequestV1 struct {
	Question string `json:"question"`
}

type briefDeepDiveRequestV1 struct {
	InsightID int64 `json:"insight_id"`
}

type briefFollowupSignalV1 struct {
	Kind      string                       `json:"类型"`
	Lifecycle string                       `json:"阶段,omitempty"`
	Title     string                       `json:"标题"`
	Summary   string                       `json:"说明"`
	Evidence  []briefFollowupEvidenceRefV1 `json:"依据位置"`
}

type briefFollowupNextStepV1 struct {
	Kind      string                       `json:"类型"`
	Label     string                       `json:"名称"`
	Rationale string                       `json:"原因"`
	Evidence  []briefFollowupEvidenceRefV1 `json:"依据位置"`
}

type briefFollowupEvidenceRefV1 struct {
	BriefSequence   int   `json:"期次"`
	InsightSequence int   `json:"情报序号"`
	ClaimSequences  []int `json:"事实序号"`
}

type briefFollowupContentV1 struct {
	Headline         string                    `json:"结论"`
	ExecutiveSummary string                    `json:"摘要"`
	DecisionState    string                    `json:"建议状态"`
	WhyForYou        string                    `json:"与你的关系"`
	Signals          []briefFollowupSignalV1   `json:"共同信号"`
	NextSteps        []briefFollowupNextStepV1 `json:"下一步"`
}

type briefFollowupStructuredInsightV1 struct {
	WhatChanged      string                 `json:"发生了什么"`
	WhyItMatters     string                 `json:"为何重要"`
	ImportanceReason string                 `json:"重要性依据"`
	Claims           []briefFollowupClaimV1 `json:"已验证事实"`
}

type briefFollowupClaimV1 struct {
	Text    string `json:"事实"`
	Excerpt string `json:"已验证摘录"`
}

type briefFollowupInsightV1 struct {
	Title       string                            `json:"标题"`
	Summary     string                            `json:"摘要"`
	SourceTitle string                            `json:"来源"`
	SourceURL   string                            `json:"来源链接,omitempty"`
	PublishedAt string                            `json:"发布时间,omitempty"`
	Structured  *briefFollowupStructuredInsightV1 `json:"结构化解读,omitempty"`
}

type briefFollowupEvidenceV1 struct {
	Sequence    int                      `json:"期次"`
	GeneratedAt string                   `json:"简报时间"`
	Insights    []briefFollowupInsightV1 `json:"情报"`
}

type briefFollowupGroundingV1 struct {
	AnswerRules    []string                  `json:"回答边界"`
	Kind           string                    `json:"材料类型"`
	Cadence        string                    `json:"周期,omitempty"`
	PeriodStart    string                    `json:"周期开始,omitempty"`
	PeriodEnd      string                    `json:"周期结束,omitempty"`
	SourceCoverage string                    `json:"来源覆盖"`
	Processing     string                    `json:"处理覆盖"`
	Content        briefFollowupContentV1    `json:"简报内容"`
	Evidence       []briefFollowupEvidenceV1 `json:"依据简报"`
}

var groundedInternalReferenceV1 = regexp.MustCompile(
	`(?i)(?:\b(?:tenant_id|user_id|task_id|run_id|workflow_id|artifact_id|` +
		`receipt_id|brief_id|insight_id|report_id|snapshot_id|delivery_id|` +
		`batch_id|claim_id|source_id|push_batch_id|run_outcome_ids?|` +
		`run_snapshot_id)\b|\b(?:brief|insight|report)[ -]ids?\b|` +
		`\bsource[_ -]?(?:refs?|references?)\b|` +
		`\bclaim[_ -]?(?:indexes|indices)\b|` +
		`\b(?:input_digest|request_digest|payload_digest|profile_digest|` +
		`outcome_digest|content_digest|artifact_digest|card_digest|` +
		`evidence_digest)\b|\bprofile_(?:epoch|version)\b|` +
		`\bschema_version\b|\bgeneration_mode\b|\bdeterministic_fallback\b|` +
		`\bdiscovered_at\b|\bsource-[1-8]\b)`)

const groundedSafeFallbackReplyV1 = "这次回答涉及仅供系统校验的内部引用，已为你隐藏。你可以按简报标题、结论、影响或下一步继续追问。"

const groundedHiddenTextV1 = "包含仅供系统校验的信息，已隐藏。"

func briefFollowupSafeTextV1(value string) string {
	if groundedInternalReferenceV1.MatchString(value) {
		return groundedHiddenTextV1
	}
	return value
}

func briefFollowupSafeSourceURLV1(value string) string {
	parsed, err := neturl.Parse(value)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") ||
		parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" ||
		parsed.ForceQuery || parsed.Fragment != "" || parsed.RawFragment != "" {
		return ""
	}
	safe := parsed.String()
	if groundedInternalReferenceV1.MatchString(safe) {
		return ""
	}
	return safe
}

func briefFollowupDecisionLabelV1(
	value types.ExecutiveDecisionStateV1,
) string {
	switch value {
	case types.ExecutiveDecisionAct:
		return "需要行动"
	case types.ExecutiveDecisionWatch:
		return "继续关注"
	case types.ExecutiveDecisionNoAction:
		return "暂不行动"
	default:
		return "证据不足"
	}
}

func briefFollowupSignalKindLabelV1(
	value types.ExecutiveSignalKindV1,
) string {
	switch value {
	case types.ExecutiveSignalOpportunity:
		return "机会"
	case types.ExecutiveSignalRisk:
		return "风险"
	case types.ExecutiveSignalTrend:
		return "趋势"
	default:
		return "变化"
	}
}

func briefFollowupSignalLifecycleLabelV1(
	value types.ExecutiveSignalLifecycleV1,
) string {
	switch value {
	case types.ExecutiveSignalNew:
		return "新出现"
	case types.ExecutiveSignalPersistent:
		return "持续"
	case types.ExecutiveSignalIntensified:
		return "增强"
	case types.ExecutiveSignalFaded:
		return "已消退"
	default:
		return ""
	}
}

func briefFollowupNextStepLabelV1(
	value types.ExecutiveNextStepKindV1,
) string {
	switch value {
	case types.ExecutiveNextStepDeepDive:
		return "深入了解"
	case types.ExecutiveNextStepMonitor:
		return "继续监控"
	case types.ExecutiveNextStepEditTask:
		return "调整监控"
	default:
		return "创建监控"
	}
}

func briefFollowupCompletenessLabelV1(
	value types.RunCompletenessV1,
) string {
	if value == types.RunCompletenessComplete {
		return "完整"
	}
	return "不完整"
}

func projectBriefFollowupEvidenceRefsV1(
	briefs []store.GroundedEvidenceBriefV1,
	refs []types.ExecutiveEvidenceRefV1,
) ([]briefFollowupEvidenceRefV1, error) {
	out := make([]briefFollowupEvidenceRefV1, 0, len(refs))
	for _, ref := range refs {
		briefIndex := -1
		insightIndex := -1
		for candidateBriefIndex, brief := range briefs {
			if ref.BriefID > 0 && brief.BriefID != ref.BriefID {
				continue
			}
			for candidateInsightIndex, insight := range brief.Insights {
				if insight.ID == ref.InsightID {
					briefIndex = candidateBriefIndex
					insightIndex = candidateInsightIndex
					break
				}
			}
			if insightIndex >= 0 {
				break
			}
		}
		if briefIndex < 0 || insightIndex < 0 {
			return nil, errors.New(
				"grounded evidence reference could not be projected")
		}
		insight := briefs[briefIndex].Insights[insightIndex]
		if insight.Structured == nil {
			return nil, errors.New(
				"grounded evidence reference has no verified claims")
		}
		projected := briefFollowupEvidenceRefV1{
			BriefSequence: briefIndex + 1, InsightSequence: insightIndex + 1,
			ClaimSequences: make([]int, len(ref.ClaimIndexes)),
		}
		for index, claimIndex := range ref.ClaimIndexes {
			if claimIndex < 0 ||
				claimIndex >= len(insight.Structured.Claims) {
				return nil, errors.New(
					"grounded evidence claim could not be projected")
			}
			projected.ClaimSequences[index] = claimIndex + 1
		}
		out = append(out, projected)
	}
	return out, nil
}

func renderBriefFollowupGroundingV1(
	contextValue store.GroundedBriefContextV1,
) (string, error) {
	kind := "执行简报"
	if contextValue.Kind == store.GroundedBriefReport {
		kind = "周期报告"
	}
	cadence := ""
	switch contextValue.Cadence {
	case "daily":
		cadence = "日报"
	case "weekly":
		cadence = "周报"
	case "monthly":
		cadence = "月报"
	}
	content := briefFollowupContentV1{
		Headline: briefFollowupSafeTextV1(
			contextValue.Content.Headline),
		ExecutiveSummary: briefFollowupSafeTextV1(
			contextValue.Content.ExecutiveSummary),
		DecisionState: briefFollowupDecisionLabelV1(
			contextValue.Content.DecisionState),
		WhyForYou: briefFollowupSafeTextV1(
			contextValue.Content.WhyForYou),
		Signals:   make([]briefFollowupSignalV1, 0, len(contextValue.Content.Signals)),
		NextSteps: make([]briefFollowupNextStepV1, 0, len(contextValue.Content.NextSteps)),
	}
	for _, signal := range contextValue.Content.Signals {
		evidence, err := projectBriefFollowupEvidenceRefsV1(
			contextValue.Evidence, signal.EvidenceRefs)
		if err != nil {
			return "", err
		}
		content.Signals = append(content.Signals, briefFollowupSignalV1{
			Kind:      briefFollowupSignalKindLabelV1(signal.Kind),
			Lifecycle: briefFollowupSignalLifecycleLabelV1(signal.Lifecycle),
			Title:     briefFollowupSafeTextV1(signal.Title),
			Summary:   briefFollowupSafeTextV1(signal.Summary),
			Evidence:  evidence,
		})
	}
	for _, step := range contextValue.Content.NextSteps {
		evidence, err := projectBriefFollowupEvidenceRefsV1(
			contextValue.Evidence, step.EvidenceRefs)
		if err != nil {
			return "", err
		}
		content.NextSteps = append(content.NextSteps, briefFollowupNextStepV1{
			Kind:      briefFollowupNextStepLabelV1(step.Kind),
			Label:     briefFollowupSafeTextV1(step.Label),
			Rationale: briefFollowupSafeTextV1(step.Rationale),
			Evidence:  evidence,
		})
	}
	grounding := briefFollowupGroundingV1{
		AnswerRules: []string{
			"只依据本材料回答；证据不足时明确说明。",
			"不得输出数据库编号、引用标签、字段名、摘要指纹或其他内部校验信息。",
			"说明依据时使用情报标题和来源名称，不逐字复述证据摘录。",
		},
		Kind:           kind,
		Cadence:        cadence,
		PeriodStart:    contextValue.PeriodStart,
		PeriodEnd:      contextValue.PeriodEnd,
		SourceCoverage: briefFollowupCompletenessLabelV1(contextValue.SourceCoverage),
		Processing:     briefFollowupCompletenessLabelV1(contextValue.Processing),
		Content:        content,
		Evidence: make(
			[]briefFollowupEvidenceV1, 0, len(contextValue.Evidence)),
	}
	for index, brief := range contextValue.Evidence {
		projected := briefFollowupEvidenceV1{
			Sequence: index + 1,
			GeneratedAt: brief.GeneratedAt.Format(
				"2006-01-02 15:04 MST"),
			Insights: make(
				[]briefFollowupInsightV1, 0, len(brief.Insights)),
		}
		for _, insight := range brief.Insights {
			item := briefFollowupInsightV1{
				Title:       briefFollowupSafeTextV1(insight.Title),
				Summary:     briefFollowupSafeTextV1(insight.BodyMD),
				SourceTitle: briefFollowupSafeTextV1(insight.SourceTitle),
				SourceURL:   briefFollowupSafeSourceURLV1(insight.SourceURL),
			}
			if insight.PublishedAt != nil {
				item.PublishedAt =
					insight.PublishedAt.Format("2006-01-02 15:04 MST")
			}
			if insight.Structured != nil {
				structured := &briefFollowupStructuredInsightV1{
					WhatChanged: briefFollowupSafeTextV1(
						insight.Structured.WhatChanged),
					WhyItMatters: briefFollowupSafeTextV1(
						insight.Structured.WhyItMatters),
					ImportanceReason: briefFollowupSafeTextV1(
						insight.Structured.ImportanceReason),
					Claims: make(
						[]briefFollowupClaimV1, 0,
						len(insight.Structured.Claims)),
				}
				for _, claim := range insight.Structured.Claims {
					structured.Claims = append(
						structured.Claims,
						briefFollowupClaimV1{
							Text: briefFollowupSafeTextV1(
								claim.Text),
							Excerpt: briefFollowupSafeTextV1(
								claim.Excerpt),
						},
					)
				}
				item.Structured = structured
			}
			projected.Insights = append(projected.Insights, item)
		}
		grounding.Evidence = append(grounding.Evidence, projected)
	}
	raw, err := json.Marshal(grounding)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

func guardBriefFollowupReplyV1(
	reply string,
) (string, error) {
	if groundedInternalReferenceV1.MatchString(reply) {
		return groundedSafeFallbackReplyV1, nil
	}
	return reply, nil
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
	grounding, err := renderBriefFollowupGroundingV1(contextValue)
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
	outcome, err := grounded.HandleGroundedMessageGuarded(
		r.Context(), principal.UserID, request.Question, grounding,
		guardBriefFollowupReplyV1)
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
