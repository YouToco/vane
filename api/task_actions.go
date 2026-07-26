package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/YouToco/vane/agent"
	"github.com/YouToco/vane/task"
	"github.com/YouToco/vane/types"
)

const maxWebTaskActionTextBytes = 8 << 10

type proposeTaskActionReq struct {
	RequestID string `json:"request_id"`
	Text      string `json:"text"`
	TaskID    string `json:"task_id,omitempty"`
}

type taskActionPreview struct {
	ID      string `json:"id"`
	Kind    string `json:"kind"`
	TaskID  string `json:"task_id,omitempty"`
	Summary string `json:"summary"`
}

type proposeTaskActionResp struct {
	Reply  string             `json:"reply"`
	Action *taskActionPreview `json:"action,omitempty"`
}

type taskActionMutationResp struct {
	Message string `json:"message"`
}

type taskActionStatusResp struct {
	ID         string `json:"id"`
	Kind       string `json:"kind"`
	Status     string `json:"status"`
	Terminal   bool   `json:"terminal"`
	TaskID     string `json:"task_id,omitempty"`
	Summary    string `json:"summary,omitempty"`
	Message    string `json:"message,omitempty"`
	Recovering bool   `json:"recovering,omitempty"`
}

func (s *server) handleProposeTaskAction(w http.ResponseWriter, r *http.Request) {
	if s.deps.TaskAgent == nil {
		writeError(w, http.StatusServiceUnavailable, "任务控制面尚未就绪")
		return
	}
	var req proposeTaskActionReq
	if err := decodeWebTaskActionRequest(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	req.Text = strings.TrimSpace(req.Text)
	req.TaskID = strings.TrimSpace(req.TaskID)
	req.RequestID = strings.TrimSpace(req.RequestID)
	if req.Text == "" {
		writeError(w, http.StatusBadRequest, "请描述任务需求")
		return
	}
	if len(req.Text) > maxWebTaskActionTextBytes ||
		len(req.TaskID) > 255 || len(req.RequestID) > 128 {
		writeError(w, http.StatusBadRequest, "任务描述过长")
		return
	}
	mode := "create"
	if req.TaskID != "" {
		mode = "edit"
	}
	if !validWebTaskActionRequestID(
		req.RequestID, mode, req.TaskID, req.Text,
	) {
		writeError(
			w,
			http.StatusConflict,
			"request_id 与本次任务请求不匹配，请生成新的请求标识",
		)
		return
	}
	principal, err := s.deps.Principal.FromContext(r.Context())
	if err != nil {
		writeAppError(w, err)
		return
	}
	userID := principal.UserID
	actionID := webTaskActionID(
		int64(principal.TenantID), userID, mode, req.TaskID, req.RequestID,
	)
	actionStore := s.taskActionStore()
	if actionStore == nil {
		writeError(w, http.StatusServiceUnavailable, "任务控制面尚未就绪")
		return
	}

	message := "确认创建，直接生成确认卡，不要再次搜索。任务需求：" + req.Text
	if req.TaskID != "" {
		if !s.deps.DefinitionEditEnabled {
			writeError(w, http.StatusServiceUnavailable, "任务编辑能力尚未开启")
			return
		}
		if _, err := actionStore.GetSchedule(
			r.Context(), req.TaskID, userID,
		); err != nil {
			writeAppError(w, err)
			return
		}
	}
	if replayed, replayErr := s.replayWebTaskAction(
		w, r, actionStore, userID, actionID, mode, req.TaskID,
	); replayErr != nil || replayed {
		if replayErr != nil {
			writeAppError(w, replayErr)
		}
		return
	}
	if !s.beginTaskActionProposal(r.Context(), userID, time.Now()) {
		writeError(
			w,
			http.StatusTooManyRequests,
			"任务方案生成过于频繁，请等待当前请求完成后再试",
		)
		return
	}
	defer s.finishTaskActionProposal(userID)

	var outcome agent.Outcome
	if req.TaskID == "" {
		outcome, err = s.deps.TaskAgent.HandleTaskCreationMessage(
			r.Context(), userID, actionID, message,
		)
	} else {
		outcome, err = s.deps.TaskAgent.HandleTaskDefinitionEditMessage(
			r.Context(), userID, actionID, req.TaskID, req.Text,
		)
	}
	if err != nil {
		writeAppError(w, err)
		return
	}
	resp := proposeTaskActionResp{Reply: outcome.Reply}
	if outcome.Confirm != nil {
		kind := "create"
		taskID := ""
		if req.TaskID != "" {
			edit, loadErr := actionStore.LoadTaskDefinitionEditOperationByActor(
				r.Context(), outcome.Confirm.ActionID, userID,
			)
			if loadErr != nil {
				writeAppError(w, loadErr)
				return
			}
			if edit == nil || edit.ID != outcome.Confirm.ActionID ||
				edit.UserID != userID || edit.TaskID != req.TaskID {
				writeError(
					w,
					http.StatusInternalServerError,
					"任务编辑方案身份校验失败，请重新发起",
				)
				return
			}
			kind = "edit"
			taskID = edit.TaskID
		}
		resp.Action = &taskActionPreview{
			ID: outcome.Confirm.ActionID, Kind: kind, TaskID: taskID,
			Summary: outcome.Confirm.Summary,
		}
	}
	writeJSON(w, http.StatusOK, resp)
}

func validWebTaskActionRequestID(
	requestID string,
	mode string,
	taskID string,
	text string,
) bool {
	prefix, digest, ok := strings.Cut(requestID, ".")
	if !ok || strings.Contains(digest, ".") {
		return false
	}
	parsed, err := uuid.Parse(prefix)
	if err != nil || parsed.String() != prefix {
		return false
	}
	expected, ok := webTaskActionPayloadDigest(mode, taskID, text)
	return ok && digest == expected
}

func webTaskActionPayloadDigest(
	mode string,
	taskID string,
	text string,
) (string, bool) {
	var canonical bytes.Buffer
	enc := json.NewEncoder(&canonical)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(struct {
		Mode   string `json:"mode"`
		TaskID string `json:"task_id"`
		Text   string `json:"text"`
	}{
		Mode: mode, TaskID: taskID, Text: text,
	}); err != nil {
		return "", false
	}
	payload := bytes.TrimSuffix(canonical.Bytes(), []byte{'\n'})
	sum := sha256.Sum256(payload)
	return fmt.Sprintf("%x", sum[:]), true
}

func webTaskActionID(
	tenantID int64,
	userID int64,
	mode string,
	taskID string,
	requestID string,
) string {
	return uuid.NewSHA1(
		uuid.NameSpaceURL,
		[]byte(
			"vane/web-task-action/v1\n"+
				strconv.FormatInt(tenantID, 10)+"\n"+
				strconv.FormatInt(userID, 10)+"\n"+
				mode+"\n"+taskID+"\n"+requestID,
		),
	).String()
}

func (s *server) replayWebTaskAction(
	w http.ResponseWriter,
	r *http.Request,
	actionStore TaskActionStore,
	userID int64,
	actionID string,
	mode string,
	taskID string,
) (bool, error) {
	if mode == "create" {
		op, err := actionStore.LoadTaskCreationOperationByUser(
			r.Context(), actionID, userID,
		)
		if err != nil {
			if errors.Is(err, types.ErrNotFound) {
				return false, nil
			}
			return false, err
		}
		if op == nil || op.ID != actionID || op.UserID != userID {
			return false, types.NewAppError(
				types.CodeConflict,
				"已保存的任务方案身份不一致",
				types.ErrConflict,
			)
		}
		writeJSON(w, http.StatusOK, proposeTaskActionResp{
			Reply: "已恢复此前生成的任务创建方案，请继续确认。",
			Action: &taskActionPreview{
				ID: op.ID, Kind: "create", Summary: op.Summary,
			},
		})
		return true, nil
	}
	op, err := actionStore.LoadTaskDefinitionEditOperationByActor(
		r.Context(), actionID, userID,
	)
	if err != nil {
		if errors.Is(err, types.ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	if op == nil || op.ID != actionID || op.UserID != userID ||
		op.TaskID != taskID {
		return false, types.NewAppError(
			types.CodeConflict,
			"已保存的任务编辑方案身份不一致",
			types.ErrConflict,
		)
	}
	writeJSON(w, http.StatusOK, proposeTaskActionResp{
		Reply: "已恢复此前生成的任务编辑方案，请继续确认。",
		Action: &taskActionPreview{
			ID: op.ID, Kind: "edit", TaskID: op.TaskID,
			Summary: "编辑任务 " + op.TaskID,
		},
	})
	return true, nil
}

func (s *server) beginTaskActionProposal(
	ctx context.Context,
	userID int64,
	now time.Time,
) bool {
	if err := ctx.Err(); err != nil {
		return false
	}
	s.taskActionMu.Lock()
	defer s.taskActionMu.Unlock()
	if _, active := s.taskActionActive[userID]; active {
		return false
	}
	if !s.taskActionLimiter.allowAndRecord(
		"task-action|"+strconv.FormatInt(userID, 10), now,
	) {
		return false
	}
	s.taskActionActive[userID] = struct{}{}
	return true
}

func (s *server) taskActionStore() TaskActionStore {
	if s.deps.TaskActions != nil {
		return s.deps.TaskActions
	}
	if s.deps.Store != nil {
		return s.deps.Store
	}
	return nil
}

func (s *server) finishTaskActionProposal(userID int64) {
	s.taskActionMu.Lock()
	delete(s.taskActionActive, userID)
	s.taskActionMu.Unlock()
}

func decodeWebTaskActionRequest(
	w http.ResponseWriter,
	r *http.Request,
	dst *proposeTaskActionReq,
) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxWebTaskActionTextBytes+1024)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return errors.New("请求体不是合法 JSON")
	}
	var extra any
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		return errors.New("请求体只能包含一个 JSON 对象")
	}
	return nil
}

func (s *server) handleConfirmTaskAction(w http.ResponseWriter, r *http.Request) {
	s.handleMutateTaskAction(w, r, false)
}

func (s *server) handleCancelTaskAction(w http.ResponseWriter, r *http.Request) {
	s.handleMutateTaskAction(w, r, true)
}

func (s *server) handleMutateTaskAction(
	w http.ResponseWriter,
	r *http.Request,
	cancel bool,
) {
	if s.deps.TaskAgent == nil {
		writeError(w, http.StatusServiceUnavailable, "任务控制面尚未就绪")
		return
	}
	actionID := strings.TrimSpace(r.PathValue("id"))
	if actionID == "" || len(actionID) > 512 {
		writeError(w, http.StatusBadRequest, "任务动作标识无效")
		return
	}
	userID, err := s.ownerUserID(r.Context())
	if err != nil {
		writeAppError(w, err)
		return
	}
	receipt := task.WebActionReceiptTarget(actionID)
	actionStore := s.taskActionStore()
	if actionStore == nil {
		writeError(w, http.StatusServiceUnavailable, "任务控制面尚未就绪")
		return
	}
	if err := requireDurableWebTaskAction(
		r.Context(), actionStore, actionID, userID,
	); err != nil {
		writeAppError(w, err)
		return
	}
	var message string
	if cancel {
		outcome, mutateErr := s.deps.TaskAgent.CancelActionWithReceipt(
			r.Context(), userID, actionID, receipt,
		)
		err = mutateErr
		message = outcome.Text
	} else {
		outcome, mutateErr := s.deps.TaskAgent.ExecuteActionWithReceipt(
			r.Context(), userID, actionID, receipt,
		)
		err = mutateErr
		message = outcome.Text
	}
	if err != nil {
		writeAppError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, taskActionMutationResp{
		Message: message,
	})
}

func requireDurableWebTaskAction(
	ctx context.Context,
	actionStore TaskActionStore,
	actionID string,
	userID int64,
) error {
	creation, creationErr := actionStore.LoadTaskCreationOperationByUser(
		ctx, actionID, userID,
	)
	if creationErr == nil {
		if creation == nil || creation.ID != actionID ||
			creation.UserID != userID {
			return types.NewAppError(
				types.CodeConflict,
				"任务动作身份校验失败",
				types.ErrConflict,
			)
		}
		return nil
	}
	if !errors.Is(creationErr, types.ErrNotFound) {
		return creationErr
	}
	edit, editErr := actionStore.LoadTaskDefinitionEditOperationByActor(
		ctx, actionID, userID,
	)
	if editErr == nil {
		if edit == nil || edit.ID != actionID || edit.UserID != userID ||
			strings.TrimSpace(edit.TaskID) == "" {
			return types.NewAppError(
				types.CodeConflict,
				"任务动作身份校验失败",
				types.ErrConflict,
			)
		}
		return nil
	}
	if errors.Is(editErr, types.ErrNotFound) {
		return types.NewAppError(
			types.CodeNotFound,
			"任务动作不存在",
			types.ErrNotFound,
		)
	}
	return editErr
}

func (s *server) handleGetTaskAction(w http.ResponseWriter, r *http.Request) {
	actionID := strings.TrimSpace(r.PathValue("id"))
	if actionID == "" || len(actionID) > 512 {
		writeError(w, http.StatusBadRequest, "任务动作标识无效")
		return
	}
	userID, err := s.ownerUserID(r.Context())
	if err != nil {
		writeAppError(w, err)
		return
	}
	actionStore := s.taskActionStore()
	if actionStore == nil {
		writeError(w, http.StatusServiceUnavailable, "任务控制面尚未就绪")
		return
	}
	creation, creationErr := actionStore.LoadTaskCreationOperationByUser(
		r.Context(), actionID, userID,
	)
	if creationErr == nil {
		writeJSON(w, http.StatusOK, creationActionStatus(creation))
		return
	}
	if !errors.Is(creationErr, types.ErrNotFound) {
		writeAppError(w, creationErr)
		return
	}
	edit, editErr := actionStore.LoadTaskDefinitionEditOperationByActor(
		r.Context(), actionID, userID,
	)
	if editErr != nil {
		if errors.Is(editErr, types.ErrNotFound) {
			writeError(w, http.StatusNotFound, "任务动作不存在")
			return
		}
		writeAppError(w, editErr)
		return
	}
	writeJSON(w, http.StatusOK, definitionEditActionStatus(edit))
}

func creationActionStatus(
	op *types.TaskCreationOperation,
) taskActionStatusResp {
	resp := taskActionStatusResp{
		ID: op.ID, Kind: "create", Status: string(op.Status),
		TaskID: op.TaskID, Summary: op.Summary,
		Recovering: op.Status == types.PendingActionStatusExecuting,
	}
	switch op.Status {
	case types.PendingActionStatusExecuted,
		types.PendingActionStatusCancelled,
		types.PendingActionStatusExpired,
		types.PendingActionStatusBlocked,
		types.PendingActionStatusFailed:
		resp.Terminal = true
	}
	switch op.Status {
	case types.PendingActionStatusExecuted:
		resp.Message = "任务已创建并开始监控。"
	case types.PendingActionStatusCancelled:
		resp.Message = "已取消，本次任务不会创建。"
	case types.PendingActionStatusExpired:
		resp.Message = "确认已过期，请重新生成方案。"
	case types.PendingActionStatusBlocked, types.PendingActionStatusFailed:
		resp.Message = strings.TrimSpace(op.ErrorMessage)
		if resp.Message == "" {
			resp.Message = "任务创建已安全停止。"
		}
	case types.PendingActionStatusExecuting:
		resp.Message = "任务正在可靠创建，无需重复确认。"
	default:
		resp.Message = "方案等待确认。"
	}
	return resp
}

func definitionEditActionStatus(
	op *types.TaskDefinitionEditOperation,
) taskActionStatusResp {
	resp := taskActionStatusResp{
		ID: op.ID, Kind: "edit", Status: string(op.Status),
		TaskID:     op.TaskID,
		Recovering: op.Status == types.TaskDefinitionEditOperationStatusExecuting,
	}
	switch op.Status {
	case types.TaskDefinitionEditOperationStatusCompleted,
		types.TaskDefinitionEditOperationStatusCancelled,
		types.TaskDefinitionEditOperationStatusExpired,
		types.TaskDefinitionEditOperationStatusBlocked,
		types.TaskDefinitionEditOperationStatusSuperseded:
		resp.Terminal = true
	}
	switch op.Status {
	case types.TaskDefinitionEditOperationStatusCompleted:
		resp.Message = "任务编辑已完成。"
	case types.TaskDefinitionEditOperationStatusCancelled:
		resp.Message = "已取消，本次编辑不会执行。"
	case types.TaskDefinitionEditOperationStatusExpired:
		resp.Message = "编辑确认已过期，请重新生成方案。"
	case types.TaskDefinitionEditOperationStatusSuperseded:
		resp.Message = "任务已发生更新，这份旧方案未执行。"
	case types.TaskDefinitionEditOperationStatusBlocked:
		resp.Message = "任务编辑因安全检查未通过而停止。"
	case types.TaskDefinitionEditOperationStatusExecuting:
		resp.Message = "任务编辑正在可靠执行，无需重复确认。"
	default:
		resp.Message = "编辑方案等待确认。"
	}
	return resp
}
