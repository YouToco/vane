package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/YouToco/vane/task"
	"github.com/YouToco/vane/types"
)

const maxWebTaskActionTextBytes = 8 << 10

type proposeTaskActionReq struct {
	Text   string `json:"text"`
	TaskID string `json:"task_id,omitempty"`
}

type taskActionPreview struct {
	ID      string `json:"id"`
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
	if req.Text == "" {
		writeError(w, http.StatusBadRequest, "请描述任务需求")
		return
	}
	if len(req.Text) > maxWebTaskActionTextBytes ||
		len(req.TaskID) > 255 {
		writeError(w, http.StatusBadRequest, "任务描述过长")
		return
	}
	userID, err := s.ownerUserID(r.Context())
	if err != nil {
		writeAppError(w, err)
		return
	}

	message := "确认创建，直接生成确认卡，不要再次搜索。任务需求：" + req.Text
	if req.TaskID != "" {
		if _, err := s.deps.Store.GetSchedule(
			r.Context(), req.TaskID, userID,
		); err != nil {
			writeAppError(w, err)
			return
		}
		message = fmt.Sprintf(
			"请为我编辑任务 id=%s，只生成需要人工确认的编辑方案，不要直接执行。变更要求：%s",
			req.TaskID, req.Text,
		)
	}
	outcome, err := s.deps.TaskAgent.HandleMessage(
		r.Context(), userID, message,
	)
	if err != nil {
		writeAppError(w, err)
		return
	}
	resp := proposeTaskActionResp{Reply: outcome.Reply}
	if outcome.Confirm != nil {
		resp.Action = &taskActionPreview{
			ID: outcome.Confirm.ActionID, Summary: outcome.Confirm.Summary,
		}
	}
	writeJSON(w, http.StatusOK, resp)
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
	creation, creationErr := s.deps.Store.LoadTaskCreationOperationByUser(
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
	edit, editErr := s.deps.Store.LoadTaskDefinitionEditOperationByActor(
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
