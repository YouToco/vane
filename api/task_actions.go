package api

import (
	"bytes"
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
)

const maxWebTaskActionTextBytes = 8 << 10

type executeTaskActionRequest struct {
	RequestID string `json:"request_id"`
	Text      string `json:"text"`
	TaskID    string `json:"task_id,omitempty"`
}

type executeTaskActionResponse struct {
	Message string `json:"message"`
}

// handleExecuteTaskAction is the Web's single agentic task mutation boundary.
// Natural-language intent is executed directly through the same durable
// creation/edit coordinators as chat. There is no proposal preview, action ID,
// confirmation callback, cancellation step, or polling resource.
func (s *server) handleExecuteTaskAction(w http.ResponseWriter, r *http.Request) {
	if s.deps.TaskAgent == nil {
		writeError(w, http.StatusServiceUnavailable, "任务控制面尚未就绪")
		return
	}
	var req executeTaskActionRequest
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
	if !validWebTaskActionRequestID(req.RequestID, mode, req.TaskID, req.Text) {
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
	operationID := webTaskActionID(
		int64(principal.TenantID), userID, mode, req.TaskID, req.RequestID,
	)
	if !s.beginTaskActionExecution(r.Context(), userID, time.Now()) {
		writeError(
			w,
			http.StatusTooManyRequests,
			"任务操作过于频繁，请等待当前请求完成后再试",
		)
		return
	}
	defer s.finishTaskActionExecution(userID)

	outcome, err := s.deps.TaskAgent.HandleWebTaskActionMessage(
		r.Context(), userID, operationID, req.TaskID, req.Text,
	)
	if err != nil {
		writeAppError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, executeTaskActionResponse{
		Message: outcome.Reply,
	})
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
			"vane/web-task-action/v2\n"+
				strconv.FormatInt(tenantID, 10)+"\n"+
				strconv.FormatInt(userID, 10)+"\n"+
				mode+"\n"+taskID+"\n"+requestID,
		),
	).String()
}

func (s *server) beginTaskActionExecution(
	ctx interface{ Err() error },
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

func (s *server) finishTaskActionExecution(userID int64) {
	s.taskActionMu.Lock()
	delete(s.taskActionActive, userID)
	s.taskActionMu.Unlock()
}

func decodeWebTaskActionRequest(
	w http.ResponseWriter,
	r *http.Request,
	dst *executeTaskActionRequest,
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
