package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/YouToco/vane/server/types"
)

func TestWriteAppErrorMapsForbiddenWithoutLeakingAuthorityDetails(t *testing.T) {
	recorder := httptest.NewRecorder()
	writeAppError(recorder, types.NewAppError(
		types.CodeForbidden, "当前角色不能执行该操作", types.ErrForbidden))
	if recorder.Code != http.StatusForbidden {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	if got := recorder.Body.String(); got != "{\"error\":\"当前角色不能执行该操作\"}\n" {
		t.Fatalf("body=%q", got)
	}
}
