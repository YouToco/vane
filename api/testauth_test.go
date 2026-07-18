package api

import (
	"net/http"
	"testing"
	"time"

	"github.com/YouToco/vane/auth"
	"github.com/YouToco/vane/types"
)

// 共享测试脚手架：D2′ 换掉共享密码后，需要鉴权的用例改用**真会话**。
//
// 改造前这些用例靠 newSessions(password).issue() 造一个 HMAC token——那套机制
// 已随共享密码一并删除。现在统一走「假 AuthStore + 预置会话」，与生产同一条
// 校验路径（cookie → 哈希 → 查会话 → 注入 principal），测试因此更贴近真实。

// authedDeps 返回带假 AuthStore 的 Deps 与一枚有效会话 cookie。
func authedDeps(t *testing.T, base Deps) (Deps, *http.Cookie) {
	t.Helper()
	fake := newFakeAuthStore()
	token, hash, err := auth.NewSessionToken()
	if err != nil {
		t.Fatal(err)
	}
	fake.sessions[string(hash)] = &types.Session{
		TokenHash: hash, UserID: 1, TenantID: 1,
		ExpiresAt: time.Now().Add(time.Hour),
	}
	base.Auth = fake
	if base.Principal == nil {
		base.Principal = auth.NewContextResolver()
	}
	return base, &http.Cookie{Name: sessionCookieName, Value: token}
}
