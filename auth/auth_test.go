package auth

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/YouToco/vane/types"
)

const testKey = "feishu_owner"

// fakeOwnerStore 是 OwnerStore 的内存替身，可注入各环节失败。
type fakeOwnerStore struct {
	raw        json.RawMessage
	settingErr error
	upsertErr  error
	userID     int64

	upsertOpenID string // 留痕：断言传给 upsert 的入参
	upsertName   string
	upsertCalls  int
}

func (f *fakeOwnerStore) GetSetting(context.Context, string) (json.RawMessage, error) {
	if f.settingErr != nil {
		return nil, f.settingErr
	}
	if f.raw == nil {
		return json.RawMessage(`{"open_id":"ou_owner","name":"Boss"}`), nil
	}
	return f.raw, nil
}

func (f *fakeOwnerStore) UpsertUserByOpenID(_ context.Context, openID, name string) (*types.User, error) {
	f.upsertCalls++
	f.upsertOpenID, f.upsertName = openID, name
	if f.upsertErr != nil {
		return nil, f.upsertErr
	}
	return &types.User{ID: f.userID, FeishuOpenID: &openID, Name: name}, nil
}

func TestFromContext_Success(t *testing.T) {
	st := &fakeOwnerStore{userID: 42}
	p, err := NewOwnerResolver(st, testKey).FromContext(context.Background())
	if err != nil {
		t.Fatalf("意外报错: %v", err)
	}
	if p.UserID != 42 {
		t.Errorf("UserID = %d, 期望 42", p.UserID)
	}
	// 过渡期不变量：租户恒为 SingleTenantID（多租户落地前不得出现别的值）。
	if p.TenantID != types.SingleTenantID {
		t.Errorf("TenantID = %d, 期望 SingleTenantID(%d)", p.TenantID, types.SingleTenantID)
	}
	// 收敛前的行为：把 record 里的 name 透传给 upsert（避免把已有昵称覆盖成空）。
	if st.upsertOpenID != "ou_owner" || st.upsertName != "Boss" {
		t.Errorf("upsert 入参 = (%q, %q), 期望 (ou_owner, Boss)", st.upsertOpenID, st.upsertName)
	}
}

// TestFromContext_NoOwner 锁住「尚无 owner 是流程未走到而非故障」这条语义：
// 必须是 CodeConflict（api 层据此回 409、a2a 据此给可行动文案、gate 据此换 -user 提示），
// 且**不得调用 upsert**。
func TestFromContext_NoOwner(t *testing.T) {
	st := &fakeOwnerStore{settingErr: types.NewAppError(types.CodeNotFound, "no row", types.ErrNotFound)}
	_, err := NewOwnerResolver(st, testKey).FromContext(context.Background())
	assertCode(t, err, types.CodeConflict)
	if !strings.Contains(errMsg(err), "尚未捕获 owner") {
		t.Errorf("文案与收敛前不一致: %q", errMsg(err))
	}
	if st.upsertCalls != 0 {
		t.Errorf("无 owner 时不应写库，实际 upsert %d 次", st.upsertCalls)
	}
}

func TestFromContext_MalformedRecord(t *testing.T) {
	cases := []struct {
		name    string
		raw     string
		code    types.ErrCode
		wantMsg string
	}{
		{"非法 JSON", `{broken`, types.CodeInternal, "owner 设置格式异常"},
		{"缺 open_id", `{"name":"Boss"}`, types.CodeConflict, "缺少 open_id"},
		{"open_id 空串", `{"open_id":"","name":"Boss"}`, types.CodeConflict, "缺少 open_id"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			st := &fakeOwnerStore{raw: json.RawMessage(c.raw)}
			_, err := NewOwnerResolver(st, testKey).FromContext(context.Background())
			assertCode(t, err, c.code)
			if !strings.Contains(errMsg(err), c.wantMsg) {
				t.Errorf("文案 = %q, 期望含 %q", errMsg(err), c.wantMsg)
			}
			if st.upsertCalls != 0 {
				t.Errorf("记录异常时不应写库，实际 upsert %d 次", st.upsertCalls)
			}
		})
	}
}

// TestFromContext_UpsertErrorPassthrough：底层写库错误原样上抛，不被吞成 Conflict。
// 这条对 a2a 尤其重要——它按错误码决定对外文案，误判成 Conflict 会给出错误的可行动提示。
func TestFromContext_UpsertErrorPassthrough(t *testing.T) {
	inner := types.NewAppError(types.CodeDatabase, "upsert 用户（open_id=ou_secret）", errors.New("pgx"))
	st := &fakeOwnerStore{upsertErr: inner}
	_, err := NewOwnerResolver(st, testKey).FromContext(context.Background())
	if !errors.Is(err, inner) {
		t.Fatalf("底层错误应原样透传，实得 %v", err)
	}
	assertCode(t, err, types.CodeDatabase)
}

// TestFromContext_SettingErrorPassthrough：非 NotFound 的读设置失败不得被误判为
// 「尚无 owner」——那会把一次数据库故障说成「请先发消息」，误导用户。
func TestFromContext_SettingErrorPassthrough(t *testing.T) {
	inner := types.NewAppError(types.CodeDatabase, "连接池耗尽", errors.New("pgx"))
	st := &fakeOwnerStore{settingErr: inner}
	_, err := NewOwnerResolver(st, testKey).FromContext(context.Background())
	if !errors.Is(err, inner) {
		t.Fatalf("非 NotFound 的读取失败应原样透传，实得 %v", err)
	}
	assertCode(t, err, types.CodeDatabase)
}

// TestSettingKeyIsInjected 锁住 I-A4 的一半：auth 包不 import feishu，
// 键名由装配方传入。改错键名应导致读不到（而不是包内硬编码兜住）。
func TestSettingKeyIsInjected(t *testing.T) {
	var gotKey string
	st := &keyCapturingStore{onGet: func(k string) { gotKey = k }}
	_, _ = NewOwnerResolver(st, "custom_key").FromContext(context.Background())
	if gotKey != "custom_key" {
		t.Errorf("传入的 settingKey 未被使用: %q", gotKey)
	}
}

type keyCapturingStore struct{ onGet func(string) }

func (k *keyCapturingStore) GetSetting(_ context.Context, key string) (json.RawMessage, error) {
	k.onGet(key)
	return json.RawMessage(`{"open_id":"ou_x","name":"n"}`), nil
}
func (k *keyCapturingStore) UpsertUserByOpenID(_ context.Context, openID, name string) (*types.User, error) {
	return &types.User{ID: 1, FeishuOpenID: &openID, Name: name}, nil
}

func assertCode(t *testing.T, err error, want types.ErrCode) {
	t.Helper()
	var ae *types.AppError
	if !errors.As(err, &ae) {
		t.Fatalf("期望 AppError，实得 %T: %v", err, err)
	}
	if ae.Code != want {
		t.Errorf("Code = %s, 期望 %s", ae.Code, want)
	}
}

func errMsg(err error) string {
	var ae *types.AppError
	if errors.As(err, &ae) {
		return ae.Message
	}
	return err.Error()
}
