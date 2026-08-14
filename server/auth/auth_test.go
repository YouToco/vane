package auth

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/YouToco/vane/server/types"
)

const testKey = "feishu_owner"

// fakeOwnerStore 是 OwnerStore 的内存替身，可注入各环节失败。
type fakeOwnerStore struct {
	raw        json.RawMessage
	settingErr error
	upsertErr  error
	userID     int64
	tenantID   int64 // 见 ListMembershipsByUser 的说明；0/负数有特殊含义

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

// tenantID 是替身返回的租户；0 表示「无归属」，负数表示「多个归属」（测响亮失败）。
func (f *fakeOwnerStore) ListMembershipsByUser(_ context.Context, userID int64) ([]types.Membership, error) {
	switch {
	case f.tenantID == 0:
		return nil, nil
	case f.tenantID < 0:
		return []types.Membership{{TenantID: 1, UserID: userID}, {TenantID: 2, UserID: userID}}, nil
	default:
		return []types.Membership{{TenantID: f.tenantID, UserID: userID}}, nil
	}
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
	st := &fakeOwnerStore{userID: 42, tenantID: 7}
	p, err := NewOwnerResolver(st, testKey).FromContext(context.Background())
	if err != nil {
		t.Fatalf("意外报错: %v", err)
	}
	if p.UserID != 42 {
		t.Errorf("UserID = %d, 期望 42", p.UserID)
	}
	// 租户取自 memberships（替身给的是 7），**不是**硬编码的 SingleTenantID。
	// 这条断言随「ownerResolver 改为真查租户」而更新：写死常量是个会静默变错的
	// 假设——owner 换到别的租户后，走本解析器的 a2a/gate 会去操作别人的数据。
	if p.TenantID != 7 {
		t.Errorf("TenantID = %d, 期望取自 memberships 的 7", p.TenantID)
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
			st := &fakeOwnerStore{raw: json.RawMessage(c.raw), tenantID: 1}
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
	st := &fakeOwnerStore{upsertErr: inner, tenantID: 1}
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
	st := &fakeOwnerStore{settingErr: inner, tenantID: 1}
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

func (k *keyCapturingStore) ListMembershipsByUser(_ context.Context, userID int64) ([]types.Membership, error) {
	return []types.Membership{{TenantID: 1, UserID: userID}}, nil
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

// TestFromContext_MultiTenantFailsLoudly 钉住「尚未支持选择租户」这件事在**所有**
// 路径上都拦住、不猜。
//
// 原实现把租户写死成 types.SingleTenantID——那是个会静默变错的假设：owner 若换到
// 别的租户，走本解析器的 a2a 与 gate 会继续以租户 1 的身份读写别人的数据，且无任何报错。
// 现在真查 memberships，多成员时响亮失败。
func TestFromContext_MultiTenantFailsLoudly(t *testing.T) {
	st := &fakeOwnerStore{userID: 42, tenantID: -1} // 负数 = 造出两条成员关系
	_, err := NewOwnerResolver(st, testKey).FromContext(context.Background())
	if err == nil {
		t.Fatal("owner 属于多个租户时必须报错，不得静默选一个")
	}
	assertCode(t, err, types.CodeConflict)
	if !strings.Contains(errMsg(err), "多个租户") {
		t.Errorf("错误文案应点明多租户，实得 %q", errMsg(err))
	}
}

// TestFromContext_NoTenantFailsLoudly：owner 无租户归属同样拦住。
func TestFromContext_NoTenantFailsLoudly(t *testing.T) {
	st := &fakeOwnerStore{userID: 42, tenantID: 0} // 0 = 无成员关系
	_, err := NewOwnerResolver(st, testKey).FromContext(context.Background())
	if err == nil {
		t.Fatal("owner 无租户归属时必须报错")
	}
	assertCode(t, err, types.CodeConflict)
}

// TestFromContext_TenantFromMemberships：租户来自 memberships 而非硬编码常量。
// 替身给的是 7，若实现仍写死 SingleTenantID(1) 本用例会红。
func TestFromContext_TenantFromMemberships(t *testing.T) {
	st := &fakeOwnerStore{userID: 42, tenantID: 7}
	p, err := NewOwnerResolver(st, testKey).FromContext(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if p.TenantID != 7 {
		t.Errorf("租户应取自 memberships(7)，实得 %d —— 疑似仍硬编码 SingleTenantID", p.TenantID)
	}
}
