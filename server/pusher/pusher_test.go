package pusher

import (
	"context"
	"errors"
	"testing"

	"github.com/YouToco/vane/pusheffect"
	"github.com/YouToco/vane/types"
)

// fakeSender 记录入参并按预设返回，替身出 FeishuSender。
type fakeSender struct {
	gotOpenID   string
	gotCardJSON string
	retID       string
	retErr      error
	called      bool
	gotApp      string
	gotUUID     string
	observation pusheffect.ProviderObservation
}

type legacyOnlySender struct{}

func (legacyOnlySender) SendCard(
	context.Context,
	string,
	string,
) (string, error) {
	return "om_legacy", nil
}

func (f *fakeSender) SendCard(_ context.Context, openID, cardJSON string) (string, error) {
	f.called = true
	f.gotOpenID = openID
	f.gotCardJSON = cardJSON
	return f.retID, f.retErr
}

func (f *fakeSender) SendCardWithUUIDResult(
	_ context.Context,
	appIdentity, openID, cardJSON, messageUUID string,
) (pusheffect.ProviderObservation, error) {
	f.called = true
	f.gotApp = appIdentity
	f.gotOpenID = openID
	f.gotCardJSON = cardJSON
	f.gotUUID = messageUUID
	return f.observation, f.retErr
}

func TestPush_EmptyOpenID(t *testing.T) {
	fs := &fakeSender{}
	p := New(fs)

	id, err := p.Push(context.Background(), "", `{"a":1}`)
	if err == nil {
		t.Fatal("空 open_id 应返回错误")
	}
	if id != "" {
		t.Errorf("空 open_id 时不应有 message_id，得到 %q", id)
	}
	// 校验失败不可重试：确保调用方（activity）不会对空收件人做无意义重试。
	if !errors.Is(err, types.ErrValidation) {
		t.Errorf("应为校验错误，得到 %v", err)
	}
	if fs.called {
		t.Error("空 open_id 时不应触达 sender")
	}
}

func TestPush_Success(t *testing.T) {
	fs := &fakeSender{retID: "om_xxx"}
	p := New(fs)

	id, err := p.Push(context.Background(), "ou_owner", `{"card":"body"}`)
	if err != nil {
		t.Fatalf("不应报错：%v", err)
	}
	if id != "om_xxx" {
		t.Errorf("应回传 sender 的 message_id om_xxx，得到 %q", id)
	}
	if fs.gotOpenID != "ou_owner" || fs.gotCardJSON != `{"card":"body"}` {
		t.Errorf("入参未原样透传：openID=%q card=%q", fs.gotOpenID, fs.gotCardJSON)
	}
}

func TestPush_SenderErrorPropagates(t *testing.T) {
	sentinel := types.NewAppError(types.CodePushFailed, "飞书拒绝", nil)
	fs := &fakeSender{retErr: sentinel}
	p := New(fs)

	_, err := p.Push(context.Background(), "ou_owner", `{}`)
	if !errors.Is(err, types.ErrPush) {
		t.Errorf("sender 的推送错误应原样透传，得到 %v", err)
	}
}

func TestPushWithUUIDPreservesFrozenProviderIdentity(t *testing.T) {
	want := pusheffect.ProviderObservation{
		Disposition: pusheffect.AttemptSent,
		AppIdentity: "app-generation-7",
		ChatID:      "oc_owner",
		MessageID:   "om_sent",
	}
	fs := &fakeSender{observation: want}
	got, err := New(fs).PushWithUUID(
		t.Context(),
		"app-generation-7",
		"ou_owner",
		`{"card":"frozen"}`,
		"2f790ab2-0622-4df8-8f93-6079a3a0f94f",
	)
	if err != nil {
		t.Fatal(err)
	}
	if got != want ||
		fs.gotApp != "app-generation-7" ||
		fs.gotOpenID != "ou_owner" ||
		fs.gotCardJSON != `{"card":"frozen"}` ||
		fs.gotUUID != "2f790ab2-0622-4df8-8f93-6079a3a0f94f" {
		t.Fatalf("durable send drifted: got=%+v sender=%+v", got, fs)
	}
}

func TestPushWithUUIDRejectsLegacyOnlySender(t *testing.T) {
	got, err := New(legacyOnlySender{}).PushWithUUID(
		t.Context(), "app", "ou_owner", `{}`, "uuid",
	)
	if !errors.Is(err, types.ErrConflict) ||
		got.Disposition != pusheffect.AttemptDefiniteNotSent {
		t.Fatalf("legacy-only result=%+v err=%v", got, err)
	}
}
