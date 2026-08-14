package main

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/YouToco/vane/server/types"
)

type researchV3UserTargetStoreFake struct {
	openID string
	err    error
}

func (f researchV3UserTargetStoreFake) GetUserFeishuOpenID(
	context.Context, int64,
) (string, error) {
	return f.openID, f.err
}

type researchV3TargetSnapshotFake struct {
	openID string
	chatID string
	appID  string
}

func (f researchV3TargetSnapshotFake) PushEffectTarget() (string, string, string) {
	return f.openID, f.chatID, f.appID
}

func TestResearchV3DeliveryTargetResolverBindsExactUserAndRoute(t *testing.T) {
	identity := types.RunIdentity{
		TenantID: 7, UserID: 42, TaskID: "task-v3",
		TemporalWorkflowID: "wf-v3", TemporalRunID: "run-v3",
	}
	resolver := newResearchV3DeliveryTargetResolver(
		researchV3UserTargetStoreFake{openID: "ou_owner"},
		researchV3TargetSnapshotFake{
			openID: "ou_owner", chatID: "oc_owner", appID: "cli_app",
		})
	target, err := resolver.ResearchDeliveryTargetV3(t.Context(), identity)
	if err != nil || target.Provider != "feishu" || target.Target != "ou_owner" ||
		target.ProviderChatID != "oc_owner" || target.AppIdentity != "cli_app" {
		t.Fatalf("target=%+v err=%v", target, err)
	}
}

func TestResearchV3DeliveryTargetResolverFailsClosed(t *testing.T) {
	identity := types.RunIdentity{TenantID: 7, UserID: 42, TaskID: "task-v3"}
	tests := []struct {
		name     string
		store    researchV3UserTargetStoreFake
		provider researchV3TargetSnapshotFake
		code     types.ErrCode
	}{
		{name: "cross user", store: researchV3UserTargetStoreFake{openID: "ou_other"},
			provider: researchV3TargetSnapshotFake{openID: "ou_owner", chatID: "oc_owner", appID: "cli_app"},
			code:     types.CodeNotFound},
		{name: "incomplete route", store: researchV3UserTargetStoreFake{openID: "ou_owner"},
			provider: researchV3TargetSnapshotFake{openID: "ou_owner", appID: "cli_app"},
			code:     types.CodeConflict},
		{name: "store error", store: researchV3UserTargetStoreFake{err: errors.New("db unavailable")},
			provider: researchV3TargetSnapshotFake{openID: "ou_owner", chatID: "oc_owner", appID: "cli_app"},
			code:     types.CodeInternal},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resolver := newResearchV3DeliveryTargetResolver(tc.store, tc.provider)
			_, err := resolver.ResearchDeliveryTargetV3(t.Context(), identity)
			if err == nil {
				t.Fatal("unsafe target was accepted")
			}
			if tc.code != types.CodeInternal && types.CodeOf(err) != tc.code {
				t.Fatalf("code=%s err=%v", types.CodeOf(err), err)
			}
		})
	}
}

func TestRenderResearchBriefCardV3IsProfessionalAndBounded(t *testing.T) {
	payload := types.ResearchBriefPayloadV3{
		SchemaVersion: types.ResearchBriefPayloadSchemaV3,
		Headline:      "Kimi 套餐开放购买", Summary: "官方页面与接口均显示可购买。",
		Significance: types.ResearchBriefSignificanceMajorV3,
		Citations: []types.ResearchBriefCitationV3{
			{Kind: types.ResearchBriefCitationCurrentEvidenceV3, Ref: "17"},
			{Kind: types.ResearchBriefCitationHistoryV3, Ref: "brief:12"},
		},
	}
	raw, err := renderResearchBriefCardV3(payload)
	if err != nil || len(raw) == 0 || len(raw) > 64<<10 {
		t.Fatalf("render bytes=%d err=%v", len(raw), err)
	}
	var card map[string]any
	if json.Unmarshal(raw, &card) != nil || card["schema"] != "2.0" {
		t.Fatalf("invalid card: %s", raw)
	}
	text := string(raw)
	for _, want := range []string{"重大更新", "Kimi 套餐开放购买", "当前证据", "历史对比"} {
		if !strings.Contains(text, want) {
			t.Fatalf("card missing %q: %s", want, text)
		}
	}
}
