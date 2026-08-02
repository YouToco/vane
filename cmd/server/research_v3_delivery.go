package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/YouToco/vane/feishu"
	"github.com/YouToco/vane/types"
	"github.com/YouToco/vane/workflow"
)

type researchV3UserTargetStore interface {
	GetUserFeishuOpenID(context.Context, int64) (string, error)
}

type researchV3ProviderTargetSnapshot interface {
	PushEffectTarget() (ownerOpenID, ownerChatID, appIdentity string)
}

func newResearchV3DeliveryTargetResolver(
	st researchV3UserTargetStore,
	provider researchV3ProviderTargetSnapshot,
) workflow.ResearchDeliveryTargetResolverV3 {
	return func(ctx context.Context, identity types.RunIdentity) (
		workflow.ResearchDeliveryTargetV3, error,
	) {
		if st == nil || provider == nil || identity.TenantID <= 0 ||
			identity.UserID <= 0 || identity.TaskID == "" {
			return workflow.ResearchDeliveryTargetV3{}, types.NewAppError(
				types.CodeValidation, "research V3 投递身份无效", types.ErrValidation)
		}
		userOpenID, err := st.GetUserFeishuOpenID(ctx, identity.UserID)
		if err != nil {
			return workflow.ResearchDeliveryTargetV3{}, err
		}
		ownerOpenID, ownerChatID, appIdentity := provider.PushEffectTarget()
		if ownerOpenID == "" || ownerChatID == "" || appIdentity == "" {
			return workflow.ResearchDeliveryTargetV3{}, types.NewAppError(
				types.CodeConflict, "飞书 owner 投递路由尚未就绪", types.ErrConflict)
		}
		if userOpenID != ownerOpenID {
			return workflow.ResearchDeliveryTargetV3{}, types.NewAppError(
				types.CodeNotFound, "research V3 任务没有可用的飞书 owner 路由", types.ErrNotFound)
		}
		return workflow.ResearchDeliveryTargetV3{
			Provider: "feishu", AppIdentity: appIdentity,
			ProviderChatID: ownerChatID, Target: ownerOpenID,
		}, nil
	}
}

func renderResearchBriefCardV3(payload types.ResearchBriefPayloadV3) ([]byte, error) {
	if err := payload.Validate(); err != nil {
		return nil, err
	}
	label := "已核验更新"
	if payload.Significance == types.ResearchBriefSignificanceMajorV3 {
		label = "重大更新"
	}
	var body strings.Builder
	fmt.Fprintf(&body, "**%s｜%s**\n\n%s\n\n---\n**核验依据**", label,
		payload.Headline, payload.Summary)
	for _, citation := range payload.Citations {
		kind := "当前证据"
		if citation.Kind == types.ResearchBriefCitationHistoryV3 {
			kind = "历史对比"
		}
		fmt.Fprintf(&body, "\n- %s `%s`", kind, citation.Ref)
	}
	raw := []byte(feishu.BuildReplyCard(body.String()))
	if len(raw) == 0 || len(raw) > 64<<10 {
		return nil, types.NewAppError(
			types.CodeValidation, "research V3 飞书卡片超出安全大小", types.ErrValidation)
	}
	return raw, nil
}
