package feishu

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"

	"github.com/YouToco/vane/pusheffect"
	"github.com/YouToco/vane/types"
)

const (
	pushHistoryPageSize = 50
	pushHistoryMaxPages = 10
)

// ResolvePushEffectMessage looks only for positive evidence in the exact
// frozen P2P chat, app identity, time window, message type and effect marker.
// An empty result is deliberately non-terminal.
func (m *Manager) ResolvePushEffectMessage(
	ctx context.Context,
	query pusheffect.HistoryQuery,
) (pusheffect.HistoryObservation, error) {
	if !validPushHistoryQuery(query) {
		return pusheffect.HistoryObservation{}, types.NewAppError(
			types.CodeValidation,
			"push effect history query is invalid",
			nil,
		)
	}
	client, _, ok := m.apiForExpectedApp(query.AppIdentity)
	if !ok {
		return pusheffect.HistoryObservation{}, types.NewAppError(
			types.CodeConflict,
			"飞书 App 身份与 push effect 历史查询不一致",
			nil,
		)
	}
	matches := make(map[string]struct{})
	pageToken := ""
	for page := 0; page < pushHistoryMaxPages; page++ {
		builder := larkim.NewListMessageReqBuilder().
			ContainerIdType("chat").
			ContainerId(query.ProviderChatID).
			StartTime(strconv.FormatInt(query.StartTime.Unix(), 10)).
			EndTime(strconv.FormatInt(query.EndTime.Unix(), 10)).
			SortType("ByCreateTimeAsc").
			PageSize(pushHistoryPageSize).
			CardMsgContentType("raw_card_content")
		if pageToken != "" {
			builder.PageToken(pageToken)
		}
		resp, err := client.Im.Message.List(ctx, builder.Build())
		if err != nil {
			return pusheffect.HistoryObservation{}, types.NewAppError(
				types.CodePushFailed,
				"飞书 push effect 历史核对失败",
				nil,
			)
		}
		if !resp.Success() || resp.Data == nil {
			return pusheffect.HistoryObservation{}, types.NewAppError(
				types.CodeConflict,
				"飞书 push effect 历史响应无效",
				nil,
			)
		}
		for _, item := range resp.Data.Items {
			if pushHistoryItemMatches(item, query) {
				matches[*item.MessageId] = struct{}{}
			}
		}
		if resp.Data.HasMore == nil || !*resp.Data.HasMore {
			return summarizePushHistoryMatches(matches), nil
		}
		if resp.Data.PageToken == nil || *resp.Data.PageToken == "" {
			return pusheffect.HistoryObservation{}, types.NewAppError(
				types.CodeConflict,
				"飞书 push effect 历史分页证据不完整",
				nil,
			)
		}
		pageToken = *resp.Data.PageToken
	}
	return pusheffect.HistoryObservation{}, types.NewAppError(
		types.CodeConflict,
		"飞书 push effect 历史超过核对上限",
		nil,
	)
}

func validPushHistoryQuery(query pusheffect.HistoryQuery) bool {
	return validStableMessageUUID(query.EffectID) &&
		validOwnerChatID(query.ProviderChatID) &&
		query.ProviderChatID != "" &&
		validPushHistoryIdentity(query.AppIdentity) &&
		!query.StartTime.IsZero() &&
		!query.EndTime.IsZero() &&
		query.EndTime.After(query.StartTime) &&
		query.EndTime.Sub(query.StartTime) <= 2*time.Hour
}

func validPushHistoryIdentity(identity string) bool {
	if identity == "" ||
		len(identity) > 512 ||
		!utf8.ValidString(identity) ||
		strings.TrimSpace(identity) != identity {
		return false
	}
	for _, r := range identity {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}

func pushHistoryItemMatches(
	item *larkim.Message,
	query pusheffect.HistoryQuery,
) bool {
	if item == nil || item.MessageId == nil || *item.MessageId == "" ||
		item.ChatId == nil || *item.ChatId != query.ProviderChatID ||
		item.MsgType == nil || *item.MsgType != "interactive" ||
		item.Sender == nil || item.Sender.Id == nil ||
		*item.Sender.Id != query.AppIdentity ||
		item.Sender.IdType == nil || *item.Sender.IdType != "app_id" ||
		item.Sender.SenderType == nil || *item.Sender.SenderType != "app" ||
		item.Body == nil || item.Body.Content == nil ||
		(item.Deleted != nil && *item.Deleted) {
		return false
	}
	return cardHasExactEffectMarker(*item.Body.Content, query.EffectID)
}

func cardHasExactEffectMarker(cardJSON, effectID string) bool {
	var value any
	if json.Unmarshal([]byte(cardJSON), &value) != nil {
		return false
	}
	markers := make([]string, 0)
	collectHistoryEffectMarkers(value, &markers)
	if len(markers) == 0 {
		return false
	}
	for _, marker := range markers {
		if marker != effectID {
			return false
		}
	}
	return true
}

func collectHistoryEffectMarkers(value any, markers *[]string) {
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			if key == "effect_id" {
				marker, ok := child.(string)
				if !ok {
					*markers = append(*markers, "")
				} else {
					*markers = append(*markers, marker)
				}
				continue
			}
			collectHistoryEffectMarkers(child, markers)
		}
	case []any:
		for _, child := range typed {
			collectHistoryEffectMarkers(child, markers)
		}
	}
}

func summarizePushHistoryMatches(
	matches map[string]struct{},
) pusheffect.HistoryObservation {
	observation := pusheffect.HistoryObservation{MatchCount: len(matches)}
	if len(matches) == 1 {
		for messageID := range matches {
			observation.MessageID = messageID
		}
	}
	return observation
}
