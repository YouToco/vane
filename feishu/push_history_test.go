package feishu

import (
	"testing"
	"time"

	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"

	"github.com/YouToco/vane/pusheffect"
)

func TestPushHistoryItemMatchesExactPositiveEvidence(t *testing.T) {
	const (
		effectID = "019f9824-39b6-7e13-b247-b5ee5713c52b"
		chatID   = "oc_owner_p2p"
		appID    = "cli_expected_app"
	)
	query := pusheffect.HistoryQuery{
		EffectID: effectID, ProviderChatID: chatID, AppIdentity: appID,
		StartTime: time.Date(2026, 7, 25, 0, 0, 0, 0, time.UTC),
		EndTime:   time.Date(2026, 7, 25, 1, 0, 0, 0, time.UTC),
	}
	if !validPushHistoryQuery(query) {
		t.Fatal("valid exact history query was rejected")
	}
	item := pushHistoryTestMessage(
		"om_exact",
		chatID,
		appID,
		`{"elements":[{"behaviors":[{"value":{`+
			`"effect_id":"`+effectID+`"}}]}]}`,
	)
	if !pushHistoryItemMatches(item, query) {
		t.Fatal("exact positive provider evidence did not match")
	}

	tests := []struct {
		name   string
		mutate func(*larkim.Message)
	}{
		{
			name: "wrong chat",
			mutate: func(message *larkim.Message) {
				message.ChatId = pushHistoryPtr("oc_other")
			},
		},
		{
			name: "wrong app",
			mutate: func(message *larkim.Message) {
				message.Sender.Id = pushHistoryPtr("cli_other")
			},
		},
		{
			name: "user sender",
			mutate: func(message *larkim.Message) {
				message.Sender.IdType = pushHistoryPtr("open_id")
				message.Sender.SenderType = pushHistoryPtr("user")
			},
		},
		{
			name: "noninteractive",
			mutate: func(message *larkim.Message) {
				message.MsgType = pushHistoryPtr("text")
			},
		},
		{
			name: "deleted",
			mutate: func(message *larkim.Message) {
				message.Deleted = pushHistoryPtr(true)
			},
		},
		{
			name: "missing marker",
			mutate: func(message *larkim.Message) {
				message.Body.Content = pushHistoryPtr(`{"effect_id":"other"}`)
			},
		},
		{
			name: "conflicting marker",
			mutate: func(message *larkim.Message) {
				message.Body.Content = pushHistoryPtr(
					`{"effect_id":"` + effectID +
						`","nested":{"effect_id":"other"}}`,
				)
			},
		},
		{
			name: "malformed card",
			mutate: func(message *larkim.Message) {
				message.Body.Content = pushHistoryPtr(`{"effect_id":`)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := pushHistoryTestMessage(
				"om_candidate",
				chatID,
				appID,
				`{"effect_id":"`+effectID+`"}`,
			)
			test.mutate(candidate)
			if pushHistoryItemMatches(candidate, query) {
				t.Fatal("non-exact provider history evidence matched")
			}
		})
	}
}

func TestValidPushHistoryQueryRejectsUnfrozenInputs(t *testing.T) {
	base := pusheffect.HistoryQuery{
		EffectID:       "019f9824-39b6-7e13-b247-b5ee5713c52b",
		ProviderChatID: "oc_owner_p2p",
		AppIdentity:    "cli_expected_app",
		StartTime:      time.Date(2026, 7, 25, 0, 0, 0, 0, time.UTC),
		EndTime:        time.Date(2026, 7, 25, 1, 0, 0, 0, time.UTC),
	}
	tests := []struct {
		name   string
		mutate func(*pusheffect.HistoryQuery)
	}{
		{
			name: "noncanonical effect id",
			mutate: func(query *pusheffect.HistoryQuery) {
				query.EffectID = "not-a-uuid"
			},
		},
		{
			name: "missing chat",
			mutate: func(query *pusheffect.HistoryQuery) {
				query.ProviderChatID = ""
			},
		},
		{
			name: "padded app identity",
			mutate: func(query *pusheffect.HistoryQuery) {
				query.AppIdentity = " cli_expected_app "
			},
		},
		{
			name: "control in app identity",
			mutate: func(query *pusheffect.HistoryQuery) {
				query.AppIdentity = "cli_expected\napp"
			},
		},
		{
			name: "reverse window",
			mutate: func(query *pusheffect.HistoryQuery) {
				query.EndTime = query.StartTime
			},
		},
		{
			name: "window exceeds cap",
			mutate: func(query *pusheffect.HistoryQuery) {
				query.EndTime = query.StartTime.Add(2*time.Hour + time.Second)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			query := base
			test.mutate(&query)
			if validPushHistoryQuery(query) {
				t.Fatalf("invalid query accepted: %+v", query)
			}
		})
	}
}

func TestSummarizePushHistoryMatchesRequiresUniqueMessage(t *testing.T) {
	if observation := summarizePushHistoryMatches(nil); observation.MatchCount != 0 || observation.MessageID != "" {
		t.Fatalf("zero matches = %+v", observation)
	}
	if observation := summarizePushHistoryMatches(
		map[string]struct{}{"om_one": {}},
	); observation.MatchCount != 1 || observation.MessageID != "om_one" {
		t.Fatalf("one match = %+v", observation)
	}
	if observation := summarizePushHistoryMatches(
		map[string]struct{}{"om_one": {}, "om_two": {}},
	); observation.MatchCount != 2 || observation.MessageID != "" {
		t.Fatalf("multiple matches = %+v", observation)
	}
}

func pushHistoryTestMessage(
	messageID string,
	chatID string,
	appID string,
	cardJSON string,
) *larkim.Message {
	return &larkim.Message{
		MessageId: pushHistoryPtr(messageID),
		ChatId:    pushHistoryPtr(chatID),
		MsgType:   pushHistoryPtr("interactive"),
		Sender: &larkim.Sender{
			Id:         pushHistoryPtr(appID),
			IdType:     pushHistoryPtr("app_id"),
			SenderType: pushHistoryPtr("app"),
		},
		Body: &larkim.MessageBody{Content: pushHistoryPtr(cardJSON)},
	}
}

func pushHistoryPtr[T any](value T) *T {
	return &value
}
