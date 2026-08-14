package feishu

import (
	"testing"
	"time"

	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"

	"github.com/YouToco/vane/pusheffect"
)

func TestPushHistoryItemMatchesRequiresExactPositiveEvidence(t *testing.T) {
	t.Parallel()

	effectID := "be68a7d2-5535-44cb-a7cb-2d3dbd88a479"
	chatID := "oc_exact"
	appID := "cli_exact"
	messageID := "om_exact"
	msgType := "interactive"
	idType := "app_id"
	senderType := "app"
	deleted := false
	card := `{"config":{"effect_id":"` + effectID + `"},"elements":[]}`
	query := pusheffect.HistoryQuery{
		EffectID: effectID, ProviderChatID: chatID, AppIdentity: appID,
		CardDigest: pusheffect.CardDigest([]byte(card)),
		StartTime:  time.Unix(100, 0).UTC(),
		EndTime:    time.Unix(200, 0).UTC(),
	}
	item := &larkim.Message{
		MessageId: &messageID,
		ChatId:    &chatID,
		MsgType:   &msgType,
		Sender: &larkim.Sender{
			Id:         &appID,
			IdType:     &idType,
			SenderType: &senderType,
		},
		Body:    &larkim.MessageBody{Content: &card},
		Deleted: &deleted,
	}
	if !pushHistoryItemMatches(item, query) {
		t.Fatal("exact positive history did not match")
	}

	wrongCard := `{"config":{"effect_id":"` + effectID + `"},"elements":[{}]}`
	item.Body.Content = &wrongCard
	if pushHistoryItemMatches(item, query) {
		t.Fatal("marker-only history bypassed exact card digest")
	}
}

func TestCardHasExactEffectMarkerRejectsConflicts(t *testing.T) {
	t.Parallel()

	effectID := "be68a7d2-5535-44cb-a7cb-2d3dbd88a479"
	tests := []struct {
		name string
		card string
		want bool
	}{
		{
			name: "single exact marker",
			card: `{"config":{"effect_id":"` + effectID + `"}}`,
			want: true,
		},
		{
			name: "no marker",
			card: `{"config":{}}`,
		},
		{
			name: "conflicting duplicate marker",
			card: `{"config":{"effect_id":"` + effectID +
				`"},"data":{"effect_id":"other"}}`,
		},
		{
			name: "non-string marker",
			card: `{"config":{"effect_id":1}}`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := cardHasExactEffectMarker(test.card, effectID); got != test.want {
				t.Fatalf("cardHasExactEffectMarker()=%v, want %v", got, test.want)
			}
		})
	}
}

func TestSummarizePushHistoryMatchesIsPositiveOnly(t *testing.T) {
	t.Parallel()

	none := summarizePushHistoryMatches(map[string]struct{}{})
	if none.MatchCount != 0 || none.MessageID != "" {
		t.Fatalf("empty history=%+v, want zero non-terminal observation", none)
	}
	one := summarizePushHistoryMatches(map[string]struct{}{"om_one": {}})
	if one.MatchCount != 1 || one.MessageID != "om_one" {
		t.Fatalf("single history=%+v", one)
	}
	many := summarizePushHistoryMatches(
		map[string]struct{}{"om_one": {}, "om_two": {}},
	)
	if many.MatchCount != 2 || many.MessageID != "" {
		t.Fatalf("conflicting history=%+v, want count-only conflict", many)
	}
}
