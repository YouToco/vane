package store

import (
	"context"

	"github.com/YouToco/vane/server/channelruntime"
)

// PrepareTelegramSendPermit freezes the effect first and only then projects a
// provider-neutral permit. A business workflow never receives a Telegram
// sender or a credential-bearing client.
func (s *Store) PrepareTelegramSendPermit(
	ctx context.Context, tenantID, userID, routeID int64,
	effectID, effectKind, text string,
) (channelruntime.SendPermit, error) {
	effect, err := s.PrepareTelegramOutbound(
		ctx, tenantID, userID, routeID, effectID, effectKind, text)
	if err != nil {
		return channelruntime.SendPermit{}, err
	}
	return channelruntime.BindDurableSend(
		channelruntime.ProviderTelegram, effect.TenantID, effect.UserID,
		effect.RouteID, effect.EffectID, effect.EffectKind, effect.PayloadDigest)
}

func (s *Store) PrepareAggregateTelegramSendPermit(
	ctx context.Context, planID string, tenantID, userID int64, taskID string,
	batchID int64, chunkIndex, chunkCount int, deliveryIDs []int64,
	routeID int64, effectID, text string,
) (channelruntime.SendPermit, error) {
	effect, err := s.PrepareAggregateTelegramOutbound(
		ctx, planID, tenantID, userID, taskID, batchID, chunkIndex,
		chunkCount, deliveryIDs, routeID, effectID, text)
	if err != nil {
		return channelruntime.SendPermit{}, err
	}
	return channelruntime.BindDurableSend(
		channelruntime.ProviderTelegram, effect.TenantID, effect.UserID,
		effect.RouteID, effect.EffectID, effect.EffectKind, effect.PayloadDigest)
}
