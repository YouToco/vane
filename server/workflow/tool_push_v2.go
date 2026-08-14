package workflow

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"

	cardgenpkg "github.com/YouToco/vane/server/cardgen"
	"github.com/YouToco/vane/server/feedback"
	"github.com/YouToco/vane/server/pusheffect"
	"github.com/YouToco/vane/server/runcontext"
	"github.com/YouToco/vane/server/types"
)

type PushToolCardsV2Input struct {
	UserID           int64                  `json:"user_id"`
	TraceID          string                 `json:"trace_id"`
	Run              CompiledToolRunInputV2 `json:"run"`
	Cards            []ToolGeneratedCardV1  `json:"cards"`
	EvidenceRequired bool                   `json:"evidence_required,omitempty"`
}

type RecordEmptyToolRunV2Input struct {
	UserID  int64                  `json:"user_id"`
	TraceID string                 `json:"trace_id"`
	Run     CompiledToolRunInputV2 `json:"run"`
	Gate    types.BatchExitGate    `json:"gate"`
	Counts  types.PipelineCounts   `json:"counts"`
}

func (a *Activities) RecordEmptyToolRunV2(
	ctx context.Context,
	in RecordEmptyToolRunV2Input,
) error {
	_, expected, err := a.loadAuthoritativeToolRunV2(
		ctx, in.UserID, in.Run)
	if err != nil {
		return retryableOrNot(err)
	}
	_, _, err = a.compiledToolStoreV2.RecordEmptyPushBatchForTaskRunV2(
		ctx, expected, in.Run.Snapshot, in.TraceID, in.Gate, in.Counts)
	return retryableOrNot(err)
}

// PushToolCardsV2 is durable-effect-only. The Source-free runtime never falls
// back to the legacy send/receipt gap.
func (a *Activities) PushToolCardsV2(
	ctx context.Context,
	in PushToolCardsV2Input,
) error {
	if strings.TrimSpace(in.TraceID) == "" ||
		a.pushEffectStore == nil {
		return nonRetryable(types.NewAppError(
			types.CodeValidation,
			"compiled Tool push input is invalid", nil))
	}
	snapshot, expected, err := a.loadAuthoritativeToolRunV2(
		ctx, in.UserID, in.Run)
	if err != nil {
		return retryableOrNot(err)
	}
	batchID, recoveryOnly, err :=
		a.compiledToolStoreV2.CreateOrRecoverPushBatchForTaskRunV2(
			ctx, expected, in.Run.Snapshot, in.TraceID)
	if err != nil {
		return retryableOrNot(err)
	}
	scope := types.PushBatchScope{
		TenantID: expected.TenantID,
		UserID:   expected.UserID,
		BatchID:  batchID,
	}
	if recoveryOnly {
		return retryableOrNot(
			a.pushEffectStore.SettlePushEffectBatchReceipt(
				ctx, scope, in.Run.Snapshot.SnapshotID))
	}
	if len(in.Cards) == 0 || a.buildAggCard == nil {
		return nonRetryable(types.NewAppError(
			types.CodeValidation,
			"compiled Tool push input is invalid", nil))
	}
	scored := make([]runcontext.ToolScoredCandidateV1, len(in.Cards))
	canonical := make([]runcontext.ToolCandidateV1, len(in.Cards))
	for i, card := range in.Cards {
		if strings.TrimSpace(card.Card.BodyMD) == "" ||
			card.Card.Structured != nil ||
			len(card.Card.EventEvidence) != 0 {
			return nonRetryable(types.NewAppError(
				types.CodeValidation,
				"compiled Tool push card is invalid", nil))
		}
		if in.EvidenceRequired && len(card.Evidence) == 0 {
			return nonRetryable(types.NewAppError(
				types.CodeValidation,
				"compiled Tool push card lacks required evidence", nil))
		}
		if len(card.Evidence) > 0 {
			candidates := make(
				[]runcontext.ToolCandidateV1, len(card.Evidence))
			sources := make(
				[]cardgenpkg.EventEvidenceSourceV1, len(card.Evidence))
			for index := range card.Evidence {
				candidates[index] = card.Evidence[index].Candidate
				sources[index] = card.Evidence[index].Source
			}
			if candidates[0].Item.ID != card.Card.Scored.Item.ID ||
				sources[0].ContentItemID != card.Card.Scored.Item.ID {
				return nonRetryable(types.NewAppError(
					types.CodeValidation,
					"compiled Tool push evidence is invalid", nil))
			}
			if err := validateToolCandidatesV2(
				snapshot, candidates); err != nil {
				return nonRetryable(err)
			}
			if err := a.validateCanonicalToolCandidatesV2(
				ctx, expected, in.Run.Snapshot, candidates); err != nil {
				return retryableOrNot(err)
			}
			canonicalSources, err := toolCardEvidenceSourcesV2(
				snapshot, candidates)
			if err != nil || !reflect.DeepEqual(canonicalSources, sources) {
				return nonRetryable(types.NewAppError(
					types.CodeValidation,
					"compiled Tool push evidence differs from canonical content",
					err))
			}
			if err := cardgenpkg.ValidateGroundedEvidenceBodyV1(
				card.Card.BodyMD, snapshot.Definition.TaskManual,
				sources); err != nil {
				return nonRetryable(err)
			}
		}
		scored[i] = runcontext.ToolScoredCandidateV1{
			InvocationDigest: card.InvocationDigest,
			Scored:           card.Card.Scored,
		}
		canonical[i] = runcontext.ToolCandidateV1{
			InvocationDigest: card.InvocationDigest,
			Item:             card.Card.Scored.Item,
		}
	}
	if err := validateToolScoredCandidatesV2(snapshot, scored); err != nil {
		return nonRetryable(err)
	}
	if err := a.validateCanonicalToolCandidatesV2(
		ctx, expected, in.Run.Snapshot, canonical); err != nil {
		return nonRetryable(err)
	}
	authority, err :=
		a.compiledToolStoreV2.
			ClaimPushBatchDeliveryAuthorityForTaskRunV2(
				ctx, expected, in.Run.Snapshot, batchID)
	if err != nil {
		return retryableOrNot(err)
	}
	if authority != types.PushBatchDeliveryAuthorityEffect {
		return nonRetryable(types.NewAppError(
			types.CodeConflict,
			"compiled Tool push lacks durable effect authority", nil))
	}
	existing, err := a.pushEffectStore.ListPushEffectsForBatch(
		ctx, scope, in.Run.Snapshot.SnapshotID)
	if err != nil {
		return retryableOrNot(err)
	}
	var partial []*pusheffect.Effect
	if len(existing) > 0 {
		complete, safeToFinish := completePushEffectPlan(existing)
		if complete {
			return retryableOrNot(
				a.sendFrozenPushEffectsWithAuthorizerAndClaimer(
					ctx, existing,
					func(effectCtx context.Context) error {
						return a.authorizeToolEffectV2(
							effectCtx, expected, in.Run.Snapshot)
					},
					func(
						claimCtx context.Context,
						params pusheffect.ClaimParams,
					) (*pusheffect.Effect, error) {
						return a.compiledToolStoreV2.
							ClaimPushEffectForTaskRunV2(
								claimCtx, expected,
								in.Run.Snapshot, params)
					}))
		}
		if !safeToFinish {
			return nonRetryable(types.NewAppError(
				types.CodeConflict,
				"compiled Tool durable push plan is incomplete after send",
				nil))
		}
		partial = existing
	}

	targetProvider, ok := a.feishu.(pushEffectTargetProvider)
	if !ok {
		return nonRetryable(types.NewAppError(
			types.CodeConflict,
			"durable push effect target provider is unavailable", nil))
	}
	ownerOpenID, ownerChatID, appIdentity :=
		targetProvider.PushEffectTarget()
	if ownerOpenID == "" || ownerChatID == "" || appIdentity == "" {
		return nonRetryable(types.NewAppError(
			types.CodeNotFound,
			"durable push effect provider identity is unavailable", nil))
	}
	if len(partial) > 0 {
		first := partial[0]
		if first.AppIdentity != appIdentity ||
			first.ProviderChatID != ownerChatID ||
			first.Target != ownerOpenID {
			return nonRetryable(types.NewAppError(
				types.CodeConflict,
				"durable push provider generation changed during Tool plan",
				nil))
		}
	}

	bindings := make(map[string]runcontext.ToolBindingV1,
		len(snapshot.ToolBindings))
	for _, binding := range snapshot.ToolBindings {
		bindings[binding.InvocationDigest] = binding
	}
	pending := make([]pushPendingItem, 0, len(in.Cards))
	for _, generated := range in.Cards {
		card := generated.Card
		contentID := card.Scored.Item.ID
		evidenceJSON, evidenceErr :=
			toolDeliveryEvidenceJSONV1(generated.Evidence)
		if evidenceErr != nil {
			return nonRetryable(evidenceErr)
		}
		deliveryID, existed, sentAlready, insertErr :=
			a.compiledToolStoreV2.InsertDeliveryForTaskRunV2(
				ctx, expected, in.Run.Snapshot, in.TraceID,
				generated.InvocationDigest,
				&types.Delivery{
					BatchID:              batchID,
					UserID:               in.UserID,
					ContentItemID:        &contentID,
					InvocationDigest:     generated.InvocationDigest,
					Score:                card.Scored.Score,
					BodyMD:               card.BodyMD,
					ToolEvidenceJSON:     evidenceJSON,
					ToolEvidenceRequired: in.EvidenceRequired,
					Status:               types.DeliveryStatusPending,
				})
		if insertErr != nil {
			return retryableOrNot(insertErr)
		}
		if existed && sentAlready {
			continue
		}
		binding := bindings[generated.InvocationDigest]
		pending = append(pending, pushPendingItem{
			delID: deliveryID,
			input: feedback.CardInput{
				BodyMD:      card.BodyMD,
				DeliveryID:  deliveryID,
				Title:       card.Scored.Item.Title,
				Score:       int(card.Scored.Score),
				URL:         card.Scored.Item.URL,
				PublishedAt: card.Scored.Item.PublishedAt,
				SourceTitle: binding.Contract.ToolName,
				Platform:    types.Platform(binding.Contract.Platform),
			},
		})
	}
	if len(pending) == 0 {
		return nonRetryable(types.NewAppError(
			types.CodeConflict,
			"compiled Tool push has no unsettled durable effect", nil))
	}

	buildChunk := func(chunk []pushPendingItem, effectID string) string {
		items := make([]feedback.CardInput, len(chunk))
		for i := range chunk {
			items[i] = chunk[i].input
		}
		title, template := "", ""
		if a.aggHeader != nil {
			title, template = a.aggHeader(
				snapshot.Definition.NLDescription, len(items))
		}
		return a.buildAggCard(feedback.AggregateCardInput{
			HeaderTitle: title, HeaderTemplate: template,
			EffectID: effectID, Items: items,
		})
	}
	planned := planPushChunks(
		pending, pushEffectMarkerWidthSeed, buildChunk)
	effects := make([]*pusheffect.Effect, 0, len(planned))
	for index, chunk := range planned {
		effectID := pushEffectID(expected, index)
		cardJSON := buildChunk(chunk.items, effectID)
		effect, prepareErr := a.prepareDurablePushChunkWithCreator(
			ctx, expected, in.Run.Snapshot.SnapshotID,
			batchID, index, len(planned), chunk.items,
			cardJSON, effectID, ownerOpenID, ownerChatID, appIdentity,
			func(
				createCtx context.Context,
				prepared pusheffect.Prepared,
			) (*pusheffect.Effect, error) {
				return a.compiledToolStoreV2.
					CreatePushEffectForTaskRunV2(
						createCtx, expected, in.Run.Snapshot, prepared)
			})
		if prepareErr != nil {
			return retryableOrNot(prepareErr)
		}
		effects = append(effects, effect)
	}
	return retryableOrNot(
		a.sendFrozenPushEffectsWithAuthorizerAndClaimer(
			ctx, effects,
			func(effectCtx context.Context) error {
				return a.authorizeToolEffectV2(
					effectCtx, expected, in.Run.Snapshot)
			},
			func(
				claimCtx context.Context,
				params pusheffect.ClaimParams,
			) (*pusheffect.Effect, error) {
				return a.compiledToolStoreV2.
					ClaimPushEffectForTaskRunV2(
						claimCtx, expected, in.Run.Snapshot, params)
			}))
}

type toolDeliveryEvidenceManifestV1 struct {
	ContentItemID    int64                            `json:"content_item_id"`
	InvocationDigest string                           `json:"invocation_digest"`
	Metadata         types.StructuredEvidenceSourceV1 `json:"metadata"`
}

func toolDeliveryEvidenceJSONV1(
	evidence []ToolCardEvidenceV1,
) (json.RawMessage, error) {
	if len(evidence) == 0 {
		return nil, nil
	}
	manifest := make(
		[]toolDeliveryEvidenceManifestV1, len(evidence))
	for i := range evidence {
		if evidence[i].Candidate.Item.ID !=
			evidence[i].Source.ContentItemID {
			return nil, types.NewAppError(types.CodeValidation,
				"compiled Tool delivery evidence is invalid", nil)
		}
		manifest[i] = toolDeliveryEvidenceManifestV1{
			ContentItemID:    evidence[i].Source.ContentItemID,
			InvocationDigest: evidence[i].Candidate.InvocationDigest,
			Metadata:         evidence[i].Source.Metadata,
		}
	}
	raw, err := json.Marshal(manifest)
	if err != nil {
		return nil, types.NewAppError(types.CodeInternal,
			"compiled Tool delivery evidence encoding failed", err)
	}
	return raw, nil
}
