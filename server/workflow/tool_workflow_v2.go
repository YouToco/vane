package workflow

import (
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/YouToco/vane/runcontext"
	"github.com/YouToco/vane/types"
)

const toolObservationQualificationVersionID = "compiled-tool-v2-observation-qualification-v1"
const toolRunOutcomeVersionID = "compiled-tool-v2-run-outcome-v1"

// runCompiledToolPipelineV2 is a separately versioned Source-free command
// sequence. No V1 ref, Source activity, or confirmation step appears here.
func runCompiledToolPipelineV2(
	ctx workflow.Context,
	p PushParams,
	traceID string,
	a *Activities,
) (retErr error) {
	log := workflow.GetLogger(ctx)
	info := workflow.GetInfo(ctx).WorkflowExecution
	expected := types.RunIdentity{
		TemporalWorkflowID: info.ID,
		TemporalRunID:      info.RunID,
		RunKind:            types.RunSnapshotKindScheduled,
		TenantID:           p.TenantID,
		UserID:             p.UserID,
		TaskID:             p.ScheduleID,
	}
	prepareCtx := workflow.WithActivityOptions(
		ctx, quickActivityOptions())
	var prepared PrepareToolRunV2Result
	if err := workflow.ExecuteActivity(
		prepareCtx, a.PrepareToolRunV2, p,
	).Get(prepareCtx, &prepared); err != nil {
		return err
	}
	if err := prepared.ValidateFor(expected); err != nil {
		return err
	}
	if !prepared.Authorized {
		log.Warn("compiled Tool pipeline is no longer authorized",
			"tenant_id", p.TenantID, "user_id", p.UserID,
			"task_id", p.ScheduleID, "trace_id", traceID)
		return nil
	}
	run := CompiledToolRunInputV2{
		TenantID: p.TenantID,
		TaskID:   p.ScheduleID,
		Snapshot: prepared.Snapshot,
	}
	outcomeCoverage := types.RunCompletenessPartial
	outcomeProcessing := types.RunCompletenessComplete
	var outcomeTerminal *runOutcomeTerminalV1
	if workflow.GetVersion(
		ctx, toolRunOutcomeVersionID,
		workflow.DefaultVersion, 1,
	) >= 1 {
		beginCtx := workflow.WithActivityOptions(ctx, quickActivityOptions())
		var marker types.RunOutcomeMarkerV1
		if err := workflow.ExecuteActivity(
			beginCtx, a.BeginToolRunOutcomeV2,
			ToolRunOutcomeBeginV2Input{
				UserID: p.UserID,
				Run:    run,
			},
		).Get(beginCtx, &marker); err != nil {
			return err
		}
		if err := marker.Validate(); err != nil ||
			marker.RunSnapshotID != run.Snapshot.SnapshotID ||
			marker.TenantID != run.TenantID ||
			marker.UserID != p.UserID ||
			marker.TaskID != run.TaskID {
			return types.NewAppError(
				types.CodeValidation, "Tool run outcome marker differs from run", err)
		}
		defer func() {
			terminal := outcomeTerminal
			if terminal == nil {
				terminal = terminalRunOutcomeForError(retErr)
			}
			processing := outcomeProcessing
			if retErr != nil && outcomeTerminal == nil {
				processing = types.RunCompletenessPartial
			}
			claim := types.RunOutcomeClaimV1{
				RunOutcomeMarkerV1: marker,
				Result:             terminal.result,
				SourceCoverage:     outcomeCoverage,
				Processing:         processing,
				FailureCode:        terminal.failureCode,
				FailureMessage:     terminal.failureMessage,
			}
			finalizeCtx, cancel := workflow.NewDisconnectedContext(ctx)
			defer cancel()
			finalizeCtx = workflow.WithActivityOptions(
				finalizeCtx, quickActivityOptions())
			err := workflow.ExecuteActivity(
				finalizeCtx, a.FinalizeToolRunOutcomeV2,
				ToolRunOutcomeFinalizeV2Input{
					UserID: p.UserID, Run: run, Claim: claim,
				},
			).Get(finalizeCtx, nil)
			if err != nil {
				log.Error("Tool run outcome finalization failed",
					"snapshot_id", marker.RunSnapshotID, "err", err)
				if retErr == nil {
					retErr = err
				}
			}
		}()
	}
	var counts types.PipelineCounts
	recordEmpty := func(gate types.BatchExitGate) {
		recordCtx := workflow.WithActivityOptions(
			ctx, quickActivityOptions())
		if err := workflow.ExecuteActivity(
			recordCtx, a.RecordEmptyToolRunV2,
			RecordEmptyToolRunV2Input{
				UserID: p.UserID, TraceID: traceID,
				Run: run, Gate: gate, Counts: counts,
			},
		).Get(recordCtx, nil); err != nil {
			log.Warn("compiled Tool empty batch record failed",
				"gate", gate, "trace_id", traceID, "err", err)
		}
	}

	// Schedule every frozen call first, then join in the same frozen order.
	// A single failure does not abandon another already-started provider call;
	// committed observation receipts remain recoverable by the next run.
	effectCtx := workflow.WithActivityOptions(
		ctx, toolSideEffectActivityOptions())
	futures := make([]workflow.Future, len(prepared.InvocationDigests))
	for i, invocationDigest := range prepared.InvocationDigests {
		futures[i] = workflow.ExecuteActivity(
			effectCtx, a.ExecuteToolInvocationV2,
			ExecuteToolInvocationV2Input{
				TenantID: p.TenantID, UserID: p.UserID,
				TaskID: p.ScheduleID, Snapshot: prepared.Snapshot,
				InvocationDigest: invocationDigest,
			})
	}
	fetched := 0
	var firstExecutionErr error
	for i, future := range futures {
		var receipt ToolInvocationReceiptV1
		err := future.Get(effectCtx, &receipt)
		if err == nil {
			err = receipt.ValidateFor(prepared.InvocationDigests[i])
		}
		if err != nil {
			if firstExecutionErr == nil {
				firstExecutionErr = err
			}
			continue
		}
		fetched += receipt.ContentCount
	}
	if firstExecutionErr != nil {
		return firstExecutionErr
	}
	outcomeCoverage = types.RunCompletenessComplete
	counts = counts.WithFetched(fetched)

	readCtx := workflow.WithActivityOptions(ctx, quickActivityOptions())
	var candidates []runcontext.ToolCandidateV1
	if err := workflow.ExecuteActivity(
		readCtx, a.CollectToolRunContentV2,
		CollectToolRunContentV2Input{
			TenantID: p.TenantID, UserID: p.UserID,
			TaskID: p.ScheduleID, Snapshot: prepared.Snapshot,
		},
	).Get(readCtx, &candidates); err != nil {
		return err
	}
	if len(candidates) == 0 {
		outcomeTerminal = quietRunOutcomeV1()
		recordEmpty(types.BatchExitGateFetch)
		return nil
	}

	var deduped []runcontext.ToolCandidateV1
	if err := workflow.ExecuteActivity(
		readCtx, a.DedupToolCandidatesV2,
		DedupToolCandidatesV2Input{
			UserID: p.UserID, TraceID: traceID,
			Run: run, Candidates: candidates,
		},
	).Get(readCtx, &deduped); err != nil {
		return err
	}
	counts = counts.WithDeduped(len(deduped))
	if len(deduped) == 0 {
		outcomeTerminal = quietRunOutcomeV1()
		recordEmpty(types.BatchExitGateDedup)
		return nil
	}

	paidCtx := workflow.WithActivityOptions(
		ctx, toolSideEffectActivityOptions())
	qualified := QualifyToolCandidatesV2Result{
		Candidates: deduped,
		Outcome:    "legacy_replay",
	}
	if workflow.GetVersion(
		ctx, toolObservationQualificationVersionID,
		workflow.DefaultVersion, 1,
	) >= 1 {
		if err := workflow.ExecuteActivity(
			paidCtx, a.QualifyToolCandidatesV2,
			QualifyToolCandidatesV2Input{
				UserID: p.UserID, TraceID: traceID,
				Run: run, Candidates: deduped,
			},
		).Get(paidCtx, &qualified); err != nil {
			if isQuotaFailure(err) {
				outcomeProcessing = types.RunCompletenessPartial
				outcomeTerminal = quietRunOutcomeV1()
				recordEmpty(types.BatchExitGateQuota)
				return nil
			}
			return err
		}
		counts = counts.WithQualified(len(qualified.Candidates))
		if len(qualified.Candidates) == 0 {
			gate := types.BatchExitGateObservationNoMatch
			if qualified.Outcome == "uncertain" {
				gate = types.BatchExitGateObservationUncertain
				outcomeProcessing = types.RunCompletenessPartial
			}
			outcomeTerminal = quietRunOutcomeV1()
			recordEmpty(gate)
			return nil
		}
	}

	var scored []runcontext.ToolScoredCandidateV1
	if err := workflow.ExecuteActivity(
		paidCtx, a.ScoreToolCandidatesV2,
		ScoreToolCandidatesV2Input{
			UserID: p.UserID, TraceID: traceID,
			Run: run, Candidates: qualified.Candidates,
		},
	).Get(paidCtx, &scored); err != nil {
		if isQuotaFailure(err) {
			outcomeProcessing = types.RunCompletenessPartial
			outcomeTerminal = quietRunOutcomeV1()
			recordEmpty(types.BatchExitGateQuota)
			return nil
		}
		return err
	}
	counts = counts.WithScored(len(scored))
	if len(scored) == 0 {
		outcomeTerminal = quietRunOutcomeV1()
		recordEmpty(types.BatchExitGateScore)
		return nil
	}

	var selected []runcontext.ToolScoredCandidateV1
	if err := workflow.ExecuteActivity(
		readCtx, a.SelectToolCandidatesV2,
		SelectToolCandidatesV2Input{
			UserID: p.UserID, TraceID: traceID,
			Run: run, Candidates: scored,
		},
	).Get(readCtx, &selected); err != nil {
		return err
	}
	counts = counts.WithSelected(len(selected))
	if len(selected) == 0 {
		outcomeTerminal = quietRunOutcomeV1()
		recordEmpty(types.BatchExitGateSelect)
		return nil
	}

	var cards []ToolGeneratedCardV1
	if err := workflow.ExecuteActivity(
		paidCtx, a.CardGenToolCandidatesV2,
		CardGenToolCandidatesV2Input{
			UserID: p.UserID, TraceID: traceID,
			Run: run, Candidates: selected,
			Evidence:         qualified.Evidence,
			EvidenceRequired: qualified.EvidenceRequired,
		},
	).Get(paidCtx, &cards); err != nil {
		if isQuotaFailure(err) {
			outcomeProcessing = types.RunCompletenessPartial
			outcomeTerminal = quietRunOutcomeV1()
			recordEmpty(types.BatchExitGateQuota)
			return nil
		}
		return err
	}
	counts = counts.WithCards(len(cards))
	if len(cards) == 0 {
		outcomeTerminal = quietRunOutcomeV1()
		recordEmpty(types.BatchExitGateCardGen)
		return nil
	}

	pushCtx := workflow.WithActivityOptions(ctx, ioActivityOptions())
	if err := workflow.ExecuteActivity(
		pushCtx, a.PushToolCardsV2,
		PushToolCardsV2Input{
			UserID: p.UserID, TraceID: traceID,
			Run: run, Cards: cards,
			EvidenceRequired: qualified.EvidenceRequired,
		},
	).Get(pushCtx, nil); err != nil {
		return err
	}
	outcomeTerminal = contentRunOutcomeV1()
	return nil
}

// Provider Tool calls and paid LLM stages are never automatically repeated
// after an Activity completion-ack loss. The durable push stage has its own
// exact Effect state machine and therefore keeps the normal retry policy.
func toolSideEffectActivityOptions() workflow.ActivityOptions {
	return workflow.ActivityOptions{
		StartToCloseTimeout: 120 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts:        1,
			NonRetryableErrorTypes: nonRetryableCodes,
		},
	}
}
