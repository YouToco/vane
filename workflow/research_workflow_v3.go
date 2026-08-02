package workflow

import (
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// runResearchPipelineV3 is a label-isolated command sequence. Task manuals,
// Tool arguments/results and Brief bytes stay behind Activity boundaries;
// Temporal history receives only immutable references and digests.
func runResearchPipelineV3(
	ctx workflow.Context, p PushParams, traceID string, a *Activities,
	deliveryAuthority bool,
) error {
	prepareCtx := workflow.WithActivityOptions(ctx, quickActivityOptions())
	var prepared PrepareResearchRunV3Result
	if err := workflow.ExecuteActivity(
		prepareCtx, a.PrepareResearchRunV3, p,
	).Get(prepareCtx, &prepared); err != nil {
		return err
	}
	info := workflow.GetInfo(ctx).WorkflowExecution
	identity := prepared.Snapshot.Identity()
	if !prepared.Authorized {
		return nil
	}
	if identity.TemporalWorkflowID != info.ID || identity.TemporalRunID != info.RunID ||
		identity.TenantID != p.TenantID || identity.UserID != p.UserID ||
		identity.TaskID != p.ScheduleID {
		return temporal.NewNonRetryableApplicationError(
			"research V3 snapshot identity differs from Workflow", "VALIDATION", nil)
	}
	run := ResearchRunV3Input{
		TenantID: p.TenantID, UserID: p.UserID, TaskID: p.ScheduleID,
		TraceID: traceID, Snapshot: prepared.Snapshot,
	}
	recoveryCtx := workflow.WithActivityOptions(ctx, researchV3StoreRecoveryOptions())
	var planned PlanResearchRunV3Result
	if err := workflow.ExecuteActivity(
		recoveryCtx, a.PlanResearchRunV3, run,
	).Get(recoveryCtx, &planned); err != nil {
		return err
	}
	if err := planned.ValidateFor(identity, prepared.Snapshot.SnapshotID); err != nil {
		return err
	}
	for ordinal := 0; ordinal < planned.Plan.StepCount; ordinal++ {
		var receipt ResearchStepReceiptV3
		if err := workflow.ExecuteActivity(
			recoveryCtx, a.ExecuteResearchStepV3, ExecuteResearchStepV3Input{
				ResearchRunV3Input: run, Plan: planned.Plan, Ordinal: ordinal,
			},
		).Get(recoveryCtx, &receipt); err != nil {
			return err
		}
		if err := receipt.Validate(ordinal); err != nil {
			return err
		}
	}
	var brief ResearchBriefRefV3
	synthesisCtx := workflow.WithActivityOptions(ctx, researchV3SynthesisOptions())
	if err := workflow.ExecuteActivity(
		synthesisCtx, a.SynthesizeResearchBriefV3, SynthesizeResearchBriefV3Input{
			ResearchRunV3Input: run, Plan: planned.Plan,
		},
	).Get(synthesisCtx, &brief); err != nil {
		return err
	}
	if err := brief.ValidateFor(identity, prepared.Snapshot.SnapshotID, planned.Plan.PlanID); err != nil {
		return err
	}
	if !deliveryAuthority || !prepared.DeliveryAllowed || !brief.DeliveryRequired {
		return nil
	}
	deliveryCtx := workflow.WithActivityOptions(ctx, researchV3DeliveryOptions())
	var receipt ResearchDeliveryReceiptV3
	if err := workflow.ExecuteActivity(
		deliveryCtx, a.DeliverResearchBriefV3, DeliverResearchBriefV3Input{
			ResearchRunV3Input: run, Plan: planned.Plan, Brief: brief,
		},
	).Get(deliveryCtx, &receipt); err != nil {
		return err
	}
	return receipt.Validate(brief.BriefID)
}

// Planner, Tool execution and synthesis each get exactly one Store-only
// recovery attempt. Their coordinators retain the actual provider call behind
// an immutable first-writer/receipt gate, so a second Activity attempt may only
// recover or conservatively settle that durable state. Delivery keeps its
// separate single-attempt contract until its own receipt-backed coordinator is
// wired for live canary traffic.
func researchV3SynthesisOptions() workflow.ActivityOptions {
	return workflow.ActivityOptions{
		// Gateway abandoned-claim recovery becomes legal after ten minutes.
		// Keep the same idempotent Activity alive beyond that durable fence.
		StartToCloseTimeout: 15 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 2, NonRetryableErrorTypes: nonRetryableCodes,
		},
	}
}

func researchV3StoreRecoveryOptions() workflow.ActivityOptions {
	return workflow.ActivityOptions{
		StartToCloseTimeout: 15 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 2, NonRetryableErrorTypes: nonRetryableCodes,
		},
	}
}

func researchV3DeliveryOptions() workflow.ActivityOptions {
	return workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 1, NonRetryableErrorTypes: nonRetryableCodes,
		},
	}
}
