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
	planCtx := workflow.WithActivityOptions(ctx, researchV3SideEffectOptions())
	var planned PlanResearchRunV3Result
	if err := workflow.ExecuteActivity(
		planCtx, a.PlanResearchRunV3, run,
	).Get(planCtx, &planned); err != nil {
		return err
	}
	if err := planned.ValidateFor(identity, prepared.Snapshot.SnapshotID); err != nil {
		return err
	}
	for ordinal := 0; ordinal < planned.Plan.StepCount; ordinal++ {
		var receipt ResearchStepReceiptV3
		if err := workflow.ExecuteActivity(
			planCtx, a.ExecuteResearchStepV3, ExecuteResearchStepV3Input{
				ResearchRunV3Input: run, Plan: planned.Plan, Ordinal: ordinal,
			},
		).Get(planCtx, &receipt); err != nil {
			return err
		}
		if err := receipt.Validate(ordinal); err != nil {
			return err
		}
	}
	var brief ResearchBriefRefV3
	if err := workflow.ExecuteActivity(
		planCtx, a.SynthesizeResearchBriefV3, SynthesizeResearchBriefV3Input{
			ResearchRunV3Input: run, Plan: planned.Plan,
		},
	).Get(planCtx, &brief); err != nil {
		return err
	}
	if err := brief.ValidateFor(identity, prepared.Snapshot.SnapshotID, planned.Plan.PlanID); err != nil {
		return err
	}
	if !brief.DeliveryRequired {
		return nil
	}
	deliveryCtx := workflow.WithActivityOptions(ctx, researchV3SideEffectOptions())
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

func researchV3SideEffectOptions() workflow.ActivityOptions {
	return workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 1, NonRetryableErrorTypes: nonRetryableCodes,
		},
	}
}
