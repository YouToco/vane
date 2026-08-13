package agentfirstaudit

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"

	"github.com/YouToco/vane/store"
	vaneworkflow "github.com/YouToco/vane/workflow"
)

type BaselineTemporalReader interface {
	NamespaceReader
	LegacyWorkflowReader
	ScheduleInventoryReader
}

type BaselineStore interface {
	ReadAgentFirstRetentionAuditSnapshot(context.Context) (*store.AgentFirstRetentionAuditSnapshot, error)
	AppendAgentFirstRetentionAttestation(context.Context,
		store.AgentFirstRetentionAttestationInput) (*store.AgentFirstRetentionAttestationEvent, error)
	LoadAgentFirstRetentionAttestation(context.Context,
		store.AgentFirstRetentionAttestationInput) (*store.AgentFirstRetentionAttestationEvent, error)
}

type BaselineClockRunner func(context.Context, TemporalAuthority) (RetentionClockEvidence, error)

type BaselineCollectorRequest struct {
	Namespace         string
	TaskQueue         string
	OperationID       string
	SourceRevision    string
	Release           VerifiedReleaseReceipt
	EvidenceDirectory string
}

type BaselineCollectorResult struct {
	Event        *store.AgentFirstRetentionAttestationEvent
	Manifest     BaselineManifest
	EvidencePath string
}

func CollectBaseline(
	ctx context.Context,
	database BaselineStore,
	temporalClient client.Client,
	request BaselineCollectorRequest,
) (BaselineCollectorResult, error) {
	if temporalClient == nil {
		return BaselineCollectorResult{}, fmt.Errorf("Temporal client is unavailable")
	}
	return collectBaselineWithClock(ctx, database, temporalClient.WorkflowService(),
		productionClockRunner(temporalClient, request), request)
}

func collectBaselineWithClock(
	ctx context.Context,
	database BaselineStore,
	temporal BaselineTemporalReader,
	runClock BaselineClockRunner,
	request BaselineCollectorRequest,
) (BaselineCollectorResult, error) {
	if database == nil || temporal == nil || runClock == nil ||
		!boundedCanonicalTemporalText(request.Namespace, 255) ||
		!boundedCanonicalTemporalText(request.TaskQueue, 255) ||
		!canonicalUUID(request.OperationID) ||
		!validSourceRevision(request.SourceRevision) || request.Release.validate() != nil ||
		request.Release.SourceRevision() != request.SourceRevision ||
		!validLowerHex(request.Release.DeployDigest(), 32) {
		return BaselineCollectorResult{}, fmt.Errorf("baseline collector request is invalid")
	}
	beforeDB, err := database.ReadAgentFirstRetentionAuditSnapshot(ctx)
	if err != nil {
		return BaselineCollectorResult{}, fmt.Errorf("read baseline database authority: %w", err)
	}
	beforeTemporal, err := ReadTemporalAuthority(ctx, temporal, request.Namespace)
	if err != nil {
		return BaselineCollectorResult{}, err
	}
	beforeStandard, err := ReadStableLegacyWorkflowInventory(
		ctx, temporal, request.Namespace, false)
	if err != nil {
		return BaselineCollectorResult{}, err
	}
	beforeArchive, err := readBaselineArchiveInventory(
		ctx, temporal, request.Namespace, beforeTemporal)
	if err != nil {
		return BaselineCollectorResult{}, err
	}
	beforeSchedules, err := ReadStableScheduleInventory(ctx, temporal, request.Namespace)
	if err != nil {
		return BaselineCollectorResult{}, err
	}
	if err := ValidateScheduleInventoryParity(
		beforeSchedules, expectedScheduleAuthority(beforeDB)); err != nil {
		return BaselineCollectorResult{}, err
	}

	clock, err := runClock(ctx, beforeTemporal)
	if err != nil {
		return BaselineCollectorResult{}, fmt.Errorf("collect retention clock: %w", err)
	}
	if err := RequireWorkflowVisible(ctx, temporal, request.Namespace,
		clock.WorkflowID, clock.RunID, vaneworkflow.AgentFirstRetentionClockWorkflowNameV1); err != nil {
		return BaselineCollectorResult{}, err
	}

	afterTemporal, err := ReadTemporalAuthority(ctx, temporal, request.Namespace)
	if err != nil {
		return BaselineCollectorResult{}, err
	}
	if afterTemporal != beforeTemporal {
		return BaselineCollectorResult{}, fmt.Errorf("Temporal authority drifted during baseline")
	}
	afterStandard, err := ReadStableLegacyWorkflowInventory(
		ctx, temporal, request.Namespace, false)
	if err != nil {
		return BaselineCollectorResult{}, err
	}
	if afterStandard.Count != beforeStandard.Count || afterStandard.Digest != beforeStandard.Digest {
		return BaselineCollectorResult{}, fmt.Errorf("standard workflow authority drifted during baseline")
	}
	afterArchive, err := readBaselineArchiveInventory(
		ctx, temporal, request.Namespace, afterTemporal)
	if err != nil {
		return BaselineCollectorResult{}, err
	}
	if afterArchive.Count != beforeArchive.Count || afterArchive.Digest != beforeArchive.Digest {
		return BaselineCollectorResult{}, fmt.Errorf("archive workflow authority drifted during baseline")
	}
	afterSchedules, err := ReadStableScheduleInventory(ctx, temporal, request.Namespace)
	if err != nil {
		return BaselineCollectorResult{}, err
	}
	if afterSchedules.Count != beforeSchedules.Count || afterSchedules.Digest != beforeSchedules.Digest {
		return BaselineCollectorResult{}, fmt.Errorf("schedule authority drifted during baseline")
	}
	afterDB, err := database.ReadAgentFirstRetentionAuditSnapshot(ctx)
	if err != nil {
		return BaselineCollectorResult{}, fmt.Errorf("reread baseline database authority: %w", err)
	}
	if afterDB.LegacyDBSnapshotDigest != beforeDB.LegacyDBSnapshotDigest ||
		afterDB.ScheduleDigest != beforeDB.ScheduleDigest ||
		!bytes.Equal(afterDB.LegacyDBSnapshot, beforeDB.LegacyDBSnapshot) {
		return BaselineCollectorResult{}, fmt.Errorf("database authority drifted during baseline")
	}
	if err := ValidateScheduleInventoryParity(
		afterSchedules, expectedScheduleAuthority(afterDB)); err != nil {
		return BaselineCollectorResult{}, err
	}

	manifest, err := BuildBaselineManifest(BaselineManifestInput{
		Temporal: afterTemporal, Clock: clock,
		StandardWorkflows: afterStandard, ArchivedWorkflows: afterArchive,
		Schedules: afterSchedules, LegacyDBSnapshotDigest: afterDB.LegacyDBSnapshotDigest,
		DatabaseScheduleDigest: afterDB.ScheduleDigest,
		SourceRevision:         request.SourceRevision, DeployDigest: request.Release.DeployDigest(),
	})
	if err != nil {
		return BaselineCollectorResult{}, err
	}
	evidencePath, err := PersistCanonicalEvidence(
		request.EvidenceDirectory, manifest.Digest, manifest.Canonical)
	if err != nil {
		return BaselineCollectorResult{}, err
	}
	input := store.AgentFirstRetentionAttestationInput{
		Phase:             store.AgentFirstRetentionPhaseBaseline,
		TemporalClusterID: afterTemporal.ClusterID, TemporalNamespace: afterTemporal.Namespace,
		TemporalNamespaceID:        afterTemporal.NamespaceID,
		RetentionSeconds:           afterTemporal.RetentionSeconds,
		HistoryArchivalState:       store.AgentFirstArchivalState(afterTemporal.HistoryArchivalState),
		HistoryArchiveURIDigest:    afterTemporal.HistoryArchiveURIDigest,
		VisibilityArchivalState:    store.AgentFirstArchivalState(afterTemporal.VisibilityArchivalState),
		VisibilityArchiveURIDigest: afterTemporal.VisibilityArchiveURIDigest,
		TemporalServerWitness:      clock.ObservedAtUTC,
		WorkflowInventoryDigest:    afterStandard.Digest,
		ScheduleInventoryDigest:    afterSchedules.Digest,
		ArchiveInventoryDigest:     afterArchive.Digest,
		TemporalEvidenceDigest:     manifest.Digest,
		SourceRevision:             request.SourceRevision, DeployDigest: request.Release.DeployDigest(),
	}
	event, appendErr := database.AppendAgentFirstRetentionAttestation(ctx, input)
	if appendErr != nil {
		adoptionContext, cancelAdoption := context.WithTimeout(
			context.WithoutCancel(ctx), 10*time.Second)
		defer cancelAdoption()
		event, err = database.LoadAgentFirstRetentionAttestation(adoptionContext, input)
		if err != nil {
			return BaselineCollectorResult{}, fmt.Errorf(
				"append baseline attestation: %w; exact adoption: %v", appendErr, err)
		}
	}
	if err := validateCommittedBaseline(event, input, afterDB); err != nil {
		return BaselineCollectorResult{}, fmt.Errorf("committed baseline differs from collected evidence")
	}
	return BaselineCollectorResult{
		Event: event, Manifest: manifest, EvidencePath: evidencePath,
	}, nil
}

func validateCommittedBaseline(
	event *store.AgentFirstRetentionAttestationEvent,
	input store.AgentFirstRetentionAttestationInput,
	snapshot *store.AgentFirstRetentionAuditSnapshot,
) error {
	if event == nil || snapshot == nil || event.ID <= 0 ||
		event.Phase != input.Phase || event.ParentDigest != nil ||
		event.TemporalClusterID != input.TemporalClusterID ||
		event.TemporalNamespace != input.TemporalNamespace ||
		event.TemporalNamespaceID != input.TemporalNamespaceID ||
		event.RetentionSeconds != input.RetentionSeconds ||
		event.HistoryArchivalState != input.HistoryArchivalState ||
		event.HistoryArchiveURIDigest != input.HistoryArchiveURIDigest ||
		event.VisibilityArchivalState != input.VisibilityArchivalState ||
		event.VisibilityArchiveURIDigest != input.VisibilityArchiveURIDigest ||
		!event.TemporalServerWitness.Equal(input.TemporalServerWitness) ||
		event.WorkflowInventoryDigest != input.WorkflowInventoryDigest ||
		event.ScheduleInventoryDigest != input.ScheduleInventoryDigest ||
		event.ArchiveInventoryDigest != input.ArchiveInventoryDigest ||
		event.TemporalEvidenceDigest != input.TemporalEvidenceDigest ||
		event.SourceRevision != input.SourceRevision ||
		event.DeployDigest != input.DeployDigest ||
		event.LegacyDBSnapshotDigest != snapshot.LegacyDBSnapshotDigest ||
		!bytes.Equal(event.LegacyDBSnapshot, snapshot.LegacyDBSnapshot) ||
		len(event.DatabaseIdentity) == 0 || len(event.CanonicalPayload) == 0 ||
		!validLowerHex(event.PayloadDigest, 32) ||
		event.IssuedAt.IsZero() || !event.ExpiresAt.After(event.IssuedAt) {
		return fmt.Errorf("committed baseline fields differ")
	}
	return nil
}

func productionClockRunner(
	temporalClient client.Client,
	request BaselineCollectorRequest,
) BaselineClockRunner {
	return func(ctx context.Context, _ TemporalAuthority) (RetentionClockEvidence, error) {
		workflowID := "agent-first-retention-clock-" + request.OperationID
		clockRequest := vaneworkflow.AgentFirstRetentionClockRequestV1{
			Nonce: request.OperationID, SourceRevision: request.SourceRevision,
		}
		run, err := temporalClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
			ID: workflowID, TaskQueue: request.TaskQueue,
			WorkflowExecutionTimeout: 5 * time.Minute, WorkflowTaskTimeout: time.Minute,
			WorkflowIDReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		}, vaneworkflow.AgentFirstRetentionClockWorkflowNameV1, clockRequest)
		if err != nil {
			var alreadyStarted *serviceerror.WorkflowExecutionAlreadyStarted
			if !errors.As(err, &alreadyStarted) {
				return RetentionClockEvidence{}, err
			}
			run = temporalClient.GetWorkflow(ctx, workflowID, alreadyStarted.RunId)
		}
		var result vaneworkflow.AgentFirstRetentionClockResultV1
		if err := run.Get(ctx, &result); err != nil {
			return RetentionClockEvidence{}, err
		}
		return ReadRetentionClockEvidence(ctx, temporalClient.WorkflowService(),
			RetentionClockExpectation{
				Namespace: request.Namespace, WorkflowID: workflowID, RunID: run.GetRunID(),
				TaskQueue: request.TaskQueue, Nonce: request.OperationID,
				SourceRevision: request.SourceRevision,
				WorkerBuildID:  "vane/" + request.SourceRevision,
			})
	}
}

func readBaselineArchiveInventory(
	ctx context.Context,
	temporal LegacyWorkflowReader,
	namespace string,
	authority TemporalAuthority,
) (LegacyWorkflowInventory, error) {
	switch authority.HistoryArchivalState {
	case "disabled":
		return DisabledArchiveInventory(), nil
	case "enabled":
		inventory, err := ReadStableLegacyWorkflowInventory(ctx, temporal, namespace, true)
		if err != nil {
			return LegacyWorkflowInventory{}, err
		}
		// An empty archive listing has no independently known execution with
		// which to calibrate archive visibility. Do not turn it into a deletion
		// claim. A later prepared collector must bind a baseline-known run or a
		// dedicated archived canary before EE can be consumed.
		if inventory.Count == 0 {
			return LegacyWorkflowInventory{}, fmt.Errorf(
				"enabled archive visibility lacks a calibration execution")
		}
		return inventory, nil
	default:
		return LegacyWorkflowInventory{}, fmt.Errorf("archive authority is unsupported")
	}
}

func expectedScheduleAuthority(
	snapshot *store.AgentFirstRetentionAuditSnapshot,
) []ExpectedScheduleAuthority {
	if snapshot == nil {
		return nil
	}
	result := make([]ExpectedScheduleAuthority, 0, len(snapshot.Schedules))
	for _, schedule := range snapshot.Schedules {
		digest := ""
		if schedule.TargetActionDigest != nil {
			digest = *schedule.TargetActionDigest
		}
		result = append(result, ExpectedScheduleAuthority{
			ScheduleID: schedule.ID, TargetActionDigest: digest,
			Paused: schedule.Status == "paused",
		})
	}
	return result
}
