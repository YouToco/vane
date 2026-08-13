package agentfirstaudit

import (
	"bytes"
	"context"
	"fmt"

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
	temporal BaselineTemporalReader,
	runClock BaselineClockRunner,
	request BaselineCollectorRequest,
) (BaselineCollectorResult, error) {
	if database == nil || temporal == nil || runClock == nil ||
		request.Namespace == "" || !validSourceRevision(request.SourceRevision) ||
		request.Release.Receipt.SourceRevision != request.SourceRevision ||
		!validLowerHex(request.Release.DeployDigest, 32) {
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
		SourceRevision:         request.SourceRevision, DeployDigest: request.Release.DeployDigest,
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
		SourceRevision:             request.SourceRevision, DeployDigest: request.Release.DeployDigest,
	}
	event, appendErr := database.AppendAgentFirstRetentionAttestation(ctx, input)
	if appendErr != nil {
		event, err = database.LoadAgentFirstRetentionAttestation(ctx, input)
		if err != nil {
			return BaselineCollectorResult{}, fmt.Errorf(
				"append baseline attestation: %w; exact adoption: %v", appendErr, err)
		}
	}
	if event == nil || event.TemporalEvidenceDigest != manifest.Digest ||
		event.LegacyDBSnapshotDigest != afterDB.LegacyDBSnapshotDigest ||
		!bytes.Equal(event.LegacyDBSnapshot, afterDB.LegacyDBSnapshot) {
		return BaselineCollectorResult{}, fmt.Errorf("committed baseline differs from collected evidence")
	}
	return BaselineCollectorResult{
		Event: event, Manifest: manifest, EvidencePath: evidencePath,
	}, nil
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
		return ReadStableLegacyWorkflowInventory(ctx, temporal, namespace, true)
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
