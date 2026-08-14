package agentfirstaudit

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"time"

	"go.temporal.io/sdk/client"

	"github.com/YouToco/vane/server/store"
	vaneworkflow "github.com/YouToco/vane/server/workflow"
)

type PreparedStore interface {
	BaselineStore
	LoadAgentFirstRetentionAttestationByDigest(context.Context, string) (
		*store.AgentFirstRetentionAttestationEvent, error)
}

type PreparedCollectorRequest struct {
	BaselineCollectorRequest
	ParentDigest string
}

type PreparedCollectorResult struct {
	Event        *store.AgentFirstRetentionAttestationEvent
	Manifest     BaselineManifest
	EvidencePath string
}

func CollectPrepared(
	ctx context.Context,
	database PreparedStore,
	temporalClient client.Client,
	request PreparedCollectorRequest,
) (PreparedCollectorResult, error) {
	if temporalClient == nil {
		return PreparedCollectorResult{}, fmt.Errorf("Temporal client is unavailable")
	}
	return collectPreparedWithClock(ctx, database, temporalClient.WorkflowService(),
		productionClockRunner(temporalClient, request.BaselineCollectorRequest), request)
}

func collectPreparedWithClock(
	ctx context.Context,
	database PreparedStore,
	temporal BaselineTemporalReader,
	runClock BaselineClockRunner,
	request PreparedCollectorRequest,
) (PreparedCollectorResult, error) {
	base := request.BaselineCollectorRequest
	if database == nil || temporal == nil || runClock == nil ||
		!validLowerHex(request.ParentDigest, sha256.Size) ||
		!boundedCanonicalTemporalText(base.Namespace, 255) ||
		!boundedCanonicalTemporalText(base.TaskQueue, 255) ||
		!canonicalUUID(base.OperationID) || !validSourceRevision(base.SourceRevision) ||
		base.Release.validate() != nil || base.Release.SourceRevision() != base.SourceRevision ||
		!validLowerHex(base.Release.DeployDigest(), sha256.Size) {
		return PreparedCollectorResult{}, fmt.Errorf("prepared collector request is invalid")
	}
	if _, err := database.AssertAgentFirstLegacyWriteFence(ctx); err != nil {
		return PreparedCollectorResult{}, fmt.Errorf("assert prepared legacy write fence: %w", err)
	}
	parent, err := database.LoadAgentFirstRetentionAttestationByDigest(ctx, request.ParentDigest)
	if err != nil {
		return PreparedCollectorResult{}, fmt.Errorf("load prepared parent baseline: %w", err)
	}
	parentEvidencePayload, err := ReadCanonicalEvidence(
		base.EvidenceDirectory, parent.TemporalEvidenceDigest)
	if err != nil {
		return PreparedCollectorResult{}, fmt.Errorf("read parent baseline evidence: %w", err)
	}
	parentManifest, err := DecodeBaselineEvidence(
		parentEvidencePayload, parent.TemporalEvidenceDigest)
	if err != nil {
		return PreparedCollectorResult{}, err
	}
	if err := validatePreparedParent(parent, parentManifest, request); err != nil {
		return PreparedCollectorResult{}, err
	}

	beforeDB, err := database.ReadAgentFirstRetentionAuditSnapshot(ctx)
	if err != nil {
		return PreparedCollectorResult{}, fmt.Errorf("read prepared database authority: %w", err)
	}
	beforeTemporal, err := ReadTemporalAuthority(ctx, temporal, base.Namespace)
	if err != nil {
		return PreparedCollectorResult{}, err
	}
	if err := validatePreparedTemporalAuthority(beforeTemporal, parentManifest); err != nil {
		return PreparedCollectorResult{}, err
	}
	beforeStandard, err := ReadStableLegacyWorkflowInventory(ctx, temporal, base.Namespace, false)
	if err != nil {
		return PreparedCollectorResult{}, err
	}
	if beforeStandard.Count != 0 {
		return PreparedCollectorResult{}, fmt.Errorf("legacy workflows remain in standard visibility")
	}
	beforeArchive, err := readPreparedArchiveInventory(
		ctx, temporal, base.Namespace, beforeTemporal, parentManifest)
	if err != nil {
		return PreparedCollectorResult{}, err
	}
	beforeSchedules, err := ReadStableScheduleInventory(ctx, temporal, base.Namespace)
	if err != nil {
		return PreparedCollectorResult{}, err
	}
	if err := ValidateScheduleInventoryParity(
		beforeSchedules, expectedScheduleAuthority(beforeDB)); err != nil {
		return PreparedCollectorResult{}, err
	}

	clock, err := runClock(ctx, beforeTemporal)
	if err != nil {
		return PreparedCollectorResult{}, fmt.Errorf("collect prepared retention clock: %w", err)
	}
	if err := RequireWorkflowVisible(ctx, temporal, base.Namespace,
		clock.WorkflowID, clock.RunID, vaneworkflow.AgentFirstRetentionClockWorkflowNameV1); err != nil {
		return PreparedCollectorResult{}, err
	}

	afterTemporal, err := ReadTemporalAuthority(ctx, temporal, base.Namespace)
	if err != nil {
		return PreparedCollectorResult{}, err
	}
	if afterTemporal != beforeTemporal {
		return PreparedCollectorResult{}, fmt.Errorf("Temporal authority drifted during prepared collection")
	}
	afterStandard, err := ReadStableLegacyWorkflowInventory(ctx, temporal, base.Namespace, false)
	if err != nil {
		return PreparedCollectorResult{}, err
	}
	if afterStandard.Count != 0 || afterStandard.Digest != beforeStandard.Digest {
		return PreparedCollectorResult{}, fmt.Errorf("standard workflow authority drifted during prepared collection")
	}
	afterArchive, err := readPreparedArchiveInventory(
		ctx, temporal, base.Namespace, afterTemporal, parentManifest)
	if err != nil {
		return PreparedCollectorResult{}, err
	}
	if afterArchive.Count != beforeArchive.Count || afterArchive.Digest != beforeArchive.Digest {
		return PreparedCollectorResult{}, fmt.Errorf("archive workflow authority drifted during prepared collection")
	}
	afterSchedules, err := ReadStableScheduleInventory(ctx, temporal, base.Namespace)
	if err != nil {
		return PreparedCollectorResult{}, err
	}
	if afterSchedules.Count != beforeSchedules.Count || afterSchedules.Digest != beforeSchedules.Digest {
		return PreparedCollectorResult{}, fmt.Errorf("schedule authority drifted during prepared collection")
	}
	afterDB, err := database.ReadAgentFirstRetentionAuditSnapshot(ctx)
	if err != nil {
		return PreparedCollectorResult{}, fmt.Errorf("reread prepared database authority: %w", err)
	}
	if afterDB.LegacyDBSnapshotDigest != beforeDB.LegacyDBSnapshotDigest ||
		afterDB.ScheduleDigest != beforeDB.ScheduleDigest ||
		!bytes.Equal(afterDB.LegacyDBSnapshot, beforeDB.LegacyDBSnapshot) {
		return PreparedCollectorResult{}, fmt.Errorf("database authority drifted during prepared collection")
	}
	if err := ValidateScheduleInventoryParity(
		afterSchedules, expectedScheduleAuthority(afterDB)); err != nil {
		return PreparedCollectorResult{}, err
	}
	if afterTemporal.HistoryArchivalState == "disabled" {
		if err := VerifyLegacyExecutionsPhysicallyAbsent(ctx, temporal, base.Namespace,
			baselineKnownRuns(parentManifest)); err != nil {
			return PreparedCollectorResult{}, err
		}
	}

	manifest, err := BuildPreparedManifest(PreparedManifestInput{
		ParentAttestationDigest: request.ParentDigest,
		ParentEvidenceDigest:    parent.TemporalEvidenceDigest,
		Parent:                  parentManifest,
		Observation: BaselineManifestInput{
			Temporal: afterTemporal, Clock: clock,
			StandardWorkflows: afterStandard, ArchivedWorkflows: afterArchive,
			Schedules: afterSchedules, LegacyDBSnapshotDigest: afterDB.LegacyDBSnapshotDigest,
			DatabaseScheduleDigest: afterDB.ScheduleDigest,
			SourceRevision:         base.SourceRevision, DeployDigest: base.Release.DeployDigest(),
		},
	})
	if err != nil {
		return PreparedCollectorResult{}, err
	}
	evidencePath, err := PersistCanonicalEvidence(
		base.EvidenceDirectory, manifest.Digest, manifest.Canonical)
	if err != nil {
		return PreparedCollectorResult{}, err
	}
	input := store.AgentFirstRetentionAttestationInput{
		Phase: store.AgentFirstRetentionPhasePrepared, ParentDigest: request.ParentDigest,
		TemporalClusterID: afterTemporal.ClusterID, TemporalNamespace: afterTemporal.Namespace,
		TemporalNamespaceID:        afterTemporal.NamespaceID,
		RetentionSeconds:           afterTemporal.RetentionSeconds,
		HistoryArchivalState:       store.AgentFirstArchivalState(afterTemporal.HistoryArchivalState),
		HistoryArchiveURIDigest:    afterTemporal.HistoryArchiveURIDigest,
		VisibilityArchivalState:    store.AgentFirstArchivalState(afterTemporal.VisibilityArchivalState),
		VisibilityArchiveURIDigest: afterTemporal.VisibilityArchiveURIDigest,
		TemporalServerWitness:      clock.ObservedAtUTC.UTC().Truncate(time.Microsecond),
		WorkflowInventoryDigest:    afterStandard.Digest,
		ScheduleInventoryDigest:    afterSchedules.Digest,
		ArchiveInventoryDigest:     afterArchive.Digest,
		TemporalEvidenceDigest:     manifest.Digest,
		SourceRevision:             base.SourceRevision, DeployDigest: base.Release.DeployDigest(),
	}
	if _, err := database.AssertAgentFirstLegacyWriteFence(ctx); err != nil {
		return PreparedCollectorResult{}, fmt.Errorf("reassert prepared legacy write fence: %w", err)
	}
	event, loadErr := database.LoadAgentFirstRetentionAttestationV132(
		ctx, input, afterDB.LegacyDBSnapshotDigest)
	if loadErr == nil {
		if err := validateCommittedPrepared(event, input, afterDB); err != nil {
			return PreparedCollectorResult{}, fmt.Errorf("existing prepared event differs from collected evidence")
		}
		return PreparedCollectorResult{Event: event, Manifest: manifest, EvidencePath: evidencePath}, nil
	}
	if !errors.Is(loadErr, store.ErrAgentFirstRetentionAttestationNotFound) {
		return PreparedCollectorResult{}, fmt.Errorf("load exact prepared event before append: %w", loadErr)
	}
	event, appendErr := database.AppendAgentFirstRetentionAttestationV132(
		ctx, input, afterDB.LegacyDBSnapshotDigest)
	if appendErr != nil {
		adoptionContext, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		event, err = database.LoadAgentFirstRetentionAttestationV132(
			adoptionContext, input, afterDB.LegacyDBSnapshotDigest)
		if err != nil {
			return PreparedCollectorResult{}, fmt.Errorf(
				"append prepared attestation: %w; exact adoption: %v", appendErr, err)
		}
	}
	if err := validateCommittedPrepared(event, input, afterDB); err != nil {
		return PreparedCollectorResult{}, fmt.Errorf("committed prepared event differs from collected evidence")
	}
	return PreparedCollectorResult{Event: event, Manifest: manifest, EvidencePath: evidencePath}, nil
}

func validatePreparedParent(
	event *store.AgentFirstRetentionAttestationEvent,
	manifest baselineManifestV1,
	request PreparedCollectorRequest,
) error {
	base := request.BaselineCollectorRequest
	if event == nil || event.Phase != store.AgentFirstRetentionPhaseBaseline ||
		event.ParentDigest != nil || event.PayloadDigest != request.ParentDigest ||
		event.TemporalEvidenceDigest == "" || event.SourceRevision != base.SourceRevision ||
		event.DeployDigest != base.Release.DeployDigest() ||
		event.TemporalClusterID != manifest.TemporalClusterID ||
		event.TemporalNamespace != manifest.Namespace ||
		event.TemporalNamespaceID != manifest.NamespaceID ||
		event.RetentionSeconds != manifest.RetentionSeconds ||
		string(event.HistoryArchivalState) != manifest.HistoryArchivalState ||
		event.HistoryArchiveURIDigest != manifest.HistoryArchiveURIDigest ||
		string(event.VisibilityArchivalState) != manifest.VisibilityArchivalState ||
		event.VisibilityArchiveURIDigest != manifest.VisibilityArchiveURIDigest ||
		event.WorkflowInventoryDigest != manifest.StandardWorkflowInventory.Digest ||
		event.ScheduleInventoryDigest != manifest.ScheduleInventory.Digest ||
		event.ArchiveInventoryDigest != manifest.ArchiveInventory.Digest ||
		event.LegacyDBSnapshotDigest != manifest.LegacyDBSnapshotDigest ||
		event.SourceRevision != manifest.SourceRevision || event.DeployDigest != manifest.DeployDigest {
		return fmt.Errorf("prepared parent baseline differs from canonical evidence")
	}
	observed, err := time.Parse("2006-01-02T15:04:05.999999999Z", manifest.Clock.ObservedAtUTC)
	if err != nil || !event.TemporalServerWitness.Equal(observed.UTC().Truncate(time.Microsecond)) {
		return fmt.Errorf("prepared parent clock differs from canonical evidence")
	}
	return nil
}

func validatePreparedTemporalAuthority(current TemporalAuthority, parent baselineManifestV1) error {
	if current.ClusterID != parent.TemporalClusterID || current.Namespace != parent.Namespace ||
		current.NamespaceID != parent.NamespaceID || current.RetentionSeconds != parent.RetentionSeconds ||
		current.HistoryArchivalState != parent.HistoryArchivalState ||
		current.HistoryArchiveURIDigest != parent.HistoryArchiveURIDigest ||
		current.VisibilityArchivalState != parent.VisibilityArchivalState ||
		current.VisibilityArchiveURIDigest != parent.VisibilityArchiveURIDigest ||
		current.ClusterDigest != parent.ClusterDigest ||
		current.NamespacePolicyDigest != parent.NamespacePolicyDigest {
		return fmt.Errorf("prepared Temporal authority differs from baseline")
	}
	return nil
}

func readPreparedArchiveInventory(
	ctx context.Context,
	temporal LegacyWorkflowReader,
	namespace string,
	authority TemporalAuthority,
	parent baselineManifestV1,
) (LegacyWorkflowInventory, error) {
	if authority.HistoryArchivalState == "disabled" {
		return DisabledArchiveInventory(), nil
	}
	if authority.HistoryArchivalState != "enabled" {
		return LegacyWorkflowInventory{}, fmt.Errorf("prepared archive authority is unsupported")
	}
	current, err := ReadStableLegacyWorkflowInventory(ctx, temporal, namespace, true)
	if err != nil {
		return LegacyWorkflowInventory{}, err
	}
	known := baselineKnownRuns(parent)
	if len(known) == 0 || !sameLegacyRuns(known, current.Runs) {
		return LegacyWorkflowInventory{}, fmt.Errorf("prepared archive does not exactly retain baseline workflows")
	}
	return current, nil
}

func sameLegacyRuns(left, right []LegacyWorkflowRun) bool {
	if len(left) != len(right) {
		return false
	}
	byKey := make(map[string]LegacyWorkflowRun, len(left))
	for _, run := range left {
		key := run.WorkflowID + "\x00" + run.RunID
		if _, duplicate := byKey[key]; duplicate {
			return false
		}
		byKey[key] = run
	}
	for _, run := range right {
		key := run.WorkflowID + "\x00" + run.RunID
		if expected, ok := byKey[key]; !ok || expected != run {
			return false
		}
		delete(byKey, key)
	}
	return len(byKey) == 0
}

func validateCommittedPrepared(
	event *store.AgentFirstRetentionAttestationEvent,
	input store.AgentFirstRetentionAttestationInput,
	snapshot *store.AgentFirstRetentionAuditSnapshot,
) error {
	if event == nil || event.ParentDigest == nil || *event.ParentDigest != input.ParentDigest {
		return fmt.Errorf("committed prepared parent differs")
	}
	parent := event.ParentDigest
	event.ParentDigest = nil
	err := validateCommittedBaseline(event, input, snapshot)
	event.ParentDigest = parent
	return err
}
