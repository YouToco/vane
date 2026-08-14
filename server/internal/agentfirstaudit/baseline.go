package agentfirstaudit

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"

	vaneworkflow "github.com/YouToco/vane/server/workflow"
)

type BaselineManifestInput struct {
	Temporal               TemporalAuthority
	Clock                  RetentionClockEvidence
	StandardWorkflows      LegacyWorkflowInventory
	ArchivedWorkflows      LegacyWorkflowInventory
	Schedules              ScheduleInventory
	LegacyDBSnapshotDigest string
	DatabaseScheduleDigest string
	SourceRevision         string
	DeployDigest           string
}

type BaselineManifest struct {
	Canonical []byte
	Digest    string
}

type baselineManifestV1 struct {
	ArchiveInventory           baselineInventoryV1  `json:"archive_inventory"`
	ArchivedWorkflowRuns       []baselineWorkflowV1 `json:"archived_workflow_runs"`
	Clock                      baselineClockV1      `json:"clock"`
	ClusterDigest              string               `json:"cluster_digest"`
	DatabaseScheduleDigest     string               `json:"database_schedule_digest"`
	DeployDigest               string               `json:"deploy_digest"`
	HistoryArchiveURIDigest    string               `json:"history_archive_uri_digest"`
	HistoryArchivalState       string               `json:"history_archival_state"`
	LegacyDBSnapshotDigest     string               `json:"legacy_db_snapshot_digest"`
	Namespace                  string               `json:"namespace"`
	NamespaceID                string               `json:"namespace_id"`
	NamespacePolicyDigest      string               `json:"namespace_policy_digest"`
	RetentionSeconds           int64                `json:"retention_seconds"`
	ScheduleInventory          baselineInventoryV1  `json:"schedule_inventory"`
	Schedules                  []baselineScheduleV1 `json:"schedules"`
	SchemaVersion              string               `json:"schema_version"`
	SourceRevision             string               `json:"source_revision"`
	StandardWorkflowInventory  baselineInventoryV1  `json:"standard_workflow_inventory"`
	StandardWorkflowRuns       []baselineWorkflowV1 `json:"standard_workflow_runs"`
	TemporalClusterID          string               `json:"temporal_cluster_id"`
	VisibilityArchiveURIDigest string               `json:"visibility_archive_uri_digest"`
	VisibilityArchivalState    string               `json:"visibility_archival_state"`
}

type baselineInventoryV1 struct {
	Count  int    `json:"count"`
	Digest string `json:"digest"`
}

type baselineClockV1 struct {
	EventCount    int    `json:"event_count"`
	HistoryDigest string `json:"history_digest"`
	ObservedAtUTC string `json:"observed_at_utc"`
	RunID         string `json:"run_id"`
	TaskQueue     string `json:"task_queue"`
	WorkerBuildID string `json:"worker_build_id"`
	WorkflowID    string `json:"workflow_id"`
}

type baselineWorkflowV1 struct {
	AuthorityDigest string `json:"authority_digest"`
	HistoryDigest   string `json:"history_digest"`
	HistoryEvents   int    `json:"history_events"`
	RunID           string `json:"run_id"`
	Status          string `json:"status"`
	WorkflowID      string `json:"workflow_id"`
}

type baselineScheduleV1 struct {
	ActionDigest      string `json:"action_digest"`
	DescriptionDigest string `json:"description_digest"`
	Paused            bool   `json:"paused"`
	ScheduleID        string `json:"schedule_id"`
	WorkflowType      string `json:"workflow_type"`
}

func DisabledArchiveInventory() LegacyWorkflowInventory {
	payload := []byte(`{"entries":[],"schema_version":"vane.agent-first-disabled-archive/v1","state":"disabled"}`)
	sum := sha256.Sum256(payload)
	digest := hex.EncodeToString(sum[:])
	return LegacyWorkflowInventory{Archived: true, Count: 0, Digest: digest, Runs: nil}
}

func BuildBaselineManifest(input BaselineManifestInput) (BaselineManifest, error) {
	temporal := input.Temporal
	if !validLowerHex(temporal.ClusterDigest, sha256.Size) ||
		!validLowerHex(temporal.NamespacePolicyDigest, sha256.Size) ||
		!canonicalUUID(temporal.NamespaceID) || temporal.RetentionSeconds <= 0 ||
		!validSourceRevision(input.SourceRevision) ||
		!validLowerHex(input.DeployDigest, sha256.Size) ||
		!validLowerHex(input.LegacyDBSnapshotDigest, sha256.Size) ||
		!validLowerHex(input.DatabaseScheduleDigest, sha256.Size) ||
		!validLowerHex(input.Clock.HistoryDigest, sha256.Size) || input.Clock.EventCount != 5 ||
		input.Clock.ObservedAtUTC.IsZero() ||
		input.Clock.WorkerBuildID != "vane/"+input.SourceRevision ||
		input.Clock.Namespace != temporal.Namespace ||
		!boundedCanonicalTemporalText(input.Clock.WorkflowID, maxTemporalAuthorityTextBytes) ||
		!canonicalUUID(input.Clock.RunID) ||
		!boundedCanonicalTemporalText(input.Clock.TaskQueue, maxTemporalAuthorityTextBytes) ||
		input.StandardWorkflows.Archived || input.StandardWorkflows.Count != len(input.StandardWorkflows.Runs) ||
		input.Schedules.Count != len(input.Schedules.Items) ||
		!validLowerHex(input.StandardWorkflows.Digest, sha256.Size) ||
		!validLowerHex(input.Schedules.Digest, sha256.Size) {
		return BaselineManifest{}, fmt.Errorf("baseline evidence is invalid")
	}
	standardDigest, err := digestLegacyWorkflowInventory(false, input.StandardWorkflows.Runs)
	if err != nil || standardDigest != input.StandardWorkflows.Digest {
		return BaselineManifest{}, fmt.Errorf("standard workflow inventory digest differs")
	}
	scheduleDigest, err := digestScheduleInventory(input.Schedules.Items)
	if err != nil || scheduleDigest != input.Schedules.Digest {
		return BaselineManifest{}, fmt.Errorf("schedule inventory digest differs")
	}
	if temporal.HistoryArchivalState != temporal.VisibilityArchivalState {
		return BaselineManifest{}, fmt.Errorf("baseline archival modes are mixed")
	}
	for _, run := range append(append([]LegacyWorkflowRun(nil),
		input.StandardWorkflows.Runs...), input.ArchivedWorkflows.Runs...) {
		if !boundedCanonicalTemporalText(run.WorkflowID, maxTemporalAuthorityTextBytes) ||
			!canonicalUUID(run.RunID) ||
			!boundedCanonicalTemporalText(run.Status, maxTemporalAuthorityTextBytes) ||
			!validLowerHex(run.AuthorityDigest, sha256.Size) ||
			!validLowerHex(run.HistoryDigest, sha256.Size) || run.HistoryEvents <= 0 {
			return BaselineManifest{}, fmt.Errorf("baseline workflow item is invalid")
		}
	}
	for _, schedule := range input.Schedules.Items {
		if !boundedCanonicalTemporalText(schedule.ScheduleID, maxTemporalAuthorityTextBytes) ||
			schedule.WorkflowType != vaneworkflow.ResearchScheduledWorkflowV3Name ||
			!validLowerHex(schedule.ActionDigest, sha256.Size) ||
			!validLowerHex(schedule.DescriptionDigest, sha256.Size) {
			return BaselineManifest{}, fmt.Errorf("baseline schedule item is invalid")
		}
	}
	switch temporal.HistoryArchivalState {
	case "disabled":
		if temporal.HistoryArchiveURIDigest != emptySHA256 ||
			temporal.VisibilityArchiveURIDigest != emptySHA256 {
			return BaselineManifest{}, fmt.Errorf("disabled archive URI digest differs")
		}
		disabled := DisabledArchiveInventory()
		if input.ArchivedWorkflows.Count != 0 || input.ArchivedWorkflows.Digest != disabled.Digest {
			return BaselineManifest{}, fmt.Errorf("disabled archive inventory differs")
		}
	case "enabled":
		if !validLowerHex(temporal.HistoryArchiveURIDigest, sha256.Size) ||
			!validLowerHex(temporal.VisibilityArchiveURIDigest, sha256.Size) ||
			temporal.HistoryArchiveURIDigest == emptySHA256 ||
			temporal.VisibilityArchiveURIDigest == emptySHA256 {
			return BaselineManifest{}, fmt.Errorf("enabled archive URI digest is invalid")
		}
		if !input.ArchivedWorkflows.Archived ||
			input.ArchivedWorkflows.Count != len(input.ArchivedWorkflows.Runs) ||
			!validLowerHex(input.ArchivedWorkflows.Digest, sha256.Size) {
			return BaselineManifest{}, fmt.Errorf("enabled archive inventory is invalid")
		}
		archiveDigest, err := digestLegacyWorkflowInventory(true, input.ArchivedWorkflows.Runs)
		if err != nil || archiveDigest != input.ArchivedWorkflows.Digest {
			return BaselineManifest{}, fmt.Errorf("archived workflow inventory digest differs")
		}
	default:
		return BaselineManifest{}, fmt.Errorf("baseline archival state is invalid")
	}
	manifest := baselineManifestV1{
		SchemaVersion:     "vane.agent-first-retention-baseline/v1",
		TemporalClusterID: temporal.ClusterID, Namespace: temporal.Namespace,
		NamespaceID: temporal.NamespaceID, RetentionSeconds: temporal.RetentionSeconds,
		ClusterDigest: temporal.ClusterDigest, NamespacePolicyDigest: temporal.NamespacePolicyDigest,
		HistoryArchivalState:       temporal.HistoryArchivalState,
		HistoryArchiveURIDigest:    temporal.HistoryArchiveURIDigest,
		VisibilityArchivalState:    temporal.VisibilityArchivalState,
		VisibilityArchiveURIDigest: temporal.VisibilityArchiveURIDigest,
		SourceRevision:             input.SourceRevision, DeployDigest: input.DeployDigest,
		LegacyDBSnapshotDigest: input.LegacyDBSnapshotDigest,
		DatabaseScheduleDigest: input.DatabaseScheduleDigest,
		StandardWorkflowInventory: baselineInventoryV1{
			Count: input.StandardWorkflows.Count, Digest: input.StandardWorkflows.Digest,
		},
		ArchiveInventory: baselineInventoryV1{
			Count: input.ArchivedWorkflows.Count, Digest: input.ArchivedWorkflows.Digest,
		},
		ScheduleInventory: baselineInventoryV1{
			Count: input.Schedules.Count, Digest: input.Schedules.Digest,
		},
		Clock: baselineClockV1{
			WorkflowID: input.Clock.WorkflowID, RunID: input.Clock.RunID,
			TaskQueue: input.Clock.TaskQueue, WorkerBuildID: input.Clock.WorkerBuildID,
			ObservedAtUTC: input.Clock.ObservedAtUTC.UTC().Format("2006-01-02T15:04:05.999999999Z"),
			HistoryDigest: input.Clock.HistoryDigest, EventCount: input.Clock.EventCount,
		},
	}
	standardRuns := append([]LegacyWorkflowRun(nil), input.StandardWorkflows.Runs...)
	sort.Slice(standardRuns, func(i, j int) bool {
		if standardRuns[i].WorkflowID == standardRuns[j].WorkflowID {
			return standardRuns[i].RunID < standardRuns[j].RunID
		}
		return standardRuns[i].WorkflowID < standardRuns[j].WorkflowID
	})
	archiveRuns := append([]LegacyWorkflowRun(nil), input.ArchivedWorkflows.Runs...)
	sort.Slice(archiveRuns, func(i, j int) bool {
		if archiveRuns[i].WorkflowID == archiveRuns[j].WorkflowID {
			return archiveRuns[i].RunID < archiveRuns[j].RunID
		}
		return archiveRuns[i].WorkflowID < archiveRuns[j].WorkflowID
	})
	schedules := append([]ScheduleAuthority(nil), input.Schedules.Items...)
	sort.Slice(schedules, func(i, j int) bool {
		return schedules[i].ScheduleID < schedules[j].ScheduleID
	})
	for _, run := range standardRuns {
		manifest.StandardWorkflowRuns = append(manifest.StandardWorkflowRuns,
			baselineWorkflowV1{
				WorkflowID: run.WorkflowID, RunID: run.RunID, Status: run.Status,
				AuthorityDigest: run.AuthorityDigest, HistoryDigest: run.HistoryDigest,
				HistoryEvents: run.HistoryEvents,
			})
	}
	for _, run := range archiveRuns {
		manifest.ArchivedWorkflowRuns = append(manifest.ArchivedWorkflowRuns,
			baselineWorkflowV1{
				WorkflowID: run.WorkflowID, RunID: run.RunID, Status: run.Status,
				AuthorityDigest: run.AuthorityDigest, HistoryDigest: run.HistoryDigest,
				HistoryEvents: run.HistoryEvents,
			})
	}
	for _, schedule := range schedules {
		manifest.Schedules = append(manifest.Schedules, baselineScheduleV1{
			ScheduleID: schedule.ScheduleID, WorkflowType: schedule.WorkflowType,
			ActionDigest:      schedule.ActionDigest,
			DescriptionDigest: schedule.DescriptionDigest, Paused: schedule.Paused,
		})
	}
	payload, err := json.Marshal(manifest)
	if err != nil {
		return BaselineManifest{}, fmt.Errorf("marshal baseline evidence: %w", err)
	}
	sum := sha256.Sum256(payload)
	return BaselineManifest{Canonical: payload, Digest: hex.EncodeToString(sum[:])}, nil
}
