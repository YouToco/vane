package agentfirstaudit

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/YouToco/vane/server/internal/strictjson"
)

type PreparedManifestInput struct {
	ParentAttestationDigest string
	ParentEvidenceDigest    string
	Parent                  baselineManifestV1
	Observation             BaselineManifestInput
}

type preparedManifestV1 struct {
	BaselineKnownRunCount   int                `json:"baseline_known_run_count"`
	BaselineKnownRunDigest  string             `json:"baseline_known_run_digest"`
	Observation             baselineManifestV1 `json:"observation"`
	ParentAttestationDigest string             `json:"parent_attestation_digest"`
	ParentEvidenceDigest    string             `json:"parent_evidence_digest"`
	SchemaVersion           string             `json:"schema_version"`
}

// DecodeBaselineEvidence accepts only the exact canonical representation that
// BuildBaselineManifest emits. Prepared collection never trusts a filename or
// a successfully decoded but non-canonical JSON object as retained evidence.
func DecodeBaselineEvidence(payload []byte, digest string) (baselineManifestV1, error) {
	if !validLowerHex(digest, sha256.Size) || len(payload) == 0 {
		return baselineManifestV1{}, fmt.Errorf("baseline evidence identity is invalid")
	}
	actual := sha256.Sum256(payload)
	if hex.EncodeToString(actual[:]) != digest {
		return baselineManifestV1{}, fmt.Errorf("baseline evidence digest differs")
	}
	var manifest baselineManifestV1
	if err := strictjson.DecodeExact(payload, &manifest); err != nil {
		return baselineManifestV1{}, fmt.Errorf("decode exact baseline evidence: %w", err)
	}
	rebuilt, err := rebuildBaselineManifest(manifest)
	if err != nil {
		return baselineManifestV1{}, err
	}
	if rebuilt.Digest != digest || !bytes.Equal(rebuilt.Canonical, payload) {
		return baselineManifestV1{}, fmt.Errorf("baseline evidence is not canonical")
	}
	return manifest, nil
}

func BuildPreparedManifest(input PreparedManifestInput) (BaselineManifest, error) {
	if !validLowerHex(input.ParentAttestationDigest, sha256.Size) ||
		!validLowerHex(input.ParentEvidenceDigest, sha256.Size) {
		return BaselineManifest{}, fmt.Errorf("prepared parent evidence is invalid")
	}
	parentRebuilt, err := rebuildBaselineManifest(input.Parent)
	if err != nil || parentRebuilt.Digest != input.ParentEvidenceDigest {
		return BaselineManifest{}, fmt.Errorf("prepared parent manifest differs")
	}
	observation, err := BuildBaselineManifest(input.Observation)
	if err != nil {
		return BaselineManifest{}, fmt.Errorf("build prepared observation: %w", err)
	}
	var observed baselineManifestV1
	if err := strictjson.DecodeExact(observation.Canonical, &observed); err != nil {
		return BaselineManifest{}, fmt.Errorf("decode prepared observation: %w", err)
	}
	knownRuns := append([]baselineWorkflowV1(nil), input.Parent.StandardWorkflowRuns...)
	knownRuns = append(knownRuns, input.Parent.ArchivedWorkflowRuns...)
	sort.Slice(knownRuns, func(i, j int) bool {
		if knownRuns[i].WorkflowID == knownRuns[j].WorkflowID {
			return knownRuns[i].RunID < knownRuns[j].RunID
		}
		return knownRuns[i].WorkflowID < knownRuns[j].WorkflowID
	})
	knownDigest, err := digestCanonical(struct {
		Runs          []baselineWorkflowV1 `json:"runs"`
		SchemaVersion string               `json:"schema_version"`
	}{knownRuns, "vane.agent-first-baseline-known-runs/v1"})
	if err != nil {
		return BaselineManifest{}, fmt.Errorf("digest baseline-known runs: %w", err)
	}
	manifest := preparedManifestV1{
		SchemaVersion:           "vane.agent-first-retention-prepared/v1",
		ParentAttestationDigest: input.ParentAttestationDigest,
		ParentEvidenceDigest:    input.ParentEvidenceDigest,
		BaselineKnownRunCount:   len(knownRuns), BaselineKnownRunDigest: knownDigest,
		Observation: observed,
	}
	payload, err := json.Marshal(manifest)
	if err != nil {
		return BaselineManifest{}, fmt.Errorf("marshal prepared evidence: %w", err)
	}
	sum := sha256.Sum256(payload)
	return BaselineManifest{Canonical: payload, Digest: hex.EncodeToString(sum[:])}, nil
}

func rebuildBaselineManifest(manifest baselineManifestV1) (BaselineManifest, error) {
	clockTime, err := time.Parse("2006-01-02T15:04:05.999999999Z", manifest.Clock.ObservedAtUTC)
	if err != nil || clockTime.Format("2006-01-02T15:04:05.999999999Z") != manifest.Clock.ObservedAtUTC {
		return BaselineManifest{}, fmt.Errorf("baseline clock timestamp is not canonical")
	}
	standard := baselineRunsToInventory(false, manifest.StandardWorkflowRuns,
		manifest.StandardWorkflowInventory)
	archived := baselineRunsToInventory(true, manifest.ArchivedWorkflowRuns,
		manifest.ArchiveInventory)
	schedules := ScheduleInventory{Count: manifest.ScheduleInventory.Count,
		Digest: manifest.ScheduleInventory.Digest}
	for _, item := range manifest.Schedules {
		schedules.Items = append(schedules.Items, ScheduleAuthority{
			ScheduleID: item.ScheduleID, WorkflowType: item.WorkflowType,
			ActionDigest: item.ActionDigest, DescriptionDigest: item.DescriptionDigest,
			Paused: item.Paused,
		})
	}
	return BuildBaselineManifest(BaselineManifestInput{
		Temporal: TemporalAuthority{
			ClusterID: manifest.TemporalClusterID, Namespace: manifest.Namespace,
			NamespaceID: manifest.NamespaceID, RetentionSeconds: manifest.RetentionSeconds,
			HistoryArchivalState:       manifest.HistoryArchivalState,
			HistoryArchiveURIDigest:    manifest.HistoryArchiveURIDigest,
			VisibilityArchivalState:    manifest.VisibilityArchivalState,
			VisibilityArchiveURIDigest: manifest.VisibilityArchiveURIDigest,
			ClusterDigest:              manifest.ClusterDigest,
			NamespacePolicyDigest:      manifest.NamespacePolicyDigest,
		},
		Clock: RetentionClockEvidence{
			Namespace: manifest.Namespace, WorkflowID: manifest.Clock.WorkflowID,
			RunID: manifest.Clock.RunID, TaskQueue: manifest.Clock.TaskQueue,
			ObservedAtUTC: clockTime, HistoryDigest: manifest.Clock.HistoryDigest,
			EventCount: manifest.Clock.EventCount, WorkerBuildID: manifest.Clock.WorkerBuildID,
		},
		StandardWorkflows: standard, ArchivedWorkflows: archived, Schedules: schedules,
		LegacyDBSnapshotDigest: manifest.LegacyDBSnapshotDigest,
		DatabaseScheduleDigest: manifest.DatabaseScheduleDigest,
		SourceRevision:         manifest.SourceRevision, DeployDigest: manifest.DeployDigest,
	})
}

func baselineRunsToInventory(
	archived bool,
	runs []baselineWorkflowV1,
	inventory baselineInventoryV1,
) LegacyWorkflowInventory {
	result := LegacyWorkflowInventory{Archived: archived, Count: inventory.Count,
		Digest: inventory.Digest}
	for _, run := range runs {
		result.Runs = append(result.Runs, LegacyWorkflowRun{
			WorkflowID: run.WorkflowID, RunID: run.RunID, Status: run.Status,
			AuthorityDigest: run.AuthorityDigest, HistoryDigest: run.HistoryDigest,
			HistoryEvents: run.HistoryEvents,
		})
	}
	return result
}

func baselineKnownRuns(manifest baselineManifestV1) []LegacyWorkflowRun {
	standard := baselineRunsToInventory(false, manifest.StandardWorkflowRuns,
		manifest.StandardWorkflowInventory)
	archived := baselineRunsToInventory(true, manifest.ArchivedWorkflowRuns,
		manifest.ArchiveInventory)
	return append(standard.Runs, archived.Runs...)
}
