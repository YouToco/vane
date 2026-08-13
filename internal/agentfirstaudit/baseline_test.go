package agentfirstaudit

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func TestBuildBaselineManifestBindsEveryInventoryItem(t *testing.T) {
	input := validBaselineManifestInput(t)
	first, err := BuildBaselineManifest(input)
	if err != nil {
		t.Fatal(err)
	}
	second, err := BuildBaselineManifest(input)
	if err != nil {
		t.Fatal(err)
	}
	if first.Digest != second.Digest || !bytes.Equal(first.Canonical, second.Canonical) ||
		!validLowerHex(first.Digest, 32) {
		t.Fatal("baseline manifest is not deterministic")
	}
	if bytes.Contains(first.Canonical, []byte("s3://")) {
		t.Fatal("baseline manifest exposed an archive URI")
	}

	for _, tc := range []struct {
		name   string
		mutate func(*BaselineManifestInput)
	}{
		{"standard digest", func(in *BaselineManifestInput) {
			in.StandardWorkflows.Digest = strings.Repeat("1", 64)
		}},
		{"standard item", func(in *BaselineManifestInput) {
			in.StandardWorkflows.Runs = append(in.StandardWorkflows.Runs, validBaselineWorkflowRun())
			in.StandardWorkflows.Count++
		}},
		{"schedule item", func(in *BaselineManifestInput) {
			in.Schedules.Items = append(in.Schedules.Items, ScheduleAuthority{
				ScheduleID: "schedule-1", WorkflowType: "ResearchScheduledWorkflowV3",
				ActionDigest: strings.Repeat("2", 64), DescriptionDigest: strings.Repeat("3", 64),
			})
			in.Schedules.Count++
		}},
		{"clock build", func(in *BaselineManifestInput) { in.Clock.WorkerBuildID = "vane/wrong" }},
		{"mixed archive", func(in *BaselineManifestInput) {
			in.Temporal.VisibilityArchivalState = "enabled"
		}},
		{"disabled archive inventory", func(in *BaselineManifestInput) {
			in.ArchivedWorkflows.Digest = strings.Repeat("4", 64)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			candidate := validBaselineManifestInput(t)
			tc.mutate(&candidate)
			if _, err := BuildBaselineManifest(candidate); err == nil {
				t.Fatal("mutated baseline evidence accepted")
			}
		})
	}
}

func TestBuildBaselineManifestAcceptsBoundEnabledArchiveInventory(t *testing.T) {
	input := validBaselineManifestInput(t)
	input.Temporal.HistoryArchivalState = "enabled"
	input.Temporal.VisibilityArchivalState = "enabled"
	input.Temporal.HistoryArchiveURIDigest = strings.Repeat("5", 64)
	input.Temporal.VisibilityArchiveURIDigest = strings.Repeat("6", 64)
	run := validBaselineWorkflowRun()
	digest, err := digestLegacyWorkflowInventory(true, []LegacyWorkflowRun{run})
	if err != nil {
		t.Fatal(err)
	}
	input.ArchivedWorkflows = LegacyWorkflowInventory{
		Archived: true, Count: 1, Digest: digest, Runs: []LegacyWorkflowRun{run},
	}
	if _, err := BuildBaselineManifest(input); err != nil {
		t.Fatal(err)
	}
	input.ArchivedWorkflows.Runs[0].HistoryEvents++
	if _, err := BuildBaselineManifest(input); err == nil {
		t.Fatal("archive item mutation accepted under the original digest")
	}
}

func validBaselineManifestInput(t *testing.T) BaselineManifestInput {
	t.Helper()
	temporal, err := ReadTemporalAuthority(t.Context(), validNamespaceReader(), "default")
	if err != nil {
		t.Fatal(err)
	}
	standardDigest, err := digestLegacyWorkflowInventory(false, nil)
	if err != nil {
		t.Fatal(err)
	}
	scheduleDigest, err := digestScheduleInventory(nil)
	if err != nil {
		t.Fatal(err)
	}
	source := strings.Repeat("a", 40)
	return BaselineManifestInput{
		Temporal: temporal,
		Clock: RetentionClockEvidence{
			ObservedAtUTC: time.Date(2026, 8, 13, 18, 0, 0, 123, time.UTC),
			HistoryDigest: strings.Repeat("b", 64), EventCount: 5,
			WorkerBuildID: "vane/" + source,
		},
		ClockWorkflowID: "agent-first-retention-clock-baseline",
		ClockRunID:      "123e4567-e89b-42d3-a456-426614174001",
		ClockTaskQueue:  "vane",
		StandardWorkflows: LegacyWorkflowInventory{
			Archived: false, Count: 0, Digest: standardDigest,
		},
		ArchivedWorkflows:      DisabledArchiveInventory(),
		Schedules:              ScheduleInventory{Digest: scheduleDigest},
		LegacyDBSnapshotDigest: strings.Repeat("c", 64),
		DatabaseScheduleDigest: strings.Repeat("d", 64),
		SourceRevision:         source,
		DeployDigest:           strings.Repeat("e", 64),
	}
}

func validBaselineWorkflowRun() LegacyWorkflowRun {
	return LegacyWorkflowRun{
		WorkflowID: "legacy-1", RunID: "123e4567-e89b-42d3-a456-426614174002",
		Status: "Completed", AuthorityDigest: strings.Repeat("7", 64),
		HistoryDigest: strings.Repeat("8", 64), HistoryEvents: 6,
	}
}
