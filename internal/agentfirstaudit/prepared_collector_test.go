package agentfirstaudit

import (
	"bytes"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/YouToco/vane/store"
)

func TestCollectPreparedBindsExactBaselineAndAdoptsLostResponse(t *testing.T) {
	fixture := newBaselineCollectorFixture(t)
	baseline, err := collectBaselineWithClock(t.Context(), fixture.database, fixture.temporal,
		fixture.clock, fixture.request)
	if err != nil {
		t.Fatal(err)
	}
	fixture.database.appendErr = errors.New("prepared response lost")
	result, err := collectPreparedWithClock(t.Context(), fixture.database, fixture.temporal,
		fixture.clock, PreparedCollectorRequest{
			BaselineCollectorRequest: fixture.request,
			ParentDigest:             baseline.Event.PayloadDigest,
		})
	if err != nil {
		t.Fatal(err)
	}
	if result.Event.Phase != store.AgentFirstRetentionPhasePrepared ||
		result.Event.ParentDigest == nil || *result.Event.ParentDigest != baseline.Event.PayloadDigest ||
		result.Event.TemporalServerWitness.Nanosecond()%int(time.Microsecond) != 0 ||
		fixture.database.appendCalls != 2 {
		t.Fatalf("result=%+v database=%+v", result, fixture.database)
	}
	if payload, err := os.ReadFile(result.EvidencePath); err != nil ||
		!bytes.Equal(payload, result.Manifest.Canonical) ||
		!bytes.Contains(payload, []byte(baseline.Event.PayloadDigest)) {
		t.Fatalf("prepared payload=%q err=%v", payload, err)
	}
}

func TestCollectPreparedRejectsWrongReleaseAndMissingEvidence(t *testing.T) {
	for name, mutate := range map[string]func(*baselineCollectorFixture, BaselineCollectorResult){
		"release": func(f *baselineCollectorFixture, _ BaselineCollectorResult) {
			f.request.Release.deployDigest = strings.Repeat("9", 64)
		},
		"evidence": func(_ *baselineCollectorFixture, baseline BaselineCollectorResult) {
			if err := os.Remove(baseline.EvidencePath); err != nil {
				t.Fatal(err)
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			fixture := newBaselineCollectorFixture(t)
			baseline, err := collectBaselineWithClock(t.Context(), fixture.database,
				fixture.temporal, fixture.clock, fixture.request)
			if err != nil {
				t.Fatal(err)
			}
			mutate(&fixture, baseline)
			if _, err := collectPreparedWithClock(t.Context(), fixture.database,
				fixture.temporal, fixture.clock, PreparedCollectorRequest{
					BaselineCollectorRequest: fixture.request,
					ParentDigest:             baseline.Event.PayloadDigest,
				}); err == nil {
				t.Fatal("invalid prepared parent accepted")
			}
			if fixture.database.appendCalls != 1 {
				t.Fatal("invalid prepared evidence reached append")
			}
		})
	}
}

func TestCollectPreparedDoesNotReviveStaleExactEvent(t *testing.T) {
	fixture := newBaselineCollectorFixture(t)
	baseline, err := collectBaselineWithClock(t.Context(), fixture.database, fixture.temporal,
		fixture.clock, fixture.request)
	if err != nil {
		t.Fatal(err)
	}
	fixture.database.loadErr = store.ErrAgentFirstRetentionAttestationStale
	if _, err := collectPreparedWithClock(t.Context(), fixture.database, fixture.temporal,
		fixture.clock, PreparedCollectorRequest{
			BaselineCollectorRequest: fixture.request,
			ParentDigest:             baseline.Event.PayloadDigest,
		}); !errors.Is(err, store.ErrAgentFirstRetentionAttestationStale) {
		t.Fatalf("stale prepared error=%v", err)
	}
	if fixture.database.appendCalls != 1 {
		t.Fatal("stale exact prepared authorized a replacement append")
	}
}

func TestDecodeBaselineEvidenceRejectsRepresentationMutation(t *testing.T) {
	manifest, err := BuildBaselineManifest(validBaselineManifestInput(t))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeBaselineEvidence(manifest.Canonical, manifest.Digest); err != nil {
		t.Fatal(err)
	}
	mutated := append([]byte(" "), manifest.Canonical...)
	if _, err := DecodeBaselineEvidence(mutated, digestBytes(mutated)); err == nil {
		t.Fatal("non-canonical baseline evidence accepted")
	}
}

func TestCollectPreparedRequiresDirectPhysicalAbsenceForEveryBaselineRun(t *testing.T) {
	for name, descriptionAbsent := range map[string]bool{
		"absent": true, "still-readable": false,
	} {
		t.Run(name, func(t *testing.T) {
			fixture := newBaselineCollectorFixture(t)
			authority, err := ReadTemporalAuthority(t.Context(), fixture.temporal, "vane")
			if err != nil {
				t.Fatal(err)
			}
			run := validBaselineWorkflowRun()
			standardDigest, err := digestLegacyWorkflowInventory(false, []LegacyWorkflowRun{run})
			if err != nil {
				t.Fatal(err)
			}
			scheduleDigest, err := digestScheduleInventory(nil)
			if err != nil {
				t.Fatal(err)
			}
			clock, err := fixture.clock(t.Context(), authority)
			if err != nil {
				t.Fatal(err)
			}
			manifest, err := BuildBaselineManifest(BaselineManifestInput{
				Temporal: authority, Clock: clock,
				StandardWorkflows: LegacyWorkflowInventory{
					Count: 1, Digest: standardDigest, Runs: []LegacyWorkflowRun{run},
				},
				ArchivedWorkflows:      DisabledArchiveInventory(),
				Schedules:              ScheduleInventory{Digest: scheduleDigest},
				LegacyDBSnapshotDigest: fixture.database.snapshot.LegacyDBSnapshotDigest,
				DatabaseScheduleDigest: fixture.database.snapshot.ScheduleDigest,
				SourceRevision:         fixture.request.SourceRevision,
				DeployDigest:           fixture.request.Release.DeployDigest(),
			})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := PersistCanonicalEvidence(fixture.request.EvidenceDirectory,
				manifest.Digest, manifest.Canonical); err != nil {
				t.Fatal(err)
			}
			parent, err := fixture.database.AppendAgentFirstRetentionAttestation(t.Context(),
				store.AgentFirstRetentionAttestationInput{
					Phase:             store.AgentFirstRetentionPhaseBaseline,
					TemporalClusterID: authority.ClusterID, TemporalNamespace: authority.Namespace,
					TemporalNamespaceID:        authority.NamespaceID,
					RetentionSeconds:           authority.RetentionSeconds,
					HistoryArchivalState:       store.AgentFirstArchivalDisabled,
					HistoryArchiveURIDigest:    authority.HistoryArchiveURIDigest,
					VisibilityArchivalState:    store.AgentFirstArchivalDisabled,
					VisibilityArchiveURIDigest: authority.VisibilityArchiveURIDigest,
					TemporalServerWitness:      clock.ObservedAtUTC.UTC().Truncate(time.Microsecond),
					WorkflowInventoryDigest:    standardDigest,
					ScheduleInventoryDigest:    scheduleDigest,
					ArchiveInventoryDigest:     DisabledArchiveInventory().Digest,
					TemporalEvidenceDigest:     manifest.Digest,
					SourceRevision:             fixture.request.SourceRevision,
					DeployDigest:               fixture.request.Release.DeployDigest(),
				})
			if err != nil {
				t.Fatal(err)
			}
			fixture.temporal.describeNotFound = descriptionAbsent
			fixture.temporal.historyNotFound = descriptionAbsent
			if !descriptionAbsent {
				fixture.temporal.infos[run.WorkflowID] = legacyWorkflowInfoFixture(
					run.WorkflowID, run.RunID)
			}
			_, err = collectPreparedWithClock(t.Context(), fixture.database,
				fixture.temporal, fixture.clock, PreparedCollectorRequest{
					BaselineCollectorRequest: fixture.request,
					ParentDigest:             parent.PayloadDigest,
				})
			if descriptionAbsent && err != nil {
				t.Fatal(err)
			}
			if !descriptionAbsent && err == nil {
				t.Fatal("readable baseline execution passed physical-absence gate")
			}
		})
	}
}
