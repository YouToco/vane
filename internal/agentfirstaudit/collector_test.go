package agentfirstaudit

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	workflowservice "go.temporal.io/api/workflowservice/v1"

	"github.com/YouToco/vane/store"
	vaneworkflow "github.com/YouToco/vane/workflow"
)

func TestCollectBaselineClosesAuditAndAdoptsLostAppendResponse(t *testing.T) {
	fixture := newBaselineCollectorFixture(t)
	fixture.database.appendErr = errors.New("response lost")
	result, err := CollectBaseline(t.Context(), fixture.database, fixture.temporal,
		fixture.clock, fixture.request)
	if err != nil {
		t.Fatal(err)
	}
	if result.Event == nil || result.Event.TemporalEvidenceDigest != result.Manifest.Digest ||
		result.EvidencePath != filepath.Join(fixture.request.EvidenceDirectory,
			result.Manifest.Digest+".json") || fixture.database.appendCalls != 1 ||
		fixture.database.loadCalls != 1 || fixture.database.readCalls != 2 {
		t.Fatalf("result=%+v database=%+v", result, fixture.database)
	}
	if payload, err := os.ReadFile(result.EvidencePath); err != nil ||
		!bytes.Equal(payload, result.Manifest.Canonical) {
		t.Fatalf("persisted=%q err=%v", payload, err)
	}
}

func TestCollectBaselineRejectsDatabaseDriftBeforeAppend(t *testing.T) {
	fixture := newBaselineCollectorFixture(t)
	fixture.database.driftSecondRead = true
	if _, err := CollectBaseline(t.Context(), fixture.database, fixture.temporal,
		fixture.clock, fixture.request); err == nil {
		t.Fatal("database drift accepted")
	}
	if fixture.database.appendCalls != 0 {
		t.Fatal("drifting evidence reached append")
	}
}

func TestCollectBaselineRejectsScheduleParityMismatchBeforeAppend(t *testing.T) {
	fixture := newBaselineCollectorFixture(t)
	digest := strings.Repeat("9", 64)
	fixture.database.snapshot.Schedules = []store.AgentFirstRetentionSchedule{{
		ID: "orphan", Status: "active", TargetActionDigest: &digest,
	}}
	if _, err := CollectBaseline(t.Context(), fixture.database, fixture.temporal,
		fixture.clock, fixture.request); err == nil {
		t.Fatal("schedule parity mismatch accepted")
	}
	if fixture.database.appendCalls != 0 {
		t.Fatal("schedule mismatch reached append")
	}
}

type baselineCollectorFixture struct {
	database *baselineStoreFake
	temporal *baselineTemporalFake
	clock    BaselineClockRunner
	request  BaselineCollectorRequest
}

func newBaselineCollectorFixture(t *testing.T) baselineCollectorFixture {
	t.Helper()
	source := strings.Repeat("a", 40)
	legacy := validLegacyWorkflowReader()
	clockID := "retention-clock-baseline"
	clockRunID := "123e4567-e89b-42d3-a456-426614174009"
	clockInfo := &workflowpb.WorkflowExecutionInfo{
		Execution: &commonpb.WorkflowExecution{WorkflowId: clockID, RunId: clockRunID},
		Type:      &commonpb.WorkflowType{Name: vaneworkflow.AgentFirstRetentionClockWorkflowNameV1},
	}
	legacy.standardPages = map[string]*workflowservice.ListWorkflowExecutionsResponse{
		"": {Executions: []*workflowpb.WorkflowExecutionInfo{clockInfo}},
	}
	legacy.archivedPages = map[string]*workflowservice.ListArchivedWorkflowExecutionsResponse{"": {}}
	schedules := validScheduleInventoryReader()
	schedules.pages = map[string]*workflowservice.ListSchedulesResponse{"": {}}
	schedules.descriptions = map[string]*workflowservice.DescribeScheduleResponse{}
	namespaceReader := validNamespaceReader()
	namespaceReader.namespace.NamespaceInfo.Name = "vane"
	temporal := &baselineTemporalFake{
		namespaceReaderFake:         namespaceReader,
		legacyWorkflowReaderFake:    legacy,
		scheduleInventoryReaderFake: schedules,
	}
	legacySnapshot := []byte(`{"schema_version":"fixture/v1"}`)
	legacyDigest := digestBytes(legacySnapshot)
	scheduleDigest := digestBytes([]byte("[]"))
	database := &baselineStoreFake{snapshot: &store.AgentFirstRetentionAuditSnapshot{
		LegacyDBSnapshot: legacySnapshot, LegacyDBSnapshotDigest: legacyDigest,
		ScheduleDigest: scheduleDigest,
	}}
	clock := func(_ context.Context, authority TemporalAuthority) (RetentionClockEvidence, error) {
		return RetentionClockEvidence{
			Namespace: authority.Namespace, WorkflowID: clockID, RunID: clockRunID,
			TaskQueue: "vane", ObservedAtUTC: time.Date(2026, 8, 13, 18, 0, 0, 0, time.UTC),
			HistoryDigest: strings.Repeat("b", 64), EventCount: 5,
			WorkerBuildID: "vane/" + source,
		}, nil
	}
	directory := filepath.Join(t.TempDir(), "evidence")
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	return baselineCollectorFixture{
		database: database, temporal: temporal, clock: clock,
		request: BaselineCollectorRequest{
			Namespace: "vane", SourceRevision: source,
			Release: VerifiedReleaseReceipt{
				Receipt:      ReleaseReceipt{SourceRevision: source},
				DeployDigest: strings.Repeat("e", 64),
			},
			EvidenceDirectory: directory,
		},
	}
}

type baselineTemporalFake struct {
	*namespaceReaderFake
	*legacyWorkflowReaderFake
	*scheduleInventoryReaderFake
}

type baselineStoreFake struct {
	snapshot        *store.AgentFirstRetentionAuditSnapshot
	readCalls       int
	appendCalls     int
	loadCalls       int
	driftSecondRead bool
	appendErr       error
	committed       *store.AgentFirstRetentionAttestationEvent
}

func (fake *baselineStoreFake) ReadAgentFirstRetentionAuditSnapshot(
	context.Context,
) (*store.AgentFirstRetentionAuditSnapshot, error) {
	fake.readCalls++
	result := *fake.snapshot
	result.LegacyDBSnapshot = bytes.Clone(fake.snapshot.LegacyDBSnapshot)
	result.Schedules = append([]store.AgentFirstRetentionSchedule(nil), fake.snapshot.Schedules...)
	if fake.driftSecondRead && fake.readCalls == 2 {
		result.LegacyDBSnapshot = []byte(`{"drift":true}`)
		result.LegacyDBSnapshotDigest = digestBytes(result.LegacyDBSnapshot)
	}
	return &result, nil
}

func (fake *baselineStoreFake) AppendAgentFirstRetentionAttestation(
	_ context.Context,
	input store.AgentFirstRetentionAttestationInput,
) (*store.AgentFirstRetentionAttestationEvent, error) {
	fake.appendCalls++
	fake.committed = &store.AgentFirstRetentionAttestationEvent{
		ID: 1, Phase: input.Phase, TemporalEvidenceDigest: input.TemporalEvidenceDigest,
		LegacyDBSnapshot:       bytes.Clone(fake.snapshot.LegacyDBSnapshot),
		LegacyDBSnapshotDigest: fake.snapshot.LegacyDBSnapshotDigest,
	}
	if fake.appendErr != nil {
		return nil, fake.appendErr
	}
	return fake.committed, nil
}

func (fake *baselineStoreFake) LoadAgentFirstRetentionAttestation(
	context.Context,
	store.AgentFirstRetentionAttestationInput,
) (*store.AgentFirstRetentionAttestationEvent, error) {
	fake.loadCalls++
	if fake.committed == nil {
		return nil, errors.New("absent")
	}
	return fake.committed, nil
}
