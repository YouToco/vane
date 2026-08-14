package agentfirstaudit

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	workflowservice "go.temporal.io/api/workflowservice/v1"

	"github.com/YouToco/vane/store"
	vaneworkflow "github.com/YouToco/vane/workflow"
)

func TestCollectBaselineClosesAuditAndAdoptsLostAppendResponse(t *testing.T) {
	fixture := newBaselineCollectorFixture(t)
	fixture.database.appendErr = errors.New("response lost")
	result, err := collectBaselineWithClock(t.Context(), fixture.database, fixture.temporal,
		fixture.clock, fixture.request)
	if err != nil {
		t.Fatal(err)
	}
	if result.Event == nil || result.Event.TemporalEvidenceDigest != result.Manifest.Digest ||
		result.EvidencePath != filepath.Join(fixture.request.EvidenceDirectory,
			result.Manifest.Digest+".json") || fixture.database.appendCalls != 1 ||
		fixture.database.loadCalls != 2 || fixture.database.readCalls != 2 {
		t.Fatalf("result=%+v database=%+v", result, fixture.database)
	}
	if payload, err := os.ReadFile(result.EvidencePath); err != nil ||
		!bytes.Equal(payload, result.Manifest.Canonical) {
		t.Fatalf("persisted=%q err=%v", payload, err)
	}
}

func TestCollectBaselineNormalizesDatabaseTimeAndAdoptsExactCommittedEvent(t *testing.T) {
	fixture := newBaselineCollectorFixture(t)
	first, err := collectBaselineWithClock(t.Context(), fixture.database, fixture.temporal,
		fixture.clock, fixture.request)
	if err != nil {
		t.Fatal(err)
	}
	if first.Event.TemporalServerWitness.Nanosecond()%int(time.Microsecond) != 0 ||
		fixture.database.appendCalls != 1 || fixture.database.loadCalls != 1 {
		t.Fatalf("first=%+v database=%+v", first.Event, fixture.database)
	}

	second, err := collectBaselineWithClock(t.Context(), fixture.database, fixture.temporal,
		fixture.clock, fixture.request)
	if err != nil {
		t.Fatal(err)
	}
	if second.Event.PayloadDigest != first.Event.PayloadDigest ||
		fixture.database.appendCalls != 1 || fixture.database.loadCalls != 2 {
		t.Fatalf("second=%+v database=%+v", second.Event, fixture.database)
	}
}

func TestCollectBaselineAdoptsAfterCallerCancellation(t *testing.T) {
	fixture := newBaselineCollectorFixture(t)
	ctx, cancel := context.WithCancel(t.Context())
	fixture.database.appendHook = cancel
	fixture.database.appendErr = errors.New("response lost after commit")
	result, err := collectBaselineWithClock(ctx, fixture.database, fixture.temporal,
		fixture.clock, fixture.request)
	if err != nil {
		t.Fatal(err)
	}
	if result.Event == nil || fixture.database.loadContextErr != nil {
		t.Fatalf("result=%+v load context=%v", result, fixture.database.loadContextErr)
	}
}

func TestCollectBaselineRejectsDatabaseDriftBeforeAppend(t *testing.T) {
	fixture := newBaselineCollectorFixture(t)
	fixture.database.driftSecondRead = true
	if _, err := collectBaselineWithClock(t.Context(), fixture.database, fixture.temporal,
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
	if _, err := collectBaselineWithClock(t.Context(), fixture.database, fixture.temporal,
		fixture.clock, fixture.request); err == nil {
		t.Fatal("schedule parity mismatch accepted")
	}
	if fixture.database.appendCalls != 0 {
		t.Fatal("schedule mismatch reached append")
	}
}

func TestCollectBaselineRejectsUncalibratedEmptyEnabledArchive(t *testing.T) {
	fixture := newBaselineCollectorFixture(t)
	fixture.temporal.namespace.Config.HistoryArchivalState = enumspb.ARCHIVAL_STATE_ENABLED
	fixture.temporal.namespace.Config.VisibilityArchivalState = enumspb.ARCHIVAL_STATE_ENABLED
	fixture.temporal.namespace.Config.HistoryArchivalUri = "s3://history"
	fixture.temporal.namespace.Config.VisibilityArchivalUri = "s3://visibility"
	if _, err := collectBaselineWithClock(t.Context(), fixture.database, fixture.temporal,
		fixture.clock, fixture.request); err == nil {
		t.Fatal("empty enabled archive was treated as calibrated")
	}
	if fixture.database.appendCalls != 0 {
		t.Fatal("uncalibrated archive reached append")
	}
}

func TestCollectBaselineRejectsMismatchedCommittedEvent(t *testing.T) {
	for name, mutate := range map[string]func(*store.AgentFirstRetentionAttestationEvent){
		"namespace": func(event *store.AgentFirstRetentionAttestationEvent) {
			event.TemporalNamespace = "other"
		},
		"payload digest": func(event *store.AgentFirstRetentionAttestationEvent) {
			event.CanonicalPayload = []byte(`{"different":true}`)
		},
	} {
		t.Run(name, func(t *testing.T) {
			fixture := newBaselineCollectorFixture(t)
			fixture.database.mutateCommitted = mutate
			if _, err := collectBaselineWithClock(t.Context(), fixture.database, fixture.temporal,
				fixture.clock, fixture.request); err == nil {
				t.Fatal("mismatched committed event accepted")
			}
		})
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
			TaskQueue: "vane", ObservedAtUTC: time.Date(2026, 8, 13, 18, 0, 0, 471, time.UTC),
			HistoryDigest: strings.Repeat("b", 64), EventCount: 5,
			WorkerBuildID: "vane/" + source,
		}, nil
	}
	directory := filepath.Join(t.TempDir(), "evidence")
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	receipt := ReleaseReceipt{
		SchemaVersion: "vane.release-receipt/v1", SourceRevision: source,
		ControlPlaneRevision: strings.Repeat("f", 40), DeployRunID: "123456",
		BuildRunAttempt:             1,
		BackendArchiveDigest:        strings.Repeat("1", 64),
		BackendManifestDigest:       strings.Repeat("2", 64),
		ServerReleaseContractDigest: strings.Repeat("3", 64),
		VaneDigest:                  strings.Repeat("4", 64),
		CollectorDigest:             strings.Repeat("5", 64),
	}
	canonicalReceipt, err := json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	return baselineCollectorFixture{
		database: database, temporal: temporal, clock: clock,
		request: BaselineCollectorRequest{
			Namespace: "vane", TaskQueue: "vane",
			OperationID:    "123e4567-e89b-42d3-a456-426614174010",
			SourceRevision: source,
			Release: VerifiedReleaseReceipt{
				receipt: receipt, canonical: canonicalReceipt,
				deployDigest: digestBytes(canonicalReceipt),
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
	appendHook      func()
	loadContextErr  error
	committed       *store.AgentFirstRetentionAttestationEvent
	events          map[string]*store.AgentFirstRetentionAttestationEvent
	mutateCommitted func(*store.AgentFirstRetentionAttestationEvent)
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
	canonicalPayload, _ := json.Marshal(struct {
		Evidence string `json:"evidence"`
		Parent   string `json:"parent"`
		Phase    string `json:"phase"`
	}{input.TemporalEvidenceDigest, input.ParentDigest, string(input.Phase)})
	fake.committed = &store.AgentFirstRetentionAttestationEvent{
		ID: 1, Phase: input.Phase,
		TemporalClusterID:          input.TemporalClusterID,
		TemporalNamespace:          input.TemporalNamespace,
		TemporalNamespaceID:        input.TemporalNamespaceID,
		RetentionSeconds:           input.RetentionSeconds,
		HistoryArchivalState:       input.HistoryArchivalState,
		HistoryArchiveURIDigest:    input.HistoryArchiveURIDigest,
		VisibilityArchivalState:    input.VisibilityArchivalState,
		VisibilityArchiveURIDigest: input.VisibilityArchiveURIDigest,
		TemporalServerWitness:      input.TemporalServerWitness,
		WorkflowInventoryDigest:    input.WorkflowInventoryDigest,
		ScheduleInventoryDigest:    input.ScheduleInventoryDigest,
		ArchiveInventoryDigest:     input.ArchiveInventoryDigest,
		TemporalEvidenceDigest:     input.TemporalEvidenceDigest,
		SourceRevision:             input.SourceRevision, DeployDigest: input.DeployDigest,
		DatabaseIdentity:       []byte(`{"schema_version":"fixture/v1"}`),
		LegacyDBSnapshot:       bytes.Clone(fake.snapshot.LegacyDBSnapshot),
		LegacyDBSnapshotDigest: fake.snapshot.LegacyDBSnapshotDigest,
		CanonicalPayload:       canonicalPayload,
		IssuedAt:               time.Date(2026, 8, 13, 18, 0, 1, 0, time.UTC),
		ExpiresAt:              time.Date(2026, 8, 13, 19, 0, 1, 0, time.UTC),
	}
	fake.committed.PayloadDigest = digestBytes(fake.committed.CanonicalPayload)
	if input.ParentDigest != "" {
		parent := input.ParentDigest
		fake.committed.ParentDigest = &parent
	}
	if fake.mutateCommitted != nil {
		fake.mutateCommitted(fake.committed)
	}
	if fake.events == nil {
		fake.events = make(map[string]*store.AgentFirstRetentionAttestationEvent)
	}
	fake.events[fake.committed.PayloadDigest] = fake.committed
	if fake.appendHook != nil {
		fake.appendHook()
	}
	if fake.appendErr != nil {
		return nil, fake.appendErr
	}
	return fake.committed, nil
}

func (fake *baselineStoreFake) LoadAgentFirstRetentionAttestationByDigest(
	_ context.Context,
	digest string,
) (*store.AgentFirstRetentionAttestationEvent, error) {
	if event := fake.events[digest]; event != nil {
		return event, nil
	}
	return nil, store.ErrAgentFirstRetentionAttestationNotFound
}

func (fake *baselineStoreFake) LoadAgentFirstRetentionAttestation(
	ctx context.Context,
	input store.AgentFirstRetentionAttestationInput,
) (*store.AgentFirstRetentionAttestationEvent, error) {
	fake.loadCalls++
	fake.loadContextErr = ctx.Err()
	if fake.committed == nil {
		return nil, store.ErrAgentFirstRetentionAttestationNotFound
	}
	if !fake.committed.TemporalServerWitness.Equal(input.TemporalServerWitness) ||
		fake.committed.TemporalEvidenceDigest != input.TemporalEvidenceDigest {
		return nil, store.ErrAgentFirstRetentionAttestationNotFound
	}
	return fake.committed, nil
}
