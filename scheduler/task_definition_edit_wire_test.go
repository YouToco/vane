package scheduler

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/YouToco/vane/taskstate"
	"github.com/YouToco/vane/types"
	"github.com/YouToco/vane/workflow"
)

type taskDefinitionEditProposalComponentFixtureV1 struct {
	BaseDefinition   json.RawMessage `json:"base_definition"`
	TargetDefinition json.RawMessage `json:"target_definition"`
	PreparedEdit     json.RawMessage `json:"prepared_edit"`
	BaseSnapshot     json.RawMessage `json:"base_snapshot"`
}

func TestTaskDefinitionEditProposalComponentFixtureV1(t *testing.T) {
	fixture := newTaskDefinitionEditFixture(t, TaskDefinitionEditOriginalStateActive)
	target := changedTaskDefinitionEditDefinition(fixture.base, "proposal")
	baseDefinition := taskDefinitionEditApprovedFixture(t, fixture.creation.TaskID, fixture.base)
	targetDefinition := taskDefinitionEditApprovedFixture(t, fixture.creation.TaskID, target)
	baseDigest, err := taskstate.DigestApprovedDefinitionV1(baseDefinition)
	if err != nil {
		t.Fatalf("digest base approved definition: %v", err)
	}
	targetDigest, err := taskstate.DigestApprovedDefinitionV1(targetDefinition)
	if err != nil {
		t.Fatalf("digest target approved definition: %v", err)
	}
	prepared, snapshot := fixture.prepare(
		t,
		"edit-proposal-fixture",
		TaskDefinitionEditHead{Version: 1, Digest: baseDigest},
		TaskDefinitionEditHead{Version: 2, Digest: targetDigest},
		fixture.base,
		target,
	)
	baseBytes, err := taskstate.EncodeApprovedDefinitionV1(baseDefinition)
	if err != nil {
		t.Fatalf("encode base approved definition: %v", err)
	}
	targetBytes, err := taskstate.EncodeApprovedDefinitionV1(targetDefinition)
	if err != nil {
		t.Fatalf("encode target approved definition: %v", err)
	}
	preparedBytes, err := EncodePreparedTaskDefinitionEdit(prepared)
	if err != nil {
		t.Fatalf("encode prepared definition edit: %v", err)
	}
	snapshotBytes, err := EncodeTaskDefinitionEditBaseSnapshot(prepared, snapshot)
	if err != nil {
		t.Fatalf("encode definition edit base snapshot: %v", err)
	}
	generated, err := json.Marshal(taskDefinitionEditProposalComponentFixtureV1{
		BaseDefinition: baseBytes, TargetDefinition: targetBytes,
		PreparedEdit: preparedBytes, BaseSnapshot: snapshotBytes,
	})
	if err != nil {
		t.Fatalf("encode proposal component fixture: %v", err)
	}
	path := filepath.Join("..", "task", "testdata", "definition_edit_proposal_components_v1.json")
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read proposal component fixture %s: %v\ngenerated fixture:\n%s", path, err, generated)
	}
	want = bytes.TrimSuffix(want, []byte("\n"))
	if !bytes.Equal(generated, want) {
		t.Fatalf("proposal component fixture drifted; update only with an intentional wire migration\n got: %s\nwant: %s", generated, want)
	}
	if prepared.Creation.PreparedDigest == baseDigest {
		t.Fatal("fixture accidentally equates creation digest with current Approved base digest")
	}
}

func taskDefinitionEditApprovedFixture(
	t *testing.T,
	taskID string,
	definition TaskDefinitionEditDefinition,
) taskstate.ApprovedDefinitionV1 {
	t.Helper()
	spec, err := json.Marshal(definition.Spec)
	if err != nil {
		t.Fatalf("encode fixture schedule spec: %v", err)
	}
	scope, err := json.Marshal(definition.Scope)
	if err != nil {
		t.Fatalf("encode fixture push scope: %v", err)
	}
	sources := make([]taskstate.ApprovedSourceV1, 0, len(definition.Scope.SourceIDs))
	planSources := make([]taskstate.PlanSourceV1, 0, len(definition.Scope.SourceIDs))
	for _, sourceID := range definition.Scope.SourceIDs {
		query := fmt.Sprintf("source-%d", sourceID)
		config := json.RawMessage(fmt.Sprintf(`{"query":%q}`, query))
		planSource := taskstate.PlanSourceV1{
			Platform: types.PlatformWeb, Capability: types.CapSearch,
			Title: "搜索: " + query, URL: "vane://web/search?q=" + query, Config: config,
		}
		planSources = append(planSources, planSource)
		sources = append(sources, taskstate.ApprovedSourceV1{
			SourceID: sourceID, Platform: planSource.Platform, Capability: planSource.Capability,
			Title: planSource.Title, URL: planSource.URL, Config: config,
		})
	}
	plan, err := json.Marshal(taskstate.FetchPlanV1{Sources: planSources})
	if err != nil {
		t.Fatalf("encode fixture fetch plan: %v", err)
	}
	const intent = "监控 proposal fixture"
	approved, err := taskstate.BuildApprovedDefinitionV1(taskstate.ApprovedDefinitionInputV1{
		TenantID: 7, UserID: 42, TaskID: taskID,
		Intent: intent, NLDescription: definition.NLDescription,
		SpecJSON: spec, ScopeJSON: scope, PlaybookContent: intent,
		SourceScope: taskstate.SourceScopeApprovedPlan, FetchPlan: plan,
		Strictness: types.StrictnessNormal, Sources: sources,
		ExecutionMode:  types.ExecutionModeCompiled,
		DeliveryPolicy: taskstate.DeliveryPolicyOwnerFeishu,
		BudgetPolicy:   taskstate.BudgetPolicyInheritTenantQuota,
	})
	if err != nil {
		t.Fatalf("build fixture approved definition: %v", err)
	}
	return approved
}

func TestPreparedTaskDefinitionEditWire_ExactCanonicalRoundTrip(t *testing.T) {
	fixture := newTaskDefinitionEditFixture(t, TaskDefinitionEditOriginalStateActive)
	target := changedTaskDefinitionEditDefinition(fixture.base, "wire")
	prepared, snapshot := fixture.prepare(
		t,
		"edit-wire-roundtrip",
		taskDefinitionEditHead(1, "a"),
		taskDefinitionEditHead(2, "b"),
		fixture.base,
		target,
	)

	preparedWire, err := EncodePreparedTaskDefinitionEdit(prepared)
	if err != nil {
		t.Fatalf("EncodePreparedTaskDefinitionEdit() error = %v", err)
	}
	decodedPrepared, err := DecodePreparedTaskDefinitionEdit(preparedWire)
	if err != nil {
		t.Fatalf("DecodePreparedTaskDefinitionEdit() error = %v", err)
	}
	if !reflect.DeepEqual(decodedPrepared, prepared) {
		t.Fatalf("decoded prepared edit differs\n got: %+v\nwant: %+v", decodedPrepared, prepared)
	}
	request := taskDefinitionEditWireRequest(fixture, decodedPrepared, target)
	if err := ValidatePreparedTaskDefinitionEditRequest(decodedPrepared, request); err != nil {
		t.Fatalf("ValidatePreparedTaskDefinitionEditRequest() error = %v", err)
	}

	snapshotWire, err := EncodeTaskDefinitionEditBaseSnapshot(decodedPrepared, snapshot)
	if err != nil {
		t.Fatalf("EncodeTaskDefinitionEditBaseSnapshot() error = %v", err)
	}
	decodedSnapshot, err := DecodeTaskDefinitionEditBaseSnapshot(decodedPrepared, snapshotWire)
	if err != nil {
		t.Fatalf("DecodeTaskDefinitionEditBaseSnapshot() error = %v", err)
	}
	if decodedSnapshot != snapshot {
		t.Fatalf("decoded snapshot = %+v, want %+v", decodedSnapshot, snapshot)
	}
}

func TestPreparedTaskDefinitionEditWire_RejectsShapeCanonicalityAndTampering(t *testing.T) {
	fixture := newTaskDefinitionEditFixture(t, TaskDefinitionEditOriginalStateActive)
	prepared, _ := fixture.prepare(
		t,
		"edit-wire-strict",
		taskDefinitionEditHead(1, "a"),
		taskDefinitionEditHead(2, "b"),
		fixture.base,
		changedTaskDefinitionEditDefinition(fixture.base, "strict"),
	)
	valid, err := EncodePreparedTaskDefinitionEdit(prepared)
	if err != nil {
		t.Fatalf("EncodePreparedTaskDefinitionEdit() error = %v", err)
	}

	tampered := prepared
	tampered.TargetFinal.State.Note += "-forged"
	tamperedWire, err := json.Marshal(tampered)
	if err != nil {
		t.Fatalf("marshal tampered prepared edit: %v", err)
	}
	tests := []struct {
		name string
		raw  []byte
	}{
		{name: "leading whitespace", raw: append([]byte(" "), valid...)},
		{name: "unknown root field", raw: insertTaskDefinitionEditWireField(valid, `,"future":true`)},
		{name: "case folded root field", raw: bytes.Replace(valid, []byte(`"wire_version"`), []byte(`"WIRE_VERSION"`), 1)},
		{name: "escaped root field", raw: bytes.Replace(valid, []byte(`"wire_version"`), []byte(`"\u0077ire_version"`), 1)},
		{name: "missing root field", raw: bytes.Replace(valid, []byte(`"wire_version":"v1",`), nil, 1)},
		{name: "null scalar", raw: bytes.Replace(valid, []byte(`"operation_id":"edit-wire-strict"`), []byte(`"operation_id":null`), 1)},
		{name: "semantic digest tamper", raw: tamperedWire},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			if _, err := DecodePreparedTaskDefinitionEdit(testCase.raw); !errors.Is(err, ErrTaskScheduleInvalid) {
				t.Fatalf("DecodePreparedTaskDefinitionEdit() error = %v, want invalid", err)
			}
		})
	}
}

func TestPreparedTaskDefinitionEditWire_TargetBinding(t *testing.T) {
	fixture := newTaskDefinitionEditFixture(t, TaskDefinitionEditOriginalStateActive)
	target := changedTaskDefinitionEditDefinition(fixture.base, "binding")
	prepared, _ := fixture.prepare(
		t,
		"edit-wire-target",
		taskDefinitionEditHead(1, "a"),
		taskDefinitionEditHead(2, "b"),
		fixture.base,
		target,
	)
	request := taskDefinitionEditWireRequest(fixture, prepared, target)
	if err := ValidatePreparedTaskDefinitionEditRequest(prepared, request); err != nil {
		t.Fatalf("valid request error = %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*TaskDefinitionEditRequest)
	}{
		{name: "operation", mutate: func(value *TaskDefinitionEditRequest) {
			value.OperationID += "-forged"
		}},
		{name: "base head", mutate: func(value *TaskDefinitionEditRequest) {
			value.BaseHead.Digest = strings.Repeat("c", 64)
		}},
		{name: "target head", mutate: func(value *TaskDefinitionEditRequest) {
			value.TargetHead.Digest = strings.Repeat("c", 64)
		}},
		{name: "original state", mutate: func(value *TaskDefinitionEditRequest) {
			value.OriginalState = TaskDefinitionEditOriginalStatePaused
		}},
		{name: "creation ownership", mutate: func(value *TaskDefinitionEditRequest) {
			value.Creation.UserID++
		}},
		{name: "base schedule spec", mutate: func(value *TaskDefinitionEditRequest) {
			value.Base.Spec = ScheduleSpec{Cron: "30 9 * * *", TZ: "Asia/Shanghai"}
		}},
		{name: "base natural language description", mutate: func(value *TaskDefinitionEditRequest) {
			value.Base.NLDescription += "-forged"
		}},
		{name: "base scope", mutate: func(value *TaskDefinitionEditRequest) {
			value.Base.Scope.TopN++
		}},
		{name: "target schedule spec", mutate: func(value *TaskDefinitionEditRequest) {
			value.Target.Spec = ScheduleSpec{Cron: "30 9 * * *", TZ: "Asia/Shanghai"}
		}},
		{name: "target natural language description", mutate: func(value *TaskDefinitionEditRequest) {
			value.Target.NLDescription += "-forged"
		}},
		{name: "target scope order", mutate: func(value *TaskDefinitionEditRequest) {
			value.Target.Scope = workflow.PushScope{
				SourceIDs: []int64{33, 22}, TopN: value.Target.Scope.TopN,
			}
		}},
		{name: "target scope top n", mutate: func(value *TaskDefinitionEditRequest) {
			value.Target.Scope.TopN++
		}},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			forged := taskDefinitionEditWireRequest(fixture, prepared, target)
			testCase.mutate(&forged)
			if err := ValidatePreparedTaskDefinitionEditRequest(prepared, forged); !errors.Is(err, ErrTaskScheduleInvalid) {
				t.Fatalf("ValidatePreparedTaskDefinitionEditRequest() error = %v, want invalid", err)
			}
		})
	}
}

func TestPreparedTaskDefinitionEditWire_CurrentWriterRunsOnlyAtSeal(t *testing.T) {
	fixture := newTaskDefinitionEditFixture(t, TaskDefinitionEditOriginalStateActive)
	target := changedTaskDefinitionEditDefinition(fixture.base, "retained-policy")
	prepared, _ := fixture.prepare(
		t,
		"edit-wire-retained-policy",
		taskDefinitionEditHead(1, "a"),
		taskDefinitionEditHead(2, "b"),
		fixture.base,
		target,
	)
	raw, err := EncodePreparedTaskDefinitionEdit(prepared)
	if err != nil {
		t.Fatalf("EncodePreparedTaskDefinitionEdit() error = %v", err)
	}
	decoded, err := DecodePreparedTaskDefinitionEdit(raw)
	if err != nil {
		t.Fatalf("DecodePreparedTaskDefinitionEdit() error = %v", err)
	}
	request := taskDefinitionEditWireRequest(fixture, decoded, target)
	if err := ValidatePreparedTaskDefinitionEditRequest(decoded, request); err != nil {
		t.Fatalf("retained recovery consulted current writer policy: %v", err)
	}

	futureWriter := func(ScheduleSpec) error {
		return errors.New("simulated future writer no longer accepts this v1 timing")
	}
	if err := validatePreparedTaskDefinitionEditRequestForWrite(
		decoded, request, futureWriter,
	); !errors.Is(err, ErrTaskScheduleInvalid) {
		t.Fatalf("proposal seal ignored simulated future writer policy: %v", err)
	}
}

func TestTaskDefinitionEditBaseSnapshotWire_RejectsWrongRevisionAndShape(t *testing.T) {
	fixture := newTaskDefinitionEditFixture(t, TaskDefinitionEditOriginalStatePaused)
	prepared, snapshot := fixture.prepare(
		t,
		"edit-snapshot-strict",
		taskDefinitionEditHead(1, "a"),
		taskDefinitionEditHead(2, "b"),
		fixture.base,
		changedTaskDefinitionEditDefinition(fixture.base, "snapshot"),
	)
	valid, err := EncodeTaskDefinitionEditBaseSnapshot(prepared, snapshot)
	if err != nil {
		t.Fatalf("EncodeTaskDefinitionEditBaseSnapshot() error = %v", err)
	}

	wrongRevision := snapshot
	wrongRevision.Revision += "AA"
	wrongRevisionWire, err := json.Marshal(wrongRevision)
	if err != nil {
		t.Fatalf("marshal wrong-revision snapshot: %v", err)
	}
	for name, raw := range map[string][]byte{
		"wrong revision": wrongRevisionWire,
		"non canonical":  append(valid, '\n'),
		"unknown field":  insertTaskDefinitionEditWireField(valid, `,"future":true`),
		"missing field":  []byte(strings.Replace(string(valid), `"phase":"base_original",`, "", 1)),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodeTaskDefinitionEditBaseSnapshot(prepared, raw); !errors.Is(err, ErrTaskScheduleInvalid) {
				t.Fatalf("DecodeTaskDefinitionEditBaseSnapshot() error = %v, want invalid", err)
			}
		})
	}
}

func TestTaskDefinitionEditPhaseSnapshotWire_ExactBindings(t *testing.T) {
	fixture := newTaskDefinitionEditFixture(t, TaskDefinitionEditOriginalStateActive)
	prepared, _ := fixture.prepare(
		t,
		"edit-phase-snapshot-wire",
		taskDefinitionEditHead(1, "a"),
		taskDefinitionEditHead(2, "b"),
		fixture.base,
		changedTaskDefinitionEditDefinition(fixture.base, "phase-snapshot"),
	)
	tests := []struct {
		name           string
		phase          TaskDefinitionEditPhase
		representation PreparedTaskDefinitionEditSchedule
	}{
		{name: "base original", phase: TaskDefinitionEditPhaseBaseOriginal, representation: prepared.BaseOriginal},
		{name: "base paused", phase: TaskDefinitionEditPhaseBasePaused, representation: prepared.BasePaused},
		{name: "target paused", phase: TaskDefinitionEditPhaseTargetPaused, representation: prepared.TargetPaused},
		{name: "target final", phase: TaskDefinitionEditPhaseTargetFinal, representation: prepared.TargetFinal},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			snapshot := taskDefinitionEditSnapshotFromRevision(
				prepared, testCase.phase, testCase.representation, "Ag",
			)
			raw, err := EncodeTaskDefinitionEditPhaseSnapshot(prepared, snapshot)
			if err != nil {
				t.Fatalf("EncodeTaskDefinitionEditPhaseSnapshot() error = %v", err)
			}
			decoded, err := DecodeTaskDefinitionEditPhaseSnapshot(prepared, raw)
			if err != nil {
				t.Fatalf("DecodeTaskDefinitionEditPhaseSnapshot() error = %v", err)
			}
			if decoded != snapshot {
				t.Fatalf("decoded snapshot = %+v, want %+v", decoded, snapshot)
			}

			wrongRepresentation := snapshot
			wrongRepresentation.RepresentationDigest = prepared.TargetFinal.Digest
			if wrongRepresentation.RepresentationDigest == snapshot.RepresentationDigest {
				wrongRepresentation.RepresentationDigest = strings.Repeat("f", 64)
			}
			for name, invalid := range map[string][]byte{
				"non canonical": append(bytes.Clone(raw), '\n'),
				"unknown field": insertTaskDefinitionEditWireField(raw, `,"future":true`),
				"wrong representation": func() []byte {
					encoded, marshalErr := json.Marshal(wrongRepresentation)
					if marshalErr != nil {
						t.Fatalf("marshal wrong representation: %v", marshalErr)
					}
					return encoded
				}(),
			} {
				t.Run(name, func(t *testing.T) {
					if _, err := DecodeTaskDefinitionEditPhaseSnapshot(
						prepared, invalid,
					); !errors.Is(err, ErrTaskScheduleInvalid) {
						t.Fatalf("DecodeTaskDefinitionEditPhaseSnapshot() error = %v, want invalid", err)
					}
				})
			}
		})
	}
}

func insertTaskDefinitionEditWireField(raw []byte, field string) []byte {
	result := bytes.Clone(raw[:len(raw)-1])
	result = append(result, field...)
	return append(result, '}')
}

func taskDefinitionEditWireRequest(
	fixture *taskDefinitionEditFixture,
	prepared PreparedTaskDefinitionEdit,
	target TaskDefinitionEditDefinition,
) TaskDefinitionEditRequest {
	return TaskDefinitionEditRequest{
		OperationID:   prepared.OperationID,
		Creation:      prepared.Creation,
		BaseHead:      prepared.BaseHead,
		TargetHead:    prepared.TargetHead,
		OriginalState: prepared.OriginalState,
		Base:          cloneTaskDefinitionEditDefinition(fixture.base),
		Target:        cloneTaskDefinitionEditDefinition(target),
	}
}
