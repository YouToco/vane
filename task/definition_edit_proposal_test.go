package task

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/YouToco/vane/scheduler"
	"github.com/YouToco/vane/taskstate"
	"github.com/YouToco/vane/types"
)

type definitionEditProposalComponentFixtureV1 struct {
	BaseDefinition   json.RawMessage `json:"base_definition"`
	TargetDefinition json.RawMessage `json:"target_definition"`
	PreparedEdit     json.RawMessage `json:"prepared_edit"`
	BaseSnapshot     json.RawMessage `json:"base_snapshot"`
}

const goldenTaskDefinitionEditProposalV1 = `{"wire_version":"vane.task-definition-edit-proposal/v1","operation_id":"edit-proposal-fixture","approval_ref":"approval-definition-edit-0001","actor":{"tenant_id":7,"user_id":42},"target":{"tenant_id":7,"user_id":42,"task_id":"task-v1-9e07ee1d79baaae1d8d4ad49fd72e449112a11961b7a534d4a913b9425fb62e8"},"session_id":91,"expires_at_unix_micros":1780000000123456,"original_status":"active","base_head":{"version":1,"digest":"93b3e82fc8ba406d715e3e157b285082774b186c61189983fa50bc05354a42e2"},"target_head":{"version":2,"digest":"d7e3c0a999459878b21ef0fc799fd81632bd9ef8e48aa704b20fb3a41189f3ae"},"target_definition_digest":"d7e3c0a999459878b21ef0fc799fd81632bd9ef8e48aa704b20fb3a41189f3ae","prepared_edit_digest":"b6d978020aeefee599eae5e58a57e1c8a4c94d19aa68287fb84c80ed2d820658","base_snapshot_digest":"331f8b0261dee63cd600e393f123b36fc43c36ae10f6a1641e2637de39264ebd"}`

type decodedDefinitionEditProposalFixture struct {
	base         taskstate.ApprovedDefinitionV1
	baseBytes    []byte
	target       taskstate.ApprovedDefinitionV1
	prepared     scheduler.PreparedTaskDefinitionEdit
	baseSnapshot scheduler.TaskDefinitionEditSnapshot
}

func TestFrozenTaskDefinitionEditProposal_CanonicalRoundTripAndBindings(t *testing.T) {
	fixture := loadDefinitionEditProposalFixture(t)
	input := validDefinitionEditProposalInput(fixture)
	frozen, err := BuildFrozenTaskDefinitionEditProposal(input)
	if err != nil {
		t.Fatalf("BuildFrozenTaskDefinitionEditProposal() error = %v", err)
	}
	if string(frozen.CanonicalProposal) != goldenTaskDefinitionEditProposalV1 {
		t.Fatalf("proposal V1 canonical wire drifted:\n got: %s\nwant: %s",
			frozen.CanonicalProposal, goldenTaskDefinitionEditProposalV1)
	}

	if frozen.Proposal.WireVersion != taskDefinitionEditProposalVersion ||
		frozen.Proposal.OperationID != fixture.prepared.OperationID ||
		frozen.Proposal.Target.TaskID != fixture.prepared.Creation.TaskID {
		t.Fatalf("proposal identity = %+v", frozen.Proposal)
	}
	if frozen.ProposalDigest != sha256Hex(frozen.CanonicalProposal) ||
		frozen.Proposal.TargetDefinitionDigest != sha256Hex(frozen.TargetDefinitionBytes) ||
		frozen.Proposal.PreparedEditDigest != sha256Hex(frozen.PreparedEditBytes) ||
		frozen.Proposal.BaseSnapshotDigest != sha256Hex(frozen.BaseSnapshotBytes) {
		t.Fatal("proposal component digest binding differs")
	}
	if fixture.prepared.Creation.PreparedDigest == fixture.prepared.BaseHead.Digest {
		t.Fatal("positive fixture accidentally aliases creation and Approved base digests")
	}

	decoded, err := DecodeFrozenTaskDefinitionEditProposal(
		frozen.CanonicalProposal,
		fixture.baseBytes,
		frozen.TargetDefinitionBytes,
		frozen.PreparedEditBytes,
		frozen.BaseSnapshotBytes,
	)
	if err != nil {
		t.Fatalf("DecodeFrozenTaskDefinitionEditProposal() error = %v", err)
	}
	if !reflect.DeepEqual(decoded.Proposal, frozen.Proposal) ||
		!reflect.DeepEqual(decoded.BaseDefinition, frozen.BaseDefinition) ||
		!bytes.Equal(decoded.BaseDefinitionBytes, frozen.BaseDefinitionBytes) ||
		!reflect.DeepEqual(decoded.TargetDefinition, frozen.TargetDefinition) ||
		!reflect.DeepEqual(decoded.PreparedEdit, frozen.PreparedEdit) ||
		decoded.BaseSnapshot != frozen.BaseSnapshot ||
		!bytes.Equal(decoded.CanonicalProposal, frozen.CanonicalProposal) {
		t.Fatal("frozen proposal round trip changed a checkpoint")
	}

	input.TargetDefinition.NLDescription = "caller-owned mutation"
	input.PreparedEdit.TargetFinal.Action.Params.NLDesc = "caller-owned mutation"
	if decoded.TargetDefinition.NLDescription == input.TargetDefinition.NLDescription ||
		decoded.PreparedEdit.TargetFinal.Action.Params.NLDesc ==
			input.PreparedEdit.TargetFinal.Action.Params.NLDesc {
		t.Fatal("frozen proposal retained caller-owned aliases")
	}
}

func TestFrozenTaskDefinitionEditProposal_RejectsExactWireViolations(t *testing.T) {
	fixture := loadDefinitionEditProposalFixture(t)
	frozen := buildDefinitionEditProposalFixture(t, fixture)
	duplicateOperation := insertDefinitionEditProposalField(
		frozen.CanonicalProposal,
		`,"operation_id":"edit-proposal-fixture"`,
	)
	tests := []struct {
		name string
		raw  []byte
	}{
		{name: "leading whitespace", raw: append([]byte(" "), frozen.CanonicalProposal...)},
		{name: "trailing value", raw: append(bytes.Clone(frozen.CanonicalProposal), []byte(` {}`)...)},
		{name: "unknown field", raw: insertDefinitionEditProposalField(frozen.CanonicalProposal, `,"future":true`)},
		{name: "duplicate field", raw: duplicateOperation},
		{name: "case folded field", raw: bytes.Replace(frozen.CanonicalProposal, []byte(`"wire_version"`), []byte(`"WIRE_VERSION"`), 1)},
		{name: "escaped field", raw: bytes.Replace(frozen.CanonicalProposal, []byte(`"wire_version"`), []byte(`"\u0077ire_version"`), 1)},
		{name: "missing field", raw: bytes.Replace(frozen.CanonicalProposal, []byte(`"wire_version":"vane.task-definition-edit-proposal/v1",`), nil, 1)},
		{name: "null scalar", raw: bytes.Replace(frozen.CanonicalProposal, []byte(`"session_id":91`), []byte(`"session_id":null`), 1)},
		{name: "invalid utf8", raw: append(bytes.Clone(frozen.CanonicalProposal[:len(frozen.CanonicalProposal)-1]), 0xff, '}')},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			assertDefinitionEditProposalDecodeInvalid(
				t, testCase.raw, frozen.BaseDefinitionBytes,
				frozen.TargetDefinitionBytes, frozen.PreparedEditBytes,
				frozen.BaseSnapshotBytes,
			)
		})
	}
}

func TestFrozenTaskDefinitionEditProposal_RejectsScopeHeadAndDigestSplices(t *testing.T) {
	fixture := loadDefinitionEditProposalFixture(t)
	frozen := buildDefinitionEditProposalFixture(t, fixture)
	tests := []struct {
		name   string
		mutate func(*TaskDefinitionEditProposalV1)
	}{
		{name: "operation", mutate: func(value *TaskDefinitionEditProposalV1) { value.OperationID += "-other" }},
		{name: "approval ref", mutate: func(value *TaskDefinitionEditProposalV1) { value.ApprovalRef = "" }},
		{name: "approval ref format character", mutate: func(value *TaskDefinitionEditProposalV1) {
			value.ApprovalRef = "approval\u200bhidden"
		}},
		{name: "approval ref bidi override", mutate: func(value *TaskDefinitionEditProposalV1) {
			value.ApprovalRef = "approval\u202ehidden"
		}},
		{name: "operation too long", mutate: func(value *TaskDefinitionEditProposalV1) {
			value.OperationID = strings.Repeat("o", maxTaskDefinitionEditOperationIDBytes+1)
		}},
		{name: "actor tenant", mutate: func(value *TaskDefinitionEditProposalV1) { value.Actor.TenantID++ }},
		{name: "actor user", mutate: func(value *TaskDefinitionEditProposalV1) { value.Actor.UserID++ }},
		{name: "target tenant", mutate: func(value *TaskDefinitionEditProposalV1) { value.Target.TenantID++ }},
		{name: "target user", mutate: func(value *TaskDefinitionEditProposalV1) { value.Target.UserID++ }},
		{name: "target task", mutate: func(value *TaskDefinitionEditProposalV1) { value.Target.TaskID += "-other" }},
		{name: "target task too long", mutate: func(value *TaskDefinitionEditProposalV1) {
			value.Target.TaskID = strings.Repeat("t", maxTaskDefinitionEditTaskIDBytes+1)
		}},
		{name: "session", mutate: func(value *TaskDefinitionEditProposalV1) { value.SessionID = 0 }},
		{name: "expiry", mutate: func(value *TaskDefinitionEditProposalV1) { value.ExpiresAtUnixMicros = 0 }},
		{name: "original status", mutate: func(value *TaskDefinitionEditProposalV1) {
			value.OriginalStatus = TaskDefinitionEditOriginalStatusV1Paused
		}},
		{name: "base version", mutate: func(value *TaskDefinitionEditProposalV1) { value.BaseHead.Version++ }},
		{name: "base digest", mutate: func(value *TaskDefinitionEditProposalV1) { value.BaseHead.Digest = strings.Repeat("c", 64) }},
		{name: "target version", mutate: func(value *TaskDefinitionEditProposalV1) { value.TargetHead.Version++ }},
		{name: "target digest", mutate: func(value *TaskDefinitionEditProposalV1) { value.TargetHead.Digest = strings.Repeat("c", 64) }},
		{name: "target bytes digest", mutate: func(value *TaskDefinitionEditProposalV1) { value.TargetDefinitionDigest = strings.Repeat("c", 64) }},
		{name: "prepared bytes digest", mutate: func(value *TaskDefinitionEditProposalV1) { value.PreparedEditDigest = strings.Repeat("c", 64) }},
		{name: "snapshot bytes digest", mutate: func(value *TaskDefinitionEditProposalV1) { value.BaseSnapshotDigest = strings.Repeat("c", 64) }},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			forged := frozen.Proposal
			testCase.mutate(&forged)
			raw, err := json.Marshal(forged)
			if err != nil {
				t.Fatalf("marshal forged proposal: %v", err)
			}
			assertDefinitionEditProposalDecodeInvalid(
				t, raw, frozen.BaseDefinitionBytes,
				frozen.TargetDefinitionBytes, frozen.PreparedEditBytes,
				frozen.BaseSnapshotBytes,
			)
		})
	}
}

func TestFrozenTaskDefinitionEditProposal_RejectsCheckpointAndWriterTampering(t *testing.T) {
	fixture := loadDefinitionEditProposalFixture(t)
	frozen := buildDefinitionEditProposalFixture(t, fixture)

	nonCanonicalTarget := append([]byte(" "), frozen.TargetDefinitionBytes...)
	assertDefinitionEditProposalDecodeInvalid(
		t, frozen.CanonicalProposal, frozen.BaseDefinitionBytes,
		nonCanonicalTarget, frozen.PreparedEditBytes, frozen.BaseSnapshotBytes,
	)
	nonCanonicalBase := append([]byte(" "), frozen.BaseDefinitionBytes...)
	assertDefinitionEditProposalDecodeInvalid(
		t, frozen.CanonicalProposal, nonCanonicalBase,
		frozen.TargetDefinitionBytes, frozen.PreparedEditBytes, frozen.BaseSnapshotBytes,
	)
	forgedPrepared := bytes.Replace(
		frozen.PreparedEditBytes,
		[]byte(`"operation_id":"edit-proposal-fixture"`),
		[]byte(`"operation_id":"edit-proposal-forged"`), 1,
	)
	assertDefinitionEditProposalDecodeInvalid(
		t, frozen.CanonicalProposal, frozen.BaseDefinitionBytes,
		frozen.TargetDefinitionBytes, forgedPrepared, frozen.BaseSnapshotBytes,
	)
	forgedSnapshot := bytes.Replace(
		frozen.BaseSnapshotBytes,
		[]byte(`"phase":"base_original"`),
		[]byte(`"phase":"target_final"`), 1,
	)
	assertDefinitionEditProposalDecodeInvalid(
		t, frozen.CanonicalProposal, frozen.BaseDefinitionBytes,
		frozen.TargetDefinitionBytes, frozen.PreparedEditBytes, forgedSnapshot,
	)

	wrongBase := fixture.base
	wrongBase.Intent += "-other"
	wrongBase.PlaybookContent = wrongBase.Intent
	wrongBaseBytes, err := taskstate.EncodeApprovedDefinitionV1(wrongBase)
	if err != nil {
		t.Fatalf("encode wrong base: %v", err)
	}
	assertDefinitionEditProposalDecodeInvalid(
		t, frozen.CanonicalProposal, wrongBaseBytes,
		frozen.TargetDefinitionBytes, frozen.PreparedEditBytes, frozen.BaseSnapshotBytes,
	)
	tamperedBase := bytes.Clone(frozen.BaseDefinitionBytes)
	tamperedBase[len(tamperedBase)-2] ^= 1
	assertDefinitionEditProposalDecodeInvalid(
		t, frozen.CanonicalProposal, tamperedBase,
		frozen.TargetDefinitionBytes, frozen.PreparedEditBytes, frozen.BaseSnapshotBytes,
	)

	invalidTarget := fixture.target
	invalidTarget.Intent += "-not-projected"
	invalidTargetBytes, err := taskstate.EncodeApprovedDefinitionV1(invalidTarget)
	if err != nil {
		t.Fatalf("encode invalid writer target: %v", err)
	}
	forgedProposal := frozen.Proposal
	forgedProposal.TargetDefinitionDigest = sha256Hex(invalidTargetBytes)
	forgedProposal.TargetHead.Digest = forgedProposal.TargetDefinitionDigest
	forgedProposalBytes, err := json.Marshal(forgedProposal)
	if err != nil {
		t.Fatalf("marshal invalid writer proposal: %v", err)
	}
	assertDefinitionEditProposalDecodeInvalid(
		t, forgedProposalBytes, frozen.BaseDefinitionBytes, invalidTargetBytes,
		frozen.PreparedEditBytes, frozen.BaseSnapshotBytes,
	)
}

func TestDefinitionEditSchedulerProjection_RejectsUnrepresentableJSON(t *testing.T) {
	fixture := loadDefinitionEditProposalFixture(t)
	tests := []struct {
		name   string
		mutate func(*taskstate.ApprovedDefinitionV1)
	}{
		{name: "unknown spec field", mutate: func(value *taskstate.ApprovedDefinitionV1) {
			value.SpecJSON = json.RawMessage(`{"cron":"25 9 * * *","tz":"Asia/Shanghai","future":true}`)
		}},
		{name: "null spec", mutate: func(value *taskstate.ApprovedDefinitionV1) {
			value.SpecJSON = json.RawMessage(`null`)
		}},
		{name: "unknown scope field", mutate: func(value *taskstate.ApprovedDefinitionV1) {
			value.ScopeJSON = json.RawMessage(`{"source_ids":[22,33],"top_n":5,"future":true}`)
		}},
		{name: "duplicate scope field", mutate: func(value *taskstate.ApprovedDefinitionV1) {
			value.ScopeJSON = json.RawMessage(`{"top_n":5,"top_n":6}`)
		}},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			definition := fixture.target
			testCase.mutate(&definition)
			if _, err := definitionEditSchedulerProjection(definition); err == nil {
				t.Fatal("definitionEditSchedulerProjection() accepted unrepresentable JSON")
			}
		})
	}
}

func TestDefinitionEditTargetPolicy_CurrentWriterOnlyAtSeal(t *testing.T) {
	fixture := loadDefinitionEditProposalFixture(t)
	canonical, err := taskstate.EncodeApprovedDefinitionV1(fixture.target)
	if err != nil {
		t.Fatalf("encode target definition: %v", err)
	}
	retainedFuture := bytes.ReplaceAll(
		canonical,
		[]byte(`"config":{"query":`),
		[]byte(`"config":{"future_option":true,"query":`),
	)
	if bytes.Equal(retainedFuture, canonical) {
		t.Fatal("future source mutation did not change fixture")
	}
	definition, err := taskstate.DecodeApprovedDefinitionV1(retainedFuture)
	if err != nil {
		t.Fatalf("frozen V1 reader rejected retained future config: %v", err)
	}
	if err := validateDefinitionEditTargetPolicy(definition, false); err != nil {
		t.Fatalf("recovery policy consulted current source registry: %v", err)
	}
	if err := validateDefinitionEditTargetPolicy(definition, true); !errors.Is(err, ErrDefinitionEditProposalInvalid) {
		t.Fatalf("proposal seal accepted unknown current source config: %v", err)
	}
	input := validDefinitionEditProposalInput(fixture)
	input.TargetDefinition = definition
	if _, err := BuildFrozenTaskDefinitionEditProposal(input); !errors.Is(err, ErrDefinitionEditProposalInvalid) {
		t.Fatalf("BuildFrozenTaskDefinitionEditProposal() accepted unknown current source config: %v", err)
	}
}

func TestDefinitionEditProposalIdentifierBoundsAreUTF8Bytes(t *testing.T) {
	t.Parallel()
	exactMultibyte := strings.Repeat("界", maxTaskDefinitionEditTaskIDBytes/3)
	if len(exactMultibyte) != maxTaskDefinitionEditTaskIDBytes ||
		!validTaskDefinitionEditIdentifier(exactMultibyte, maxTaskDefinitionEditTaskIDBytes) {
		t.Fatalf("exact %d-byte UTF-8 identifier was rejected", maxTaskDefinitionEditTaskIDBytes)
	}
	if validTaskDefinitionEditIdentifier(
		exactMultibyte+"a", maxTaskDefinitionEditTaskIDBytes,
	) {
		t.Fatal("max+1 UTF-8 identifier was accepted")
	}
	for _, invalid := range []string{"approval\u200bhidden", "approval\u202ehidden"} {
		if validTaskDefinitionEditIdentifier(invalid, maxTaskDefinitionEditReferenceBytes) {
			t.Fatalf("format-control identifier %q was accepted", invalid)
		}
	}
}

func loadDefinitionEditProposalFixture(t *testing.T) decodedDefinitionEditProposalFixture {
	t.Helper()
	raw, err := os.ReadFile("testdata/definition_edit_proposal_components_v1.json")
	if err != nil {
		t.Fatalf("read proposal component fixture: %v", err)
	}
	var wire definitionEditProposalComponentFixtureV1
	if err := json.Unmarshal(raw, &wire); err != nil {
		t.Fatalf("decode proposal component fixture: %v", err)
	}
	base, err := taskstate.DecodeApprovedDefinitionV1(wire.BaseDefinition)
	if err != nil {
		t.Fatalf("decode fixture base definition: %v", err)
	}
	target, err := taskstate.DecodeApprovedDefinitionV1(wire.TargetDefinition)
	if err != nil {
		t.Fatalf("decode fixture target definition: %v", err)
	}
	prepared, err := scheduler.DecodePreparedTaskDefinitionEdit(wire.PreparedEdit)
	if err != nil {
		t.Fatalf("decode fixture prepared edit: %v", err)
	}
	baseSnapshot, err := scheduler.DecodeTaskDefinitionEditBaseSnapshot(prepared, wire.BaseSnapshot)
	if err != nil {
		t.Fatalf("decode fixture base snapshot: %v", err)
	}
	return decodedDefinitionEditProposalFixture{
		base: base, baseBytes: bytes.Clone(wire.BaseDefinition),
		target: target, prepared: prepared, baseSnapshot: baseSnapshot,
	}
}

func validDefinitionEditProposalInput(
	fixture decodedDefinitionEditProposalFixture,
) BuildTaskDefinitionEditProposalInput {
	return BuildTaskDefinitionEditProposalInput{
		OperationID:      fixture.prepared.OperationID,
		ApprovalRef:      "approval-definition-edit-0001",
		ActorTenantID:    fixture.prepared.Creation.TenantID,
		ActorUserID:      fixture.prepared.Creation.UserID,
		TargetTenantID:   fixture.prepared.Creation.TenantID,
		TargetUserID:     fixture.prepared.Creation.UserID,
		TaskID:           fixture.prepared.Creation.TaskID,
		SessionID:        91,
		ExpiresAt:        time.UnixMicro(1_780_000_000_123_456).UTC(),
		OriginalStatus:   types.ScheduleStatusActive,
		BaseHead:         fixture.prepared.BaseHead,
		TargetHead:       fixture.prepared.TargetHead,
		BaseDefinition:   fixture.base,
		TargetDefinition: fixture.target,
		PreparedEdit:     fixture.prepared,
		BaseSnapshot:     fixture.baseSnapshot,
	}
}

func buildDefinitionEditProposalFixture(
	t *testing.T,
	fixture decodedDefinitionEditProposalFixture,
) FrozenTaskDefinitionEditProposal {
	t.Helper()
	frozen, err := BuildFrozenTaskDefinitionEditProposal(
		validDefinitionEditProposalInput(fixture),
	)
	if err != nil {
		t.Fatalf("BuildFrozenTaskDefinitionEditProposal() error = %v", err)
	}
	return frozen
}

func assertDefinitionEditProposalDecodeInvalid(
	t *testing.T,
	proposal []byte,
	base []byte,
	target []byte,
	prepared []byte,
	baseSnapshot []byte,
) {
	t.Helper()
	if _, err := DecodeFrozenTaskDefinitionEditProposal(
		proposal, base, target, prepared, baseSnapshot,
	); !errors.Is(err, ErrDefinitionEditProposalInvalid) {
		t.Fatalf("DecodeFrozenTaskDefinitionEditProposal() error = %v, want invalid", err)
	}
}

func insertDefinitionEditProposalField(raw []byte, field string) []byte {
	result := bytes.Clone(raw[:len(raw)-1])
	result = append(result, field...)
	return append(result, '}')
}
