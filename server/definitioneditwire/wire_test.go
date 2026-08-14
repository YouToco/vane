package definitioneditwire

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/YouToco/vane/taskstate"
	"github.com/YouToco/vane/workflowruntime"
)

type componentFixtureV1 struct {
	BaseDefinition   json.RawMessage `json:"base_definition"`
	TargetDefinition json.RawMessage `json:"target_definition"`
	PreparedEdit     json.RawMessage `json:"prepared_edit"`
	BaseSnapshot     json.RawMessage `json:"base_snapshot"`
}

func TestDecodeFrozenProposal_CanonicalBindings(t *testing.T) {
	fixture, proposal := loadFixture(t)
	frozen, err := DecodeFrozenProposal(
		proposal, fixture.BaseDefinition, fixture.TargetDefinition,
		fixture.PreparedEdit, fixture.BaseSnapshot,
	)
	if err != nil {
		t.Fatalf("DecodeFrozenProposal() error = %v", err)
	}
	if frozen.Proposal.OperationID != frozen.Prepared.OperationID ||
		frozen.Proposal.Target.TaskID != frozen.Prepared.Creation.TaskID ||
		frozen.BaseSnapshot.Phase != SnapshotPhaseBaseOriginal ||
		frozen.ProposalDigest != digest(proposal) {
		t.Fatalf("decoded frozen proposal differs: %+v", frozen.Proposal)
	}
	creation, err := CanonicalCreation(frozen.Prepared)
	if err != nil {
		t.Fatalf("CanonicalCreation() error = %v", err)
	}
	wantCreation, err := json.Marshal(frozen.Prepared.Creation)
	if err != nil || !bytes.Equal(creation, wantCreation) {
		t.Fatalf("creation provenance differs: %v", err)
	}

	fixture.PreparedEdit[0] ^= 1
	fixture.BaseSnapshot[0] ^= 1
	if bytes.Equal(frozen.PreparedEditBytes, fixture.PreparedEdit) ||
		bytes.Equal(frozen.BaseSnapshotBytes, fixture.BaseSnapshot) {
		t.Fatal("frozen proposal retained caller-owned byte aliases")
	}
}

func TestDecodeFrozenProposal_RetainsLegacyV1ApprovalRefReader(t *testing.T) {
	fixture, currentRaw := loadFixture(t)
	var current ProposalV2
	if err := json.Unmarshal(currentRaw, &current); err != nil {
		t.Fatal(err)
	}
	legacyRaw := mustMarshal(t, legacyProposalV1{
		WireVersion: proposalWireVersionV1,
		OperationID: current.OperationID,
		ApprovalRef: current.OperationRef,
		Actor: legacyProposalActorV1{
			TenantID: current.Actor.TenantID,
			UserID:   current.Actor.UserID,
		},
		Target: legacyProposalTargetV1{
			TenantID: current.Target.TenantID,
			UserID:   current.Target.UserID,
			TaskID:   current.Target.TaskID,
		},
		SessionID:              current.SessionID,
		ExpiresAtUnixMicros:    current.ExpiresAtUnixMicros,
		OriginalStatus:         current.OriginalStatus,
		BaseHead:               current.BaseHead,
		TargetHead:             current.TargetHead,
		TargetDefinitionDigest: current.TargetDefinitionDigest,
		PreparedEditDigest:     current.PreparedEditDigest,
		BaseSnapshotDigest:     current.BaseSnapshotDigest,
	})
	frozen, err := DecodeFrozenProposal(
		legacyRaw,
		fixture.BaseDefinition,
		fixture.TargetDefinition,
		fixture.PreparedEdit,
		fixture.BaseSnapshot,
	)
	if err != nil {
		t.Fatalf("legacy proposal/v1 reader failed: %v", err)
	}
	if frozen.Proposal.WireVersion != proposalWireVersionV1 ||
		frozen.Proposal.OperationRef != current.OperationRef {
		t.Fatalf("legacy proposal was not normalized: %+v", frozen.Proposal)
	}
}

func TestDecodeFrozenProposal_AcceptsTerminalPausedMarkerAfterActivation(t *testing.T) {
	fixture, proposalRaw := loadFixture(t)
	var proposal ProposalV2
	if err := json.Unmarshal(proposalRaw, &proposal); err != nil {
		t.Fatal(err)
	}
	var prepared PreparedEditV1
	if err := json.Unmarshal(fixture.PreparedEdit, &prepared); err != nil {
		t.Fatal(err)
	}
	var snapshot SnapshotV1
	if err := json.Unmarshal(fixture.BaseSnapshot, &snapshot); err != nil {
		t.Fatal(err)
	}

	prepared.BaseOriginal.State.Paused = false
	prepared.BaseOriginal.State.Note = "runtime cutover activated finalized task"
	rebindPreparedEditDigests(t, &prepared, "final_paused")
	preparedRaw := mustMarshal(t, prepared)
	snapshot.RequestDigest = prepared.RequestDigest
	snapshot.RepresentationDigest = prepared.BaseOriginal.Digest
	snapshotRaw := mustMarshal(t, snapshot)
	proposal.PreparedEditDigest = digest(preparedRaw)
	proposal.BaseSnapshotDigest = digest(snapshotRaw)
	proposalRaw = mustMarshal(t, proposal)

	frozen, err := DecodeFrozenProposal(
		proposalRaw, fixture.BaseDefinition, fixture.TargetDefinition,
		preparedRaw, snapshotRaw,
	)
	if err != nil {
		t.Fatalf("DecodeFrozenProposal() terminal paused marker after activation: %v", err)
	}
	if frozen.Prepared.BaseOriginal.Fingerprint.EditPhase != "final_paused" ||
		frozen.Prepared.BaseOriginal.State.Paused ||
		frozen.Prepared.BaseOriginal.State.Note !=
			"runtime cutover activated finalized task" {
		t.Fatalf("decoded activated terminal marker differs: %+v",
			frozen.Prepared.BaseOriginal)
	}

	prepared.BaseOriginal.Fingerprint.EditPhase = "base_paused"
	recomputeRepresentationDigest(t, &prepared.BaseOriginal)
	recomputePreparedRequestDigest(t, &prepared)
	preparedRaw = mustMarshal(t, prepared)
	snapshot.RequestDigest = prepared.RequestDigest
	snapshot.RepresentationDigest = prepared.BaseOriginal.Digest
	snapshotRaw = mustMarshal(t, snapshot)
	proposal.PreparedEditDigest = digest(preparedRaw)
	proposal.BaseSnapshotDigest = digest(snapshotRaw)
	if _, err := DecodeFrozenProposal(
		mustMarshal(t, proposal), fixture.BaseDefinition, fixture.TargetDefinition,
		preparedRaw, snapshotRaw,
	); !errors.Is(err, ErrInvalid) {
		t.Fatalf("DecodeFrozenProposal() nonterminal base marker error = %v, want ErrInvalid", err)
	}
}

func TestDecodeFrozenProposal_RejectsExactAndCrossSplicedWire(t *testing.T) {
	fixture, proposalRaw := loadFixture(t)
	var proposal ProposalV2
	if err := json.Unmarshal(proposalRaw, &proposal); err != nil {
		t.Fatal(err)
	}
	var prepared PreparedEditV1
	if err := json.Unmarshal(fixture.PreparedEdit, &prepared); err != nil {
		t.Fatal(err)
	}
	var snapshot SnapshotV1
	if err := json.Unmarshal(fixture.BaseSnapshot, &snapshot); err != nil {
		t.Fatal(err)
	}

	splicedPrepared := prepared
	splicedPrepared.OperationID += "-other"
	splicedPreparedRaw := mustMarshal(t, splicedPrepared)
	splicedProposal := proposal
	splicedProposal.PreparedEditDigest = digest(splicedPreparedRaw)

	wrongSnapshot := snapshot
	wrongSnapshot.Phase = SnapshotPhaseTargetFinal
	wrongSnapshot.RepresentationDigest = prepared.TargetFinal.Digest
	wrongSnapshotRaw := mustMarshal(t, wrongSnapshot)
	wrongSnapshotProposal := proposal
	wrongSnapshotProposal.BaseSnapshotDigest = digest(wrongSnapshotRaw)

	unsafePrepared := prepared
	unsafePrepared.TargetFinal.State.Paused = true
	unsafePreparedRaw := mustMarshal(t, unsafePrepared)
	unsafeProposal := proposal
	unsafeProposal.PreparedEditDigest = digest(unsafePreparedRaw)

	reinterpretedPrepared := prepared
	reinterpretedPrepared.WireVersion = preparedWireVersionV2
	reinterpretedPreparedRaw := mustMarshal(t, reinterpretedPrepared)
	reinterpretedProposal := proposal
	reinterpretedProposal.PreparedEditDigest = digest(reinterpretedPreparedRaw)

	tests := []struct {
		name         string
		proposal     []byte
		prepared     []byte
		baseSnapshot []byte
	}{
		{
			name: "proposal leading whitespace", proposal: append([]byte(" "), proposalRaw...),
			prepared: fixture.PreparedEdit, baseSnapshot: fixture.BaseSnapshot,
		},
		{
			name: "prepared unknown nested field", proposal: proposalRaw,
			prepared: bytes.Replace(fixture.PreparedEdit,
				[]byte(`"base_head":{"version"`), []byte(`"base_head":{"future":true,"version"`), 1),
			baseSnapshot: fixture.BaseSnapshot,
		},
		{
			name: "proposal prepared digest splice", proposal: proposalRaw,
			prepared: splicedPreparedRaw, baseSnapshot: fixture.BaseSnapshot,
		},
		{
			name: "prepared operation scope splice", proposal: mustMarshal(t, splicedProposal),
			prepared: splicedPreparedRaw, baseSnapshot: fixture.BaseSnapshot,
		},
		{
			name: "base snapshot is later phase", proposal: mustMarshal(t, wrongSnapshotProposal),
			prepared: fixture.PreparedEdit, baseSnapshot: wrongSnapshotRaw,
		},
		{
			name: "unsafe active restore", proposal: mustMarshal(t, unsafeProposal),
			prepared: unsafePreparedRaw, baseSnapshot: fixture.BaseSnapshot,
		},
		{
			name:     "v1 wire reinterpreted as v2",
			proposal: mustMarshal(t, reinterpretedProposal),
			prepared: reinterpretedPreparedRaw, baseSnapshot: fixture.BaseSnapshot,
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := DecodeFrozenProposal(
				testCase.proposal, fixture.BaseDefinition, fixture.TargetDefinition,
				testCase.prepared, testCase.baseSnapshot,
			)
			if !errors.Is(err, ErrInvalid) {
				t.Fatalf("DecodeFrozenProposal() error = %v, want ErrInvalid", err)
			}
		})
	}
}

func TestDecodePhaseSnapshotBytes_BindsExactPreparedRepresentation(t *testing.T) {
	fixture, _ := loadFixture(t)
	var prepared PreparedEditV1
	if err := json.Unmarshal(fixture.PreparedEdit, &prepared); err != nil {
		t.Fatal(err)
	}
	snapshot := SnapshotV1{
		TaskID:               prepared.Creation.TaskID,
		RequestDigest:        prepared.RequestDigest,
		Phase:                SnapshotPhaseTargetPaused,
		RepresentationDigest: prepared.TargetPaused.Digest,
		Revision:             "Aw",
	}
	raw := mustMarshal(t, snapshot)
	decoded, err := DecodePhaseSnapshotBytes(fixture.PreparedEdit, raw)
	if err != nil {
		t.Fatalf("DecodePhaseSnapshotBytes() error = %v", err)
	}
	if decoded != snapshot {
		t.Fatalf("decoded snapshot = %+v, want %+v", decoded, snapshot)
	}

	for name, invalidRaw := range map[string][]byte{
		"non canonical": append([]byte("\n"), raw...),
		"wrong digest": mustMarshal(t, func() SnapshotV1 {
			value := snapshot
			value.RepresentationDigest = prepared.BasePaused.Digest
			return value
		}()),
		"padded revision": mustMarshal(t, func() SnapshotV1 {
			value := snapshot
			value.Revision = "Aw=="
			return value
		}()),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodePhaseSnapshotBytes(
				fixture.PreparedEdit, invalidRaw,
			); !errors.Is(err, ErrInvalid) {
				t.Fatalf("DecodePhaseSnapshotBytes() error = %v, want ErrInvalid", err)
			}
		})
	}
}

func TestDecodeFrozenProposal_RecomputesSemanticDigests(t *testing.T) {
	fixture, proposalRaw := loadFixture(t)
	var proposal ProposalV2
	if err := json.Unmarshal(proposalRaw, &proposal); err != nil {
		t.Fatal(err)
	}
	var prepared PreparedEditV1
	if err := json.Unmarshal(fixture.PreparedEdit, &prepared); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		mutate func(*PreparedEditV1)
	}{
		{
			name: "representation digest self claim",
			mutate: func(value *PreparedEditV1) {
				value.TargetFinal.Digest = strings.Repeat("a", 64)
			},
		},
		{
			name: "operation digest self claim",
			mutate: func(value *PreparedEditV1) {
				value.OperationDigest = strings.Repeat("b", 64)
			},
		},
		{
			name: "request digest self claim",
			mutate: func(value *PreparedEditV1) {
				value.RequestDigest = strings.Repeat("c", 64)
			},
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			changed := prepared
			testCase.mutate(&changed)
			changedRaw := mustMarshal(t, changed)
			changedProposal := proposal
			changedProposal.PreparedEditDigest = digest(changedRaw)
			_, err := DecodeFrozenProposal(
				mustMarshal(t, changedProposal), fixture.BaseDefinition,
				fixture.TargetDefinition, changedRaw, fixture.BaseSnapshot,
			)
			if !errors.Is(err, ErrInvalid) {
				t.Fatalf("DecodeFrozenProposal() error = %v, want ErrInvalid", err)
			}
		})
	}
}

func TestValidateApprovedProjectionBindings(t *testing.T) {
	fixture, proposal := loadFixture(t)
	frozen, err := DecodeFrozenProposal(
		proposal, fixture.BaseDefinition, fixture.TargetDefinition,
		fixture.PreparedEdit, fixture.BaseSnapshot,
	)
	if err != nil {
		t.Fatal(err)
	}
	type approvedProjectionFields struct {
		NLDescription string          `json:"nl_description"`
		SpecJSON      json.RawMessage `json:"spec_json"`
		ScopeJSON     json.RawMessage `json:"scope_json"`
	}
	var base, target approvedProjectionFields
	if err := json.Unmarshal(fixture.BaseDefinition, &base); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(fixture.TargetDefinition, &target); err != nil {
		t.Fatal(err)
	}
	if err := ValidateApprovedProjectionBindings(
		frozen.Prepared,
		base.SpecJSON, base.ScopeJSON, base.NLDescription,
		target.SpecJSON, target.ScopeJSON, target.NLDescription,
	); err != nil {
		t.Fatalf("valid projection bindings rejected: %v", err)
	}
	if err := ValidateApprovedProjectionBindings(
		frozen.Prepared,
		base.SpecJSON, base.ScopeJSON, base.NLDescription,
		target.SpecJSON, target.ScopeJSON, target.NLDescription+" changed",
	); !errors.Is(err, ErrInvalid) {
		t.Fatalf("changed target projection error = %v, want ErrInvalid", err)
	}
}

func TestValidateApprovedProjectionBindings_AnchoredIntervalUsesWriterLayout(t *testing.T) {
	specRaw := []byte(`{"anchor_at":"2026-07-21T08:00:00Z","every_seconds":7200,"tz":"UTC"}`)
	scopeRaw := []byte(`{"source_ids":[11,22],"top_n":3}`)
	nlDescription := "Every two hours from the approved anchor"
	wantProjection := approvedProjectionV1{
		Spec: ScheduleSpecV1{
			EverySeconds: 7200,
			AnchorAt:     "2026-07-21T08:00:00Z",
			TZ:           "UTC",
		},
		Scope:         PushScopeV1{SourceIDs: []int64{11, 22}, TopN: 3},
		NLDescription: nlDescription,
	}
	wantDigest, err := digestJSON(wantProjection)
	if err != nil {
		t.Fatal(err)
	}
	prepared := PreparedEditV1{
		BaseProjectionDigest:   wantDigest,
		TargetProjectionDigest: wantDigest,
		BaseOriginal: RepresentationV1{Action: PreparedActionV1{
			Params: PushParamsV1{Scope: wantProjection.Scope, NLDesc: nlDescription},
		}},
		TargetFinal: RepresentationV1{Action: PreparedActionV1{
			Params: PushParamsV1{Scope: wantProjection.Scope, NLDesc: nlDescription},
		}},
	}
	if err := ValidateApprovedProjectionBindings(
		prepared,
		specRaw, scopeRaw, nlDescription,
		specRaw, scopeRaw, nlDescription,
	); err != nil {
		t.Fatalf("anchored interval projection rejected: %v", err)
	}
}

func TestValidateApprovedProjectionBindings_ObservationStaysOutsideTemporalProjection(t *testing.T) {
	specRaw := []byte(`{"cron":"0 9 * * 1","tz":"Asia/Shanghai"}`)
	legacyScope := PushScopeV1{SourceIDs: []int64{11, 22}, TopN: 3}
	scopeRaw := []byte(`{"observation":{"effective_at":"2026-07-25T01:00:00Z","evidence":{"requirement":"trusted_allowed"},"late_policy":"strict","mode":"content","schema":"vane.observation-policy/v1","unknown_time":"reject","window":{"kind":"schedule_interval"}},"source_ids":[11,22],"top_n":3}`)
	nlDescription := "每周一 09:00 检查官方更新"
	wantProjection := approvedProjectionV1{
		Spec: ScheduleSpecV1{
			Cron: "0 9 * * 1",
			TZ:   "Asia/Shanghai",
		},
		Scope:         legacyScope,
		NLDescription: nlDescription,
	}
	wantDigest, err := digestJSON(wantProjection)
	if err != nil {
		t.Fatal(err)
	}
	prepared := PreparedEditV1{
		BaseProjectionDigest:   wantDigest,
		TargetProjectionDigest: wantDigest,
		BaseOriginal: RepresentationV1{Action: PreparedActionV1{
			Params: PushParamsV1{Scope: legacyScope, NLDesc: nlDescription},
		}},
		TargetFinal: RepresentationV1{Action: PreparedActionV1{
			Params: PushParamsV1{Scope: legacyScope, NLDesc: nlDescription},
		}},
	}
	if err := ValidateApprovedProjectionBindings(
		prepared,
		specRaw, scopeRaw, nlDescription,
		specRaw, scopeRaw, nlDescription,
	); err != nil {
		t.Fatalf("observation policy changed retained Temporal projection: %v", err)
	}

	invalid := bytes.Replace(scopeRaw, []byte(`"schema":`),
		[]byte(`"future":true,"schema":`), 1)
	if err := ValidateApprovedProjectionBindings(
		prepared,
		specRaw, scopeRaw, nlDescription,
		specRaw, invalid, nlDescription,
	); !errors.Is(err, ErrInvalid) {
		t.Fatalf("unknown observation field error = %v, want ErrInvalid", err)
	}
}

func TestRetainedActionOwnerAcceptsEveryCurrentWriterRuntime(t *testing.T) {
	creation := PreparedCreationV1{
		TenantID: 7,
		UserID:   42,
		TaskID:   "task-v1-runtime-compat",
	}
	action := PreparedActionV1{
		TaskQueue:    "vane-push",
		WorkflowType: "PushPipelineWorkflow",
		ActionID:     "push-task-v1-runtime-compat",
		Params: PushParamsV1{
			TenantID:      creation.TenantID,
			UserID:        creation.UserID,
			RunKind:       "scheduled",
			ExecutionMode: "compiled",
			ScheduleID:    creation.TaskID,
		},
	}
	runtimes := []string{
		"",
		workflowruntime.CompiledSnapshotV1,
		workflowruntime.RunOutcomeV1,
		workflowruntime.CanonicalBriefV1,
		workflowruntime.StructuredInsightV1,
		workflowruntime.StructuredEventEvidenceV1,
		workflowruntime.ExecutiveBriefV1,
		workflowruntime.CompiledToolSnapshotV2,
	}
	for _, runtime := range runtimes {
		action.Params.RuntimeVersion = runtime
		if err := validateActionOwner(action, creation); err != nil {
			t.Fatalf("current writer runtime %q rejected: %v", runtime, err)
		}
	}
	action.Params.RuntimeVersion = "compiled-snapshot/v1+future/v1"
	if err := validateActionOwner(action, creation); err == nil {
		t.Fatal("unknown future runtime accepted")
	}
}

func TestValidateApprovedProjectionBindings_AcceptsTaskstateCanonicalObservationScope(t *testing.T) {
	fixture, proposal := loadFixture(t)
	frozen, err := DecodeFrozenProposal(
		proposal, fixture.BaseDefinition, fixture.TargetDefinition,
		fixture.PreparedEdit, fixture.BaseSnapshot,
	)
	if err != nil {
		t.Fatal(err)
	}
	target, err := taskstate.DecodeApprovedDefinitionV1(fixture.TargetDefinition)
	if err != nil {
		t.Fatal(err)
	}
	var legacyScope PushScopeV1
	if err := json.Unmarshal(target.ScopeJSON, &legacyScope); err != nil {
		t.Fatal(err)
	}
	target.ScopeJSON, err = json.Marshal(struct {
		SourceIDs   []int64         `json:"source_ids,omitempty"`
		TopN        int             `json:"top_n,omitempty"`
		Observation json.RawMessage `json:"observation"`
	}{
		SourceIDs: legacyScope.SourceIDs,
		TopN:      legacyScope.TopN,
		Observation: json.RawMessage(
			`{"schema":"vane.observation-policy/v1","mode":"content","window":{"kind":"schedule_interval"},"late_policy":"strict","evidence":{"requirement":"trusted_allowed"},"unknown_time":"reject","effective_at":"2026-07-25T01:00:00Z"}`,
		),
	})
	if err != nil {
		t.Fatal(err)
	}
	targetBytes, err := taskstate.EncodeApprovedDefinitionV1(target)
	if err != nil {
		t.Fatal(err)
	}
	type projectionFields struct {
		NLDescription string          `json:"nl_description"`
		SpecJSON      json.RawMessage `json:"spec_json"`
		ScopeJSON     json.RawMessage `json:"scope_json"`
	}
	var baseWire, targetWire projectionFields
	if err := json.Unmarshal(fixture.BaseDefinition, &baseWire); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(targetBytes, &targetWire); err != nil {
		t.Fatal(err)
	}
	if !bytes.HasPrefix(targetWire.ScopeJSON, []byte(`{"observation":`)) {
		t.Fatalf("taskstate scope was not map-canonical: %s", targetWire.ScopeJSON)
	}
	if err := ValidateApprovedProjectionBindings(
		frozen.Prepared,
		baseWire.SpecJSON, baseWire.ScopeJSON, baseWire.NLDescription,
		targetWire.SpecJSON, targetWire.ScopeJSON, targetWire.NLDescription,
	); err != nil {
		t.Fatalf("taskstate-canonical observation scope rejected: %v", err)
	}
}

func loadFixture(t *testing.T) (componentFixtureV1, []byte) {
	t.Helper()
	raw, err := os.ReadFile("../task/testdata/definition_edit_proposal_components_v1.json")
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	var fixture componentFixtureV1
	if err := json.Unmarshal(raw, &fixture); err != nil {
		t.Fatalf("decode fixture: %v", err)
	}
	var prepared PreparedEditV1
	if err := json.Unmarshal(fixture.PreparedEdit, &prepared); err != nil {
		t.Fatalf("decode prepared fixture: %v", err)
	}
	proposal := ProposalV2{
		WireVersion:  proposalWireVersionV2,
		OperationID:  prepared.OperationID,
		OperationRef: "approval-definition-edit-0001",
		Actor: ProposalActorV2{
			TenantID: prepared.Creation.TenantID,
			UserID:   prepared.Creation.UserID,
		},
		Target: ProposalTargetV2{
			TenantID: prepared.Creation.TenantID,
			UserID:   prepared.Creation.UserID,
			TaskID:   prepared.Creation.TaskID,
		},
		SessionID:              91,
		ExpiresAtUnixMicros:    1_780_000_000_123_456,
		OriginalStatus:         prepared.OriginalState,
		BaseHead:               prepared.BaseHead,
		TargetHead:             prepared.TargetHead,
		TargetDefinitionDigest: digest(fixture.TargetDefinition),
		PreparedEditDigest:     digest(fixture.PreparedEdit),
		BaseSnapshotDigest:     digest(fixture.BaseSnapshot),
	}
	return fixture, mustMarshal(t, proposal)
}

func mustMarshal(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal fixture: %v", err)
	}
	return raw
}

func rebindPreparedEditDigests(
	t *testing.T,
	prepared *PreparedEditV1,
	basePhase string,
) {
	t.Helper()
	operationDigest, err := digestJSON(operationSeedV1{
		WireVersion:            prepared.WireVersion,
		OperationID:            prepared.OperationID,
		CreationRequestDigest:  prepared.Creation.RequestDigest,
		TenantID:               prepared.Creation.TenantID,
		UserID:                 prepared.Creation.UserID,
		TaskID:                 prepared.Creation.TaskID,
		BaseHead:               prepared.BaseHead,
		TargetHead:             prepared.TargetHead,
		OriginalState:          prepared.OriginalState,
		BaseProjectionDigest:   prepared.BaseProjectionDigest,
		TargetProjectionDigest: prepared.TargetProjectionDigest,
		BaseTiming:             prepared.BaseOriginal.Timing,
		BaseAction:             prepared.BaseOriginal.Action,
		BasePolicy:             prepared.BaseOriginal.Policy,
		BaseReusePolicy:        prepared.BaseOriginal.WorkflowIDReusePolicy,
		BaseState:              prepared.BaseOriginal.State,
		TargetTiming:           prepared.TargetFinal.Timing,
		TargetAction:           prepared.TargetFinal.Action,
		TargetPolicy:           prepared.TargetFinal.Policy,
		TargetReusePolicy:      prepared.TargetFinal.WorkflowIDReusePolicy,
	})
	if err != nil {
		t.Fatalf("digest operation seed: %v", err)
	}
	prepared.OperationDigest = operationDigest

	phases := []struct {
		representation *RepresentationV1
		head           HeadV1
		phase          string
		updateNote     bool
	}{
		{&prepared.BaseOriginal, prepared.BaseHead, basePhase, false},
		{&prepared.BasePaused, prepared.BaseHead, "base_paused", true},
		{&prepared.TargetPaused, prepared.TargetHead, "target_paused", true},
		{&prepared.TargetFinal, prepared.TargetHead, "final_active", true},
	}
	for _, phase := range phases {
		phase.representation.Fingerprint.DefinitionVersion = phase.head.Version
		phase.representation.Fingerprint.DefinitionDigest = phase.head.Digest
		phase.representation.Fingerprint.EditOperationDigest = operationDigest
		phase.representation.Fingerprint.EditPhase = phase.phase
		if phase.updateNote {
			phase.representation.State.Note = noteFor(phase.phase, operationDigest)
		}
		recomputeRepresentationDigest(t, phase.representation)
	}
	recomputePreparedRequestDigest(t, prepared)
}

func recomputeRepresentationDigest(t *testing.T, representation *RepresentationV1) {
	t.Helper()
	seed := *representation
	seed.Digest = ""
	value, err := digestJSON(seed)
	if err != nil {
		t.Fatalf("digest representation: %v", err)
	}
	representation.Digest = value
}

func recomputePreparedRequestDigest(t *testing.T, prepared *PreparedEditV1) {
	t.Helper()
	seed := *prepared
	seed.RequestDigest = ""
	value, err := digestJSON(seed)
	if err != nil {
		t.Fatalf("digest prepared request: %v", err)
	}
	prepared.RequestDigest = value
}
