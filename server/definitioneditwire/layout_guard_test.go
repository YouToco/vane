package definitioneditwire_test

import (
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/YouToco/vane/definitioneditwire"
	"github.com/YouToco/vane/scheduler"
	"github.com/YouToco/vane/task"
	"github.com/YouToco/vane/taskstate"
	"github.com/YouToco/vane/types"
	"github.com/YouToco/vane/workflow"
)

// The retained reader deliberately duplicates only JSON layout, not runtime
// scheduler behavior. This guard makes any source-wire field change fail at
// review time instead of silently causing newly sealed operations to become
// unreadable by the Store.
func TestRetainedReaderMirrorsFrozenSourceLayouts(t *testing.T) {
	t.Parallel()
	pairs := []struct {
		name      string
		retained  any
		authority any
	}{
		{"proposal", definitioneditwire.ProposalV2{}, task.TaskDefinitionEditProposalV2{}},
		{"proposal actor", definitioneditwire.ProposalActorV2{}, task.TaskDefinitionEditProposalActorV2{}},
		{"proposal target", definitioneditwire.ProposalTargetV2{}, task.TaskDefinitionEditProposalTargetV2{}},
		{"head", definitioneditwire.HeadV1{}, scheduler.TaskDefinitionEditHead{}},
		{"schedule spec", definitioneditwire.ScheduleSpecV1{}, scheduler.ScheduleSpec{}},
		{"prepared edit", definitioneditwire.PreparedEditV1{}, scheduler.PreparedTaskDefinitionEdit{}},
		{"creation", definitioneditwire.PreparedCreationV1{}, scheduler.PreparedTaskSchedule{}},
		{"timing", definitioneditwire.PreparedTimingV1{}, scheduler.PreparedTaskScheduleTiming{}},
		{"calendar", definitioneditwire.PreparedCalendarV1{}, scheduler.PreparedTaskScheduleCalendar{}},
		{"action", definitioneditwire.PreparedActionV1{}, scheduler.PreparedTaskScheduleAction{}},
		{"params", definitioneditwire.PushParamsV1{}, workflow.PushParams{}},
		{"scope", definitioneditwire.PushScopeV1{}, workflow.PushScope{}},
		{"policy", definitioneditwire.PreparedPolicyV1{}, scheduler.PreparedTaskSchedulePolicy{}},
		{"creation state", definitioneditwire.PreparedCreationStateV1{}, scheduler.PreparedTaskScheduleCreation{}},
		{"representation", definitioneditwire.RepresentationV1{}, scheduler.PreparedTaskDefinitionEditSchedule{}},
		{"schedule state", definitioneditwire.ScheduleStateV1{}, scheduler.TaskDefinitionEditScheduleState{}},
		{"fingerprint", definitioneditwire.FingerprintV1{}, scheduler.TaskDefinitionEditFingerprint{}},
		{"snapshot", definitioneditwire.SnapshotV1{}, scheduler.TaskDefinitionEditSnapshot{}},
	}
	for _, pair := range pairs {
		t.Run(pair.name, func(t *testing.T) {
			got := jsonLayout(reflect.TypeOf(pair.retained))
			want := jsonLayout(reflect.TypeOf(pair.authority))
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("retained layout=%v, authority=%v", got, want)
			}
		})
	}
}

func TestRetainedReaderAcceptsCurrentWriterCheckpoints(t *testing.T) {
	raw, err := os.ReadFile("../task/testdata/definition_edit_proposal_components_v1.json")
	if err != nil {
		t.Fatal(err)
	}
	var components struct {
		BaseDefinition   json.RawMessage `json:"base_definition"`
		TargetDefinition json.RawMessage `json:"target_definition"`
		PreparedEdit     json.RawMessage `json:"prepared_edit"`
		BaseSnapshot     json.RawMessage `json:"base_snapshot"`
	}
	if err := json.Unmarshal(raw, &components); err != nil {
		t.Fatal(err)
	}
	base, err := taskstate.DecodeApprovedDefinitionV1(components.BaseDefinition)
	if err != nil {
		t.Fatal(err)
	}
	target, err := taskstate.DecodeApprovedDefinitionV1(components.TargetDefinition)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := scheduler.DecodePreparedTaskDefinitionEdit(components.PreparedEdit)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := scheduler.DecodeTaskDefinitionEditBaseSnapshot(
		prepared, components.BaseSnapshot,
	)
	if err != nil {
		t.Fatal(err)
	}
	frozen, err := task.BuildFrozenTaskDefinitionEditProposal(
		task.BuildTaskDefinitionEditProposalInput{
			OperationID:      prepared.OperationID,
			OperationRef:     "approval-definition-edit-0001",
			ActorTenantID:    prepared.Creation.TenantID,
			ActorUserID:      prepared.Creation.UserID,
			TargetTenantID:   prepared.Creation.TenantID,
			TargetUserID:     prepared.Creation.UserID,
			TaskID:           prepared.Creation.TaskID,
			SessionID:        91,
			ExpiresAt:        time.UnixMicro(1_780_000_000_123_456).UTC(),
			OriginalStatus:   types.ScheduleStatusActive,
			BaseHead:         prepared.BaseHead,
			TargetHead:       prepared.TargetHead,
			BaseDefinition:   base,
			TargetDefinition: target,
			PreparedEdit:     prepared,
			BaseSnapshot:     snapshot,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := definitioneditwire.DecodeFrozenProposal(
		frozen.CanonicalProposal,
		frozen.BaseDefinitionBytes,
		frozen.TargetDefinitionBytes,
		frozen.PreparedEditBytes,
		frozen.BaseSnapshotBytes,
	); err != nil {
		t.Fatalf("retained reader rejected current writer checkpoints: %v", err)
	}
}

func jsonLayout(value reflect.Type) []string {
	fields := make([]string, 0, value.NumField())
	for index := range value.NumField() {
		field := value.Field(index)
		if !field.IsExported() {
			continue
		}
		tag := field.Tag.Get("json")
		if tag == "-" {
			continue
		}
		name, options, _ := strings.Cut(tag, ",")
		if name == "" {
			name = field.Name
		}
		fields = append(fields, name+","+options)
	}
	return fields
}
