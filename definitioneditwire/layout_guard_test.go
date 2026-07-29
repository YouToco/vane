package definitioneditwire_test

import (
	"reflect"
	"strings"
	"testing"

	"github.com/YouToco/vane/definitioneditwire"
	"github.com/YouToco/vane/scheduler"
	"github.com/YouToco/vane/task"
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
