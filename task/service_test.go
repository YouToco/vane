package task

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/YouToco/vane/scheduler"
	"github.com/YouToco/vane/types"
	"github.com/YouToco/vane/workflow"
)

type recordedCreatePush struct {
	events *[]string

	scheduleID string
	err        error

	userID int64
	spec   scheduler.ScheduleSpec
	scope  workflow.PushScope
	nlDesc string
}

func (f *recordedCreatePush) CreatePush(
	_ context.Context,
	userID int64,
	spec scheduler.ScheduleSpec,
	scope workflow.PushScope,
	nlDesc string,
) (string, error) {
	*f.events = append(*f.events, "schedule")
	f.userID = userID
	f.spec = spec
	f.scope = scope
	f.nlDesc = nlDesc
	return f.scheduleID, f.err
}

type recordedPlaybookWriter struct {
	events *[]string

	ok  bool
	err error

	userID     int64
	scheduleID string
	content    string
}

func (f *recordedPlaybookWriter) UpsertSchedulePlaybook(
	_ context.Context,
	userID int64,
	scheduleID, content string,
) (bool, error) {
	*f.events = append(*f.events, "playbook")
	f.userID = userID
	f.scheduleID = scheduleID
	f.content = content
	return f.ok, f.err
}

type recordedPlaybookCompiler struct {
	events *[]string

	userID     int64
	scheduleID string
	content    string
}

func (f *recordedPlaybookCompiler) Compile(
	_ context.Context,
	userID int64,
	scheduleID, content string,
) {
	*f.events = append(*f.events, "compiler")
	f.userID = userID
	f.scheduleID = scheduleID
	f.content = content
}

type recordedStrictnessWriter struct {
	events *[]string
	err    error

	userID     int64
	scheduleID string
	strictness types.PushStrictness
}

func (f *recordedStrictnessWriter) SetScheduleStrictness(
	_ context.Context,
	scheduleID string,
	userID int64,
	strictness types.PushStrictness,
) error {
	*f.events = append(*f.events, "strictness")
	f.userID = userID
	f.scheduleID = scheduleID
	f.strictness = strictness
	return f.err
}

func TestServiceCreateSuccessPreservesOrderAndInputs(t *testing.T) {
	t.Parallel()

	var events []string
	schedules := &recordedCreatePush{events: &events, scheduleID: "sched-42"}
	playbooks := &recordedPlaybookWriter{events: &events, ok: true}
	compiler := &recordedPlaybookCompiler{events: &events}
	strictness := &recordedStrictnessWriter{events: &events}
	service := New(Deps{
		Schedules:  schedules,
		Playbooks:  playbooks,
		Compiler:   compiler,
		Strictness: strictness,
	})
	spec := scheduler.ScheduleSpec{
		Cron:     "0 8 * * *",
		AnchorAt: "2026-07-21T08:00:00+08:00",
		TZ:       "Asia/Shanghai",
	}
	input := CreateInput{
		UserID:          73,
		Spec:            spec,
		NLDescription:   "raw description that is intentionally not capped",
		PlaybookContent: "bounded playbook content",
		Strictness:      types.StrictnessStrict,
	}

	got, err := service.Create(context.Background(), input)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if want := (CreateResult{ScheduleID: "sched-42", StrictnessApplied: true}); got != want {
		t.Fatalf("Create() result = %+v, want %+v", got, want)
	}
	if want := []string{"schedule", "playbook", "compiler", "strictness"}; !reflect.DeepEqual(events, want) {
		t.Fatalf("call order = %v, want %v", events, want)
	}
	if schedules.userID != input.UserID || schedules.spec != spec || schedules.nlDesc != input.NLDescription {
		t.Fatalf("CreatePush inputs = user=%d spec=%+v nlDesc=%q, want user=%d spec=%+v nlDesc=%q",
			schedules.userID, schedules.spec, schedules.nlDesc,
			input.UserID, spec, input.NLDescription)
	}
	if !reflect.DeepEqual(schedules.scope, workflow.PushScope{}) {
		t.Fatalf("CreatePush scope = %+v, want zero value", schedules.scope)
	}
	if playbooks.userID != input.UserID || playbooks.scheduleID != got.ScheduleID || playbooks.content != input.PlaybookContent {
		t.Fatalf("playbook inputs = user=%d schedule=%q content=%q, want user=%d schedule=%q content=%q",
			playbooks.userID, playbooks.scheduleID, playbooks.content,
			input.UserID, got.ScheduleID, input.PlaybookContent)
	}
	if compiler.userID != input.UserID || compiler.scheduleID != got.ScheduleID || compiler.content != input.PlaybookContent {
		t.Fatalf("compiler inputs = user=%d schedule=%q content=%q, want user=%d schedule=%q content=%q",
			compiler.userID, compiler.scheduleID, compiler.content,
			input.UserID, got.ScheduleID, input.PlaybookContent)
	}
	if strictness.userID != input.UserID || strictness.scheduleID != got.ScheduleID || strictness.strictness != input.Strictness {
		t.Fatalf("strictness inputs = user=%d schedule=%q strictness=%q, want user=%d schedule=%q strictness=%q",
			strictness.userID, strictness.scheduleID, strictness.strictness,
			input.UserID, got.ScheduleID, input.Strictness)
	}
}

func TestServiceCreateSchedulerErrorStopsDownstreamAndPreservesIdentity(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("scheduler unavailable")
	var events []string
	service := New(Deps{
		Schedules: &recordedCreatePush{
			events: &events,
			err:    sentinel,
		},
		Playbooks: &recordedPlaybookWriter{events: &events, ok: true},
		Compiler:  &recordedPlaybookCompiler{events: &events},
		Strictness: &recordedStrictnessWriter{
			events: &events,
		},
	})

	got, err := service.Create(context.Background(), CreateInput{
		UserID:          73,
		PlaybookContent: "bounded",
		Strictness:      types.StrictnessNormal,
	})
	if !errors.Is(err, sentinel) || err != sentinel {
		t.Fatalf("Create() error = %v, want exact sentinel %v", err, sentinel)
	}
	if got != (CreateResult{}) {
		t.Fatalf("Create() result = %+v, want zero value", got)
	}
	if want := []string{"schedule"}; !reflect.DeepEqual(events, want) {
		t.Fatalf("calls = %v, want %v", events, want)
	}
}

func TestServiceCreatePlaybookOutcomesRemainBestEffort(t *testing.T) {
	t.Parallel()

	playbookErr := errors.New("playbook unavailable")
	tests := []struct {
		name        string
		playbookOK  bool
		playbookErr error
		wantEvents  []string
	}{
		{
			name:        "write error",
			playbookErr: playbookErr,
			wantEvents:  []string{"schedule", "playbook", "strictness"},
		},
		{
			name:       "ownership miss",
			playbookOK: false,
			wantEvents: []string{"schedule", "playbook", "strictness"},
		},
		{
			name:       "success compiles before strictness",
			playbookOK: true,
			wantEvents: []string{"schedule", "playbook", "compiler", "strictness"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var events []string
			service := New(Deps{
				Schedules: &recordedCreatePush{
					events:     &events,
					scheduleID: "sched-42",
				},
				Playbooks: &recordedPlaybookWriter{
					events: &events,
					ok:     tt.playbookOK,
					err:    tt.playbookErr,
				},
				Compiler: &recordedPlaybookCompiler{events: &events},
				Strictness: &recordedStrictnessWriter{
					events: &events,
				},
			})

			got, err := service.Create(context.Background(), CreateInput{
				UserID:          73,
				PlaybookContent: "bounded",
				Strictness:      types.StrictnessNormal,
			})
			if err != nil {
				t.Fatalf("Create() error = %v", err)
			}
			if want := (CreateResult{ScheduleID: "sched-42", StrictnessApplied: true}); got != want {
				t.Fatalf("Create() result = %+v, want %+v", got, want)
			}
			if !reflect.DeepEqual(events, tt.wantEvents) {
				t.Fatalf("calls = %v, want %v", events, tt.wantEvents)
			}
		})
	}
}

func TestServiceCreateStrictnessOutcomes(t *testing.T) {
	t.Parallel()

	strictnessErr := errors.New("strictness unavailable")
	tests := []struct {
		name        string
		strictness  types.PushStrictness
		writer      func(*[]string) StrictnessWriter
		wantApplied bool
		wantEvents  []string
	}{
		{
			name:       "write error",
			strictness: types.StrictnessStrict,
			writer: func(events *[]string) StrictnessWriter {
				return &recordedStrictnessWriter{events: events, err: strictnessErr}
			},
			wantEvents: []string{"schedule", "playbook", "compiler", "strictness"},
		},
		{
			name:        "success",
			strictness:  types.StrictnessLoose,
			writer:      func(events *[]string) StrictnessWriter { return &recordedStrictnessWriter{events: events} },
			wantApplied: true,
			wantEvents:  []string{"schedule", "playbook", "compiler", "strictness"},
		},
		{
			name:       "empty strictness skips writer",
			writer:     func(events *[]string) StrictnessWriter { return &recordedStrictnessWriter{events: events} },
			wantEvents: []string{"schedule", "playbook", "compiler"},
		},
		{
			name:       "nil writer skips strictness",
			strictness: types.StrictnessNormal,
			wantEvents: []string{"schedule", "playbook", "compiler"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var events []string
			var writer StrictnessWriter
			if tt.writer != nil {
				writer = tt.writer(&events)
			}
			service := New(Deps{
				Schedules: &recordedCreatePush{
					events:     &events,
					scheduleID: "sched-42",
				},
				Playbooks:  &recordedPlaybookWriter{events: &events, ok: true},
				Compiler:   &recordedPlaybookCompiler{events: &events},
				Strictness: writer,
			})

			got, err := service.Create(context.Background(), CreateInput{
				UserID:          73,
				PlaybookContent: "bounded",
				Strictness:      tt.strictness,
			})
			if err != nil {
				t.Fatalf("Create() error = %v", err)
			}
			if got.ScheduleID != "sched-42" || got.StrictnessApplied != tt.wantApplied {
				t.Fatalf("Create() result = %+v, want schedule=sched-42 applied=%t", got, tt.wantApplied)
			}
			if !reflect.DeepEqual(events, tt.wantEvents) {
				t.Fatalf("calls = %v, want %v", events, tt.wantEvents)
			}
		})
	}
}

func TestServiceCreateNilCompilerSkipsCompilation(t *testing.T) {
	t.Parallel()

	var events []string
	service := New(Deps{
		Schedules: &recordedCreatePush{
			events:     &events,
			scheduleID: "sched-42",
		},
		Playbooks: &recordedPlaybookWriter{events: &events, ok: true},
	})

	got, err := service.Create(context.Background(), CreateInput{
		UserID:          73,
		PlaybookContent: "bounded",
	})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if got != (CreateResult{ScheduleID: "sched-42"}) {
		t.Fatalf("Create() result = %+v, want schedule only", got)
	}
	if want := []string{"schedule", "playbook"}; !reflect.DeepEqual(events, want) {
		t.Fatalf("calls = %v, want %v", events, want)
	}
}
