package api

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/YouToco/vane/types"
)

func TestScheduleDetailCapabilitiesExposeDefinitionEditFlag(t *testing.T) {
	for _, tc := range []struct {
		name string
		flag bool
		want string
	}{
		{
			name: "disabled remains explicit",
			flag: false,
			want: `"capabilities":{"definition_edit":false}`,
		},
		{
			name: "enabled",
			flag: true,
			want: `"capabilities":{"definition_edit":true}`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := json.Marshal(scheduleDetailResp{
				Capabilities: scheduleCapabilitiesDTO{
					DefinitionEdit: tc.flag,
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(raw), tc.want) {
				t.Fatalf("response=%s, want %s", raw, tc.want)
			}
		})
	}
}

type scheduleNextRunReaderStub struct {
	next  *time.Time
	err   error
	calls int
}

func (s *scheduleNextRunReaderStub) NextRun(
	context.Context,
	string,
	int64,
) (*time.Time, error) {
	s.calls++
	return s.next, s.err
}

func TestScheduleDetailNextRunStateDistinguishesLiveProjection(t *testing.T) {
	next := time.Date(2026, 7, 27, 1, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name      string
		status    types.ScheduleStatus
		reader    *scheduleNextRunReaderStub
		wantState scheduleNextRunState
		wantNext  bool
		wantCalls int
		wantErr   bool
	}{
		{
			name: "paused does not query Temporal", status: types.ScheduleStatusPaused,
			reader:    &scheduleNextRunReaderStub{err: errors.New("must not run")},
			wantState: scheduleNextRunPaused,
		},
		{
			name: "active with no action", status: types.ScheduleStatusActive,
			reader:    &scheduleNextRunReaderStub{},
			wantState: scheduleNextRunNone, wantCalls: 1,
		},
		{
			name: "Temporal failure is explicit", status: types.ScheduleStatusActive,
			reader:    &scheduleNextRunReaderStub{err: context.DeadlineExceeded},
			wantState: scheduleNextRunUnavailable, wantCalls: 1, wantErr: true,
		},
		{
			name: "next action is scheduled", status: types.ScheduleStatusActive,
			reader:    &scheduleNextRunReaderStub{next: &next},
			wantState: scheduleNextRunScheduled, wantNext: true, wantCalls: 1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gotNext, gotState, err := projectScheduleNextRun(
				t.Context(),
				&types.Schedule{
					ID: "detail-task", UserID: 7, Status: tc.status,
				},
				tc.reader,
			)
			if (err != nil) != tc.wantErr {
				t.Fatalf("error=%v wantErr=%t", err, tc.wantErr)
			}
			if gotState != tc.wantState {
				t.Fatalf("state=%q want=%q", gotState, tc.wantState)
			}
			if (gotNext != nil) != tc.wantNext {
				t.Fatalf("next=%v wantPresent=%t", gotNext, tc.wantNext)
			}
			if tc.reader.calls != tc.wantCalls {
				t.Fatalf("Temporal calls=%d want=%d",
					tc.reader.calls, tc.wantCalls)
			}
		})
	}
}
