package scheduler

import (
	"strings"
	"testing"

	"github.com/YouToco/vane/server/workflow"
)

func TestValidateTaskScheduleSpec_UsesCompleteA3TimingContract(t *testing.T) {
	t.Parallel()

	valid := []ScheduleSpec{
		{Cron: "15 8 * * MON-FRI", TZ: "Asia/Shanghai"},
		{EverySeconds: 7200, AnchorAt: "2026-07-21T08:00:00+08:00", TZ: "Asia/Shanghai"},
	}
	for _, spec := range valid {
		if err := ValidateTaskScheduleSpec(spec); err != nil {
			t.Fatalf("valid spec %+v: %v", spec, err)
		}
	}

	invalid := []ScheduleSpec{
		{Cron: "15 25 * * *", TZ: "Asia/Shanghai"},
		{Cron: "15 8 * * *", TZ: "Not/A-Time-Zone"},
		{EverySeconds: 3600, AnchorAt: "2026-07-21T08:00:00.1+08:00"},
		{EverySeconds: 3599},
		{Cron: "*/5 * * * *"},
	}
	for _, spec := range invalid {
		if err := ValidateTaskScheduleSpec(spec); err == nil {
			t.Fatalf("invalid spec unexpectedly accepted: %+v", spec)
		}
	}
}

func TestValidatePreparedTaskScheduleRequest_BindsRequestFields(t *testing.T) {
	t.Parallel()

	s := newTaskScheduleTestScheduler(newTaskScheduleFakeClient())
	req := validTaskScheduleRequest()
	prepared := preparedTaskSchedule(t, s, req)
	if err := ValidatePreparedTaskScheduleRequest(prepared, req); err != nil {
		t.Fatalf("valid prepared/request pair: %v", err)
	}

	tests := []struct {
		name string
		req  TaskScheduleRequest
	}{
		{
			name: "definition digest",
			req: func() TaskScheduleRequest {
				changed := req
				changed.PreparedDigest = strings.Repeat("b", 64)
				return changed
			}(),
		},
		{
			name: "timing",
			req: func() TaskScheduleRequest {
				changed := req
				changed.Spec = ScheduleSpec{Cron: "30 9 * * *", TZ: "Asia/Shanghai"}
				return changed
			}(),
		},
		{
			name: "description",
			req: func() TaskScheduleRequest {
				changed := req
				changed.NLDescription = "另一份任务"
				return changed
			}(),
		},
		{
			name: "scope",
			req: func() TaskScheduleRequest {
				changed := req
				changed.Scope = workflow.PushScope{SourceIDs: []int64{11}, TopN: 3}
				return changed
			}(),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := ValidatePreparedTaskScheduleRequest(prepared, tc.req); err == nil {
				t.Fatal("mismatched request unexpectedly accepted")
			}
		})
	}

	// A self-consistent Prepared value for a different schedule must also be
	// rejected. Merely checking RequestDigest against itself would miss this.
	otherReq := req
	otherReq.Spec = ScheduleSpec{Cron: "30 9 * * *", TZ: "Asia/Shanghai"}
	otherPrepared := preparedTaskSchedule(t, s, otherReq)
	if err := ValidatePreparedTaskScheduleRequest(otherPrepared, req); err == nil {
		t.Fatal("self-consistent prepared schedule for another timing was accepted")
	}
}
