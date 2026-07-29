package periodicbrief

import (
	"testing"
	"time"

	"github.com/YouToco/vane/store"
)

func TestExistingPeriodicBriefRunBindingIsIdempotent(t *testing.T) {
	for _, test := range []struct {
		name      string
		stored    string
		reported  string
		wantRunID string
		wantBind  bool
	}{
		{
			name:   "sealed identity survives process restart",
			stored: "original-run", reported: "reset-run",
		},
		{
			name:     "crash window binds Temporal reported identity",
			reported: "started-run", wantRunID: "started-run",
			wantBind: true,
		},
		{
			name:     "legacy Temporal error falls back to describe",
			wantBind: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			runID, bind := existingPeriodicBriefRunBinding(
				test.stored, test.reported)
			if runID != test.wantRunID || bind != test.wantBind {
				t.Fatalf(
					"binding=(%q,%v), want (%q,%v)",
					runID, bind, test.wantRunID, test.wantBind,
				)
			}
		})
	}
}

func TestPreviousNaturalPeriodV1UsesCalendarAcrossDST(t *testing.T) {
	now := time.Date(2026, 3, 9, 12, 0, 0, 0, time.UTC)
	start, end, err := previousNaturalPeriodV1(
		now, "America/Los_Angeles", store.BriefReportCadenceDaily)
	if err != nil {
		t.Fatal(err)
	}
	if got := end.Sub(start); got != 23*time.Hour {
		t.Fatalf("DST day must be 23h, got %s (%s..%s)", got, start, end)
	}
	weeklyStart, weeklyEnd, err := previousNaturalPeriodV1(
		time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC),
		"Asia/Shanghai", store.BriefReportCadenceWeekly)
	if err != nil {
		t.Fatal(err)
	}
	location, _ := time.LoadLocation("Asia/Shanghai")
	if weeklyStart.In(location).Weekday() != time.Monday ||
		weeklyEnd.In(location).Weekday() != time.Monday ||
		weeklyEnd.In(location).Hour() != 0 {
		t.Fatalf("weekly bounds are not natural Mondays: %s..%s",
			weeklyStart, weeklyEnd)
	}
}
