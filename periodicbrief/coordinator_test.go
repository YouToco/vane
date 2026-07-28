package periodicbrief

import (
	"testing"
	"time"

	"github.com/YouToco/vane/store"
)

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
