package store

import "testing"

func TestReportScopeInfersNoHistoryCadenceFromTaskFrequency(t *testing.T) {
	tests := []struct {
		name string
		spec string
		want BriefReportCadenceV1
	}{
		{
			name: "twice daily interval",
			spec: `{"tz":"Asia/Shanghai","every_seconds":43200}`,
			want: BriefReportCadenceDaily,
		},
		{
			name: "twice weekly interval",
			spec: `{"tz":"Asia/Shanghai","every_seconds":302400}`,
			want: BriefReportCadenceWeekly,
		},
		{
			name: "twice daily cron",
			spec: `{"tz":"America/Los_Angeles","cron":"0 */12 * * *"}`,
			want: BriefReportCadenceDaily,
		},
		{
			name: "twice weekly cron",
			spec: `{"tz":"America/Los_Angeles","cron":"0 9 * * 1,4"}`,
			want: BriefReportCadenceWeekly,
		},
		{
			name: "monthly cron",
			spec: `{"tz":"America/Los_Angeles","cron":"0 9 1 * *"}`,
			want: BriefReportCadenceMonthly,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			scope, err := reportScopeFromSpecV1([]byte(test.spec))
			if err != nil {
				t.Fatal(err)
			}
			if scope.Cadence != test.want {
				t.Fatalf("cadence=%s want=%s",
					scope.Cadence, test.want)
			}
		})
	}
}
