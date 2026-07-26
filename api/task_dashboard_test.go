package api

import (
	"encoding/json"
	"strings"
	"testing"
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
