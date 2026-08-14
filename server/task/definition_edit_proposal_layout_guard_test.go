package task

import (
	"reflect"
	"testing"

	"github.com/YouToco/vane/server/scheduler"
)

type definitionEditProposalActorLayoutV2 struct {
	TenantID int64 `json:"tenant_id"`
	UserID   int64 `json:"user_id"`
}

type definitionEditProposalTargetLayoutV2 struct {
	TenantID int64  `json:"tenant_id"`
	UserID   int64  `json:"user_id"`
	TaskID   string `json:"task_id"`
}

type definitionEditProposalLayoutV2 struct {
	WireVersion            string                             `json:"wire_version"`
	OperationID            string                             `json:"operation_id"`
	OperationRef           string                             `json:"operation_ref"`
	Actor                  TaskDefinitionEditProposalActorV2  `json:"actor"`
	Target                 TaskDefinitionEditProposalTargetV2 `json:"target"`
	SessionID              int64                              `json:"session_id"`
	ExpiresAtUnixMicros    int64                              `json:"expires_at_unix_micros"`
	OriginalStatus         TaskDefinitionEditOriginalStatusV2 `json:"original_status"`
	BaseHead               scheduler.TaskDefinitionEditHead   `json:"base_head"`
	TargetHead             scheduler.TaskDefinitionEditHead   `json:"target_head"`
	TargetDefinitionDigest string                             `json:"target_definition_digest"`
	PreparedEditDigest     string                             `json:"prepared_edit_digest"`
	BaseSnapshotDigest     string                             `json:"base_snapshot_digest"`
}

func TestDefinitionEditProposalV2LayoutsAreFrozen(t *testing.T) {
	t.Parallel()
	assertDefinitionEditProposalLayoutV2[
		TaskDefinitionEditProposalActorV2, definitionEditProposalActorLayoutV2,
	](t, "TaskDefinitionEditProposalActorV2")
	assertDefinitionEditProposalLayoutV2[
		TaskDefinitionEditProposalTargetV2, definitionEditProposalTargetLayoutV2,
	](t, "TaskDefinitionEditProposalTargetV2")
	assertDefinitionEditProposalLayoutV2[
		TaskDefinitionEditProposalV2, definitionEditProposalLayoutV2,
	](t, "TaskDefinitionEditProposalV2")
}

func assertDefinitionEditProposalLayoutV2[Actual, Frozen any](t *testing.T, name string) {
	t.Helper()
	actual := reflect.TypeFor[Actual]()
	frozen := reflect.TypeFor[Frozen]()
	if actual.Kind() != reflect.Struct || frozen.Kind() != reflect.Struct ||
		actual.NumField() != frozen.NumField() {
		t.Fatalf(
			"%s changed from proposal/v2; add and retain a new wire instead of updating this guard",
			name,
		)
	}
	for index := range actual.NumField() {
		got := actual.Field(index)
		want := frozen.Field(index)
		if got.Name != want.Name || got.Type != want.Type ||
			got.Tag.Get("json") != want.Tag.Get("json") || got.Anonymous != want.Anonymous {
			t.Fatalf(
				"%s field %d changed from proposal/v2: got %s %s %q, want %s %s %q; add and retain a new wire",
				name, index, got.Name, got.Type, got.Tag.Get("json"),
				want.Name, want.Type, want.Tag.Get("json"),
			)
		}
	}
}
