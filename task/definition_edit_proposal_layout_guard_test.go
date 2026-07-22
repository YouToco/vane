package task

import (
	"reflect"
	"testing"

	"github.com/YouToco/vane/scheduler"
)

type definitionEditProposalActorLayoutV1 struct {
	TenantID int64 `json:"tenant_id"`
	UserID   int64 `json:"user_id"`
}

type definitionEditProposalTargetLayoutV1 struct {
	TenantID int64  `json:"tenant_id"`
	UserID   int64  `json:"user_id"`
	TaskID   string `json:"task_id"`
}

type definitionEditProposalLayoutV1 struct {
	WireVersion            string                             `json:"wire_version"`
	OperationID            string                             `json:"operation_id"`
	ApprovalRef            string                             `json:"approval_ref"`
	Actor                  TaskDefinitionEditProposalActorV1  `json:"actor"`
	Target                 TaskDefinitionEditProposalTargetV1 `json:"target"`
	SessionID              int64                              `json:"session_id"`
	ExpiresAtUnixMicros    int64                              `json:"expires_at_unix_micros"`
	OriginalStatus         TaskDefinitionEditOriginalStatusV1 `json:"original_status"`
	BaseHead               scheduler.TaskDefinitionEditHead   `json:"base_head"`
	TargetHead             scheduler.TaskDefinitionEditHead   `json:"target_head"`
	TargetDefinitionDigest string                             `json:"target_definition_digest"`
	PreparedEditDigest     string                             `json:"prepared_edit_digest"`
	BaseSnapshotDigest     string                             `json:"base_snapshot_digest"`
}

func TestDefinitionEditProposalV1LayoutsAreFrozen(t *testing.T) {
	t.Parallel()
	assertDefinitionEditProposalLayoutV1[
		TaskDefinitionEditProposalActorV1, definitionEditProposalActorLayoutV1,
	](t, "TaskDefinitionEditProposalActorV1")
	assertDefinitionEditProposalLayoutV1[
		TaskDefinitionEditProposalTargetV1, definitionEditProposalTargetLayoutV1,
	](t, "TaskDefinitionEditProposalTargetV1")
	assertDefinitionEditProposalLayoutV1[
		TaskDefinitionEditProposalV1, definitionEditProposalLayoutV1,
	](t, "TaskDefinitionEditProposalV1")
}

func assertDefinitionEditProposalLayoutV1[Actual, Frozen any](t *testing.T, name string) {
	t.Helper()
	actual := reflect.TypeFor[Actual]()
	frozen := reflect.TypeFor[Frozen]()
	if actual.Kind() != reflect.Struct || frozen.Kind() != reflect.Struct ||
		actual.NumField() != frozen.NumField() {
		t.Fatalf(
			"%s changed from proposal/v1; add and retain a new wire instead of updating this guard",
			name,
		)
	}
	for index := range actual.NumField() {
		got := actual.Field(index)
		want := frozen.Field(index)
		if got.Name != want.Name || got.Type != want.Type ||
			got.Tag.Get("json") != want.Tag.Get("json") || got.Anonymous != want.Anonymous {
			t.Fatalf(
				"%s field %d changed from proposal/v1: got %s %s %q, want %s %s %q; add and retain a new wire",
				name, index, got.Name, got.Type, got.Tag.Get("json"),
				want.Name, want.Type, want.Tag.Get("json"),
			)
		}
	}
}
