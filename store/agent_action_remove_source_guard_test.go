package store

import (
	"os"
	"strings"
	"testing"
)

func TestRemoveSourceDurableEffectKeepsExplicitScopePredicate(
	t *testing.T,
) {
	raw, err := os.ReadFile("agent_action_projection.go")
	if err != nil {
		t.Fatal(err)
	}
	const scopedDelete = `DELETE FROM subscriptions
			  WHERE tenant_id=$1 AND user_id=$2
			    AND source_id=ANY($3::bigint[])`
	if !strings.Contains(string(raw), scopedDelete) {
		t.Fatal(
			"remove_source durable DELETE lost its explicit tenant/user scope",
		)
	}
}
