package main

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestMigrateAndProvisionOrdersClusterIdentityAfterSchema(t *testing.T) {
	var calls []string
	step := func(name string, result error) func(context.Context, string) error {
		return func(_ context.Context, databaseURL string) error {
			if databaseURL != "postgres://owner/target" {
				t.Fatalf("unexpected database URL %q", databaseURL)
			}
			calls = append(calls, name)
			return result
		}
	}

	if err := migrateAndProvision(t.Context(), "postgres://owner/target",
		step("schema", nil), step("provision", nil)); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(calls, []string{"schema", "provision"}) {
		t.Fatalf("steps=%v", calls)
	}

	calls = nil
	schemaErr := errors.New("schema failed")
	err := migrateAndProvision(t.Context(), "postgres://owner/target",
		step("schema", schemaErr), step("provision", nil))
	if !errors.Is(err, schemaErr) || !reflect.DeepEqual(calls, []string{"schema"}) {
		t.Fatalf("schema failure err=%v calls=%v", err, calls)
	}

	calls = nil
	provisionErr := errors.New("provision failed")
	err = migrateAndProvision(t.Context(), "postgres://owner/target",
		step("schema", nil), step("provision", provisionErr))
	if !errors.Is(err, provisionErr) ||
		!strings.Contains(err.Error(), "server runtime provision failed") ||
		!reflect.DeepEqual(calls, []string{"schema", "provision"}) {
		t.Fatalf("provision failure err=%v calls=%v", err, calls)
	}
}
