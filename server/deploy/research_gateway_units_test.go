package deploy

import (
	"os"
	"strings"
	"testing"
)

func TestResearchGatewaySocketPermissions(t *testing.T) {
	unit, err := os.ReadFile("vane-research-gateway.socket")
	if err != nil {
		t.Fatal(err)
	}
	text := string(unit)

	for _, required := range []string{
		"SocketUser=vane-research-gateway",
		"SocketGroup=vane",
		"SocketMode=0660",
		"DirectoryMode=0711",
	} {
		if !strings.Contains(text, required) {
			t.Fatalf("gateway socket unit must contain %q", required)
		}
	}
	if strings.Contains(text, "SocketMode=0666") {
		t.Fatal("gateway socket must not be world-accessible")
	}
}

func TestNativeV3EditRecoveryUsesIndependentSystemdCredential(t *testing.T) {
	unit, err := os.ReadFile("vane.service")
	if err != nil {
		t.Fatal(err)
	}
	const load = "LoadCredential=native_v3_edit_recovery_db_url:/etc/vane/credentials/native_v3_edit_recovery_db_url"
	if !strings.Contains(string(unit), load) {
		t.Fatalf("server unit must contain %q", load)
	}
	environment, err := os.ReadFile("server.env.example")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(environment),
		"VANE_DB_NATIVE_V3_EDIT_RECOVERY_RUNTIME_URL=") {
		t.Fatal("edit recovery credential must not be stored in server.env")
	}
}
