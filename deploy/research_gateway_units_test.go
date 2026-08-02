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
