package types

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestMCPConnectionVersionNeverSerializesCredentialReference(t *testing.T) {
	payload, err := json.Marshal(MCPConnectionVersion{
		EndpointURL:   "https://mcp.example.com/v1",
		CredentialRef: "vault:credential-123",
	})
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(payload, []byte("credential-123")) ||
		bytes.Contains(payload, []byte("credential_ref")) {
		t.Fatalf("opaque credential reference leaked: %s", payload)
	}
}
