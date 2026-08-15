package store

import (
	"crypto/sha256"
	"fmt"
	"strings"
	"testing"

	"github.com/YouToco/vane/server/types"
)

func TestInsertLLMCallRejectsInvalidPolicyManifestBeforeDatabase(t *testing.T) {
	payload := `{"schema_version":"vane.interactive-agent-policy-manifest/v1"}`
	sum := sha256.Sum256([]byte(payload))
	validDigest := fmt.Sprintf("%x", sum[:])
	invalid := []*types.LLMCall{
		{PolicyManifestPayload: payload},
		{PolicyManifestDigest: validDigest},
		{PolicyManifestPayload: payload, PolicyManifestDigest: strings.Repeat("0", 64)},
		{PolicyManifestPayload: strings.Repeat("x", (16<<10)+1), PolicyManifestDigest: validDigest},
	}
	for i, call := range invalid {
		if _, err := (&Store{}).InsertLLMCall(t.Context(), call); err == nil {
			t.Fatalf("invalid manifest case %d reached database", i)
		}
	}
}
