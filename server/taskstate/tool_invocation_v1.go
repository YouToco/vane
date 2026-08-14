package taskstate

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/YouToco/vane/server/internal/strictjson"
)

const (
	// ToolInvocationSchemaVersionV1 identifies the bytes covered by an
	// invocation digest. It is deliberately independent from task-definition
	// and run-snapshot versions so a tool contract can evolve explicitly.
	ToolInvocationSchemaVersionV1 = "vane.tool-invocation/v1"
)

// ToolInvocationV1 is one approved acquisition Tool call. Arguments are the
// canonical user-level Tool arguments, not materialized URLs, provider
// endpoints, credentials, or Source IDs.
type ToolInvocationV1 struct {
	ToolName            string          `json:"tool_name"`
	ToolContractVersion string          `json:"tool_contract_version"`
	Arguments           json.RawMessage `json:"arguments"`
	Digest              string          `json:"invocation_digest"`
}

type toolInvocationDigestEnvelopeV1 struct {
	SchemaVersion       string          `json:"schema_version"`
	ToolName            string          `json:"tool_name"`
	ToolContractVersion string          `json:"tool_contract_version"`
	Arguments           json.RawMessage `json:"arguments"`
}

// BuildToolInvocationV1 canonicalizes arguments and seals the exact logical
// call. ToolContractVersion is the logical argument/result contract selected
// by the compiler, not a provider route or worker build revision.
func BuildToolInvocationV1(
	toolName, toolVersion string,
	arguments json.RawMessage,
) (ToolInvocationV1, error) {
	invocation := ToolInvocationV1{
		ToolName: toolName, ToolContractVersion: toolVersion, Arguments: arguments,
	}
	return normalizeToolInvocationV1(invocation)
}

// Validate verifies that the stored digest still matches the canonical call.
func (i ToolInvocationV1) Validate() error {
	_, err := normalizeToolInvocationV1(i)
	return err
}

func normalizeToolInvocationV1(
	invocation ToolInvocationV1,
) (ToolInvocationV1, error) {
	if !validIdentifier(invocation.ToolName, maxToolNameBytes) ||
		!validIdentifier(invocation.ToolContractVersion, maxToolContractVersionBytes) {
		return ToolInvocationV1{}, invalidState("tool invocation identity is invalid")
	}
	arguments, err := canonicalJSONObject(invocation.Arguments, "tool invocation arguments")
	if err != nil {
		return ToolInvocationV1{}, err
	}
	invocation.Arguments = arguments
	digest, err := digestToolInvocationV1(
		invocation.ToolName, invocation.ToolContractVersion, invocation.Arguments)
	if err != nil {
		return ToolInvocationV1{}, err
	}
	if invocation.Digest != "" && invocation.Digest != digest {
		return ToolInvocationV1{}, invalidState("tool invocation digest differs from canonical call")
	}
	invocation.Digest = digest
	return invocation, nil
}

func digestToolInvocationV1(
	toolName, toolVersion string,
	arguments json.RawMessage,
) (string, error) {
	payload, err := marshalBounded(toolInvocationDigestEnvelopeV1{
		SchemaVersion:       ToolInvocationSchemaVersionV1,
		ToolName:            toolName,
		ToolContractVersion: toolVersion,
		Arguments:           arguments,
	}, maxJSONObjectBytes, "tool invocation digest envelope")
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}

// DecodeToolInvocationV1 strictly decodes and verifies one durable call.
func DecodeToolInvocationV1(payload []byte) (ToolInvocationV1, error) {
	var invocation ToolInvocationV1
	if len(payload) == 0 || len(payload) > maxJSONObjectBytes ||
		strictjson.DecodeExact(payload, &invocation) != nil {
		return ToolInvocationV1{}, invalidState("tool invocation json is invalid")
	}
	return normalizeToolInvocationV1(invocation)
}

// EncodeToolInvocationV1 returns canonical bytes after digest verification.
func EncodeToolInvocationV1(invocation ToolInvocationV1) ([]byte, error) {
	normalized, err := normalizeToolInvocationV1(invocation)
	if err != nil {
		return nil, err
	}
	return json.Marshal(normalized)
}
