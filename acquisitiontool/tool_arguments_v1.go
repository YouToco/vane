package acquisitiontool

import (
	"encoding/json"
	"errors"
	"strings"

	"github.com/YouToco/vane/internal/strictjson"
)

// CanonicalizeToolArgumentsV1 is the exact current-write boundary for
// acquisition Tool arguments. The same decoder bound to the model-visible Tool
// definition is also used by MaterializeApprovedToolCallV1.
func CanonicalizeToolArgumentsV1(
	toolName string,
	raw json.RawMessage,
) (json.RawMessage, error) {
	var fields map[string]json.RawMessage
	if strictjson.DecodeExact(raw, &fields) != nil || fields == nil {
		return nil, errors.New(
			"Tool arguments must be a non-null JSON object",
		)
	}
	requirement, err := decodeApprovedToolArgumentsV1(toolName, raw)
	if err != nil {
		return nil, err
	}
	target, message := BuildTarget(requirement)
	if message != "" || target == nil {
		if message == "" {
			message = "Tool arguments cannot be materialized"
		}
		return nil, errors.New(message)
	}
	canonical, err := json.Marshal(fields)
	if err != nil {
		return nil, errors.New("Tool arguments cannot be encoded")
	}
	return canonical, nil
}

func exactlyOne(left, right string) bool {
	return (strings.TrimSpace(left) == "") !=
		(strings.TrimSpace(right) == "")
}
