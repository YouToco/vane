package tikhubcatalog

import (
	"encoding/json"
	"strings"
	"unicode/utf8"

	"github.com/YouToco/vane/toolsearch"
)

const modelDescriptionMaxRunes = 300

// modelToolEntry projects provider routing metadata into the provider-neutral
// definition exposed to the model. Callers must run agentEligible first.
func modelToolEntry(entry Entry) toolsearch.Entry {
	return toolsearch.Entry{
		Namespace:   "social/" + entry.Platform,
		Name:        entry.Name,
		Description: modelToolDescription(entry),
		Parameters:  modelParametersSchema(entry),
		Tags:        []string{entry.Platform},
	}
}

func modelToolDescription(entry Entry) string {
	parts := make([]string, 0, 3)
	if summary := publicModelText(entry.Summary); summary != "" {
		parts = append(parts, summary)
	}
	if entry.Description != "" {
		if description := publicModelText(entry.Description); description != "" {
			parts = append(parts, truncateModelRunes(description, modelDescriptionMaxRunes))
		}
	}
	parts = append(parts, "（社媒公开数据查询，可能计费，请按需调用）")
	return strings.Join(parts, "\n")
}

func publicModelText(value string) string {
	lines := strings.Split(value, "\n")
	out := make([]string, 0, len(lines))
	for _, line := range lines {
		lower := strings.ToLower(strings.TrimSpace(line))
		if lower == "" {
			continue
		}
		if strings.Contains(lower, "[example]") ||
			strings.Contains(lower, "### example") ||
			strings.Contains(lower, "### 示例") ||
			strings.Contains(lower, "[示例]") {
			break
		}
		if containsProviderTransport(lower) ||
			strings.Contains(lower, "api priority") ||
			strings.Contains(lower, "接口优先级") ||
			strings.Contains(lower, "接口推荐") ||
			strings.Contains(lower, "本接口") ||
			strings.Contains(lower, "request method") ||
			strings.Contains(lower, "请求方法") ||
			strings.Contains(lower, "endpoint path") ||
			strings.Contains(lower, "接口路径") {
			continue
		}
		out = append(out, strings.TrimSpace(line))
	}
	return strings.Join(out, "\n")
}

func modelParametersSchema(entry Entry) json.RawMessage {
	properties := make(map[string]any, len(entry.Params))
	var required []string
	for _, parameter := range entry.Params {
		property := map[string]any{}
		switch {
		case parameter.Type == "integer" || parameter.Type == "number" || parameter.Type == "boolean" || parameter.Type == "object":
			property["type"] = parameter.Type
		case strings.HasPrefix(parameter.Type, "array:"):
			item := strings.TrimPrefix(parameter.Type, "array:")
			switch item {
			case "integer", "number", "boolean", "object":
			default:
				item = "string"
			}
			property["type"] = "array"
			property["items"] = map[string]any{"type": item}
		default:
			property["type"] = "string"
		}
		if parameter.Desc != "" {
			property["description"] = publicModelText(parameter.Desc)
		}
		if len(parameter.Enum) > 0 {
			publicEnum := make([]any, 0, len(parameter.Enum))
			for _, value := range parameter.Enum {
				if publicValue, ok := publicModelValue(value); ok {
					publicEnum = append(publicEnum, publicValue)
				}
			}
			if len(publicEnum) > 0 {
				property["enum"] = publicEnum
			}
		}
		if parameter.Default != nil {
			if publicDefault, ok := publicModelValue(parameter.Default); ok {
				property["default"] = publicDefault
			}
		}
		properties[parameter.Name] = property
		if parameter.Required {
			required = append(required, parameter.Name)
		}
	}
	schema := map[string]any{"type": "object", "properties": properties}
	if len(required) > 0 {
		schema["required"] = required
	}
	raw, err := json.Marshal(schema)
	if err != nil {
		panic("tikhubcatalog: encode model tool schema: " + err.Error())
	}
	return raw
}

func publicModelLiteral(value string) (string, bool) {
	lower := strings.ToLower(strings.TrimSpace(value))
	if containsProviderTransport(lower) {
		return "", false
	}
	return value, true
}

func publicModelValue(value any) (any, bool) {
	switch typed := value.(type) {
	case string:
		return publicModelLiteral(typed)
	case []any:
		out := make([]any, 0, len(typed))
		for _, child := range typed {
			if publicChild, ok := publicModelValue(child); ok {
				out = append(out, publicChild)
			}
		}
		return out, true
	case map[string]any:
		out := make(map[string]any, len(typed))
		for key, child := range typed {
			if _, ok := publicModelLiteral(key); !ok {
				continue
			}
			if publicChild, ok := publicModelValue(child); ok {
				out[key] = publicChild
			}
		}
		return out, true
	case []string:
		out := make([]any, 0, len(typed))
		for _, child := range typed {
			if publicChild, ok := publicModelLiteral(child); ok {
				out = append(out, publicChild)
			}
		}
		return out, true
	case map[string]string:
		out := make(map[string]any, len(typed))
		for key, child := range typed {
			if _, ok := publicModelLiteral(key); !ok {
				continue
			}
			if publicChild, ok := publicModelLiteral(child); ok {
				out[key] = publicChild
			}
		}
		return out, true
	default:
		return value, true
	}
}

func containsProviderTransport(value string) bool {
	normalized := strings.NewReplacer("_", " ", "-", " ").Replace(strings.ToLower(value))
	for _, marker := range []string{
		"tikhub", "cookie", "/api/",
		"request method", "endpoint path", "请求方法", "接口路径",
	} {
		if strings.Contains(normalized, marker) {
			return true
		}
	}
	return containsEndpointSlug(value)
}

func containsEndpointSlug(value string) bool {
	value = strings.ToLower(value)
	for start := 0; start < len(value); start++ {
		if value[start] != '/' || start+1 >= len(value) {
			continue
		}
		if start > 0 {
			previous := value[start-1]
			if previous == '_' || previous >= 'a' && previous <= 'z' || previous >= '0' && previous <= '9' {
				continue
			}
		}
		hasUnderscore := false
		end := start + 1
		for end < len(value) {
			character := value[end]
			if character == '_' {
				hasUnderscore = true
				end++
				continue
			}
			if character >= 'a' && character <= 'z' || character >= '0' && character <= '9' {
				end++
				continue
			}
			break
		}
		if hasUnderscore && end > start+1 {
			return true
		}
	}
	return false
}

func truncateModelRunes(value string, max int) string {
	if utf8.RuneCountInString(value) <= max {
		return value
	}
	return string([]rune(value)[:max]) + "…"
}
