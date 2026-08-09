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
		Tags:        []string{entry.Platform, entry.Tag},
	}
}

func modelToolDescription(entry Entry) string {
	description := publicModelText(entry.Summary)
	if entry.Description != "" {
		description += "\n" + truncateModelRunes(
			publicModelText(entry.Description),
			modelDescriptionMaxRunes,
		)
	}
	return description + "\n（社媒公开数据查询，可能计费，请按需调用）"
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
		if strings.Contains(lower, "tikhub") ||
			strings.Contains(lower, "cookie") ||
			strings.Contains(lower, "/api/v") ||
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
				if text, isString := value.(string); isString {
					if text, ok := publicModelLiteral(text); ok {
						publicEnum = append(publicEnum, text)
					}
				} else {
					publicEnum = append(publicEnum, value)
				}
			}
			if len(publicEnum) > 0 {
				property["enum"] = publicEnum
			}
		}
		if parameter.Default != nil {
			if value, isString := parameter.Default.(string); isString {
				if value, ok := publicModelLiteral(value); ok {
					property["default"] = value
				}
			} else {
				property["default"] = parameter.Default
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
	if strings.Contains(lower, "tikhub") ||
		strings.Contains(lower, "/api/v") ||
		strings.Contains(lower, "request method") ||
		strings.Contains(lower, "endpoint path") {
		return "", false
	}
	return value, true
}

func truncateModelRunes(value string, max int) string {
	if utf8.RuneCountInString(value) <= max {
		return value
	}
	return string([]rune(value)[:max]) + "…"
}
