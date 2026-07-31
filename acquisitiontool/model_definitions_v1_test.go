package acquisitiontool

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestModelToolDefinitionsV1OwnRuntimeContractsAndSchema(t *testing.T) {
	definitions := ModelToolDefinitionsV1()
	if len(definitions) == 0 {
		t.Fatal("model Tool definitions must not be empty")
	}
	seen := make(map[string]struct{}, len(definitions))
	for index, definition := range definitions {
		name := definition.Contract.Name
		if name == "" || definition.Description == "" ||
			!json.Valid(definition.ArgumentsSchema) ||
			modelToolDefinitionsV1[index].decoder == nil {
			t.Fatalf("invalid model Tool definition: %+v", definition)
		}
		var argumentSchema struct {
			Properties map[string]any `json:"properties"`
		}
		if err := json.Unmarshal(
			definition.ArgumentsSchema, &argumentSchema,
		); err != nil {
			t.Fatal(err)
		}
		for _, locator := range definition.ExternalLocators {
			if _, exists := argumentSchema.Properties[locator.Argument]; !exists {
				t.Fatalf("%s locator %q is absent from arguments schema",
					name, locator.Argument)
			}
			switch locator.Kind {
			case ExternalLocatorLiteralV1,
				ExternalLocatorDomainsV1,
				ExternalLocatorXHandleV1:
			default:
				t.Fatalf("%s locator %q has unknown kind %q",
					name, locator.Argument, locator.Kind)
			}
		}
		if _, duplicate := seen[name]; duplicate {
			t.Fatalf("duplicate model Tool definition %q", name)
		}
		seen[name] = struct{}{}
		contract, ok := LookupToolContractV1(name)
		if !ok || contract != definition.Contract {
			t.Fatalf("model/runtime contract drift for %q: %+v %+v",
				name, definition.Contract, contract)
		}
	}

	raw, err := ToolCallSchemaV1()
	if err != nil || !json.Valid(raw) {
		t.Fatalf("ToolCallSchemaV1() = %s, %v", raw, err)
	}
	var schema struct {
		OneOf []json.RawMessage `json:"oneOf"`
	}
	if err := json.Unmarshal(raw, &schema); err != nil ||
		len(schema.OneOf) != len(definitions) {
		t.Fatalf("Tool call union drifted: %s err=%v", raw, err)
	}
}

func TestModelToolDefinitionsV1ReturnsDefensiveCopies(t *testing.T) {
	first := ModelToolDefinitionsV1()
	before := bytes.Clone(first[0].ArgumentsSchema)
	first[0].ArgumentsSchema[0] = '['
	first[0].ExternalLocators[0].Argument = "mutated"
	second := ModelToolDefinitionsV1()
	if !bytes.Equal(second[0].ArgumentsSchema, before) {
		t.Fatal("caller mutation changed registered Tool schema")
	}
	if second[0].ExternalLocators[0].Argument != "include_domains" {
		t.Fatal("caller mutation changed registered locator policy")
	}
}

func TestModelToolDefinitionsV1ExamplesPassStrictRuntimeDecoder(t *testing.T) {
	examples := map[string]json.RawMessage{
		"web_search":           json.RawMessage(`{"query":"AI 官方更新"}`),
		"web_feed":             json.RawMessage(`{"feed_url":"https://example.com/feed.xml"}`),
		"web_contents":         json.RawMessage(`{"page_url":"https://example.com/changelog"}`),
		"web_product_status":   json.RawMessage(`{"page_url":"https://www.kimi.com/membership/pricing"}`),
		"x_user_posts":         json.RawMessage(`{"screen_name":"OpenAI"}`),
		"xhs_search":           json.RawMessage(`{"keyword":"AI 产品"}`),
		"xhs_user_posts":       json.RawMessage(`{"user_id":"6a5578b3000000000e03cc00"}`),
		"xhs_hot_list":         json.RawMessage(`{}`),
		"xhs_topic_feed":       json.RawMessage(`{"page_id":"6301c499df9bea0001dc6f47"}`),
		"xhs_faved_notes":      json.RawMessage(`{"user_id":"6a5578b3000000000e03cc00"}`),
		"weibo_user_posts":     json.RawMessage(`{"uid":"2803301701"}`),
		"weibo_hot_list":       json.RawMessage(`{}`),
		"wechat_mp_user_posts": json.RawMessage(`{"username":"gh_363b924965e9"}`),
	}
	definitions := ModelToolDefinitionsV1()
	if len(examples) != len(definitions) {
		t.Fatalf("examples=%d definitions=%d", len(examples), len(definitions))
	}
	for _, definition := range definitions {
		raw, ok := examples[definition.Contract.Name]
		if !ok {
			t.Fatalf("missing strict decoder example for %q",
				definition.Contract.Name)
		}
		if _, err := CanonicalizeToolArgumentsV1(
			definition.Contract.Name, raw,
		); err != nil {
			t.Fatalf("%s example rejected by runtime decoder: %v",
				definition.Contract.Name, err)
		}
	}
}
