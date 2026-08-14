package acquisitiontool

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/YouToco/vane/runtimepolicy"
	"github.com/YouToco/vane/types"
)

// ModelToolDefinitionV1 is the one model-visible definition for a scheduled
// acquisition Tool. Its name also owns the runtime contract and strict
// argument decoder in this package; Agent must render these definitions rather
// than restating them.
type ModelToolDefinitionV1 struct {
	Contract         ToolContractV1
	Description      string
	ArgumentsSchema  json.RawMessage
	ExternalLocators []ExternalLocatorV1
	decoder          func(json.RawMessage) (Requirement, error)
}

type ExternalLocatorKindV1 string

const (
	ExternalLocatorLiteralV1 ExternalLocatorKindV1 = "literal"
	ExternalLocatorDomainsV1 ExternalLocatorKindV1 = "domains"
	ExternalLocatorXHandleV1 ExternalLocatorKindV1 = "x_handle"
)

type ExternalLocatorV1 struct {
	Argument string
	Kind     ExternalLocatorKindV1
}

var modelToolDefinitionsV1 = []ModelToolDefinitionV1{
	{
		Contract: ToolContractV1{
			Name: "web_search", Platform: types.PlatformWeb,
			Capability: types.CapSearch, Kind: types.KindArticle,
			ImplementationVersion: runtimepolicy.CapabilityImplementationExaV1,
		},
		Description: "公开网页实时搜索（public web search）：official announcement、model/API/Agent update、交叉核验。query 按手册整理；include_domains 仅限认证请求/可信手册冻结的用户裸域名。",
		decoder:     decodeWebSearchArgumentsV1,
		ExternalLocators: []ExternalLocatorV1{
			{Argument: "include_domains", Kind: ExternalLocatorDomainsV1},
		},
		ArgumentsSchema: json.RawMessage(`{
			"type":"object",
			"properties":{
				"query":{"type":"string","minLength":1,"description":"实时搜索词，可按用户目标语义整理"},
				"category":{"type":"string","description":"可选公开网页类别，如 news"},
				"include_domains":{"type":"array","uniqueItems":true,"items":{"type":"string"},"description":"可选裸域名白名单；只能逐字使用已冻结在认证当前请求或可信任务手册中的用户明确域名"}
			},
			"required":["query"],
			"additionalProperties":false
		}`),
	},
	{
		Contract: ToolContractV1{
			Name: "web_feed", Platform: types.PlatformWeb,
			Capability: types.CapFeed, Kind: types.KindArticle,
			ImplementationVersion: runtimepolicy.CapabilityImplementationRSSV1,
		},
		Description: "每次运行读取一个已知 RSS/Atom 地址。feed_url 必须由用户当前消息明确提供，不能根据机构名猜测。",
		decoder:     decodeWebFeedArgumentsV1,
		ExternalLocators: []ExternalLocatorV1{
			{Argument: "feed_url", Kind: ExternalLocatorLiteralV1},
		},
		ArgumentsSchema: json.RawMessage(`{
			"type":"object",
			"properties":{
				"feed_url":{"type":"string","description":"用户明确给出的 RSS/Atom http/https 地址"},
				"categories":{"type":"array","items":{"type":"string"},"description":"可选 RSS 分类过滤"}
			},
			"required":["feed_url"],
			"additionalProperties":false
		}`),
	},
	{
		Contract: ToolContractV1{
			Name: "web_contents", Platform: types.PlatformWeb,
			Capability: types.CapContents, Kind: types.KindPageContent,
			ImplementationVersion: runtimepolicy.CapabilityImplementationExaV1,
		},
		Description: "每次运行读取一个指定网页并检测内容变化。page_url 必须由用户明确提供并冻结在认证当前请求或可信任务手册中。",
		decoder:     decodeWebContentsArgumentsV1,
		ExternalLocators: []ExternalLocatorV1{
			{Argument: "page_url", Kind: ExternalLocatorLiteralV1},
		},
		ArgumentsSchema: json.RawMessage(`{
			"type":"object",
			"properties":{"page_url":{"type":"string","description":"用户明确提供并冻结在认证当前请求或可信任务手册中的普通 http/https 页面地址"}},
			"required":["page_url"],
			"additionalProperties":false
		}`),
	},
	{
		Contract: ToolContractV1{
			Name: "web_product_status", Platform: types.PlatformWeb,
			Capability: types.CapProductStatus, Kind: types.KindPageContent,
			ImplementationVersion: runtimepolicy.CapabilityImplementationProductStatusV1,
		},
		Description: "每次运行从受支持的官方套餐页及其第一方公开接口读取结构化购买状态；适合动态渲染、普通网页抓取拿不到按钮的套餐页。当前支持 Kimi 会员定价页。page_url 必须由用户当前消息明确提供。",
		decoder:     decodeWebProductStatusArgumentsV1,
		ExternalLocators: []ExternalLocatorV1{
			{Argument: "page_url", Kind: ExternalLocatorLiteralV1},
		},
		ArgumentsSchema: json.RawMessage(`{
			"type":"object",
			"properties":{"page_url":{"type":"string","enum":["https://www.kimi.com/membership/pricing"],"description":"Kimi 官方会员定价页；运行时只调用代码白名单内的第一方商品接口"}},
			"required":["page_url"],
			"additionalProperties":false
		}`),
	},
	{
		Contract: ToolContractV1{
			Name: "x_user_posts", Platform: types.PlatformX,
			Capability: types.CapUserPosts, Kind: types.KindArticle,
			ImplementationVersion: runtimepolicy.CapabilityImplementationBindingV1,
		},
		Description: "每次运行获取指定 X 账号的新帖子。screen_name 必须来自用户明确给出的 @handle 或账号主页。",
		decoder:     decodeXUserPostsArgumentsV1,
		ExternalLocators: []ExternalLocatorV1{
			{Argument: "screen_name", Kind: ExternalLocatorXHandleV1},
		},
		ArgumentsSchema: json.RawMessage(`{
			"type":"object",
			"properties":{"screen_name":{"type":"string","description":"用户明确给出的 X @handle，不含 @"}},
			"required":["screen_name"],
			"additionalProperties":false
		}`),
	},
	{
		Contract: ToolContractV1{
			Name: "xhs_search", Platform: types.PlatformXHS,
			Capability: types.CapSearch, Kind: types.KindArticle,
			ImplementationVersion: runtimepolicy.CapabilityImplementationBindingV1,
		},
		Description: "每次运行按关键词搜索小红书公开内容。keyword 可由模型按任务手册整理。",
		decoder:     decodeXHSSearchArgumentsV1,
		ArgumentsSchema: json.RawMessage(`{
			"type":"object",
			"properties":{"keyword":{"type":"string","minLength":1,"description":"小红书语义搜索词"}},
			"required":["keyword"],
			"additionalProperties":false
		}`),
	},
	{
		Contract: ToolContractV1{
			Name: "xhs_user_posts", Platform: types.PlatformXHS,
			Capability: types.CapUserPosts, Kind: types.KindArticle,
			ImplementationVersion: runtimepolicy.CapabilityImplementationBindingV1,
		},
		Description:     "每次运行获取指定小红书用户的新笔记。用户必须明确提供 24 位 user_id 或主页 profile_url。",
		ArgumentsSchema: xhsUserLocatorSchemaV1,
		decoder:         decodeXHSUserPostsArgumentsV1,
		ExternalLocators: []ExternalLocatorV1{
			{Argument: "user_id", Kind: ExternalLocatorLiteralV1},
			{Argument: "profile_url", Kind: ExternalLocatorLiteralV1},
		},
	},
	{
		Contract: ToolContractV1{
			Name: "xhs_hot_list", Platform: types.PlatformXHS,
			Capability: types.CapHotList, Kind: types.KindArticle,
			ImplementationVersion: runtimepolicy.CapabilityImplementationBindingV1,
		},
		Description:     "每次运行读取小红书公开热榜，不需要账号或定位参数。",
		ArgumentsSchema: json.RawMessage(`{"type":"object","properties":{},"additionalProperties":false}`),
		decoder:         decodeXHSHotListArgumentsV1,
	},
	{
		Contract: ToolContractV1{
			Name: "xhs_topic_feed", Platform: types.PlatformXHS,
			Capability: types.CapTopicFeed, Kind: types.KindArticle,
			ImplementationVersion: runtimepolicy.CapabilityImplementationBindingV1,
		},
		Description: "每次运行获取指定小红书话题的新内容。用户必须明确提供 24 位 page_id 或话题 topic_url。",
		decoder:     decodeXHSTopicFeedArgumentsV1,
		ExternalLocators: []ExternalLocatorV1{
			{Argument: "page_id", Kind: ExternalLocatorLiteralV1},
			{Argument: "topic_url", Kind: ExternalLocatorLiteralV1},
		},
		ArgumentsSchema: json.RawMessage(`{
			"type":"object",
			"properties":{
				"page_id":{"type":"string","pattern":"^[0-9a-f]{24}$","description":"用户明确给出的 24 位小写十六进制话题 ID"},
				"topic_url":{"type":"string","description":"用户明确给出的小红书话题链接"}
			},
			"oneOf":[{"required":["page_id"]},{"required":["topic_url"]}],
			"additionalProperties":false
		}`),
	},
	{
		Contract: ToolContractV1{
			Name: "xhs_faved_notes", Platform: types.PlatformXHS,
			Capability: types.CapFavedNotes, Kind: types.KindArticle,
			ImplementationVersion: runtimepolicy.CapabilityImplementationBindingV1,
		},
		Description:     "每次运行获取指定小红书用户公开可见的收藏。用户必须明确提供 24 位 user_id 或主页 profile_url。",
		ArgumentsSchema: xhsUserLocatorSchemaV1,
		decoder:         decodeXHSFavedNotesArgumentsV1,
		ExternalLocators: []ExternalLocatorV1{
			{Argument: "user_id", Kind: ExternalLocatorLiteralV1},
			{Argument: "profile_url", Kind: ExternalLocatorLiteralV1},
		},
	},
	{
		Contract: ToolContractV1{
			Name: "weibo_user_posts", Platform: types.PlatformWeibo,
			Capability: types.CapUserPosts, Kind: types.KindArticle,
			ImplementationVersion: runtimepolicy.CapabilityImplementationBindingV1,
		},
		Description: "每次运行获取指定微博账号的新帖子。用户必须明确提供数字 uid 或主页 profile_url。",
		decoder:     decodeWeiboUserPostsArgumentsV1,
		ExternalLocators: []ExternalLocatorV1{
			{Argument: "uid", Kind: ExternalLocatorLiteralV1},
			{Argument: "profile_url", Kind: ExternalLocatorLiteralV1},
		},
		ArgumentsSchema: json.RawMessage(`{
			"type":"object",
			"properties":{
				"uid":{"type":"string","pattern":"^[0-9]+$","description":"用户明确给出的微博数字用户 ID"},
				"profile_url":{"type":"string","description":"用户明确给出的微博用户主页"}
			},
			"oneOf":[{"required":["uid"]},{"required":["profile_url"]}],
			"additionalProperties":false
		}`),
	},
	{
		Contract: ToolContractV1{
			Name: "weibo_hot_list", Platform: types.PlatformWeibo,
			Capability: types.CapHotList, Kind: types.KindArticle,
			ImplementationVersion: runtimepolicy.CapabilityImplementationBindingV1,
		},
		Description:     "每次运行读取微博公开热搜榜，不需要账号或定位参数。",
		ArgumentsSchema: json.RawMessage(`{"type":"object","properties":{},"additionalProperties":false}`),
		decoder:         decodeWeiboHotListArgumentsV1,
	},
	{
		Contract: ToolContractV1{
			Name: "wechat_mp_user_posts", Platform: types.PlatformWechatMP,
			Capability: types.CapUserPosts, Kind: types.KindArticle,
			ImplementationVersion: runtimepolicy.CapabilityImplementationBindingV1,
		},
		Description: "每次运行获取指定公众号的新文章。username 必须是用户明确提供的 gh_ 原始 ID。",
		decoder:     decodeWechatMPUserPostsArgumentsV1,
		ExternalLocators: []ExternalLocatorV1{
			{Argument: "username", Kind: ExternalLocatorLiteralV1},
		},
		ArgumentsSchema: json.RawMessage(`{
			"type":"object",
			"properties":{"username":{"type":"string","pattern":"^gh_.+$","description":"用户明确给出的公众号 gh_ 原始 ID"}},
			"required":["username"],
			"additionalProperties":false
		}`),
	},
}

var xhsUserLocatorSchemaV1 = json.RawMessage(`{
	"type":"object",
	"properties":{
		"user_id":{"type":"string","pattern":"^[0-9a-f]{24}$","description":"用户明确给出的 24 位小写十六进制用户 ID"},
		"profile_url":{"type":"string","description":"用户明确给出的小红书用户主页"}
	},
	"oneOf":[{"required":["user_id"]},{"required":["profile_url"]}],
	"additionalProperties":false
}`)

func buildToolContractsV1() map[string]ToolContractV1 {
	contracts := make(map[string]ToolContractV1, len(modelToolDefinitionsV1))
	for _, definition := range modelToolDefinitionsV1 {
		if definition.Contract.Name == "" {
			panic("acquisitiontool: empty Tool name")
		}
		if _, exists := contracts[definition.Contract.Name]; exists {
			panic("acquisitiontool: duplicate Tool name " + definition.Contract.Name)
		}
		contracts[definition.Contract.Name] = definition.Contract
	}
	return contracts
}

// ModelToolDefinitionsV1 returns defensive copies in stable presentation
// order. Callers may render them but cannot mutate the runtime registry.
func ModelToolDefinitionsV1() []ModelToolDefinitionV1 {
	out := make([]ModelToolDefinitionV1, len(modelToolDefinitionsV1))
	for index, definition := range modelToolDefinitionsV1 {
		out[index] = definition
		out[index].ArgumentsSchema = bytes.Clone(definition.ArgumentsSchema)
		out[index].ExternalLocators = append(
			[]ExternalLocatorV1(nil), definition.ExternalLocators...,
		)
	}
	return out
}

func LookupModelToolDefinitionV1(
	name string,
) (ModelToolDefinitionV1, bool) {
	definition, ok := lookupModelToolDefinitionV1(name)
	if !ok {
		return ModelToolDefinitionV1{}, false
	}
	definition.ArgumentsSchema = bytes.Clone(
		definition.ArgumentsSchema,
	)
	definition.ExternalLocators = append(
		[]ExternalLocatorV1(nil), definition.ExternalLocators...,
	)
	return definition, true
}

func lookupModelToolDefinitionV1(
	name string,
) (ModelToolDefinitionV1, bool) {
	for _, definition := range modelToolDefinitionsV1 {
		if definition.Contract.Name != name {
			continue
		}
		return definition, true
	}
	return ModelToolDefinitionV1{}, false
}

// ToolCallSchemaV1 renders the exact discriminated union embedded in
// create_schedule. Tool names, descriptions and argument schemas therefore
// cannot drift from this package's runtime contracts.
func ToolCallSchemaV1() (json.RawMessage, error) {
	variants := make([]json.RawMessage, 0, len(modelToolDefinitionsV1))
	for _, definition := range modelToolDefinitionsV1 {
		var arguments any
		if err := json.Unmarshal(definition.ArgumentsSchema, &arguments); err != nil {
			return nil, fmt.Errorf(
				"acquisitiontool: invalid arguments schema for %s: %w",
				definition.Contract.Name, err,
			)
		}
		variant, err := json.Marshal(map[string]any{
			"type": "object",
			"properties": map[string]any{
				"name": map[string]any{
					"type": "string", "const": definition.Contract.Name,
					"description": definition.Description,
				},
				"arguments": arguments,
			},
			"required":             []string{"name", "arguments"},
			"additionalProperties": false,
		})
		if err != nil {
			return nil, err
		}
		variants = append(variants, variant)
	}
	return json.Marshal(map[string]any{"oneOf": variants})
}
