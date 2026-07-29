package acquisitiontool

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/YouToco/vane/internal/strictjson"
	"github.com/YouToco/vane/types"
)

// CanonicalizeToolArgumentsV1 is the exact current-write boundary for
// acquisition Tool arguments. Unknown fields (including api_key/token),
// duplicate keys, explicit null arrays, and semantically invalid values are
// rejected before an Approved definition can persist them.
func CanonicalizeToolArgumentsV1(
	toolName string,
	raw json.RawMessage,
) (json.RawMessage, error) {
	var fields map[string]json.RawMessage
	if strictjson.DecodeExact(raw, &fields) != nil || fields == nil {
		return nil, errors.New("Tool arguments must be a non-null JSON object")
	}
	validate := func(requirement Requirement) error {
		target, message := BuildTarget(requirement)
		if message != "" || target == nil {
			if message == "" {
				message = "Tool arguments cannot be materialized"
			}
			return errors.New(message)
		}
		return nil
	}
	marshal := func(_ any, requirement Requirement) (json.RawMessage, error) {
		if err := validate(requirement); err != nil {
			return nil, err
		}
		canonical, err := json.Marshal(fields)
		if err != nil {
			return nil, errors.New("Tool arguments cannot be encoded")
		}
		return canonical, nil
	}
	explicitNull := func(name string) bool {
		value, ok := fields[name]
		return ok && bytes.Equal(bytes.TrimSpace(value), []byte("null"))
	}

	switch toolName {
	case "web_search":
		if explicitNull("include_domains") {
			return nil, errors.New("include_domains must be an array")
		}
		var input struct {
			Query          string   `json:"query"`
			Category       string   `json:"category,omitempty"`
			IncludeDomains []string `json:"include_domains,omitempty"`
		}
		if err := strictjson.DecodeExact(raw, &input); err != nil {
			return nil, err
		}
		params := map[string]string{"query": input.Query}
		if input.Category != "" {
			params["category"] = input.Category
		}
		if input.IncludeDomains != nil {
			encoded, err := json.Marshal(input.IncludeDomains)
			if err != nil {
				return nil, err
			}
			params["include_domains"] = string(encoded)
		}
		return marshal(input, Requirement{
			Platform: string(types.PlatformWeb), Capability: string(types.CapSearch),
			Params: params,
		})
	case "web_feed":
		if explicitNull("categories") {
			return nil, errors.New("categories must be an array")
		}
		var input struct {
			FeedURL    string   `json:"feed_url"`
			Categories []string `json:"categories,omitempty"`
		}
		if err := strictjson.DecodeExact(raw, &input); err != nil {
			return nil, err
		}
		params := map[string]string{"url": input.FeedURL}
		if input.Categories != nil {
			encoded, err := json.Marshal(input.Categories)
			if err != nil {
				return nil, err
			}
			params["categories"] = string(encoded)
		}
		return marshal(input, Requirement{
			Platform: string(types.PlatformWeb), Capability: string(types.CapFeed),
			Params: params,
		})
	case "web_contents":
		var input struct {
			PageURL string `json:"page_url"`
		}
		if err := strictjson.DecodeExact(raw, &input); err != nil {
			return nil, err
		}
		return marshal(input, Requirement{
			Platform: string(types.PlatformWeb), Capability: string(types.CapContents),
			Params: map[string]string{"url": input.PageURL},
		})
	case "x_user_posts":
		var input struct {
			ScreenName string `json:"screen_name"`
		}
		if err := strictjson.DecodeExact(raw, &input); err != nil {
			return nil, err
		}
		return marshal(input, Requirement{
			Platform: string(types.PlatformX), Capability: string(types.CapUserPosts),
			Params: map[string]string{"screen_name": input.ScreenName},
		})
	case "xhs_search":
		var input struct {
			Keyword string `json:"keyword"`
		}
		if err := strictjson.DecodeExact(raw, &input); err != nil {
			return nil, err
		}
		return marshal(input, Requirement{
			Platform: string(types.PlatformXHS), Capability: string(types.CapSearch),
			Params: map[string]string{"keyword": input.Keyword},
		})
	case "xhs_user_posts", "xhs_faved_notes":
		var input struct {
			UserID     string `json:"user_id,omitempty"`
			ProfileURL string `json:"profile_url,omitempty"`
		}
		if err := strictjson.DecodeExact(raw, &input); err != nil {
			return nil, err
		}
		if exactlyOne(input.UserID, input.ProfileURL) == false {
			return nil, errors.New("exactly one of user_id or profile_url is required")
		}
		capability := types.CapUserPosts
		if toolName == "xhs_faved_notes" {
			capability = types.CapFavedNotes
		}
		return marshal(input, Requirement{
			Platform: string(types.PlatformXHS), Capability: string(capability),
			Params: map[string]string{
				"user_id": input.UserID, "profile_url": input.ProfileURL,
			},
		})
	case "xhs_hot_list":
		var input struct{}
		if err := strictjson.DecodeExact(raw, &input); err != nil {
			return nil, err
		}
		return marshal(input, Requirement{
			Platform: string(types.PlatformXHS), Capability: string(types.CapHotList),
			Params: map[string]string{},
		})
	case "xhs_topic_feed":
		var input struct {
			PageID   string `json:"page_id,omitempty"`
			TopicURL string `json:"topic_url,omitempty"`
		}
		if err := strictjson.DecodeExact(raw, &input); err != nil {
			return nil, err
		}
		if !exactlyOne(input.PageID, input.TopicURL) {
			return nil, errors.New("exactly one of page_id or topic_url is required")
		}
		return marshal(input, Requirement{
			Platform: string(types.PlatformXHS), Capability: string(types.CapTopicFeed),
			Params: map[string]string{
				"page_id": input.PageID, "topic_url": input.TopicURL,
			},
		})
	case "weibo_user_posts":
		var input struct {
			UID        string `json:"uid,omitempty"`
			ProfileURL string `json:"profile_url,omitempty"`
		}
		if err := strictjson.DecodeExact(raw, &input); err != nil {
			return nil, err
		}
		if !exactlyOne(input.UID, input.ProfileURL) {
			return nil, errors.New("exactly one of uid or profile_url is required")
		}
		return marshal(input, Requirement{
			Platform: string(types.PlatformWeibo), Capability: string(types.CapUserPosts),
			Params: map[string]string{
				"uid": input.UID, "profile_url": input.ProfileURL,
			},
		})
	case "weibo_hot_list":
		var input struct{}
		if err := strictjson.DecodeExact(raw, &input); err != nil {
			return nil, err
		}
		return marshal(input, Requirement{
			Platform: string(types.PlatformWeibo), Capability: string(types.CapHotList),
			Params: map[string]string{},
		})
	case "wechat_mp_user_posts":
		var input struct {
			Username string `json:"username"`
		}
		if err := strictjson.DecodeExact(raw, &input); err != nil {
			return nil, err
		}
		return marshal(input, Requirement{
			Platform: string(types.PlatformWechatMP), Capability: string(types.CapUserPosts),
			Params: map[string]string{"username": input.Username},
		})
	default:
		return nil, fmt.Errorf("unsupported acquisition Tool %q", toolName)
	}
}

func exactlyOne(left, right string) bool {
	return (strings.TrimSpace(left) == "") != (strings.TrimSpace(right) == "")
}
