package acquisitiontool

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/YouToco/vane/internal/strictjson"
	"github.com/YouToco/vane/types"
)

// MaterializeApprovedToolCallV1 is a current-write compiler: it converts the
// canonical arguments of an Approved V2 definition into the exact internal
// request that will be frozen in a run snapshot. Historical execution must use
// that frozen request and must never call this function or its current builders.
func MaterializeApprovedToolCallV1(
	toolName string,
	toolContractVersion string,
	raw json.RawMessage,
) (*types.FetchTarget, error) {
	if toolContractVersion != "v1" {
		return nil, errors.New("frozen Tool contract version is unsupported")
	}
	requirement, err := decodeApprovedToolArgumentsV1(toolName, raw)
	if err != nil {
		return nil, err
	}
	target, message := BuildTarget(requirement)
	if message != "" || target == nil {
		if message == "" {
			message = "frozen Tool call cannot be materialized"
		}
		return nil, errors.New(message)
	}
	// A Source-free execution request has no product/source identity or shared
	// due/health state. Fetchers may use the remaining fields only.
	target.ID = 0
	target.FetchIntervalSeconds = 0
	target.NextFetchAt = target.NextFetchAt.UTC()
	target.LastFetchedAt = nil
	target.FailCount = 0
	return target, nil
}

func decodeApprovedToolArgumentsV1(
	toolName string,
	raw json.RawMessage,
) (Requirement, error) {
	if len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return Requirement{}, errors.New("frozen Tool arguments must be an object")
	}
	switch toolName {
	case "web_search":
		var input struct {
			Query          string   `json:"query"`
			Category       string   `json:"category,omitempty"`
			IncludeDomains []string `json:"include_domains,omitempty"`
		}
		if err := strictjson.DecodeExact(raw, &input); err != nil {
			return Requirement{}, err
		}
		params := map[string]string{"query": input.Query}
		if input.Category != "" {
			params["category"] = input.Category
		}
		if input.IncludeDomains != nil {
			encoded, err := json.Marshal(input.IncludeDomains)
			if err != nil {
				return Requirement{}, err
			}
			params["include_domains"] = string(encoded)
		}
		return Requirement{
			Platform:   string(types.PlatformWeb),
			Capability: string(types.CapSearch), Params: params,
		}, nil
	case "web_feed":
		var input struct {
			FeedURL    string   `json:"feed_url"`
			Categories []string `json:"categories,omitempty"`
		}
		if err := strictjson.DecodeExact(raw, &input); err != nil {
			return Requirement{}, err
		}
		params := map[string]string{"url": input.FeedURL}
		if input.Categories != nil {
			encoded, err := json.Marshal(input.Categories)
			if err != nil {
				return Requirement{}, err
			}
			params["categories"] = string(encoded)
		}
		return Requirement{
			Platform:   string(types.PlatformWeb),
			Capability: string(types.CapFeed), Params: params,
		}, nil
	case "web_contents":
		var input struct {
			PageURL string `json:"page_url"`
		}
		if err := strictjson.DecodeExact(raw, &input); err != nil {
			return Requirement{}, err
		}
		return Requirement{
			Platform:   string(types.PlatformWeb),
			Capability: string(types.CapContents),
			Params:     map[string]string{"url": input.PageURL},
		}, nil
	case "x_user_posts":
		var input struct {
			ScreenName string `json:"screen_name"`
		}
		if err := strictjson.DecodeExact(raw, &input); err != nil {
			return Requirement{}, err
		}
		return Requirement{
			Platform:   string(types.PlatformX),
			Capability: string(types.CapUserPosts),
			Params:     map[string]string{"screen_name": input.ScreenName},
		}, nil
	case "xhs_search":
		var input struct {
			Keyword string `json:"keyword"`
		}
		if err := strictjson.DecodeExact(raw, &input); err != nil {
			return Requirement{}, err
		}
		return Requirement{
			Platform:   string(types.PlatformXHS),
			Capability: string(types.CapSearch),
			Params:     map[string]string{"keyword": input.Keyword},
		}, nil
	case "xhs_user_posts", "xhs_faved_notes":
		var input struct {
			UserID     string `json:"user_id,omitempty"`
			ProfileURL string `json:"profile_url,omitempty"`
		}
		if err := strictjson.DecodeExact(raw, &input); err != nil {
			return Requirement{}, err
		}
		if !exactlyOne(input.UserID, input.ProfileURL) {
			return Requirement{},
				errors.New("exactly one of user_id or profile_url is required")
		}
		capability := types.CapUserPosts
		if toolName == "xhs_faved_notes" {
			capability = types.CapFavedNotes
		}
		return Requirement{
			Platform: string(types.PlatformXHS), Capability: string(capability),
			Params: map[string]string{
				"user_id": input.UserID, "profile_url": input.ProfileURL,
			},
		}, nil
	case "xhs_hot_list":
		var input struct{}
		if err := strictjson.DecodeExact(raw, &input); err != nil {
			return Requirement{}, err
		}
		return Requirement{
			Platform:   string(types.PlatformXHS),
			Capability: string(types.CapHotList), Params: map[string]string{},
		}, nil
	case "xhs_topic_feed":
		var input struct {
			PageID   string `json:"page_id,omitempty"`
			TopicURL string `json:"topic_url,omitempty"`
		}
		if err := strictjson.DecodeExact(raw, &input); err != nil {
			return Requirement{}, err
		}
		if !exactlyOne(input.PageID, input.TopicURL) {
			return Requirement{},
				errors.New("exactly one of page_id or topic_url is required")
		}
		return Requirement{
			Platform:   string(types.PlatformXHS),
			Capability: string(types.CapTopicFeed),
			Params: map[string]string{
				"page_id": input.PageID, "topic_url": input.TopicURL,
			},
		}, nil
	case "weibo_user_posts":
		var input struct {
			UID        string `json:"uid,omitempty"`
			ProfileURL string `json:"profile_url,omitempty"`
		}
		if err := strictjson.DecodeExact(raw, &input); err != nil {
			return Requirement{}, err
		}
		if !exactlyOne(input.UID, input.ProfileURL) {
			return Requirement{},
				errors.New("exactly one of uid or profile_url is required")
		}
		return Requirement{
			Platform:   string(types.PlatformWeibo),
			Capability: string(types.CapUserPosts),
			Params: map[string]string{
				"uid": input.UID, "profile_url": input.ProfileURL,
			},
		}, nil
	case "weibo_hot_list":
		var input struct{}
		if err := strictjson.DecodeExact(raw, &input); err != nil {
			return Requirement{}, err
		}
		return Requirement{
			Platform:   string(types.PlatformWeibo),
			Capability: string(types.CapHotList), Params: map[string]string{},
		}, nil
	case "wechat_mp_user_posts":
		var input struct {
			Username string `json:"username"`
		}
		if err := strictjson.DecodeExact(raw, &input); err != nil {
			return Requirement{}, err
		}
		return Requirement{
			Platform:   string(types.PlatformWechatMP),
			Capability: string(types.CapUserPosts),
			Params:     map[string]string{"username": input.Username},
		}, nil
	default:
		return Requirement{}, fmt.Errorf(
			"unsupported approved acquisition Tool %q", toolName)
	}
}
