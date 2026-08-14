package fetcher

import (
	"encoding/json"
	"fmt"

	"github.com/YouToco/vane/tikhubcatalog"
	"github.com/YouToco/vane/types"
)

// retainedBindingCatalogV1 is the transport subset required by the immutable
// fetcher.binding/v1 templates. It is intentionally independent from the
// generated current TikHub catalog, whose entries may be regenerated or
// removed without reinterpreting an already-sealed run.
var retainedBindingCatalogV1 = map[string]tikhubcatalog.Entry{
	"xiaohongshu_app_v2_search_notes": retainedGETV1(
		"xiaohongshu_app_v2_search_notes",
		"/api/v1/xiaohongshu/app_v2/search_notes",
		queryParamV1("keyword", true, "string"),
		queryParamV1("page", false, "integer"),
		queryParamV1("sort_type", false, "string"),
		queryParamV1("note_type", false, "string"),
	),
	"xiaohongshu_web_v3_fetch_note_detail": retainedGETV1(
		"xiaohongshu_web_v3_fetch_note_detail",
		"/api/v1/xiaohongshu/web_v3/fetch_note_detail",
		queryParamV1("note_id", true, "string"),
		queryParamV1("xsec_token", true, "string"),
	),
	"xiaohongshu_app_v2_get_user_posted_notes": retainedGETV1(
		"xiaohongshu_app_v2_get_user_posted_notes",
		"/api/v1/xiaohongshu/app_v2/get_user_posted_notes",
		queryParamV1("user_id", false, "string"),
		queryParamV1("cursor", false, "string"),
	),
	"twitter_web_fetch_user_post_tweet": retainedGETV1(
		"twitter_web_fetch_user_post_tweet",
		"/api/v1/twitter/web/fetch_user_post_tweet",
		queryParamV1("screen_name", false, "string"),
	),
	"xiaohongshu_web_v3_fetch_hot_list": retainedGETV1(
		"xiaohongshu_web_v3_fetch_hot_list",
		"/api/v1/xiaohongshu/web_v3/fetch_hot_list",
	),
	"xiaohongshu_app_v2_get_topic_feed": retainedGETV1(
		"xiaohongshu_app_v2_get_topic_feed",
		"/api/v1/xiaohongshu/app_v2/get_topic_feed",
		queryParamV1("page_id", true, "string"),
		queryParamV1("sort", false, "string"),
	),
	"xiaohongshu_app_v2_get_user_faved_notes": retainedGETV1(
		"xiaohongshu_app_v2_get_user_faved_notes",
		"/api/v1/xiaohongshu/app_v2/get_user_faved_notes",
		queryParamV1("user_id", false, "string"),
	),
	"weibo_web_v2_fetch_user_posts": retainedGETV1(
		"weibo_web_v2_fetch_user_posts",
		"/api/v1/weibo/web_v2/fetch_user_posts",
		queryParamV1("uid", true, "string"),
		queryParamV1("feature", false, "integer"),
	),
	"weibo_web_v2_fetch_hot_search": retainedGETV1(
		"weibo_web_v2_fetch_hot_search",
		"/api/v1/weibo/web_v2/fetch_hot_search",
	),
	"wechat_mp_v2_fetch_account_articles": {
		Name: "wechat_mp_v2_fetch_account_articles", Method: "POST",
		Path: "/api/v1/wechat_mp/v2/fetch_account_articles",
		Params: []tikhubcatalog.Param{
			{Name: "username", In: "body", Required: true, Type: "string"},
			{Name: "raw", In: "body", Type: "boolean"},
		},
	},
}

type retainedBindingRouteV1 struct {
	spec        bindingSpec
	entry       tikhubcatalog.Entry
	enrichEntry *tikhubcatalog.Entry
}

func resolveRetainedBindingRouteV1(
	platform types.Platform,
	capability types.Capability,
) (retainedBindingRouteV1, error) {
	spec, ok := bindingTemplatesV1[bindingKey{platform, capability}]
	if !ok {
		return retainedBindingRouteV1{},
			fmt.Errorf("fetcher: retained binding v1 template is unavailable")
	}
	spec, err := cloneBindingSpecV1(spec)
	if err != nil {
		return retainedBindingRouteV1{}, err
	}
	entry, ok := retainedBindingCatalogV1[spec.Endpoint]
	if !ok {
		return retainedBindingRouteV1{},
			fmt.Errorf("fetcher: retained binding v1 endpoint is unavailable")
	}
	route := retainedBindingRouteV1{spec: spec, entry: cloneCatalogEntryV1(entry)}
	if spec.Enrich != nil {
		enrich, ok := retainedBindingCatalogV1[spec.Enrich.Endpoint]
		if !ok {
			return retainedBindingRouteV1{},
				fmt.Errorf("fetcher: retained binding v1 enrich endpoint is unavailable")
		}
		cloned := cloneCatalogEntryV1(enrich)
		route.enrichEntry = &cloned
	}
	return route, nil
}

func cloneBindingSpecV1(spec bindingSpec) (bindingSpec, error) {
	raw, err := json.Marshal(spec)
	if err != nil {
		return bindingSpec{}, fmt.Errorf(
			"fetcher: retained binding v1 template cannot be cloned: %w", err)
	}
	var cloned bindingSpec
	if err := json.Unmarshal(raw, &cloned); err != nil {
		return bindingSpec{}, fmt.Errorf(
			"fetcher: retained binding v1 template cannot be decoded: %w", err)
	}
	return cloned, nil
}

func cloneCatalogEntryV1(entry tikhubcatalog.Entry) tikhubcatalog.Entry {
	cloned := entry
	cloned.Params = append([]tikhubcatalog.Param(nil), entry.Params...)
	return cloned
}

func retainedGETV1(
	name string,
	path string,
	params ...tikhubcatalog.Param,
) tikhubcatalog.Entry {
	return tikhubcatalog.Entry{
		Name: name, Method: "GET", Path: path, Params: params,
	}
}

func queryParamV1(
	name string,
	required bool,
	kind string,
) tikhubcatalog.Param {
	return tikhubcatalog.Param{
		Name: name, In: "query", Required: required, Type: kind,
	}
}
