package main

import "testing"

func TestExcludedByRiskClassifiesCapabilityNotMethod(t *testing.T) {
	for _, name := range []string{
		"douyin_web_fetch_douyin_web_guest_cookie",
		"tiktok_web_generate_wss_xb_signature",
		"tiktok_app_v3_encrypt_decrypt_login_request",
		"douyin_app_v3_register_device",
		"douyin_app_v3_open_douyin_app_to_send_private_message",
		"douyin_app_v3_add_video_play_count",
		"social_publish_post",
	} {
		if !excludedByRisk(name, "/api/v1/"+name, operation{}) {
			t.Errorf("%s should be excluded", name)
		}
	}
	for _, name := range []string{
		"douyin_web_fetch_user_like_videos",
		"wechat_mp_v2_fetch_article_comments",
		"zhihu_web_fetch_user_follow_topics",
		"youtube_web_v2_get_signed_stream_url",
	} {
		if excludedByRisk(name, "/api/v1/"+name, operation{}) {
			t.Errorf("%s should remain read-only", name)
		}
	}
}
