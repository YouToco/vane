package agent

import "strings"

func legacyGeneralChatTool(name string) bool {
	switch name {
	case "list_schedules", "view_task_playbook", "view_task_latest_run",
		"view_profile", "create_schedule", "edit_task_definition",
		"run_task_now", "remove_schedule":
		return true
	default:
		return false
	}
}

func agentFirstOnlyTool(name string) bool {
	return name == "query_my_intelligence" || name == "manage_tasks"
}

// classifyOwnerIntents is deliberately deterministic and only consumes the
// authenticated current-user request. It narrows the first-turn schema set; it
// is not an authorization decision, and missing a tag can only hide a tool.
func classifyOwnerIntents(text string) ToolIntent {
	normalized := strings.ToLower(strings.Join(strings.Fields(text), ""))
	var intents ToolIntent
	if containsAny(normalized,
		"信源", "订阅源", "rss", "feed", "公众号", "微博源", "来源",
		"订阅这个", "持续关注", "持续追踪", "盯住", "监控这个页面",
	) {
		// Legacy user vocabulary still means a recurring task; it no longer
		// exposes a separate source-management toolkit.
		intents |= IntentTasks
	}
	if containsAny(normalized,
		"任务", "定时", "日程", "计划", "早报", "日报", "周报", "月报",
		"推送", "立即运行", "马上运行", "cron", "schedule", "提醒",
		"每小时", "每天", "每周", "每月",
	) {
		intents |= IntentTasks
	}
	if containsAny(normalized,
		"画像", "行业", "职业", "岗位", "关注标签", "我的偏好", "profile",
		"我是", "我在做", "我负责", "我关注",
	) {
		intents |= IntentProfile
	}
	if containsAny(normalized,
		"社媒", "社交媒体", "账号", "评论", "热榜", "热搜", "粉丝",
		"抖音", "douyin", "tiktok", "小红书", "xiaohongshu", "微博",
		"weibo", "b站", "bilibili", "知乎", "zhihu", "快手", "kuaishou",
		"youtube", "instagram", "twitter", "threads", "reddit", "linkedin",
		"telegram", "公众号", "视频号",
	) {
		intents |= IntentSocialResearch
	}
	if containsAny(normalized,
		"查", "搜索", "检索", "最新", "现在", "是否", "定价", "价格",
		"官网", "官方", "网页", "页面", "链接", "http://", "https://",
		"research", "search", "latest", "pricing",
	) {
		intents |= IntentWebResearch
	}
	return intents
}

func explicitOwnerToolIntent(toolName, _ string) bool {
	switch toolName {
	case "remove_schedule", "run_task_now", "create_schedule",
		"update_profile":
		// Task mutations/delivery are never authorized lexically. The isolated
		// semantic action gate (or the dedicated direct-creation lane) must name
		// the exact allowed tool for this turn.
		return false
	default:
		return true
	}
}

func toolVisibleForRequest(spec ToolSpec, state *toolRunState) bool {
	if !spec.Policy.RoutingConfigured {
		return true
	}
	if state == nil {
		return spec.Policy.Exposure == ExposureAlways
	}
	switch spec.Policy.Exposure {
	case ExposureAlways:
		return true
	case ExposureIntent:
		if spec.Policy.DirectOnExplicitIntent &&
			state.allowedSideEffectTool == spec.Name() {
			return true
		}
		if !spec.Policy.Intents.HasAny(state.intents) {
			return false
		}
		if spec.Policy.DirectOnExplicitIntent {
			return explicitOwnerToolIntent(spec.Name(), state.ownerRequest)
		}
		return true
	case ExposureContext:
		if spec.Name() == "read_endpoint_result" {
			return state.hasLocalResultHandles()
		}
		return spec.Policy.Intents.HasAny(state.intents)
	default:
		return false
	}
}
