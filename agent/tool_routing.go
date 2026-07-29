package agent

import "strings"

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

func explicitOwnerToolIntent(toolName, text string) bool {
	normalized := strings.ToLower(strings.Join(strings.Fields(text), ""))
	switch toolName {
	case "remove_schedule":
		if containsAny(normalized,
			"不要删除", "别删除", "不删除", "无需删除", "不用删除",
			"不要取消", "别取消", "不取消", "无需取消", "不用取消",
			"donotdelete", "don'tdelete", "donotremove", "don'tremove",
		) {
			return false
		}
		if containsAny(normalized,
			"为什么", "怎么删除", "如何删除", "是否", "要不要",
			"会怎样", "有什么影响", "利弊", "why", "howto",
		) {
			return false
		}
		return startsWithAny(normalized,
			"删除", "请删除", "帮我删除", "取消", "请取消",
			"停止", "请停止", "关掉", "请关掉", "移除", "请移除",
		) || containsAny(normalized,
			"删除任务", "取消任务", "停止任务", "关掉任务", "移除任务",
			"删除早报", "取消早报", "删掉", "removeschedule", "deletetask",
		)
	case "run_task_now":
		return containsAny(normalized,
			"立即推送", "现在推送", "马上推送", "立即运行", "现在运行",
			"马上运行", "立即检查", "现在检查", "pushnow", "runnow",
		)
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
