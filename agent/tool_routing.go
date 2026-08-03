package agent

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
