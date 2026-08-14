package agent

func retiredOwnerToolName(name string) bool {
	switch name {
	case "list_schedules", "view_task_playbook", "view_task_latest_run",
		"view_profile", "create_schedule", "edit_task_definition",
		"run_task_now", "remove_schedule":
		return true
	default:
		return false
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
