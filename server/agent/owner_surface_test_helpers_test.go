package agent

// ownerTestTools returns the complete owner catalog while allowing a test to
// replace one or more handlers by name. NewChecked intentionally rejects
// partial owner catalogs because production must never degrade to a narrow or
// legacy task surface.
func ownerTestTools(overrides ...ToolSpec) []ToolSpec {
	tools := BuildOwnerTools(nil, nil, ManageTasksDeps{}, nil, nil, nil)
	for _, override := range overrides {
		replaced := false
		for i := range tools {
			if tools[i].Name() == override.Name() {
				tools[i] = override
				replaced = true
				break
			}
		}
		if !replaced {
			tools = append(tools, override)
		}
	}
	return tools
}
