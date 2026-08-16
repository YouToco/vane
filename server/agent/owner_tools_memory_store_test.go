package agent

import "testing"

func TestBuildOwnerToolsSeparatesMemoryRuntimeStore(t *testing.T) {
	memoryStore := &fakeAgentMemoryStore{}
	tools := BuildOwnerTools(nil, memoryStore, ManageTasksDeps{}, nil, nil, nil)
	if len(tools) < 5 {
		t.Fatalf("owner tool count=%d, want at least 5", len(tools))
	}
	recall, ok := tools[3].Tool.(*recallMemoryTool)
	if !ok || recall.dispatcher.st != memoryStore {
		t.Fatal("recall_memory does not use the dedicated memory runtime Store")
	}
	manage, ok := tools[4].Tool.(*manageMemoryTool)
	if !ok || manage.dispatcher.st != memoryStore {
		t.Fatal("manage_memory does not use the dedicated memory runtime Store")
	}
}
