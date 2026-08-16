package main

import (
	"os"
	"strings"
	"testing"

	"github.com/YouToco/vane/server/config"
	"github.com/YouToco/vane/server/runtimeconfig"
	"github.com/YouToco/vane/server/runtimepolicy"
)

// This is an intentional release fence, not the end-state architecture.  The
// server-runtime constructor may only return after the recovery catalog and
// normal request graph have a real PostgreSQL test proving that RLS cannot
// turn an authorized read into a vacuous empty result.
func TestPrimaryStoreDefersServerRuntimeCutover(t *testing.T) {
	payload, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	source := string(payload)
	if !strings.Contains(source, "store.New(ctx, cfg.DB.URL)") {
		t.Fatal("owner-compatible primary Store constructor is missing")
	}
	if strings.Contains(source, "store.NewWithResearchRuntimeCapability(") {
		t.Fatal("primary Store still carries a research runtime pool")
	}
}

func TestResearchV3UsesIndependentRestrictedControlStore(t *testing.T) {
	payload, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	source := string(payload)
	for _, required := range []string{
		"store.NewServerRuntimeWithResearchRuntimeCapabilityAndEditRecovery(",
		"ctx, cfg.DB.ResearchControlURL, cfg.DB.ResearchRuntimeURL,",
		"cfg.DB.NativeV3EditRecoveryRuntimeURL,",
		"researchControlStore, gatewayClient, researchExecutor,",
		"researchControlStore.LoadResearchQuotaRuleV3(",
		"ResearchModel:              cfg.LLM.ResearchModel,",
		"researchControlStore, push,",
		"newResearchV3DeliveryTargetResolver(researchControlStore, manager)",
		"closeStores := func() { closeServerStores(st, researchControlStore) }",
		"readinessStores = append(readinessStores, researchControlStore)",
		"handleReadyz(readinessStores...)",
	} {
		if !strings.Contains(source, required) {
			t.Fatalf("research control Store wiring is missing %q", required)
		}
	}
	if strings.Contains(source, "st.Close()") {
		t.Fatal("an error or shutdown path bypasses dual-Store close")
	}
}

func TestOwnerMemoryToolsUseRestrictedRuntimeStore(t *testing.T) {
	payload, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	source := string(payload)
	const wiring = `researchControlStore,
		agent.ManageTasksDeps{`
	if !strings.Contains(source, wiring) {
		t.Fatal("owner memory tools are not wired to the restricted runtime Store")
	}
}

func TestDefaultResearchModelFreezesFlashAcrossEveryV3Stage(t *testing.T) {
	cfg := config.Config{DB: config.DBConfig{URL: "postgres://test"}}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	runtime, err := runtimeconfig.BuildResearchRuntimeV3(
		runtimeconfig.CurrentCompiledV1Input{
			Model: "pipeline-model", ResearchModel: cfg.LLM.ResearchModel,
			ModelEndpointGeneration:    cfg.LLM.CompiledEndpointGeneration,
			ModelCredentialGeneration:  cfg.LLM.CompiledCredentialGeneration,
			ExaCredentialGeneration:    cfg.Fetch.CompiledExaCredentialGeneration,
			TikHubCredentialGeneration: cfg.Fetch.CompiledTikHubCredentialGeneration,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	scoped, err := runtimepolicy.WithExplicitEventWindowV36(runtime.Model)
	if err != nil {
		t.Fatal(err)
	}
	if scoped.Planner.Model != "deepseek-v4-flash" ||
		scoped.Synthesis.Model != "deepseek-v4-flash" ||
		scoped.GroundingVerifier == nil ||
		scoped.GroundingVerifier.Model != "deepseek-v4-flash" ||
		scoped.GroundingCorrector == nil ||
		scoped.GroundingCorrector.Model != "deepseek-v4-flash" {
		t.Fatalf("default research stages do not all freeze flash: %+v",
			scoped)
	}
}

func TestAgentCompositionFailuresCloseAcquiredResourcesBeforeWorkerStart(t *testing.T) {
	payload, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	source := string(payload)
	workerStart := strings.Index(source, "if err := w.Start(); err != nil")
	a2aBuild := strings.Index(source, "a2aLoop, loopErr = agent.NewChecked(")
	backgroundStart := strings.Index(source, "runMaintenance(func()")
	if workerStart < 0 || a2aBuild < 0 || a2aBuild >= workerStart {
		t.Fatalf("A2A composition must finish before worker start: a2a=%d worker=%d",
			a2aBuild, workerStart)
	}
	if backgroundStart < 0 || a2aBuild >= backgroundStart {
		t.Fatalf("A2A composition must finish before background DB users: a2a=%d background=%d",
			a2aBuild, backgroundStart)
	}
	for _, failure := range []string{
		"装配 Agent 工具注册表: %w",
		"筛选 A2A Agent 工具: %w",
		"装配 A2A Agent 工具注册表: %w",
	} {
		failureAt := strings.Index(source, failure)
		if failureAt < 0 {
			t.Fatalf("startup failure %q is missing", failure)
		}
		start := failureAt - 240
		if start < 0 {
			start = 0
		}
		if !strings.Contains(source[start:failureAt],
			"closeServerStartupResources(temporalClient.Close, closeStores)") {
			t.Fatalf("startup failure %q leaks acquired resources", failure)
		}
	}
}

func TestOwnerCompatibilityReleaseContract(t *testing.T) {
	const want = "vane.server-release-contract/v2 primary_store=owner_compat_v1 research_control_store=restricted_v1 research_store=restricted_v1"
	if serverReleaseContractV2 != want {
		t.Fatalf("release contract=%q want %q", serverReleaseContractV2, want)
	}
}
