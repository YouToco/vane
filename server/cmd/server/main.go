// vane server 入口：加载配置 → 初始化日志 → 建库连接 →
// LLM 客户端/记账 → 飞书 Manager → HTTP 服务（healthz/readyz + Dashboard API）→ 优雅关停。
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"github.com/YouToco/vane/server/a2a"
	"github.com/YouToco/vane/server/agent"
	"github.com/YouToco/vane/server/api"
	"github.com/YouToco/vane/server/auth"
	"github.com/YouToco/vane/server/cardgen"
	"github.com/YouToco/vane/server/config"
	"github.com/YouToco/vane/server/eventqualifier"
	"github.com/YouToco/vane/server/evolver"
	"github.com/YouToco/vane/server/executivebriefrecovery"
	"github.com/YouToco/vane/server/feedback"
	"github.com/YouToco/vane/server/feishu"
	"github.com/YouToco/vane/server/fetcher"
	"github.com/YouToco/vane/server/llm"
	"github.com/YouToco/vane/server/periodicbrief"
	"github.com/YouToco/vane/server/profilehint"
	"github.com/YouToco/vane/server/pusheffect"
	"github.com/YouToco/vane/server/pusher"
	"github.com/YouToco/vane/server/pushrecovery"
	"github.com/YouToco/vane/server/researchgateway"
	"github.com/YouToco/vane/server/runoutcome"
	"github.com/YouToco/vane/server/runtimeconfig"
	"github.com/YouToco/vane/server/runtimepolicy"
	"github.com/YouToco/vane/server/scheduler"
	"github.com/YouToco/vane/server/scorer"
	"github.com/YouToco/vane/server/store"
	"github.com/YouToco/vane/server/task"
	"github.com/YouToco/vane/server/tikhubinvoke"
	"github.com/YouToco/vane/server/types"
	"github.com/YouToco/vane/server/workflow"
)

// vaneVersion 是服务版本串（进 A2A AgentCard.version，a2a-contract §7）。
// 值 = CHANGELOG 最上方已发布版本号，随发版手动同步；不为此新增 ldflags 基建。
const vaneVersion = "0.5.1"

// serverReleaseContractV2 is a machine-readable, side-effect-free deployment
// assertion.  The control plane checks this exact value before it is allowed
// to pair a binary with the temporary owner-compatible primary Store contract.
// V3 execution remains on the independently authenticated research runtime.
const serverReleaseContractV2 = "vane.server-release-contract/v2 primary_store=owner_compat_v1 research_control_store=restricted_v1 research_store=restricted_v1"

var (
	_ task.CreationSagaStore       = (*store.LegacyAdmissionFencedStore)(nil)
	_ task.TaskDefinitionEditStore = (*store.LegacyAdmissionFencedStore)(nil)
)

func main() {
	if len(os.Args) == 2 && os.Args[1] == "-print-release-contract" {
		fmt.Println(serverReleaseContractV2)
		return
	}
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "vane: %v\n", err)
		os.Exit(1)
	}
}

func closeServerStores(primary, researchControl *store.Store) {
	if researchControl != nil && researchControl != primary {
		researchControl.Close()
	}
	if primary != nil {
		primary.Close()
	}
}

func closeServerStartupResources(closeTemporal, closeStores func()) {
	if closeTemporal != nil {
		closeTemporal()
	}
	if closeStores != nil {
		closeStores()
	}
}

func run() error {
	// 顶层 ctx：SIGINT/SIGTERM 时取消，触发优雅关停。
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// path 传空：按 ./config.yaml → /opt/vane/config/config.yaml 自动探测，缺失则纯环境变量运行。
	cfg, err := config.Load("")
	if err != nil {
		return fmt.Errorf("加载配置: %w", err)
	}
	// The owner surface always contains manage_tasks create. Fail before the
	// first Store connection (and therefore before workers or ingress) instead
	// of advertising a durable action whose V3 runtime is dark.
	if err := requireOwnerAgentResearchV3Runtime(cfg); err != nil {
		return fmt.Errorf("Owner Agent 启动 Gate: %w", err)
	}

	initLogger(cfg.Log.Level)

	// The primary Store still contains retained recovery and reconciliation
	// readers which enumerate tenant shards before re-entering a tenant-scoped
	// role.  Running that graph as vane_server_runtime currently turns some of
	// those reads into either permission errors or RLS-empty success.  Keep the
	// primary pool on the schema-owner compatibility boundary until the full
	// recovery catalog and every per-tenant reader have passed the real
	// PostgreSQL server-runtime gate. It deliberately carries no research pool
	// or V3 capability keyring.
	st, err := store.New(ctx, cfg.DB.URL)
	if err != nil {
		return fmt.Errorf("初始化数据库连接池: %w", err)
	}
	if cfg.CredentialVault.ActiveKeyID != "" {
		if err := st.ConfigureCredentialVault(
			cfg.CredentialVault.ActiveKeyID,
			cfg.CredentialVault.ActiveKeyHex,
			cfg.CredentialVault.RetiredKeys,
		); err != nil {
			closeServerStores(st, nil)
			return fmt.Errorf("初始化加密凭证库: %w", err)
		}
	} else {
		slog.Warn("加密凭证库未配置，Web 凭证管理将 fail-closed")
	}
	if err := applyStoredLLMCredential(ctx, st, &cfg.LLM); err != nil {
		closeServerStores(st, nil)
		return fmt.Errorf("加载数据库 LLM 凭证: %w", err)
	}
	var researchControlStore *store.Store
	closeStores := func() { closeServerStores(st, researchControlStore) }
	if _, err := st.AssertAgentFirstLegacyWriteFence(ctx); err != nil {
		closeStores()
		return fmt.Errorf("验证 Agent-first legacy 写入冻结: %w", err)
	}
	legacyStore, err := store.NewLegacyAdmissionFencedStore(st)
	if err != nil {
		closeStores()
		return fmt.Errorf("关闭 legacy control-plane admission: %w", err)
	}
	gatewayClient, err := researchgateway.NewUnixClientV1(cfg.ResearchGateway.SocketPath)
	if err != nil {
		closeStores()
		return fmt.Errorf("初始化 research gateway client: %w", err)
	}
	var researchRuntimeOption workflow.ActivitiesOption
	var researchDeliveryOption workflow.ActivitiesOption
	if shouldInitializeResearchV3Runtime(cfg) {
		researchControlStore, err = store.NewServerRuntimeWithResearchRuntimeCapabilityAndEditRecovery(
			ctx, cfg.DB.ResearchControlURL, cfg.DB.ResearchRuntimeURL,
			cfg.DB.NativeV3EditRecoveryRuntimeURL,
			store.ResearchRunCapabilityConfigV1{
				ActiveKeyID:  cfg.DB.ResearchCapabilityKeyID,
				ActiveKeyHex: cfg.DB.ResearchCapabilityKeyHex,
				RetiredKeys:  cfg.DB.ResearchCapabilityRetiredKeys,
				TTL: time.Duration(cfg.DB.ResearchCapabilityTTLDays) *
					24 * time.Hour,
			},
		)
		if err != nil {
			closeStores()
			return fmt.Errorf("初始化 research V3 control Store: %w", err)
		}
		researchExecutor, executorErr := fetcher.NewResearchExecutorV3(cfg.Fetch)
		if executorErr != nil {
			closeStores()
			return fmt.Errorf("初始化 research V3 executor: %w", executorErr)
		}
		researchRuntime, runtimeErr := workflow.NewProductionResearchRuntimeV3(
			researchControlStore, gatewayClient, researchExecutor,
			func(ctx context.Context, identity types.RunIdentity) (
				runtimepolicy.BundleV1,
				runtimepolicy.ResearchToolPolicyV3,
				runtimepolicy.ResearchModelPolicyV3,
				error,
			) {
				for _, bucket := range []store.QuotaBucket{
					store.QuotaLLMTokens, store.QuotaExaCalls,
				} {
					if _, err := researchControlStore.LoadResearchQuotaRuleV3(
						ctx, identity, bucket,
					); err != nil {
						return runtimepolicy.BundleV1{},
							runtimepolicy.ResearchToolPolicyV3{},
							runtimepolicy.ResearchModelPolicyV3{}, err
					}
				}
				current, err := runtimeconfig.BuildResearchRuntimeV3(
					runtimeconfig.CurrentCompiledV1Input{
						Model:                      cfg.LLM.Model,
						ResearchModel:              cfg.LLM.ResearchModel,
						TaskInstructionEnabled:     true,
						ModelEndpointGeneration:    cfg.LLM.CompiledEndpointGeneration,
						ModelCredentialGeneration:  cfg.LLM.CompiledCredentialGeneration,
						ExaCredentialGeneration:    cfg.Fetch.CompiledExaCredentialGeneration,
						TikHubCredentialGeneration: cfg.Fetch.CompiledTikHubCredentialGeneration,
					})
				if err != nil {
					return runtimepolicy.BundleV1{},
						runtimepolicy.ResearchToolPolicyV3{},
						runtimepolicy.ResearchModelPolicyV3{}, err
				}
				return current.Bundle, current.Tools, current.Model, nil
			},
			func(ctx context.Context, identity types.RunIdentity, authorityToken string) (bool, error) {
				return authorizeResearchV3Runtime(
					ctx, cfg, researchControlStore, identity, authorityToken,
				)
			},
		)
		if runtimeErr != nil {
			closeStores()
			return fmt.Errorf("初始化 research V3 coordinator: %w", runtimeErr)
		}
		researchRuntimeOption = workflow.WithResearchRuntimeV3(researchRuntime)
	}

	// LLM 客户端 + 调用记账（写库失败只记日志，不影响主流程）。
	llmClient := llm.New(cfg.LLM)
	agentLLMConfig, err := cfg.LLM.AgentClientConfig()
	if err != nil {
		closeStores()
		return fmt.Errorf("初始化 Agent LLM 路由: %w", err)
	}
	agentLLMClient := llm.New(agentLLMConfig)
	recorder := llm.NewRecorder(st)
	compiledModelResolver, err := llm.NewRuntimeModelResolverV1(llm.RuntimeModelRouteV1{
		Provider: runtimepolicy.ModelProviderDeepSeekV1,
		Endpoint: runtimepolicy.EndpointRefV1{
			ID:         runtimepolicy.EndpointIDDeepSeekCompatiblePrimaryV1,
			Generation: cfg.LLM.CompiledEndpointGeneration,
		},
		CredentialRef: runtimepolicy.CredentialRefV1{
			ID:         runtimepolicy.CredentialIDLLMPrimaryV1,
			Generation: cfg.LLM.CompiledCredentialGeneration,
		},
		Client: llmClient,
	})
	if err != nil {
		closeStores()
		return fmt.Errorf("初始化 compiled LLM 路由: %w", err)
	}

	// 飞书 Manager：凭证存 settings 表而非 config——用户在 Dashboard 向导中填入。
	// 先构造不 Start：推送管道（pusher）要用它做主动发卡的出口；WS 连接推迟到
	// worker/scheduler 就绪后再拉起（B10 装配顺序），保证首个定时触发时出口已备好。
	manager := feishu.NewManager(st, llmClient, recorder)

	// Temporal 客户端：worker 与 HTTP server 同进程。Temporal 是 M3 推送管道的核心，
	// 连不上则拒绝启动（而非降级）——定时/立即推送都依赖它，静默半可用只会掩盖故障。
	temporalClient, err := client.Dial(client.Options{
		HostPort:  cfg.Temporal.Host,
		Namespace: cfg.Temporal.Namespace,
	})
	if err != nil {
		closeStores()
		return fmt.Errorf("连接 Temporal(%s): %w", cfg.Temporal.Host, err)
	}

	// 组装推送管道各步依赖，注入 Activities（所有 I/O 只在 activity 内做，满足确定性约束）。
	// hints 由 scorer 与 cardgen 共享同一实例（M5 契约 §13）：同一 trace 内两者拿到
	// 同一份画像快照——卡片"为什么与你有关"与打分依据必须是同一个画像。
	// st 作为 fetcher.SeenChecker 注入：TikHub 详情补全按次计费，只为未入库的新笔记
	// 付费（见 fetcher.SeenChecker）。传真实 store 而非 nil，否则补全整体被跳过。
	// st 同时作为 BindingCallRecorder：绑定引擎每次上游调用落 tool_calls（契约 §5）。
	fetch := fetcher.NewMulti(cfg.Fetch, st, st)
	hints := profilehint.NewCache(st)
	score := scorer.New(llmClient, recorder, st, hints)
	cards := cardgen.New(llmClient, recorder, hints)
	push := pusher.New(manager)
	if shouldEnableResearchV3Delivery(cfg) {
		delivery, deliveryErr := workflow.NewReceiptBackedResearchDeliveryV3(
			researchControlStore, push,
			newResearchV3DeliveryTargetResolver(researchControlStore, manager),
			renderResearchBriefCardV3,
		)
		if deliveryErr != nil {
			temporalClient.Close()
			closeStores()
			return fmt.Errorf("初始化 research V3 receipt-backed delivery: %w", deliveryErr)
		}
		researchDeliveryOption = workflow.WithResearchDeliveryV3(delivery)
	}
	// 构卡函数注入而非 workflow 直接 import feishu：feishu→agent→workflow 依赖链
	// 已存在，直接调用会成环（M5 契约 §8.2）。
	ev := evolver.New(llmClient, recorder, st)
	ev.SetTaskPolicySuggestionNotifier(func(
		ctx context.Context,
		tenantID, userID, deliveryID int64,
		claimToken string,
	) (evolver.TaskPolicySuggestionNotification, error) {
		openID, err := st.GetUserFeishuOpenID(ctx, userID)
		if err != nil {
			return evolver.TaskPolicySuggestionNotification{
				DefinitelyNotSent: true,
			}, err
		}
		dispatch, err := st.BeginTaskPolicySuggestionDispatch(
			ctx, tenantID, userID, claimToken,
		)
		if err != nil {
			return evolver.TaskPolicySuggestionNotification{
				DefinitelyNotSent: true,
			}, err
		}
		if !dispatch {
			return evolver.TaskPolicySuggestionNotification{
				Suppressed: true, DefinitelyNotSent: true,
			}, nil
		}
		observation, err := manager.SendCardWithUUIDResult(
			ctx, manager.AppIdentity(), openID, feishu.BuildReplyCard(
				fmt.Sprintf(
					"你反馈推送 #%d 过时，但该任务还没有明确的新鲜度窗口。"+
						"请回复我希望采用的范围（例如“只看相邻两次 9 点之间”）；"+
						"我会按你补充的范围直接修改任务；若语义仍有歧义会继续追问。",
					deliveryID,
				)), claimToken)
		if err == nil &&
			observation.Disposition != pusheffect.AttemptSent {
			err = fmt.Errorf(
				"任务策略建议发送返回非 sent 状态：%s",
				observation.Disposition)
		}
		return evolver.TaskPolicySuggestionNotification{
			MessageID: observation.MessageID,
			DefinitelyNotSent: observation.Disposition ==
				pusheffect.AttemptDefiniteNotSent,
		}, err
	})
	// buildNotice=feishu.BuildReplyCard：抓取失败告警走无按钮的普通卡（功能 5.2），
	// 与 buildCard（带反馈按钮的 delivery 卡）分开注入，不碰 M5 卡片反馈路径。
	activities := workflow.NewActivities(fetch, score, cards, push, st, manager, ev,
		feishu.BuildReplyCard,
		feishu.BuildAggregateCard, feishu.AggHeaderForTask,
		workflow.WithPlaybookPromptPolicy(cfg.Pipeline.PlaybookPromptsEnabled,
			cfg.Pipeline.PlaybookPromptCanaryScheduleID),
		workflow.WithCompiledRuntimeV1(st,
			func(ctx context.Context, tenantID int64, taskInstructionEnabled bool) (runtimepolicy.BundleV1, error) {
				quota, err := st.LoadQuotaRule(ctx, tenantID, store.QuotaLLMTokens)
				if err != nil {
					return runtimepolicy.BundleV1{}, err
				}
				_ = quota // readiness check; rate/burst/balance remain live authorization state.
				return runtimeconfig.BuildCurrentCompiledV1(runtimeconfig.CurrentCompiledV1Input{
					Model:                      cfg.LLM.Model,
					TaskInstructionEnabled:     taskInstructionEnabled,
					ModelEndpointGeneration:    cfg.LLM.CompiledEndpointGeneration,
					ModelCredentialGeneration:  cfg.LLM.CompiledCredentialGeneration,
					ExaCredentialGeneration:    cfg.Fetch.CompiledExaCredentialGeneration,
					TikHubCredentialGeneration: cfg.Fetch.CompiledTikHubCredentialGeneration,
				})
			}, compiledModelResolver),
		workflow.WithCompiledToolRuntimeV2(st,
			func(ctx context.Context, tenantID int64, taskInstructionEnabled bool) (runtimepolicy.BundleV1, error) {
				quota, err := st.LoadQuotaRule(ctx, tenantID, store.QuotaLLMTokens)
				if err != nil {
					return runtimepolicy.BundleV1{}, err
				}
				_ = quota
				return runtimeconfig.BuildStructuredInsightCompiledV1(runtimeconfig.CurrentCompiledV1Input{
					Model:                      cfg.LLM.Model,
					TaskInstructionEnabled:     taskInstructionEnabled,
					ModelEndpointGeneration:    cfg.LLM.CompiledEndpointGeneration,
					ModelCredentialGeneration:  cfg.LLM.CompiledCredentialGeneration,
					ExaCredentialGeneration:    cfg.Fetch.CompiledExaCredentialGeneration,
					TikHubCredentialGeneration: cfg.Fetch.CompiledTikHubCredentialGeneration,
				})
			}, compiledModelResolver),
		workflow.WithStructuredInsightRuntimeV1(
			func(ctx context.Context, tenantID int64, taskInstructionEnabled bool) (runtimepolicy.BundleV1, error) {
				quota, err := st.LoadQuotaRule(ctx, tenantID, store.QuotaLLMTokens)
				if err != nil {
					return runtimepolicy.BundleV1{}, err
				}
				_ = quota
				return runtimeconfig.BuildStructuredInsightCompiledV1(
					runtimeconfig.CurrentCompiledV1Input{
						Model:                      cfg.LLM.Model,
						TaskInstructionEnabled:     taskInstructionEnabled,
						ModelEndpointGeneration:    cfg.LLM.CompiledEndpointGeneration,
						ModelCredentialGeneration:  cfg.LLM.CompiledCredentialGeneration,
						ExaCredentialGeneration:    cfg.Fetch.CompiledExaCredentialGeneration,
						TikHubCredentialGeneration: cfg.Fetch.CompiledTikHubCredentialGeneration,
					})
			}),
		workflow.WithExecutiveBriefRuntimeV1(
			func(ctx context.Context, tenantID int64, taskInstructionEnabled bool) (runtimepolicy.BundleV1, error) {
				quota, err := st.LoadQuotaRule(ctx, tenantID, store.QuotaLLMTokens)
				if err != nil {
					return runtimepolicy.BundleV1{}, err
				}
				_ = quota
				return runtimeconfig.BuildExecutiveBriefCompiledV1(
					runtimeconfig.CurrentCompiledV1Input{
						Model:                      cfg.LLM.Model,
						TaskInstructionEnabled:     taskInstructionEnabled,
						ModelEndpointGeneration:    cfg.LLM.CompiledEndpointGeneration,
						ModelCredentialGeneration:  cfg.LLM.CompiledCredentialGeneration,
						ExaCredentialGeneration:    cfg.Fetch.CompiledExaCredentialGeneration,
						TikHubCredentialGeneration: cfg.Fetch.CompiledTikHubCredentialGeneration,
					})
			}, st, recorder),
		workflow.WithRunOutcomeStoreV1(st),
		workflow.WithRunOutcomeStoreV2(st),
		workflow.WithCanonicalBriefStoreV1(st),
		workflow.WithCanonicalBriefRendererV1(
			cfg.Pipeline.CanonicalBriefRendererCanaryScheduleID,
			cfg.Dashboard.Origin,
		),
		workflow.WithExecutiveBriefRendererV1(
			cfg.Pipeline.ExecutiveBriefRendererCanaryScheduleID),
		workflow.WithSnapshotV2ShadowCanary(
			st, cfg.Pipeline.SnapshotV2ShadowCanaryScheduleID),
		workflow.WithSnapshotV2ReadAuditCanary(
			st, cfg.Pipeline.SnapshotV2ReadAuditCanaryScheduleID),
		workflow.WithObservationRuntime(
			st, eventqualifier.New(recorder),
			cfg.Pipeline.ObservationShadowCanaryScheduleID,
			cfg.Pipeline.ObservationAuthorityCanaryScheduleID),
		workflow.WithPushEffectCanary(
			st, cfg.Pipeline.PushEffectCanaryScheduleID),
		researchRuntimeOption,
		researchDeliveryOption)
	periodicActivities, err := periodicbrief.NewActivities(
		st, compiledModelResolver, recorder,
		manager, cfg.Dashboard.Origin,
		cfg.Pipeline.ExecutiveBriefRendererCanaryScheduleID)
	if err != nil {
		temporalClient.Close()
		closeStores()
		return fmt.Errorf("装配周期 Brief Activities: %w", err)
	}
	slog.Info("task playbook prompt policy configured",
		"enabled", cfg.Pipeline.PlaybookPromptsEnabled,
		"canary_schedule_id", cfg.Pipeline.PlaybookPromptCanaryScheduleID,
		"allow_all", cfg.Pipeline.PlaybookPromptsAllowAll)
	slog.Info("observation rollout configured",
		"shadow_task_id",
		cfg.Pipeline.ObservationShadowCanaryScheduleID,
		"authority_task_id",
		cfg.Pipeline.ObservationAuthorityCanaryScheduleID)

	// worker：非阻塞 Start，关停时 Stop()（见下方顺序关停）。显式 stop timeout
	// 保证 Stop 不会采用 SDK 的 0 秒默认值、在 Activity 仍收尾时就释放 DB/Temporal。
	w := worker.New(temporalClient, cfg.Temporal.TaskQueue, temporalWorkerOptions())
	w.RegisterWorkflow(workflow.ResearchShadowWorkflowV3)
	w.RegisterWorkflow(workflow.ResearchScheduledWorkflowV3)
	w.RegisterWorkflow(workflow.AgentFirstRetentionClockWorkflowV1)
	w.RegisterWorkflow(periodicbrief.WorkflowV1)
	// Only V3 research activities remain registered in the production worker.
	// V1/V2 implementations stay in source solely for retained-history replay and
	// decoding; after the retention gate they must not be executable by a live
	// worker. workflow/registration_test.go pins both sides of this inventory.
	w.RegisterActivity(activities.PrepareResearchRunV3)
	w.RegisterActivity(activities.PlanResearchRunV3)
	w.RegisterActivity(activities.ExecuteResearchStepV3)
	w.RegisterActivity(activities.SynthesizeResearchBriefV3)
	w.RegisterActivity(activities.DeliverResearchBriefV3)
	w.RegisterActivity(periodicActivities.SynthesizePeriodicBriefV1)
	w.RegisterActivity(periodicActivities.DeliverPeriodicBriefV1)

	// scheduler 是唯一直接碰 SDK client 的调度封装（供 API 建/删/触发调度）。
	sched := scheduler.New(temporalClient, cfg.Temporal.TaskQueue, st,
		scheduler.WithTaskScheduleNamespace(cfg.Temporal.Namespace),
		scheduler.WithCompiledRuntimeRollout(
			cfg.Pipeline.CompiledRuntimeEnabled,
			cfg.Pipeline.CompiledRuntimeCanaryScheduleID,
			cfg.Pipeline.CompiledRuntimeAllowAll,
		),
		scheduler.WithCompiledToolRuntimeCanary(
			cfg.Pipeline.ToolRuntimeCanaryScheduleID,
		),
		scheduler.WithResearchRuntimeV3ShadowCanary(
			cfg.Pipeline.ResearchV3ShadowCanaryScheduleID,
		),
		scheduler.WithResearchRuntimeV3AuthorityCanary(
			cfg.Pipeline.ResearchV3AuthorityCanaryScheduleID,
		),
		scheduler.WithRunOutcomeRollout(
			cfg.Pipeline.RunOutcomeEnabled,
			cfg.Pipeline.RunOutcomeCanaryScheduleID,
			cfg.Pipeline.RunOutcomeAllowAll,
		),
		scheduler.WithCanonicalBriefRollout(
			cfg.Pipeline.CanonicalBriefEnabled,
			cfg.Pipeline.CanonicalBriefCanaryScheduleID,
			cfg.Pipeline.CanonicalBriefAllowAll,
		),
		scheduler.WithStructuredInsightRollout(
			cfg.Pipeline.StructuredInsightEnabled,
			cfg.Pipeline.StructuredInsightCanaryScheduleID,
			cfg.Pipeline.StructuredInsightAllowAll,
			cfg.Pipeline.StructuredInsightRendererEnabled,
			cfg.Pipeline.StructuredInsightRendererCanaryScheduleID,
			cfg.Pipeline.StructuredInsightRendererAllowAll,
		),
		scheduler.WithStructuredEventEvidenceRollout(
			cfg.Pipeline.StructuredEventEvidenceEnabled,
			cfg.Pipeline.StructuredEventEvidenceCanaryScheduleID,
			cfg.Pipeline.StructuredEventEvidenceAllowAll,
			cfg.Pipeline.ObservationAuthorityCanaryScheduleID,
		),
		scheduler.WithExecutiveBriefRollout(
			cfg.Pipeline.ExecutiveBriefEnabled,
			cfg.Pipeline.ExecutiveBriefCanaryScheduleID,
			cfg.Pipeline.ExecutiveBriefAllowAll,
		))
	periodicCoordinator, err := periodicbrief.NewCoordinator(
		st, temporalClient, cfg.Temporal.TaskQueue,
		cfg.Pipeline.ExecutiveBriefEnabled,
		cfg.Pipeline.ExecutiveBriefCanaryScheduleID,
		cfg.Pipeline.ExecutiveBriefAllowAll,
		slog.Default())
	if err != nil {
		temporalClient.Close()
		closeStores()
		return fmt.Errorf("装配周期 Brief coordinator: %w", err)
	}
	periodicRecoveryRunner, err := periodicbrief.NewRecoveryRunner(
		st, temporalClient, manager, cfg.Dashboard.Origin,
		cfg.Pipeline.ExecutiveBriefRendererCanaryScheduleID,
		slog.Default())
	if err != nil {
		temporalClient.Close()
		closeStores()
		return fmt.Errorf("装配周期 Brief recovery: %w", err)
	}
	creationCoordinator := task.NewResearchCreationCoordinatorV3(
		legacyStore, sched, slog.Default(), nativeResearchV3CreationPolicy(),
	)
	var researchDefinitionEditCoordinator *task.ResearchTaskDefinitionEditCoordinatorV3
	if researchControlStore != nil {
		researchDefinitionEditCoordinator = task.NewResearchTaskDefinitionEditCoordinatorV3(
			researchControlStore, sched, slog.Default())
	}

	commandRoleGateCtx, cancelCommandRoleGate := context.WithTimeout(
		ctx, 10*time.Second,
	)
	commandRoleGateErr := st.ValidateScheduleCommandRuntimeRole(
		commandRoleGateCtx,
	)
	cancelCommandRoleGate()
	if commandRoleGateErr != nil {
		temporalClient.Close()
		closeStores()
		return fmt.Errorf("任务命令受限角色 Gate: %w", commandRoleGateErr)
	}
	if researchDefinitionEditCoordinator != nil {
		if err := researchDefinitionEditCoordinator.RecoverStaleOnceV3(ctx); err != nil {
			temporalClient.Close()
			closeStores()
			return fmt.Errorf("Research V3 任务定义编辑首轮恢复 Gate: %w", err)
		}
	}
	scheduleCommandStartupCtx, cancelScheduleCommandStartup :=
		context.WithTimeout(
			ctx, scheduler.ScheduleCommandRecoveryPassTimeout,
		)
	scheduleCommandStartupErr :=
		sched.RecoverScheduleCommandsOnce(scheduleCommandStartupCtx)
	cancelScheduleCommandStartup()
	if scheduleCommandStartupErr != nil {
		temporalClient.Close()
		closeStores()
		return fmt.Errorf(
			"任务命令首轮恢复 Gate（%s budget）: %w",
			scheduler.ScheduleCommandRecoveryPassTimeout,
			scheduleCommandStartupErr,
		)
	}
	if err := sched.ReconcileActions(ctx); err != nil {
		temporalClient.Close()
		closeStores()
		return fmt.Errorf("scheduler: 存量调度 Action reconcile 安全 Gate: %w", err)
	}

	// P1-B recovery is rollout-independent: disabling new marker creation must
	// never strand pending rows created by an earlier deployment.
	outcomeRecoveryRunner, err := runoutcome.NewRunner(
		st,
		runoutcome.TemporalInspector{Client: temporalClient},
		slog.Default(),
	)
	if err != nil {
		temporalClient.Close()
		closeStores()
		return fmt.Errorf("装配 run outcome recovery: %w", err)
	}
	if err := outcomeRecoveryRunner.RunStartup(ctx); err != nil {
		temporalClient.Close()
		closeStores()
		return fmt.Errorf("run outcome recovery 首轮恢复 Gate: %w", err)
	}
	executiveRecoveryRunner, err :=
		executivebriefrecovery.NewRunner(st, slog.Default())
	if err != nil {
		temporalClient.Close()
		closeStores()
		return fmt.Errorf("装配 executive Brief recovery: %w", err)
	}
	if err := executiveRecoveryRunner.RunStartup(ctx); err != nil {
		temporalClient.Close()
		closeStores()
		return fmt.Errorf(
			"executive Brief recovery 首轮恢复 Gate: %w", err)
	}
	if err := periodicCoordinator.RunStartup(ctx); err != nil {
		temporalClient.Close()
		closeStores()
		return fmt.Errorf("周期 Brief coordinator 首轮 Gate: %w", err)
	}
	if err := periodicRecoveryRunner.RunStartup(ctx); err != nil {
		temporalClient.Close()
		closeStores()
		return fmt.Errorf("周期 Brief recovery 首轮 Gate: %w", err)
	}

	// Compiled recovery retains its exact-task switch. Research V3 recovery is
	// discovered from enabled database authorities, so cutover tasks remain
	// recoverable after a transient operator canary disappears.
	var pushRecoveryRunner *pushrecovery.Runner
	var authorityPushRecoveryRunner *pushrecovery.AuthorityRunner
	if cfg.Pipeline.PushEffectRecoveryCanaryScheduleID != "" ||
		cfg.Pipeline.ResearchV3RuntimeEnabled {
		outboundCtx, cancelOutbound := context.WithTimeout(ctx, 30*time.Second)
		outboundErr := manager.PrepareOutbound(outboundCtx)
		cancelOutbound()
		if outboundErr != nil {
			temporalClient.Close()
			closeStores()
			return fmt.Errorf("push effect recovery 飞书发送端 Gate: %w",
				outboundErr)
		}
	}
	if cfg.Pipeline.PushEffectRecoveryCanaryScheduleID != "" {
		pushRecoveryCoordinator, err := pushrecovery.New(
			pushrecovery.Deps{
				Store: st, Sender: push, HistoryResolver: manager,
				Config: pushrecovery.Config{
					ExactTaskID: cfg.Pipeline.PushEffectRecoveryCanaryScheduleID,
				},
			},
		)
		if err != nil {
			temporalClient.Close()
			closeStores()
			return fmt.Errorf("装配 push effect recovery coordinator: %w",
				err)
		}
		pushRecoveryRunner, err = pushrecovery.NewRunner(
			pushrecovery.RunnerDeps{
				Store: st, Coordinator: pushRecoveryCoordinator,
				Config: pushrecovery.RunnerConfig{
					ExactTaskID: cfg.Pipeline.PushEffectRecoveryCanaryScheduleID,
				},
				Logger: slog.Default(),
			},
		)
		if err != nil {
			temporalClient.Close()
			closeStores()
			return fmt.Errorf("装配 push effect recovery lifecycle: %w", err)
		}
		if err := pushRecoveryRunner.RunStartup(ctx); err != nil {
			temporalClient.Close()
			closeStores()
			return fmt.Errorf("push effect recovery 首轮恢复 Gate: %w", err)
		}
	}
	if cfg.Pipeline.ResearchV3RuntimeEnabled {
		authorityPushRecoveryRunner, err = pushrecovery.NewAuthorityRunner(
			pushrecovery.AuthorityRunnerDeps{
				Store: st, Sender: push, HistoryResolver: manager,
				RunnerConfig:  pushrecovery.RunnerConfig{},
				Logger:        slog.Default(),
				ExcludeTaskID: cfg.Pipeline.PushEffectRecoveryCanaryScheduleID,
			},
		)
		if err != nil {
			temporalClient.Close()
			closeStores()
			return fmt.Errorf("装配 V3 authority push recovery: %w", err)
		}
		if err := authorityPushRecoveryRunner.RunStartup(ctx); err != nil {
			temporalClient.Close()
			closeStores()
			return fmt.Errorf("V3 authority push recovery 首轮恢复 Gate: %w", err)
		}
	}
	var maintenanceWG sync.WaitGroup
	runMaintenance := func(fn func()) {
		maintenanceWG.Add(1)
		go func() {
			defer maintenanceWG.Done()
			fn()
		}()
	}
	waitMaintenance := func(ctx context.Context) error {
		done := make(chan struct{})
		go func() {
			maintenanceWG.Wait()
			close(done)
		}()
		select {
		case <-done:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	// agent loop（M4 契约 §9 装配序：store → llm → scheduler → tools → agent.New →
	// manager 注入）：任务级立即运行依赖 scheduler，
	// 故装配在 scheduler 之后；注入须在 manager.Start 之前，保证 WS 连接建立时
	// 消息链已能走 agent 而非回退 chat_reply。
	// TikHub 端点工具面（端点注册表契约 §3）：key 未配置则不装配（endpoints=nil），
	// agent 退化为纯静态工具面——比装一个恒报"key 缺失"的检索工具干净。
	var endpoints *agent.EndpointTools
	if cfg.Fetch.TikhubAPIKey != "" {
		// 内联上限由 agent 模型声明的上下文窗口派生（llm/context.go）：模型换代
		// 时上限自动跟随，不再依赖有人记得改常量（6000 rune 失真一年的教训）。
		endpoints = agent.NewEndpointTools(tikhubinvoke.New(cfg.Fetch), st,
			cfg.Agent.EndpointDailyCap,
			llm.ContextWindowTokens(cfg.LLM.AgentModel))
	}
	// prober 传 fetch（*fetcher.Multi）而非 fetch.Binding()：1.5 起试跑=准入统一由
	// Multi.Probe 分派（绑定能力走绑定引擎，web/feed 与 web/contents 走各自 provider）。
	// Exa ad-hoc 工具对（web_search/read_page）：key 未配置则不装配（exaTools=nil），
	// 与 endpoints 同语义——不广告用不了的工具。
	var exaTools *agent.ExaTools
	if cfg.Fetch.ExaAPIKey != "" {
		exaTools = agent.NewExaTools(fetch.Exa(), fetch.ExaContents(), st,
			cfg.Agent.ExaDailyCap)
	}
	// Migration 074 removed the legacy confirmation/action roots. Owner task
	// writes now enter only through the native V3 manage_tasks dependencies below.
	authorizer := agent.NewModelOwnerActionAuthorizer(
		agentLLMClient, recorder, cfg.LLM.AgentModel,
	)
	// Owner chat and the Dashboard mutate the same per-user Agent session.
	// Their distinct Loop/catalog instances therefore share one admission
	// domain; the sessionless A2A Loop below intentionally does not receive it.
	sessionAdmission := agent.NewSessionAdmissionCoordinator()
	tools := agent.BuildOwnerTools(
		st,
		agent.ManageTasksDeps{
			Queries: st,
			Creator: agent.NewResearchTaskCreationV3Executor(
				creationCoordinator,
			),
			Editor: agent.NewResearchTaskDefinitionEditV3Executor(
				researchDefinitionEditCoordinator,
			),
			Runner: sched, Deleter: sched,
			Authorizer: authorizer,
		},
		authorizer, endpoints, exaTools,
	)
	agentLoop, err := agent.NewChecked(agent.Deps{
		Client:           agentLLMClient,
		Recorder:         recorder,
		Store:            st,
		Profiles:         st,
		Tools:            tools,
		Model:            cfg.LLM.AgentModel,
		MaxTurns:         cfg.Agent.MaxTurns,
		SessionTTL:       time.Duration(cfg.Agent.SessionTTLMinutes) * time.Minute,
		SessionAdmission: sessionAdmission,
		OwnerAgent:       true,
		Endpoints:        endpoints,
		ToolCalls:        agent.NewToolCallRecorder(st), // 工具调用记账（契约 §6，全量工具）
		Evidence:         st,
		TurnReplay:       st,
	})
	if err != nil {
		closeServerStartupResources(temporalClient.Close, closeStores)
		return fmt.Errorf("装配 Agent 工具注册表: %w", err)
	}
	manager.SetAgent(agentLoop)
	telegramManager, err := buildTelegramManager(cfg.Telegram, st, agentLoop)
	if err != nil {
		closeServerStartupResources(temporalClient.Close, closeStores)
		return fmt.Errorf("装配 Telegram Bot: %w", err)
	}

	// Build the A2A agent before any worker or recovery goroutine starts. A
	// composition error is then an ordinary startup failure: no admitted work
	// needs draining, and both Store boundaries can be closed immediately.
	var a2aLoop *agent.Loop
	if cfg.A2A.Enabled {
		a2aTools, filterErr := agent.FilterAuthorizedTools(
			agent.BuildPublicResearchTools(endpoints, exaTools),
			agent.AuthorizationA2AReadOnly,
		)
		if filterErr != nil {
			closeServerStartupResources(temporalClient.Close, closeStores)
			return fmt.Errorf("筛选 A2A Agent 工具: %w", filterErr)
		}
		var loopErr error
		a2aLoop, loopErr = agent.NewChecked(agent.Deps{
			Client:       agentLLMClient,
			Recorder:     recorder,
			Tools:        a2aTools,
			Model:        cfg.LLM.AgentModel,
			MaxTurns:     cfg.Agent.MaxTurns,
			SystemPrompt: a2a.ChatSystemPrompt,
			ToolCalls:    agent.NewToolCallRecorder(st), // 工具调用同样记账（契约 §6）
		})
		if loopErr != nil {
			closeServerStartupResources(temporalClient.Close, closeStores)
			return fmt.Errorf("装配 A2A Agent 工具注册表: %w", loopErr)
		}
	}

	// Only launch background database users after every fallible Agent
	// composition step has succeeded. Before this point startup failures can
	// close Temporal and both Stores without racing an admitted goroutine.
	runMaintenance(func() {
		executiveRecoveryRunner.Run(ctx)
	})
	runMaintenance(func() {
		periodicCoordinator.Run(ctx)
	})
	runMaintenance(func() {
		periodicRecoveryRunner.Run(ctx)
	})

	// 配额 reconcile（契约 §2.7）：给缺配额行的租户补齐默认额度。同样后台跑、幂等。
	//
	// 两种缺行都要靠它兜住，而且两种都是**静默的**：
	//   · 建租户时 seed 失败——那条路径刻意只记日志（塞进事务会让一次 seed 失败
	//     升级成整个注册失败），代价是用户注册"成功"却什么都用不了；
	//   · 迁移漏回填——025 第一版就漏了存量租户，配合"缺行即拒绝"上线即锁死推送，
	//     而下游把额度用尽当正常终态，Temporal 一片绿、零告警。
	// 缺行的后果是"这个租户什么都用不了"，它不该靠谁记得手工补。
	runMaintenance(func() {
		n, err := st.ReconcileTenantQuota(ctx)
		if err != nil {
			slog.Error("配额 reconcile 整体失败（不影响启动）", "err", err)
			return
		}
		if n > 0 {
			slog.Info("配额 reconcile 完成", "seeded_tenants", n)
		}
	})

	// 反馈服务（M5 契约 §13）：装在 agent 之后。普通态度/原因由 056 耐久投影；
	// 仅 deep_dive 成功送达后仍通过 Notifier=agentLoop 做 legacy best-effort 会话回调。
	// 同样须在 manager.Start 之前注入，否则 WS 连上后的首批 deep_dive 点击会
	// 落到 nil runner 上。
	// deep_dive 走质量档 AgentModel（Boss 拍板③）：用户显式请求、低频、长文质量敏感。
	fbSvc := feedback.New(feedback.Deps{
		Store:    st,
		Client:   agentLLMClient,
		Recorder: recorder,
		Sender:   manager,
		// 056 owns attitude/misjudged continuation. Deep-dive is explicitly
		// outside that outbox and still needs this legacy success callback.
		Notifier:        agentLoop,
		BuildCard:       feishu.BuildDeliveryCard,
		BuildAggCard:    feishu.BuildAggregateCard,
		DashboardOrigin: cfg.Dashboard.Origin,
		DeepDiveModel:   cfg.LLM.AgentModel,
		SessionTTL: time.Duration(
			cfg.Agent.SessionTTLMinutes) * time.Minute,
	})
	manager.SetFeedback(fbSvc)

	receiptDispatcher, err := task.NewCreationReceiptDispatcher(
		task.CreationReceiptDispatcherDeps{
			Store: st, Sessions: agentLoop, Logger: slog.Default(),
		},
	)
	if err != nil {
		stop()
		maintenanceCtx, cancelMaintenance := context.WithTimeout(context.Background(), 30*time.Second)
		maintenanceErr := waitMaintenance(maintenanceCtx)
		cancelMaintenance()
		if maintenanceErr != nil {
			return errors.Join(
				fmt.Errorf("装配任务创建耐久回执: %w", err),
				fmt.Errorf("排空启动维护任务: %w", maintenanceErr),
			)
		}
		temporalClient.Close()
		closeStores()
		return fmt.Errorf("装配任务创建耐久回执: %w", err)
	}
	definitionEditReceiptDispatcher, err :=
		task.NewDefinitionEditReceiptDispatcher(
			task.DefinitionEditReceiptDispatcherDeps{
				Store: st, Sessions: agentLoop, Logger: slog.Default(),
			},
		)
	if err != nil {
		stop()
		maintenanceCtx, cancelMaintenance := context.WithTimeout(
			context.Background(), 30*time.Second,
		)
		maintenanceErr := waitMaintenance(maintenanceCtx)
		cancelMaintenance()
		if maintenanceErr != nil {
			return errors.Join(
				fmt.Errorf("装配任务定义编辑耐久回执: %w", err),
				fmt.Errorf("排空启动维护任务: %w", maintenanceErr),
			)
		}
		temporalClient.Close()
		closeStores()
		return fmt.Errorf("装配任务定义编辑耐久回执: %w", err)
	}
	// Native V3 definition-edit recovery starts before any Feishu/HTTP ingress.
	// Retained V1/V2 journals are audit-only and have no production recovery
	// writer after the physical cleanup Gate.
	definitionEditRecoveryCtx, cancelDefinitionEditRecovery := context.WithCancel(ctx)
	definitionEditRecoveryDone := make(chan struct{})
	if researchDefinitionEditCoordinator == nil {
		close(definitionEditRecoveryDone)
	} else {
		go func() {
			defer close(definitionEditRecoveryDone)
			researchDefinitionEditCoordinator.RunRecoveryV3(
				definitionEditRecoveryCtx)
		}()
	}
	stopDefinitionEditRecovery := func() error {
		cancelDefinitionEditRecovery()
		select {
		case <-definitionEditRecoveryDone:
			return nil
		case <-time.After(30 * time.Second):
			return errors.New("task definition edit recovery 关停超时")
		}
	}

	scheduleCommandRecoveryCtx, cancelScheduleCommandRecovery :=
		context.WithCancel(ctx)
	scheduleCommandRecoveryDone := make(chan struct{})
	go func() {
		defer close(scheduleCommandRecoveryDone)
		sched.RunScheduleCommandRecovery(scheduleCommandRecoveryCtx)
	}()
	stopScheduleCommandRecovery := func() error {
		cancelScheduleCommandRecovery()
		select {
		case <-scheduleCommandRecoveryDone:
			return nil
		case <-time.After(30 * time.Second):
			return errors.New("schedule command recovery 关停超时")
		}
	}

	outcomeRecoveryCtx, cancelOutcomeRecovery := context.WithCancel(ctx)
	outcomeRecoveryDone := make(chan struct{})
	go func() {
		defer close(outcomeRecoveryDone)
		outcomeRecoveryRunner.Run(outcomeRecoveryCtx)
	}()
	stopOutcomeRecovery := func() error {
		cancelOutcomeRecovery()
		select {
		case <-outcomeRecoveryDone:
			return nil
		case <-time.After(30 * time.Second):
			return errors.New("run outcome recovery 关停超时")
		}
	}

	pushRecoveryCtx, cancelPushRecovery := context.WithCancel(ctx)
	pushRecoveryDone := make(chan struct{})
	if pushRecoveryRunner == nil && authorityPushRecoveryRunner == nil {
		close(pushRecoveryDone)
	} else {
		go func() {
			defer close(pushRecoveryDone)
			var recoveryWG sync.WaitGroup
			if pushRecoveryRunner != nil {
				recoveryWG.Go(func() { pushRecoveryRunner.Run(pushRecoveryCtx) })
			}
			if authorityPushRecoveryRunner != nil {
				recoveryWG.Go(func() {
					authorityPushRecoveryRunner.Run(pushRecoveryCtx)
				})
			}
			recoveryWG.Wait()
		}()
	}
	stopPushRecovery := func() error {
		cancelPushRecovery()
		select {
		case <-pushRecoveryDone:
			return nil
		case <-time.After(30 * time.Second):
			return errors.New("push effect recovery 关停超时")
		}
	}

	// Native V3 creation recovery starts before ingress and never enumerates or
	// acquires retained V1 creation journals.
	creationRecoveryCtx, cancelCreationRecovery := context.WithCancel(ctx)
	creationRecoveryDone := make(chan struct{})
	go func() {
		defer close(creationRecoveryDone)
		creationCoordinator.RunRecoveryV3(creationRecoveryCtx)
	}()
	stopCreationRecovery := func() error {
		cancelCreationRecovery()
		select {
		case <-creationRecoveryDone:
			return nil
		case <-time.After(30 * time.Second):
			return errors.New("task creation recovery 关停超时")
		}
	}

	// Both durable outboxes start before every ingress. Terminal operation and
	// receipt identity are one transaction; neither dispatcher reconstructs
	// user-visible content from live configuration.
	receiptDispatchCtx, cancelReceiptDispatch := context.WithCancel(ctx)
	receiptDispatchDone := make(chan struct{})
	go func() {
		defer close(receiptDispatchDone)
		receiptDispatcher.Run(receiptDispatchCtx)
	}()
	stopReceiptDispatch := func() error {
		cancelReceiptDispatch()
		select {
		case <-receiptDispatchDone:
			return nil
		case <-time.After(30 * time.Second):
			return errors.New("task creation receipt dispatcher 关停超时")
		}
	}
	definitionEditReceiptDispatchCtx, cancelDefinitionEditReceiptDispatch :=
		context.WithCancel(ctx)
	definitionEditReceiptDispatchDone := make(chan struct{})
	go func() {
		defer close(definitionEditReceiptDispatchDone)
		definitionEditReceiptDispatcher.Run(
			definitionEditReceiptDispatchCtx,
		)
	}()
	stopDefinitionEditReceiptDispatch := func() error {
		cancelDefinitionEditReceiptDispatch()
		select {
		case <-definitionEditReceiptDispatchDone:
			return nil
		case <-time.After(30 * time.Second):
			return errors.New(
				"task definition edit receipt dispatcher 关停超时",
			)
		}
	}

	// The worker is an ingress too: a registered task queue can immediately
	// receive scheduled runs. Start it only after every configured recovery
	// loop and terminal outbox consumer has started.
	if err := w.Start(); err != nil {
		stop()
		recoveryErr := stopCreationRecovery()
		definitionEditRecoveryErr := stopDefinitionEditRecovery()
		scheduleCommandRecoveryErr := stopScheduleCommandRecovery()
		outcomeRecoveryErr := stopOutcomeRecovery()
		pushRecoveryErr := stopPushRecovery()
		creationReceiptErr := stopReceiptDispatch()
		definitionEditReceiptErr :=
			stopDefinitionEditReceiptDispatch()
		maintenanceCtx, cancelMaintenance := context.WithTimeout(
			context.Background(), 30*time.Second,
		)
		maintenanceErr := waitMaintenance(maintenanceCtx)
		cancelMaintenance()
		drainErr := errors.Join(
			recoveryErr, definitionEditRecoveryErr,
			scheduleCommandRecoveryErr,
			outcomeRecoveryErr,
			pushRecoveryErr,
			creationReceiptErr, definitionEditReceiptErr,
			maintenanceErr,
		)
		if drainErr != nil {
			return errors.Join(
				fmt.Errorf("启动 Temporal worker: %w", err),
				fmt.Errorf(
					"worker 启动失败后未能安全排空: %w", drainErr,
				),
			)
		}
		temporalClient.Close()
		closeStores()
		return fmt.Errorf("启动 Temporal worker: %w", err)
	}

	// 依赖就绪后再拉飞书 WS 连接：无配置静默待命，ctx 取消时断开。
	manager.Start(ctx)

	// Stop ingress first, then drain every nested background layer before the
	// resources they use. Agent session writes are drained separately after the
	// A6 receipt dispatcher, because the dispatcher deliberately shares the
	// same per-user conversation lock.
	drainIngress := func() error {
		var drainErrs []error
		if telegramErr := shutdownTelegramIngress(
			telegramManager, 2*time.Minute+10*time.Second); telegramErr != nil {
			drainErrs = append(drainErrs, telegramErr)
		}
		managerCtx, cancelManager := context.WithTimeout(context.Background(), 50*time.Second)
		managerErr := manager.Shutdown(managerCtx)
		cancelManager()
		if managerErr != nil {
			drainErrs = append(drainErrs, fmt.Errorf("排空飞书回调: %w", managerErr))
		}

		feedbackCtx, cancelFeedback := context.WithTimeout(context.Background(), feedbackShutdownTimeout)
		feedbackErr := fbSvc.Shutdown(feedbackCtx)
		cancelFeedback()
		if feedbackErr != nil {
			drainErrs = append(drainErrs, fmt.Errorf("排空反馈后台任务: %w", feedbackErr))
		}

		return errors.Join(drainErrs...)
	}
	drainAgentSessions := func() error {
		sessionCtx, cancelSessions := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancelSessions()
		if err := agentLoop.DrainSessionWrites(sessionCtx); err != nil {
			return fmt.Errorf("排空 Agent 会话回写: %w", err)
		}
		return nil
	}
	if err := telegramManager.Start(ctx); err != nil {
		stop()
		drainErr := drainIngress()
		recoveryErr := stopCreationRecovery()
		definitionEditRecoveryErr := stopDefinitionEditRecovery()
		scheduleCommandRecoveryErr := stopScheduleCommandRecovery()
		outcomeRecoveryErr := stopOutcomeRecovery()
		pushRecoveryErr := stopPushRecovery()
		receiptErr := stopReceiptDispatch()
		definitionEditReceiptErr := stopDefinitionEditReceiptDispatch()
		sessionErr := drainAgentSessions()
		maintenanceCtx, cancelMaintenance := context.WithTimeout(
			context.Background(), 30*time.Second)
		maintenanceErr := waitMaintenance(maintenanceCtx)
		cancelMaintenance()
		if joined := errors.Join(
			drainErr, recoveryErr, definitionEditRecoveryErr,
			scheduleCommandRecoveryErr, outcomeRecoveryErr,
			pushRecoveryErr, receiptErr, definitionEditReceiptErr,
			sessionErr, maintenanceErr,
		); joined != nil {
			return errors.Join(
				fmt.Errorf("启动 Telegram Bot: %w", err), joined)
		}
		w.Stop()
		temporalClient.Close()
		closeStores()
		return fmt.Errorf("启动 Telegram Bot: %w", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", handleHealthz)
	readinessStores := []readyzStore{st}
	if researchControlStore != nil {
		// The control Store Ping covers both its vane_server_runtime control pool
		// and the independently authenticated research executor pool.
		readinessStores = append(readinessStores, researchControlStore)
	}
	readinessStores = appendTelegramReadiness(
		readinessStores, cfg.Telegram.Enabled, telegramManager)
	mux.HandleFunc("GET /readyz", handleReadyz(readinessStores...))
	// principal 解析器：全系统唯一的 principal 来源（企业级契约 §1.1，不变量 I-A1）。
	// 过渡期实现是「全局 owner 回退 + 租户恒为 1」，行为与收敛前逐字一致；
	// 真实认证落地后只换这一处构造，api/a2a/gate 三处调用点零改动。
	principals := auth.NewOwnerResolver(st, feishu.SettingKeyOwner)

	api.Mount(mux, api.Deps{
		Store:         st,
		Auth:          st,
		Manager:       manager,
		Scheduler:     sched,
		TaskAgent:     agentLoop,
		Telegram:      telegramManager,
		BriefFeedback: fbSvc,
		ExecutiveBriefWebCanaryScheduleID: cfg.Pipeline.
			ExecutiveBriefWebCanaryScheduleID,
		ExecutiveBriefWebProjectionAllowAll: cfg.Pipeline.
			ExecutiveBriefWebProjectionAllowAll,
		// HTTP 面的 principal 来自会话中间件注入的 ctx（企业级契约 §1.1 的最终形态）；
		// a2a/gate 无 HTTP 会话，仍用 owner 回退——这正是把 principal 做成接口的价值。
		Principal: auth.NewContextResolver(),
		Origin:    cfg.Dashboard.Origin,
	})
	mountTelegramWebhook(mux, cfg.Telegram.Enabled, telegramManager.Handler())

	// A2A server（a2a-contract §7）：enabled=false 时不 Mount——/a2a 与
	// agent-card 路径在 mux 上根本不存在（404），零新增暴露面。
	var a2aRuntime *a2a.Runtime
	if cfg.A2A.Enabled {
		// 启动时清理上次进程遗留的滞留任务（对抗审查 A-1）：assistant.chat 跑在 SDK
		// 后台 goroutine，重启硬杀会让在飞任务永久停在 WORKING，轮询终态的对端挂死。
		// 此刻本进程尚未接流量，因此数据库里所有非终态任务都属于上个进程，
		// 无论只遗留 1 秒还是 15 分钟都已没有执行者，必须立即置 FAILED。
		// 失败只记日志不拒启动：清账是尽力而为，不该拖垮服务拉起。
		cleanupCtx, cancelCleanup := context.WithTimeout(ctx, a2aStartupCleanupTimeout)
		n, cleanupErr := st.FailStaleA2ATasks(cleanupCtx, time.Now())
		cancelCleanup()
		if cleanupErr != nil {
			slog.Warn("a2a: 清理滞留任务失败（不阻塞启动）", "err", cleanupErr)
		} else if n > 0 {
			slog.Info("a2a: 已清理上次进程遗留的滞留任务", "count", n)
		}

		a2aRuntime, err = a2a.Mount(mux, a2a.Deps{
			Storage:   st,
			Content:   st,
			Chat:      a2aLoop,
			Principal: principals,
			Token:     cfg.A2A.Token,
			BaseURL:   cfg.A2A.BaseURL,
			Version:   vaneVersion,
		})
		if err != nil {
			stop()
			drainErr := drainIngress()
			recoveryErr := stopCreationRecovery()
			definitionEditRecoveryErr := stopDefinitionEditRecovery()
			scheduleCommandRecoveryErr := stopScheduleCommandRecovery()
			outcomeRecoveryErr := stopOutcomeRecovery()
			pushRecoveryErr := stopPushRecovery()
			receiptErr := stopReceiptDispatch()
			definitionEditReceiptErr :=
				stopDefinitionEditReceiptDispatch()
			sessionErr := drainAgentSessions()
			maintenanceCtx, cancelMaintenance := context.WithTimeout(context.Background(), 30*time.Second)
			maintenanceErr := waitMaintenance(maintenanceCtx)
			cancelMaintenance()
			if drainErr != nil || recoveryErr != nil ||
				definitionEditRecoveryErr != nil ||
				scheduleCommandRecoveryErr != nil ||
				outcomeRecoveryErr != nil ||
				pushRecoveryErr != nil ||
				receiptErr != nil || definitionEditReceiptErr != nil ||
				sessionErr != nil ||
				maintenanceErr != nil {
				// An admitted callback may still own DB/Temporal work. Do not close
				// those resources underneath it; returning exits the process.
				return errors.Join(fmt.Errorf("挂载 A2A server: %w", err),
					drainErr, recoveryErr, definitionEditRecoveryErr,
					scheduleCommandRecoveryErr, outcomeRecoveryErr,
					pushRecoveryErr, receiptErr,
					definitionEditReceiptErr,
					sessionErr, maintenanceErr)
			}
			w.Stop()
			temporalClient.Close()
			closeStores()
			return fmt.Errorf("挂载 A2A server: %w", err)
		}
	}

	srv := &http.Server{
		Addr:              cfg.Server.Addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second, // 不设则空闲 keep-alive 连接永不回收
	}

	// 过期会话清理（安全审查发现：migration 019 为「清理任务」专门建了
	// idx_user_sessions_expires 索引，而那个任务从未接线——DeleteExpiredSessions
	// 全树只有测试在调，生产里过期行永不回收）。
	// 用最朴素的 ticker：清理是幂等的纯删除，不值得为它引入 Temporal workflow。
	runMaintenance(func() { runSessionCleanup(ctx, st) })

	// gate 探针每日巡检（2026-07-19 Boss 拍板做进服务内）：每天 01:30 UTC（北京 09:30，
	// 窗口覆盖 00:30 UTC 早报批次）+ 每次启动后 3 分钟各跑一轮 probe.Run，红灯或探针
	// 自身故障时给 owner 发飞书告警卡——§16.1 曾挂红半天没人知道，靠人工 SSH 跑 gate
	// 才发现。同上：只读旁路巡检不值得引入 Temporal，丢一轮无害（次日自动再跑）。
	// push 复用推送管道的 Pusher 出口，principals 与 gate CLI 同走 owner 回退解析。
	// st 传两次：第一份是 probe.Store（读指标），第二份是 fingerprintStore（告警
	// 指纹落盘，migration 027）——两个独立职责窄接口，生产实现恰好都是 *store.Store。
	runMaintenance(func() {
		newProbeWatcher(st, st, principals, manager, push, feishu.BuildReplyCard).run(ctx)
	})

	serveErr := make(chan error, 1)
	go func() {
		slog.Info("HTTP 服务启动", "addr", cfg.Server.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErr <- err
		}
	}()

	var serveFailure error
	select {
	case <-ctx.Done():
		slog.Info("收到关停信号，开始优雅关停")
	case err := <-serveErr:
		serveFailure = err
		// 统一进入下方逆序拆栈，不在错误分支绕过 Manager/Agent drain。
		stop()
	}

	// 顺序关停（契约 B10）：HTTP 关准入 → 独立入口并行排空 → 创建/定义编辑恢复器/
	// 维护任务 → worker → Temporal client → DB。任一排空没有得到肯定结果，
	// 都不得主动释放共享依赖。
	var shutdownErrs []error
	// Shutdown closes listeners before waiting. Run it concurrently so a valid
	// 120s synchronous A2A handler can be canceled/drained by its runtime rather
	// than turning the initial 5s wait into a latched shutdown failure.
	httpShutdown := beginHTTPShutdown(srv, httpInitialShutdownTimeout)

	// A2A SDK 会把 ReturnImmediately/CancelTask 执行脱离 HTTP request context。
	// Runtime 在 HTTP 关准入后独立关闸并排空；与飞书/反馈并行，避免把 120s
	// chat 预算串加到总关停时长。Cleaner 返回前 task-store 终态写仍未完成。
	var a2aDrain <-chan error
	if a2aRuntime != nil {
		done := make(chan error, 1)
		a2aDrain = done
		go func() {
			a2aCtx, cancelA2A := context.WithTimeout(context.Background(), a2aShutdownTimeout)
			defer cancelA2A()
			done <- a2aRuntime.Shutdown(a2aCtx)
		}()
	}

	if err := drainIngress(); err != nil {
		shutdownErrs = append(shutdownErrs, err)
	}
	if a2aDrain != nil {
		if err := <-a2aDrain; err != nil {
			shutdownErrs = append(shutdownErrs, fmt.Errorf("排空 A2A 后台执行: %w", err))
		}
	}
	// 停创建恢复器：不再发 Temporal/DB 请求，等在途 operation 收束或超时。
	if err := stopCreationRecovery(); err != nil {
		shutdownErrs = append(shutdownErrs, fmt.Errorf("排空创建恢复器: %w", err))
	}
	// C2b3-2c 定义编辑仍无新准入，但恢复器可能正持有 Store fence
	// 或在执行 Temporal exact-CAS；必须在关闭两个依赖前得到它的退出证明。
	if err := stopDefinitionEditRecovery(); err != nil {
		shutdownErrs = append(shutdownErrs, fmt.Errorf("排空任务定义编辑恢复器: %w", err))
	}
	if err := stopScheduleCommandRecovery(); err != nil {
		shutdownErrs = append(
			shutdownErrs, fmt.Errorf("排空任务命令恢复器: %w", err),
		)
	}
	if err := stopOutcomeRecovery(); err != nil {
		shutdownErrs = append(
			shutdownErrs, fmt.Errorf("排空 run outcome recovery: %w", err),
		)
	}
	if err := stopPushRecovery(); err != nil {
		shutdownErrs = append(
			shutdownErrs, fmt.Errorf("排空 push effect recovery: %w", err))
	}
	// No producer remains after callback and durable-operation recovery drain.
	// Stop the explicit terminal outbox consumers and wait for their fenced
	// PATCH checkpoints, then close admission for legacy best-effort session
	// writes. The continuation projector is owned by the root ctx; stop()
	// cancels it and the maintenanceWG drain below proves its exit and fenced
	// session checkpoint before shared resources close.
	if err := stopReceiptDispatch(); err != nil {
		shutdownErrs = append(shutdownErrs, fmt.Errorf("排空任务创建耐久回执: %w", err))
	}
	if err := stopDefinitionEditReceiptDispatch(); err != nil {
		shutdownErrs = append(
			shutdownErrs,
			fmt.Errorf("排空任务定义编辑耐久回执: %w", err),
		)
	}
	if err := drainAgentSessions(); err != nil {
		shutdownErrs = append(shutdownErrs, err)
	}
	// 等所有进程级维护任务退出；它们也共享 DB/Temporal/飞书发送端。
	maintenanceCtx, cancelMaintenance := context.WithTimeout(context.Background(), 30*time.Second)
	maintenanceErr := waitMaintenance(maintenanceCtx)
	cancelMaintenance()
	if maintenanceErr != nil {
		shutdownErrs = append(shutdownErrs, fmt.Errorf("排空维护任务: %w", maintenanceErr))
	}
	if err := completeHTTPShutdown(srv, httpShutdown, httpDrainProofTimeout); err != nil {
		shutdownErrs = append(shutdownErrs, fmt.Errorf("停止 HTTP 接入: %w", err))
	}

	drainErr := errors.Join(shutdownErrs...)
	if err := releaseAfterSafeDrain(drainErr, func() {
		// WorkerStopTimeout 让 Stop 等在跑 Activity 收尾；之后才释放 Temporal/DB。
		w.Stop()
		temporalClient.Close()
		// pgxpool.Close 会等借出连接归还；到这里所有入口、SDK detached work、
		// recovery、maintenance 与 Activity 都已确认退出。
		closeStores()
	}); err != nil {
		unsafeErr := fmt.Errorf("优雅关停未能安全排空，共享依赖保持打开: %w", err)
		if serveFailure != nil {
			return errors.Join(fmt.Errorf("HTTP 服务异常退出: %w", serveFailure), unsafeErr)
		}
		return unsafeErr
	}

	// 信号与 ListenAndServe 失败同时发生时 select 可能选中信号分支，
	// 这里补捞一次真实的服务错误，避免启动失败被伪装成正常关停（退出码 0）。
	if serveFailure != nil {
		return fmt.Errorf("HTTP 服务异常退出: %w", serveFailure)
	}
	select {
	case err := <-serveErr:
		return fmt.Errorf("HTTP 服务异常退出: %w", err)
	default:
	}

	slog.Info("关停完成")
	return nil
}

// initLogger 按配置级别初始化全局 slog（JSON 输出，便于日志聚合）。
// 级别解析失败时回退 info，不阻塞启动。
func initLogger(level string) {
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(level)); err != nil {
		lvl = slog.LevelInfo
	}
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl})))
}

// handleHealthz 是存活探针：进程活着即 200，不依赖任何下游。
func handleHealthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, `{"status":"ok"}`)
}

type readyzStore interface {
	Ping(context.Context) error
}

// handleReadyz 是就绪探针：所有已启用 Store 的连接池 Ping 通过返回 200，否则 503。
func handleReadyz(stores ...readyzStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		results := make(chan error, len(stores))
		for _, st := range stores {
			go func() { results <- st.Ping(ctx) }()
		}
		var pingErrs []error
		if len(stores) == 0 {
			pingErrs = append(pingErrs, errors.New("readyz Store 未配置"))
		}
		for range stores {
			if err := <-results; err != nil {
				pingErrs = append(pingErrs, err)
			}
		}
		if err := errors.Join(pingErrs...); err != nil {
			slog.Warn("readyz 数据库探活失败", "err", err)
			writeJSON(w, http.StatusServiceUnavailable, `{"status":"unavailable"}`)
			return
		}
		writeJSON(w, http.StatusOK, `{"status":"ok"}`)
	}
}

func writeJSON(w http.ResponseWriter, status int, body string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	io.WriteString(w, body) //nolint:errcheck // 响应写失败无从补救
}

// sessionCleanupInterval 是过期会话清理周期。
// 1 小时足够：会话 TTL 是 30 天，过期行多留一小时无害，而更频繁的全表扫描
// 对一台还跑着 Postgres/Temporal 的单 VPS 是纯浪费。
const sessionCleanupInterval = time.Hour

// runSessionCleanup 周期性删除过期会话，直到 ctx 取消。
//
// 失败只记日志不退出：清理是旁路维护，一次失败下轮再来；
// 让它把进程带崩才是真的坏事。
func runSessionCleanup(ctx context.Context, st *store.Store) {
	t := time.NewTicker(sessionCleanupInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			cctx, cancel := context.WithTimeout(ctx, 30*time.Second)
			n, err := st.DeleteExpiredSessions(cctx)
			cancel()
			if err != nil {
				slog.Warn("清理过期会话失败（下轮重试）", "err", err)
				continue
			}
			if n > 0 {
				slog.Info("清理过期会话", "deleted", n)
			}
		}
	}
}
