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

	"github.com/YouToco/vane/a2a"
	"github.com/YouToco/vane/agent"
	"github.com/YouToco/vane/agentcontinuation"
	"github.com/YouToco/vane/api"
	"github.com/YouToco/vane/auth"
	"github.com/YouToco/vane/cardgen"
	"github.com/YouToco/vane/config"
	"github.com/YouToco/vane/eventqualifier"
	"github.com/YouToco/vane/evolver"
	"github.com/YouToco/vane/executivebriefrecovery"
	"github.com/YouToco/vane/feedback"
	"github.com/YouToco/vane/feishu"
	"github.com/YouToco/vane/fetcher"
	"github.com/YouToco/vane/llm"
	"github.com/YouToco/vane/periodicbrief"
	"github.com/YouToco/vane/profilehint"
	"github.com/YouToco/vane/pusheffect"
	"github.com/YouToco/vane/pusher"
	"github.com/YouToco/vane/pushrecovery"
	"github.com/YouToco/vane/researchgateway"
	"github.com/YouToco/vane/runoutcome"
	"github.com/YouToco/vane/runtimeconfig"
	"github.com/YouToco/vane/runtimepolicy"
	"github.com/YouToco/vane/scheduler"
	"github.com/YouToco/vane/scorer"
	"github.com/YouToco/vane/store"
	"github.com/YouToco/vane/task"
	"github.com/YouToco/vane/tikhubinvoke"
	"github.com/YouToco/vane/types"
	"github.com/YouToco/vane/workflow"
)

// vaneVersion 是服务版本串（进 A2A AgentCard.version，a2a-contract §7）。
// 值 = CHANGELOG 最上方已发布版本号，随发版手动同步；不为此新增 ldflags 基建。
const vaneVersion = "0.5.1"

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "vane: %v\n", err)
		os.Exit(1)
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

	initLogger(cfg.Log.Level)

	st, err := store.NewServerRuntimeWithResearchRuntimeCapability(
		ctx, cfg.DB.URL, cfg.DB.ResearchRuntimeURL,
		store.ResearchRunCapabilityConfigV1{
			ActiveKeyID:  cfg.DB.ResearchCapabilityKeyID,
			ActiveKeyHex: cfg.DB.ResearchCapabilityKeyHex,
			RetiredKeys:  cfg.DB.ResearchCapabilityRetiredKeys,
			TTL:          time.Duration(cfg.DB.ResearchCapabilityTTLDays) * 24 * time.Hour,
		},
	)
	if err != nil {
		return fmt.Errorf("初始化数据库连接池: %w", err)
	}
	gatewayClient, err := researchgateway.NewUnixClientV1(cfg.ResearchGateway.SocketPath)
	if err != nil {
		st.Close()
		return fmt.Errorf("初始化 research gateway client: %w", err)
	}
	var researchRuntimeOption workflow.ActivitiesOption
	if cfg.Pipeline.ResearchV3ShadowCanaryScheduleID != "" ||
		cfg.Pipeline.ResearchV3AuthorityCanaryScheduleID != "" {
		researchExecutor, executorErr := fetcher.NewResearchExecutorV3(cfg.Fetch)
		if executorErr != nil {
			st.Close()
			return fmt.Errorf("初始化 research V3 executor: %w", executorErr)
		}
		researchRuntime, runtimeErr := workflow.NewProductionResearchRuntimeV3(
			st, gatewayClient, researchExecutor,
			func(ctx context.Context, identity types.RunIdentity) (
				runtimepolicy.BundleV1,
				runtimepolicy.ResearchToolPolicyV3,
				runtimepolicy.ResearchModelPolicyV3,
				error,
			) {
				for _, bucket := range []store.QuotaBucket{
					store.QuotaLLMTokens, store.QuotaExaCalls,
				} {
					if _, err := st.LoadQuotaRule(ctx, identity.TenantID, bucket); err != nil {
						return runtimepolicy.BundleV1{},
							runtimepolicy.ResearchToolPolicyV3{},
							runtimepolicy.ResearchModelPolicyV3{}, err
					}
				}
				current, err := runtimeconfig.BuildResearchRuntimeV3(
					runtimeconfig.CurrentCompiledV1Input{
						Model:                      cfg.LLM.Model,
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
			func(identity types.RunIdentity) bool {
				return identity.TaskID != "" &&
					(identity.TaskID == cfg.Pipeline.ResearchV3ShadowCanaryScheduleID ||
						identity.TaskID == cfg.Pipeline.ResearchV3AuthorityCanaryScheduleID)
			},
		)
		if runtimeErr != nil {
			st.Close()
			return fmt.Errorf("初始化 research V3 coordinator: %w", runtimeErr)
		}
		researchRuntimeOption = workflow.WithResearchRuntimeV3(researchRuntime)
	}

	// LLM 客户端 + 调用记账（写库失败只记日志，不影响主流程）。
	llmClient := llm.New(cfg.LLM)
	agentLLMConfig, err := cfg.LLM.AgentClientConfig()
	if err != nil {
		st.Close()
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
		st.Close()
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
		st.Close()
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
		researchRuntimeOption)
	periodicActivities, err := periodicbrief.NewActivities(
		st, compiledModelResolver, recorder,
		manager, cfg.Dashboard.Origin,
		cfg.Pipeline.ExecutiveBriefRendererCanaryScheduleID)
	if err != nil {
		temporalClient.Close()
		st.Close()
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
	w.RegisterWorkflow(workflow.PushPipelineWorkflow)
	w.RegisterWorkflow(workflow.ResearchShadowWorkflowV3)
	w.RegisterWorkflow(workflow.ResearchScheduledWorkflowV3)
	w.RegisterWorkflow(periodicbrief.WorkflowV1)
	// 逐个注册（非整体 Register）：漏注册不会启动失败，而是每批推送在该活动上
	// 重试到超时——EvolveProfile 的错误被 workflow 刻意吞掉，漏注册只会表现为
	// 推送莫名变慢（M5 契约 §13 明示）。
	//
	// 这份清单漏一个的后果在 009 上真实发生过：RecordEmptyBatch 加进 workflow 却
	// 忘了加进这里，全套测试与 go build 照样绿（Temporal 按名查表是运行时行为），
	// 而线上五处闸门的记账**全部静默失败**——整个"空批次可见化"沦为死代码，
	// 库里依旧零行，与没做这个功能逐字一致。由怀疑者审查在合并前抓出。
	// 现已由 workflow/registration_test.go 钉死：它反射 *Activities 的全部
	// Activity 方法并逐字比对本清单，漏一个 CI 就红。**新增 Activity 时改这里即可，
	// 那个测试会告诉你漏没漏。**
	w.RegisterActivity(activities.AuthorizeRun)
	w.RegisterActivity(activities.PrepareRun)
	w.RegisterActivity(activities.PrepareToolRunV2)
	w.RegisterActivity(activities.ExecuteToolInvocationV2)
	w.RegisterActivity(activities.CollectToolRunContentV2)
	w.RegisterActivity(activities.DedupToolCandidatesV2)
	w.RegisterActivity(activities.QualifyToolCandidatesV2)
	w.RegisterActivity(activities.ScoreToolCandidatesV2)
	w.RegisterActivity(activities.SelectToolCandidatesV2)
	w.RegisterActivity(activities.CardGenToolCandidatesV2)
	w.RegisterActivity(activities.PushToolCardsV2)
	w.RegisterActivity(activities.RecordEmptyToolRunV2)
	w.RegisterActivity(activities.PrepareResearchRunV3)
	w.RegisterActivity(activities.PlanResearchRunV3)
	w.RegisterActivity(activities.ExecuteResearchStepV3)
	w.RegisterActivity(activities.SynthesizeResearchBriefV3)
	w.RegisterActivity(activities.DeliverResearchBriefV3)
	w.RegisterActivity(activities.BeginRunOutcomeV1)
	w.RegisterActivity(activities.FinalizeRunOutcomeV1)
	w.RegisterActivity(activities.BeginToolRunOutcomeV2)
	w.RegisterActivity(activities.FinalizeToolRunOutcomeV2)
	w.RegisterActivity(activities.PrepareCanonicalBriefV1)
	w.RegisterActivity(activities.SynthesizeExecutiveBriefV1)
	w.RegisterActivity(activities.FreezeExecutiveBriefV1)
	w.RegisterActivity(activities.EvolveProfile)
	w.RegisterActivity(activities.Fetch)
	w.RegisterActivity(activities.FetchOutcomeV1)
	w.RegisterActivity(activities.Dedup)
	w.RegisterActivity(activities.QualifyEvents)
	w.RegisterActivity(activities.Score)
	w.RegisterActivity(activities.ScoreOutcomeV1)
	w.RegisterActivity(activities.Select)
	w.RegisterActivity(activities.CardGen)
	w.RegisterActivity(activities.CardGenOutcomeV1)
	w.RegisterActivity(activities.CardGenOutcomeV2)
	w.RegisterActivity(activities.CardGenOutcomeV3)
	w.RegisterActivity(activities.RecordEmptyBatch)
	w.RegisterActivity(activities.NotifyEmptyResult)
	w.RegisterActivity(activities.Push)
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
		st.Close()
		return fmt.Errorf("装配周期 Brief coordinator: %w", err)
	}
	periodicRecoveryRunner, err := periodicbrief.NewRecoveryRunner(
		st, temporalClient, manager, cfg.Dashboard.Origin,
		cfg.Pipeline.ExecutiveBriefRendererCanaryScheduleID,
		slog.Default())
	if err != nil {
		temporalClient.Close()
		st.Close()
		return fmt.Errorf("装配周期 Brief recovery: %w", err)
	}
	creationCoordinator := task.NewCreationCoordinator(st, sched, slog.Default())
	// C2b3-2c keeps definition editing dark at every ingress, but already owns
	// recovery for durable operations left by a prior process. The coordinator is
	// intentionally retained only in this composition root: it is not injected
	// into Agent, HTTP, Feishu, or the receipt dispatcher.
	definitionEditCoordinator := task.NewTaskDefinitionEditCoordinator(st, sched, slog.Default())

	roleGateCtx, cancelRoleGate := context.WithTimeout(ctx, 10*time.Second)
	roleGateErr := st.ValidateTaskDefinitionEditRuntimeRoles(roleGateCtx)
	cancelRoleGate()
	if roleGateErr != nil {
		temporalClient.Close()
		st.Close()
		return fmt.Errorf("任务定义编辑受限角色 Gate: %w", roleGateErr)
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
		st.Close()
		return fmt.Errorf("任务命令受限角色 Gate: %w", commandRoleGateErr)
	}
	environmentGateCtx, cancelEnvironmentGate := context.WithTimeout(ctx, 90*time.Second)
	environmentGateErr := definitionEditCoordinator.ValidateRuntimeEnvironment(
		environmentGateCtx,
	)
	cancelEnvironmentGate()
	if environmentGateErr != nil {
		temporalClient.Close()
		st.Close()
		return fmt.Errorf("任务定义编辑 Temporal 环境 Gate: %w", environmentGateErr)
	}

	// C2b3-2d startup barrier: finish one bounded recovery pass before legacy
	// Action reconciliation. Reconcile additionally re-authorizes each schedule
	// under the same PostgreSQL advisory lock used by edit quiesce, so a failed
	// recovery pass cannot turn an old active discovery snapshot into a write
	// across a live operation marker.
	if err := definitionEditCoordinator.RecoverStaleOnce(ctx); err != nil {
		temporalClient.Close()
		st.Close()
		return fmt.Errorf("任务定义编辑首轮恢复 Gate: %w", err)
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
		st.Close()
		return fmt.Errorf(
			"任务命令首轮恢复 Gate（%s budget）: %w",
			scheduler.ScheduleCommandRecoveryPassTimeout,
			scheduleCommandStartupErr,
		)
	}
	if err := sched.ReconcileActions(ctx); err != nil {
		temporalClient.Close()
		st.Close()
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
		st.Close()
		return fmt.Errorf("装配 run outcome recovery: %w", err)
	}
	if err := outcomeRecoveryRunner.RunStartup(ctx); err != nil {
		temporalClient.Close()
		st.Close()
		return fmt.Errorf("run outcome recovery 首轮恢复 Gate: %w", err)
	}
	executiveRecoveryRunner, err :=
		executivebriefrecovery.NewRunner(st, slog.Default())
	if err != nil {
		temporalClient.Close()
		st.Close()
		return fmt.Errorf("装配 executive Brief recovery: %w", err)
	}
	if err := executiveRecoveryRunner.RunStartup(ctx); err != nil {
		temporalClient.Close()
		st.Close()
		return fmt.Errorf(
			"executive Brief recovery 首轮恢复 Gate: %w", err)
	}
	if err := periodicCoordinator.RunStartup(ctx); err != nil {
		temporalClient.Close()
		st.Close()
		return fmt.Errorf("周期 Brief coordinator 首轮 Gate: %w", err)
	}
	if err := periodicRecoveryRunner.RunStartup(ctx); err != nil {
		temporalClient.Close()
		st.Close()
		return fmt.Errorf("周期 Brief recovery 首轮 Gate: %w", err)
	}

	// Push-effect recovery has a separate exact-task switch from fresh sends.
	// When enabled, prepare outbound API authority without opening Feishu WS,
	// then finish a complete recovery pass before any external ingress.
	var pushRecoveryRunner *pushrecovery.Runner
	if cfg.Pipeline.PushEffectRecoveryCanaryScheduleID != "" {
		outboundCtx, cancelOutbound := context.WithTimeout(ctx, 30*time.Second)
		outboundErr := manager.PrepareOutbound(outboundCtx)
		cancelOutbound()
		if outboundErr != nil {
			temporalClient.Close()
			st.Close()
			return fmt.Errorf("push effect recovery 飞书发送端 Gate: %w",
				outboundErr)
		}
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
			st.Close()
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
			st.Close()
			return fmt.Errorf("装配 push effect recovery lifecycle: %w", err)
		}
		if err := pushRecoveryRunner.RunStartup(ctx); err != nil {
			temporalClient.Close()
			st.Close()
			return fmt.Errorf("push effect recovery 首轮恢复 Gate: %w", err)
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
	// Legacy v0 create_schedule cards are deliberately drained without execution
	// in Loop.ExecuteAction. Passing no legacy creator makes the old active-first
	// CreatePush path unreachable even if that guard regresses.
	definitionEditController := task.NewDefinitionEditController(
		st, definitionEditCoordinator,
	)
	var definitionEditToolController agent.DefinitionEditController
	if cfg.Agent.DefinitionEditEnabled {
		definitionEditToolController = definitionEditController
	}
	tools := agent.BuildTools(
		st, sched, sched, endpoints, exaTools,
		definitionEditToolController,
	)
	if cfg.Agent.AgentFirstOwnerCanary {
		authorizer := agent.NewModelOwnerActionAuthorizer(
			agentLLMClient, recorder, cfg.LLM.AgentModel,
		)
		withoutLegacyProfileWriter := tools[:0]
		for _, tool := range tools {
			if tool.Name() != "update_profile" {
				withoutLegacyProfileWriter = append(withoutLegacyProfileWriter, tool)
			}
		}
		tools = withoutLegacyProfileWriter
		tools = append(tools,
			agent.NewQueryMyIntelligenceTool(st),
			agent.NewAuthorizedUpdateProfileTool(st, authorizer),
			agent.NewManageTasksTool(agent.ManageTasksDeps{
				Queries: st, Runner: sched, Deleter: sched,
				Edits:      definitionEditController,
				Authorizer: authorizer,
			}))
	}
	agentLoop, err := agent.NewChecked(agent.Deps{
		Client:     agentLLMClient,
		Recorder:   recorder,
		Store:      st,
		Profiles:   st,
		Tools:      tools,
		Model:      cfg.LLM.AgentModel,
		MaxTurns:   cfg.Agent.MaxTurns,
		SessionTTL: time.Duration(cfg.Agent.SessionTTLMinutes) * time.Minute,
		IntentToolkitsEnabled: cfg.Agent.IntentToolkitsOwnerCanary ||
			cfg.Agent.IntentToolkitsAllowAll,
		IntentToolkitsShadow: cfg.Agent.IntentToolkitsShadowEnabled &&
			!cfg.Agent.IntentToolkitsOwnerCanary &&
			!cfg.Agent.IntentToolkitsAllowAll,
		AgentFirstEnabled:      cfg.Agent.AgentFirstOwnerCanary,
		AgentFirstCanaryUserID: cfg.Agent.AgentFirstCanaryUserID,
		Endpoints:              endpoints,
		ToolCalls:              agent.NewToolCallRecorder(st), // 工具调用记账（契约 §6，全量工具）
		Evidence:               st,
		TaskCreation:           creationCoordinator,
		// The current controller serves direct durable execution only.
		TaskDefinitionEdit: definitionEditController,
	})
	if err != nil {
		return fmt.Errorf("装配 Agent 工具注册表: %w", err)
	}
	manager.SetAgent(agentLoop)

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
		st.Close()
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
		st.Close()
		return fmt.Errorf("装配任务定义编辑耐久回执: %w", err)
	}
	continuationDispatcher, err := agentcontinuation.New(
		st, slog.Default(),
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
				fmt.Errorf("装配 Agent 耐久延续投影器: %w", err),
				fmt.Errorf("排空启动维护任务: %w", maintenanceErr),
			)
		}
		temporalClient.Close()
		st.Close()
		return fmt.Errorf("装配 Agent 耐久延续投影器: %w", err)
	}
	// Definition-edit recovery starts before any Feishu/HTTP ingress is admitted.
	// The optional Agent controller below shares this coordinator only after all
	// startup Gates; terminal receipt dispatch remains a separate later step.
	definitionEditRecoveryCtx, cancelDefinitionEditRecovery := context.WithCancel(ctx)
	definitionEditRecoveryDone := make(chan struct{})
	go func() {
		defer close(definitionEditRecoveryDone)
		definitionEditCoordinator.RunRecovery(definitionEditRecoveryCtx)
	}()
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
	if pushRecoveryRunner == nil {
		close(pushRecoveryDone)
	} else {
		go func() {
			defer close(pushRecoveryDone)
			pushRecoveryRunner.Run(pushRecoveryCtx)
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

	// A5 创建恢复器在飞书 WS/HTTP 接收新确认之前启动。首次扫描与周期扫描都
	// 在后台执行，不阻塞 readyz；关停时必须先等它退出，再释放 Temporal/DB。
	creationRecoveryCtx, cancelCreationRecovery := context.WithCancel(ctx)
	creationRecoveryDone := make(chan struct{})
	go func() {
		defer close(creationRecoveryDone)
		creationCoordinator.RunRecovery(creationRecoveryCtx)
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

	// Feedback producers freeze exact session scope in their business
	// transaction. The projector starts before every ingress, scans only that
	// durable outbox, and has no provider dependency.
	runMaintenance(func() {
		continuationDispatcher.Run(ctx)
	})

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
		st.Close()
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

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", handleHealthz)
	mux.HandleFunc("GET /readyz", handleReadyz(st))
	// principal 解析器：全系统唯一的 principal 来源（企业级契约 §1.1，不变量 I-A1）。
	// 过渡期实现是「全局 owner 回退 + 租户恒为 1」，行为与收敛前逐字一致；
	// 真实认证落地后只换这一处构造，api/a2a/gate 三处调用点零改动。
	principals := auth.NewOwnerResolver(st, feishu.SettingKeyOwner)

	api.Mount(mux, api.Deps{
		Store:                 st,
		Auth:                  st,
		Manager:               manager,
		Scheduler:             sched,
		TaskAgent:             agentLoop,
		BriefFeedback:         fbSvc,
		DefinitionEditEnabled: cfg.Agent.DefinitionEditEnabled,
		ExecutiveBriefWebCanaryScheduleID: cfg.Pipeline.
			ExecutiveBriefWebCanaryScheduleID,
		ExecutiveBriefWebProjectionAllowAll: cfg.Pipeline.
			ExecutiveBriefWebProjectionAllowAll,
		// HTTP 面的 principal 来自会话中间件注入的 ctx（企业级契约 §1.1 的最终形态）；
		// a2a/gate 无 HTTP 会话，仍用 owner 回退——这正是把 principal 做成接口的价值。
		Principal: auth.NewContextResolver(),
		Origin:    cfg.Dashboard.Origin,
	})

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

		// assistant.chat 的 A2A 轨 agent 实例（契约 §12 P2）：与飞书轨完全隔离——
		// 工具只由 agent 包的本地 authorization policy 筛选；main 不解释 effect，
		// 更不读取远端 metadata。当前策略精确授权 list_schedules；
		// system prompt 换 A2A 语境；Store/Profiles 不注入（RunOnce 不碰会话与画像，
		// 误用 HandleMessage 会在 loadOrCreateSession 处 nil panic——响亮的装配错误）。
		a2aTools, filterErr := agent.FilterAuthorizedTools(
			tools, agent.AuthorizationA2AReadOnly,
		)
		if filterErr != nil {
			return fmt.Errorf("筛选 A2A Agent 工具: %w", filterErr)
		}
		a2aLoop, loopErr := agent.NewChecked(agent.Deps{
			Client:                agentLLMClient,
			Recorder:              recorder,
			Tools:                 a2aTools,
			Model:                 cfg.LLM.AgentModel,
			MaxTurns:              cfg.Agent.MaxTurns,
			SystemPrompt:          a2a.ChatSystemPrompt,
			IntentToolkitsEnabled: cfg.Agent.IntentToolkitsAllowAll,
			IntentToolkitsShadow: cfg.Agent.IntentToolkitsShadowEnabled &&
				!cfg.Agent.IntentToolkitsAllowAll,
			ToolCalls: agent.NewToolCallRecorder(st), // 工具调用同样记账（契约 §6）
		})
		if loopErr != nil {
			return fmt.Errorf("装配 A2A Agent 工具注册表: %w", loopErr)
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
			st.Close()
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
		st.Close()
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

// handleReadyz 是就绪探针：DB Ping 通过返回 200，否则 503。
func handleReadyz(st *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := st.Ping(ctx); err != nil {
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
