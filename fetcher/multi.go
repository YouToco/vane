// Multi 是多信源分发器：按 (Platform, Capability) 把抓取请求路由到具体抓取器。
// workflow 的 Fetch Activity 只依赖单方法接口 Fetch(ctx, src)，新增信源类型
// 只需在 sourcecatalog 注册一行 + 这里接一个 case，pipeline 与装配代码零改动。
//
// 分发前先过 sourcecatalog 门禁（契约 §2/§6）：未注册的组合与"注册了但 Unavailable"
// 是两种不同的错误——前者是"没这个东西"，后者是"有但故意不做，且带得出原因"。
// 把 Unavailable 的 Reason 带进报错，agent 就能主动回答"X 关键词搜索为何不支持"，
// 而不是静默改用别的能力凑合（契约 §2.2 的核心动机）。
package fetcher

import (
	"context"
	"fmt"

	"github.com/YouToco/vane/config"
	"github.com/YouToco/vane/runtimepolicy"
	"github.com/YouToco/vane/sourcecatalog"
	"github.com/YouToco/vane/types"
)

// Multi 持有各类型抓取器。零外部状态，多 goroutine 可并发复用。
type Multi struct {
	rss          *Fetcher
	exa          *ExaFetcher
	exaContents  *ExaContentsFetcher
	binding      *BindingFetcher
	runtimeV1    *RuntimeFetchResolverV1
	runtimeV1Err error
}

type runtimeFetchExecutorSetV1 struct {
	rss         *Fetcher
	exa         *ExaFetcher
	exaContents *ExaContentsFetcher
	binding     *BindingFetcher
}

// NewMulti 按抓取配置构造全部抓取器。未配置 key 的信源类型仍会构造
// （构造零成本），等真有该类型的源被抓取时才在 Fetch 返回配置缺失错误。
//
// seen 只被绑定引擎的 enrich 用于"这条笔记正文是否已补全"，从而只为新笔记调用
// 按次计费的详情接口（见 SeenChecker）。传 nil 合法：详情补全会被跳过，
// 小红书搜索正文退回 60 字摘要——不改善，但也不比补全上线前更差。
//
// rec 是绑定引擎的调用记账（tool_calls，契约 §5）；nil 合法（只是不记账）。
func NewMulti(cfg config.FetchConfig, seen SeenChecker, rec BindingCallRecorder) *Multi {
	multi, err := NewMultiWithRuntimeRoutesV1(cfg, seen, rec)
	if err == nil {
		return multi
	}
	// Config.Validate rejects invalid generations in production. Preserve the
	// legacy fetch surface for direct/test construction while making compiled
	// execution fail closed with the composition error.
	set := newRuntimeFetchExecutorSetV1(cfg, seen, rec)
	return &Multi{
		rss: set.rss, exa: set.exa, exaContents: set.exaContents,
		binding: set.binding, runtimeV1Err: err,
	}
}

// NewMultiWithRuntimeRoutesV1 composes the current legacy executors plus an
// immutable compiled-route registry. retainedRoutes lets a deployment keep
// older endpoint/key generations alive during rotation; duplicate identities
// are rejected and never replaced by the current route.
func NewMultiWithRuntimeRoutesV1(
	cfg config.FetchConfig,
	seen SeenChecker,
	rec BindingCallRecorder,
	retainedRoutes ...RuntimeFetchRouteV1,
) (*Multi, error) {
	set := newRuntimeFetchExecutorSetV1(cfg, seen, rec)
	currentRoutes, err := runtimeFetchRoutesV1(cfg, set)
	if err != nil {
		return nil, err
	}
	routes := make([]RuntimeFetchRouteV1, 0, len(currentRoutes)+len(retainedRoutes))
	routes = append(routes, currentRoutes...)
	routes = append(routes, retainedRoutes...)
	resolver, err := NewRuntimeFetchResolverV1(routes...)
	if err != nil {
		return nil, err
	}
	return &Multi{
		rss: set.rss, exa: set.exa, exaContents: set.exaContents,
		binding: set.binding, runtimeV1: resolver,
	}, nil
}

// NewRuntimeFetchRoutesV1 constructs one independently retained provider set.
// The returned routes can be supplied to NewMultiWithRuntimeRoutesV1 while an
// older generation is still allowed to finish already-prepared runs.
func NewRuntimeFetchRoutesV1(
	cfg config.FetchConfig,
	seen SeenChecker,
	rec BindingCallRecorder,
) ([]RuntimeFetchRouteV1, error) {
	return runtimeFetchRoutesV1(cfg, newRuntimeFetchExecutorSetV1(cfg, seen, rec))
}

func newRuntimeFetchExecutorSetV1(
	cfg config.FetchConfig,
	seen SeenChecker,
	rec BindingCallRecorder,
) runtimeFetchExecutorSetV1 {
	// RSS 抓取器接上正文补全：链接型聚合器（HN/Lobsters/Reddit）的条目只给标题和
	// 链接、不带正文，不补的话整批被 §12.3 护栏丢弃（2026-07-18 生产实证）。
	// 复用已经构造好的 exaContents 取正文、复用 seen 做付费闸门，不新增装配参数。
	rss := New(cfg)
	exaContents := NewExaContents(cfg, rec)
	rss.enricher = exaContents
	rss.seen = seen

	return runtimeFetchExecutorSetV1{
		rss:         rss,
		exa:         NewExa(cfg, rec),
		exaContents: exaContents,
		binding:     NewBinding(cfg, seen, rec),
	}
}

func runtimeFetchRoutesV1(
	cfg config.FetchConfig,
	set runtimeFetchExecutorSetV1,
) ([]RuntimeFetchRouteV1, error) {
	exaGeneration := cfg.CompiledExaCredentialGeneration
	if exaGeneration == 0 {
		exaGeneration = runtimepolicy.PrimaryGenerationV1
	}
	tikHubGeneration := cfg.CompiledTikHubCredentialGeneration
	if tikHubGeneration == 0 {
		tikHubGeneration = runtimepolicy.PrimaryGenerationV1
	}
	if exaGeneration < 0 || tikHubGeneration < 0 {
		return nil, fmt.Errorf("fetcher: compiled credential generation must be positive")
	}
	exaRef := runtimepolicy.CredentialRefV1{
		ID: runtimepolicy.CredentialIDExaPrimaryV1, Generation: exaGeneration,
	}
	tikHubRef := runtimepolicy.CredentialRefV1{
		ID: runtimepolicy.CredentialIDTikHubPrimaryV1, Generation: tikHubGeneration,
	}
	routes := []RuntimeFetchRouteV1{
		{
			Capability: runtimepolicy.CapabilityV1{
				Platform: string(types.PlatformWeb), Capability: string(types.CapFeed),
				Kind:                     string(types.KindArticle),
				ImplementationVersion:    runtimepolicy.CapabilityImplementationRSSV1,
				DependencyCredentialRefs: []runtimepolicy.CredentialRefV1{exaRef},
			},
			RSS: set.rss,
		},
		{
			Capability: runtimepolicy.CapabilityV1{
				Platform: string(types.PlatformWeb), Capability: string(types.CapSearch),
				Kind:                  string(types.KindArticle),
				ImplementationVersion: runtimepolicy.CapabilityImplementationExaV1,
				CredentialRef:         exaRef,
			},
			ExaSearch: set.exa,
		},
		{
			Capability: runtimepolicy.CapabilityV1{
				Platform: string(types.PlatformWeb), Capability: string(types.CapContents),
				Kind:                  string(types.KindPageContent),
				ImplementationVersion: runtimepolicy.CapabilityImplementationExaV1,
				CredentialRef:         exaRef,
			},
			ExaContents: set.exaContents,
		},
	}
	for _, pair := range []struct {
		platform   types.Platform
		capability types.Capability
	}{
		{types.PlatformX, types.CapUserPosts},
		{types.PlatformXHS, types.CapSearch},
		{types.PlatformXHS, types.CapUserPosts},
		{types.PlatformXHS, types.CapHotList},
		{types.PlatformXHS, types.CapTopicFeed},
		{types.PlatformXHS, types.CapFavedNotes},
	} {
		routes = append(routes, RuntimeFetchRouteV1{
			Capability: runtimepolicy.CapabilityV1{
				Platform: string(pair.platform), Capability: string(pair.capability),
				Kind:                  string(types.KindArticle),
				ImplementationVersion: runtimepolicy.CapabilityImplementationBindingV1,
				CredentialRef:         tikHubRef,
			},
			Binding: set.binding,
		})
	}
	return routes, nil
}

// Binding 暴露绑定引擎（agent 的试跑准入用 Probe，见 endpoint-binding-contract.md §2.2）。
func (m *Multi) Binding() *BindingFetcher { return m.binding }

// Exa / ExaContents 暴露 Exa 两个抓取器（agent 的 web_search/read_page ad-hoc 工具用）：
// 与信源周期抓取共享同一实例——同一 http.Client、同一记账通道（tool_calls）。
func (m *Multi) Exa() *ExaFetcher                 { return m.exa }
func (m *Multi) ExaContents() *ExaContentsFetcher { return m.exaContents }

// Fetch 按 (Platform, Capability) 分发。
//
//   - 组合不在 sourcecatalog 里          → CodeValidation（"未知能力"，数据问题，不可重试）
//   - 在表里但 Status != Available       → CodeValidation，Message 带 Entry.Reason
//     （让"为什么这个源不工作"进到用户/agent 看得见的地方，不再只是 const 注释）
//   - Available 但下面 switch 没接上      → CodeInternal（注册表与装配漂移，装配漏接了 provider）
func (m *Multi) Fetch(ctx context.Context, src types.Source) ([]types.ContentItem, error) {
	entry, ok := sourcecatalog.Lookup(src.Platform, src.Capability)
	if !ok {
		return nil, types.NewAppError(types.CodeValidation,
			fmt.Sprintf("未知信源能力 %q/%q（source_id=%d）", src.Platform, src.Capability, src.ID), nil)
	}
	if !entry.Available() {
		return nil, types.NewAppError(types.CodeValidation,
			fmt.Sprintf("信源能力 %q/%q 当前不可用：%s（source_id=%d）",
				src.Platform, src.Capability, entry.Reason, src.ID), nil)
	}

	switch src.Platform {
	case types.PlatformWeb:
		switch src.Capability {
		case types.CapFeed:
			return m.rss.FetchRSS(ctx, src)
		case types.CapSearch:
			return m.exa.Fetch(ctx, src)
		case types.CapContents:
			return m.exaContents.Fetch(ctx, src)
		}

	case types.PlatformXHS:
		switch src.Capability {
		case types.CapSearch, types.CapUserPosts, types.CapHotList, types.CapTopicFeed, types.CapFavedNotes:
			return m.binding.Fetch(ctx, src)
		}

	case types.PlatformX:
		switch src.Capability {
		case types.CapUserPosts:
			return m.binding.Fetch(ctx, src)
		}
	}

	// 走到这里说明 sourcecatalog 标记该组合 Available，但上面的 switch 没有对应 provider
	// ——即注册表与装配漂移（新注册了能力却忘了在此接抓取器）。这是编程/装配错误，不是
	// 数据错误，故用 CodeInternal 而非 CodeValidation，让它在探针里显性暴露而非静默当作坏源。
	return nil, types.NewAppError(types.CodeInternal,
		fmt.Sprintf("信源能力 %q/%q 已注册为可用但无对应抓取器（装配漏接，source_id=%d）",
			src.Platform, src.Capability, src.ID), nil)
}
