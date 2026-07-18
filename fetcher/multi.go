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
	"github.com/YouToco/vane/sourcecatalog"
	"github.com/YouToco/vane/types"
)

// Multi 持有各类型抓取器。零外部状态，多 goroutine 可并发复用。
type Multi struct {
	rss     *Fetcher
	exa     *ExaFetcher
	tikhub  *TikHubFetcher
	xhsUser *XHSUserFetcher
	twitter *TwitterFetcher
}

// NewMulti 按抓取配置构造全部抓取器。未配置 key 的信源类型仍会构造
// （构造零成本），等真有该类型的源被抓取时才在 Fetch 返回配置缺失错误。
//
// seen 只被 TikHub 搜索抓取器用于"这条笔记是否已入库"，从而只为新笔记调用按次计费的
// 详情接口（见 SeenChecker）。传 nil 合法：详情补全会被跳过，小红书搜索正文退回 60 字
// 摘要——不改善，但也不比补全上线前更差。xhs/user_posts 不做详情补全，不受 seen 影响。
func NewMulti(cfg config.FetchConfig, seen SeenChecker) *Multi {
	return &Multi{
		rss:     New(cfg),
		exa:     NewExa(cfg),
		tikhub:  NewTikHub(cfg, seen),
		xhsUser: NewXHSUser(cfg),
		twitter: NewTwitter(cfg),
	}
}

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
		}

	case types.PlatformXHS:
		switch src.Capability {
		case types.CapSearch:
			return m.tikhub.Fetch(ctx, src)
		case types.CapUserPosts:
			return m.xhsUser.Fetch(ctx, src)
		}

	case types.PlatformX:
		switch src.Capability {
		case types.CapUserPosts:
			return m.twitter.Fetch(ctx, src)
		}
	}

	// 走到这里说明 sourcecatalog 标记该组合 Available，但上面的 switch 没有对应 provider
	// ——即注册表与装配漂移（新注册了能力却忘了在此接抓取器）。这是编程/装配错误，不是
	// 数据错误，故用 CodeInternal 而非 CodeValidation，让它在探针里显性暴露而非静默当作坏源。
	return nil, types.NewAppError(types.CodeInternal,
		fmt.Sprintf("信源能力 %q/%q 已注册为可用但无对应抓取器（装配漏接，source_id=%d）",
			src.Platform, src.Capability, src.ID), nil)
}
