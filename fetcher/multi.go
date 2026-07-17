// Multi 是多信源分发器：按 (Platform, Capability) 把抓取请求路由到具体抓取器。
// workflow 的 Fetch Activity 只依赖单方法接口 Fetch(ctx, src)，新增信源类型
// 只需在此扩一个 case，pipeline 与装配代码零改动。
package fetcher

import (
	"context"
	"fmt"

	"github.com/YouToco/vane/config"
	"github.com/YouToco/vane/types"
)

// Multi 持有各类型抓取器。零外部状态，多 goroutine 可并发复用。
type Multi struct {
	rss       *Fetcher
	exa       *ExaFetcher
	tikhub    *TikHubFetcher
	twitter   *TwitterFetcher
	pageWatch *PageWatchFetcher
}

// NewMulti 按抓取配置构造全部抓取器。未配置 key 的信源类型仍会构造
// （构造零成本），等真有该类型的源被抓取时才在 Fetch 返回配置缺失错误。
//
// seen 只被 TikHub 抓取器用于"这条笔记是否已入库"，从而只为新笔记调用按次计费的
// 详情接口（见 SeenChecker）。传 nil 合法：详情补全会被跳过，小红书正文退回搜索
// 给的 60 字摘要——不改善，但也不比补全上线前更差。
func NewMulti(cfg config.FetchConfig, seen SeenChecker, snaps SnapshotStore) *Multi {
	rss := New(cfg)
	return &Multi{
		rss:       rss,
		exa:       NewExa(cfg),
		tikhub:    NewTikHub(cfg, seen),
		twitter:   NewTwitter(cfg),
		pageWatch: NewPageWatch(rss, snaps),
	}
}

// Fetch 按 (Platform, Capability) 分发。未知组合返回 CodeValidation（不可重试）。
func (m *Multi) Fetch(ctx context.Context, src types.Source) ([]types.ContentItem, error) {
	switch src.Platform {
	case types.PlatformWeb:
		switch src.Capability {
		case types.CapFeed:
			return m.rss.FetchRSS(ctx, src)
		case types.CapSearch:
			return m.exa.Fetch(ctx, src)
		case types.CapPageWatch:
			return m.pageWatch.Fetch(ctx, src)
		default:
			return nil, types.NewAppError(types.CodeValidation,
				fmt.Sprintf("web 平台不支持 %q 能力（source_id=%d）", src.Capability, src.ID), nil)
		}

	case types.PlatformXHS:
		switch src.Capability {
		case types.CapSearch:
			return m.tikhub.Fetch(ctx, src)
		default:
			return nil, types.NewAppError(types.CodeValidation,
				fmt.Sprintf("xhs 平台不支持 %q 能力（source_id=%d）", src.Capability, src.ID), nil)
		}

	case types.PlatformX:
		switch src.Capability {
		case types.CapUserPosts:
			return m.twitter.Fetch(ctx, src)
		default:
			return nil, types.NewAppError(types.CodeValidation,
				fmt.Sprintf("x 平台不支持 %q 能力（source_id=%d）", src.Capability, src.ID), nil)
		}

	default:
		return nil, types.NewAppError(types.CodeValidation,
			fmt.Sprintf("未知平台 %q（source_id=%d）", src.Platform, src.ID), nil)
	}
}
