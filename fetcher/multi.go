// Multi 是多信源分发器：按 src.Type 把抓取请求路由到具体抓取器（RSS/Exa/TikHub）。
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
	rss    *Fetcher
	exa    *ExaFetcher
	tikhub *TikHubFetcher
}

// NewMulti 按抓取配置构造全部抓取器。未配置 key 的信源类型仍会构造
// （构造零成本），等真有该类型的源被抓取时才在 Fetch 返回配置缺失错误。
func NewMulti(cfg config.FetchConfig) *Multi {
	return &Multi{
		rss:    New(cfg),
		exa:    NewExa(cfg),
		tikhub: NewTikHub(cfg),
	}
}

// Fetch 按信源类型分发。未知类型返回 CodeValidation（不可重试）——
// 这是数据问题（sources.type 存了未支持的值），重试无益。
func (m *Multi) Fetch(ctx context.Context, src types.Source) ([]types.ContentItem, error) {
	switch src.Type {
	case types.SourceTypeRSS:
		return m.rss.FetchRSS(ctx, src)
	case types.SourceTypeExa:
		return m.exa.Fetch(ctx, src)
	case types.SourceTypeTikHubXHS:
		return m.tikhub.Fetch(ctx, src)
	default:
		return nil, types.NewAppError(types.CodeValidation,
			fmt.Sprintf("未知信源类型 %q（source_id=%d）", src.Type, src.ID), nil)
	}
}
