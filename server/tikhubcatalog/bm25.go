// BM25 倒排索引：注册表检索的实现。
//
// 为什么是 BM25 而不是 embedding（端点注册表契约 §3）：
//   - Anthropic Tool Search Tool 开箱只提供 regex/BM25 两个变体，embedding 仅是
//     cookbook 选装——工具目录这种短文本结构化语料，词法检索已被官方验证够用；
//   - DeepSeek 无 embedding API，语义检索要引第三方依赖 + 索引持久化，MVP 不值得；
//   - 检索质量的真实短板在描述质量而非算法（多篇研究一致），TikHub spec 全量中英
//     双语描述，词法命中面足够。升级 embedding 的决策留给 tool_calls 日志实证
//     （retrieval_query + candidate_tools 字段专为此留痕）。
//
// 分词（中英混合语料的最小可用方案）：
//   - ASCII 字母/数字连续段 → 小写单词（fetch_post_detail → fetch/post/detail）；
//   - CJK 连续段 → 相邻双字 bigram（"热榜数据" → 热榜/榜数/数据），单字段落原样成词；
//   - 查询与文档同一分词器，天然对齐。
package tikhubcatalog

import (
	"strings"

	"github.com/YouToco/vane/toolsearch"
)

// docText 拼接一个端点的可检索文本。各域直接连接：BM25F 分域加权对这个体量是
// 过度设计；工具名与 summary 的词天然稀有（df 低），BM25 的 IDF 已给了它们高权。
func docText(e Entry) string {
	var b strings.Builder
	b.WriteString(strings.ReplaceAll(e.Name, "_", " "))
	b.WriteByte(' ')
	b.WriteString(e.Platform)
	b.WriteByte(' ')
	b.WriteString(strings.ReplaceAll(e.Tag, "-", " "))
	b.WriteByte(' ')
	b.WriteString(e.Summary)
	b.WriteByte(' ')
	b.WriteString(e.Description)
	for _, p := range e.Params {
		b.WriteByte(' ')
		b.WriteString(p.Name)
		b.WriteByte(' ')
		b.WriteString(p.Desc)
	}
	return b.String()
}

func explicitAdvancedAnalyticsQuery(query string) bool {
	normalized := strings.ToLower(query)
	for _, marker := range []string{
		"广告", "投放", "店铺", "电商", "带货", "创作者分析", "达人分析",
		"星图",
	} {
		if strings.Contains(normalized, marker) {
			return true
		}
	}
	tokens := toolsearch.Tokenize(normalized)
	for _, token := range tokens {
		switch token {
		case "ads", "advertising", "shop", "commerce", "merchant", "douplus", "xingtu":
			return true
		}
	}
	for i := 0; i+1 < len(tokens); i++ {
		if tokens[i] == "creator" && tokens[i+1] == "analytics" {
			return true
		}
	}
	return false
}

func advancedAnalyticsEntry(entry Entry) bool {
	normalized := strings.ToLower(strings.Join([]string{
		entry.Name, entry.Tag, entry.Summary,
	}, " "))
	for _, marker := range []string{
		"douplus", "xingtu", "星图", "广告", "投放", "shop", "commerce",
		"merchant", "creator analytics", "创作者分析", "达人分析",
	} {
		if strings.Contains(normalized, marker) {
			return true
		}
	}
	return false
}
