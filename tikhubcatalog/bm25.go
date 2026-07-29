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
	"math"
	"sort"
	"strings"
	"unicode"
)

// BM25 标准参数（Robertson/Walker 经验默认，Lucene/ES 同款）。
const (
	bm25K1 = 1.2
	bm25B  = 0.75
)

type posting struct {
	doc int // entries 下标
	tf  int
}

type bm25Index struct {
	postings map[string][]posting
	docLen   []int
	avgLen   float64
}

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

func buildIndex(es []Entry) *bm25Index {
	ix := &bm25Index{
		postings: make(map[string][]posting),
		docLen:   make([]int, len(es)),
	}
	total := 0
	for i, e := range es {
		tf := make(map[string]int)
		n := 0
		for _, tok := range tokenize(docText(e)) {
			tf[tok]++
			n++
		}
		ix.docLen[i] = n
		total += n
		for tok, c := range tf {
			ix.postings[tok] = append(ix.postings[tok], posting{doc: i, tf: c})
		}
	}
	if len(es) > 0 {
		ix.avgLen = float64(total) / float64(len(es))
	}
	return ix
}

func (ix *bm25Index) search(query, platform string, topK int) []Hit {
	qToks := tokenize(query)
	if len(qToks) == 0 {
		return nil
	}
	// 查询词去重：BM25 对重复查询词累加没有检索意义，还会放大误拼权重。
	seen := make(map[string]bool, len(qToks))
	scores := make(map[int]float64)
	n := float64(len(agentEntries))
	for _, tok := range qToks {
		if seen[tok] {
			continue
		}
		seen[tok] = true
		plist := ix.postings[tok]
		if len(plist) == 0 {
			continue
		}
		// IDF 用 Lucene 的非负变体：log(1 + (N-df+0.5)/(df+0.5))，高频词权重趋零但不为负。
		idf := math.Log(1 + (n-float64(len(plist))+0.5)/(float64(len(plist))+0.5))
		for _, p := range plist {
			dl := float64(ix.docLen[p.doc])
			tf := float64(p.tf)
			scores[p.doc] += idf * tf * (bm25K1 + 1) / (tf + bm25K1*(1-bm25B+bm25B*dl/ix.avgLen))
		}
	}

	platform = strings.ToLower(strings.TrimSpace(platform))
	allowAdvanced := explicitAdvancedAnalyticsQuery(query)
	hits := make([]Hit, 0, len(scores))
	for doc, s := range scores {
		if platform != "" && agentEntries[doc].Platform != platform {
			continue
		}
		if advancedAnalyticsEntry(agentEntries[doc]) && !allowAdvanced {
			continue
		}
		hits = append(hits, Hit{Entry: agentEntries[doc], Score: s})
	}
	// 分数降序，同分按名字典序——检索结果给模型看，顺序不能因 map 遍历序抖动。
	sort.Slice(hits, func(i, j int) bool {
		if hits[i].Score != hits[j].Score {
			return hits[i].Score > hits[j].Score
		}
		return hits[i].Entry.Name < hits[j].Entry.Name
	})
	if len(hits) > topK {
		hits = hits[:topK]
	}
	return hits
}

func explicitAdvancedAnalyticsQuery(query string) bool {
	normalized := strings.ToLower(query)
	for _, marker := range []string{
		"广告", "投放", "店铺", "电商", "带货", "创作者分析", "达人分析",
		"ads", "advertising", "shop", "commerce", "creator analytics",
		"merchant", "douplus", "星图", "xingtu",
	} {
		if strings.Contains(normalized, marker) {
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

// tokenize 中英混合分词，规则见文件头注。
func tokenize(s string) []string {
	var toks []string
	runes := []rune(strings.ToLower(s))
	i := 0
	for i < len(runes) {
		r := runes[i]
		switch {
		case r >= 'a' && r <= 'z' || r >= '0' && r <= '9':
			j := i
			for j < len(runes) && (runes[j] >= 'a' && runes[j] <= 'z' || runes[j] >= '0' && runes[j] <= '9') {
				j++
			}
			toks = append(toks, string(runes[i:j]))
			i = j
		case unicode.Is(unicode.Han, r):
			j := i
			for j < len(runes) && unicode.Is(unicode.Han, runes[j]) {
				j++
			}
			if j-i == 1 {
				toks = append(toks, string(runes[i]))
			}
			for k := i; k+1 < j; k++ {
				toks = append(toks, string(runes[k:k+2]))
			}
			i = j
		default:
			i++
		}
	}
	return toks
}
