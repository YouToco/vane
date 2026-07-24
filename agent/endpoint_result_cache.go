// 端点大结果的「缓存句柄 + 取回工具」（端点注册表契约 §3.5，2026-07-18）。
//
// 动机：端点响应超限时原先只有硬截断 + 一行提示，被截部分**没有任何取回通道**——
// Boss 在 B 站评论查询上撞到「内容被截断」死路。业界调研（Claude Code / Pi /
// OpenClaw 源码级，详见契约 §3.5 注）结论收敛：没有一家裸移除上限，共识是
// 「cap + 全量留存 + 句柄 + 取回工具」；Codex 是唯一只截不给取回的反例，其社区
// 正开着 issue 求这套方案。本文件是该共识的 vane 实现：
//
//   - 超限响应全量进进程内 LRU 缓存（句柄 res-N，TTL 30min，随服务重启失效）；
//   - 截断提示附 JSON 结构摘要（模型不读全文就知道形状，可直接写路径查询）；
//   - read_endpoint_result 工具按点路径取子树（最省 token）或按 offset 分页续读；
//   - 提示措辞强命令式——Claude Code 社区实测模型有 ~70% 概率把预览当全文作答，
//     必须显式压制（「不要把预览当作完整数据回答」）。
//
// 缓存条目绑定 userID：句柄是短串可猜（res-3），多租户下不绑定就是跨用户读取面。
package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

const (
	// resultCacheTTL 句柄有效期。对齐会话 TTL 量级：取回是「同一段对话里的下一步」，
	// 不是长期存储——数据资产在 tool_calls（result_preview）与上游，缓存只是通道。
	resultCacheTTL = 30 * time.Minute
	// resultCacheMaxEntries 缓存条目上限（LRU 逐出最旧）。响应体已被 tikhubinvoke
	// 的 2MiB 内存护栏封顶，16 条最坏 32MB，可接受。
	resultCacheMaxEntries = 16
)

type cachedResult struct {
	body     []byte
	endpoint string
	userID   int64
	seq      int64 // put 序号（nextID），逐出时序的权威——见 evictOldestLocked
	at       time.Time
}

// resultCache 进程内端点大结果缓存。零持久化是刻意的：重启丢失 → 模型收到
// 「句柄过期请重查」，代价是一次重调；换来的是零 migration、零磁盘管理。
type resultCache struct {
	mu      sync.Mutex
	entries map[string]*cachedResult
	nextID  int64
}

func newResultCache() *resultCache {
	return &resultCache{entries: map[string]*cachedResult{}}
}

// put 存入完整响应体，返回句柄。超量时逐出最旧条目（LRU by 存入序号——
// 取回不刷新时序：句柄的生命周期语义是「这次调用后的 30 分钟」，不是活跃度）。
func (c *resultCache) put(userID int64, endpoint string, body []byte) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nextID++
	handle := "res-" + strconv.FormatInt(c.nextID, 10)
	c.entries[handle] = &cachedResult{body: body, endpoint: endpoint, userID: userID, seq: c.nextID, at: time.Now()}
	if len(c.entries) > resultCacheMaxEntries {
		c.evictOldestLocked()
	}
	return handle
}

// evictOldestLocked 逐出最早存入的一条（调用方须持锁）。按 put 序号而非 at 比较：
// 时钟粒度粗时（Windows tick 0.5–15.6ms）同 tick 连续 put 的 at 完全相同，旧实现
// 「严格 Before + time.Now() 初值」在全员并列时选不出牺牲者，delete("") 静默无效、
// 缓存越限（2026-07-19 实 bug）；且并列下按 at 选谁都是 map 随机序，序号才是
// 插入次序的无损全序。
func (c *resultCache) evictOldestLocked() {
	var oldest string
	var oldestSeq int64
	for h, e := range c.entries {
		if oldest == "" || e.seq < oldestSeq {
			oldest, oldestSeq = h, e.seq
		}
	}
	delete(c.entries, oldest)
}

// get 取句柄；过期/不存在/非本人一律 miss（不区分口径，防句柄探测）。
func (c *resultCache) get(userID int64, handle string) (*cachedResult, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[handle]
	if !ok || e.userID != userID || time.Since(e.at) > resultCacheTTL {
		if ok && time.Since(e.at) > resultCacheTTL {
			delete(c.entries, handle)
		}
		return nil, false
	}
	return e, true
}

// ────────── JSON 结构摘要 ──────────

// summarizeJSONStructure 给出响应的形状概要（深度 2、每层前 10 键），让模型
// 不读全文即可写出路径查询——这是 Claude Code 大结果消息里最值钱的细节。
func summarizeJSONStructure(body []byte) string {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return "" // 非 JSON（或截断损坏）：不给摘要，预览自证
	}
	return renderShape(v, 2)
}

func renderShape(v any, depth int) string {
	switch t := v.(type) {
	case map[string]any:
		if depth <= 0 {
			return "{…}"
		}
		keys := make([]string, 0, len(t))
		for k := range t {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		shown := keys
		if len(shown) > 10 {
			shown = shown[:10]
		}
		parts := make([]string, 0, len(shown))
		for _, k := range shown {
			parts = append(parts, k+":"+renderShape(t[k], depth-1))
		}
		suffix := ""
		if len(keys) > 10 {
			suffix = fmt.Sprintf(",…共%d键", len(keys))
		}
		return "{" + strings.Join(parts, ",") + suffix + "}"
	case []any:
		// 数组同样消耗深度预算：不减深度的话嵌套数组会穿透「深度 2」承诺，
		// 巨型响应的摘要能长到几 KB（对抗审查 LOW）。
		if len(t) == 0 || depth <= 0 {
			return fmt.Sprintf("array[%d]", len(t))
		}
		return fmt.Sprintf("array[%d]，元素%s", len(t), renderShape(t[0], depth-1))
	case string:
		return "str"
	case json.Number:
		return "num"
	case bool:
		return "bool"
	case nil:
		return "null"
	default:
		return "?"
	}
}

// ────────── 点路径取子树 ──────────

// pathSegRe 支持 name、name[3]、name[1:5]、[3]（根即数组）——点路径 + 索引 + 切片，
// 与绑定引擎同一 sub-Turing 纪律（无谓词、无递归下钻、无正则）。
var pathSegRe = regexp.MustCompile(`^([^\[\]]*)(?:\[(\d+)(?::(\d+))?\])?$`)

// resolveResultPath 沿路径下钻缓存的 JSON。错误信息面向模型自纠（指明哪一段失败）。
func resolveResultPath(body []byte, path string) (any, error) {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	var cur any
	if err := dec.Decode(&cur); err != nil {
		return nil, fmt.Errorf("缓存内容不是合法 JSON，请改用 offset 分页读取原文")
	}
	if strings.TrimSpace(path) == "" {
		return cur, nil
	}
	for _, seg := range strings.Split(path, ".") {
		m := pathSegRe.FindStringSubmatch(seg)
		if m == nil {
			return nil, fmt.Errorf("路径段 %q 不合法（支持 name、name[i]、name[i:j]）", seg)
		}
		name, idxs, ends := m[1], m[2], m[3]
		if name != "" {
			obj, ok := cur.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("路径段 %q 处不是对象，无法取键", seg)
			}
			cur, ok = obj[name]
			if !ok {
				return nil, fmt.Errorf("键 %q 不存在（可先不带 path 看结构摘要）", name)
			}
		}
		if idxs != "" {
			arr, ok := cur.([]any)
			if !ok {
				return nil, fmt.Errorf("路径段 %q 处不是数组，无法取下标", seg)
			}
			i, _ := strconv.Atoi(idxs)
			if ends != "" {
				j, _ := strconv.Atoi(ends)
				if i > len(arr) {
					i = len(arr)
				}
				if j > len(arr) {
					j = len(arr)
				}
				if i > j {
					i = j
				}
				cur = arr[i:j]
			} else {
				if i >= len(arr) {
					return nil, fmt.Errorf("下标 %d 越界（数组长度 %d）", i, len(arr))
				}
				cur = arr[i]
			}
		}
	}
	return cur, nil
}

// ────────── read_endpoint_result 工具 ──────────

// readEndpointResultTool 取回被截断的端点响应。本地缓存读取：不打上游、不计费、
// 不占端点限额（msgCap/dailyCap 只数真实上游调用）。
type readEndpointResultTool struct {
	cache *resultCache
	// perRead 单次取回的字符上限，与端点内联同源（由模型窗口派生）：
	// 取回读的是同一个上下文，凭什么比首屏更小或更大。
	perRead int
}

const readEndpointResultSchema = `{
  "type": "object",
  "properties": {
    "handle": {"type": "string", "description": "截断提示里给出的句柄（如 res-7）"},
    "path": {"type": "string", "description": "可选：点路径取子树（如 data.data.items[3] 或 data.items[0:5]），比分页读全文省得多，优先用"},
    "offset": {"type": "integer", "description": "可选：按字符续读的起点（不带 path 时对原文、带 path 时对子树 JSON 文本生效）"},
    "limit": {"type": "integer", "description": "可选：本次返回的最大字符数；不传或超上限时按系统上限（由模型上下文窗口派生）"}
  },
  "required": ["handle"]
}`

func (t *readEndpointResultTool) Name() string { return "read_endpoint_result" }
func (t *readEndpointResultTool) Description() string {
	return "读取此前端点调用被截断的完整响应（本地缓存，不再计费）。优先用 path 取需要的子树；" +
		"内容仍超长时按提示带 offset 续读。句柄 30 分钟内有效。"
}
func (t *readEndpointResultTool) Parameters() json.RawMessage {
	return json.RawMessage(readEndpointResultSchema)
}

// 缓存只保存 TikHub 第三方原文：读取后仍保持 taint；但它不访问网络、不读画像/
// 任务等内部状态，允许作为 taint 后唯一的本地分页续读能力。
func (t *readEndpointResultTool) untrustedResult() bool    { return true }
func (t *readEndpointResultTool) safeAfterUntrusted() bool { return true }
func (t *readEndpointResultTool) allowedAfterUntrusted(state *toolRunState, args json.RawMessage) bool {
	var a readEndpointResultArgs
	if json.Unmarshal(args, &a) != nil {
		return false
	}
	return state.allowsLocalResultHandle(strings.TrimSpace(a.Handle))
}

type readEndpointResultArgs struct {
	Handle string `json:"handle"`
	Path   string `json:"path"`
	Offset int    `json:"offset"`
	Limit  int    `json:"limit"`
}

func (t *readEndpointResultTool) Execute(_ context.Context, userID int64, args json.RawMessage) (string, error) {
	var a readEndpointResultArgs
	if err := json.Unmarshal(args, &a); err != nil {
		return "参数不是合法 JSON，请修正后重试", nil
	}
	entry, ok := t.cache.get(userID, a.Handle)
	if !ok {
		return "句柄不存在或已过期（缓存 30 分钟、服务重启后失效）。请重新调用原端点获取数据。", nil
	}

	text := string(entry.body)
	if strings.TrimSpace(a.Path) != "" {
		sub, err := resolveResultPath(entry.body, a.Path)
		if err != nil {
			return "路径取数失败：" + err.Error(), nil
		}
		raw, err := json.Marshal(sub)
		if err != nil {
			return "子树序列化失败，请改用 offset 分页读取", nil
		}
		text = string(raw)
	}

	limit := a.Limit
	if limit <= 0 || limit > t.perRead {
		limit = t.perRead
	}
	offset := a.Offset
	if offset < 0 {
		offset = 0
	}
	// 按 rune 顺序走出字节区间，不物化 []rune（2MiB 响应会分配 8MB，且分页
	// 每页都重来一次——对抗审查 LOW 的性能形态）。
	out, total, hasMore := sliceRunes(text, offset, limit)
	if offset >= total {
		return fmt.Sprintf("offset=%d 已超出内容长度（共 %d 字符），没有更多内容。", offset, total), nil
	}
	end := offset + utf8.RuneCountInString(out)
	if hasMore {
		out += fmt.Sprintf("\n（第 %d-%d 字符，共 %d。续读：{\"handle\":%q", offset, end, total, a.Handle)
		if a.Path != "" {
			out += fmt.Sprintf(",\"path\":%q", a.Path)
		}
		out += fmt.Sprintf(",\"offset\":%d}）", end)
	}
	return out, nil
}

func (t *readEndpointResultTool) Summarize(args json.RawMessage) string {
	var a readEndpointResultArgs
	_ = json.Unmarshal(args, &a)
	return "读取端点结果缓存 " + a.Handle
}

// sliceRunes 取 s 的 [offset, offset+limit) rune 区间，返回该片段、总 rune 数、
// 是否还有后续。单次顺序扫描、零大对象分配（对比 []rune(s) 对 2MiB 文本的 8MB 分配）。
func sliceRunes(s string, offset, limit int) (piece string, total int, hasMore bool) {
	startByte, endByte := -1, len(s)
	n := 0
	for i := range s {
		if n == offset {
			startByte = i
		}
		if n == offset+limit {
			endByte = i
		}
		n++
	}
	if n == offset {
		startByte = len(s)
	}
	total = n
	if startByte < 0 {
		return "", total, false // offset 越界，调用方按 total 给终止文案
	}
	if endByte < startByte {
		endByte = startByte
	}
	return s[startByte:endByte], total, endByte < len(s)
}

// buildTruncationNote 组装截断提示：大小 + 句柄 + 结构摘要 + 强命令式取回指引。
// 措辞三要素缺一不可（调研结论）：句柄可兑现（有取回工具）、结构摘要（免读全文
// 直接写路径）、显式压制「拿预览当全文」。
func buildTruncationNote(handle string, totalBytes int, body []byte, upstreamTruncated bool, shownRunes int) string {
	var b strings.Builder
	// 缓存完整性必须诚实（对抗审查 MEDIUM）：上游响应超 2MiB 读取上限时缓存的
	// 也只是前缀，宣称「完整数据已缓存」会让模型读完前缀却以为拿到了全量。
	scope := "完整数据已缓存"
	if upstreamTruncated {
		scope = fmt.Sprintf("上游响应超过 %d 字节读取上限，仅缓存了前 %d 字节（**数据不完整**，"+
			"需要全量请缩小查询范围或用分页参数重查）", totalBytes, totalBytes)
	}
	fmt.Fprintf(&b, "\n（响应共 %d 字节，仅展示前 %d 字符。%s：句柄 %s，最长 30 分钟内有效（缓存紧张时可能提前失效）。",
		totalBytes, shownRunes, scope, handle)
	if shape := summarizeJSONStructure(body); shape != "" {
		fmt.Fprintf(&b, "\n结构摘要：%s", truncateRunes(shape, 400))
	}
	// 续读 offset 必须是**本次实际展示量**而非常量上限：累计预算降级后展示量会变小，
	// 用常量会让模型从错误的位置续读、跳过中间一段（预算曲线引入的新坑）。
	fmt.Fprintf(&b, "\n剩余内容必须用 read_endpoint_result 工具获取——优先按路径取子树（最省）："+
		"{\"handle\":%q,\"path\":\"<按上面的结构摘要写>\"}；或分页续读：{\"handle\":%q,\"offset\":%d}。"+
		"不要把上面的预览当作完整数据回答。）", handle, handle, shownRunes)
	return b.String()
}

// 编译期断言：readEndpointResultTool 满足 Tool 接口。
var _ Tool = (*readEndpointResultTool)(nil)
