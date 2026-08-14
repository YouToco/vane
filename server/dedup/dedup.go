// Package dedup 提供内容去重的两级判定：
//   - content_hash：sha256(title+url) 精确去重，命中即同一条内容；
//   - simhash：64-bit 局部敏感哈希，近似去重（改动少量文字仍判为重复），
//     用于同一事件被多家源转述、标题微调等"换皮"内容的 72h 窗口拦截。
//
// simhash 自实现（不引第三方库）：分词 → 每词 64-bit 哈希 → 按位加权累加
// → 符号量化。汉明距离越小越相似，阈值内即视为近似重复。
package dedup

import (
	"crypto/sha256"
	"encoding/hex"
	"hash/fnv"
	"math/bits"
	"strings"
	"unicode"

	"github.com/YouToco/vane/server/types"
)

// ContentHash 计算精确去重键 = hex(sha256(title + "\n" + url))。
// 用换行分隔而非直接拼接，避免 "ab"+"c" 与 "a"+"bc" 碰撞成同一串。
func ContentHash(item types.ContentItem) string {
	sum := sha256.Sum256([]byte(item.Title + "\n" + item.URL))
	return hex.EncodeToString(sum[:])
}

// Simhash 计算文本的 64-bit simhash 指纹。
// 空文本返回 0；两段相同文本必然得到相同指纹（HammingDistance=0）。
func Simhash(text string) int64 {
	tokens := tokenize(text)
	if len(tokens) == 0 {
		return 0
	}

	// v[i] 累加第 i 位的加权投票：该位为 1 的词 +1，为 0 的词 -1。
	// 高频词自然通过多次出现获得更大权重，无需显式统计词频。
	var v [64]int
	for _, tok := range tokens {
		h := fnv.New64a()
		_, _ = h.Write([]byte(tok))
		hash := h.Sum64()
		for i := 0; i < 64; i++ {
			if hash&(uint64(1)<<uint(i)) != 0 {
				v[i]++
			} else {
				v[i]--
			}
		}
	}

	// 符号量化：v[i] >= 0 该位取 1。结果按 bit pattern 存入 int64（可能为负，
	// 对应最高位为 1，符合 DB BIGINT 语义）。
	var sh uint64
	for i := 0; i < 64; i++ {
		if v[i] >= 0 {
			sh |= uint64(1) << uint(i)
		}
	}
	return int64(sh)
}

// HammingDistance 返回两个 simhash 指纹不同的比特位数 = popcount(a ^ b)。
func HammingDistance(a, b int64) int {
	return bits.OnesCount64(uint64(a) ^ uint64(b))
}

// IsNearDup 判断指纹 sh 是否与 recent 中任一指纹的汉明距离 ≤ threshold。
// threshold 建议 3（契约 B4）：距离 0 完全相同，≤3 视为换皮重复。
func IsNearDup(sh int64, recent []int64, threshold int) bool {
	for _, r := range recent {
		if HammingDistance(sh, r) <= threshold {
			return true
		}
	}
	return false
}

// tokenize 把文本切成用于 simhash 的 token。
// 规则：统一小写；连续的字母/数字聚成一个词；每个 CJK 表意文字单独成词
// （中文无空格分隔，逐字成 token 才能让 simhash 对中文敏感）；其余字符作分隔。
func tokenize(text string) []string {
	text = strings.ToLower(text)
	var tokens []string
	var buf []rune

	flush := func() {
		if len(buf) > 0 {
			tokens = append(tokens, string(buf))
			buf = buf[:0]
		}
	}

	for _, r := range text {
		switch {
		case isCJK(r):
			flush()
			tokens = append(tokens, string(r))
		case unicode.IsLetter(r) || unicode.IsDigit(r):
			buf = append(buf, r)
		default:
			flush()
		}
	}
	flush()
	return tokens
}

// isCJK 判定是否为常用中日韩表意文字（基本区 + 扩展 A）。
func isCJK(r rune) bool {
	return (r >= 0x4E00 && r <= 0x9FFF) || (r >= 0x3400 && r <= 0x4DBF)
}
