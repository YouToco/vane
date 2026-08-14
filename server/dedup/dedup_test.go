package dedup

import (
	"testing"

	"github.com/YouToco/vane/server/types"
)

func TestContentHash(t *testing.T) {
	a := types.ContentItem{Title: "Hello", URL: "https://x.com/1"}
	b := types.ContentItem{Title: "Hello", URL: "https://x.com/1"}
	c := types.ContentItem{Title: "Hello", URL: "https://x.com/2"}

	if ContentHash(a) != ContentHash(b) {
		t.Error("相同 title+url 应得到相同 hash")
	}
	if ContentHash(a) == ContentHash(c) {
		t.Error("不同 url 应得到不同 hash")
	}
	if len(ContentHash(a)) != 64 {
		t.Errorf("sha256 hex 应为 64 字符，实际 %d", len(ContentHash(a)))
	}

	// 防拼接碰撞：("ab","c") 与 ("a","bc") 不应撞。
	h1 := ContentHash(types.ContentItem{Title: "ab", URL: "c"})
	h2 := ContentHash(types.ContentItem{Title: "a", URL: "bc"})
	if h1 == h2 {
		t.Error("拼接边界应被分隔符隔开，不应碰撞")
	}
}

func TestSimhash_IdenticalIsZeroDistance(t *testing.T) {
	text := "The quick brown fox jumps over the lazy dog"
	if d := HammingDistance(Simhash(text), Simhash(text)); d != 0 {
		t.Errorf("相同文本距离应为 0，实际 %d", d)
	}
}

func TestSimhash_SmallEditSmallDistance(t *testing.T) {
	base := "Apple releases new iPhone with faster chip and better camera this autumn"
	edited := "Apple releases new iPhone with faster chip and better camera this fall"

	full := "Global stock markets tumble as central banks signal aggressive rate hikes ahead"

	near := HammingDistance(Simhash(base), Simhash(edited))
	far := HammingDistance(Simhash(base), Simhash(full))

	if near == 0 {
		t.Error("改了一个词，距离不应恰为 0（但应很小）")
	}
	if near >= far {
		t.Errorf("小改动距离(%d)应明显小于完全不同(%d)", near, far)
	}
	if near > 10 {
		t.Errorf("单词改动距离应较小，实际 %d 偏大", near)
	}
}

func TestSimhash_DifferentTextLargeDistance(t *testing.T) {
	a := Simhash("machine learning models require large amounts of training data")
	b := Simhash("the weather today is sunny with a gentle breeze from the west")
	if d := HammingDistance(a, b); d < 15 {
		t.Errorf("完全不同的文本汉明距离应较大，实际 %d", d)
	}
}

func TestSimhash_Chinese(t *testing.T) {
	base := "苹果发布全新手机搭载更快芯片"
	edited := "苹果发布全新手机搭载更强芯片" // 改一字
	diff := "全球股市今日大幅下跌投资者恐慌"

	near := HammingDistance(Simhash(base), Simhash(edited))
	far := HammingDistance(Simhash(base), Simhash(diff))
	if HammingDistance(Simhash(base), Simhash(base)) != 0 {
		t.Error("相同中文文本距离应为 0")
	}
	if near >= far {
		t.Errorf("中文小改动距离(%d)应小于完全不同(%d)", near, far)
	}
}

func TestSimhash_Empty(t *testing.T) {
	if Simhash("") != 0 {
		t.Error("空文本 simhash 应为 0")
	}
	if Simhash("   \n\t") != 0 {
		t.Error("纯分隔符文本 simhash 应为 0")
	}
}

func TestHammingDistance(t *testing.T) {
	if HammingDistance(0, 0) != 0 {
		t.Error("0^0 popcount 应为 0")
	}
	if HammingDistance(0, 7) != 3 {
		t.Errorf("0^7 应有 3 个 1 位，实际 %d", HammingDistance(0, 7))
	}
	// 最高位差异（负数场景）：-1 = 全 1，0 = 全 0，距离应为 64。
	if HammingDistance(-1, 0) != 64 {
		t.Errorf("全 1 与全 0 距离应为 64，实际 %d", HammingDistance(-1, 0))
	}
}

func TestIsNearDup(t *testing.T) {
	// 阈值 3 是"换皮"级别的近似（个别措辞/标点差异），面向真实文章长度的文本。
	// 短句改一个内容词会产生较大距离，不属于近似重复——这正是期望行为。
	const article = "The central bank announced today that it would raise interest rates by fifty " +
		"basis points in an effort to combat rising inflation across the economy while maintaining " +
		"employment levels and supporting continued economic growth over the coming year"
	// 同一篇报道被另一家源转述，仅一个词不同（maintaining→sustaining）。
	const restated = "The central bank announced today that it would raise interest rates by fifty " +
		"basis points in an effort to combat rising inflation across the economy while sustaining " +
		"employment levels and supporting continued economic growth over the coming year"

	base := Simhash(article)
	restate := Simhash(restated)
	other := Simhash("Scientists discover a new species of deep sea fish living near an ocean trench")

	recent := []int64{other, restate}
	if !IsNearDup(base, recent, 3) {
		t.Error("应识别出与转述版本近似重复（阈值 3 内）")
	}
	if IsNearDup(base, []int64{other}, 3) {
		t.Error("与 other 差异大，阈值 3 内不应判为重复")
	}
	if IsNearDup(base, nil, 3) {
		t.Error("空 recent 不应判为重复")
	}
	// 完全相同必然命中（距离 0 ≤ 任意非负阈值）。
	if !IsNearDup(base, []int64{base}, 0) {
		t.Error("完全相同应在阈值 0 命中")
	}
}
