package probe

import (
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"

	"github.com/YouToco/vane/server/profilehint"
	"github.com/YouToco/vane/server/types"
)

// 字面量漂移守卫：直接读源码文本做断言。
//
// 为什么要写这种怪测试——探针与打分链路之间是**跨包的字面量耦合**，编译器抓不到：
//
//   - store/observability.go 的 profileHintPrefix/profileAbsentPrefix 是 scorer.go:205/207
//     那两行 WriteString 的**手抄副本**（不 import 是刻意的：写入方把它们硬编码在各自
//     的 CallMeta/prompt 里，全仓没有共享常量，而本 PR 是只读功能，不重构核心路径）。
//   - profilehint 的 negPrefix 是演化 prompt 规则 2 锁定的句式，探针 ⑤ 拿它比对
//     llm_calls.user_prompt 的画像行。
//
// 漂了的后果不是报错，是**假绿**：SQL 去 LIKE 一个谁都不会写出来的串，
// 永远 0 命中，而 0 命中在探针 ④⑤ 里恰好读作"未命中即正常"。一个恒绿的探针
// 比没有探针更危险——它让契约 §16 要求的部署后复跑变成走过场。
//
// 探针 ④ 的 Unrecognized 自检位能在**线上**把这件事变成红灯，但那时坏东西已经上线了。
// 本测试把它挡在 CI：改 scorer 的人在 push 前就会看见这里红，而不是 Gate 当天。
//
// 断言用宽松正则（允许 gofmt 对齐、允许挪进/挪出 const 块），只钉住"串本身还在"——
// 太严会让无关的格式化变更假红，那种红灯很快就没人看了。

// srcPath 定位仓库内的源码文件。用 runtime.Caller 而非 cwd 相对路径：
// go test 的 cwd 恰好是包目录只是当前行为，不是承诺。
func srcPath(t *testing.T, rel string) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller 定位本测试文件失败，无法找到被守卫的源码")
	}
	repoRoot := filepath.Dir(filepath.Dir(thisFile)) // probe/literals_test.go → probe/ → 仓库根
	return filepath.Join(repoRoot, filepath.FromSlash(rel))
}

func readSrc(t *testing.T, rel string) string {
	t.Helper()
	b, err := os.ReadFile(srcPath(t, rel))
	if err != nil {
		t.Fatalf("读取被守卫的源码 %s 失败: %v", rel, err)
	}
	return string(b)
}

// scorerAbsentLiteral 是 scorer.go:205 无画像分支写死的整句。
// store 的 profileAbsentPrefix 是它的前缀，探针 ④ 靠 LIKE '前缀%' 数 Absent。
const scorerAbsentLiteral = "用户画像：暂无，按通用资讯价值判断。"

// scorerHintPrefix 是 scorer.go:205 与 :207 两个分支**共同**的开头。
// 探针 ④ 的自检位（Unrecognized = Total - Absent - Present）以"两个分支都以它开头"
// 为前提：这个前提破了，恒等式就不再恒为 0。
const scorerHintPrefix = "用户画像："

// TestLiteral_ScorerProfileHint 守卫打分 prompt 的画像行字面量。
func TestLiteral_ScorerProfileHint(t *testing.T) {
	src := readSrc(t, "scorer/scorer.go")

	if !strings.Contains(src, scorerAbsentLiteral) {
		t.Errorf("scorer/scorer.go 里找不到无画像分支的字面量 %q。\n"+
			"若这是有意的改动，请同步 store/observability.go 的 profileAbsentPrefix "+
			"与本文件的常量——否则探针 ④ 会永远数出 Absent=0 并假绿。", scorerAbsentLiteral)
	}
	if !strings.Contains(src, `"`+scorerHintPrefix+`"`) {
		t.Errorf("scorer/scorer.go 里找不到有画像分支的前缀字面量 %q（应为独立的 WriteString 实参）。\n"+
			"探针 ④ 的 Present 计数与 Unrecognized 自检位都锚在它上面。", scorerHintPrefix)
	}
	// 恒等式的结构前提：无画像分支的整句必须以有画像分支的前缀开头，
	// 否则 Present/Absent 的 LIKE 会互相重叠（store/observability.go:172 的 NOT LIKE 靠它排他）。
	if !strings.HasPrefix(scorerAbsentLiteral, scorerHintPrefix) {
		t.Fatalf("用例前提破了：%q 应以 %q 开头", scorerAbsentLiteral, scorerHintPrefix)
	}
}

// TestLiteral_StoreProbePrefixesMatchScorer 守卫**副本与正本相等**。
//
// 上一个用例证明 scorer 侧的串还在，这个用例证明探针抄的那两个常量还等于它。
// 两个用例缺一不可：只验前者的话，有人改了 store 常量、scorer 没动，探针照样假绿。
func TestLiteral_StoreProbePrefixesMatchScorer(t *testing.T) {
	src := readSrc(t, "store/observability.go")

	// 宽松匹配：容忍 gofmt 的等号对齐与 const 块重排，只钉赋值本身。
	for _, tc := range []struct {
		konst string
		want  string
	}{
		{"profileHintPrefix", scorerHintPrefix},
		{"profileAbsentPrefix", "用户画像：暂无"},
	} {
		re := regexp.MustCompile(regexp.QuoteMeta(tc.konst) + `\s*=\s*"([^"]*)"`)
		m := re.FindStringSubmatch(src)
		if m == nil {
			t.Errorf("store/observability.go 里找不到常量 %s 的赋值", tc.konst)
			continue
		}
		if m[1] != tc.want {
			t.Errorf("store/observability.go 的 %s = %q，期望 %q——探针字面量已与 scorer 漂移",
				tc.konst, m[1], tc.want)
		}
		// 探针抄的前缀必须真的是 scorer 那句整句的前缀，否则 LIKE '前缀%' 一条都命中不了。
		if !strings.HasPrefix(scorerAbsentLiteral, m[1]) {
			t.Errorf("%s = %q 不是 scorer 无画像整句 %q 的前缀——LIKE 将 0 命中并假绿",
				tc.konst, m[1], scorerAbsentLiteral)
		}
	}
}

// TestLiteral_ProfilehintNegPrefix 守卫负面清单句式前缀。
//
// negPrefix 是 profilehint 未导出的，probe 引用不到；而探针 ⑤ 的期望串完全由
// profilehint.NegTail 产出，故这里两头都验：源码里的常量文本 + NegTail 的实际行为。
func TestLiteral_ProfilehintNegPrefix(t *testing.T) {
	const want = "不感兴趣："
	src := readSrc(t, "profilehint/profilehint.go")

	re := regexp.MustCompile(`negPrefix\s*=\s*"([^"]*)"`)
	m := re.FindStringSubmatch(src)
	if m == nil {
		t.Fatal("profilehint/profilehint.go 里找不到 negPrefix 的赋值")
	}
	if m[1] != want {
		t.Errorf("negPrefix = %q，期望 %q。\n"+
			"这串由演化 prompt 规则 2 锁定：改了它，历史画像里已有的负面句就再也摘不出来，"+
			"探针 ⑤ 的期望串恒为空 → 恒 yellow（不适用），F1 保尾从此无人验证。", m[1], want)
	}

	// 行为侧闭环：NegTail 摘出来的串必须真的以这个前缀开头。
	// 只验常量文本的话，splitNegTail 被换成别的实现时这里还是绿的。
	got := profilehint.NegTail(&types.Profile{Summary: "关注 AI。" + want + "股市、明星八卦。"})
	if !strings.HasPrefix(got, want) {
		t.Errorf("NegTail 摘出的负面句应以 %q 开头，实际 %q", want, got)
	}
	// 快通道区块头【近期不感兴趣·…】里也有"不感兴趣"却**没有冒号**，
	// 故它不会被 negPrefix 命中。这条不变量是 store 侧第一行锚定之外的第二道保险。
	if strings.Contains("【近期不感兴趣·以下是用户最近标记不感兴趣的内容标题】", want) {
		t.Errorf("scorer 的快通道区块头含 %q——探针 ⑤ 的全文比对将被区块头误命中", want)
	}
}
