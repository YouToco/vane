package llm

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// ============================================================
// 配额闸门的**结构守卫**（行为测试见 quota_behavior_test.go）
// ============================================================
//
// 分工要说清楚，否则两边都会以为对方守住了：
//
//   - 行为测试守"闸门有没有效"——额度耗尽时上游零调用。它能杀死"调用了但忽略
//     返回值"这类变异，是主力。
//   - 本文件守"闸门在不在、覆不覆盖全部入口"——它能杀死行为测试杀不掉的那类：
//     **新增一个上游入口而忘了装闸门**。行为测试只测它知道的入口，对不存在的
//     入口无话可说；而"新增入口"恰恰是这套系统最可能的失守方式（第一版就是
//     这么漏掉 DoChat 的）。
//
// 2026-07-19 对抗审查指出本文件前一版的两个缺陷，都已修正：
//  1. 只断言调用存在、不管返回值是否被处置 → 保留调用但忽略返回值即可完全摘除
//     闸门而 27 个包全绿。现由行为测试兜住，本文件另加"返回值必须被判定"的断言。
//  2. 第二条守卫用 ast.Inspect(整个文件) 而非 fn.Body，把 ConsumeQuota 搬进一个
//     从不被调用的孤儿函数仍然全绿。现已限定作用域。

// upstreamEntry 是一个"会向上游发请求"的函数，以及它必须调用的闸门。
type upstreamEntry struct {
	file      string
	fn        string
	upstream  string // 发请求的方法名
	why       string
	estimator string // 估算入参的来源函数（必须出现在闸门之前）
}

// upstreamEntries 是全部会花钱的入口。**新增入口必须登记在这里**，
// 否则 TestInvariant_NoUnguardedUpstreamEntry 会变红。
var upstreamEntries = []upstreamEntry{
	{"do.go", "Do", "Complete",
		"单轮调用：打分 / 出卡 / 画像演化 / playbook 翻译", "estimateTokens"},
	{"chat.go", "DoChat", "Chat",
		"多轮 function calling：agent 循环 / 深挖 / A2A assistant.chat——" +
			"生产实测 prompt 均值 4381、峰值 44871，是最贵的一条", "estimateTokens"},
}

func parseLLMFile(t *testing.T, name string) (*token.FileSet, *ast.File) {
	t.Helper()
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, name, nil, 0)
	if err != nil {
		t.Fatalf("解析 %s 失败: %v", name, err)
	}
	return fset, f
}

func findFunc(t *testing.T, f *ast.File, name string) *ast.FuncDecl {
	t.Helper()
	for _, d := range f.Decls {
		if fd, ok := d.(*ast.FuncDecl); ok && fd.Name.Name == name && fd.Recv == nil {
			return fd
		}
	}
	t.Fatalf("找不到函数 %s —— 它若被改名或拆分，守卫必须重新对准新的入口", name)
	return nil
}

// firstCallPos 返回函数体内某个方法名首次被调用的位置（0 表示没有）。
func firstCallPos(body *ast.BlockStmt, name string) token.Pos {
	var at token.Pos
	ast.Inspect(body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		var got string
		switch fn := call.Fun.(type) {
		case *ast.SelectorExpr:
			got = fn.Sel.Name
		case *ast.Ident:
			got = fn.Name
		}
		if got == name && !at.IsValid() {
			at = call.Pos()
		}
		return true
	})
	return at
}

// TestInvariant_EveryUpstreamEntryIsGuarded：每个会花钱的入口都必须
// 在发请求**之前**按估算预扣额度。
func TestInvariant_EveryUpstreamEntryIsGuarded(t *testing.T) {
	for _, e := range upstreamEntries {
		t.Run(e.fn, func(t *testing.T) {
			_, f := parseLLMFile(t, e.file)
			fn := findFunc(t, f, e.fn)

			gateAt := firstCallPos(fn.Body, "CheckQuota")
			upAt := firstCallPos(fn.Body, e.upstream)
			estAt := firstCallPos(fn.Body, e.estimator)
			recAt := firstCallPos(fn.Body, "ReconcileQuota")

			if !gateAt.IsValid() {
				t.Fatalf("%s 没有调用 CheckQuota —— 这条路径不受配额约束。用途：%s", e.fn, e.why)
			}
			if !upAt.IsValid() {
				t.Fatalf("%s 里找不到 %s 调用 —— 守卫的参照点没了，需重新对准", e.fn, e.upstream)
			}
			if gateAt > upAt {
				t.Errorf("%s 的 CheckQuota 在 %s 之后 —— 钱已经花了才检查，闸门形同虚设",
					e.fn, e.upstream)
			}
			if !estAt.IsValid() || estAt > gateAt {
				t.Errorf("%s 必须在预扣前用 %s 算出估算量。缺了它意味着预扣的是常量，"+
					"而象征性的小额预扣正是第一版超发 4.9 倍的成因", e.fn, e.estimator)
			}
			if !recAt.IsValid() || recAt < upAt {
				t.Errorf("%s 必须在调用后 ReconcileQuota 对账实际用量。"+
					"只预扣估算而不对账，桶里记的永远是猜测值", e.fn)
			}
		})
	}
}

// TestInvariant_GateResultIsHonored：闸门的返回值必须被真正判定，而不是只调不看。
//
// 这条针对的是对抗审查实际做出来的那个变异：**保留 CheckQuota 调用、位置也对，
// 但不理会返回值继续往下走**——前一版守卫完全看不见它，27 个包全绿。
// 判据是"闸门调用必须出现在 if 语句的初始化子句里，且函数体内有 return"，
// 因为只有 return 才能真的挡住下游那次花钱的调用。
func TestInvariant_GateResultIsHonored(t *testing.T) {
	for _, e := range upstreamEntries {
		t.Run(e.fn, func(t *testing.T) {
			_, f := parseLLMFile(t, e.file)
			fn := findFunc(t, f, e.fn)

			guarded := false
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				ifs, ok := n.(*ast.IfStmt)
				if !ok || ifs.Init == nil {
					return true
				}
				if !firstCallPos(&ast.BlockStmt{List: []ast.Stmt{ifs.Init}}, "CheckQuota").IsValid() {
					return true
				}
				// 该 if 的分支里必须有 return，否则拦不住下面那次调用。
				ast.Inspect(ifs.Body, func(m ast.Node) bool {
					if _, ok := m.(*ast.ReturnStmt); ok {
						guarded = true
					}
					return true
				})
				return true
			})

			if !guarded {
				t.Errorf("%s 里 CheckQuota 的返回值没有导向 return —— "+
					"「调用了闸门」不等于「闸门起作用」。对抗审查证明过：保留调用但忽略"+
					"返回值可以完全摘除配额而全仓测试无一变红", e.fn)
			}
		})
	}
}

// TestInvariant_NoUnguardedUpstreamEntry：llm 包里不得存在**未登记**的上游入口。
//
// 这是本文件存在的主要理由。行为测试只能测它知道的入口，对"有人新加了一个
// 发请求的函数"无话可说——而那正是第一版漏掉 DoChat 的方式：当时的注释信誓旦旦
// 写着"llm.Do 是全部 LLM 调用的唯一咽喉"，而 chat.go 里的 DoChat 就在隔壁。
func TestInvariant_NoUnguardedUpstreamEntry(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("列文件失败: %v", err)
	}
	registered := map[string]bool{}
	for _, e := range upstreamEntries {
		registered[e.fn] = true
	}
	// 发请求的底层方法：它们是 Client 上真正打 HTTP 的那两个。
	lowLevel := map[string]bool{"Complete": true, "Chat": true}

	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		src, err := os.ReadFile(name)
		if err != nil {
			t.Fatalf("读 %s 失败: %v", name, err)
		}
		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, name, src, 0)
		if err != nil {
			t.Fatalf("解析 %s 失败: %v", name, err)
		}
		for _, d := range f.Decls {
			fd, ok := d.(*ast.FuncDecl)
			// 只看包级导出函数：Client 自己的方法（Complete/Chat）是被包装的对象，
			// 闸门装在包装层而不是它们身上。
			if !ok || fd.Recv != nil || !fd.Name.IsExported() || fd.Body == nil {
				continue
			}
			for m := range lowLevel {
				if firstCallPos(fd.Body, m).IsValid() && !registered[fd.Name.Name] {
					t.Errorf("%s 里的 %s 会调用上游 %s，但没有登记进 upstreamEntries —— "+
						"新增的花钱入口必须登记并装配额闸门。第一版正是这样漏掉 DoChat 的："+
						"当时注释还写着「llm.Do 是唯一咽喉」，而 DoChat 就在隔壁文件",
						name, fd.Name.Name, m)
				}
			}
		}
	}
}
