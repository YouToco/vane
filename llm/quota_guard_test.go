package llm

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
)

// TestInvariant_DoChecksQuotaBeforeUpstream 锁住配额闸门的位置：
// **CheckQuota 必须在 c.Complete 之前**。
//
// 为什么用结构守卫而不是行为测试：Recorder 持有具体的 *store.Store，
// 没有接口可替身，而拉起真库只为验一次调用顺序不划算。这条守卫覆盖的
// 恰好是行为测试盖不到的那个缺口——「闸门还在不在、还在不在花钱之前」。
//
// 它要防的两种改动，都不会有任何编译或测试报错：
//  1. 删掉 CheckQuota（比如重构时觉得"这里不该有 store 依赖"）→ 配额彻底失效，
//     而 store 层那些 TryConsume 用例照样全绿，因为它们测的是桶本身。
//  2. 把 CheckQuota 挪到 Complete 之后 → 钱已经花了才检查，闸门形同虚设。
//
// llm.Do 是全部 LLM 调用的唯一咽喉（打分/出卡/agent/深挖/演化/A2A 都从这过），
// 所以这一个点的失守等于全线失守。
func TestInvariant_DoChecksQuotaBeforeUpstream(t *testing.T) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "do.go", nil, 0)
	if err != nil {
		t.Fatalf("解析 do.go 失败: %v", err)
	}

	var fn *ast.FuncDecl
	for _, d := range f.Decls {
		if fd, ok := d.(*ast.FuncDecl); ok && fd.Name.Name == "Do" && fd.Recv == nil {
			fn = fd
		}
	}
	if fn == nil {
		t.Fatal("找不到 llm.Do —— 它若被改名或拆分，本守卫必须重新对准新的咽喉点")
	}

	// 记录两个调用各自首次出现的位置（用源码偏移比较先后）。
	var quotaAt, completeAt token.Pos
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		switch sel.Sel.Name {
		case "CheckQuota":
			if !quotaAt.IsValid() {
				quotaAt = call.Pos()
			}
		case "Complete":
			if !completeAt.IsValid() {
				completeAt = call.Pos()
			}
		}
		return true
	})

	if !quotaAt.IsValid() {
		t.Fatal("llm.Do 里没有 CheckQuota —— 配额闸门被删了。" +
			"llm.Do 是全部 LLM 调用的唯一咽喉，这里失守等于配额全线失效，" +
			"而 store 层的桶用例会照样全绿（它们测的是桶，不是有没有人用桶）")
	}
	if !completeAt.IsValid() {
		t.Fatal("llm.Do 里没有 Complete 调用 —— 本守卫的参照点没了，需重新对准")
	}
	if quotaAt > completeAt {
		t.Error("CheckQuota 出现在 Complete 之后 —— 钱已经花了才检查额度，闸门形同虚设")
	}
}

// TestInvariant_DoConsumesActualTokens：事后必须扣真实用量。
//
// 只有事前探针（扣 1）而没有事后扣减，桶几乎永远扣不空——一次几千 token 的调用
// 只扣 1 个令牌，配额上限就形同放大了几千倍，账单照样失控。
func TestInvariant_DoConsumesActualTokens(t *testing.T) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "do.go", nil, 0)
	if err != nil {
		t.Fatalf("解析 do.go 失败: %v", err)
	}
	found := false
	ast.Inspect(f, func(n ast.Node) bool {
		if sel, ok := n.(*ast.SelectorExpr); ok && sel.Sel.Name == "ConsumeQuota" {
			found = true
		}
		return true
	})
	if !found {
		t.Error("llm.Do 里没有 ConsumeQuota —— 只扣事前探针的 1 个令牌，" +
			"而真实调用动辄几千 token，配额上限等于被放大几千倍，账单照样失控")
	}
}
