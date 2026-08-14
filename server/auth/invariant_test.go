package auth_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// exemptMarker 是文件级豁免标记，必须写成注释。带此标记的文件跳过本守卫。
//
// **豁免不是免罪牌，是一处必须写明理由的例外**：标记要求写在被豁免文件自身
// （而不是攒在本测试的白名单里），谁将来想在该文件加 principal 解析，一眼就能
// 看见豁免存在及其理由，而不是改完提交才被 CI 拦——或更糟，因为文件恰好在
// 白名单里而根本不被拦。当前唯一豁免是 feishu/handler.go，理由见该文件头部。
//
// 已知残留局限（诚实记录，不假装守住了）：本守卫做的是语法级判定，看不到数据流，
// 因此**无法**分辨豁免文件里将来新增的代码是不是真的 principal 复述。
// 豁免文件是本守卫的盲区，加豁免要慎重。
const exemptMarker = "//go:principal-exempt"

// TestInvariant_SinglePrincipalSource 是不变量 I-A1 的守卫（企业级契约 §1.1）。
//
// 收敛前，「当前用户是谁」被 api/a2a/gate 三处各自复述了一遍——认证一旦要改
// （加租户、换真实登录），要同时改三处且极易漏改，而漏改的后果是「某条入口仍以
// 全局 owner 身份执行」，即越权。本测试把「又冒出第四份副本」变成 CI 红灯。
//
// 判据：除 auth 包与显式豁免文件外，任何 .go 文件都不得同时出现
// ① 对 owner 设置键的引用（标识符 SettingKeyOwner/settingKeyOwner，或**任何**
// 字面量其值含 feishu_owner）与 ② 对 UpsertUserByOpenID 的**调用**。二者并存
// 正是 principal 解析链的指纹。
//
// # 为什么是 AST 而不是正则
//
// 首版用「剥注释 + 正则」，被对抗审查用实证打穿两次，两次都是 fail-open（静默放行）：
//
//  1. 剥注释器不认字符串字面量：字符串里出现 `/*` 而无闭合时，它会**丢弃文件剩余
//     全部内容**。审查实测本仓库当日就有两个文件被静默截断——fetcher/fetcher.go
//     的 Accept 头含 `*/*;q=0.5`（丢掉 87% 字节）、store/migrate_test.go 的
//     `"migrations/*.sql"`。任何加在截断点之后的复述都能 CI 全绿。
//     首版注释还断言「误剥字符串不会造成漏报」——恰好说反了。
//  2. 正则写死双引号 "feishu_owner"，而 store 包里最自然的复述写法是原生 SQL
//     单引号 `WHERE key = 'feishu_owner'`，直接绕过。（且 feishu 导入 store、
//     反向成环，未来 store/owner.go 只能硬编码 key，必然落进这个盲区。）
//
// AST 从根上消掉这两类问题：字面量与标识符由解析器区分，引号形式无关；
// 方法**调用**（CallExpr+SelectorExpr）与方法**定义**（FuncDecl）天然是不同节点。
func TestInvariant_SinglePrincipalSource(t *testing.T) {
	root := repoRoot(t)
	fset := token.NewFileSet()

	var offenders []string
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			// 跳过 vendor 化的第三方 SDK 与非源码目录：不是我们的代码，不受本不变量约束。
			switch info.Name() {
			case "third_party", ".git", "node_modules", "web":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		rel, _ := filepath.Rel(root, path)
		// auth 包是唯一合法的 principal 来源；本测试自身也在其中。
		if strings.HasPrefix(rel, "auth"+string(filepath.Separator)) {
			return nil
		}

		f, perr := parser.ParseFile(fset, path, nil, parser.ParseComments)
		if perr != nil {
			// **解析失败必须响亮失败，不能静默跳过**——静默跳过正是首版被打穿的
			// 那种 fail-open 姿势：一个永远不报警的守卫等于没有守卫。
			t.Errorf("守卫无法解析 %s（无法判定是否违反 I-A1）: %v", rel, perr)
			return nil
		}
		// 豁免标记按注释节点判定，不按裸文本搜索：否则字符串字面量里出现该标记
		// 就能骗过豁免——那是首版同一类 bug 的翻版。
		if hasExemptComment(f) {
			return nil
		}
		if refsOwnerKey(f) && callsUpsert(f) {
			offenders = append(offenders, rel)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("遍历仓库失败: %v", err)
	}

	if len(offenders) > 0 {
		t.Errorf("不变量 I-A1 被破坏——以下文件疑似复述了 principal 解析链"+
			"（同时引用 owner 设置键与调用 UpsertUserByOpenID）：\n  %s\n"+
			"principal 只能有一个来源：auth.PrincipalResolver。若该文件确有正当理由"+
			"同时出现二者（如 owner 捕获路径），请在文件头加 %s 并写明理由。",
			strings.Join(offenders, "\n  "), exemptMarker)
	}
}

// hasExemptComment 判定文件是否带豁免标记（只认注释节点）。
func hasExemptComment(f *ast.File) bool {
	for _, cg := range f.Comments {
		for _, c := range cg.List {
			if strings.Contains(c.Text, exemptMarker) {
				return true
			}
		}
	}
	return false
}

// refsOwnerKey 判定文件是否引用了 owner 设置键。覆盖三种写法：
//   - 标识符 SettingKeyOwner / settingKeyOwner（含 feishu.SettingKeyOwner 选择器）
//   - 任何字符串字面量，其**值**含 feishu_owner——引号形式无关，
//     故 "feishu_owner"、`… WHERE key = 'feishu_owner'` 都能命中
func refsOwnerKey(f *ast.File) bool {
	found := false
	ast.Inspect(f, func(n ast.Node) bool {
		if found {
			return false
		}
		switch x := n.(type) {
		case *ast.Ident:
			if strings.EqualFold(x.Name, "SettingKeyOwner") {
				found = true
			}
		case *ast.SelectorExpr:
			if strings.EqualFold(x.Sel.Name, "SettingKeyOwner") {
				found = true
			}
		case *ast.BasicLit:
			if x.Kind != token.STRING {
				return true
			}
			// Unquote 同时处理解释型字符串与反引号原生字符串；失败则退回原文匹配，
			// 宁可多报也不漏报。
			s, uerr := strconv.Unquote(x.Value)
			if uerr != nil {
				s = x.Value
			}
			if strings.Contains(s, "feishu_owner") {
				found = true
			}
		}
		return true
	})
	return found
}

// callsUpsert 判定文件是否**调用**了 UpsertUserByOpenID。
// 只认 CallExpr 上的 SelectorExpr——方法定义是 FuncDecl，天然不会命中，
// 故测试替身实现该方法不算违规（首版正则要靠特判才能区分，AST 免费得到）。
func callsUpsert(f *ast.File) bool {
	found := false
	ast.Inspect(f, func(n ast.Node) bool {
		if found {
			return false
		}
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		if sel, ok := call.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "UpsertUserByOpenID" {
			found = true
		}
		return true
	})
	return found
}

// repoRoot 从当前包目录（auth/）上溯到含 go.mod 的仓库根。
func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("取工作目录失败: %v", err)
	}
	for i := 0; i < 10; i++ {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	t.Fatal("未找到仓库根（go.mod）")
	return ""
}
