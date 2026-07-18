package auth_test

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// TestInvariant_SinglePrincipalSource 是不变量 I-A1 的守卫（企业级契约 §1.1）。
//
// 收敛前，「当前用户是谁」被 api/a2a/gate 三处各自复述了一遍——认证一旦要改
// （加租户、换真实登录），要同时改三处且极易漏改，而漏改的后果是「某条入口仍以
// 全局 owner 身份执行」，即越权。本测试把「又冒出第四份副本」变成 CI 红灯。
//
// 判据：除 auth 包自身外，任何 .go 文件都不得同时出现「owner 设置键」与
// 「**调用** UpsertUserByOpenID」——两者并存正是 principal 解析链的指纹。
//
// 判据经过两次收紧（首版误报两个，收紧后为零）：
//   - **剥掉注释再匹配**：cmd/gate/main.go 的注释里提到 UpsertUserByOpenID
//     以说明「解析会写一次库」的取舍，那是文档不是实现。
//   - **只认调用形式 `.UpsertUserByOpenID(`，不认方法定义**：a2a/chat_test.go 的
//     测试替身要实现这个方法，那是替身不是第二个 principal 来源。
//
// 反过来，真要复述解析链就必然写出 `store.UpsertUserByOpenID(ctx, ...)` 这样的调用，
// 仍会被抓住——收紧的是误报，不是漏报。
func TestInvariant_SinglePrincipalSource(t *testing.T) {
	root := repoRoot(t)
	ownerKeyRe := regexp.MustCompile(`SettingKeyOwner|"feishu_owner"`)
	// 调用形式：<接收者>.UpsertUserByOpenID( —— 与 `func (f *x) UpsertUserByOpenID(` 区分开。
	upsertCallRe := regexp.MustCompile(`\w\.UpsertUserByOpenID\(`)

	var offenders []string
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			// 跳过 vendor 化的第三方 SDK 与 .git：不是我们的代码，不受本不变量约束。
			if name := info.Name(); name == "third_party" || name == ".git" || name == "node_modules" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		rel, _ := filepath.Rel(root, path)
		// auth 包是唯一合法的 principal 来源；本测试自身也豁免。
		if strings.HasPrefix(rel, "auth"+string(filepath.Separator)) {
			return nil
		}
		src, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		text := stripComments(string(src))
		if ownerKeyRe.MatchString(text) && upsertCallRe.MatchString(text) {
			offenders = append(offenders, rel)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("遍历仓库失败: %v", err)
	}

	if len(offenders) > 0 {
		t.Errorf("不变量 I-A1 被破坏——以下文件疑似复述了 principal 解析链"+
			"（同时出现 owner 设置键与 UpsertUserByOpenID）：\n  %s\n"+
			"principal 只能有一个来源：auth.PrincipalResolver。"+
			"若确需读取 owner 设置做别的事，请拆分该文件或在此显式豁免并说明理由。",
			strings.Join(offenders, "\n  "))
	}
}

// stripComments 去掉 Go 源码里的行注释与块注释，避免「注释里提到某个方法名」
// 被误判成实现。不做完整词法分析：字符串字面量里的 "//" 会被误剥，但本守卫只
// 关心两个标识符是否并存，误剥字符串不会造成漏报（标识符不会藏在被剥掉的片段里
// 却又不出现在别处）。
func stripComments(src string) string {
	var b strings.Builder
	b.Grow(len(src))
	for i := 0; i < len(src); {
		switch {
		case strings.HasPrefix(src[i:], "//"):
			j := strings.IndexByte(src[i:], '\n')
			if j < 0 {
				return b.String()
			}
			b.WriteByte('\n')
			i += j + 1
		case strings.HasPrefix(src[i:], "/*"):
			j := strings.Index(src[i+2:], "*/")
			if j < 0 {
				return b.String()
			}
			b.WriteByte(' ')
			i += 2 + j + 2
		default:
			b.WriteByte(src[i])
			i++
		}
	}
	return b.String()
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
