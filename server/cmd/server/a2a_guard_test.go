package main

import (
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"
)

// TestA2AMountGuardedByEnabled 装配项守卫（a2a-contract §9.5，仿 workflow/
// registration_test.go 读源码先例）：a2a.Mount 必须且只能出现在
// `if cfg.A2A.Enabled` 条件块内。为什么要这么钉：enabled=false ⇒ /a2a 与
// agent card 404 是契约 §7 的安全边界（零暴露面），把 Mount 挪出条件块后
// go build 与全部测试仍绿（审查突变实验实证）——只有读源码能让这次回归变红。
func TestA2AMountGuardedByEnabled(t *testing.T) {
	_, self, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller 定位不到本测试文件")
	}
	b, err := os.ReadFile(filepath.Join(filepath.Dir(self), "main.go"))
	if err != nil {
		t.Fatalf("读 main.go 失败: %v（装配文件挪窝了？本测试得跟着改）", err)
	}
	src := string(b)

	if n := strings.Count(src, "a2a.Mount("); n != 1 {
		t.Fatalf("a2a.Mount 应恰出现 1 次，实际 %d 次（多处装配 = 守卫失效面扩大）", n)
	}
	guardIdx := regexp.MustCompile(`if cfg\.A2A\.Enabled \{`).FindStringIndex(src)
	if guardIdx == nil {
		t.Fatal("main.go 里找不到 `if cfg.A2A.Enabled {` 条件块（enabled 门被删了？）")
	}
	mountIdx := strings.Index(src, "a2a.Mount(")
	if mountIdx < guardIdx[1] {
		t.Fatal("a2a.Mount 出现在 enabled 条件之前——未被门控")
	}
	// 花括号配平：从条件块开括号起数深度，Mount 位置深度必须 >0（仍在块内）。
	depth := 0
	for i := guardIdx[1] - 1; i < mountIdx; i++ {
		switch src[i] {
		case '{':
			depth++
		case '}':
			depth--
		}
	}
	if depth < 1 {
		t.Fatalf("a2a.Mount 不在 if cfg.A2A.Enabled 块内（深度 %d）——enabled=false 的零暴露面被破坏", depth)
	}
}
