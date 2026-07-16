package workflow

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime"
	"sort"
	"testing"
)

// Activity 注册完整性守卫。
//
// 为什么需要这么一个怪测试：Temporal 的 Activity 注册是**运行时按名查表**，
// 漏注册一个 Activity 编译器抓不到、go vet 抓不到、单测也抓不到（testsuite 的
// TestActivityEnvironment 各测各的，不看生产装配）。它只在**线上**表现为
// ActivityNotRegisteredError，而本 pipeline 恰好又刻意吞掉记账类活动的错误
// （无内容可推必须仍是正常终态），于是漏注册的净效果是：
// 功能完全不工作，日志只有一行 Warn，Temporal 显示 Completed，测试全绿。
//
// 这不是假设。008 加 RecordEmptyBatch 时就是这么漏的——整个"空批次可见化"
// 在生产上是死代码，库里依旧零行，与没做这个功能逐字一致，由合并前的怀疑者
// 审查抓出。契约 §13 早已写明这个失效模式（"漏注册=每批推送静默拖慢数分钟——
// 必须显式列出"），也没能挡住它：光靠"记得写"是挡不住的，得让它响。
//
// 手法沿用 probe/literals_test.go 的先例：跨包的字面量耦合编译器管不着时，
// 就直接读源码比对。丑，但它咬得住——而"咬得住"是这里唯一重要的性质。
//
// 为什么不干脆改成 w.RegisterActivity(activities)（注册整个结构体，天然免疫）：
// 契约 §13 明确裁定"main 是逐个注册…必须显式列出"（审查一致性 MEDIUM）。
// 本测试在不推翻那个裁定的前提下补上它缺的那半边——契约要的是"显式"，
// 而显式的代价就是会漏，那就让漏这件事变红。

// activityRegisterRe 抓 main.go 里的 w.RegisterActivity(activities.Xxx)。
var activityRegisterRe = regexp.MustCompile(`w\.RegisterActivity\(activities\.(\w+)\)`)

// nonActivityMethods 是 *Activities 上**不是** Temporal Activity 的导出方法。
// 目前为空——若将来给 Activities 加了导出的辅助方法（非 Activity），登记在这里，
// 并写清为什么它不该被注册。留空且有注释，比没有这个口子更诚实。
var nonActivityMethods = map[string]bool{}

func TestEveryActivityIsRegisteredInMain(t *testing.T) {
	// 反射拿 *Activities 的全部导出方法 = 应当被注册的全集。
	var want []string
	rt := reflect.TypeOf(&Activities{})
	for i := 0; i < rt.NumMethod(); i++ {
		name := rt.Method(i).Name
		if nonActivityMethods[name] {
			continue
		}
		want = append(want, name)
	}
	sort.Strings(want)
	if len(want) == 0 {
		t.Fatal("反射没拿到任何 Activity 方法——测试本身坏了，不是代码对了")
	}

	src := readMainSource(t)
	var got []string
	for _, m := range activityRegisterRe.FindAllStringSubmatch(src, -1) {
		got = append(got, m[1])
	}
	sort.Strings(got)
	if len(got) == 0 {
		t.Fatal("没能从 cmd/server/main.go 解析出任何 RegisterActivity 调用——" +
			"要么装配方式变了（那本测试得跟着改），要么正则失效了。别忽略这条。")
	}

	wantSet := map[string]bool{}
	for _, n := range want {
		wantSet[n] = true
	}
	gotSet := map[string]bool{}
	for _, n := range got {
		gotSet[n] = true
	}

	for _, n := range want {
		if !gotSet[n] {
			t.Errorf("Activity %s 没有在 cmd/server/main.go 注册。"+
				"后果不是启动失败，是线上**静默不工作**——Temporal 按名查表找不到它，"+
				"而 workflow 侧对记账类活动是吞错只 Warn 的。补一行 "+
				"w.RegisterActivity(activities.%s)。", n, n)
		}
	}
	for _, n := range got {
		if !wantSet[n] {
			t.Errorf("cmd/server/main.go 注册了 activities.%s，但 *Activities 上没有这个"+
				"导出方法——清单已过期（方法被删/改名了？）", n)
		}
	}
}

// TestActivitiesMethodsLookLikeActivities 兜住上面那个测试的前提：
// 它假设"*Activities 的导出方法 == Activity 全集"。若哪天有人给 Activities
// 加了个导出的辅助方法，上面会误报它没注册——本测试让那个假设自己也受检，
// 顺带逼着加方法的人去 nonActivityMethods 里登记并写明理由。
func TestActivitiesMethodsLookLikeActivities(t *testing.T) {
	ctxType := reflect.TypeOf((*context.Context)(nil)).Elem()
	errType := reflect.TypeOf((*error)(nil)).Elem()

	rt := reflect.TypeOf(&Activities{})
	for i := 0; i < rt.NumMethod(); i++ {
		m := rt.Method(i)
		if nonActivityMethods[m.Name] {
			continue
		}
		ft := m.Type
		// 方法值签名：func(*Activities, context.Context, In) (Out..., error)
		if ft.NumIn() < 2 || ft.In(1) != ctxType {
			t.Errorf("%s 的第一个参数不是 context.Context——它要么不是 Activity"+
				"（那就登记进 nonActivityMethods 并写明理由），要么签名写错了", m.Name)
			continue
		}
		if ft.NumOut() == 0 || ft.Out(ft.NumOut()-1) != errType {
			t.Errorf("%s 的最后一个返回值不是 error——Activity 必须能报错", m.Name)
		}
	}
}

// readMainSource 读 cmd/server/main.go 的源码。用 runtime.Caller 定位而非相对
// 路径拼接：go test 的 cwd 是包目录，但别的运行方式（IDE、覆盖率工具）未必。
func readMainSource(t *testing.T) string {
	t.Helper()
	_, self, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller 定位不到本测试文件")
	}
	p := filepath.Join(filepath.Dir(self), "..", "cmd", "server", "main.go")
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatalf("读 %s 失败: %v（装配文件挪窝了？本测试得跟着改）", p, err)
	}
	return string(b)
}
