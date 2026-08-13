package workflow

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime"
	"testing"
)

// Production registration is an admission boundary, not a reflection of every
// historical method that remains compilable for replay. The live worker must
// expose the complete V3 activity set and no retired V1/V2 activity.

// activityRegisterRe 抓 main.go 里的 w.RegisterActivity(activities.Xxx)。
var activityRegisterRe = regexp.MustCompile(`w\.RegisterActivity\(activities\.(\w+)\)`)
var workflowRegisterRe = regexp.MustCompile(`w\.RegisterWorkflow\(workflow\.(\w+)\)`)
var periodicActivityRegisterRe = regexp.MustCompile(`w\.RegisterActivity\(periodicActivities\.(\w+)\)`)
var periodicWorkflowRegisterRe = regexp.MustCompile(`w\.RegisterWorkflow\(periodicbrief\.(\w+)\)`)

var productionResearchActivitiesV3 = map[string]bool{
	"PrepareResearchRunV3":      true,
	"PlanResearchRunV3":         true,
	"ExecuteResearchStepV3":     true,
	"SynthesizeResearchBriefV3": true,
	"DeliverResearchBriefV3":    true,
}

// nonActivityMethods 是 *Activities 上**不是** Temporal Activity 的导出方法。
// 目前为空——若将来给 Activities 加了导出的辅助方法（非 Activity），登记在这里，
// 并写清为什么它不该被注册。留空且有注释，比没有这个口子更诚实。
var nonActivityMethods = map[string]bool{}

func TestProductionWorkerRegistersOnlyResearchActivitiesV3(t *testing.T) {
	src := readMainSource(t)
	got := map[string]bool{}
	for _, m := range activityRegisterRe.FindAllStringSubmatch(src, -1) {
		got[m[1]] = true
	}
	if len(got) == 0 {
		t.Fatal("没能从 cmd/server/main.go 解析出任何 RegisterActivity 调用——" +
			"要么装配方式变了（那本测试得跟着改），要么正则失效了。别忽略这条。")
	}
	for name := range productionResearchActivitiesV3 {
		if !got[name] {
			t.Errorf("current V3 Activity %s is not registered", name)
		}
	}
	for name := range got {
		if !productionResearchActivitiesV3[name] {
			t.Errorf("retired non-V3 Activity %s is still registered", name)
		}
	}
}

func TestRetiredPushWorkflowIsNotRegisteredInProduction(t *testing.T) {
	src := readMainSource(t)
	got := map[string]bool{}
	for _, m := range workflowRegisterRe.FindAllStringSubmatch(src, -1) {
		got[m[1]] = true
	}
	for _, name := range []string{
		"ResearchShadowWorkflowV3", "ResearchScheduledWorkflowV3",
		"AgentFirstRetentionClockWorkflowV1",
	} {
		if !got[name] {
			t.Errorf("current V3 Workflow %s is not registered", name)
		}
	}
	if got["PushPipelineWorkflow"] {
		t.Error("retired PushPipelineWorkflow remains registered in production")
	}
}

func TestProductionWorkerKeepsPeriodicBriefRuntime(t *testing.T) {
	src := readMainSource(t)
	workflows := periodicWorkflowRegisterRe.FindAllStringSubmatch(src, -1)
	if len(workflows) != 1 || workflows[0][1] != "WorkflowV1" {
		t.Fatalf("periodic workflow registrations=%v, want only WorkflowV1", workflows)
	}
	got := map[string]bool{}
	for _, match := range periodicActivityRegisterRe.FindAllStringSubmatch(src, -1) {
		got[match[1]] = true
	}
	want := map[string]bool{
		"SynthesizePeriodicBriefV1": true,
		"DeliverPeriodicBriefV1":    true,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("periodic activity registrations=%v, want %v", got, want)
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
