package auth

import (
	"go/parser"
	"go/token"
	"strconv"
	"strings"
	"testing"
)

// TestNewInviteCode_Unguessable 钉住邀请码的随机性。
//
// 邀请码是**不变量 I-A2 的唯一载体**——「无有效邀请码不得创建租户」，
// 而它守的是 D3（第三方 API 成本平台全垫付）的财务敞口。码可猜 =
// 把按次计费的 TikHub/LLM 调用对公网开放。
//
// **本用例能验的与不能验的**（探针实测，不是推测）：
// 能验——长度、字母表、不重复、逐位无偏（模偏置或随机源彻底退化会在这里露出来）。
// **不能验——crypto/rand 被换成 math/rand**。Go 1.20+ 的 math/rand 自动随机播种，
// 500 次抽样同样不重复、同样无偏；统计测试在原理上就区分不了 PRNG 与 CSPRNG，
// 差别在"观察到输出后能否预测下一个"。那条退化由下方的导入守卫拦。
func TestNewInviteCode_Unguessable(t *testing.T) {
	const n = 500
	seen := make(map[string]struct{}, n)
	for range n {
		c, err := NewInviteCode()
		if err != nil {
			t.Fatalf("生成失败: %v", err)
		}
		if len(c) != inviteCodeLen {
			t.Fatalf("码长应为 %d，实得 %d（%q）", inviteCodeLen, len(c), c)
		}
		if _, dup := seen[c]; dup {
			t.Fatalf("%d 次生成里出现重复码 %q —— 熵严重不足（换成了 math/rand 或时间戳？）", n, c)
		}
		seen[c] = struct{}{}
	}

	// 逐位统计：某一位若被单个字符垄断，说明取样有偏（如 `b[i] % len(alphabet)` 的模偏置，
	// 或随机源退化）。500 个样本、31 个字符，任一位上单字符占比不该接近 1。
	for pos := range inviteCodeLen {
		freq := map[byte]int{}
		for c := range seen {
			freq[c[pos]]++
		}
		for ch, cnt := range freq {
			if float64(cnt)/float64(len(seen)) > 0.25 {
				t.Errorf("第 %d 位上字符 %q 占了 %.0f%%（%d/%d）—— 取样有偏，随机源可能退化",
					pos, string(ch), float64(cnt)/float64(len(seen))*100, cnt, len(seen))
			}
		}
	}
}

// TestNewInviteCode_NoLookalikeChars：字母表不得含形近字符。
//
// 邀请码要靠人念给人、贴进聊天窗口。0/O、1/I/L 抄错的代价是「码是对的但对方输错了」，
// 表现出来是「邀请码无效」——用户和你都查不出问题在哪，只会怀疑系统坏了。
func TestNewInviteCode_NoLookalikeChars(t *testing.T) {
	for _, bad := range []string{"0", "O", "1", "I", "L"} {
		if strings.Contains(inviteAlphabet, bad) {
			t.Errorf("字母表含形近字符 %q —— 人工转抄时会产生查不出原因的「邀请码无效」", bad)
		}
	}
	c, err := NewInviteCode()
	if err != nil {
		t.Fatalf("生成失败: %v", err)
	}
	for i := range len(c) {
		if !strings.ContainsRune(inviteAlphabet, rune(c[i])) {
			t.Errorf("生成的码含字母表之外的字符 %q：%s", string(c[i]), c)
		}
	}
}

// TestInviteDefaults_ExpiryIsNotForever：默认必须有有效期。
//
// schema 允许 expires_at 为 NULL（永不过期），但默认给 NULL 是危险的：一个永不过期的码
// 一旦流出（截图、转发、离职员工的聊天记录）就是一张永久有效的注册白条，
// 而 D4 准入闸门存在的全部意义正是「财务敞口由发出的邀请数封顶」。
func TestInviteDefaults_ExpiryIsNotForever(t *testing.T) {
	if DefaultInviteExpireDays <= 0 {
		t.Errorf("默认有效期为 %d 天（≤0 意味着永不过期）——默认值不该把财务敞口敞开，"+
			"要永久应当由调用方显式选择", DefaultInviteExpireDays)
	}
}

// TestInviteCode_NoWeakRandomImport 是随机性用例测不到的那一半。
//
// crypto/rand 与 math/rand 的产物在统计上无法区分，所以只能从**来源**上拦：
// 本包一旦导入 math/rand，就意味着某处的随机数可被预测。auth 包里的随机量
// 全是凭证级——邀请码（I-A2 的唯一载体）、密码盐、会话 token——任何一个
// 可预测都等于把对应闸门对公网开放，所以守卫扫的是**整个包**而非单个文件。
//
// 用 go/ast 而非文本 grep：注释、字符串里的 "math/rand" 不该误报，
// 而 `mrand "math/rand"` 这类改名导入不该漏报（探针实测过：改名导入能骗过 grep）。
func TestInviteCode_NoWeakRandomImport(t *testing.T) {
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, ".", nil, parser.ImportsOnly)
	if err != nil {
		t.Fatalf("解析本包失败: %v", err)
	}
	for _, pkg := range pkgs {
		for name, f := range pkg.Files {
			if strings.HasSuffix(name, "_test.go") {
				continue // 测试文件用 math/rand 造夹具是合法的。
			}
			for _, imp := range f.Imports {
				path, err := strconv.Unquote(imp.Path.Value)
				if err != nil {
					continue
				}
				if path == "math/rand" || path == "math/rand/v2" {
					t.Errorf("%s 导入了 %s —— auth 包的随机量全是凭证级（邀请码/密码盐/会话 token），"+
						"可预测的随机数等于把对应闸门对公网开放；请用 crypto/rand",
						name, path)
				}
			}
		}
	}
}
