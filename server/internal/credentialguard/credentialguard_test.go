package credentialguard

import "testing"

func TestContainsCredentialRejectsHighConfidenceMaterial(t *testing.T) {
	for _, input := range []string{
		"-----BEGIN PRIVATE KEY-----\nabc",
		"postgres://owner:secret-value@db.internal/vane",
		"请记住 sk-1234567890abcdefghijklmnop",
		"github token ghp_123456789012345678901234567890",
		"aws AKIA1234567890ABCDEF",
		"token: 1234567890abcdef",
		"密码是 correct-horse-battery",
		"eyJabcdefghijk.abcdefghijkl.abcdefghijk",
	} {
		if !ContainsCredential(input) {
			t.Errorf("credential accepted: %q", input)
		}
	}
}

func TestContainsCredentialKeepsNonSecretExperience(t *testing.T) {
	for _, input := range []string{
		"生产研究模型使用 deepseek-v4-flash",
		"API key 必须按季度轮换，但不要把值写入长期记忆",
		"发布前检查数据库连接是否可用",
		"密码管理采用专门的凭证库",
	} {
		if ContainsCredential(input) {
			t.Errorf("ordinary experience rejected: %q", input)
		}
	}
}
