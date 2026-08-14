package a2a

import (
	"errors"
	"fmt"
	"testing"

	"github.com/YouToco/vane/server/types"
)

// TestSanitize 对外文案翻译逐字钉死（契约 §9.1）。
func TestSanitize(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"AppError取Message", types.NewAppError(types.CodeDatabase, "检索失败", errors.New("pgx: dial tcp refused")), "检索失败"},
		{"裸error固定文案", errors.New("pgx: connection refused"), internalErrorText},
		{"包装链里的AppError", fmt.Errorf("outer: %w", types.NewAppError(types.CodeValidation, "参数非法", nil)), "参数非法"},
		{"空Message回退固定文案", types.NewAppError(types.CodeInternal, "", errors.New("raw")), internalErrorText},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := sanitize(tc.err); got != tc.want {
				t.Fatalf("sanitize=%q，期望 %q", got, tc.want)
			}
		})
	}
}

// TestLiteralsSkillSource skill 常量守卫（契约 §9.5，仿 probe/literals_test.go）：
// executor REJECTED 判定与 card skill 声明必须同源同值。
func TestLiteralsSkillSource(t *testing.T) {
	if skillContentQuery != "content.query" {
		t.Fatalf("skill 常量漂移: %q（契约 §5.4 钉死 content.query）", skillContentQuery)
	}
	card := buildCard(Deps{})
	if card.Skills[0].ID != skillContentQuery {
		t.Fatal("card skill id 与 executor 判定常量必须同源")
	}
}
