package agent

import (
	"context"

	"github.com/YouToco/vane/task"
)

// taskPlaybookCompiler adapts the Agent-owned playbook compiler to task.Service.
// The implementation stays in agent because edit_task_playbook shares the same
// compilePlaybookPlan pipeline; task only knows that compilation is best-effort.
type taskPlaybookCompiler struct {
	st playbookStore
	tr playbookTranslator
}

// NewTaskPlaybookCompiler exposes the playbook compilation adapter for root
// composition without leaking Agent's private store/translator interfaces.
func NewTaskPlaybookCompiler(st playbookStore, tr playbookTranslator) task.PlaybookCompiler {
	return &taskPlaybookCompiler{st: st, tr: tr}
}

func (c *taskPlaybookCompiler) Compile(
	ctx context.Context,
	userID int64,
	scheduleID string,
	content string,
) {
	compilePlaybookPlan(ctx, c.st, c.tr, userID, scheduleID, content)
}
