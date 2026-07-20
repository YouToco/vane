// Package task contains the task control-plane orchestration shared by Vane's
// user-facing task creation entry points.
package task

import (
	"context"
	"log/slog"

	"github.com/YouToco/vane/scheduler"
	"github.com/YouToco/vane/types"
	"github.com/YouToco/vane/workflow"
)

// ScheduleCreator is the scheduling capability needed to create a task.
type ScheduleCreator interface {
	CreatePush(ctx context.Context, userID int64, spec scheduler.ScheduleSpec, scope workflow.PushScope, nlDesc string) (string, error)
}

// PlaybookWriter initializes the playbook belonging to a newly created task.
type PlaybookWriter interface {
	UpsertSchedulePlaybook(ctx context.Context, userID int64, scheduleID, content string) (bool, error)
}

// PlaybookCompiler compiles an initialized playbook on a best-effort basis.
// Implementations own their failure logging and must not fail task creation.
type PlaybookCompiler interface {
	Compile(ctx context.Context, userID int64, scheduleID, content string)
}

// StrictnessWriter persists the optional initial push threshold.
type StrictnessWriter interface {
	SetScheduleStrictness(ctx context.Context, scheduleID string, userID int64, strictness types.PushStrictness) error
}

// Deps are the narrow capabilities used by Service.
type Deps struct {
	Schedules  ScheduleCreator
	Playbooks  PlaybookWriter
	Compiler   PlaybookCompiler
	Strictness StrictnessWriter
}

// Service orchestrates task control-plane operations without owning transport
// validation, error mapping, or user-facing reply formatting.
type Service struct {
	deps Deps
}

// New constructs a task control-plane service.
func New(deps Deps) *Service {
	return &Service{deps: deps}
}

// CreateInput contains the already validated inputs for task creation.
//
// NLDescription is the unmodified user description passed to the scheduler.
// PlaybookContent is the independently bounded content persisted and compiled
// as the task playbook.
type CreateInput struct {
	UserID          int64
	Spec            scheduler.ScheduleSpec
	NLDescription   string
	PlaybookContent string
	Strictness      types.PushStrictness
}

// CreateResult reports the created task and whether its optional initial
// strictness was successfully persisted.
type CreateResult struct {
	ScheduleID        string
	StrictnessApplied bool
}

// Create creates the schedule first, then initializes its playbook and
// optional strictness. The latter steps are best-effort and never compensate
// the already-created schedule.
func (s *Service) Create(ctx context.Context, in CreateInput) (CreateResult, error) {
	scheduleID, err := s.deps.Schedules.CreatePush(
		ctx,
		in.UserID,
		in.Spec,
		workflow.PushScope{},
		in.NLDescription,
	)
	if err != nil {
		return CreateResult{}, err
	}

	if ok, playbookErr := s.deps.Playbooks.UpsertSchedulePlaybook(
		ctx,
		in.UserID,
		scheduleID,
		in.PlaybookContent,
	); playbookErr != nil {
		slog.Error("agent: create_schedule 初始化任务手册失败（调度已创建）", "schedule_id", scheduleID, "err", playbookErr)
	} else if !ok {
		slog.Warn("agent: create_schedule 初始化任务手册未命中刚建的调度（异常）", "schedule_id", scheduleID)
	} else if s.deps.Compiler != nil {
		s.deps.Compiler.Compile(ctx, in.UserID, scheduleID, in.PlaybookContent)
	}

	result := CreateResult{ScheduleID: scheduleID}
	if in.Strictness == "" || s.deps.Strictness == nil {
		return result, nil
	}
	if strictnessErr := s.deps.Strictness.SetScheduleStrictness(
		ctx,
		scheduleID,
		in.UserID,
		in.Strictness,
	); strictnessErr != nil {
		slog.Error("agent: create_schedule 落初始门槛档位失败（调度已创建，按兜底档运行）",
			"schedule_id", scheduleID, "strictness", in.Strictness, "err", strictnessErr)
		return result, nil
	}
	result.StrictnessApplied = true
	return result, nil
}
