package scheduler

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	"google.golang.org/protobuf/proto"

	"github.com/YouToco/vane/taskstate"
	"github.com/YouToco/vane/types"
	"github.com/YouToco/vane/workflow"
)

type researchV3CutoverJournal interface {
	GetSchedule(context.Context, string, int64) (*types.Schedule, error)
	LoadCurrentResearchApprovedDefinitionV3Head(
		context.Context, int64, int64, string,
	) (types.ResearchV3DefinitionHead, error)
	RequireSuccessfulResearchV3ShadowPreflight(
		context.Context, int64, int64, string, types.ResearchV3DefinitionHead,
	) error
	BeginResearchV3Cutover(
		context.Context, types.BeginResearchV3CutoverParams,
	) (types.ResearchV3CutoverOperation, error)
	LoadResearchV3Cutover(
		context.Context, int64, int64, string, string,
	) (types.ResearchV3CutoverOperation, bool, error)
	LoadResearchV3CutoverAuthorityStatus(
		context.Context, types.ResearchV3CutoverOperation,
	) (string, error)
	VerifyResearchV3CutoverDatabaseState(
		context.Context, types.ResearchV3CutoverOperation,
	) error
	RecheckResearchV3CutoverDefinition(
		context.Context, types.ResearchV3CutoverOperation,
	) error
	PromoteResearchV3PreparedDefinition(
		context.Context, types.ResearchV3CutoverOperation,
	) (types.ResearchV3CutoverOperation, error)
	RestoreResearchV3OriginalDefinition(
		context.Context, types.ResearchV3CutoverOperation,
	) (types.ResearchV3CutoverOperation, error)
	BeginResearchV3RollbackPause(
		context.Context, types.ResearchV3CutoverOperation, []byte, string,
	) (types.ResearchV3CutoverOperation, error)
	AdvanceResearchV3Cutover(
		context.Context, types.ResearchV3CutoverOperation,
		types.ResearchV3CutoverPhase, types.ResearchV3CutoverPhase,
	) (types.ResearchV3CutoverOperation, error)
	RevokeResearchV3DeliveryAuthority(
		context.Context, types.ResearchV3CutoverOperation,
	) error
}

type researchV3ScheduleRemote interface {
	Describe(context.Context, string) (*workflowservice.DescribeScheduleResponse, error)
	CompareAndSwap(context.Context, string, *schedulepb.Schedule, []byte, string) error
}

type researchV3DefinitionPrepareStore interface {
	GetSchedule(context.Context, string, int64) (*types.Schedule, error)
	PrepareResearchV3Definition(
		context.Context, taskstate.ResearchV3DefinitionPrepareParams,
	) (types.ResearchV3DefinitionPrepareOperation, error)
	RollbackResearchV3DefinitionPrepare(
		context.Context, int64, int64, string, string,
	) (types.ResearchV3DefinitionPrepareOperation, error)
}

type researchV3ParamsEncoder func(any) (*commonpb.Payload, error)

// researchV3CutoverCoordinator is reachable only through the exact-task
// operator methods below. It is not exposed to HTTP or the Agent tool surface.
type researchV3CutoverCoordinator struct {
	exactTaskID string
	journal     researchV3CutoverJournal
	remote      researchV3ScheduleRemote
	encode      researchV3ParamsEncoder
}

type researchV3CutoverRequest struct {
	TaskID         string
	UserID         int64
	IdempotencyKey string
}

const researchV3CutoverConflictRollbackTimeout = 30 * time.Second

// PrepareResearchV3Definition creates only a delivery-dark sidecar head for
// the exact configured shadow task. Tenant scope is resolved from the owner
// mirror rather than accepted from an operator argument.
func (s *Scheduler) PrepareResearchV3Definition(
	ctx context.Context, p taskstate.ResearchV3DefinitionPrepareParams,
) (types.ResearchV3DefinitionPrepareOperation, error) {
	if s == nil || !s.researchV3.shadowIDMatch(p.TaskID) || p.TenantID != 0 ||
		p.UserID <= 0 {
		return types.ResearchV3DefinitionPrepareOperation{}, types.NewAppError(
			types.CodeNotFound, "research V3 prepare task is not configured", types.ErrNotFound)
	}
	preparer, ok := s.st.(researchV3DefinitionPrepareStore)
	if !ok {
		return types.ResearchV3DefinitionPrepareOperation{}, types.NewAppError(
			types.CodeInternal, "research V3 prepare dependencies are unavailable", nil)
	}
	mirror, err := preparer.GetSchedule(ctx, p.TaskID, p.UserID)
	if err != nil {
		return types.ResearchV3DefinitionPrepareOperation{}, err
	}
	if mirror == nil || mirror.ID != p.TaskID || mirror.UserID != p.UserID || mirror.TenantID <= 0 {
		return types.ResearchV3DefinitionPrepareOperation{}, types.ErrNotFound
	}
	p.TenantID = mirror.TenantID
	return preparer.PrepareResearchV3Definition(ctx, p)
}

func (s *Scheduler) RollbackResearchV3DefinitionPrepare(
	ctx context.Context, taskID string, userID int64, idempotencyKey string,
) (types.ResearchV3DefinitionPrepareOperation, error) {
	if s == nil || !s.researchV3.shadowIDMatch(taskID) || userID <= 0 {
		return types.ResearchV3DefinitionPrepareOperation{}, types.ErrNotFound
	}
	preparer, ok := s.st.(researchV3DefinitionPrepareStore)
	if !ok {
		return types.ResearchV3DefinitionPrepareOperation{}, types.ErrInternal
	}
	mirror, err := preparer.GetSchedule(ctx, taskID, userID)
	if err != nil {
		return types.ResearchV3DefinitionPrepareOperation{}, err
	}
	if mirror == nil || mirror.TenantID <= 0 || mirror.ID != taskID || mirror.UserID != userID {
		return types.ResearchV3DefinitionPrepareOperation{}, types.ErrNotFound
	}
	return preparer.RollbackResearchV3DefinitionPrepare(
		ctx, mirror.TenantID, userID, taskID, idempotencyKey)
}

func newResearchV3CutoverCoordinator(
	exactTaskID string, journal researchV3CutoverJournal,
	remote researchV3ScheduleRemote, encode researchV3ParamsEncoder,
) (*researchV3CutoverCoordinator, error) {
	if exactTaskID == "" || strings.TrimSpace(exactTaskID) != exactTaskID ||
		len(exactTaskID) > 255 || journal == nil || remote == nil || encode == nil {
		return nil, types.NewAppError(types.CodeValidation,
			"research V3 cutover coordinator is invalid", types.ErrValidation)
	}
	return &researchV3CutoverCoordinator{
		exactTaskID: exactTaskID, journal: journal, remote: remote, encode: encode,
	}, nil
}

func (s *Scheduler) newResearchV3CutoverCore(
	exactTaskID string,
) (*researchV3CutoverCoordinator, error) {
	if s == nil || s.c == nil || s.st == nil || s.taskScheduleEnv.namespace == "" {
		return nil, types.NewAppError(types.CodeInternal,
			"research V3 cutover dependencies are unavailable", nil)
	}
	journal, ok := s.st.(researchV3CutoverJournal)
	if !ok {
		return nil, types.NewAppError(types.CodeInternal,
			"research V3 cutover dependencies are unavailable", nil)
	}
	encode, err := s.researchV3CutoverParamsEncoder()
	if err != nil {
		return nil, err
	}
	return newResearchV3CutoverCoordinator(exactTaskID, journal,
		&schedulerResearchV3ScheduleRemote{scheduler: s},
		encode)
}

// researchV3CutoverParamsEncoder binds the new Research V3 Action payload to
// the reserved Temporal JSON protocol. The task ID is an ownership key, never
// a data-converter registry key. Looking a converter up by task ID returns nil
// in production and used to make the one-shot cutover command panic before it
// could create its durable journal.
func (s *Scheduler) researchV3CutoverParamsEncoder() (
	researchV3ParamsEncoder, error,
) {
	if s == nil {
		return nil, types.NewAppError(types.CodeInternal,
			"research V3 cutover data converter is unavailable", nil)
	}
	dc := s.taskScheduleDecoder(taskScheduleDefaultConverterID)
	if dc == nil {
		return nil, types.NewAppError(types.CodeInternal,
			"research V3 cutover data converter is unavailable", nil)
	}
	return func(params any) (*commonpb.Payload, error) {
		payload, err := dc.ToPayload(params)
		if err != nil || payload == nil {
			return nil, types.NewAppError(types.CodeInternal,
				"encode research V3 Schedule Action", err)
		}
		return payload, nil
	}, nil
}

// CutoverResearchV3 replaces one already-shadowed task's Schedule Action via
// the durable pause/CAS/restore saga. Config alone never changes an Action.
func (s *Scheduler) CutoverResearchV3(
	ctx context.Context, scope types.ResearchV3OperatorScope,
	idempotencyKey, expectedPlanDigest string,
) (types.ResearchV3CutoverOperation, error) {
	if s == nil || s.researchV3.authorityID == "" ||
		scope.TaskID != s.researchV3.authorityID {
		return types.ResearchV3CutoverOperation{}, types.NewAppError(
			types.CodeNotFound, "research V3 cutover task is not configured", types.ErrNotFound)
	}
	core, err := s.newResearchV3CutoverCore(scope.TaskID)
	if err != nil {
		return types.ResearchV3CutoverOperation{}, err
	}
	return core.CutoverWithPreflight(ctx, scope, researchV3CutoverRequest{
		TaskID: scope.TaskID, UserID: scope.UserID, IdempotencyKey: idempotencyKey,
	}, expectedPlanDigest)
}

// PreflightResearchV3 is read-only. It binds the exact database owner scope,
// successful delivery-dark shadow and full Temporal Schedule bytes into a
// digest that CutoverResearchV3 must receive unchanged.
func (s *Scheduler) PreflightResearchV3(
	ctx context.Context, scope types.ResearchV3OperatorScope,
	idempotencyKey string,
) (types.ResearchV3CutoverInspection, error) {
	if s == nil || s.researchV3.authorityID == "" ||
		scope.TaskID != s.researchV3.authorityID {
		return types.ResearchV3CutoverInspection{}, types.NewAppError(
			types.CodeNotFound, "research V3 preflight task is not configured", types.ErrNotFound)
	}
	core, err := s.newResearchV3CutoverCore(scope.TaskID)
	if err != nil {
		return types.ResearchV3CutoverInspection{}, err
	}
	return core.Preflight(ctx, scope, researchV3CutoverRequest{
		TaskID: scope.TaskID, UserID: scope.UserID, IdempotencyKey: idempotencyKey,
	})
}

func (c *researchV3CutoverCoordinator) Preflight(
	ctx context.Context, scope types.ResearchV3OperatorScope,
	req researchV3CutoverRequest,
) (types.ResearchV3CutoverInspection, error) {
	if err := c.validateRequest(req); err != nil ||
		scope.TaskID != req.TaskID || scope.UserID != req.UserID || scope.TenantID <= 0 ||
		(scope.Status != types.ScheduleStatusActive &&
			scope.Status != types.ScheduleStatusPaused) {
		if err != nil {
			return types.ResearchV3CutoverInspection{}, err
		}
		return types.ResearchV3CutoverInspection{}, types.NewAppError(
			types.CodeValidation, "research V3 preflight scope is invalid", types.ErrValidation)
	}
	if existing, found, err := c.journal.LoadResearchV3Cutover(
		ctx, scope.TenantID, scope.UserID, scope.TaskID, req.IdempotencyKey,
	); err != nil {
		return types.ResearchV3CutoverInspection{}, err
	} else if found {
		return types.ResearchV3CutoverInspection{}, types.NewAppError(
			types.CodeConflict,
			fmt.Sprintf("research V3 cutover already began at phase %s", existing.Phase),
			types.ErrConflict)
	}
	mirror, err := c.journal.GetSchedule(ctx, scope.TaskID, scope.UserID)
	if err != nil {
		return types.ResearchV3CutoverInspection{}, err
	}
	if mirror == nil || mirror.TenantID != scope.TenantID ||
		mirror.UserID != scope.UserID || mirror.ID != scope.TaskID ||
		mirror.Status != scope.Status || mirror.ExecutionMode != scope.ExecutionMode ||
		!bytes.Equal(mirror.SpecJSON, scope.SpecJSON) {
		return types.ResearchV3CutoverInspection{}, types.NewAppError(
			types.CodeConflict, "research V3 preflight database scope changed", types.ErrConflict)
	}
	head, err := c.journal.LoadCurrentResearchApprovedDefinitionV3Head(
		ctx, scope.TenantID, scope.UserID, scope.TaskID)
	if err != nil {
		return types.ResearchV3CutoverInspection{}, err
	}
	if err := c.journal.RequireSuccessfulResearchV3ShadowPreflight(
		ctx, scope.TenantID, scope.UserID, scope.TaskID, head); err != nil {
		return types.ResearchV3CutoverInspection{}, err
	}
	desc, err := c.remote.Describe(ctx, scope.TaskID)
	if err != nil {
		return types.ResearchV3CutoverInspection{}, cutoverRemoteError("preflight describe", err)
	}
	frozen, err := cloneDescribedScheduleV3(desc)
	if err != nil {
		return types.ResearchV3CutoverInspection{}, err
	}
	paused := frozen.GetState().GetPaused()
	if paused != (scope.Status == types.ScheduleStatusPaused) {
		return types.ResearchV3CutoverInspection{}, types.NewAppError(
			types.CodeConflict, "research V3 preflight database and Temporal pause differ", types.ErrConflict)
	}
	if desc.GetInfo() != nil && len(desc.GetInfo().GetRunningWorkflows()) != 0 {
		return types.ResearchV3CutoverInspection{}, types.NewAppError(
			types.CodeConflict, "research V3 preflight has running workflows", types.ErrConflict)
	}
	_, frozenDigest, err := marshalScheduleArtifactV3(frozen)
	if err != nil {
		return types.ResearchV3CutoverInspection{}, err
	}
	planDigest := researchV3PreflightDigest(
		scope, head, frozenDigest, req.IdempotencyKey)
	return types.ResearchV3CutoverInspection{
		SchemaVersion: "vane.research-v3-cutover-inspection/v1",
		TaskID:        scope.TaskID, TenantID: scope.TenantID, UserID: scope.UserID,
		ScheduleStatus: scope.Status, ExecutionMode: scope.ExecutionMode,
		ProductionHead: scope.ProductionHead, PreparedHead: head,
		TemporalPaused: paused, FrozenScheduleDigest: frozenDigest,
		PlanDigest: planDigest, Ready: true,
	}, nil
}

func researchV3PreflightDigest(
	scope types.ResearchV3OperatorScope, head types.ResearchV3DefinitionHead,
	frozenScheduleDigest, idempotencyKey string,
) string {
	productionVersion, productionDigest := int64(0), ""
	if scope.ProductionHead != nil {
		productionVersion = scope.ProductionHead.Version
		productionDigest = scope.ProductionHead.Digest
	}
	payload := fmt.Sprintf(
		"vane/research-v3-cutover-preflight/v2\x00%d\x00%d\x00%s\x00%s\x00%s\x00%d\x00%s\x00%d\x00%s\x00%s\x00%s",
		scope.TenantID, scope.UserID, scope.TaskID, scope.Status,
		scope.ExecutionMode, productionVersion, productionDigest,
		head.Version, head.Digest, frozenScheduleDigest, idempotencyKey)
	digest := sha256.Sum256([]byte(payload))
	return hex.EncodeToString(digest[:])
}

// StatusResearchV3 describes a durable operation without resuming it.
func (s *Scheduler) StatusResearchV3(
	ctx context.Context, scope types.ResearchV3OperatorScope, idempotencyKey string,
) (types.ResearchV3CutoverInspection, error) {
	if s == nil || s.researchV3.authorityID == "" ||
		scope.TaskID != s.researchV3.authorityID {
		return types.ResearchV3CutoverInspection{}, types.ErrNotFound
	}
	core, err := s.newResearchV3CutoverCore(scope.TaskID)
	if err != nil {
		return types.ResearchV3CutoverInspection{}, err
	}
	req := researchV3CutoverRequest{
		TaskID: scope.TaskID, UserID: scope.UserID, IdempotencyKey: idempotencyKey,
	}
	if err := core.validateRequest(req); err != nil {
		return types.ResearchV3CutoverInspection{}, err
	}
	op, found, err := core.journal.LoadResearchV3Cutover(
		ctx, scope.TenantID, scope.UserID, scope.TaskID, idempotencyKey)
	if err != nil || !found {
		if err == nil {
			err = types.NewAppError(types.CodeNotFound,
				"research V3 cutover journal is unavailable", types.ErrNotFound)
		}
		return types.ResearchV3CutoverInspection{}, err
	}
	frozen, target, err := decodeCutoverArtifactsV3(op)
	if err != nil {
		return types.ResearchV3CutoverInspection{}, err
	}
	desc, err := core.remote.Describe(ctx, scope.TaskID)
	if err != nil {
		return types.ResearchV3CutoverInspection{}, cutoverRemoteError("status describe", err)
	}
	observed, err := verifyCutoverScheduleV3(desc, frozen, target)
	if err != nil {
		return types.ResearchV3CutoverInspection{}, err
	}
	authorityStatus, err := core.journal.LoadResearchV3CutoverAuthorityStatus(ctx, op)
	if err != nil {
		return types.ResearchV3CutoverInspection{}, err
	}
	inspection := types.ResearchV3CutoverInspection{
		SchemaVersion: "vane.research-v3-cutover-inspection/v1",
		TaskID:        op.TaskID, TenantID: op.TenantID, UserID: op.UserID,
		ScheduleStatus: scope.Status, ExecutionMode: scope.ExecutionMode,
		ProductionHead: scope.ProductionHead, PreparedHead: op.Definition,
		TemporalPaused:       observed.paused,
		FrozenScheduleDigest: op.FrozenScheduleDigest,
		PlanDigest:           op.PreflightDigest, OperationGeneration: op.Generation,
		OperationPhase: op.Phase, DeliveryAuthorityState: authorityStatus,
	}
	terminalRemote := (op.Phase == types.ResearchV3CutoverActive && observed.target &&
		observed.paused == op.OriginalPaused) ||
		((op.Phase == types.ResearchV3CutoverRolledBack ||
			op.Phase == types.ResearchV3CutoverAborted) && !observed.target &&
			observed.paused == op.OriginalPaused)
	if terminalRemote {
		inspection.Verified = core.journal.VerifyResearchV3CutoverDatabaseState(ctx, op) == nil
	}
	return inspection, nil
}

func (s *Scheduler) VerifyResearchV3(
	ctx context.Context, scope types.ResearchV3OperatorScope, idempotencyKey string,
) (types.ResearchV3CutoverInspection, error) {
	inspection, err := s.StatusResearchV3(ctx, scope, idempotencyKey)
	if err != nil {
		return types.ResearchV3CutoverInspection{}, err
	}
	if !inspection.Verified {
		return inspection, types.NewAppError(types.CodeConflict,
			"research V3 cutover terminal invariants are not verified", types.ErrConflict)
	}
	return inspection, nil
}

// RollbackResearchV3 revokes delivery authority before restoring the frozen
// Action. It derives tenant scope from the exact task/user mirror.
func (s *Scheduler) RollbackResearchV3(
	ctx context.Context, taskID string, userID int64, idempotencyKey string,
) (types.ResearchV3CutoverOperation, error) {
	if s == nil || s.researchV3.authorityID == "" ||
		taskID != s.researchV3.authorityID {
		return types.ResearchV3CutoverOperation{}, types.NewAppError(
			types.CodeNotFound, "research V3 rollback task is not configured", types.ErrNotFound)
	}
	core, err := s.newResearchV3CutoverCore(taskID)
	if err != nil {
		return types.ResearchV3CutoverOperation{}, err
	}
	mirror, err := core.journal.GetSchedule(ctx, taskID, userID)
	if err != nil {
		return types.ResearchV3CutoverOperation{}, err
	}
	if mirror == nil || mirror.ID != taskID || mirror.UserID != userID ||
		mirror.TenantID <= 0 {
		return types.ResearchV3CutoverOperation{}, types.NewAppError(
			types.CodeNotFound, "research V3 rollback task is unavailable", types.ErrNotFound)
	}
	return core.Rollback(ctx, mirror.TenantID, researchV3CutoverRequest{
		TaskID: taskID, UserID: userID, IdempotencyKey: idempotencyKey,
	})
}

// Cutover is the package-level saga entry retained for focused coordinator
// tests. It derives the same immutable scope and plan digest as the operator
// preflight; production callers use CutoverResearchV3 and must echo that digest.
func (c *researchV3CutoverCoordinator) Cutover(
	ctx context.Context, req researchV3CutoverRequest,
) (types.ResearchV3CutoverOperation, error) {
	if err := c.validateRequest(req); err != nil {
		return types.ResearchV3CutoverOperation{}, err
	}
	mirror, err := c.journal.GetSchedule(ctx, req.TaskID, req.UserID)
	if err != nil {
		return types.ResearchV3CutoverOperation{}, err
	}
	if mirror == nil {
		return types.ResearchV3CutoverOperation{}, types.ErrNotFound
	}
	scope := types.ResearchV3OperatorScope{
		TenantID: mirror.TenantID, UserID: mirror.UserID, TaskID: mirror.ID,
		Status: mirror.Status, ExecutionMode: mirror.ExecutionMode,
		SpecJSON: append([]byte(nil), mirror.SpecJSON...),
	}
	if existing, found, loadErr := c.journal.LoadResearchV3Cutover(
		ctx, scope.TenantID, scope.UserID, scope.TaskID, req.IdempotencyKey,
	); loadErr != nil {
		return types.ResearchV3CutoverOperation{}, loadErr
	} else if found {
		return c.CutoverWithPreflight(ctx, scope, req, existing.PreflightDigest)
	}
	inspection, err := c.Preflight(ctx, scope, req)
	if err != nil {
		return types.ResearchV3CutoverOperation{}, err
	}
	return c.CutoverWithPreflight(ctx, scope, req, inspection.PlanDigest)
}

func (c *researchV3CutoverCoordinator) CutoverWithPreflight(
	ctx context.Context, scope types.ResearchV3OperatorScope,
	req researchV3CutoverRequest, expectedPlanDigest string,
) (types.ResearchV3CutoverOperation, error) {
	if err := c.validateRequest(req); err != nil {
		return types.ResearchV3CutoverOperation{}, err
	}
	mirror, err := c.journal.GetSchedule(ctx, req.TaskID, req.UserID)
	if err != nil {
		return types.ResearchV3CutoverOperation{}, err
	}
	if mirror == nil || mirror.ID != req.TaskID || mirror.UserID != req.UserID ||
		mirror.TenantID <= 0 || mirror.TenantID != scope.TenantID ||
		mirror.Status != scope.Status || mirror.ExecutionMode != scope.ExecutionMode ||
		!bytes.Equal(mirror.SpecJSON, scope.SpecJSON) ||
		(mirror.Status != types.ScheduleStatusActive &&
			mirror.Status != types.ScheduleStatusPaused) {
		return types.ResearchV3CutoverOperation{}, types.NewAppError(
			types.CodeNotFound, "research V3 cutover task scope changed", types.ErrNotFound)
	}
	if existing, found, loadErr := c.journal.LoadResearchV3Cutover(
		ctx, mirror.TenantID, req.UserID, req.TaskID, req.IdempotencyKey,
	); loadErr != nil {
		return types.ResearchV3CutoverOperation{}, loadErr
	} else if found {
		if expectedPlanDigest == "" || existing.PreflightDigest != expectedPlanDigest {
			return types.ResearchV3CutoverOperation{}, types.NewAppError(
				types.CodeConflict, "research V3 cutover preflight digest differs", types.ErrConflict)
		}
		return c.resumeCutover(ctx, existing)
	}
	inspection, err := c.Preflight(ctx, scope, req)
	if err != nil {
		return types.ResearchV3CutoverOperation{}, err
	}
	if expectedPlanDigest == "" || inspection.PlanDigest != expectedPlanDigest {
		return types.ResearchV3CutoverOperation{}, types.NewAppError(
			types.CodeConflict, "research V3 cutover preflight digest differs", types.ErrConflict)
	}
	head, err := c.journal.LoadCurrentResearchApprovedDefinitionV3Head(
		ctx, mirror.TenantID, req.UserID, req.TaskID)
	if err != nil {
		return types.ResearchV3CutoverOperation{}, err
	}
	if err := c.journal.RequireSuccessfulResearchV3ShadowPreflight(
		ctx, mirror.TenantID, req.UserID, req.TaskID, head); err != nil {
		return types.ResearchV3CutoverOperation{}, err
	}
	desc, err := c.remote.Describe(ctx, req.TaskID)
	if err != nil {
		return types.ResearchV3CutoverOperation{}, cutoverRemoteError("describe", err)
	}
	frozen, err := cloneDescribedScheduleV3(desc)
	if err != nil {
		return types.ResearchV3CutoverOperation{}, err
	}
	authorityToken, authorityDigest, err := newResearchV3ActionAuthorization()
	if err != nil {
		return types.ResearchV3CutoverOperation{}, err
	}
	targetAction, err := c.buildTargetAction(mirror, frozen.GetAction(), authorityToken)
	if err != nil {
		return types.ResearchV3CutoverOperation{}, err
	}
	frozenBytes, frozenDigest, err := marshalScheduleArtifactV3(frozen)
	if err != nil {
		return types.ResearchV3CutoverOperation{}, err
	}
	targetBytes, targetDigest, err := marshalScheduleArtifactV3(targetAction)
	if err != nil {
		return types.ResearchV3CutoverOperation{}, err
	}
	conflictToken := append([]byte(nil), desc.GetConflictToken()...)
	conflictDigestBytes := sha256.Sum256(conflictToken)
	op, err := c.journal.BeginResearchV3Cutover(ctx,
		types.BeginResearchV3CutoverParams{
			TenantID: mirror.TenantID, UserID: req.UserID, TaskID: req.TaskID,
			IdempotencyKey: req.IdempotencyKey, Definition: head,
			FrozenSchedule: frozenBytes, FrozenScheduleDigest: frozenDigest,
			FrozenConflictToken: conflictToken,
			ConflictTokenDigest: hex.EncodeToString(conflictDigestBytes[:]),
			TargetAction:        targetBytes, TargetActionDigest: targetDigest,
			ActionAuthorizationDigest: authorityDigest,
			OriginalPaused:            frozen.GetState().GetPaused(),
			OriginalScheduleStatus:    scope.Status,
			PreflightDigest:           inspection.PlanDigest,
		})
	if err != nil {
		return types.ResearchV3CutoverOperation{}, err
	}
	return c.resumeCutover(ctx, op)
}

func (c *researchV3CutoverCoordinator) Resume(
	ctx context.Context, tenantID int64, req researchV3CutoverRequest,
) (types.ResearchV3CutoverOperation, error) {
	if err := c.validateRequest(req); err != nil || tenantID <= 0 {
		if err != nil {
			return types.ResearchV3CutoverOperation{}, err
		}
		return types.ResearchV3CutoverOperation{}, types.NewAppError(
			types.CodeValidation, "research V3 cutover tenant is invalid", types.ErrValidation)
	}
	op, found, err := c.journal.LoadResearchV3Cutover(
		ctx, tenantID, req.UserID, req.TaskID, req.IdempotencyKey)
	if err != nil {
		return types.ResearchV3CutoverOperation{}, err
	}
	if !found {
		return types.ResearchV3CutoverOperation{}, types.NewAppError(
			types.CodeNotFound, "research V3 cutover journal is unavailable", types.ErrNotFound)
	}
	return c.resumeCutover(ctx, op)
}

func (c *researchV3CutoverCoordinator) resumeCutover(
	ctx context.Context, op types.ResearchV3CutoverOperation,
) (types.ResearchV3CutoverOperation, error) {
	frozen, target, err := decodeCutoverArtifactsV3(op)
	if err != nil {
		return types.ResearchV3CutoverOperation{}, err
	}
	for attempt := 0; attempt < 12; attempt++ {
		desc, describeErr := c.remote.Describe(ctx, op.TaskID)
		if describeErr != nil {
			return types.ResearchV3CutoverOperation{}, cutoverRemoteError("recover describe", describeErr)
		}
		observed, err := verifyCutoverScheduleV3(desc, frozen, target)
		if err != nil {
			return types.ResearchV3CutoverOperation{}, err
		}
		switch op.Phase {
		case types.ResearchV3CutoverPrepared:
			if observed.target {
				return types.ResearchV3CutoverOperation{}, c.rollbackCutoverConflict(ctx, op,
					types.NewAppError(types.CodeConflict,
						"research V3 target Action appeared before pause ownership", types.ErrConflict))
			}
			if !op.OriginalPaused && observed.paused {
				return types.ResearchV3CutoverOperation{}, c.abortUnownedSchedulePause(ctx, op)
			}
			if !bytes.Equal(desc.GetConflictToken(), op.FrozenConflictToken) {
				return types.ResearchV3CutoverOperation{}, c.abortUnownedSchedulePause(ctx, op)
			}
			current := op
			op, err = c.advance(ctx, current, types.ResearchV3CutoverPrepared,
				types.ResearchV3CutoverPauseRequested)
			if err != nil && types.CodeOf(err) == types.CodeConflict {
				op, err = c.recoverForwardStoreConflict(ctx, current, err)
				if err != nil {
					return types.ResearchV3CutoverOperation{}, err
				}
				continue
			}
		case types.ResearchV3CutoverPauseRequested:
			if observed.target {
				return types.ResearchV3CutoverOperation{}, c.rollbackCutoverConflict(ctx, op,
					types.NewAppError(types.CodeConflict,
						"research V3 target Action appeared before pause checkpoint", types.ErrConflict))
			}
			if op.OriginalPaused {
				if !observed.paused {
					return types.ResearchV3CutoverOperation{}, c.abortUnownedSchedulePause(ctx, op)
				}
				current := op
				op, err = c.advance(ctx, current, types.ResearchV3CutoverPauseRequested,
					types.ResearchV3CutoverPaused)
				if err != nil && types.CodeOf(err) == types.CodeConflict {
					op, err = c.recoverForwardStoreConflict(ctx, current, err)
					if err != nil {
						return types.ResearchV3CutoverOperation{}, err
					}
					continue
				}
			} else if !observed.paused {
				if !bytes.Equal(desc.GetConflictToken(), op.FrozenConflictToken) {
					return types.ResearchV3CutoverOperation{}, c.abortUnownedSchedulePause(ctx, op)
				}
				err = c.casState(ctx, op, desc, frozen, false, true, "pause")
				if err == nil {
					current := op
					op, err = c.advance(ctx, current, types.ResearchV3CutoverPauseRequested,
						types.ResearchV3CutoverPaused)
					if err != nil && types.CodeOf(err) == types.CodeConflict {
						op, err = c.recoverForwardStoreConflict(ctx, current, err)
						if err != nil {
							return types.ResearchV3CutoverOperation{}, err
						}
						continue
					}
				}
			} else {
				// Re-submit the exact initial-token/request-id mutation. Temporal's
				// request receipt proves this pause was ours; a stale-token failure
				// is treated as an independent operator pause and is never undone.
				paused := proto.Clone(frozen).(*schedulepb.Schedule)
				if paused.State == nil {
					paused.State = &schedulepb.ScheduleState{}
				}
				paused.State.Paused = true
				err = c.remote.CompareAndSwap(ctx, op.TaskID, paused,
					op.FrozenConflictToken, researchV3CutoverRequestID(op, "pause"))
				if err != nil {
					return types.ResearchV3CutoverOperation{}, c.abortUnownedSchedulePause(ctx, op)
				}
				current := op
				op, err = c.advance(ctx, current, types.ResearchV3CutoverPauseRequested,
					types.ResearchV3CutoverPaused)
				if err != nil && types.CodeOf(err) == types.CodeConflict {
					op, err = c.recoverForwardStoreConflict(ctx, current, err)
					if err != nil {
						return types.ResearchV3CutoverOperation{}, err
					}
					continue
				}
			}
		case types.ResearchV3CutoverPaused:
			if observed.target {
				return types.ResearchV3CutoverOperation{}, c.rollbackCutoverConflict(ctx, op,
					types.NewAppError(types.CodeConflict, "research V3 target Action appeared before definition promotion", types.ErrConflict))
			} else if !observed.paused {
				err = types.NewAppError(types.CodeConflict,
					"research V3 cutover lost its paused fence", types.ErrConflict)
			} else {
				current := op
				op, err = c.journal.PromoteResearchV3PreparedDefinition(ctx, current)
				if err != nil && types.CodeOf(err) == types.CodeConflict {
					op, err = c.recoverForwardStoreConflict(ctx, current, err)
					if err != nil {
						return types.ResearchV3CutoverOperation{}, err
					}
					continue
				}
			}
		case types.ResearchV3CutoverDefinitionPromoted:
			if !observed.paused {
				err = types.NewAppError(types.CodeConflict,
					"research V3 cutover lost its paused fence after definition promotion", types.ErrConflict)
			} else if observed.target {
				current := op
				op, err = c.advance(ctx, current, types.ResearchV3CutoverDefinitionPromoted,
					types.ResearchV3CutoverActionSwapped)
				if err != nil && types.CodeOf(err) == types.CodeConflict {
					op, err = c.recoverForwardStoreConflict(ctx, current, err)
					if err != nil {
						return types.ResearchV3CutoverOperation{}, err
					}
					continue
				}
			} else if err = c.journal.RecheckResearchV3CutoverDefinition(ctx, op); err == nil {
				err = c.casAction(ctx, op, desc, frozen, target, "swap-action")
			} else if types.CodeOf(err) == types.CodeConflict {
				return types.ResearchV3CutoverOperation{}, c.rollbackCutoverConflict(ctx, op, err)
			}
		case types.ResearchV3CutoverActionSwapped:
			if !observed.target {
				err = types.NewAppError(types.CodeConflict,
					"research V3 cutover target Action disappeared", types.ErrConflict)
			} else if observed.paused == op.OriginalPaused {
				current := op
				var advanced types.ResearchV3CutoverOperation
				advanced, err = c.advance(ctx, current, types.ResearchV3CutoverActionSwapped, types.ResearchV3CutoverActive)
				if err != nil && types.CodeOf(err) == types.CodeConflict {
					op, err = c.recoverForwardStoreConflict(ctx, current, err)
					if err != nil {
						return types.ResearchV3CutoverOperation{}, err
					}
					continue
				}
				if err == nil {
					op = advanced
				} else {
					op = current
				}
			} else {
				err = c.casState(ctx, op, desc, targetScheduleV3(frozen, target), true,
					op.OriginalPaused, "restore-state")
			}
		case types.ResearchV3CutoverActive:
			if !observed.target || observed.paused != op.OriginalPaused {
				return types.ResearchV3CutoverOperation{}, types.NewAppError(
					types.CodeConflict, "research V3 cutover active checkpoint drifted", types.ErrConflict)
			}
			return op, nil
		case types.ResearchV3CutoverAborted:
			return types.ResearchV3CutoverOperation{}, types.NewAppError(
				types.CodeConflict, "research V3 cutover was aborted without Schedule ownership", types.ErrConflict)
		case types.ResearchV3CutoverRollbackPaused, types.ResearchV3CutoverDefinitionRestored, types.ResearchV3CutoverRolledBack:
			return types.ResearchV3CutoverOperation{}, types.NewAppError(
				types.CodeConflict, "research V3 cutover is rolling back", types.ErrConflict)
		default:
			return types.ResearchV3CutoverOperation{}, types.NewAppError(
				types.CodeValidation, "research V3 cutover phase is invalid", types.ErrValidation)
		}
		if err != nil {
			// An UpdateSchedule response can be lost after Temporal applied it.
			// Only a fresh Describe may decide the outcome, so loop once more.
			if ctx.Err() != nil {
				return types.ResearchV3CutoverOperation{}, cutoverRemoteError("cutover canceled", ctx.Err())
			}
			if latest, found, loadErr := c.journal.LoadResearchV3Cutover(
				ctx, op.TenantID, op.UserID, op.TaskID, op.IdempotencyKey,
			); loadErr == nil && found {
				op = latest
			}
			continue
		}
	}
	return types.ResearchV3CutoverOperation{}, types.NewAppError(
		types.CodeInternal, "research V3 cutover recovery budget exhausted", nil)
}

func (c *researchV3CutoverCoordinator) abortUnownedSchedulePause(
	ctx context.Context, op types.ResearchV3CutoverOperation,
) error {
	cause := types.NewAppError(types.CodeConflict,
		"research V3 cutover cannot prove Schedule pause ownership", types.ErrConflict)
	if err := c.journal.RevokeResearchV3DeliveryAuthority(ctx, op); err != nil {
		return errors.Join(cause, err)
	}
	if _, err := c.advance(ctx, op, op.Phase, types.ResearchV3CutoverAborted); err != nil {
		return errors.Join(cause, err)
	}
	return cause
}

// rollbackCutoverConflict closes the unavoidable Postgres-to-Temporal gap.
// A definition edit can commit after the pre-CAS DB check but before the
// Schedule CAS.  The post-CAS checkpoint rechecks the head; on drift, this
// helper revokes the still-staged authority and restores the frozen Action
// before returning the original conflict.  A disconnected bounded context
// keeps the compensation alive if the caller canceled after Temporal applied.
func (c *researchV3CutoverCoordinator) rollbackCutoverConflict(
	ctx context.Context, op types.ResearchV3CutoverOperation, cause error,
) error {
	recoveryCtx, cancel := context.WithTimeout(
		context.WithoutCancel(ctx), researchV3CutoverConflictRollbackTimeout)
	defer cancel()
	_, rollbackErr := c.Rollback(recoveryCtx, op.TenantID, researchV3CutoverRequest{
		TaskID: op.TaskID, UserID: op.UserID, IdempotencyKey: op.IdempotencyKey,
	})
	if rollbackErr != nil {
		return errors.Join(cause, types.NewAppError(types.CodeInternal,
			"research V3 cutover conflict rollback failed", rollbackErr))
	}
	return cause
}

func (c *researchV3CutoverCoordinator) Rollback(
	ctx context.Context, tenantID int64, req researchV3CutoverRequest,
) (types.ResearchV3CutoverOperation, error) {
	if err := c.validateRequest(req); err != nil || tenantID <= 0 {
		if err != nil {
			return types.ResearchV3CutoverOperation{}, err
		}
		return types.ResearchV3CutoverOperation{}, types.NewAppError(
			types.CodeValidation, "research V3 rollback tenant is invalid", types.ErrValidation)
	}
	op, found, err := c.journal.LoadResearchV3Cutover(
		ctx, tenantID, req.UserID, req.TaskID, req.IdempotencyKey)
	if err != nil || !found {
		if err == nil {
			err = types.NewAppError(types.CodeNotFound,
				"research V3 cutover journal is unavailable", types.ErrNotFound)
		}
		return types.ResearchV3CutoverOperation{}, err
	}
	if err := c.journal.RevokeResearchV3DeliveryAuthority(ctx, op); err != nil {
		return types.ResearchV3CutoverOperation{}, err
	}
	frozen, target, err := decodeCutoverArtifactsV3(op)
	if err != nil {
		return types.ResearchV3CutoverOperation{}, err
	}
	for attempt := 0; attempt < 12; attempt++ {
		desc, describeErr := c.remote.Describe(ctx, op.TaskID)
		if describeErr != nil {
			return types.ResearchV3CutoverOperation{}, cutoverRemoteError("rollback describe", describeErr)
		}
		observed, err := verifyCutoverScheduleV3(desc, frozen, target)
		if err != nil {
			return types.ResearchV3CutoverOperation{}, err
		}
		if op.Phase == types.ResearchV3CutoverAborted {
			if observed.target {
				return types.ResearchV3CutoverOperation{}, types.NewAppError(
					types.CodeConflict, "aborted research V3 cutover has target Action", types.ErrConflict)
			}
			return op, nil
		}
		if op.Phase == types.ResearchV3CutoverManualIntervention {
			return types.ResearchV3CutoverOperation{}, types.NewAppError(
				types.CodeConflict, "research V3 rollback requires manual intervention", types.ErrConflict)
		}
		if op.Phase == types.ResearchV3CutoverPrepared && !observed.target {
			if observed.paused != op.OriginalPaused {
				op, err = c.advance(ctx, op, types.ResearchV3CutoverPrepared,
					types.ResearchV3CutoverAborted)
				if err != nil {
					return types.ResearchV3CutoverOperation{}, err
				}
				return op, nil
			}
			op, err = c.advance(ctx, op, types.ResearchV3CutoverPrepared,
				types.ResearchV3CutoverRollbackPaused)
			if err != nil {
				continue
			}
			continue
		}
		if op.Phase == types.ResearchV3CutoverPauseRequested {
			if observed.target {
				return types.ResearchV3CutoverOperation{}, types.NewAppError(
					types.CodeConflict, "rollback found target Action before pause ownership", types.ErrConflict)
			}
			if !op.OriginalPaused && observed.paused {
				paused := proto.Clone(frozen).(*schedulepb.Schedule)
				if paused.State == nil {
					paused.State = &schedulepb.ScheduleState{}
				}
				paused.State.Paused = true
				if proofErr := c.remote.CompareAndSwap(ctx, op.TaskID, paused,
					op.FrozenConflictToken, researchV3CutoverRequestID(op, "pause")); proofErr != nil {
					op, err = c.advance(ctx, op, types.ResearchV3CutoverPauseRequested,
						types.ResearchV3CutoverAborted)
					if err != nil {
						return types.ResearchV3CutoverOperation{}, err
					}
					return op, nil
				}
			}
			op, err = c.advance(ctx, op, types.ResearchV3CutoverPauseRequested,
				types.ResearchV3CutoverRollbackPaused)
			if err != nil {
				continue
			}
			continue
		}
		if op.Phase == types.ResearchV3CutoverRolledBack {
			if observed.target || observed.paused != op.OriginalPaused {
				return types.ResearchV3CutoverOperation{}, types.NewAppError(
					types.CodeConflict, "research V3 rollback checkpoint drifted", types.ErrConflict)
			}
			return op, nil
		}
		if op.Phase == types.ResearchV3CutoverDefinitionRestored {
			if observed.target {
				return types.ResearchV3CutoverOperation{}, types.NewAppError(
					types.CodeConflict, "research V3 Action remained after definition restore", types.ErrConflict)
			}
			if observed.paused != op.OriginalPaused {
				err = c.casState(ctx, op, desc, frozen, true, op.OriginalPaused, "rollback-state")
				if err != nil && ctx.Err() != nil {
					return types.ResearchV3CutoverOperation{}, err
				}
				continue
			}
			op, err = c.advance(ctx, op, types.ResearchV3CutoverDefinitionRestored,
				types.ResearchV3CutoverRolledBack)
			if err == nil {
				return op, nil
			}
			continue
		}
		if op.Phase != types.ResearchV3CutoverRollbackPaused {
			switch op.Phase {
			case types.ResearchV3CutoverActive:
				if observed.paused && !op.OriginalPaused {
					return types.ResearchV3CutoverOperation{}, c.markManualIntervention(
						ctx, op, "research V3 rollback cannot prove emergency pause ownership")
				}
				if observed.paused {
					op, err = c.advance(ctx, op, op.Phase, types.ResearchV3CutoverRollbackPaused)
				} else {
					token := append([]byte(nil), desc.GetConflictToken()...)
					digest := sha256.Sum256(token)
					op, err = c.journal.BeginResearchV3RollbackPause(
						ctx, op, token, hex.EncodeToString(digest[:]))
				}
			case types.ResearchV3CutoverRollbackPauseRequested:
				if len(op.RollbackConflictToken) == 0 {
					return types.ResearchV3CutoverOperation{}, c.markManualIntervention(
						ctx, op, "research V3 rollback pause proof is unavailable")
				}
				pausedSchedule := chooseObservedScheduleV3(frozen, target, observed.target)
				pausedSchedule = proto.Clone(pausedSchedule).(*schedulepb.Schedule)
				if pausedSchedule.State == nil {
					pausedSchedule.State = &schedulepb.ScheduleState{}
				}
				pausedSchedule.State.Paused = true
				if !observed.paused && !bytes.Equal(desc.GetConflictToken(), op.RollbackConflictToken) {
					return types.ResearchV3CutoverOperation{}, c.markManualIntervention(
						ctx, op, "research V3 rollback pause conflict token drifted")
				}
				if proofErr := c.remote.CompareAndSwap(ctx, op.TaskID, pausedSchedule,
					op.RollbackConflictToken, researchV3CutoverRequestID(op, "rollback-pause")); proofErr != nil {
					if !observed.paused && ctx.Err() == nil {
						// The provider may have committed the exact request and lost
						// the response. A fresh Describe followed by the same durable
						// request id is the only ownership proof.
						continue
					}
					return types.ResearchV3CutoverOperation{}, c.markManualIntervention(
						ctx, op, "research V3 rollback cannot prove pause ownership")
				}
				op, err = c.advance(ctx, op, op.Phase, types.ResearchV3CutoverRollbackPaused)
			case types.ResearchV3CutoverActionSwapped, types.ResearchV3CutoverDefinitionPromoted:
				if !observed.paused {
					token := append([]byte(nil), desc.GetConflictToken()...)
					digest := sha256.Sum256(token)
					op, err = c.journal.BeginResearchV3RollbackPause(
						ctx, op, token, hex.EncodeToString(digest[:]))
				} else {
					op, err = c.advance(ctx, op, op.Phase, types.ResearchV3CutoverRollbackPaused)
				}
			case types.ResearchV3CutoverPaused:
				if !observed.paused {
					return types.ResearchV3CutoverOperation{}, c.markManualIntervention(
						ctx, op, "research V3 rollback lost the cutover pause fence")
				}
				op, err = c.advance(ctx, op, op.Phase, types.ResearchV3CutoverRollbackPaused)
			default:
				return types.ResearchV3CutoverOperation{}, types.NewAppError(
					types.CodeConflict, "research V3 rollback checkpoint is not recoverable", types.ErrConflict)
			}
			if err != nil {
				if latest, found, loadErr := c.journal.LoadResearchV3Cutover(
					ctx, op.TenantID, op.UserID, op.TaskID, op.IdempotencyKey,
				); loadErr == nil && found {
					op = latest
				}
				continue
			}
		}
		if observed.target {
			err = c.casAction(ctx, op, desc, targetScheduleV3(frozen, target), frozen.GetAction(), "rollback-action")
			if err != nil && ctx.Err() != nil {
				return types.ResearchV3CutoverOperation{}, err
			}
			continue
		}
		op, err = c.journal.RestoreResearchV3OriginalDefinition(ctx, op)
		if err == nil {
			continue
		}
		if latest, found, loadErr := c.journal.LoadResearchV3Cutover(
			ctx, op.TenantID, op.UserID, op.TaskID, op.IdempotencyKey,
		); loadErr == nil && found {
			op = latest
		}
	}
	return types.ResearchV3CutoverOperation{}, types.NewAppError(
		types.CodeInternal, "research V3 rollback recovery budget exhausted", nil)
}

func (c *researchV3CutoverCoordinator) markManualIntervention(
	ctx context.Context, op types.ResearchV3CutoverOperation, message string,
) error {
	cause := types.NewAppError(types.CodeConflict, message, types.ErrConflict)
	if _, err := c.advance(ctx, op, op.Phase, types.ResearchV3CutoverManualIntervention); err != nil {
		return errors.Join(cause, err)
	}
	return cause
}

func (c *researchV3CutoverCoordinator) validateRequest(req researchV3CutoverRequest) error {
	if req.TaskID != c.exactTaskID || req.UserID <= 0 || req.IdempotencyKey == "" ||
		strings.TrimSpace(req.IdempotencyKey) != req.IdempotencyKey || len(req.IdempotencyKey) > 512 {
		return types.NewAppError(types.CodeNotFound,
			"research V3 exact cutover task is unavailable", types.ErrNotFound)
	}
	return nil
}

func (c *researchV3CutoverCoordinator) buildTargetAction(
	mirror *types.Schedule, old *schedulepb.ScheduleAction, authorityToken string,
) (*schedulepb.ScheduleAction, error) {
	if old == nil || old.GetStartWorkflow() == nil {
		return nil, types.NewAppError(types.CodeConflict,
			"research V3 cutover requires a workflow Action", types.ErrConflict)
	}
	payload, err := c.encode(workflow.ResearchScheduledInputV3{
		TenantID: mirror.TenantID, UserID: mirror.UserID, TaskID: mirror.ID,
		ActionAuthorizationToken: authorityToken,
	})
	if err != nil || payload == nil {
		return nil, types.NewAppError(types.CodeInternal,
			"encode research V3 Schedule Action", err)
	}
	target := proto.Clone(old).(*schedulepb.ScheduleAction)
	target.GetStartWorkflow().WorkflowType = &commonpb.WorkflowType{
		Name: workflow.ResearchScheduledWorkflowV3Name,
	}
	target.GetStartWorkflow().Input = &commonpb.Payloads{Payloads: []*commonpb.Payload{payload}}
	return target, nil
}

func newResearchV3ActionAuthorization() (string, string, error) {
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		return "", "", types.NewAppError(types.CodeInternal,
			"mint research V3 Action authorization", err)
	}
	token := hex.EncodeToString(secret)
	digest := sha256.Sum256([]byte(token))
	return token, hex.EncodeToString(digest[:]), nil
}

type observedCutoverScheduleV3 struct {
	target bool
	paused bool
}

func verifyCutoverScheduleV3(
	desc *workflowservice.DescribeScheduleResponse,
	frozen *schedulepb.Schedule, target *schedulepb.ScheduleAction,
) (observedCutoverScheduleV3, error) {
	if desc == nil || desc.GetSchedule() == nil || len(desc.GetConflictToken()) == 0 {
		return observedCutoverScheduleV3{}, types.NewAppError(types.CodeConflict,
			"research V3 cutover Describe is incomplete", types.ErrConflict)
	}
	current := desc.GetSchedule()
	if !proto.Equal(current.GetSpec(), frozen.GetSpec()) ||
		!proto.Equal(current.GetPolicies(), frozen.GetPolicies()) {
		return observedCutoverScheduleV3{}, types.NewAppError(types.CodeConflict,
			"research V3 cutover Schedule spec or policy changed", types.ErrConflict)
	}
	isOld := proto.Equal(current.GetAction(), frozen.GetAction())
	isTarget := proto.Equal(current.GetAction(), target)
	if !isOld && !isTarget {
		return observedCutoverScheduleV3{}, types.NewAppError(types.CodeConflict,
			"research V3 cutover Schedule Action changed independently", types.ErrConflict)
	}
	if !stateEqualsExceptPausedV3(current.GetState(), frozen.GetState()) {
		return observedCutoverScheduleV3{}, types.NewAppError(types.CodeConflict,
			"research V3 cutover Schedule state changed independently", types.ErrConflict)
	}
	return observedCutoverScheduleV3{target: isTarget, paused: current.GetState().GetPaused()}, nil
}

func stateEqualsExceptPausedV3(left, right *schedulepb.ScheduleState) bool {
	l := proto.Clone(left)
	r := proto.Clone(right)
	if l == nil {
		l = &schedulepb.ScheduleState{}
	}
	if r == nil {
		r = &schedulepb.ScheduleState{}
	}
	ls := l.(*schedulepb.ScheduleState)
	rs := r.(*schedulepb.ScheduleState)
	ls.Paused, rs.Paused = false, false
	return proto.Equal(ls, rs)
}

func (c *researchV3CutoverCoordinator) casState(
	ctx context.Context, op types.ResearchV3CutoverOperation,
	desc *workflowservice.DescribeScheduleResponse, base *schedulepb.Schedule,
	expectedPaused, targetPaused bool, step string,
) error {
	if desc.GetSchedule().GetState().GetPaused() != expectedPaused {
		return types.NewAppError(types.CodeConflict,
			"research V3 cutover Schedule state raced", types.ErrConflict)
	}
	next := proto.Clone(base).(*schedulepb.Schedule)
	if next.State == nil {
		next.State = &schedulepb.ScheduleState{}
	}
	next.State.Paused = targetPaused
	return c.remote.CompareAndSwap(ctx, op.TaskID, next,
		desc.GetConflictToken(), researchV3CutoverRequestID(op, step))
}

func (c *researchV3CutoverCoordinator) casAction(
	ctx context.Context, op types.ResearchV3CutoverOperation,
	desc *workflowservice.DescribeScheduleResponse, base *schedulepb.Schedule,
	target *schedulepb.ScheduleAction, step string,
) error {
	if !desc.GetSchedule().GetState().GetPaused() {
		return types.NewAppError(types.CodeConflict,
			"research V3 cutover Action CAS requires paused Schedule", types.ErrConflict)
	}
	next := proto.Clone(base).(*schedulepb.Schedule)
	next.Action = proto.Clone(target).(*schedulepb.ScheduleAction)
	if next.State == nil {
		next.State = &schedulepb.ScheduleState{}
	}
	next.State.Paused = true
	return c.remote.CompareAndSwap(ctx, op.TaskID, next,
		desc.GetConflictToken(), researchV3CutoverRequestID(op, step))
}

func (c *researchV3CutoverCoordinator) advance(
	ctx context.Context, op types.ResearchV3CutoverOperation,
	expected, next types.ResearchV3CutoverPhase,
) (types.ResearchV3CutoverOperation, error) {
	advanced, err := c.journal.AdvanceResearchV3Cutover(ctx, op, expected, next)
	if err != nil {
		return op, err
	}
	return advanced, nil
}

// recoverForwardStoreConflict separates a stale optimistic checkpoint from a
// verified definition drift. A second Resume may have legally advanced the
// same journal; compensating that stale caller would undo another caller's
// owned Temporal mutation.
func (c *researchV3CutoverCoordinator) recoverForwardStoreConflict(
	ctx context.Context, stale types.ResearchV3CutoverOperation, cause error,
) (types.ResearchV3CutoverOperation, error) {
	latest, found, err := c.journal.LoadResearchV3Cutover(
		ctx, stale.TenantID, stale.UserID, stale.TaskID, stale.IdempotencyKey)
	if err != nil {
		return types.ResearchV3CutoverOperation{}, err
	}
	if !found {
		return types.ResearchV3CutoverOperation{}, cause
	}
	if latest.Phase != stale.Phase {
		return latest, nil
	}
	if errors.Is(cause, types.ErrResearchV3CutoverDrift) {
		return types.ResearchV3CutoverOperation{}, c.rollbackCutoverConflict(ctx, latest, cause)
	}
	return types.ResearchV3CutoverOperation{}, cause
}

func cloneDescribedScheduleV3(
	desc *workflowservice.DescribeScheduleResponse,
) (*schedulepb.Schedule, error) {
	if desc == nil || desc.GetSchedule() == nil || desc.GetSchedule().GetAction() == nil ||
		len(desc.GetConflictToken()) == 0 {
		return nil, types.NewAppError(types.CodeConflict,
			"research V3 cutover Describe is incomplete", types.ErrConflict)
	}
	return proto.Clone(desc.GetSchedule()).(*schedulepb.Schedule), nil
}

func targetScheduleV3(
	frozen *schedulepb.Schedule, target *schedulepb.ScheduleAction,
) *schedulepb.Schedule {
	result := proto.Clone(frozen).(*schedulepb.Schedule)
	result.Action = proto.Clone(target).(*schedulepb.ScheduleAction)
	return result
}

func chooseObservedScheduleV3(
	frozen *schedulepb.Schedule, target *schedulepb.ScheduleAction, targetObserved bool,
) *schedulepb.Schedule {
	if targetObserved {
		return targetScheduleV3(frozen, target)
	}
	return frozen
}

func marshalScheduleArtifactV3(message proto.Message) ([]byte, string, error) {
	payload, err := (proto.MarshalOptions{Deterministic: true}).Marshal(message)
	if err != nil || len(payload) == 0 {
		return nil, "", types.NewAppError(types.CodeInternal,
			"encode research V3 cutover artifact", err)
	}
	digest := sha256.Sum256(payload)
	return payload, hex.EncodeToString(digest[:]), nil
}

func decodeCutoverArtifactsV3(
	op types.ResearchV3CutoverOperation,
) (*schedulepb.Schedule, *schedulepb.ScheduleAction, error) {
	check := func(payload []byte, expected string) bool {
		digest := sha256.Sum256(payload)
		return hex.EncodeToString(digest[:]) == expected
	}
	if !check(op.FrozenSchedule, op.FrozenScheduleDigest) ||
		!check(op.FrozenConflictToken, op.ConflictTokenDigest) ||
		!check(op.TargetAction, op.TargetActionDigest) {
		return nil, nil, types.NewAppError(types.CodeValidation,
			"research V3 cutover journal digest mismatch", types.ErrValidation)
	}
	frozen := new(schedulepb.Schedule)
	target := new(schedulepb.ScheduleAction)
	if err := proto.Unmarshal(op.FrozenSchedule, frozen); err != nil {
		return nil, nil, types.NewAppError(types.CodeValidation,
			"decode research V3 frozen Schedule", err)
	}
	if err := proto.Unmarshal(op.TargetAction, target); err != nil {
		return nil, nil, types.NewAppError(types.CodeValidation,
			"decode research V3 target Action", err)
	}
	return frozen, target, nil
}

func researchV3CutoverRequestID(op types.ResearchV3CutoverOperation, step string) string {
	digest := sha256.Sum256([]byte(fmt.Sprintf(
		"vane/research-v3-cutover/%d/%d/%s", op.ID, op.Generation, step)))
	return hex.EncodeToString(digest[:])
}

func cutoverRemoteError(operation string, err error) error {
	if err == nil {
		err = errors.New("remote operation failed")
	}
	return types.NewAppError(types.CodeInternal,
		"research V3 cutover "+operation+" failed", err)
}

type schedulerResearchV3ScheduleRemote struct {
	scheduler *Scheduler
}

func (r *schedulerResearchV3ScheduleRemote) Describe(
	ctx context.Context, taskID string,
) (*workflowservice.DescribeScheduleResponse, error) {
	return r.scheduler.describeResearchV3CutoverSchedule(ctx, taskID)
}

func (s *Scheduler) describeResearchV3CutoverSchedule(
	ctx context.Context, taskID string,
) (*workflowservice.DescribeScheduleResponse, error) {
	return s.c.WorkflowService().DescribeSchedule(
		ctx, &workflowservice.DescribeScheduleRequest{
			Namespace: s.taskScheduleEnv.namespace, ScheduleId: taskID,
		})
}

func (r *schedulerResearchV3ScheduleRemote) CompareAndSwap(
	ctx context.Context, taskID string, schedule *schedulepb.Schedule,
	conflictToken []byte, requestID string,
) error {
	if schedule == nil || len(conflictToken) == 0 {
		return types.ErrValidation
	}
	return r.scheduler.compareAndSwapResearchV3CutoverSchedule(
		ctx, taskID, schedule, conflictToken, requestID)
}

func (s *Scheduler) compareAndSwapResearchV3CutoverSchedule(
	ctx context.Context, taskID string, schedule *schedulepb.Schedule,
	conflictToken []byte, requestID string,
) error {
	_, err := s.c.WorkflowService().UpdateSchedule(
		ctx, &workflowservice.UpdateScheduleRequest{
			Namespace: s.taskScheduleEnv.namespace, ScheduleId: taskID,
			Schedule: schedule, ConflictToken: append([]byte(nil), conflictToken...),
			RequestId: requestID, Identity: "vane/research-v3-exact-cutover/v1",
		})
	return err
}
