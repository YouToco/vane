package store

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/runtimepolicy"
	"github.com/YouToco/vane/types"
)

// PeriodicSynthesisPolicyV1 is the minimal, non-secret policy projection used
// by one periodic synthesis Activity. It is loaded from the immutable compiled
// snapshot of the newest canonical Brief in the frozen period input.
type PeriodicSynthesisPolicyV1 struct {
	RunSnapshotID int64
	SystemPrompt  string
	Renderer      string
	ModelPolicy   runtimepolicy.ModelPolicyV1
	ModelCall     runtimepolicy.ModelCallV1
	QuotaRule     runtimepolicy.QuotaBucketV1
	PolicyDigest  string
}

// LoadPeriodicSynthesisPolicyV1 validates every stored snapshot digest before
// returning the exact optional periodic stage. Current task policy is never
// consulted, so edits made after the source run cannot alter a sealed period.
func (s *Store) LoadPeriodicSynthesisPolicyV1(
	ctx context.Context,
	tenantID, userID int64,
	taskID string,
	runSnapshotID int64,
) (PeriodicSynthesisPolicyV1, error) {
	if tenantID <= 0 || userID <= 0 || taskID == "" || runSnapshotID <= 0 {
		return PeriodicSynthesisPolicyV1{}, types.NewAppError(
			types.CodeValidation, "周期综合运行策略范围无效", nil)
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{
		IsoLevel: pgx.RepeatableRead,
	})
	if err != nil {
		return PeriodicSynthesisPolicyV1{},
			briefFeedDBError("开启周期综合策略事务", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var authorized bool
	err = tx.QueryRow(ctx,
		`SELECT true
		   FROM schedules s
		   JOIN memberships m
		     ON m.tenant_id=s.tenant_id AND m.user_id=s.user_id
		  WHERE s.tenant_id=$1 AND s.user_id=$2 AND s.id=$3
		    AND s.execution_mode='compiled'
		  FOR KEY SHARE OF s,m`,
		tenantID, userID, taskID).Scan(&authorized)
	if errors.Is(err, pgx.ErrNoRows) {
		return PeriodicSynthesisPolicyV1{}, types.NewAppError(
			types.CodeNotFound, "周期综合运行策略范围不存在", nil)
	}
	if err != nil {
		return PeriodicSynthesisPolicyV1{},
			briefFeedDBError("校验周期综合策略范围", err)
	}
	snapshot, err := scanTaskRunSnapshot(tx.QueryRow(ctx,
		`SELECT `+taskRunSnapshotColumns+`
		   FROM task_run_snapshots
		  WHERE id=$1 AND tenant_id=$2 AND user_id=$3 AND task_id=$4`,
		runSnapshotID, tenantID, userID, taskID))
	if errors.Is(err, pgx.ErrNoRows) {
		return PeriodicSynthesisPolicyV1{}, types.NewAppError(
			types.CodeNotFound, "周期综合运行快照不存在", nil)
	}
	if err != nil {
		return PeriodicSynthesisPolicyV1{},
			briefFeedDBError("读取周期综合运行快照", err)
	}
	ref, err := snapshot.safeRef()
	if err != nil {
		return PeriodicSynthesisPolicyV1{}, taskRunIntegrityError()
	}
	expected := types.RunIdentity{
		TemporalWorkflowID: snapshot.TemporalWorkflowID,
		TemporalRunID:      snapshot.TemporalRunID,
		RunKind:            snapshot.RunKind,
		TenantID:           tenantID,
		UserID:             userID,
		TaskID:             taskID,
	}
	_, compiled, _, err := loadAuthoritativeCompiledTaskRunSnapshot(
		ctx, tx, expected, ref)
	if err != nil {
		return PeriodicSynthesisPolicyV1{}, err
	}
	prompt := compiled.Policy.PromptPolicy.PeriodicSynthesis
	call, hasCall := compiled.Policy.ModelPolicy.Call(
		runtimepolicy.ModelStagePeriodicSynthesis)
	quota, hasQuota := compiled.Policy.QuotaPolicy.Bucket(
		string(QuotaLLMTokens))
	if prompt == nil || !hasCall || !hasQuota {
		return PeriodicSynthesisPolicyV1{}, types.NewAppError(
			types.CodeNotFound, "该运行未启用周期综合策略", nil)
	}
	policyDigest, err := digestPeriodicIdentityV1(struct {
		RunSnapshotID int64                       `json:"run_snapshot_id"`
		Prompt        runtimepolicy.PromptStageV1 `json:"prompt"`
		Model         runtimepolicy.ModelCallV1   `json:"model"`
		Quota         runtimepolicy.QuotaBucketV1 `json:"quota"`
	}{
		RunSnapshotID: runSnapshotID,
		Prompt:        *prompt,
		Model:         call,
		Quota:         quota,
	})
	if err != nil {
		return PeriodicSynthesisPolicyV1{}, err
	}
	out := PeriodicSynthesisPolicyV1{
		RunSnapshotID: runSnapshotID,
		SystemPrompt:  prompt.SystemPrompt,
		Renderer:      prompt.RendererVersion,
		ModelPolicy:   compiled.Policy.ModelPolicy,
		ModelCall:     call,
		QuotaRule:     quota,
		PolicyDigest:  policyDigest,
	}
	if err := tx.Commit(ctx); err != nil {
		return PeriodicSynthesisPolicyV1{},
			briefFeedDBError("提交周期综合策略读取", err)
	}
	return out, nil
}
