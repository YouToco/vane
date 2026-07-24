package store

import (
	"bytes"
	"context"
	"errors"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/runcontext"
	"github.com/YouToco/vane/runtimepolicy"
	"github.com/YouToco/vane/taskstate"
	"github.com/YouToco/vane/types"
)

// CompiledRunSnapshotV2AuditStatus is an observation-only result. It never
// authorizes a run and never selects the runtime source of truth.
type CompiledRunSnapshotV2AuditStatus string

const (
	CompiledRunSnapshotV2AuditMatch         CompiledRunSnapshotV2AuditStatus = "match"
	CompiledRunSnapshotV2AuditMissing       CompiledRunSnapshotV2AuditStatus = "missing"
	CompiledRunSnapshotV2AuditNonMatch      CompiledRunSnapshotV2AuditStatus = "non_match"
	CompiledRunSnapshotV2AuditTypedMismatch CompiledRunSnapshotV2AuditStatus = "typed_mismatch"
)

type CompiledRunSnapshotV2AuditResult struct {
	Status              CompiledRunSnapshotV2AuditStatus `json:"status"`
	ShadowStatus        TaskRunSnapshotShadowStatus      `json:"shadow_status,omitempty"`
	ShadowPayloadDigest string                           `json:"shadow_payload_digest,omitempty"`
	TypedEqual          bool                             `json:"typed_equal"`
}

// AuditCompiledTaskRunSnapshotV2 materializes the retained v2 shadow for one
// exact Activity identity and sealed v1 reference. The v1 parent remains the
// sole runtime authority: the package compares the materialized value
// internally and exposes only a narrow, non-authorizing audit result.
func (s *Store) AuditCompiledTaskRunSnapshotV2(
	ctx context.Context,
	expected types.RunIdentity,
	ref types.RunSnapshotRef,
) (CompiledRunSnapshotV2AuditResult, error) {
	tx, err := s.beginTx(ctx, pgx.TxOptions{
		IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return CompiledRunSnapshotV2AuditResult{},
			taskRunDatabaseError("begin task run snapshot v2 audit read", err)
	}
	defer rollbackCompiledTaskTx(ctx, tx)

	_, result, err := auditCompiledTaskRunSnapshotV2(
		ctx, tx, expected, ref)
	if err != nil {
		return CompiledRunSnapshotV2AuditResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return CompiledRunSnapshotV2AuditResult{},
			taskRunDatabaseError("commit task run snapshot v2 audit read", err)
	}
	return result, nil
}

func auditCompiledTaskRunSnapshotV2(
	ctx context.Context,
	q taskRunSnapshotQueryer,
	expected types.RunIdentity,
	ref types.RunSnapshotRef,
) (runcontext.CompiledSnapshotV1, CompiledRunSnapshotV2AuditResult, error) {
	parent, fixedV1, err := loadCompiledTaskRunSnapshotV1(ctx, q, expected, ref)
	if err != nil {
		return runcontext.CompiledSnapshotV1{}, CompiledRunSnapshotV2AuditResult{}, err
	}

	var (
		runSnapshotID             int64
		tenantID                  int64
		userID                    int64
		taskID                    string
		workflowID                string
		runID                     string
		rawStatus                 string
		approvedDefinitionVersion *int64
		approvedDefinitionDigest  *string
		adaptiveVersion           int64
		adaptiveDigest            *string
		payload                   []byte
		payloadDigest             string
	)
	err = q.QueryRow(ctx,
		`SELECT run_snapshot_id, tenant_id, user_id, task_id,
		        temporal_workflow_id, temporal_run_id, status,
		        approved_definition_version, approved_definition_digest,
		        adaptive_version, adaptive_digest, payload, payload_digest
		   FROM task_run_snapshot_v2_shadows
		  WHERE run_snapshot_id=$1`,
		parent.ID,
	).Scan(
		&runSnapshotID, &tenantID, &userID, &taskID, &workflowID, &runID,
		&rawStatus, &approvedDefinitionVersion, &approvedDefinitionDigest,
		&adaptiveVersion, &adaptiveDigest, &payload, &payloadDigest,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return runcontext.CompiledSnapshotV1{}, CompiledRunSnapshotV2AuditResult{
			Status: CompiledRunSnapshotV2AuditMissing,
		}, nil
	}
	if err != nil {
		return runcontext.CompiledSnapshotV1{}, CompiledRunSnapshotV2AuditResult{},
			taskRunDatabaseError("load task run snapshot v2 audit material", err)
	}

	decoded, canonical, err := readTaskRunSnapshotShadowPayloadV2(payload)
	if err != nil ||
		runSnapshotID != parent.ID ||
		tenantID != parent.TenantID || userID != parent.UserID ||
		taskID != parent.TaskID ||
		workflowID != parent.TemporalWorkflowID || runID != parent.TemporalRunID ||
		rawStatus != string(decoded.Status) ||
		(decoded.Approved == nil) != (approvedDefinitionVersion == nil) ||
		(decoded.Approved == nil) != (approvedDefinitionDigest == nil) ||
		approvedVersionValue(decoded.Approved) != pointerValue(approvedDefinitionVersion) ||
		approvedDigestValue(decoded.Approved) != pointerStringValue(approvedDefinitionDigest) ||
		(decoded.Adaptive == nil) != (adaptiveDigest == nil) ||
		adaptiveVersionValue(decoded.Adaptive) != adaptiveVersion ||
		adaptiveDigestValue(decoded.Adaptive) != pointerStringValue(adaptiveDigest) ||
		!bytes.Equal(canonical, payload) ||
		!constantTimeDigestEqual(sha256Hex(payload), payloadDigest) ||
		decoded.Identity.TenantID != expected.TenantID ||
		decoded.Identity.UserID != expected.UserID ||
		decoded.Identity.TaskID != expected.TaskID ||
		decoded.Identity.TemporalWorkflowID != expected.TemporalWorkflowID ||
		decoded.Identity.TemporalRunID != expected.TemporalRunID ||
		decoded.Legacy.SnapshotID != parent.ID ||
		decoded.Legacy.PayloadDigest != parent.PayloadDigest ||
		!bytes.Equal(decoded.Legacy.Payload, parent.Payload) {
		return runcontext.CompiledSnapshotV1{}, CompiledRunSnapshotV2AuditResult{},
			taskRunIntegrityError()
	}

	result := CompiledRunSnapshotV2AuditResult{
		Status:              CompiledRunSnapshotV2AuditNonMatch,
		ShadowStatus:        decoded.Status,
		ShadowPayloadDigest: payloadDigest,
	}
	if decoded.Status != TaskRunSnapshotShadowMatch {
		return runcontext.CompiledSnapshotV1{}, result, nil
	}

	embeddedV1, err := decodeCompiledTaskRunSnapshotV1(
		fixedV1.Ref, decoded.Legacy.Payload)
	if err != nil || decoded.Approved == nil {
		return runcontext.CompiledSnapshotV1{}, CompiledRunSnapshotV2AuditResult{},
			taskRunIntegrityError()
	}
	approved, err := taskstate.DecodeApprovedDefinitionV1(decoded.Approved.Payload)
	if err != nil {
		return runcontext.CompiledSnapshotV1{}, CompiledRunSnapshotV2AuditResult{},
			taskRunIntegrityError()
	}
	materialized := embeddedV1
	materialized.Definition = approvedDefinitionRunContextV1(approved)
	if !compiledSnapshotV1ExactEqual(fixedV1, materialized) {
		result.Status = CompiledRunSnapshotV2AuditTypedMismatch
		return runcontext.CompiledSnapshotV1{}, result, nil
	}
	result.Status = CompiledRunSnapshotV2AuditMatch
	result.TypedEqual = true
	return materialized, result, nil
}

func approvedDefinitionRunContextV1(
	definition taskstate.ApprovedDefinitionV1,
) runcontext.DefinitionV1 {
	sources := make([]runcontext.SourceV1, len(definition.Sources))
	for i, source := range definition.Sources {
		sources[i] = runcontext.SourceV1{
			SourceID: source.SourceID, Platform: source.Platform,
			Capability: source.Capability, Title: source.Title,
			URL: source.URL, Config: bytes.Clone(source.Config),
		}
	}
	return runcontext.DefinitionV1{
		TaskID: definition.TaskID, TenantID: definition.TenantID,
		UserID: definition.UserID, NLDescription: definition.NLDescription,
		SpecJSON:        bytes.Clone(definition.SpecJSON),
		ScopeJSON:       bytes.Clone(definition.ScopeJSON),
		PlaybookContent: definition.PlaybookContent,
		Strictness:      definition.Strictness,
		SourceScope:     string(definition.SourceScope),
		FetchPlan:       bytes.Clone(definition.FetchPlan),
		Sources:         sources,
	}
}

func compiledSnapshotV1ExactEqual(
	left runcontext.CompiledSnapshotV1,
	right runcontext.CompiledSnapshotV1,
) bool {
	leftPolicy, leftPolicyErr := runtimepolicy.EncodeBundleV1(left.Policy)
	rightPolicy, rightPolicyErr := runtimepolicy.EncodeBundleV1(right.Policy)
	return left.Ref == right.Ref &&
		left.Mode == right.Mode &&
		left.AdaptiveVersion == right.AdaptiveVersion &&
		left.Budget == right.Budget &&
		leftPolicyErr == nil && rightPolicyErr == nil &&
		rawBytesExactEqual(leftPolicy, rightPolicy) &&
		compiledDefinitionV1ExactEqual(left.Definition, right.Definition)
}

func compiledDefinitionV1ExactEqual(
	left runcontext.DefinitionV1,
	right runcontext.DefinitionV1,
) bool {
	if left.TaskID != right.TaskID ||
		left.TenantID != right.TenantID ||
		left.UserID != right.UserID ||
		left.NLDescription != right.NLDescription ||
		!rawBytesExactEqual(left.SpecJSON, right.SpecJSON) ||
		!rawBytesExactEqual(left.ScopeJSON, right.ScopeJSON) ||
		left.PlaybookContent != right.PlaybookContent ||
		left.Strictness != right.Strictness ||
		left.SourceScope != right.SourceScope ||
		!rawBytesExactEqual(left.FetchPlan, right.FetchPlan) ||
		len(left.Sources) != len(right.Sources) {
		return false
	}
	for i := range left.Sources {
		l, r := left.Sources[i], right.Sources[i]
		if l.SourceID != r.SourceID ||
			l.Platform != r.Platform ||
			l.Capability != r.Capability ||
			l.Title != r.Title ||
			l.URL != r.URL ||
			!rawBytesExactEqual(l.Config, r.Config) {
			return false
		}
	}
	return true
}

func rawBytesExactEqual(left []byte, right []byte) bool {
	return (left == nil) == (right == nil) && bytes.Equal(left, right)
}
