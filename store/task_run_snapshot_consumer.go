package store

import (
	"bytes"
	"context"
	"encoding/json"

	"github.com/YouToco/vane/runcontext"
	"github.com/YouToco/vane/runtimepolicy"
	"github.com/YouToco/vane/types"
)

// LoadCompiledRunSnapshotRefV1 performs the exact, fully scoped recovery read
// used before PrepareRun rebuilds current policy. It is what makes a committed
// first writer survive a lost Activity response even if the task is deleted or
// the next worker deployment can no longer build the old policy. No partial
// identity or reference-derived scope is accepted.
func (s *Store) LoadCompiledRunSnapshotRefV1(
	ctx context.Context,
	expected types.RunIdentity,
) (types.RunSnapshotRef, bool, error) {
	if err := validateTaskRunExpectedIdentityV1(expected); err != nil {
		return types.RunSnapshotRef{}, false,
			taskRunValidationError("scheduled v1 run identity is invalid")
	}
	lookup := CreateOrGetTaskRunSnapshotParams{
		TenantID: expected.TenantID, UserID: expected.UserID, TaskID: expected.TaskID,
		TemporalWorkflowID: expected.TemporalWorkflowID,
		TemporalRunID:      expected.TemporalRunID,
	}
	snapshot, found, err := loadTaskRunSnapshot(ctx, s.pool, lookup)
	if err != nil || !found {
		return types.RunSnapshotRef{}, found, err
	}
	ref, err := snapshot.safeRef()
	if err != nil {
		return types.RunSnapshotRef{}, false, taskRunIntegrityError()
	}
	if _, err := validateTaskRunSnapshotReferenceForExpectedV1(ref, expected); err != nil {
		return types.RunSnapshotRef{}, false, taskRunIntegrityError()
	}
	return ref, true, nil
}

// LoadCompiledTaskRunSnapshotV1 returns the typed, Activity-only view of an
// immutable compiled snapshot. The caller supplies the identity it observed
// from Temporal ActivityInfo plus trusted task input; the sealed reference is
// never accepted as a bearer token or used to invent that expected identity.
//
// The lookup starts at Tenant/User/Task scope, validates every persisted
// digest and canonical byte through the pinned V1 reader, then requires the
// caller and stored references to match exactly. It never reads the current
// schedule, playbook, source metadata, application config, or credentials.
func (s *Store) LoadCompiledTaskRunSnapshotV1(
	ctx context.Context,
	expected types.RunIdentity,
	ref types.RunSnapshotRef,
) (runcontext.CompiledSnapshotV1, error) {
	_, compiled, err := loadCompiledTaskRunSnapshotV1(
		ctx, s.pool, expected, ref)
	return compiled, err
}

func loadCompiledTaskRunSnapshotV1(
	ctx context.Context,
	q taskRunSnapshotQueryer,
	expected types.RunIdentity,
	ref types.RunSnapshotRef,
) (*taskRunSnapshot, runcontext.CompiledSnapshotV1, error) {
	callerReference, err := validateTaskRunSnapshotReferenceForExpectedV1(ref, expected)
	if err != nil {
		return nil, runcontext.CompiledSnapshotV1{},
			taskRunValidationError("task run snapshot reference is invalid")
	}
	lookup := CreateOrGetTaskRunSnapshotParams{
		TenantID: expected.TenantID, UserID: expected.UserID, TaskID: expected.TaskID,
		TemporalWorkflowID: expected.TemporalWorkflowID,
		TemporalRunID:      expected.TemporalRunID,
	}
	snapshot, found, err := loadTaskRunSnapshot(ctx, q, lookup)
	if err != nil {
		return nil, runcontext.CompiledSnapshotV1{}, err
	}
	if !found {
		return nil, runcontext.CompiledSnapshotV1{}, taskRunNotFound()
	}
	storedRef, err := snapshot.safeRef()
	if err != nil {
		return nil, runcontext.CompiledSnapshotV1{}, taskRunIntegrityError()
	}
	storedReference, err := validateTaskRunSnapshotReferenceForExpectedV1(storedRef, expected)
	if err != nil || storedReference != callerReference {
		return nil, runcontext.CompiledSnapshotV1{}, taskRunIntegrityError()
	}

	compiled, err := decodeCompiledTaskRunSnapshotV1(storedRef, snapshot.Payload)
	if err != nil {
		return nil, runcontext.CompiledSnapshotV1{}, err
	}
	return snapshot, compiled, nil
}

func decodeCompiledTaskRunSnapshotV1(
	storedRef types.RunSnapshotRef,
	raw []byte,
) (runcontext.CompiledSnapshotV1, error) {
	decoded, err := readTaskRunSnapshotPayload(raw)
	if err != nil || decoded.Payload == nil || decoded.Payload.Definition == nil ||
		decoded.Payload.Policies == nil || decoded.Payload.Budget == nil {
		return runcontext.CompiledSnapshotV1{}, taskRunIntegrityError()
	}
	policies := decoded.Payload.Policies
	capability, err := runtimepolicy.DecodeCapabilityCatalogV1(policies.CapabilityCatalog)
	if err != nil {
		return runcontext.CompiledSnapshotV1{}, taskRunIntegrityError()
	}
	tools, err := runtimepolicy.DecodeToolPolicyV1(policies.ToolPolicy)
	if err != nil {
		return runcontext.CompiledSnapshotV1{}, taskRunIntegrityError()
	}
	prompts, err := runtimepolicy.DecodePromptPolicyV1(policies.PromptPolicy)
	if err != nil {
		return runcontext.CompiledSnapshotV1{}, taskRunIntegrityError()
	}
	models, err := runtimepolicy.DecodeModelPolicyV1(policies.ModelPolicy)
	if err != nil {
		return runcontext.CompiledSnapshotV1{}, taskRunIntegrityError()
	}
	quotas, err := runtimepolicy.DecodeQuotaPolicyV1(policies.QuotaPolicy)
	if err != nil {
		return runcontext.CompiledSnapshotV1{}, taskRunIntegrityError()
	}
	// Strict decoders normalize ordering. Re-encoding must reproduce the exact
	// persisted body, otherwise a non-canonical historical body could acquire a
	// different meaning while retaining a valid generic JSON digest.
	if !sameRuntimePolicyV1(policies.CapabilityCatalog,
		runtimepolicy.EncodeCapabilityCatalogV1, capability) ||
		!sameRuntimePolicyV1(policies.ToolPolicy, runtimepolicy.EncodeToolPolicyV1, tools) ||
		!sameRuntimePolicyV1(policies.PromptPolicy, runtimepolicy.EncodePromptPolicyV1, prompts) ||
		!sameRuntimePolicyV1(policies.ModelPolicy, runtimepolicy.EncodeModelPolicyV1, models) ||
		!sameRuntimePolicyV1(policies.QuotaPolicy, runtimepolicy.EncodeQuotaPolicyV1, quotas) {
		return runcontext.CompiledSnapshotV1{}, taskRunIntegrityError()
	}

	definition := decoded.Payload.Definition
	sources := make([]runcontext.SourceV1, len(definition.Sources))
	for i, source := range definition.Sources {
		sources[i] = runcontext.SourceV1{
			SourceID: source.SourceID, Platform: types.Platform(source.Platform),
			Capability: types.Capability(source.Capability), Title: source.Title,
			URL: source.URL, Config: bytes.Clone(source.Config),
		}
	}
	return runcontext.CompiledSnapshotV1{
		Ref: storedRef, Mode: types.ExecutionMode(decoded.Payload.Mode),
		AdaptiveVersion: decoded.Payload.AdaptiveVersion,
		Budget: types.PlannerBudget{
			MaxPlannerRounds: decoded.Payload.Budget.MaxPlannerRounds,
			MaxToolCalls:     decoded.Payload.Budget.MaxToolCalls,
			MaxTokens:        decoded.Payload.Budget.MaxTokens,
			MaxCostMicroUSD:  decoded.Payload.Budget.MaxCostMicroUSD,
			DurationMs:       decoded.Payload.Budget.DurationMs,
		},
		Definition: runcontext.DefinitionV1{
			TaskID: definition.TaskID, TenantID: definition.TenantID,
			UserID: definition.UserID, NLDescription: definition.NLDescription,
			SpecJSON: bytes.Clone(definition.SpecJSON), ScopeJSON: bytes.Clone(definition.ScopeJSON),
			PlaybookContent: definition.PlaybookContent,
			Strictness:      types.PushStrictness(definition.Strictness),
			SourceScope:     definition.SourceScope,
			FetchPlan:       bytes.Clone(definition.FetchPlan), Sources: sources,
		},
		Policy: runtimepolicy.BundleV1{
			SchemaVersion:     runtimepolicy.BundleSchemaVersionV1,
			CapabilityCatalog: capability, ToolPolicy: tools,
			PromptPolicy: prompts, ModelPolicy: models, QuotaPolicy: quotas,
		},
	}, nil
}

func sameRuntimePolicyV1[T any](
	want json.RawMessage,
	encode func(T) ([]byte, error),
	value T,
) bool {
	got, err := encode(value)
	if err != nil {
		return false
	}
	// Snapshot persistence canonicalizes every opaque policy object through the
	// pinned generic JSON canonicalizer before hashing it. Apply that same
	// representation step after the typed encoder has normalized semantic
	// ordering (for example model-call arrays), then compare exact bytes.
	got, err = canonicalTaskRunJSONObjectV1(got)
	return err == nil && bytes.Equal(got, want)
}
