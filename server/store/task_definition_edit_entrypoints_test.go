package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/YouToco/vane/server/definitioneditwire"
	"github.com/YouToco/vane/server/taskstate"
	"github.com/YouToco/vane/server/types"
)

type taskDefinitionEditEntrypointComponents struct {
	BaseDefinition   json.RawMessage `json:"base_definition"`
	TargetDefinition json.RawMessage `json:"target_definition"`
	PreparedEdit     json.RawMessage `json:"prepared_edit"`
	BaseSnapshot     json.RawMessage `json:"base_snapshot"`
}

type taskDefinitionEditEntrypointFixture struct {
	store             *Store
	base              taskstate.ApprovedDefinitionV1
	target            taskstate.ApprovedDefinitionV1
	baseRecord        ApprovedDefinitionVersionRecord
	baseBytes         []byte
	targetBytes       []byte
	prepared          definitioneditwire.PreparedEditV1
	preparedBytes     []byte
	baseSnapshotBytes []byte
	sessionID         int64
	provenanceID      string
}

func TestTaskDefinitionEditEntrypoints_CreateReplayAndValidation(t *testing.T) {
	t.Run("success and exact replay", func(t *testing.T) {
		f := newTaskDefinitionEditEntrypointFixture(t, true)
		frozen := f.buildProposal(t, f.databaseNow(t).Add(time.Hour),
			"entrypoint-create-success")
		params := taskDefinitionEditCreateParams(frozen)

		created, err := f.store.CreateTaskDefinitionEditOperation(t.Context(), params)
		if err != nil {
			t.Fatalf("CreateTaskDefinitionEditOperation: %v", err)
		}
		if created.Status != types.TaskDefinitionEditOperationStatusPending ||
			created.Phase != types.TaskDefinitionEditPhaseProposalSealed ||
			created.TenantID != f.base.TenantID || created.UserID != f.base.UserID ||
			created.TaskID != f.base.TaskID || created.SessionID != f.sessionID ||
			created.BaseDefinitionVersion != f.baseRecord.Version ||
			created.BaseDefinitionDigest != f.baseRecord.Digest ||
			!bytes.Equal(created.CanonicalProposal, frozen.CanonicalProposal) ||
			!bytes.Equal(created.PreparedEdit, frozen.PreparedEditBytes) {
			t.Fatalf("created operation differs from frozen proposal: %+v", created)
		}

		replayed, err := f.store.CreateTaskDefinitionEditOperation(t.Context(), params)
		if err != nil {
			t.Fatalf("exact response-loss replay: %v", err)
		}
		if replayed.ID != created.ID || replayed.ProposalDigest != created.ProposalDigest ||
			!replayed.CreatedAt.Equal(created.CreatedAt) ||
			!replayed.UpdatedAt.Equal(created.UpdatedAt) {
			t.Fatalf("replay created a different operation: first=%+v replay=%+v",
				created, replayed)
		}
		f.assertOperationCount(t, 1)
	})

	t.Run("missing successful creation provenance", func(t *testing.T) {
		f := newTaskDefinitionEditEntrypointFixture(t, false)
		frozen := f.buildProposal(t, f.databaseNow(t).Add(time.Hour),
			"entrypoint-create-no-provenance")
		if _, err := f.store.CreateTaskDefinitionEditOperation(
			t.Context(), taskDefinitionEditCreateParams(frozen),
		); !errors.Is(err, types.ErrConflict) {
			t.Fatalf("missing creation provenance error=%v, want conflict", err)
		}
		f.assertOperationCount(t, 0)
	})

	t.Run("database-clock expired proposal", func(t *testing.T) {
		f := newTaskDefinitionEditEntrypointFixture(t, true)
		// Derive the deadline from PostgreSQL itself. Process-local wall clock is
		// deliberately absent from the rejection oracle.
		frozen := f.buildProposal(t, f.databaseNow(t).Add(-time.Microsecond),
			"entrypoint-create-expired")
		if _, err := f.store.CreateTaskDefinitionEditOperation(
			t.Context(), taskDefinitionEditCreateParams(frozen),
		); !errors.Is(err, types.ErrValidation) {
			t.Fatalf("DB-clock expired proposal error=%v, want validation", err)
		}
		f.assertOperationCount(t, 0)
	})
}

func TestTaskDefinitionEditEntrypoints_ExpireTerminalOutbox(t *testing.T) {
	t.Run("expire exact replay", func(t *testing.T) {
		f := newTaskDefinitionEditEntrypointFixture(t, true)
		op := f.createOperation(t, f.databaseNow(t).Add(250*time.Millisecond),
			"entrypoint-expire")
		f.waitUntilOperationExpires(t, op)
		params := types.ExpireTaskDefinitionEditOperationParams{
			Scope: op.Scope(), ReceiptProvider: "feishu_card_patch:entrypoint",
			ReceiptTarget: "om_entrypoint_expire",
		}

		expired, err := f.store.ExpireTaskDefinitionEditOperation(t.Context(), params)
		if err != nil {
			t.Fatalf("ExpireTaskDefinitionEditOperation: %v", err)
		}
		assertTaskDefinitionEditEntrypointTerminal(
			t, f, expired, types.TaskDefinitionEditOperationStatusExpired,
			params.ReceiptProvider, params.ReceiptTarget)
		replayed, err := f.store.ExpireTaskDefinitionEditOperation(t.Context(), params)
		if err != nil {
			t.Fatalf("expire response-loss replay: %v", err)
		}
		if replayed.TombstonedAt == nil || expired.TombstonedAt == nil ||
			!replayed.TombstonedAt.Equal(*expired.TombstonedAt) {
			t.Fatalf("expire replay changed tombstone: first=%+v replay=%+v",
				expired, replayed)
		}
		f.assertReceiptCount(t, 1)

		different := params
		different.ReceiptTarget += "-different"
		if _, err := f.store.ExpireTaskDefinitionEditOperation(
			t.Context(), different); !errors.Is(err, types.ErrConflict) {
			t.Fatalf("different expire replay error=%v, want conflict", err)
		}
	})
}

func TestTaskDefinitionEditEntrypoints_AuthorizeRemotePhaseFailsClosed(t *testing.T) {
	t.Run("exact phase permitted and other phases forbidden", func(t *testing.T) {
		f := newTaskDefinitionEditEntrypointFixture(t, true)
		lease := f.acquireAndQuiesce(t, "entrypoint-authorize-exact")

		authorized, err := f.store.AuthorizeTaskDefinitionEditRemotePhase(
			t.Context(), lease, types.TaskDefinitionEditPhaseDBQuiesced)
		if err != nil {
			t.Fatalf("AuthorizeTaskDefinitionEditRemotePhase exact phase: %v", err)
		}
		if authorized.Status != types.TaskDefinitionEditOperationStatusExecuting ||
			authorized.Phase != types.TaskDefinitionEditPhaseDBQuiesced ||
			authorized.Fence != lease.Fence {
			t.Fatalf("authorized operation=%+v", authorized)
		}
		if _, err := f.store.AuthorizeTaskDefinitionEditRemotePhase(
			t.Context(), lease, types.TaskDefinitionEditPhaseDefinitionCommitted,
		); !errors.Is(err, types.ErrConflict) {
			t.Fatalf("wrong durable phase error=%v, want conflict", err)
		}
		if _, err := f.store.AuthorizeTaskDefinitionEditRemotePhase(
			t.Context(), lease, types.TaskDefinitionEditPhaseProposalSealed,
		); !errors.Is(err, types.ErrValidation) {
			t.Fatalf("non-remote phase error=%v, want validation", err)
		}
		f.assertReceiptCount(t, 0)
	})

	t.Run("schedule deletion is terminal quarantine", func(t *testing.T) {
		f := newTaskDefinitionEditEntrypointFixture(t, true)
		lease := f.acquireAndQuiesce(t, "entrypoint-authorize-deleted")
		if _, err := f.store.pool.Exec(t.Context(),
			`DELETE FROM schedules WHERE tenant_id=$1 AND user_id=$2 AND id=$3`,
			lease.TargetTenantID, lease.TargetUserID, lease.TaskID); err != nil {
			t.Fatalf("delete schedule fixture: %v", err)
		}

		blocked, err := f.store.AuthorizeTaskDefinitionEditRemotePhase(
			t.Context(), lease, types.TaskDefinitionEditPhaseDBQuiesced)
		if !errors.Is(err, types.ErrTaskDefinitionEditTerminal) {
			t.Fatalf("deleted schedule authorization error=%v, want terminal", err)
		}
		if blocked == nil ||
			blocked.Status != types.TaskDefinitionEditOperationStatusBlocked ||
			blocked.Phase != types.TaskDefinitionEditPhaseDBQuiesced ||
			blocked.ErrorCode != string(types.TaskDefinitionEditBlockScheduleDeleted) {
			t.Fatalf("deleted schedule terminal operation=%+v", blocked)
		}
		assertTaskDefinitionEditEntrypointReceipt(t, f, blocked,
			"feishu_card_patch:entrypoint", "om_entrypoint_authorize")
		f.assertReceiptCount(t, 1)
	})

	t.Run("newer approved head is superseded quarantine", func(t *testing.T) {
		f := newTaskDefinitionEditEntrypointFixture(t, true)
		lease := f.acquireAndQuiesce(t, "entrypoint-authorize-superseded")
		f.advanceHeadPastTarget(t)

		superseded, err := f.store.AuthorizeTaskDefinitionEditRemotePhase(
			t.Context(), lease, types.TaskDefinitionEditPhaseDBQuiesced)
		if !errors.Is(err, types.ErrTaskDefinitionEditTerminal) {
			t.Fatalf("superseded authorization error=%v, want terminal", err)
		}
		if superseded == nil ||
			superseded.Status != types.TaskDefinitionEditOperationStatusSuperseded ||
			superseded.Phase != types.TaskDefinitionEditPhaseDBQuiesced ||
			superseded.ErrorCode != "definition_superseded" {
			t.Fatalf("superseded terminal operation=%+v", superseded)
		}
		assertTaskDefinitionEditEntrypointReceipt(t, f, superseded,
			"feishu_card_patch:entrypoint", "om_entrypoint_authorize")
		f.assertReceiptCount(t, 1)

		var status types.ScheduleStatus
		var markerID *string
		var markerFence *int64
		if err := f.store.pool.QueryRow(t.Context(), `
			SELECT status, definition_edit_operation_id, definition_edit_fence
			  FROM schedules WHERE tenant_id=$1 AND user_id=$2 AND id=$3`,
			lease.TargetTenantID, lease.TargetUserID, lease.TaskID,
		).Scan(&status, &markerID, &markerFence); err != nil {
			t.Fatal(err)
		}
		if status != types.ScheduleStatusPaused || markerID == nil ||
			*markerID != lease.ID || markerFence == nil || *markerFence != lease.Fence {
			t.Fatalf("superseded quarantine was cleared: status=%s marker=%v/%v",
				status, markerID, markerFence)
		}
	})
}

func newTaskDefinitionEditEntrypointFixture(
	t *testing.T,
	withProvenance bool,
) *taskDefinitionEditEntrypointFixture {
	t.Helper()
	st := taskDefinitionEditEntrypointTestStore(t)
	components := loadTaskDefinitionEditEntrypointComponents(t)
	base, err := taskstate.DecodeApprovedDefinitionV1(components.BaseDefinition)
	if err != nil {
		t.Fatalf("decode entrypoint base definition: %v", err)
	}
	target, err := taskstate.DecodeApprovedDefinitionV1(components.TargetDefinition)
	if err != nil {
		t.Fatalf("decode entrypoint target definition: %v", err)
	}
	var prepared definitioneditwire.PreparedEditV1
	if err := json.Unmarshal(components.PreparedEdit, &prepared); err != nil {
		t.Fatalf("decode entrypoint prepared edit: %v", err)
	}
	if base.TenantID != target.TenantID || base.UserID != target.UserID ||
		base.TaskID != target.TaskID || prepared.Creation.TenantID != base.TenantID ||
		prepared.Creation.UserID != base.UserID || prepared.Creation.TaskID != base.TaskID {
		t.Fatal("retained entrypoint fixture scope is not closed")
	}
	assertTaskDefinitionEditEntrypointFixtureAvailable(t, st, base, target)

	f := &taskDefinitionEditEntrypointFixture{
		store: st, base: base, target: target,
		baseBytes:   bytes.Clone(components.BaseDefinition),
		targetBytes: bytes.Clone(components.TargetDefinition),
		prepared:    prepared, preparedBytes: bytes.Clone(components.PreparedEdit),
		baseSnapshotBytes: bytes.Clone(components.BaseSnapshot),
	}
	registerTaskDefinitionEditEntrypointCleanup(t, f)
	f.insertScopeAndCurrentProjection(t)
	baseRecord, err := st.InsertInitialApprovedDefinition(
		t.Context(), base, "entrypoint-base:"+base.TaskID)
	if err != nil {
		t.Fatalf("insert entrypoint approved base: %v", err)
	}
	if baseRecord.Version != prepared.BaseHead.Version ||
		baseRecord.Digest != prepared.BaseHead.Digest ||
		!bytes.Equal(baseRecord.Payload, components.BaseDefinition) {
		t.Fatalf("entrypoint approved base differs from retained wire: %+v", baseRecord)
	}
	f.baseRecord = baseRecord

	if err := st.pool.QueryRow(t.Context(), `
		INSERT INTO agent_sessions (tenant_id, user_id, status, messages)
		VALUES ($1,$2,'active','[]'::jsonb)
		RETURNING id`, base.TenantID, base.UserID).Scan(&f.sessionID); err != nil {
		t.Fatalf("insert entrypoint agent session: %v", err)
	}
	if withProvenance {
		f.insertSuccessfulCreationProvenance(t, components.PreparedEdit)
	}
	return f
}

func taskDefinitionEditEntrypointTestStore(t *testing.T) *Store {
	t.Helper()
	dbURL, _, provider := migration039Scratch(t)
	if _, err := provider.Up(t.Context()); err != nil {
		t.Fatalf("migrate retained entrypoint scratch DB: %v", err)
	}
	st, err := New(t.Context(), dbURL)
	if err != nil {
		t.Fatalf("open retained entrypoint scratch Store: %v", err)
	}
	t.Cleanup(st.Close)
	return st
}

func loadTaskDefinitionEditEntrypointComponents(
	t *testing.T,
) taskDefinitionEditEntrypointComponents {
	t.Helper()
	raw, err := os.ReadFile("../task/testdata/definition_edit_proposal_components_v1.json")
	if err != nil {
		t.Fatalf("read entrypoint proposal components: %v", err)
	}
	var components taskDefinitionEditEntrypointComponents
	if err := json.Unmarshal(raw, &components); err != nil {
		t.Fatalf("decode entrypoint proposal components: %v", err)
	}
	return components
}

func assertTaskDefinitionEditEntrypointFixtureAvailable(
	t *testing.T,
	st *Store,
	base taskstate.ApprovedDefinitionV1,
	target taskstate.ApprovedDefinitionV1,
) {
	t.Helper()
	sourceIDs := make([]int64, 0, len(base.Sources)+len(target.Sources))
	sourceURLs := make([]string, 0, len(base.Sources)+len(target.Sources))
	seen := make(map[int64]struct{})
	for _, definition := range []taskstate.ApprovedDefinitionV1{base, target} {
		for _, source := range definition.Sources {
			if _, ok := seen[source.SourceID]; ok {
				continue
			}
			seen[source.SourceID] = struct{}{}
			sourceIDs = append(sourceIDs, source.SourceID)
			sourceURLs = append(sourceURLs, source.URL)
		}
	}
	var collisions int
	if err := st.pool.QueryRow(t.Context(), `
		SELECT
		  (SELECT count(*) FROM tenants WHERE id=$1) +
		  (SELECT count(*) FROM users WHERE id=$2) +
		  (SELECT count(*) FROM schedules WHERE id=$3) +
		  (SELECT count(*) FROM task_definition_edit_operations WHERE id=$4) +
		  (SELECT count(*) FROM fetch_targets
		    WHERE id=ANY($5::bigint[]) OR url=ANY($6::text[]))`,
		base.TenantID, base.UserID, base.TaskID, "edit-proposal-fixture",
		sourceIDs, sourceURLs,
	).Scan(&collisions); err != nil {
		t.Fatalf("check entrypoint fixture availability: %v", err)
	}
	if collisions != 0 {
		t.Fatalf("retained entrypoint fixture scope has %d pre-existing rows", collisions)
	}
}

func registerTaskDefinitionEditEntrypointCleanup(
	t *testing.T,
	f *taskDefinitionEditEntrypointFixture,
) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(ctx, t, f.store,
			`DELETE FROM task_definition_edit_receipts WHERE tenant_id=$1`, f.base.TenantID)
		cleanupExec(ctx, t, f.store,
			`DELETE FROM schedules WHERE tenant_id=$1 AND user_id=$2 AND id=$3`,
			f.base.TenantID, f.base.UserID, f.base.TaskID)
		cleanupExec(ctx, t, f.store,
			`DELETE FROM task_definition_edit_operations WHERE tenant_id=$1`, f.base.TenantID)
		cleanupExec(ctx, t, f.store,
			`DELETE FROM task_creation_operations WHERE tenant_id=$1`, f.base.TenantID)
		cleanupExec(ctx, t, f.store,
			`DELETE FROM agent_sessions WHERE tenant_id=$1`, f.base.TenantID)
		cleanupExec(ctx, t, f.store,
			`DELETE FROM memberships WHERE tenant_id=$1`, f.base.TenantID)
		cleanupExec(ctx, t, f.store,
			`DELETE FROM tenants WHERE id=$1`, f.base.TenantID)
		cleanupExec(ctx, t, f.store,
			`DELETE FROM users WHERE id=$1`, f.base.UserID)
		for _, definition := range []taskstate.ApprovedDefinitionV1{f.base, f.target} {
			for _, source := range definition.Sources {
				cleanupExec(ctx, t, f.store,
					`DELETE FROM fetch_targets WHERE id=$1`, source.SourceID)
			}
		}
	})
}

func (f *taskDefinitionEditEntrypointFixture) insertScopeAndCurrentProjection(
	t *testing.T,
) {
	t.Helper()
	ctx := t.Context()
	tx, err := f.store.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin entrypoint fixture: %v", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := tx.Exec(ctx,
		`INSERT INTO tenants (id,status,plan) VALUES ($1,'active','free')`,
		f.base.TenantID); err != nil {
		t.Fatalf("insert entrypoint tenant: %v", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO users (id,feishu_open_id,name)
		VALUES ($1,$2,'definition edit entrypoint fixture')`,
		f.base.UserID, "ou_definition_edit_entrypoint_"+uuid.NewString()); err != nil {
		t.Fatalf("insert entrypoint user: %v", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO memberships (tenant_id,user_id,role) VALUES ($1,$2,'owner')`,
		f.base.TenantID, f.base.UserID); err != nil {
		t.Fatalf("insert entrypoint membership: %v", err)
	}

	sources := make(map[int64]taskstate.ApprovedSourceV1)
	for _, definition := range []taskstate.ApprovedDefinitionV1{f.base, f.target} {
		for _, source := range definition.Sources {
			if existing, ok := sources[source.SourceID]; ok &&
				(existing.URL != source.URL || existing.Platform != source.Platform ||
					existing.Capability != source.Capability) {
				t.Fatalf("fixture source %d has conflicting identities", source.SourceID)
			}
			sources[source.SourceID] = source
		}
	}
	for _, source := range sources {
		if _, err := tx.Exec(ctx, `
			INSERT INTO fetch_targets (id,url,title,config,platform,capability)
			VALUES ($1,$2,$3,$4,$5,$6)`, source.SourceID, source.URL,
			source.Title, source.Config, source.Platform, source.Capability); err != nil {
			t.Fatalf("insert entrypoint source %d: %v", source.SourceID, err)
		}
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO schedules (
			id,tenant_id,user_id,nl_description,spec_json,scope_json,status,
			push_strictness,execution_mode
		) VALUES ($1,$2,$3,$4,$5,$6,'active',$7,$8)`,
		f.base.TaskID, f.base.TenantID, f.base.UserID, f.base.NLDescription,
		f.base.SpecJSON, f.base.ScopeJSON, f.base.Strictness,
		f.base.ExecutionMode); err != nil {
		t.Fatalf("insert entrypoint schedule: %v", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO schedule_playbooks (schedule_id,content,fetch_plan)
		VALUES ($1,$2,$3)`, f.base.TaskID, f.base.PlaybookContent,
		mustCurrentFetchPlanFromApprovedDefinitionV1(t, f.base)); err != nil {
		t.Fatalf("insert entrypoint playbook: %v", err)
	}
	for _, source := range f.base.Sources {
		if _, err := tx.Exec(ctx, `
			INSERT INTO task_fetch_targets (schedule_id,fetch_target_id) VALUES ($1,$2)`,
			f.base.TaskID, source.SourceID); err != nil {
			t.Fatalf("insert entrypoint schedule source: %v", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit entrypoint fixture: %v", err)
	}
}

func mustCurrentFetchPlanFromApprovedDefinitionV1(
	t *testing.T,
	definition taskstate.ApprovedDefinitionV1,
) []byte {
	t.Helper()
	plan, err := currentFetchPlanFromApprovedDefinitionV1(definition)
	if err != nil {
		t.Fatalf("project current entrypoint fetch plan: %v", err)
	}
	return plan
}

func (f *taskDefinitionEditEntrypointFixture) insertSuccessfulCreationProvenance(
	t *testing.T,
	_ []byte,
) {
	t.Helper()
	creation, err := definitioneditwire.CanonicalCreation(f.prepared)
	if err != nil {
		t.Fatalf("canonicalize retained creation provenance: %v", err)
	}
	f.provenanceID = "entrypoint-create-provenance-" + uuid.NewString()
	if _, err := f.store.pool.Exec(t.Context(), `
		INSERT INTO task_creation_operations (
			id,tenant_id,user_id,session_id,tool_name,args,summary,status,
			expires_at,executed_at,execution_version,phase,fence,attempt,
			normalized_command,compiled_definition,compiled_digest,
			prepared_schedule,ensure_receipt,task_id,result,tombstoned_at
		) VALUES (
			$1,$2,$3,$4,'create_schedule','{}'::jsonb,$5,$6,
			clock_timestamp()+interval '1 hour',clock_timestamp(),$7,$8,1,1,
			$9,$10,$11,$12,$13,$14,$15,clock_timestamp()
		)`, f.provenanceID, f.base.TenantID, f.base.UserID, f.sessionID,
		"successful retained creation provenance", types.TaskOperationStatusExecuted,
		types.TaskCreationExecutionVersionV1, types.TaskCreationPhaseCompleted,
		[]byte(`{"command":"create schedule"}`), f.baseRecord.Payload,
		f.prepared.Creation.PreparedDigest, creation, []byte(`{"ensured":true}`),
		f.base.TaskID, json.RawMessage(`{"created":true}`)); err != nil {
		t.Fatalf("insert successful creation provenance: %v", err)
	}
}

func (f *taskDefinitionEditEntrypointFixture) buildProposal(
	t *testing.T,
	expiresAt time.Time,
	approvalRef string,
) definitioneditwire.FrozenProposal {
	t.Helper()
	proposal := definitioneditwire.ProposalV2{
		WireVersion: "vane.task-definition-edit-proposal/v2",
		OperationID: f.prepared.OperationID, OperationRef: approvalRef,
		Actor: definitioneditwire.ProposalActorV2{
			TenantID: f.base.TenantID, UserID: f.base.UserID,
		},
		Target: definitioneditwire.ProposalTargetV2{
			TenantID: f.base.TenantID, UserID: f.base.UserID, TaskID: f.base.TaskID,
		},
		SessionID: f.sessionID, ExpiresAtUnixMicros: expiresAt.UnixMicro(),
		OriginalStatus: definitioneditwire.OriginalStatusActive,
		BaseHead: definitioneditwire.HeadV1{
			Version: f.baseRecord.Version, Digest: f.baseRecord.Digest,
		},
		TargetHead:             f.prepared.TargetHead,
		TargetDefinitionDigest: sha256HexTaskDefinitionEdit(f.targetBytes),
		PreparedEditDigest:     sha256HexTaskDefinitionEdit(f.preparedBytes),
		BaseSnapshotDigest:     sha256HexTaskDefinitionEdit(f.baseSnapshotBytes),
	}
	canonical, err := json.Marshal(proposal)
	if err != nil {
		t.Fatalf("encode entrypoint proposal: %v", err)
	}
	frozen, err := definitioneditwire.DecodeFrozenProposal(
		canonical, f.baseBytes, f.targetBytes, f.preparedBytes,
		f.baseSnapshotBytes)
	if err != nil {
		t.Fatalf("DecodeFrozenProposal: %v", err)
	}
	return frozen
}

func taskDefinitionEditCreateParams(
	frozen definitioneditwire.FrozenProposal,
) types.CreateTaskDefinitionEditOperationParams {
	return types.CreateTaskDefinitionEditOperationParams{
		CanonicalProposal: frozen.CanonicalProposal,
		BaseDefinition:    frozen.BaseDefinitionBytes,
		TargetDefinition:  frozen.TargetDefinitionBytes,
		PreparedEdit:      frozen.PreparedEditBytes,
		BaseSnapshot:      frozen.BaseSnapshotBytes,
	}
}

func (f *taskDefinitionEditEntrypointFixture) createOperation(
	t *testing.T,
	expiresAt time.Time,
	approvalRef string,
) *types.TaskDefinitionEditOperation {
	t.Helper()
	frozen := f.buildProposal(t, expiresAt, approvalRef)
	op, err := f.store.CreateTaskDefinitionEditOperation(
		t.Context(), taskDefinitionEditCreateParams(frozen))
	if err != nil {
		t.Fatalf("create entrypoint operation: %v", err)
	}
	return op
}

func (f *taskDefinitionEditEntrypointFixture) acquireAndQuiesce(
	t *testing.T,
	approvalRef string,
) types.TaskDefinitionEditLease {
	t.Helper()
	op := f.createOperation(t, f.databaseNow(t).Add(time.Hour), approvalRef)
	acquired, err := f.store.AcquireTaskDefinitionEditOperation(t.Context(),
		types.AcquireTaskDefinitionEditOperationParams{
			Scope: op.Scope(), LeaseOwner: "entrypoint-authorize-worker",
			LeaseDuration:   time.Minute,
			ReceiptProvider: "feishu_card_patch:entrypoint",
			ReceiptTarget:   "om_entrypoint_authorize",
		})
	if err != nil {
		t.Fatalf("acquire entrypoint operation: %v", err)
	}
	if err := f.store.QuiesceTaskDefinitionEdit(t.Context(), acquired.Lease()); err != nil {
		t.Fatalf("quiesce entrypoint operation: %v", err)
	}
	return acquired.Lease()
}

func (f *taskDefinitionEditEntrypointFixture) databaseNow(t *testing.T) time.Time {
	t.Helper()
	var now time.Time
	if err := f.store.pool.QueryRow(t.Context(), `SELECT clock_timestamp()`).Scan(&now); err != nil {
		t.Fatalf("read PostgreSQL clock: %v", err)
	}
	return now
}

func (f *taskDefinitionEditEntrypointFixture) waitUntilOperationExpires(
	t *testing.T,
	op *types.TaskDefinitionEditOperation,
) {
	t.Helper()
	if _, err := f.store.pool.Exec(t.Context(), `
		SELECT pg_sleep(
			GREATEST(0, EXTRACT(EPOCH FROM ($1::timestamptz-clock_timestamp())))+0.02
		)`, op.ExpiresAt); err != nil {
		t.Fatalf("wait on PostgreSQL proposal clock: %v", err)
	}
}

func (f *taskDefinitionEditEntrypointFixture) advanceHeadPastTarget(t *testing.T) {
	t.Helper()
	targetPayload, err := taskstate.EncodeApprovedDefinitionV1(f.target)
	if err != nil {
		t.Fatal(err)
	}
	newer := f.target
	newer.Intent = "newer approved definition wins"
	newer.PlaybookContent = newer.Intent
	newerPayload, err := taskstate.EncodeApprovedDefinitionV1(newer)
	if err != nil {
		t.Fatal(err)
	}
	newerDigest := digestTaskStatePayload(newerPayload)
	tx, err := f.store.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	for _, row := range []struct {
		version    int64
		definition taskstate.ApprovedDefinitionV1
		payload    []byte
		digest     string
		approval   string
	}{
		{f.prepared.TargetHead.Version, f.target, targetPayload,
			f.prepared.TargetHead.Digest, "entrypoint-superseded-v2"},
		{f.prepared.TargetHead.Version + 1, newer, newerPayload,
			newerDigest, "entrypoint-superseded-v3"},
	} {
		if _, err := tx.Exec(t.Context(), `
			INSERT INTO task_approved_definition_versions (
				tenant_id,user_id,task_id,version,schema_version,execution_mode,
				definition_digest,payload,operation_ref
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
			f.base.TenantID, f.base.UserID, f.base.TaskID, row.version,
			row.definition.SchemaVersion, row.definition.ExecutionMode,
			row.digest, row.payload, row.approval); err != nil {
			t.Fatalf("insert superseding definition v%d: %v", row.version, err)
		}
	}
	if _, err := tx.Exec(t.Context(), `
		UPDATE schedules
		   SET approved_definition_version=$4, approved_definition_digest=$5
		 WHERE tenant_id=$1 AND user_id=$2 AND id=$3`,
		f.base.TenantID, f.base.UserID, f.base.TaskID,
		f.prepared.TargetHead.Version+1, newerDigest); err != nil {
		t.Fatalf("advance schedule past target: %v", err)
	}
	if err := tx.Commit(t.Context()); err != nil {
		t.Fatalf("commit superseding head: %v", err)
	}
}

func (f *taskDefinitionEditEntrypointFixture) assertOperationCount(
	t *testing.T,
	want int,
) {
	t.Helper()
	var got int
	if err := f.store.pool.QueryRow(t.Context(), `
		SELECT count(*) FROM task_definition_edit_operations
		 WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3`,
		f.base.TenantID, f.base.UserID, f.base.TaskID).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("definition edit operation count=%d, want %d", got, want)
	}
}

func (f *taskDefinitionEditEntrypointFixture) assertReceiptCount(
	t *testing.T,
	want int,
) {
	t.Helper()
	var got int
	if err := f.store.pool.QueryRow(t.Context(), `
		SELECT count(*) FROM task_definition_edit_receipts
		 WHERE tenant_id=$1 AND user_id=$2`,
		f.base.TenantID, f.base.UserID).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("definition edit receipt count=%d, want %d", got, want)
	}
}

func assertTaskDefinitionEditEntrypointTerminal(
	t *testing.T,
	f *taskDefinitionEditEntrypointFixture,
	op *types.TaskDefinitionEditOperation,
	wantStatus types.TaskDefinitionEditOperationStatus,
	provider string,
	target string,
) {
	t.Helper()
	if op.Status != wantStatus ||
		op.Phase != types.TaskDefinitionEditPhaseProposalSealed ||
		op.TombstonedAt == nil || op.ExecutionStartedAt != nil || op.LeaseOwner != "" ||
		op.LeaseUntil != nil || op.TakeoverNotBefore != nil ||
		op.Fence != 0 || op.Attempt != 0 {
		t.Fatalf("pending terminal operation=%+v", op)
	}
	assertTaskDefinitionEditEntrypointReceipt(t, f, op, provider, target)
}

func assertTaskDefinitionEditEntrypointReceipt(
	t *testing.T,
	f *taskDefinitionEditEntrypointFixture,
	op *types.TaskDefinitionEditOperation,
	provider string,
	target string,
) {
	t.Helper()
	receipt, err := f.store.LoadTaskDefinitionEditReceiptByOperation(
		t.Context(), op.ID, op.TenantID, op.UserID)
	if err != nil {
		t.Fatalf("load terminal edit receipt: %v", err)
	}
	if receipt.Status != types.TaskDefinitionEditReceiptStatusPending ||
		receipt.OperationStatus != op.Status || receipt.OperationPhase != op.Phase ||
		receipt.Provider != provider || receipt.Target != target ||
		receipt.SessionID != f.sessionID || receipt.TaskID != f.base.TaskID {
		t.Fatalf("terminal edit receipt differs: %+v", receipt)
	}
}
