package store

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/YouToco/vane/taskstate"
)

func TestTaskRunSnapshotShadowV2CodecRejectsDrift(t *testing.T) {
	f := newTaskRunSnapshotFixture(t)
	taskID := f.taskID()
	f.createApprovedTask(t, taskID, 1)
	result, err := f.st.reconcileTaskDefinitionBaseline(t.Context(),
		TaskDefinitionBaselineApply, TaskDefinitionBaselineCursor{
			TenantID: f.tenantID, UserID: f.userID, TaskID: taskID,
		})
	if err != nil || result.Status != TaskDefinitionBaselineApplied {
		t.Fatalf("apply baseline = %+v, %v", result, err)
	}
	snapshot, err := f.st.createOrGetTaskRunSnapshotWithShadowV2(
		t.Context(), f.params(taskID, "shadow-codec-run"), true)
	if err != nil {
		t.Fatalf("create shadow snapshot: %v", err)
	}
	var raw []byte
	if err := f.st.pool.QueryRow(t.Context(),
		`SELECT payload FROM task_run_snapshot_v2_shadows WHERE run_snapshot_id=$1`,
		snapshot.ID).Scan(&raw); err != nil {
		t.Fatalf("load shadow payload: %v", err)
	}
	decoded, canonical, err := readTaskRunSnapshotShadowPayloadV2(raw)
	if err != nil || decoded.Status != TaskRunSnapshotShadowMatch ||
		!bytes.Equal(canonical, raw) {
		t.Fatalf("decode shadow = status %q canonical=%v err=%v",
			decoded.Status, bytes.Equal(canonical, raw), err)
	}
	var object map[string]any
	if err := json.Unmarshal(raw, &object); err != nil {
		t.Fatal(err)
	}
	object["unknown"] = true
	drifted, _ := json.Marshal(object)
	if _, _, err := readTaskRunSnapshotShadowPayloadV2(drifted); err == nil {
		t.Fatal("v2 shadow reader accepted unknown field")
	}
}

func TestTaskRunSnapshotShadowV2AdaptiveIsNeverMatch(t *testing.T) {
	f := newTaskRunSnapshotFixture(t)
	taskID := f.taskID()
	sourceIDs := f.createApprovedTask(t, taskID, 1)
	result, err := f.st.reconcileTaskDefinitionBaseline(t.Context(),
		TaskDefinitionBaselineApply, TaskDefinitionBaselineCursor{
			TenantID: f.tenantID, UserID: f.userID, TaskID: taskID,
		})
	if err != nil || result.Status != TaskDefinitionBaselineApplied {
		t.Fatalf("apply baseline = %+v, %v", result, err)
	}
	state, err := taskstate.BuildAdaptiveStateV1(taskstate.AdaptiveStateInputV1{
		TenantID: f.tenantID, UserID: f.userID, TaskID: taskID,
		QueryVariants: []taskstate.QueryVariantV1{},
		CapabilityOrder: []taskstate.ReadCapabilityV1{{
			Platform: "web", Capability: "search",
		}},
		SourceOrder: sourceIDs,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.st.CompareAndSwapAdaptiveState(t.Context(), 0,
		ApprovedDefinitionFence{Version: result.Version, Digest: result.Digest},
		state, nil); err != nil {
		t.Fatalf("create adaptive state: %v", err)
	}
	snapshot, err := f.st.createOrGetTaskRunSnapshotWithShadowV2(
		t.Context(), f.params(taskID, "shadow-adaptive-present"), true)
	if err != nil {
		t.Fatalf("create shadow snapshot: %v", err)
	}
	var status string
	if err := f.st.pool.QueryRow(t.Context(),
		`SELECT status FROM task_run_snapshot_v2_shadows WHERE run_snapshot_id=$1`,
		snapshot.ID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != string(TaskRunSnapshotShadowAdaptivePresent) {
		t.Fatalf("adaptive shadow status = %q", status)
	}
}

func TestCreateTaskRunSnapshotShadowV2AtomicAndNoBackfill(t *testing.T) {
	f := newTaskRunSnapshotFixture(t)
	taskID := f.taskID()
	f.createApprovedTask(t, taskID, 1)
	result, err := f.st.reconcileTaskDefinitionBaseline(t.Context(),
		TaskDefinitionBaselineApply, TaskDefinitionBaselineCursor{
			TenantID: f.tenantID, UserID: f.userID, TaskID: taskID,
		})
	if err != nil || result.Status != TaskDefinitionBaselineApplied {
		t.Fatalf("apply baseline = %+v, %v", result, err)
	}

	first, err := f.st.createOrGetTaskRunSnapshotWithShadowV2(
		t.Context(), f.params(taskID, "shadow-atomic-run"), true)
	if err != nil {
		t.Fatalf("create v1+shadow: %v", err)
	}
	var status string
	var payload, digest []byte
	if err := f.st.pool.QueryRow(t.Context(),
		`SELECT status, payload, decode(payload_digest, 'hex')
		   FROM task_run_snapshot_v2_shadows WHERE run_snapshot_id=$1`,
		first.ID).Scan(&status, &payload, &digest); err != nil {
		t.Fatalf("load shadow: %v", err)
	}
	if status != string(TaskRunSnapshotShadowMatch) ||
		!bytes.Equal(digest, mustDecodeDigest(t, sha256Hex(payload))) {
		t.Fatalf("shadow status/digest = %q/%x", status, digest)
	}
	auditSince := time.Now().Add(-time.Minute)

	legacyRun := "shadow-preexisting-v1"
	legacy, err := f.st.createOrGetTaskRunSnapshot(
		t.Context(), f.params(taskID, legacyRun))
	if err != nil {
		t.Fatalf("create pre-shadow v1: %v", err)
	}
	replayed, err := f.st.createOrGetTaskRunSnapshotWithShadowV2(
		t.Context(), f.params(taskID, legacyRun), true)
	if err != nil || replayed.ID != legacy.ID {
		t.Fatalf("replay pre-shadow v1 = id %d err %v", replayed.ID, err)
	}
	var count int
	if err := f.st.pool.QueryRow(t.Context(),
		`SELECT count(*) FROM task_run_snapshot_v2_shadows WHERE run_snapshot_id=$1`,
		legacy.ID).Scan(&count); err != nil || count != 0 {
		t.Fatalf("pre-shadow replay sidecars = %d, %v", count, err)
	}
	page, err := f.st.AuditTaskRunSnapshotShadowsV2(
		t.Context(), taskID, auditSince, 0, 1)
	if err != nil || len(page.Items) != 1 || page.Next == nil ||
		page.Items[0].Status != TaskRunSnapshotShadowMatch ||
		page.Items[0].V1PayloadDigest == "" ||
		page.Items[0].ShadowPayloadDigest == "" {
		t.Fatalf("first audit page = %+v, %v", page, err)
	}
	page, err = f.st.AuditTaskRunSnapshotShadowsV2(
		t.Context(), taskID, auditSince, *page.Next, 1)
	if err != nil || len(page.Items) != 1 || page.Next != nil ||
		page.Items[0].Status != "missing" ||
		page.Items[0].ShadowPayloadDigest != "" {
		t.Fatalf("second audit page = %+v, %v", page, err)
	}
}

func TestTaskRunSnapshotShadowV2CompositeParentFence(t *testing.T) {
	f := newTaskRunSnapshotFixture(t)
	taskID := f.taskID()
	f.createApprovedTask(t, taskID, 1)
	result, err := f.st.reconcileTaskDefinitionBaseline(t.Context(),
		TaskDefinitionBaselineApply, TaskDefinitionBaselineCursor{
			TenantID: f.tenantID, UserID: f.userID, TaskID: taskID,
		})
	if err != nil || result.Status != TaskDefinitionBaselineApplied {
		t.Fatalf("apply baseline = %+v, %v", result, err)
	}
	snapshot, err := f.st.createOrGetTaskRunSnapshot(
		t.Context(), f.params(taskID, "shadow-parent-fence"))
	if err != nil {
		t.Fatalf("create parent v1: %v", err)
	}

	other := newTaskRunSnapshotFixture(t)
	otherTask := other.taskID()
	other.createApprovedTask(t, otherTask, 1)
	otherResult, err := other.st.reconcileTaskDefinitionBaseline(t.Context(),
		TaskDefinitionBaselineApply, TaskDefinitionBaselineCursor{
			TenantID: other.tenantID, UserID: other.userID, TaskID: otherTask,
		})
	if err != nil || otherResult.Status != TaskDefinitionBaselineApplied {
		t.Fatalf("apply other baseline = %+v, %v", otherResult, err)
	}
	otherSnapshot, err := other.st.createOrGetTaskRunSnapshotWithShadowV2(
		t.Context(), other.params(otherTask, "shadow-other-parent"), true)
	if err != nil {
		t.Fatalf("create other shadow: %v", err)
	}
	var payload []byte
	var payloadDigest, approvedDigest *string
	var approvedVersion *int64
	if err := other.st.pool.QueryRow(t.Context(),
		`SELECT payload, payload_digest, approved_definition_version,
		        approved_definition_digest
		   FROM task_run_snapshot_v2_shadows WHERE run_snapshot_id=$1`,
		otherSnapshot.ID).Scan(
		&payload, &payloadDigest, &approvedVersion, &approvedDigest); err != nil {
		t.Fatalf("load other shadow: %v", err)
	}
	if _, err := other.st.pool.Exec(t.Context(),
		`DELETE FROM task_run_snapshot_v2_shadows WHERE run_snapshot_id=$1`,
		otherSnapshot.ID); err != nil {
		t.Fatalf("remove owner-created comparison row: %v", err)
	}
	decodedPayload, _, err := readTaskRunSnapshotShadowPayloadV2(payload)
	if err != nil {
		t.Fatal(err)
	}
	decodedPayload.Legacy.SnapshotID = snapshot.ID
	payload, payloadDigestValue, err := encodeTaskRunSnapshotShadowPayloadV2(decodedPayload)
	if err != nil {
		t.Fatal(err)
	}
	payloadDigest = &payloadDigestValue
	_, err = other.st.pool.Exec(t.Context(),
		`INSERT INTO task_run_snapshot_v2_shadows (
			run_snapshot_id, tenant_id, user_id, task_id,
			temporal_workflow_id, temporal_run_id, status,
			approved_definition_version, approved_definition_digest,
			adaptive_version, adaptive_digest, payload, payload_digest
		 ) VALUES ($1,$2,$3,$4,$5,$6,'match',$7,$8,0,NULL,$9,$10)`,
		snapshot.ID, other.tenantID, other.userID, otherTask,
		otherSnapshot.TemporalWorkflowID, otherSnapshot.TemporalRunID,
		approvedVersion, approvedDigest, payload, payloadDigest)
	var pgErr *pgconn.PgError
	if err == nil || !errors.As(err, &pgErr) || pgErr.Code != "23503" {
		t.Fatalf("cross-scope parent insert error = %v", err)
	}
}

func mustDecodeDigest(t *testing.T, value string) []byte {
	t.Helper()
	decoded, err := hex.DecodeString(value)
	if err != nil {
		t.Fatal(err)
	}
	return decoded
}
