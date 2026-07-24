package store

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/YouToco/vane/taskstate"
	"github.com/YouToco/vane/types"
)

type shadowCommitResponseLostTx struct {
	pgx.Tx
	cancel context.CancelFunc
}

func (tx *shadowCommitResponseLostTx) Commit(ctx context.Context) error {
	if err := tx.Tx.Commit(ctx); err != nil {
		return err
	}
	tx.cancel()
	return errors.New("injected shadow commit response loss")
}

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

func TestTaskRunSnapshotShadowV2SerializesAdaptiveCAS(t *testing.T) {
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
	paused := make(chan struct{})
	release := make(chan struct{})
	lockingStore := *f.st
	originalBegin := f.st.beginTx
	lockingStore.beginTx = func(ctx context.Context, opts pgx.TxOptions) (pgx.Tx, error) {
		tx, err := originalBegin(ctx, opts)
		if err != nil {
			return nil, err
		}
		return &taskCreationObservedTx{
			Tx: tx, pauseAfter: "FROM schedules s",
			paused: paused, release: release,
		}, nil
	}
	snapshotDone := make(chan error, 1)
	go func() {
		_, err := lockingStore.createOrGetTaskRunSnapshotWithShadowV2(
			t.Context(), f.params(taskID, "shadow-adaptive-race"), true)
		snapshotDone <- err
	}()
	select {
	case <-paused:
	case <-time.After(5 * time.Second):
		t.Fatal("snapshot did not reach held schedule share lock")
	}
	adaptiveDone := make(chan error, 1)
	go func() {
		_, err := f.st.CompareAndSwapAdaptiveState(t.Context(), 0,
			ApprovedDefinitionFence{Version: result.Version, Digest: result.Digest},
			state, nil)
		adaptiveDone <- err
	}()
	select {
	case err := <-adaptiveDone:
		t.Fatalf("adaptive CAS escaped snapshot schedule lock: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	if err := <-snapshotDone; err != nil {
		t.Fatalf("snapshot failed: %v", err)
	}
	if err := <-adaptiveDone; err != nil {
		t.Fatalf("adaptive CAS failed after snapshot: %v", err)
	}
	var status string
	if err := f.st.pool.QueryRow(t.Context(),
		`SELECT status FROM task_run_snapshot_v2_shadows
		  WHERE temporal_run_id='shadow-adaptive-race'`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != string(TaskRunSnapshotShadowMatch) {
		t.Fatalf("snapshot observed torn adaptive state: %q", status)
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

func TestTaskRunSnapshotShadowV2OldNewWriterRaceConverges(t *testing.T) {
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
	params := f.params(taskID, "shadow-old-new-race")
	start := make(chan struct{})
	type outcome struct {
		snapshot *taskRunSnapshot
		err      error
	}
	outcomes := make(chan outcome, 2)
	go func() {
		<-start
		snapshot, err := f.st.createOrGetTaskRunSnapshot(t.Context(), params)
		outcomes <- outcome{snapshot: snapshot, err: err}
	}()
	go func() {
		<-start
		snapshot, err := f.st.createOrGetTaskRunSnapshotWithShadowV2(
			t.Context(), params, true)
		outcomes <- outcome{snapshot: snapshot, err: err}
	}()
	close(start)
	left, right := <-outcomes, <-outcomes
	if left.err != nil || right.err != nil || left.snapshot == nil ||
		right.snapshot == nil || left.snapshot.ID != right.snapshot.ID ||
		left.snapshot.ReferenceDigest != right.snapshot.ReferenceDigest {
		t.Fatalf("old/new outcomes = %+v / %+v", left, right)
	}
	var parents, shadows int
	if err := f.st.pool.QueryRow(t.Context(),
		`SELECT
		    (SELECT count(*) FROM task_run_snapshots WHERE temporal_run_id=$1),
		    (SELECT count(*) FROM task_run_snapshot_v2_shadows WHERE temporal_run_id=$1)`,
		params.TemporalRunID).Scan(&parents, &shadows); err != nil {
		t.Fatal(err)
	}
	if parents != 1 || (shadows != 0 && shadows != 1) {
		t.Fatalf("old/new race rows = %d/%d", parents, shadows)
	}
}

func TestCreateTaskRunSnapshotShadowV2CommitLossUsesDetachedRecovery(t *testing.T) {
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
	callCtx, cancel := context.WithCancel(t.Context())
	defer cancel()
	faultStore := *f.st
	originalBegin := f.st.beginTx
	var begins atomic.Int32
	faultStore.beginTx = func(ctx context.Context, opts pgx.TxOptions) (pgx.Tx, error) {
		tx, err := originalBegin(ctx, opts)
		if err != nil {
			return nil, err
		}
		if begins.Add(1) == 1 {
			return &shadowCommitResponseLostTx{Tx: tx, cancel: cancel}, nil
		}
		return tx, nil
	}
	snapshot, err := faultStore.createOrGetTaskRunSnapshotWithShadowV2(
		callCtx, f.params(taskID, "shadow-commit-loss"), true)
	if err != nil || snapshot == nil || callCtx.Err() == nil {
		t.Fatalf("commit-loss recovery = snapshot %+v err %v ctx %v",
			snapshot, err, callCtx.Err())
	}
	var parents, shadows int
	if err := f.st.pool.QueryRow(t.Context(),
		`SELECT
		    (SELECT count(*) FROM task_run_snapshots WHERE temporal_run_id=$1),
		    (SELECT count(*) FROM task_run_snapshot_v2_shadows WHERE temporal_run_id=$1)`,
		"shadow-commit-loss").Scan(&parents, &shadows); err != nil {
		t.Fatal(err)
	}
	if parents != 1 || shadows != 1 {
		t.Fatalf("commit-loss rows parent/shadow = %d/%d", parents, shadows)
	}
}

func TestCreateTaskRunSnapshotShadowV2InsertFailureRollsBackBoth(t *testing.T) {
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
	faultStore := *f.st
	originalBegin := f.st.beginTx
	faultStore.beginTx = func(ctx context.Context, opts pgx.TxOptions) (pgx.Tx, error) {
		tx, err := originalBegin(ctx, opts)
		if err != nil {
			return nil, err
		}
		return &compiledTaskFaultTx{
			Tx: tx, failContains: "INSERT INTO task_run_snapshot_v2_shadows",
		}, nil
	}
	_, err = faultStore.createOrGetTaskRunSnapshotWithShadowV2(
		t.Context(), f.params(taskID, "shadow-insert-fault"), true)
	if !errors.Is(err, types.ErrDatabase) {
		t.Fatalf("shadow insert fault = %v", err)
	}
	var parents, shadows int
	if err := f.st.pool.QueryRow(t.Context(),
		`SELECT
		    (SELECT count(*) FROM task_run_snapshots WHERE temporal_run_id=$1),
		    (SELECT count(*) FROM task_run_snapshot_v2_shadows WHERE temporal_run_id=$1)`,
		"shadow-insert-fault").Scan(&parents, &shadows); err != nil {
		t.Fatal(err)
	}
	if parents != 0 || shadows != 0 {
		t.Fatalf("fault left parent/shadow = %d/%d", parents, shadows)
	}
}

func TestTaskRunSnapshotShadowV2CorruptReplayFailsClosed(t *testing.T) {
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
	params := f.params(taskID, "shadow-corrupt-replay")
	snapshot, err := f.st.createOrGetTaskRunSnapshotWithShadowV2(
		t.Context(), params, true)
	if err != nil {
		t.Fatal(err)
	}
	var raw []byte
	if err := f.st.pool.QueryRow(t.Context(),
		`SELECT payload FROM task_run_snapshot_v2_shadows WHERE run_snapshot_id=$1`,
		snapshot.ID).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var envelope map[string]any
	if err := json.Unmarshal(raw, &envelope); err != nil {
		t.Fatal(err)
	}
	legacy := envelope["legacy"].(map[string]any)
	legacyPayload := legacy["payload"].(map[string]any)
	legacyPayload["tenant_id"] = float64(f.tenantID + 999)
	corruptLegacy, _ := json.Marshal(legacyPayload)
	legacy["payload_digest"] = sha256Hex(corruptLegacy)
	corrupt, _ := json.Marshal(envelope)
	if _, err := f.st.pool.Exec(t.Context(),
		`UPDATE task_run_snapshot_v2_shadows
		    SET payload=$2, payload_digest=$3
		  WHERE run_snapshot_id=$1`,
		snapshot.ID, corrupt, sha256Hex(corrupt)); err != nil {
		t.Fatalf("install owner corruption fixture: %v", err)
	}
	if _, err := f.st.createOrGetTaskRunSnapshotWithShadowV2(
		t.Context(), params, true); !errors.Is(err, types.ErrInternal) {
		t.Fatalf("corrupt replay error = %v", err)
	}
	if _, err := f.st.AuditTaskRunSnapshotShadowsV2(
		t.Context(), taskID, time.Now().Add(-time.Minute), 0, 10,
	); !errors.Is(err, types.ErrInternal) {
		t.Fatalf("corrupt audit error = %v", err)
	}
}

func TestTaskRunSnapshotShadowV2RejectsReSealedFalseMatch(t *testing.T) {
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
		t.Context(), f.params(taskID, "shadow-false-match"), true)
	if err != nil {
		t.Fatal(err)
	}
	var raw []byte
	if err := f.st.pool.QueryRow(t.Context(),
		`SELECT payload FROM task_run_snapshot_v2_shadows WHERE run_snapshot_id=$1`,
		snapshot.ID).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	payload, _, err := readTaskRunSnapshotShadowPayloadV2(raw)
	if err != nil || payload.Approved == nil {
		t.Fatalf("read valid shadow: %v", err)
	}
	approved, err := taskstate.DecodeApprovedDefinitionV1(payload.Approved.Payload)
	if err != nil {
		t.Fatal(err)
	}
	approved.NLDescription += " attacker drift"
	approvedRaw, err := taskstate.EncodeApprovedDefinitionV1(approved)
	if err != nil {
		t.Fatal(err)
	}
	payload.Approved.Payload = approvedRaw
	payload.Approved.Digest = sha256Hex(approvedRaw)
	resealed, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := readTaskRunSnapshotShadowPayloadV2(resealed); err == nil {
		t.Fatal("retained reader accepted re-sealed false match")
	}
	if _, err := f.st.pool.Exec(t.Context(),
		`UPDATE task_run_snapshot_v2_shadows
		    SET approved_definition_digest=$2, payload=$3, payload_digest=$4
		  WHERE run_snapshot_id=$1`,
		snapshot.ID, payload.Approved.Digest, resealed, sha256Hex(resealed)); err != nil {
		t.Fatalf("install re-sealed owner mutation: %v", err)
	}
	if _, err := f.st.AuditTaskRunSnapshotShadowsV2(
		t.Context(), taskID, time.Now().Add(-time.Minute), 0, 10,
	); !errors.Is(err, types.ErrInternal) {
		t.Fatalf("audit accepted re-sealed false match: %v", err)
	}
}

func TestTaskRunSnapshotShadowV2HasOnlyAuditRuntimeConsumer(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate shadow test")
	}
	repoRoot := filepath.Dir(filepath.Dir(thisFile))
	allowedTableReferences := map[string]bool{
		filepath.Clean(filepath.Join(repoRoot, "store", "task_run_snapshot_shadow_v2.go")):    true,
		filepath.Clean(filepath.Join(repoRoot, "store", "task_run_snapshot_shadow_audit.go")): true,
		filepath.Clean(filepath.Join(repoRoot, "store", "task_run_snapshot_consumer_v2.go")):  true,
		filepath.Clean(filepath.Join(repoRoot, "store", "tenant_purge.go")):                   true,
	}
	allowedCalls := map[string]bool{
		"CreateOrGetCompiledRunSnapshotShadowV2|workflow/activities.go|PrepareRun":                                   true,
		"AuditCompiledTaskRunSnapshotV2|workflow/activities.go|auditCompiledSnapshotV2":                              true,
		"AuditTaskRunSnapshotShadowsV2Through|cmd/runtimeadmin/main.go|runSnapshotShadow":                            true,
		"AuditTaskRunSnapshotShadowsV2Through|store/task_run_snapshot_shadow_audit.go|AuditTaskRunSnapshotShadowsV2": true,
		"auditCompiledTaskRunSnapshotV2|store/task_run_snapshot_consumer_v2.go|AuditCompiledTaskRunSnapshotV2":       true,
		"auditCompiledTaskRunSnapshotV2|store/task_run_snapshot_shadow_audit.go|auditTaskRunSnapshotShadowsV2":       true,
	}
	guardedCalls := map[string]bool{
		"CreateOrGetCompiledRunSnapshotShadowV2": true,
		"AuditCompiledTaskRunSnapshotV2":         true,
		"AuditTaskRunSnapshotShadowsV2Through":   true,
		"auditCompiledTaskRunSnapshotV2":         true,
	}
	var escaped []string
	err := filepath.WalkDir(repoRoot, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if entry.Name() == ".git" || entry.Name() == "vendor" {
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if bytes.Contains(raw, []byte("task_run_snapshot_v2_shadows")) &&
			!allowedTableReferences[filepath.Clean(path)] {
			escaped = append(escaped, path)
		}

		relativePath, err := filepath.Rel(repoRoot, path)
		if err != nil {
			return err
		}
		relativePath = filepath.ToSlash(relativePath)
		fileSet := token.NewFileSet()
		parsed, err := parser.ParseFile(fileSet, path, raw, 0)
		if err != nil {
			return err
		}
		for _, declaration := range parsed.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || function.Body == nil {
				continue
			}
			ast.Inspect(function.Body, func(node ast.Node) bool {
				var callName string
				switch expression := node.(type) {
				case *ast.Ident:
					callName = expression.Name
				case *ast.SelectorExpr:
					callName = expression.Sel.Name
				}
				if !guardedCalls[callName] {
					return true
				}
				key := fmt.Sprintf("%s|%s|%s", callName, relativePath, function.Name.Name)
				if !allowedCalls[key] {
					position := fileSet.Position(node.Pos())
					escaped = append(escaped, fmt.Sprintf("%s:%d calls %s from %s",
						relativePath, position.Line, callName, function.Name.Name))
				}
				return true
			})
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(escaped) != 0 {
		t.Fatalf("v2 shadow table escaped persistence/audit boundary: %v", escaped)
	}

	reader, err := os.ReadFile(filepath.Join(
		repoRoot, "store", "task_run_snapshot_consumer_v2.go"))
	if err != nil {
		t.Fatal(err)
	}
	if violations := retainedV2CurrentStateViolations(reader); len(violations) != 0 {
		t.Errorf("retained v2 audit reader reached current state: %v", violations)
	}

	for _, table := range []string{
		"schedules", "schedule_playbooks", "schedule_sources", "subscriptions",
		"sources", "task_approved_definition_versions", "task_adaptive_states",
	} {
		t.Run("reject raw sql "+table, func(t *testing.T) {
			source := []byte("package store\nfunc injected() { _ = `SELECT * FROM " +
				table + "` }\n")
			if violations := retainedV2CurrentStateViolations(source); len(violations) == 0 {
				t.Fatalf("current table %q mutation escaped guard", table)
			}
		})
	}
	for _, source := range []string{
		"package store\nfunc injected() { _ = loadTaskRunDefinition }\n",
		"package store\nfunc injected() { alias := loadApprovedDefinitionVersionTx; _ = alias }\n",
		"package store\nfunc injected(s *Store) { _ = s.GetAdaptiveStateForDefinition }\n",
		"package store\nfunc injected(s *Store) { _ = s.GetSchedulePlaybook }\n",
		"package store\nfunc injected(s *Store) { alias := s.ListDueSourcesByUser; _ = alias }\n",
		"package store\nfunc injected() { _ = `SELECT * FROM audit_unapproved_table` }\n",
	} {
		if violations := retainedV2CurrentStateViolations([]byte(source)); len(violations) == 0 {
			t.Fatalf("current helper mutation escaped guard: %s", source)
		}
	}
}

func retainedV2CurrentStateViolations(source []byte) []string {
	file, err := parser.ParseFile(token.NewFileSet(), "reader.go", source, 0)
	if err != nil {
		return []string{"parse_error"}
	}
	forbiddenTables := []string{
		"schedules", "schedule_playbooks", "schedule_sources", "subscriptions",
		"sources", "task_approved_definition_versions", "task_adaptive_states",
	}
	forbiddenHelpers := map[string]bool{
		"loadTaskRunDefinition":                 true,
		"loadApprovedTaskRunSources":            true,
		"loadLegacyTaskRunSources":              true,
		"GetSchedule":                           true,
		"GetScheduleStrictness":                 true,
		"GetSchedulePlaybook":                   true,
		"ListDueSourcesBySchedule":              true,
		"ListDueSourcesByUser":                  true,
		"ListSubscribedSourcesByUser":           true,
		"ListDueSourcesByIDs":                   true,
		"GetCurrentApprovedDefinition":          true,
		"GetApprovedDefinitionVersion":          true,
		"GetAdaptiveStateForDefinition":         true,
		"loadApprovedDefinitionVersionTx":       true,
		"loadApprovedDefinitionByApprovalRefTx": true,
		"loadAdaptiveStateTx":                   true,
	}
	var violations []string
	sqlTable := regexp.MustCompile(`(?i)\b(?:from|join)\s+([a-z_][a-z0-9_]*)`)
	allowedTables := map[string]bool{
		"task_run_snapshots":           true,
		"task_run_snapshot_v2_shadows": true,
	}
	ast.Inspect(file, func(node ast.Node) bool {
		if literal, ok := node.(*ast.BasicLit); ok && literal.Kind == token.STRING {
			value, err := strconv.Unquote(literal.Value)
			if err == nil {
				value = strings.ToLower(value)
				for _, match := range sqlTable.FindAllStringSubmatch(value, -1) {
					if len(match) == 2 && !allowedTables[match[1]] {
						violations = append(violations, "sql_table:"+match[1])
					}
				}
				for _, table := range forbiddenTables {
					if strings.Contains(value, table) {
						violations = append(violations, "table:"+table)
					}
				}
			}
		}
		var name string
		switch expression := node.(type) {
		case *ast.Ident:
			name = expression.Name
		case *ast.SelectorExpr:
			name = expression.Sel.Name
		}
		if forbiddenHelpers[name] {
			violations = append(violations, "helper:"+name)
		}
		return true
	})
	return violations
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

func TestTaskRunSnapshotShadowV2SchemaPrivileges(t *testing.T) {
	st := tenantTestStore(t)
	var rls, canSelect, canInsertColumn, canUpdate, canDelete bool
	var sequenceUsage, sequenceSelect bool
	if err := st.pool.QueryRow(t.Context(),
		`SELECT
		    (SELECT relrowsecurity FROM pg_class
		      WHERE oid='task_run_snapshot_v2_shadows'::regclass),
		    has_table_privilege('vane_app','task_run_snapshot_v2_shadows','SELECT'),
		    has_column_privilege('vane_app','task_run_snapshot_v2_shadows',
		                         'payload','INSERT'),
		    has_table_privilege('vane_app','task_run_snapshot_v2_shadows','UPDATE'),
		    has_table_privilege('vane_app','task_run_snapshot_v2_shadows','DELETE'),
		    has_sequence_privilege('vane_app',
		                           'task_run_snapshot_v2_shadows_id_seq','USAGE'),
		    has_sequence_privilege('vane_app',
		                           'task_run_snapshot_v2_shadows_id_seq','SELECT')`,
	).Scan(&rls, &canSelect, &canInsertColumn, &canUpdate, &canDelete,
		&sequenceUsage, &sequenceSelect); err != nil {
		t.Fatal(err)
	}
	if !rls || !canSelect || !canInsertColumn || canUpdate || canDelete ||
		!sequenceUsage || sequenceSelect {
		t.Fatalf("shadow privileges rls/select/insert/update/delete/seq-use/seq-select = "+
			"%v/%v/%v/%v/%v/%v/%v", rls, canSelect, canInsertColumn,
			canUpdate, canDelete, sequenceUsage, sequenceSelect)
	}
}

func TestTaskRunSnapshotShadowV2DatabaseRejectsNullPassStatuses(t *testing.T) {
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
	parent, err := f.st.createOrGetTaskRunSnapshot(
		t.Context(), f.params(taskID, "shadow-null-pass-parent"))
	if err != nil {
		t.Fatal(err)
	}
	statuses := []struct {
		status   TaskRunSnapshotShadowStatus
		adaptive bool
	}{
		{status: TaskRunSnapshotShadowMatch},
		{status: TaskRunSnapshotShadowLegacyCompatible},
		{status: TaskRunSnapshotShadowProjectionMismatch},
		{status: TaskRunSnapshotShadowAdaptivePresent, adaptive: true},
		{status: TaskRunSnapshotShadowAdaptiveBasisMismatch, adaptive: true},
		{status: TaskRunSnapshotShadowAdaptiveForLegacy, adaptive: true},
	}
	for _, test := range statuses {
		t.Run(string(test.status), func(t *testing.T) {
			tx, err := f.st.pool.Begin(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback(t.Context())
			if _, err := tx.Exec(t.Context(), `SET LOCAL ROLE vane_app`); err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Exec(t.Context(),
				`SELECT set_config('app.tenant_id',$1,true)`,
				fmt.Sprint(f.tenantID)); err != nil {
				t.Fatal(err)
			}
			adaptiveVersion := int64(0)
			var adaptiveDigest *string
			adaptiveJSON := "null"
			if test.adaptive {
				adaptiveVersion = 1
				digest := sha256Hex([]byte("{}"))
				adaptiveDigest = &digest
				// Deliberately lacks basis fields and its retained payload lacks
				// schema/identity. PostgreSQL CHECK must evaluate false, not NULL.
				adaptiveJSON = fmt.Sprintf(
					`{"version":1,"digest":%q,"schema_version":"vane.task-adaptive-state/v1",`+
						`"payload":{},"last_known_good_definition_version":null}`,
					digest)
			}
			body := fmt.Sprintf(
				`{"schema_version":"vane.task-run-snapshot-shadow/v2",`+
					`"status":%q,`+
					`"identity":{"tenant_id":%d,"user_id":%d,"task_id":%q,`+
					`"temporal_workflow_id":%q,"temporal_run_id":%q},`+
					`"legacy":{"snapshot_id":%d},`+
					`"approved":{"version":%d,"digest":%q,`+
					`"schema_version":"vane.task-approved-definition/v1","payload":{}},`+
					`"adaptive":%s}`,
				test.status, f.tenantID, f.userID, taskID,
				parent.TemporalWorkflowID,
				parent.TemporalRunID,
				parent.ID, result.Version, result.Digest, adaptiveJSON)
			_, err = tx.Exec(t.Context(),
				`INSERT INTO task_run_snapshot_v2_shadows (
					run_snapshot_id,tenant_id,user_id,task_id,
					temporal_workflow_id,temporal_run_id,status,
					approved_definition_version,approved_definition_digest,
					adaptive_version,adaptive_digest,payload,payload_digest
				 ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,
				           convert_to($12,'UTF8'),
				           encode(sha256(convert_to($12,'UTF8')),'hex'))`,
				parent.ID, f.tenantID, f.userID, taskID,
				parent.TemporalWorkflowID,
				parent.TemporalRunID,
				test.status, result.Version, result.Digest,
				adaptiveVersion, adaptiveDigest, body)
			var pgErr *pgconn.PgError
			if err == nil || !errors.As(err, &pgErr) || pgErr.Code != "23514" {
				t.Fatalf("null-pass mutation error = %v", err)
			}
		})
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
