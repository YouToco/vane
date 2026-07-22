package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/sourcespec"
	"github.com/YouToco/vane/taskstate"
	"github.com/YouToco/vane/types"
)

func TestCommitApprovedDefinitionEdit_ExactProjectionAndReplay(t *testing.T) {
	f, base := newApprovedDefinitionEditFixture(t)
	candidate, sourceIDs := buildApprovedDefinitionEditCandidate(t, f, "完整 C2b-2 新意图", true)
	params := ApprovedDefinitionEditParams{
		ExpectedHead: ApprovedDefinitionFence{Version: base.Version, Digest: base.Digest},
		Definition:   candidate,
		ApprovalRef:  "c2b2-edit-" + uuid.NewString(),
	}

	created, err := f.store.CommitApprovedDefinitionEdit(t.Context(), params)
	if err != nil {
		t.Fatalf("CommitApprovedDefinitionEdit: %v", err)
	}
	if created.Version != 2 || created.ApprovalRef != params.ApprovalRef {
		t.Fatalf("created record=%+v", created)
	}
	wantPayload, err := taskstate.EncodeApprovedDefinitionV1(created.Definition)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(created.Payload, wantPayload) ||
		!constantTimeTaskStateDigestEqual(created.Digest, digestTaskStatePayload(wantPayload)) {
		t.Fatalf("created payload/digest is not canonical: %+v", created)
	}
	assertApprovedDefinitionEditProjection(t, f, created, sourceIDs)

	retained, err := f.store.GetApprovedDefinitionVersion(
		t.Context(), f.tenantID, f.userID, f.taskID, base.Version)
	if err != nil || !bytes.Equal(retained.Payload, base.Payload) || retained.Digest != base.Digest {
		t.Fatalf("immutable v1 changed: record=%+v err=%v", retained, err)
	}
	replayed, err := f.store.CommitApprovedDefinitionEdit(t.Context(), params)
	if err != nil || replayed.Version != created.Version || replayed.Digest != created.Digest ||
		!replayed.CreatedAt.Equal(created.CreatedAt) {
		t.Fatalf("exact replay=%+v err=%v, want %+v", replayed, err, created)
	}
	assertApprovedDefinitionVersionCount(t, f, 2)
}

func TestCommitApprovedDefinitionEdit_RejectsStaleDriftAndIdentityReuse(t *testing.T) {
	t.Run("stale digest leaves both projections unchanged", func(t *testing.T) {
		f, base := newApprovedDefinitionEditFixture(t)
		candidate, _ := buildApprovedDefinitionEditCandidate(t, f, "不会落库的 stale 编辑", false)
		wrong := ApprovedDefinitionEditParams{
			ExpectedHead: ApprovedDefinitionFence{
				Version: base.Version,
				Digest:  "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
			},
			Definition: candidate, ApprovalRef: "c2b2-stale-" + uuid.NewString(),
		}
		if _, err := f.store.CommitApprovedDefinitionEdit(
			t.Context(), wrong); !errors.Is(err, types.ErrConflict) {
			t.Fatalf("stale digest error=%v, want Conflict", err)
		}
		wrong.ExpectedHead = ApprovedDefinitionFence{
			Version: base.Version + 1, Digest: base.Digest,
		}
		wrong.ApprovalRef = "c2b2-stale-version-" + uuid.NewString()
		if _, err := f.store.CommitApprovedDefinitionEdit(
			t.Context(), wrong); !errors.Is(err, types.ErrConflict) {
			t.Fatalf("stale version error=%v, want Conflict", err)
		}
		assertApprovedDefinitionEditProjection(t, f, base, []int64{f.sourceID})
		assertApprovedDefinitionVersionCount(t, f, 1)
	})

	t.Run("legacy drift is not silently repaired", func(t *testing.T) {
		f, base := newApprovedDefinitionEditFixture(t)
		if _, err := f.store.pool.Exec(t.Context(),
			`UPDATE schedules SET nl_description='drifted outside control plane'
			  WHERE tenant_id=$1 AND user_id=$2 AND id=$3`,
			f.tenantID, f.userID, f.taskID); err != nil {
			t.Fatal(err)
		}
		candidate, _ := buildApprovedDefinitionEditCandidate(t, f, "不可修复漂移", false)
		_, err := f.store.CommitApprovedDefinitionEdit(t.Context(), ApprovedDefinitionEditParams{
			ExpectedHead: ApprovedDefinitionFence{Version: base.Version, Digest: base.Digest},
			Definition:   candidate, ApprovalRef: "c2b2-drift-" + uuid.NewString(),
		})
		if !errors.Is(err, types.ErrConflict) {
			t.Fatalf("projection drift error=%v, want Conflict", err)
		}
		var retained string
		if err := f.store.pool.QueryRow(t.Context(),
			`SELECT nl_description FROM schedules WHERE id=$1`, f.taskID,
		).Scan(&retained); err != nil || retained != "drifted outside control plane" {
			t.Fatalf("schedule drift was repaired: retained=%q err=%v", retained, err)
		}
		assertApprovedDefinitionVersionCount(t, f, 1)
	})

	t.Run("playbook drift is not silently repaired", func(t *testing.T) {
		f, base := newApprovedDefinitionEditFixture(t)
		const drift = "drifted playbook outside control plane"
		if _, err := f.store.pool.Exec(t.Context(),
			`UPDATE schedule_playbooks SET content=$2 WHERE schedule_id=$1`,
			f.taskID, drift); err != nil {
			t.Fatal(err)
		}
		candidate, _ := buildApprovedDefinitionEditCandidate(t, f, "不可覆盖 playbook 漂移", false)
		_, err := f.store.CommitApprovedDefinitionEdit(t.Context(), ApprovedDefinitionEditParams{
			ExpectedHead: ApprovedDefinitionFence{Version: base.Version, Digest: base.Digest},
			Definition:   candidate, ApprovalRef: "c2b2-playbook-drift-" + uuid.NewString(),
		})
		if !errors.Is(err, types.ErrConflict) {
			t.Fatalf("playbook drift error=%v, want Conflict", err)
		}
		var retained string
		if err := f.store.pool.QueryRow(t.Context(),
			`SELECT content FROM schedule_playbooks WHERE schedule_id=$1`, f.taskID,
		).Scan(&retained); err != nil || retained != drift {
			t.Fatalf("playbook drift was repaired: retained=%q err=%v", retained, err)
		}
		assertApprovedDefinitionVersionCount(t, f, 1)
	})

	t.Run("source-set drift is not silently repaired", func(t *testing.T) {
		f, base := newApprovedDefinitionEditFixture(t)
		if _, err := f.store.pool.Exec(t.Context(),
			`DELETE FROM schedule_sources WHERE schedule_id=$1`, f.taskID); err != nil {
			t.Fatal(err)
		}
		candidate, _ := buildApprovedDefinitionEditCandidate(t, f, "不可覆盖 source 漂移", false)
		_, err := f.store.CommitApprovedDefinitionEdit(t.Context(), ApprovedDefinitionEditParams{
			ExpectedHead: ApprovedDefinitionFence{Version: base.Version, Digest: base.Digest},
			Definition:   candidate, ApprovalRef: "c2b2-source-drift-" + uuid.NewString(),
		})
		if !errors.Is(err, types.ErrConflict) {
			t.Fatalf("source drift error=%v, want Conflict", err)
		}
		var retained int
		if err := f.store.pool.QueryRow(t.Context(),
			`SELECT count(*) FROM schedule_sources WHERE schedule_id=$1`, f.taskID,
		).Scan(&retained); err != nil || retained != 0 {
			t.Fatalf("source drift was repaired: retained=%d err=%v", retained, err)
		}
		assertApprovedDefinitionVersionCount(t, f, 1)
	})

	t.Run("same confirmation cannot name another result", func(t *testing.T) {
		f, base := newApprovedDefinitionEditFixture(t)
		candidate, _ := buildApprovedDefinitionEditCandidate(t, f, "原始确认编辑", false)
		params := ApprovedDefinitionEditParams{
			ExpectedHead: ApprovedDefinitionFence{Version: base.Version, Digest: base.Digest},
			Definition:   candidate, ApprovalRef: "c2b2-reuse-" + uuid.NewString(),
		}
		created, err := f.store.CommitApprovedDefinitionEdit(t.Context(), params)
		if err != nil {
			t.Fatal(err)
		}
		changed := candidate
		changed.Intent = "同 action 的另一个结果"
		changed.PlaybookContent = changed.Intent
		params.Definition = changed
		if _, err := f.store.CommitApprovedDefinitionEdit(
			t.Context(), params); !errors.Is(err, types.ErrConflict) {
			t.Fatalf("approval identity reuse error=%v, want Conflict", err)
		}
		assertApprovedDefinitionEditProjection(t, f, created, []int64{f.sourceID})
		assertApprovedDefinitionVersionCount(t, f, 2)
	})

	t.Run("same confirmation cannot move to another base version", func(t *testing.T) {
		f, base := newApprovedDefinitionEditFixture(t)
		candidate, _ := buildApprovedDefinitionEditCandidate(t, f, "已落库的 v2", false)
		params := ApprovedDefinitionEditParams{
			ExpectedHead: ApprovedDefinitionFence{Version: base.Version, Digest: base.Digest},
			Definition:   candidate, ApprovalRef: "c2b2-base-reuse-" + uuid.NewString(),
		}
		created, err := f.store.CommitApprovedDefinitionEdit(t.Context(), params)
		if err != nil {
			t.Fatal(err)
		}
		candidate.Intent = "同 confirmation 企图从 v2 再推进"
		candidate.PlaybookContent = candidate.Intent
		params.ExpectedHead = ApprovedDefinitionFence{
			Version: created.Version, Digest: created.Digest,
		}
		params.Definition = candidate
		if _, err := f.store.CommitApprovedDefinitionEdit(
			t.Context(), params); !errors.Is(err, types.ErrConflict) {
			t.Fatalf("confirmation base reuse error=%v, want Conflict", err)
		}
		assertApprovedDefinitionEditProjection(t, f, created, []int64{f.sourceID})
		assertApprovedDefinitionVersionCount(t, f, 2)
	})

	t.Run("same confirmation cannot name another base digest", func(t *testing.T) {
		f, base := newApprovedDefinitionEditFixture(t)
		candidate, _ := buildApprovedDefinitionEditCandidate(t, f, "固定 confirmation 的 v2", false)
		params := ApprovedDefinitionEditParams{
			ExpectedHead: ApprovedDefinitionFence{Version: base.Version, Digest: base.Digest},
			Definition:   candidate, ApprovalRef: "c2b2-base-digest-" + uuid.NewString(),
		}
		created, err := f.store.CommitApprovedDefinitionEdit(t.Context(), params)
		if err != nil {
			t.Fatal(err)
		}
		// Keep the original version and exact candidate so replay reaches the
		// immutable base-digest check rather than failing an earlier identity check.
		wrongFirst := byte('0')
		if base.Digest[0] == wrongFirst {
			wrongFirst = '1'
		}
		params.ExpectedHead.Digest = string(wrongFirst) + base.Digest[1:]
		if _, err := f.store.CommitApprovedDefinitionEdit(
			t.Context(), params); !errors.Is(err, types.ErrConflict) {
			t.Fatalf("confirmation base digest error=%v, want Conflict", err)
		}
		assertApprovedDefinitionEditProjection(t, f, created, []int64{f.sourceID})
		assertApprovedDefinitionVersionCount(t, f, 2)
	})

	t.Run("invalid source id rolls back appended version", func(t *testing.T) {
		f, base := newApprovedDefinitionEditFixture(t)
		candidate, _ := buildApprovedDefinitionEditCandidate(t, f, "错误 source identity", false)
		candidate.Sources[0].SourceID += 999999
		_, err := f.store.CommitApprovedDefinitionEdit(t.Context(), ApprovedDefinitionEditParams{
			ExpectedHead: ApprovedDefinitionFence{Version: base.Version, Digest: base.Digest},
			Definition:   candidate, ApprovalRef: "c2b2-source-" + uuid.NewString(),
		})
		if !errors.Is(err, types.ErrConflict) {
			t.Fatalf("source identity error=%v, want Conflict", err)
		}
		assertApprovedDefinitionEditProjection(t, f, base, []int64{f.sourceID})
		assertApprovedDefinitionVersionCount(t, f, 1)
	})

	t.Run("source id cannot be rebound to another url", func(t *testing.T) {
		f, base := newApprovedDefinitionEditFixture(t)
		candidate, _ := buildApprovedDefinitionEditCandidate(t, f, "交换 source identity", true)
		if len(candidate.Sources) != 2 {
			t.Fatalf("identity fixture sources=%d, want 2", len(candidate.Sources))
		}
		candidate.Sources[0].SourceID, candidate.Sources[1].SourceID =
			candidate.Sources[1].SourceID, candidate.Sources[0].SourceID
		_, err := f.store.CommitApprovedDefinitionEdit(t.Context(), ApprovedDefinitionEditParams{
			ExpectedHead: ApprovedDefinitionFence{Version: base.Version, Digest: base.Digest},
			Definition:   candidate, ApprovalRef: "c2b2-source-swap-" + uuid.NewString(),
		})
		if !errors.Is(err, types.ErrConflict) {
			t.Fatalf("source identity swap error=%v, want Conflict", err)
		}
		assertApprovedDefinitionEditProjection(t, f, base, []int64{f.sourceID})
		assertApprovedDefinitionVersionCount(t, f, 1)
	})
}

func TestCommitApprovedDefinitionEdit_ConcurrentCASAndLateReplay(t *testing.T) {
	t.Run("same confirmation converges to one immutable result", func(t *testing.T) {
		f, base := newApprovedDefinitionEditFixture(t)
		candidate, _ := buildApprovedDefinitionEditCandidate(t, f, "并发相同确认", false)
		params := ApprovedDefinitionEditParams{
			ExpectedHead: ApprovedDefinitionFence{Version: base.Version, Digest: base.Digest},
			Definition:   candidate, ApprovalRef: "c2b2-same-race-" + uuid.NewString(),
		}
		start := make(chan struct{})
		results := make(chan struct {
			record ApprovedDefinitionVersionRecord
			err    error
		}, 2)
		var wg sync.WaitGroup
		for range 2 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				record, err := f.store.CommitApprovedDefinitionEdit(t.Context(), params)
				results <- struct {
					record ApprovedDefinitionVersionRecord
					err    error
				}{record: record, err: err}
			}()
		}
		close(start)
		wg.Wait()
		close(results)
		var records []ApprovedDefinitionVersionRecord
		for result := range results {
			if result.err != nil {
				t.Fatalf("same-confirmation concurrent result: %v", result.err)
			}
			records = append(records, result.record)
		}
		if len(records) != 2 || records[0].Version != records[1].Version ||
			records[0].Digest != records[1].Digest ||
			!records[0].CreatedAt.Equal(records[1].CreatedAt) {
			t.Fatalf("same-confirmation results diverged: %+v", records)
		}
		assertApprovedDefinitionVersionCount(t, f, 2)
	})

	t.Run("different confirmations have one winner", func(t *testing.T) {
		f, base := newApprovedDefinitionEditFixture(t)
		left, _ := buildApprovedDefinitionEditCandidate(t, f, "并发左候选", false)
		right, _ := buildApprovedDefinitionEditCandidate(t, f, "并发右候选", false)
		start := make(chan struct{})
		results := make(chan struct {
			record ApprovedDefinitionVersionRecord
			err    error
		}, 2)
		var wg sync.WaitGroup
		for index, candidate := range []taskstate.ApprovedDefinitionV1{left, right} {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				record, err := f.store.CommitApprovedDefinitionEdit(t.Context(),
					ApprovedDefinitionEditParams{
						ExpectedHead: ApprovedDefinitionFence{
							Version: base.Version, Digest: base.Digest,
						},
						Definition:  candidate,
						ApprovalRef: fmt.Sprintf("c2b2-race-%d-%s", index, uuid.NewString()),
					})
				results <- struct {
					record ApprovedDefinitionVersionRecord
					err    error
				}{record: record, err: err}
			}()
		}
		close(start)
		wg.Wait()
		close(results)
		var winner ApprovedDefinitionVersionRecord
		var succeeded, conflicted int
		for result := range results {
			switch {
			case result.err == nil:
				succeeded++
				winner = result.record
			case errors.Is(result.err, types.ErrConflict):
				conflicted++
			default:
				t.Fatalf("unexpected concurrent result: %v", result.err)
			}
		}
		if succeeded != 1 || conflicted != 1 {
			t.Fatalf("success/conflict=%d/%d, want 1/1", succeeded, conflicted)
		}
		assertApprovedDefinitionEditProjection(t, f, winner, []int64{f.sourceID})
		assertApprovedDefinitionVersionCount(t, f, 2)
	})

	t.Run("late replay returns v2 without rolling back v3", func(t *testing.T) {
		f, base := newApprovedDefinitionEditFixture(t)
		v2Candidate, _ := buildApprovedDefinitionEditCandidate(t, f, "已确认 v2", false)
		v2Params := ApprovedDefinitionEditParams{
			ExpectedHead: ApprovedDefinitionFence{Version: base.Version, Digest: base.Digest},
			Definition:   v2Candidate, ApprovalRef: "c2b2-v2-" + uuid.NewString(),
		}
		v2, err := f.store.CommitApprovedDefinitionEdit(t.Context(), v2Params)
		if err != nil {
			t.Fatal(err)
		}
		v3Candidate := v2.Definition
		v3Candidate.Intent = "随后确认 v3"
		v3Candidate.PlaybookContent = v3Candidate.Intent
		v3Candidate.NLDescription = "v3 当前投影"
		v3, err := f.store.CommitApprovedDefinitionEdit(t.Context(),
			ApprovedDefinitionEditParams{
				ExpectedHead: ApprovedDefinitionFence{Version: v2.Version, Digest: v2.Digest},
				Definition:   v3Candidate, ApprovalRef: "c2b2-v3-" + uuid.NewString(),
			})
		if err != nil {
			t.Fatal(err)
		}
		late, err := f.store.CommitApprovedDefinitionEdit(t.Context(), v2Params)
		if err != nil || late.Version != v2.Version || late.Digest != v2.Digest ||
			!late.CreatedAt.Equal(v2.CreatedAt) {
			t.Fatalf("late replay=%+v err=%v, want v2=%+v", late, err, v2)
		}
		assertApprovedDefinitionEditProjection(t, f, v3, []int64{f.sourceID})
		assertApprovedDefinitionVersionCount(t, f, 3)
	})
}

type blockAfterDefinitionScheduleLockTx struct {
	pgx.Tx
	ready   chan struct{}
	release chan struct{}
	once    sync.Once
}

func (tx *blockAfterDefinitionScheduleLockTx) QueryRow(
	ctx context.Context,
	sql string,
	args ...any,
) pgx.Row {
	row := tx.Tx.QueryRow(ctx, sql, args...)
	if !strings.Contains(sql, "FOR UPDATE OF s FOR SHARE OF t, m") {
		return row
	}
	return blockAfterDefinitionScheduleLockRow{
		Row: row,
		afterScan: func() error {
			tx.once.Do(func() { close(tx.ready) })
			select {
			case <-tx.release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	}
}

type blockAfterDefinitionScheduleLockRow struct {
	pgx.Row
	afterScan func() error
}

func (row blockAfterDefinitionScheduleLockRow) Scan(dest ...any) error {
	if err := row.Row.Scan(dest...); err != nil {
		return err
	}
	return row.afterScan()
}

func TestCommitApprovedDefinitionEdit_HoldsScheduleLockAcrossTransaction(t *testing.T) {
	f, base := newApprovedDefinitionEditFixture(t)
	candidate, sourceIDs := buildApprovedDefinitionEditCandidate(t, f, "持锁编辑", true)
	params := ApprovedDefinitionEditParams{
		ExpectedHead: ApprovedDefinitionFence{Version: base.Version, Digest: base.Digest},
		Definition:   candidate, ApprovalRef: "c2b2-lock-" + uuid.NewString(),
	}
	ready := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseEdit := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseEdit)

	lockingStore := *f.store
	lockingStore.beginTx = func(ctx context.Context, opts pgx.TxOptions) (pgx.Tx, error) {
		realTx, err := f.store.pool.BeginTx(ctx, opts)
		if err != nil {
			return nil, err
		}
		return &blockAfterDefinitionScheduleLockTx{
			Tx: realTx, ready: ready, release: release,
		}, nil
	}
	callCtx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	type result struct {
		record ApprovedDefinitionVersionRecord
		err    error
	}
	resultCh := make(chan result, 1)
	go func() {
		record, err := lockingStore.CommitApprovedDefinitionEdit(callCtx, params)
		resultCh <- result{record: record, err: err}
	}()

	select {
	case <-ready:
		// The edit has scanned the exact scope and now holds the schedule's
		// FOR UPDATE lock while its transaction remains deliberately open.
	case <-callCtx.Done():
		t.Fatalf("wait for definition schedule lock: %v", callCtx.Err())
	}
	assertRowLockTimeout(t, f.store,
		`UPDATE schedules SET updated_at=clock_timestamp()
		  WHERE tenant_id=$1 AND user_id=$2 AND id=$3`,
		f.tenantID, f.userID, f.taskID)
	releaseEdit()
	got := <-resultCh
	if got.err != nil {
		t.Fatalf("edit after releasing schedule lock: %v", got.err)
	}
	assertApprovedDefinitionEditProjection(t, f, got.record, sourceIDs)
	assertApprovedDefinitionVersionCount(t, f, 2)
}

func TestCommitApprovedDefinitionEdit_FailsClosedAtUnsupportedBoundaries(t *testing.T) {
	t.Run("invalid fence confirmation and overflow fail before persistence", func(t *testing.T) {
		f, base := newApprovedDefinitionEditFixture(t)
		candidate, _ := buildApprovedDefinitionEditCandidate(t, f, "参数校验", false)
		cases := []ApprovedDefinitionEditParams{
			{
				ExpectedHead: ApprovedDefinitionFence{Version: 0, Digest: base.Digest},
				Definition:   candidate, ApprovalRef: "c2b2-zero-" + uuid.NewString(),
			},
			{
				ExpectedHead: ApprovedDefinitionFence{Version: math.MaxInt64, Digest: base.Digest},
				Definition:   candidate, ApprovalRef: "c2b2-overflow-" + uuid.NewString(),
			},
			{
				ExpectedHead: ApprovedDefinitionFence{Version: base.Version, Digest: base.Digest},
				Definition:   candidate, ApprovalRef: " leading-space",
			},
		}
		for index, params := range cases {
			if _, err := f.store.CommitApprovedDefinitionEdit(
				t.Context(), params); !errors.Is(err, types.ErrValidation) {
				t.Fatalf("invalid params[%d] error=%v, want Validation", index, err)
			}
		}
		assertApprovedDefinitionEditProjection(t, f, base, []int64{f.sourceID})
		assertApprovedDefinitionVersionCount(t, f, 1)
	})

	t.Run("legacy subscription scope cannot become a new approved version", func(t *testing.T) {
		f, base := newApprovedDefinitionEditFixture(t)
		candidate, _ := buildApprovedDefinitionEditCandidate(t, f, "不能降级成兼容范围", false)
		candidate.SourceScope = taskstate.SourceScopeLegacySubscriptions
		candidate.FetchPlan = json.RawMessage(`{}`)
		candidate.Sources = []taskstate.ApprovedSourceV1{}
		_, err := f.store.CommitApprovedDefinitionEdit(t.Context(),
			ApprovedDefinitionEditParams{
				ExpectedHead: ApprovedDefinitionFence{Version: base.Version, Digest: base.Digest},
				Definition:   candidate, ApprovalRef: "c2b2-legacy-" + uuid.NewString(),
			})
		if !errors.Is(err, types.ErrValidation) {
			t.Fatalf("legacy-scope edit error=%v, want Validation", err)
		}
		assertApprovedDefinitionEditProjection(t, f, base, []int64{f.sourceID})
		assertApprovedDefinitionVersionCount(t, f, 1)
	})

	t.Run("intent must be losslessly projected", func(t *testing.T) {
		f, base := newApprovedDefinitionEditFixture(t)
		candidate, _ := buildApprovedDefinitionEditCandidate(t, f, "可表示的正文", false)
		candidate.Intent = "与 playbook 不同"
		_, err := f.store.CommitApprovedDefinitionEdit(t.Context(),
			ApprovedDefinitionEditParams{
				ExpectedHead: ApprovedDefinitionFence{Version: base.Version, Digest: base.Digest},
				Definition:   candidate, ApprovalRef: "c2b2-intent-" + uuid.NewString(),
			})
		if !errors.Is(err, types.ErrValidation) {
			t.Fatalf("unrepresentable intent error=%v, want Validation", err)
		}
	})

	t.Run("dynamic mode remains unwritable", func(t *testing.T) {
		f, base := newApprovedDefinitionEditFixture(t)
		candidate, _ := buildApprovedDefinitionEditCandidate(t, f, "动态候选", false)
		candidate.ExecutionMode = types.ExecutionModeDiscoverAtRun
		_, err := f.store.CommitApprovedDefinitionEdit(t.Context(),
			ApprovedDefinitionEditParams{
				ExpectedHead: ApprovedDefinitionFence{Version: base.Version, Digest: base.Digest},
				Definition:   candidate, ApprovalRef: "c2b2-dynamic-" + uuid.NewString(),
			})
		if !errors.Is(err, types.ErrValidation) {
			t.Fatalf("dynamic edit error=%v, want Validation", err)
		}
	})

	t.Run("existing adaptive state requires explicit transition", func(t *testing.T) {
		f, base := newApprovedDefinitionEditFixture(t)
		basis := ApprovedDefinitionFence{Version: base.Version, Digest: base.Digest}
		state := buildAdaptiveState(t, f, taskstate.RunStatsV1{})
		if _, err := f.store.CompareAndSwapAdaptiveState(
			t.Context(), 0, basis, state, nil); err != nil {
			t.Fatal(err)
		}
		candidate, _ := buildApprovedDefinitionEditCandidate(t, f, "不能吞旧 adaptive", false)
		_, err := f.store.CommitApprovedDefinitionEdit(t.Context(),
			ApprovedDefinitionEditParams{
				ExpectedHead: basis, Definition: candidate,
				ApprovalRef: "c2b2-adaptive-" + uuid.NewString(),
			})
		if !errors.Is(err, types.ErrConflict) {
			t.Fatalf("adaptive edit error=%v, want Conflict", err)
		}
		assertApprovedDefinitionEditProjection(t, f, base, []int64{f.sourceID})
		assertApprovedDefinitionVersionCount(t, f, 1)
	})

	t.Run("headless and foreign scope are invisible", func(t *testing.T) {
		f := newTaskDefinitionStateFixture(t)
		candidate, _ := buildApprovedDefinitionEditCandidate(t, f, "headless", false)
		payload, err := taskstate.EncodeApprovedDefinitionV1(f.definition)
		if err != nil {
			t.Fatal(err)
		}
		base := ApprovedDefinitionFence{Version: 1, Digest: digestTaskStatePayload(payload)}
		_, err = f.store.CommitApprovedDefinitionEdit(t.Context(),
			ApprovedDefinitionEditParams{
				ExpectedHead: base, Definition: candidate,
				ApprovalRef: "c2b2-headless-" + uuid.NewString(),
			})
		if !errors.Is(err, types.ErrConflict) {
			t.Fatalf("headless edit error=%v, want Conflict", err)
		}
		candidate.UserID++
		_, err = f.store.CommitApprovedDefinitionEdit(t.Context(),
			ApprovedDefinitionEditParams{
				ExpectedHead: base, Definition: candidate,
				ApprovalRef: "c2b2-foreign-" + uuid.NewString(),
			})
		if !errors.Is(err, types.ErrNotFound) {
			t.Fatalf("foreign edit error=%v, want NotFound", err)
		}
	})
}

func TestCommitApprovedDefinitionEdit_AdaptiveCASSerializesWithEdit(t *testing.T) {
	f, base := newApprovedDefinitionEditFixture(t)
	basis := ApprovedDefinitionFence{Version: base.Version, Digest: base.Digest}
	state := buildAdaptiveState(t, f, taskstate.RunStatsV1{})
	candidate, _ := buildApprovedDefinitionEditCandidate(t, f, "与 adaptive 竞争", false)
	params := ApprovedDefinitionEditParams{
		ExpectedHead: basis, Definition: candidate,
		ApprovalRef: "c2b2-adaptive-race-" + uuid.NewString(),
	}

	type result struct {
		kind   string
		record ApprovedDefinitionVersionRecord
		err    error
	}
	start := make(chan struct{})
	results := make(chan result, 2)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		record, err := f.store.CommitApprovedDefinitionEdit(t.Context(), params)
		results <- result{kind: "edit", record: record, err: err}
	}()
	go func() {
		defer wg.Done()
		<-start
		_, err := f.store.CompareAndSwapAdaptiveState(
			t.Context(), 0, basis, state, nil)
		results <- result{kind: "adaptive", err: err}
	}()
	close(start)
	wg.Wait()
	close(results)

	var winner string
	var editRecord ApprovedDefinitionVersionRecord
	for got := range results {
		switch {
		case got.err == nil:
			if winner != "" {
				t.Fatalf("both competing writes succeeded: first=%s second=%s", winner, got.kind)
			}
			winner = got.kind
			editRecord = got.record
		case errors.Is(got.err, types.ErrConflict):
		default:
			t.Fatalf("%s returned unexpected error: %v", got.kind, got.err)
		}
	}
	var adaptiveRows int
	if err := f.store.pool.QueryRow(t.Context(),
		`SELECT count(*) FROM task_adaptive_states
		  WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3`,
		f.tenantID, f.userID, f.taskID).Scan(&adaptiveRows); err != nil {
		t.Fatal(err)
	}
	switch winner {
	case "edit":
		assertApprovedDefinitionEditProjection(t, f, editRecord, []int64{f.sourceID})
		assertApprovedDefinitionVersionCount(t, f, 2)
		if adaptiveRows != 0 {
			t.Fatalf("edit won but adaptive rows=%d, want 0", adaptiveRows)
		}
	case "adaptive":
		assertApprovedDefinitionEditProjection(t, f, base, []int64{f.sourceID})
		assertApprovedDefinitionVersionCount(t, f, 1)
		if adaptiveRows != 1 {
			t.Fatalf("adaptive won but adaptive rows=%d, want 1", adaptiveRows)
		}
	default:
		t.Fatal("neither competing write succeeded")
	}
}

func TestCommitApprovedDefinitionEdit_StatementFailuresRollbackEverything(t *testing.T) {
	cases := []struct {
		name           string
		failContains   string
		commitErr      error
		cancelOnCommit bool
	}{
		{name: "immutable append", failContains: "INSERT INTO task_approved_definition_versions"},
		{name: "playbook projection", failContains: "UPDATE schedule_playbooks"},
		{name: "clear source projection", failContains: "DELETE FROM schedule_sources"},
		{name: "insert source projection", failContains: "INSERT INTO schedule_sources"},
		{name: "advance head", failContains: "UPDATE schedules s"},
		{name: "commit after caller cancellation", commitErr: errInjectedCompiledTask,
			cancelOnCommit: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f, base := newApprovedDefinitionEditFixture(t)
			candidate, _ := buildApprovedDefinitionEditCandidate(
				t, f, "事务失败必须全回滚", true)
			faultStore := *f.store
			var wrapped *compiledTaskFaultTx
			callCtx := t.Context()
			var cancelCall context.CancelFunc
			if tc.cancelOnCommit {
				callCtx, cancelCall = context.WithCancel(callCtx)
				defer cancelCall()
			}
			faultStore.beginTx = func(ctx context.Context, opts pgx.TxOptions) (pgx.Tx, error) {
				realTx, err := f.store.pool.BeginTx(ctx, opts)
				if err != nil {
					return nil, err
				}
				wrapped = &compiledTaskFaultTx{
					Tx: realTx, failContains: tc.failContains, commitErr: tc.commitErr,
					cancelOnCommit: cancelCall,
				}
				return wrapped, nil
			}
			_, err := faultStore.CommitApprovedDefinitionEdit(
				callCtx, ApprovedDefinitionEditParams{
					ExpectedHead: ApprovedDefinitionFence{
						Version: base.Version, Digest: base.Digest,
					},
					Definition: candidate, ApprovalRef: "c2b2-fault-" + uuid.NewString(),
				})
			if !errors.Is(err, types.ErrDatabase) {
				t.Fatalf("fault error=%v, want Database", err)
			}
			if wrapped == nil || wrapped.rollbackCalls != 1 || wrapped.rollbackContextErr != nil {
				t.Fatalf("rollback wrapper=%+v", wrapped)
			}
			assertApprovedDefinitionEditProjection(t, f, base, []int64{f.sourceID})
			assertApprovedDefinitionVersionCount(t, f, 1)
		})
	}
}

func newApprovedDefinitionEditFixture(
	t *testing.T,
) (taskDefinitionStateFixture, ApprovedDefinitionVersionRecord) {
	t.Helper()
	f := newTaskDefinitionStateFixture(t)
	base, err := f.store.InsertInitialApprovedDefinition(
		t.Context(), f.definition, "c2b2-base:"+f.taskID)
	if err != nil {
		t.Fatalf("seed approved head: %v", err)
	}
	return f, base
}

func buildApprovedDefinitionEditCandidate(
	t *testing.T,
	f taskDefinitionStateFixture,
	intent string,
	addSource bool,
) (taskstate.ApprovedDefinitionV1, []int64) {
	t.Helper()
	planSources := make([]taskstate.PlanSourceV1, 0, 2)
	approvedSources := make([]taskstate.ApprovedSourceV1, 0, 2)
	first := f.definition.Sources[0]
	planSources = append(planSources, taskstate.PlanSourceV1{
		Platform: first.Platform, Capability: first.Capability,
		Title: first.Title, URL: first.URL, Config: bytes.Clone(first.Config),
	})
	approvedSources = append(approvedSources, first)
	sourceIDs := []int64{first.SourceID}
	if addSource {
		source, message := sourcespec.Build(sourcespec.Spec{
			Platform: string(types.PlatformWeb), Capability: string(types.CapSearch),
			Params: map[string]string{"query": "c2b2-extra-" + uuid.NewString()},
			Title:  "C2b2 second source",
		})
		if message != "" || source == nil {
			t.Fatalf("build second source: %q", message)
		}
		sourceID, _, err := f.store.GetOrCreateSource(t.Context(), source)
		if err != nil {
			t.Fatalf("materialize second source: %v", err)
		}
		t.Cleanup(func() {
			_, _ = f.store.pool.Exec(context.Background(),
				`DELETE FROM schedule_sources WHERE schedule_id=$1 AND source_id=$2`,
				f.taskID, sourceID)
			_, _ = f.store.pool.Exec(context.Background(),
				`DELETE FROM sources WHERE id=$1`, sourceID)
		})
		// Preserve a deliberately non-canonical execution order. The immutable
		// Sources set will sort by URL, while FetchPlan must retain this order.
		planSources = append([]taskstate.PlanSourceV1{{
			Platform: source.Platform, Capability: source.Capability,
			Title: source.Title, URL: source.URL, Config: bytes.Clone(source.Config),
		}}, planSources...)
		approvedSources = append(approvedSources, taskstate.ApprovedSourceV1{
			SourceID: sourceID, Platform: source.Platform, Capability: source.Capability,
			Title: source.Title, URL: source.URL, Config: bytes.Clone(source.Config),
		})
		sourceIDs = append(sourceIDs, sourceID)
	}
	plan, err := json.Marshal(taskstate.FetchPlanV1{Sources: planSources})
	if err != nil {
		t.Fatal(err)
	}
	definition, err := taskstate.BuildApprovedDefinitionV1(
		taskstate.ApprovedDefinitionInputV1{
			TenantID: f.tenantID, UserID: f.userID, TaskID: f.taskID,
			Intent: intent, NLDescription: "C2b2 已批准展示名",
			SpecJSON: json.RawMessage(`{"cron":"30 9 * * 1","tz":"Asia/Shanghai"}`),
			ScopeJSON: json.RawMessage(fmt.Sprintf(
				`{"source_ids":[%d],"top_n":3}`, f.sourceID)),
			PlaybookContent: intent, SourceScope: taskstate.SourceScopeApprovedPlan,
			FetchPlan: plan, Strictness: types.StrictnessStrict,
			Sources: approvedSources, ExecutionMode: types.ExecutionModeCompiled,
			DeliveryPolicy: taskstate.DeliveryPolicyOwnerFeishu,
			BudgetPolicy:   taskstate.BudgetPolicyInheritTenantQuota,
		})
	if err != nil {
		t.Fatalf("build edit candidate: %v", err)
	}
	return definition, sourceIDs
}

func assertApprovedDefinitionEditProjection(
	t *testing.T,
	f taskDefinitionStateFixture,
	want ApprovedDefinitionVersionRecord,
	wantSourceIDs []int64,
) {
	t.Helper()
	current, err := f.store.GetCurrentApprovedDefinition(
		t.Context(), f.tenantID, f.userID, f.taskID)
	if err != nil {
		t.Fatalf("load current approved definition: %v", err)
	}
	if current.Version != want.Version || current.Digest != want.Digest ||
		!bytes.Equal(current.Payload, want.Payload) {
		t.Fatalf("current head=%+v, want %+v", current, want)
	}
	var nlDescription, rawMode string
	var rawStrictness *string
	var specJSON, scopeJSON []byte
	var headVersion int64
	var headDigest string
	if err := f.store.pool.QueryRow(t.Context(),
		`SELECT nl_description, spec_json, scope_json, push_strictness,
		        execution_mode, approved_definition_version,
		        approved_definition_digest
		   FROM schedules
		  WHERE tenant_id=$1 AND user_id=$2 AND id=$3`,
		f.tenantID, f.userID, f.taskID,
	).Scan(&nlDescription, &specJSON, &scopeJSON, &rawStrictness,
		&rawMode, &headVersion, &headDigest); err != nil {
		t.Fatal(err)
	}
	definition := want.Definition
	strictness := types.StrictnessLoose
	if rawStrictness != nil {
		strictness = types.PushStrictness(*rawStrictness)
	}
	if nlDescription != definition.NLDescription ||
		!jsonBytesEqual(t, specJSON, definition.SpecJSON) ||
		!jsonBytesEqual(t, scopeJSON, definition.ScopeJSON) ||
		strictness != definition.Strictness ||
		rawMode != string(definition.ExecutionMode) ||
		headVersion != want.Version || headDigest != want.Digest {
		t.Fatalf("schedule projection differs: nl=%q spec=%s scope=%s strict=%v mode=%q head=%d/%s",
			nlDescription, specJSON, scopeJSON, rawStrictness, rawMode,
			headVersion, headDigest)
	}
	playbook, err := f.store.GetSchedulePlaybook(t.Context(), f.userID, f.taskID)
	if err != nil || playbook.Content != definition.PlaybookContent ||
		!jsonBytesEqual(t, playbook.FetchPlan, definition.FetchPlan) {
		t.Fatalf("playbook projection=%+v err=%v", playbook, err)
	}
	gotSourceIDs, err := f.store.ListScheduleSourceIDs(t.Context(), f.userID, f.taskID)
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(wantSourceIDs)
	if !slices.Equal(gotSourceIDs, wantSourceIDs) {
		t.Fatalf("source projection=%v, want %v", gotSourceIDs, wantSourceIDs)
	}
}

func assertApprovedDefinitionVersionCount(
	t *testing.T,
	f taskDefinitionStateFixture,
	want int,
) {
	t.Helper()
	var got int
	if err := f.store.pool.QueryRow(t.Context(),
		`SELECT count(*) FROM task_approved_definition_versions
		  WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3`,
		f.tenantID, f.userID, f.taskID,
	).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("approved version count=%d, want %d", got, want)
	}
}

func jsonBytesEqual(t *testing.T, left, right []byte) bool {
	t.Helper()
	var leftValue, rightValue any
	if err := json.Unmarshal(left, &leftValue); err != nil {
		t.Fatalf("decode left JSON %s: %v", left, err)
	}
	if err := json.Unmarshal(right, &rightValue); err != nil {
		t.Fatalf("decode right JSON %s: %v", right, err)
	}
	leftCanonical, err := json.Marshal(leftValue)
	if err != nil {
		t.Fatal(err)
	}
	rightCanonical, err := json.Marshal(rightValue)
	if err != nil {
		t.Fatal(err)
	}
	return bytes.Equal(leftCanonical, rightCanonical)
}
