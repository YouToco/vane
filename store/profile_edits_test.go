package store

import (
	"context"
	"errors"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/YouToco/vane/types"
)

func TestProfileManualAuthority(t *testing.T) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("未设置 DATABASE_URL，跳过画像 authority 真 PostgreSQL 测试")
	}
	if err := Migrate(t.Context(), dbURL); err != nil {
		t.Fatal(err)
	}
	st, err := New(t.Context(), dbURL)
	if err != nil {
		t.Fatal(err)
	}
	registerStoreClose(t, st)
	u, err := st.UpsertUserByOpenID(
		t.Context(), "profile_authority_"+uuid.NewString(), "authority")
	if err != nil {
		t.Fatal(err)
	}
	attachTenant(t, st, u.ID)
	t.Cleanup(func() {
		ctx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(ctx, t, st, `DELETE FROM profile_edit_receipts WHERE user_id=$1`, u.ID)
		cleanupExec(ctx, t, st, `DELETE FROM profile_edit_revisions WHERE user_id=$1`, u.ID)
		cleanupExec(ctx, t, st, `DELETE FROM profiles WHERE user_id=$1`, u.ID)
		cleanupExec(ctx, t, st, `DELETE FROM users WHERE id=$1`, u.ID)
	})

	industry := "AI"
	tagsA := []string{"A", "B"}
	patch := types.ProfileEditPatch{Industry: &industry, Tags: &tagsA}
	first, err := st.PatchProfile(
		t.Context(), 1, u.ID, nil, patch, "create-1", strings.Repeat("a", 64))
	if err != nil {
		t.Fatalf("首次创建: %v", err)
	}
	if first.Industry != "AI" || len(first.Tags) != 2 || first.Summary != "" {
		t.Fatalf("首次响应异常: %+v", first)
	}
	restrictedRead, err := st.GetProfileView(t.Context(), 1, u.ID)
	if err != nil || restrictedRead.Industry != first.Industry ||
		!restrictedRead.UpdatedAt.Equal(first.UpdatedAt) {
		t.Fatalf("restricted GET=%+v err=%v", restrictedRead, err)
	}

	// 响应丢失：同键同 canonical bytes 精确重放首次快照。
	replay, err := st.PatchProfile(
		t.Context(), 1, u.ID, nil, patch, "create-1", strings.Repeat("a", 64))
	if err != nil || !replay.UpdatedAt.Equal(first.UpdatedAt) {
		t.Fatalf("幂等重放=%+v err=%v", replay, err)
	}
	if _, err := st.PatchProfile(
		t.Context(), 1, u.ID, nil, patch, "create-1", strings.Repeat("b", 64),
	); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("同键异请求应冲突: %v", err)
	}
	if _, err := st.UndoProfileEdit(
		t.Context(), 1, u.ID, 999999999, first.UpdatedAt,
		"create-1", strings.Repeat("u", 64),
	); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("PATCH key 跨端点复用于无效 target 应先冲突: %v", err)
	}

	// 人工删除 B：黑名单应为 B。
	tagsOnlyA := []string{"A"}
	second, err := st.PatchProfile(
		t.Context(), 1, u.ID, &first.UpdatedAt,
		types.ProfileEditPatch{Tags: &tagsOnlyA},
		"edit-2", strings.Repeat("c", 64))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(second.RemovedTags, ",") != "B" {
		t.Fatalf("PATCH response removed_tags=%v", second.RemovedTags)
	}
	p, err := st.GetProfileForTenant(t.Context(), 1, u.ID)
	if err != nil || strings.Join(p.RemovedTags, ",") != "B" {
		t.Fatalf("删除后黑名单=%v err=%v", p.RemovedTags, err)
	}

	// 反例：before tags[A], removed[B] -> edit tags[C] => removed[A,B]；
	// undo 必须精确回到 tags[A], removed[B]，不能走普通替换公式变成[B,C]。
	tagsC := []string{"C"}
	third, err := st.PatchProfile(
		t.Context(), 1, u.ID, &second.UpdatedAt,
		types.ProfileEditPatch{Tags: &tagsC},
		"edit-3", strings.Repeat("d", 64))
	if err != nil {
		t.Fatal(err)
	}
	cursorState, err := st.GetProfileForTenant(t.Context(), 1, u.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.AdvanceProfileCursor(
		t.Context(), u.ID, cursorState.LastEvolvedFeedbackID+10,
		cursorState.UpdatedAt, cursorState.LastEvolvedFeedbackID); err != nil {
		t.Fatal(err)
	}
	edits, err := st.ListProfileEdits(t.Context(), 1, u.ID, 20)
	if err != nil || len(edits) < 3 || !edits[0].Undoable {
		t.Fatalf("历史=%+v err=%v", edits, err)
	}
	targetID, _ := strconv.ParseInt(edits[0].ID, 10, 64)
	restored, err := st.UndoProfileEdit(
		t.Context(), 1, u.ID, targetID, third.UpdatedAt,
		"undo-3", strings.Repeat("e", 64))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(restored.Tags, ",") != "A" {
		t.Fatalf("撤销 tags=%v", restored.Tags)
	}
	if strings.Join(restored.RemovedTags, ",") != "B" {
		t.Fatalf("撤销响应 removed_tags=%v", restored.RemovedTags)
	}
	p, err = st.GetProfileForTenant(t.Context(), 1, u.ID)
	if err != nil || strings.Join(p.RemovedTags, ",") != "B" {
		t.Fatalf("撤销 removed_tags=%v err=%v", p.RemovedTags, err)
	}
	if p.LastEvolvedFeedbackID != cursorState.LastEvolvedFeedbackID+10 {
		t.Fatalf("撤销改写游标=%d", p.LastEvolvedFeedbackID)
	}
	if _, err := st.UndoProfileEdit(
		t.Context(), 1, u.ID, targetID, restored.UpdatedAt,
		"undo-again", strings.Repeat("f", 64),
	); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("禁止 redo/重复撤销: %v", err)
	}

	// legacy Agent 人工写刷 updated_at 后，旧 Web revision 不得覆盖它。
	occupation := "legacy-won"
	postUndo, err := st.PatchProfile(
		t.Context(), 1, u.ID, &restored.UpdatedAt,
		types.ProfileEditPatch{Occupation: &occupation},
		"post-undo-edit", strings.Repeat("9", 64))
	if err != nil {
		t.Fatal(err)
	}
	edits, err = st.ListProfileEdits(t.Context(), 1, u.ID, 1)
	if err != nil {
		t.Fatal(err)
	}
	latestID, _ := strconv.ParseInt(edits[0].ID, 10, 64)
	if _, err := st.UpsertProfileFields(t.Context(), u.ID, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := st.UndoProfileEdit(
		t.Context(), 1, u.ID, latestID, postUndo.UpdatedAt,
		"legacy-conflict", strings.Repeat("0", 64),
	); !errors.Is(err, types.ErrConflict) {
		t.Fatalf("legacy write 后撤销应冲突: %v", err)
	}

	// 审计 JSON 只含结构化 authority 字段，不复制 summary/token/cursor。
	var auditText string
	if err := st.pool.QueryRow(t.Context(),
		`SELECT before_fields::text || after_fields::text
		   FROM profile_edit_revisions WHERE tenant_id=1 AND user_id=$1
		   ORDER BY id DESC LIMIT 1`, u.ID).Scan(&auditText); err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"summary", "token", "cursor", "last_evolved"} {
		if strings.Contains(auditText, forbidden) {
			t.Fatalf("审计泄漏 %q: %s", forbidden, auditText)
		}
	}
}

func TestPublicProfileChangesCreateOmitsSyntheticEmptyFields(t *testing.T) {
	got := publicProfileChanges(
		profileEditFields{Exists: false, Tags: []string{}, RemovedTags: []string{}},
		profileEditFields{
			Exists: true, Industry: "AI",
			Tags: []string{}, RemovedTags: []string{},
		})
	if len(got) != 1 || got[0].Field != "industry" ||
		got[0].Before != "" || got[0].After != "AI" {
		t.Fatalf("create changes=%+v", got)
	}
}

func TestProfileManualAuthorityConcurrentCAS(t *testing.T) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("未设置 DATABASE_URL")
	}
	if err := Migrate(t.Context(), dbURL); err != nil {
		t.Fatal(err)
	}
	st, err := New(t.Context(), dbURL)
	if err != nil {
		t.Fatal(err)
	}
	registerStoreClose(t, st)
	userID := testUserWithTenant(t, st, "profile-cas")
	t.Cleanup(func() {
		ctx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(ctx, t, st, `DELETE FROM profile_edit_receipts WHERE user_id=$1`, userID)
		cleanupExec(ctx, t, st, `DELETE FROM profile_edit_revisions WHERE user_id=$1`, userID)
		cleanupExec(ctx, t, st, `DELETE FROM profiles WHERE user_id=$1`, userID)
		cleanupExec(ctx, t, st, `DELETE FROM users WHERE id=$1`, userID)
	})
	a, b := "A", "B"
	type result struct {
		profile *types.ProfileView
		err     error
	}
	run := func(expected *time.Time, key, digest string, value *string, start <-chan struct{}, out chan<- result) {
		<-start
		p, err := st.PatchProfile(
			t.Context(), 1, userID, expected,
			types.ProfileEditPatch{Industry: value}, key, digest)
		out <- result{p, err}
	}
	start := make(chan struct{})
	out := make(chan result, 2)
	go run(nil, "first-a", strings.Repeat("5", 64), &a, start, out)
	go run(nil, "first-b", strings.Repeat("6", 64), &b, start, out)
	close(start)
	results := []result{<-out, <-out}
	success, conflicts := 0, 0
	var base *types.ProfileView
	for _, r := range results {
		if r.err == nil {
			success++
			base = r.profile
		} else if errors.Is(r.err, types.ErrConflict) {
			conflicts++
		} else {
			t.Fatalf("首建竞态异常: %v", r.err)
		}
	}
	if success != 1 || conflicts != 1 {
		t.Fatalf("首建 success=%d conflict=%d", success, conflicts)
	}

	start = make(chan struct{})
	out = make(chan result, 2)
	c, d := "C", "D"
	go run(&base.UpdatedAt, "tab-a", strings.Repeat("7", 64), &c, start, out)
	go run(&base.UpdatedAt, "tab-b", strings.Repeat("8", 64), &d, start, out)
	close(start)
	results = []result{<-out, <-out}
	success, conflicts = 0, 0
	for _, r := range results {
		if r.err == nil {
			success++
		} else if errors.Is(r.err, types.ErrConflict) {
			conflicts++
		} else {
			t.Fatalf("双 tab 异常: %v", r.err)
		}
	}
	if success != 1 || conflicts != 1 {
		t.Fatalf("双tab success=%d conflict=%d", success, conflicts)
	}

	// 直接 SQL-CAS 竞态：两事务共享同一 expected token，只有一个 UPDATE
	// 能命中。该用例刻意绕开 FOR UPDATE；删掉 updateProfileTx 的
	// `AND updated_at=$3` 会让两边都成功，精确咬住 CAS predicate。
	current, err := st.GetProfileForTenant(t.Context(), 1, userID)
	if err != nil {
		t.Fatal(err)
	}
	tx1, err := st.beginProfileEditTx(t.Context(), 1, userID)
	if err != nil {
		t.Fatal(err)
	}
	tx2, err := st.beginProfileEditTx(t.Context(), 1, userID)
	if err != nil {
		_ = tx1.Rollback(t.Context())
		t.Fatal(err)
	}
	type sqlCASResult struct {
		index int
		err   error
	}
	sqlResults := make(chan sqlCASResult, 2)
	startSQL := make(chan struct{})
	e, f := "E", "F"
	go func() {
		<-startSQL
		_, err := updateProfileTx(
			t.Context(), tx1, 1, userID, current.UpdatedAt,
			types.ProfileEditPatch{Industry: &e})
		sqlResults <- sqlCASResult{1, err}
	}()
	go func() {
		<-startSQL
		_, err := updateProfileTx(
			t.Context(), tx2, 1, userID, current.UpdatedAt,
			types.ProfileEditPatch{Industry: &f})
		sqlResults <- sqlCASResult{2, err}
	}()
	close(startSQL)
	firstSQL := <-sqlResults
	if firstSQL.err != nil {
		_ = tx1.Rollback(t.Context())
		_ = tx2.Rollback(t.Context())
		t.Fatalf("首个 SQL-CAS 应成功: %v", firstSQL.err)
	}
	if firstSQL.index == 1 {
		if err := tx1.Commit(t.Context()); err != nil {
			t.Fatal(err)
		}
	} else if err := tx2.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	secondSQL := <-sqlResults
	if !errors.Is(secondSQL.err, pgx.ErrNoRows) {
		t.Fatalf("后到 SQL-CAS 应 ErrNoRows: %v", secondSQL.err)
	}
	if secondSQL.index == 1 {
		_ = tx1.Rollback(t.Context())
	} else {
		_ = tx2.Rollback(t.Context())
	}
}

func TestProfileManualAuthorityEmptyArraysUndoAndExactUserRLS(t *testing.T) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("未设置 DATABASE_URL")
	}
	if err := Migrate(t.Context(), dbURL); err != nil {
		t.Fatal(err)
	}
	st, err := New(t.Context(), dbURL)
	if err != nil {
		t.Fatal(err)
	}
	registerStoreClose(t, st)
	user1 := testUserWithTenant(t, st, "profile-empty-owner")
	user2 := testUserWithTenant(t, st, "profile-same-tenant-other")
	t.Cleanup(func() {
		ctx, cancel := cleanupContext()
		defer cancel()
		for _, id := range []int64{user1, user2} {
			cleanupExec(ctx, t, st, `DELETE FROM profile_edit_receipts WHERE user_id=$1`, id)
			cleanupExec(ctx, t, st, `DELETE FROM profile_edit_revisions WHERE user_id=$1`, id)
			cleanupExec(ctx, t, st, `DELETE FROM profiles WHERE user_id=$1`, id)
			cleanupExec(ctx, t, st, `DELETE FROM users WHERE id=$1`, id)
		}
	})
	empty := []string{}
	created, err := st.PatchProfile(
		t.Context(), 1, user1, nil,
		types.ProfileEditPatch{Tags: &empty},
		"empty-create", strings.Repeat("2", 64))
	if err != nil {
		t.Fatal(err)
	}
	occupation := "builder"
	edited, err := st.PatchProfile(
		t.Context(), 1, user1, &created.UpdatedAt,
		types.ProfileEditPatch{Occupation: &occupation},
		"empty-edit", strings.Repeat("3", 64))
	if err != nil {
		t.Fatal(err)
	}
	edits, err := st.ListProfileEdits(t.Context(), 1, user1, 5)
	if err != nil {
		t.Fatal(err)
	}
	target, _ := strconv.ParseInt(edits[0].ID, 10, 64)
	restored, err := st.UndoProfileEdit(
		t.Context(), 1, user1, target, edited.UpdatedAt,
		"empty-undo", strings.Repeat("4", 64))
	if err != nil {
		t.Fatal(err)
	}
	if restored.Tags == nil || restored.RemovedTags == nil ||
		len(restored.Tags) != 0 || len(restored.RemovedTags) != 0 {
		t.Fatalf("empty arrays not preserved: %+v", restored)
	}

	otherIndustry := "other"
	if _, err := st.UpsertProfileFields(
		t.Context(), user2, &otherIndustry, nil, nil); err != nil {
		t.Fatal(err)
	}
	otherBefore, err := st.GetProfileForTenant(t.Context(), 1, user2)
	if err != nil {
		t.Fatal(err)
	}
	shouldRollback := "must-not-commit"
	if _, err := st.PatchProfile(
		t.Context(), 1, user2, &otherBefore.UpdatedAt,
		types.ProfileEditPatch{Industry: &shouldRollback},
		strings.Repeat("k", 129), strings.Repeat("a", 64),
	); !errors.Is(err, types.ErrDatabase) {
		t.Fatalf("receipt constraint should abort transaction: %v", err)
	}
	otherAfter, err := st.GetProfileForTenant(t.Context(), 1, user2)
	if err != nil {
		t.Fatal(err)
	}
	if otherAfter.Industry != otherBefore.Industry ||
		!otherAfter.UpdatedAt.Equal(otherBefore.UpdatedAt) {
		t.Fatalf("receipt failure leaked profile update: before=%+v after=%+v",
			otherBefore, otherAfter)
	}
	if _, err := st.UndoProfileEdit(
		t.Context(), 1, user2, target, otherAfter.UpdatedAt,
		"cross-user-undo", strings.Repeat("b", 64),
	); !errors.Is(err, types.ErrNotFound) {
		t.Fatalf("跨用户 revision id 应统一 NotFound: %v", err)
	}
	var revisions int
	if err := st.pool.QueryRow(t.Context(),
		`SELECT count(*) FROM profile_edit_revisions WHERE user_id=$1`,
		user2).Scan(&revisions); err != nil || revisions != 0 {
		t.Fatalf("receipt failure leaked revisions=%d err=%v", revisions, err)
	}
	var extraTenant int64
	if err := st.pool.QueryRow(t.Context(),
		`INSERT INTO tenants (status,plan)
		 VALUES ('active','free') RETURNING id`).Scan(&extraTenant); err != nil {
		t.Fatal(err)
	}
	if _, err := st.pool.Exec(t.Context(),
		`INSERT INTO memberships (tenant_id,user_id,role)
		 VALUES ($1,$2,'member')`, extraTenant, user1); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(ctx, t, st,
			`DELETE FROM memberships WHERE tenant_id=$1 AND user_id=$2`,
			extraTenant, user1)
		cleanupExec(ctx, t, st, `DELETE FROM tenants WHERE id=$1`, extraTenant)
	})
	editorVisible := func(
		tenantSetting, userSetting *string, table string,
	) (int, error) {
		t.Helper()
		scopeTx, err := st.pool.BeginTx(t.Context(), pgx.TxOptions{})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = scopeTx.Rollback(context.WithoutCancel(t.Context())) }()
		if tenantSetting != nil {
			if _, err := scopeTx.Exec(t.Context(),
				`SELECT set_config('app.tenant_id',$1,true)`, *tenantSetting); err != nil {
				t.Fatal(err)
			}
		}
		if userSetting != nil {
			if _, err := scopeTx.Exec(t.Context(),
				`SELECT set_config('app.user_id',$1,true)`, *userSetting); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := scopeTx.Exec(
			t.Context(), `SET LOCAL ROLE vane_profile_editor`); err != nil {
			t.Fatal(err)
		}
		var count int
		if err := scopeTx.QueryRow(
			t.Context(), `SELECT count(*) FROM `+table+` WHERE user_id=$1`,
			user1).Scan(&count); err != nil {
			return 0, err
		}
		return count, nil
	}
	tenantOne := "1"
	userOne := strconv.FormatInt(user1, 10)
	wrongTenant := strconv.FormatInt(extraTenant, 10)
	wrongUser := strconv.FormatInt(user2, 10)
	for _, table := range []string{
		"profiles", "profile_edit_revisions", "profile_edit_receipts",
	} {
		got, err := editorVisible(&tenantOne, &userOne, table)
		if err != nil {
			t.Fatalf("exact scope read %s: %v", table, err)
		}
		if got == 0 {
			t.Fatalf("exact scope cannot read %s", table)
		}
		for name, scope := range map[string][2]*string{
			"missing":      {nil, nil},
			"missing user": {&tenantOne, nil},
			"wrong tenant": {&wrongTenant, &userOne},
			"wrong user":   {&tenantOne, &wrongUser},
		} {
			got, err := editorVisible(scope[0], scope[1], table)
			if err != nil {
				var pgErr *pgconn.PgError
				if strings.HasPrefix(name, "missing") &&
					errors.As(err, &pgErr) && pgErr.Code == "22P02" {
					// 022's older tenant policy casts an empty, reset GUC
					// directly to bigint. A reused connection may therefore
					// reject a missing scope instead of returning zero rows;
					// either result is fail-closed.
					continue
				}
				t.Fatalf("%s scope read %s: %v", name, table, err)
			}
			if got != 0 {
				t.Fatalf("%s scope saw %d rows in %s", name, got, table)
			}
		}
	}
	tx, err := st.pool.BeginTx(t.Context(), pgx.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(t.Context())) }()
	if _, err := tx.Exec(t.Context(),
		`SELECT set_config('app.tenant_id','1',true),
		        set_config('app.user_id',$1,true)`,
		strconv.FormatInt(user1, 10)); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(t.Context(), `SET LOCAL ROLE vane_profile_editor`); err != nil {
		t.Fatal(err)
	}
	var visible int
	if err := tx.QueryRow(t.Context(),
		`SELECT count(*) FROM profiles WHERE user_id=$1`, user2).Scan(&visible); err != nil {
		t.Fatal(err)
	}
	if visible != 0 {
		t.Fatalf("editor saw same-tenant other user: %d", visible)
	}
	if err := tx.QueryRow(t.Context(),
		`SELECT count(*) FROM memberships
		  WHERE tenant_id<>1 OR user_id<>$1`, user1).Scan(&visible); err != nil {
		t.Fatal(err)
	}
	if visible != 0 {
		t.Fatalf("editor saw membership outside exact tenant+user: %d", visible)
	}
	tag, err := tx.Exec(t.Context(),
		`UPDATE profiles SET industry='forbidden' WHERE user_id=$1`, user2)
	if err != nil {
		t.Fatal(err)
	}
	if tag.RowsAffected() != 0 {
		t.Fatalf("editor mutated same-tenant other user")
	}
	if err := tx.Rollback(t.Context()); err != nil {
		t.Fatal(err)
	}
	assertEditorDenied := func(sql string, args ...any) {
		t.Helper()
		deniedTx, err := st.pool.BeginTx(t.Context(), pgx.TxOptions{})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = deniedTx.Rollback(context.WithoutCancel(t.Context())) }()
		if _, err := deniedTx.Exec(t.Context(),
			`SELECT set_config('app.tenant_id','1',true),
			        set_config('app.user_id',$1,true)`,
			strconv.FormatInt(user1, 10)); err != nil {
			t.Fatal(err)
		}
		if _, err := deniedTx.Exec(
			t.Context(), `SET LOCAL ROLE vane_profile_editor`); err != nil {
			t.Fatal(err)
		}
		_, err = deniedTx.Exec(t.Context(), sql, args...)
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "42501" {
			t.Fatalf("profile editor unexpectedly allowed: %s", sql)
		}
	}
	assertEditorDenied(`UPDATE profiles SET summary='forbidden' WHERE user_id=$1`, user1)
	assertEditorDenied(`UPDATE profiles SET last_evolved_feedback_id=999 WHERE user_id=$1`, user1)
	assertEditorDenied(`UPDATE profiles SET token_budget_daily=0 WHERE user_id=$1`, user1)
	assertEditorDenied(`DELETE FROM profiles WHERE user_id=$1`, user1)
	assertEditorDenied(`SELECT token_budget_daily FROM profiles WHERE user_id=$1`, user1)
	assertEditorDenied(`SELECT last_value FROM profiles_id_seq`)
	assertEditorDenied(`UPDATE profile_edit_revisions SET kind='undo' WHERE user_id=$1`, user1)
	assertEditorDenied(`DELETE FROM profile_edit_revisions WHERE user_id=$1`, user1)
	assertEditorDenied(`UPDATE profile_edit_receipts SET request_digest=request_digest WHERE user_id=$1`, user1)
	assertEditorDenied(`DELETE FROM profile_edit_receipts WHERE user_id=$1`, user1)
	assertEditorDenied(`TRUNCATE profile_edit_receipts, profile_edit_revisions`)
}

func TestProfileManualAuthorityRejectsCrossTenant(t *testing.T) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("未设置 DATABASE_URL")
	}
	if err := Migrate(t.Context(), dbURL); err != nil {
		t.Fatal(err)
	}
	st, err := New(t.Context(), dbURL)
	if err != nil {
		t.Fatal(err)
	}
	registerStoreClose(t, st)
	var tenantID int64
	if err := st.pool.QueryRow(t.Context(),
		`INSERT INTO tenants (status,plan) VALUES ('active','free') RETURNING id`,
	).Scan(&tenantID); err != nil {
		t.Fatal(err)
	}
	u, err := st.UpsertUserByOpenID(
		t.Context(), "profile_cross_"+uuid.NewString(), "cross")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.pool.Exec(t.Context(),
		`INSERT INTO memberships (tenant_id,user_id,role) VALUES ($1,$2,'owner')`,
		tenantID, u.ID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(ctx, t, st, `DELETE FROM users WHERE id=$1`, u.ID)
		cleanupExec(ctx, t, st, `DELETE FROM tenants WHERE id=$1`, tenantID)
	})
	value := "forbidden"
	_, err = st.PatchProfile(
		t.Context(), 1, u.ID, nil,
		types.ProfileEditPatch{Industry: &value},
		"cross-tenant", strings.Repeat("1", 64))
	if !errors.Is(err, types.ErrConflict) {
		t.Fatalf("跨租户创建应冲突: %v", err)
	}
	var count int
	if err := st.pool.QueryRow(t.Context(),
		`SELECT count(*) FROM profiles WHERE user_id=$1`, u.ID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("跨租户写入产生 %d 行", count)
	}
}

func TestProfileEditTxPinsSearchPath(t *testing.T) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("未设置 DATABASE_URL")
	}
	if err := Migrate(t.Context(), dbURL); err != nil {
		t.Fatal(err)
	}
	st, err := New(t.Context(), dbURL)
	if err != nil {
		t.Fatal(err)
	}
	registerStoreClose(t, st)
	userID := testUserWithTenant(t, st, "profile-search-path")
	t.Cleanup(func() {
		ctx, cancel := cleanupContext()
		defer cancel()
		cleanupExec(ctx, t, st,
			`DELETE FROM profile_edit_receipts WHERE user_id=$1`, userID)
		cleanupExec(ctx, t, st,
			`DELETE FROM profile_edit_revisions WHERE user_id=$1`, userID)
		cleanupExec(ctx, t, st, `DELETE FROM profiles WHERE user_id=$1`, userID)
		cleanupExec(ctx, t, st, `DELETE FROM users WHERE id=$1`, userID)
	})
	industry := "public-safe"
	if _, err := st.PatchProfile(
		t.Context(), 1, userID, nil,
		types.ProfileEditPatch{Industry: &industry},
		"search-path-create", strings.Repeat("7", 64),
	); err != nil {
		t.Fatal(err)
	}

	conn, err := pgx.Connect(t.Context(), dbURL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(context.Background()) }()
	if _, err := conn.Exec(t.Context(),
		`CREATE TEMP TABLE profiles (user_id bigint, industry text)`); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(t.Context(),
		`INSERT INTO profiles VALUES ($1,'pg-temp-trap')`, userID); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(
		t.Context(), `SET search_path=pg_temp,public`); err != nil {
		t.Fatal(err)
	}
	poisoned := &Store{beginTx: conn.BeginTx}
	tx, err := poisoned.beginProfileEditTx(t.Context(), 1, userID)
	if err != nil {
		t.Fatal(err)
	}
	var searchPath, got string
	if err := tx.QueryRow(t.Context(), `SHOW search_path`).Scan(&searchPath); err != nil {
		t.Fatal(err)
	}
	if searchPath != "pg_catalog, public, pg_temp" {
		t.Fatalf("profile edit transaction inherited poisoned search_path=%q", searchPath)
	}
	if err := tx.QueryRow(t.Context(),
		`SELECT industry FROM profiles WHERE user_id=$1`,
		userID).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != industry {
		t.Fatalf("unqualified profile query resolved to %q, want public %q", got, industry)
	}
	if err := tx.Rollback(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := conn.QueryRow(
		t.Context(), `SHOW search_path`).Scan(&searchPath); err != nil {
		t.Fatal(err)
	}
	if searchPath != "pg_temp, public" {
		t.Fatalf("transaction-local safe search_path leaked to session: %q", searchPath)
	}
}
