package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/server/types"
)

type profileEditFields struct {
	Exists      bool     `json:"exists"`
	Industry    string   `json:"industry"`
	Occupation  string   `json:"occupation"`
	Tags        []string `json:"tags"`
	RemovedTags []string `json:"removed_tags"`
}

const profileEditColumns = `industry,occupation,tags,removed_tags,summary,
	created_at,updated_at`

func scanProfileEdit(row pgx.Row, p *types.Profile) error {
	return row.Scan(
		&p.Industry, &p.Occupation, &p.Tags, &p.RemovedTags, &p.Summary,
		&p.CreatedAt, &p.UpdatedAt,
	)
}

type profileEditRevisionRow struct {
	ID               int64
	Kind             string
	TargetRevisionID *int64
	Before           profileEditFields
	After            profileEditFields
	ResultUpdatedAt  time.Time
	CreatedAt        time.Time
}

func ProfileEditRequestDigest(v any) (string, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return "", types.NewAppError(types.CodeValidation, "画像修改请求无法规范化", err)
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

func (s *Store) PatchProfile(
	ctx context.Context,
	tenantID, userID int64,
	expectedUpdatedAt *time.Time,
	patch types.ProfileEditPatch,
	idempotencyKey, requestDigest string,
) (*types.ProfileView, error) {
	if tenantID <= 0 || userID <= 0 || idempotencyKey == "" || requestDigest == "" {
		return nil, types.NewAppError(types.CodeValidation, "画像修改范围或幂等凭据无效", nil)
	}
	tx, err := s.beginProfileEditWriteTx(ctx, tenantID, userID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if p, found, err := replayProfileEditTx(ctx, tx, tenantID, userID, idempotencyKey, requestDigest); err != nil {
		return nil, err
	} else if found {
		return p, commitProfileReplay(ctx, tx)
	}

	before := profileEditFields{Tags: []string{}, RemovedTags: []string{}}
	var afterProfile *types.Profile
	if expectedUpdatedAt == nil {
		if _, err := tx.Exec(ctx, `SET LOCAL ROLE vane_profile_claim_editor`); err != nil {
			return nil, profileClaimDBError("enter claim role for initial profile", err)
		}
		afterProfile, err = insertAbsentProfileTx(ctx, tx, tenantID, userID, patch)
		if err == nil {
			err = seedInitialManualProfileClaimsTx(
				ctx, tx, tenantID, userID, afterProfile)
		}
		if _, roleErr := tx.Exec(ctx, `SET LOCAL ROLE vane_profile_editor`); roleErr != nil && err == nil {
			err = profileClaimDBError("restore profile editor after initial profile", roleErr)
		}
		if errors.Is(err, pgx.ErrNoRows) {
			if p, found, replayErr := replayProfileEditTx(ctx, tx, tenantID, userID, idempotencyKey, requestDigest); replayErr != nil {
				return nil, replayErr
			} else if found {
				return p, commitProfileReplay(ctx, tx)
			}
			return nil, types.NewAppError(types.CodeConflict, "画像已经存在，请刷新后重试", nil)
		}
	} else {
		if p, found, replayErr := replayProfileEditTx(ctx, tx, tenantID, userID, idempotencyKey, requestDigest); replayErr != nil {
			return nil, replayErr
		} else if found {
			return p, commitProfileReplay(ctx, tx)
		}
		return nil, types.NewAppError(
			types.CodeConflict,
			"来源级画像 authority 已启用，请通过画像主张纠正",
			nil,
		)
	}
	if err != nil {
		return nil, profileEditDBError("write profile patch", err)
	}
	after := editableFields(afterProfile)
	if before.Exists && equalProfileEditFields(before, after) {
		return nil, types.NewAppError(types.CodeValidation, "画像内容没有变化", nil)
	}
	revisionID, err := insertProfileRevisionTx(ctx, tx, tenantID, userID, "edit", nil, before, after, afterProfile.UpdatedAt)
	if err != nil {
		return nil, err
	}
	response := publicProfile(afterProfile)
	if err := insertProfileReceiptTx(ctx, tx, tenantID, userID, idempotencyKey, requestDigest, revisionID, &response); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, profileEditDBError("commit profile patch", err)
	}
	return &response, nil
}

func (s *Store) UndoProfileEdit(
	ctx context.Context,
	tenantID, userID, targetRevisionID int64,
	expectedUpdatedAt time.Time,
	idempotencyKey, requestDigest string,
) (*types.ProfileView, error) {
	if tenantID <= 0 || userID <= 0 || targetRevisionID <= 0 ||
		idempotencyKey == "" || requestDigest == "" {
		return nil, types.NewAppError(types.CodeValidation, "画像撤销范围或幂等凭据无效", nil)
	}
	tx, err := s.beginProfileEditWriteTx(ctx, tenantID, userID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if p, found, err := replayProfileEditTx(ctx, tx, tenantID, userID, idempotencyKey, requestDigest); err != nil {
		return nil, err
	} else if found {
		return p, commitProfileReplay(ctx, tx)
	}
	return nil, types.NewAppError(
		types.CodeConflict,
		"来源级画像 authority 已启用，旧画像编辑不可撤销；请撤销对应主张事件",
		nil,
	)
}

func (s *Store) ListProfileEdits(
	ctx context.Context, tenantID, userID int64, limit int,
) ([]types.ProfileEditRevision, error) {
	if tenantID <= 0 || userID <= 0 || limit < 1 || limit > 50 {
		return nil, types.NewAppError(types.CodeValidation, "画像历史查询范围无效", nil)
	}
	tx, err := s.beginProfileEditTx(ctx, tenantID, userID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	rows, err := tx.Query(ctx,
		`SELECT id,kind,target_revision_id,before_fields,after_fields,
		        result_updated_at,created_at
		   FROM profile_edit_revisions
		  WHERE tenant_id=$1 AND user_id=$2
		  ORDER BY id DESC LIMIT $3`, tenantID, userID, limit)
	if err != nil {
		return nil, profileEditDBError("list profile revisions", err)
	}
	defer rows.Close()
	var raw []profileEditRevisionRow
	for rows.Next() {
		var r profileEditRevisionRow
		var beforeJSON, afterJSON []byte
		if err := rows.Scan(&r.ID, &r.Kind, &r.TargetRevisionID, &beforeJSON, &afterJSON, &r.ResultUpdatedAt, &r.CreatedAt); err != nil {
			return nil, profileEditDBError("scan profile revision", err)
		}
		if err := json.Unmarshal(beforeJSON, &r.Before); err != nil {
			return nil, types.NewAppError(types.CodeInternal, "画像历史 before 数据损坏", err)
		}
		if err := json.Unmarshal(afterJSON, &r.After); err != nil {
			return nil, types.NewAppError(types.CodeInternal, "画像历史 after 数据损坏", err)
		}
		raw = append(raw, r)
	}
	if err := rows.Err(); err != nil {
		return nil, profileEditDBError("iterate profile revisions", err)
	}
	out := make([]types.ProfileEditRevision, 0, len(raw))
	for _, r := range raw {
		out = append(out, types.ProfileEditRevision{
			ID: strconv.FormatInt(r.ID, 10), CreatedAt: r.CreatedAt,
			Actor: "self", Kind: r.Kind, Changes: publicProfileChanges(r.Before, r.After),
			// Since 062, legacy revisions are audit-only. Recovery is an
			// append-only revoke event in the claim ledger.
			Undoable: false,
		})
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, profileEditDBError("commit profile history read", err)
	}
	return out, nil
}

func (s *Store) GetProfileView(
	ctx context.Context, tenantID, userID int64,
) (*types.ProfileView, error) {
	if tenantID <= 0 || userID <= 0 {
		return nil, types.NewAppError(types.CodeValidation, "画像读取范围无效", nil)
	}
	tx, err := s.beginProfileEditTx(ctx, tenantID, userID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	p, err := locklessProfileTx(ctx, tx, tenantID, userID)
	if err != nil {
		return nil, err
	}
	view := publicProfile(p)
	if err := tx.Commit(ctx); err != nil {
		return nil, profileEditDBError("commit profile read", err)
	}
	return &view, nil
}

func (s *Store) beginProfileEditTx(ctx context.Context, tenantID int64, userIDs ...int64) (pgx.Tx, error) {
	return s.beginProfileEditScopedTx(ctx, tenantID, false, userIDs...)
}

func (s *Store) beginProfileEditWriteTx(
	ctx context.Context, tenantID int64, userIDs ...int64,
) (pgx.Tx, error) {
	return s.beginProfileEditScopedTx(ctx, tenantID, true, userIDs...)
}

func (s *Store) beginProfileEditScopedTx(
	ctx context.Context,
	tenantID int64,
	write bool,
	userIDs ...int64,
) (pgx.Tx, error) {
	if tenantID <= 0 || len(userIDs) != 1 || userIDs[0] <= 0 {
		return nil, types.NewAppError(
			types.CodeValidation, "画像编辑用户范围无效", nil)
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, profileEditDBError("begin profile edit", err)
	}
	// Pin relation resolution before the owner-scoped admission helper reads
	// tenants. Listing pg_temp explicitly last prevents its implicit precedence.
	if _, err := tx.Exec(
		ctx, `SET LOCAL search_path = pg_catalog, public, pg_temp`); err != nil {
		_ = tx.Rollback(ctx)
		return nil, profileEditDBError("pin profile editor search path", err)
	}
	if write {
		// Purge takes this same tenant root before every child row lock. Profile
		// writes take it before receipt/revision/profile access and never lock a
		// schedule or membership row, so compiled schedule -> profile remains
		// acyclic while purge reports cannot miss a late profile edit.
		exists, err := lockTenantAdmissionRoot(ctx, tx, tenantID)
		if err != nil {
			_ = tx.Rollback(ctx)
			return nil, profileEditDBError(
				"lock profile edit tenant admission", err)
		}
		if !exists {
			_ = tx.Rollback(ctx)
			return nil, types.NewAppError(
				types.CodeNotFound, "租户不存在", nil)
		}
	}
	if _, err := tx.Exec(ctx, `SELECT set_config('app.tenant_id',$1,true)`, strconv.FormatInt(tenantID, 10)); err != nil {
		_ = tx.Rollback(ctx)
		return nil, profileEditDBError("set profile edit tenant", err)
	}
	if _, err := tx.Exec(ctx, `SELECT set_config('app.user_id',$1,true)`, strconv.FormatInt(userIDs[0], 10)); err != nil {
		_ = tx.Rollback(ctx)
		return nil, profileEditDBError("set profile edit user", err)
	}
	if _, err := tx.Exec(ctx, `SET LOCAL ROLE vane_profile_editor`); err != nil {
		_ = tx.Rollback(ctx)
		return nil, profileEditDBError("enter profile editor role", err)
	}
	return tx, nil
}

func replayProfileEditTx(ctx context.Context, tx pgx.Tx, tenantID, userID int64, key, digest string) (*types.ProfileView, bool, error) {
	var storedDigest string
	var payload []byte
	err := tx.QueryRow(ctx,
		`SELECT request_digest,response_profile
		   FROM profile_edit_receipts
		  WHERE tenant_id=$1 AND user_id=$2 AND idempotency_key=$3`,
		tenantID, userID, key).Scan(&storedDigest, &payload)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, profileEditDBError("read profile edit receipt", err)
	}
	if storedDigest != digest {
		return nil, false, types.NewAppError(types.CodeConflict, "Idempotency-Key 已用于另一画像请求", nil)
	}
	var p types.ProfileView
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, false, types.NewAppError(types.CodeInternal, "画像修改回执损坏", err)
	}
	return &p, true, nil
}

func commitProfileReplay(ctx context.Context, tx pgx.Tx) error {
	if err := tx.Commit(ctx); err != nil {
		return profileEditDBError("commit profile edit replay", err)
	}
	return nil
}

func insertAbsentProfileTx(ctx context.Context, tx pgx.Tx, tenantID, userID int64, patch types.ProfileEditPatch) (*types.Profile, error) {
	industry, occupation := "", ""
	tags := []string{}
	if patch.Industry != nil {
		industry = *patch.Industry
	}
	if patch.Occupation != nil {
		occupation = *patch.Occupation
	}
	if patch.Tags != nil {
		tags = *patch.Tags
	}
	var p types.Profile
	err := scanProfileEdit(tx.QueryRow(ctx,
		`INSERT INTO profiles (tenant_id,user_id,industry,occupation,tags,updated_at)
		 SELECT $1,$2,$3,$4,$5,clock_timestamp()
		   FROM memberships
		  WHERE tenant_id=$1 AND user_id=$2
		 ON CONFLICT (user_id) DO NOTHING
		 RETURNING `+profileEditColumns,
		tenantID, userID, industry, occupation, tags), &p)
	return &p, err
}

func lockProfileTx(ctx context.Context, tx pgx.Tx, tenantID, userID int64) (*types.Profile, error) {
	var p types.Profile
	err := scanProfileEdit(tx.QueryRow(ctx,
		`SELECT `+profileEditColumns+` FROM profiles
		  WHERE tenant_id=$1 AND user_id=$2 FOR UPDATE`, tenantID, userID), &p)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, types.NewAppError(types.CodeConflict, "画像不存在，请以 expected_updated_at:null 创建", nil)
	}
	if err != nil {
		return nil, profileEditDBError("lock profile", err)
	}
	return &p, nil
}

func locklessProfileTx(ctx context.Context, tx pgx.Tx, tenantID, userID int64) (*types.Profile, error) {
	var p types.Profile
	err := scanProfileEdit(tx.QueryRow(ctx,
		`SELECT `+profileEditColumns+` FROM profiles WHERE tenant_id=$1 AND user_id=$2`,
		tenantID, userID), &p)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, types.NewAppError(types.CodeNotFound, "画像不存在", nil)
	}
	if err != nil {
		return nil, profileEditDBError("read profile", err)
	}
	return &p, nil
}

func updateProfileTx(ctx context.Context, tx pgx.Tx, tenantID, userID int64, expected time.Time, patch types.ProfileEditPatch) (*types.Profile, error) {
	var p types.Profile
	err := scanProfileEdit(tx.QueryRow(ctx,
		`UPDATE profiles
		    SET industry=COALESCE($4,industry),
		        occupation=COALESCE($5,occupation),
		        tags=COALESCE($6::text[],tags),
		        removed_tags=CASE WHEN $6::text[] IS NULL THEN removed_tags ELSE
		            (SELECT COALESCE(array_agg(t ORDER BY t),'{}'::text[]) FROM (
		                SELECT unnest(removed_tags || tags) AS t
		                EXCEPT SELECT unnest($6::text[])
		            ) diff(t))
		        END,
		        updated_at=GREATEST(clock_timestamp(),updated_at+interval '1 microsecond')
		  WHERE tenant_id=$1 AND user_id=$2 AND updated_at=$3
		  RETURNING `+profileEditColumns,
		tenantID, userID, expected, patch.Industry, patch.Occupation, patch.Tags), &p)
	return &p, err
}

func insertProfileRevisionTx(ctx context.Context, tx pgx.Tx, tenantID, userID int64, kind string, target *int64, before, after profileEditFields, updatedAt time.Time) (int64, error) {
	beforeJSON, _ := json.Marshal(before)
	afterJSON, _ := json.Marshal(after)
	var id int64
	err := tx.QueryRow(ctx,
		`INSERT INTO profile_edit_revisions
		    (tenant_id,user_id,actor_user_id,kind,target_revision_id,
		     before_fields,after_fields,result_updated_at)
		 VALUES ($1,$2,$2,$3,$4,$5,$6,$7) RETURNING id`,
		tenantID, userID, kind, target, beforeJSON, afterJSON, updatedAt).Scan(&id)
	if err != nil {
		return 0, profileEditDBError("append profile revision", err)
	}
	return id, nil
}

func insertProfileReceiptTx(ctx context.Context, tx pgx.Tx, tenantID, userID int64, key, digest string, revisionID int64, p *types.ProfileView) error {
	payload, err := json.Marshal(p)
	if err != nil {
		return types.NewAppError(types.CodeInternal, "编码画像修改回执失败", err)
	}
	_, err = tx.Exec(ctx,
		`INSERT INTO profile_edit_receipts
		    (tenant_id,user_id,idempotency_key,request_digest,revision_id,response_profile)
		 VALUES ($1,$2,$3,$4,$5,$6)`,
		tenantID, userID, key, digest, revisionID, payload)
	if err != nil {
		return profileEditDBError("write profile edit receipt", err)
	}
	return nil
}

func latestProfileRevisionTx(ctx context.Context, tx pgx.Tx, tenantID, userID int64) (profileEditRevisionRow, error) {
	var r profileEditRevisionRow
	var beforeJSON, afterJSON []byte
	err := tx.QueryRow(ctx,
		`SELECT id,kind,target_revision_id,before_fields,after_fields,
		        result_updated_at,created_at
		   FROM profile_edit_revisions
		  WHERE tenant_id=$1 AND user_id=$2
		  ORDER BY id DESC LIMIT 1`,
		tenantID, userID).Scan(&r.ID, &r.Kind, &r.TargetRevisionID,
		&beforeJSON, &afterJSON, &r.ResultUpdatedAt, &r.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return r, types.NewAppError(types.CodeConflict, "没有可撤销的画像编辑", nil)
	}
	if err != nil {
		return r, profileEditDBError("read latest profile revision", err)
	}
	if err := json.Unmarshal(beforeJSON, &r.Before); err != nil {
		return r, types.NewAppError(types.CodeInternal, "画像历史 before 数据损坏", err)
	}
	if err := json.Unmarshal(afterJSON, &r.After); err != nil {
		return r, types.NewAppError(types.CodeInternal, "画像历史 after 数据损坏", err)
	}
	return r, nil
}

func profileRevisionByIDTx(
	ctx context.Context, tx pgx.Tx, tenantID, userID, revisionID int64,
) (profileEditRevisionRow, error) {
	var r profileEditRevisionRow
	var beforeJSON, afterJSON []byte
	err := tx.QueryRow(ctx,
		`SELECT id,kind,target_revision_id,before_fields,after_fields,
		        result_updated_at,created_at
		   FROM profile_edit_revisions
		  WHERE tenant_id=$1 AND user_id=$2 AND id=$3`,
		tenantID, userID, revisionID).Scan(
		&r.ID, &r.Kind, &r.TargetRevisionID, &beforeJSON, &afterJSON,
		&r.ResultUpdatedAt, &r.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return r, types.NewAppError(
			types.CodeNotFound, "画像编辑记录不存在", nil)
	}
	if err != nil {
		return r, profileEditDBError("read profile revision", err)
	}
	if err := json.Unmarshal(beforeJSON, &r.Before); err != nil {
		return r, types.NewAppError(types.CodeInternal, "画像历史 before 数据损坏", err)
	}
	if err := json.Unmarshal(afterJSON, &r.After); err != nil {
		return r, types.NewAppError(types.CodeInternal, "画像历史 after 数据损坏", err)
	}
	return r, nil
}

func editableFields(p *types.Profile) profileEditFields {
	return profileEditFields{
		Exists: true, Industry: p.Industry, Occupation: p.Occupation,
		Tags:        append([]string{}, p.Tags...),
		RemovedTags: append([]string{}, p.RemovedTags...),
	}
}

func publicProfile(p *types.Profile) types.ProfileView {
	return types.ProfileView{
		Industry: p.Industry, Occupation: p.Occupation,
		Tags:        append([]string{}, p.Tags...),
		RemovedTags: append([]string{}, p.RemovedTags...), Summary: p.Summary,
		CreatedAt: p.CreatedAt, UpdatedAt: p.UpdatedAt,
	}
}

func equalProfileEditFields(a, b profileEditFields) bool {
	aj, _ := json.Marshal(a)
	bj, _ := json.Marshal(b)
	return string(aj) == string(bj)
}

func publicProfileChanges(before, after profileEditFields) []types.ProfileEditChange {
	changes := make([]types.ProfileEditChange, 0, 3)
	if (!before.Exists && after.Industry != "") ||
		(before.Exists && before.Industry != after.Industry) {
		var old any = ""
		if before.Exists {
			old = before.Industry
		}
		changes = append(changes, types.ProfileEditChange{Field: "industry", Before: old, After: after.Industry})
	}
	if (!before.Exists && after.Occupation != "") ||
		(before.Exists && before.Occupation != after.Occupation) {
		var old any = ""
		if before.Exists {
			old = before.Occupation
		}
		changes = append(changes, types.ProfileEditChange{Field: "occupation", Before: old, After: after.Occupation})
	}
	beforeTags, _ := json.Marshal(before.Tags)
	afterTags, _ := json.Marshal(after.Tags)
	if (!before.Exists && len(after.Tags) > 0) ||
		(before.Exists && string(beforeTags) != string(afterTags)) {
		var old any = []string{}
		if before.Exists {
			old = before.Tags
		}
		changes = append(changes, types.ProfileEditChange{Field: "tags", Before: old, After: after.Tags})
	}
	return changes
}

func profileEditDBError(action string, err error) error {
	return types.NewAppError(
		types.CodeDatabase, "画像操作暂时失败，请稍后重试",
		fmt.Errorf("%s: %w", action, err))
}
