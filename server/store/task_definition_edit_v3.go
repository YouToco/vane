package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/server/internal/strictjson"
	"github.com/YouToco/vane/server/taskstate"
	"github.com/YouToco/vane/server/types"
)

const researchTaskDefinitionEditProposalV3 = "vane.research-task-definition-edit-proposal/v3"

const (
	preparedResearchTaskScheduleV3StoreWire = "vane.prepared-research-task-schedule/v3"
	preparedResearchTaskEditV3StoreWire     = "vane.prepared-research-task-definition-edit/v3"
	researchTaskEditSnapshotV3StoreWire     = "vane.research-task-definition-edit-snapshot/v3"
)

type preparedResearchTaskScheduleV3StoreView struct {
	WireVersion string          `json:"wire_version"`
	Schedule    json.RawMessage `json:"schedule"`
	Input       struct {
		TenantID                 int64  `json:"tenant_id"`
		UserID                   int64  `json:"user_id"`
		TaskID                   string `json:"task_id"`
		ActionAuthorizationToken string `json:"action_authorization_token"`
	} `json:"input"`
	TargetAction              []byte `json:"target_action"`
	TargetActionDigest        string `json:"target_action_digest"`
	ActionAuthorizationDigest string `json:"action_authorization_digest"`
}

type preparedResearchTaskScheduleIdentityV3StoreView struct {
	TaskID         string `json:"task_id"`
	TenantID       int64  `json:"tenant_id"`
	UserID         int64  `json:"user_id"`
	OperationID    string `json:"operation_id"`
	PreparedDigest string `json:"prepared_digest"`
}

type preparedResearchTaskEditV3StoreView struct {
	WireVersion             string          `json:"wire_version"`
	OperationID             string          `json:"operation_id"`
	OriginalState           string          `json:"original_state"`
	BaseDefinitionVersion   int64           `json:"base_definition_version"`
	BaseDefinitionDigest    string          `json:"base_definition_digest"`
	TargetDefinitionVersion int64           `json:"target_definition_version"`
	TargetDefinitionDigest  string          `json:"target_definition_digest"`
	Base                    json.RawMessage `json:"base"`
	Target                  json.RawMessage `json:"target"`
	RequestDigest           string          `json:"request_digest"`
}

type researchTaskEditSnapshotV3StoreView struct {
	WireVersion      string `json:"wire_version"`
	Phase            string `json:"phase"`
	TaskID           string `json:"task_id"`
	DefinitionDigest string `json:"definition_digest"`
	Revision         string `json:"revision"`
	Paused           bool   `json:"paused"`
}

func decodePreparedResearchTaskScheduleV3Store(
	raw []byte,
) (preparedResearchTaskScheduleV3StoreView,
	preparedResearchTaskScheduleIdentityV3StoreView, error) {
	var wire preparedResearchTaskScheduleV3StoreView
	if len(raw) == 0 || len(raw) > 1<<20 ||
		strictjson.DecodeExact(raw, &wire) != nil ||
		wire.WireVersion != preparedResearchTaskScheduleV3StoreWire {
		return wire, preparedResearchTaskScheduleIdentityV3StoreView{},
			taskDefinitionEditIntegrity()
	}
	canonical, err := json.Marshal(wire)
	if err != nil || !bytes.Equal(canonical, raw) {
		return wire, preparedResearchTaskScheduleIdentityV3StoreView{},
			taskDefinitionEditIntegrity()
	}
	var schedule preparedResearchTaskScheduleIdentityV3StoreView
	if json.Unmarshal(wire.Schedule, &schedule) != nil || schedule.TenantID <= 0 ||
		schedule.UserID <= 0 || schedule.TaskID == "" || schedule.OperationID == "" ||
		!validTaskStateDigest(schedule.PreparedDigest) ||
		wire.Input.TenantID != schedule.TenantID || wire.Input.UserID != schedule.UserID ||
		wire.Input.TaskID != schedule.TaskID || len(wire.TargetAction) == 0 ||
		!validTaskStateDigest(wire.TargetActionDigest) ||
		!validTaskStateDigest(wire.ActionAuthorizationDigest) ||
		len(wire.Input.ActionAuthorizationToken) != sha256.Size*2 ||
		strings.ToLower(wire.Input.ActionAuthorizationToken) !=
			wire.Input.ActionAuthorizationToken {
		return wire, schedule, taskDefinitionEditIntegrity()
	}
	actionSum := sha256.Sum256(wire.TargetAction)
	authorizationSum := sha256.Sum256(
		[]byte(wire.Input.ActionAuthorizationToken))
	if hex.EncodeToString(actionSum[:]) != wire.TargetActionDigest ||
		hex.EncodeToString(authorizationSum[:]) != wire.ActionAuthorizationDigest {
		return wire, schedule, taskDefinitionEditIntegrity()
	}
	return wire, schedule, nil
}

func decodePreparedResearchTaskEditV3Store(
	raw []byte,
) (preparedResearchTaskEditV3StoreView,
	preparedResearchTaskScheduleV3StoreView,
	preparedResearchTaskScheduleIdentityV3StoreView,
	preparedResearchTaskScheduleV3StoreView,
	preparedResearchTaskScheduleIdentityV3StoreView, error) {
	var wire preparedResearchTaskEditV3StoreView
	if len(raw) == 0 || len(raw) > 4<<20 ||
		strictjson.DecodeExact(raw, &wire) != nil ||
		wire.WireVersion != preparedResearchTaskEditV3StoreWire ||
		wire.OperationID == "" || wire.BaseDefinitionVersion <= 0 ||
		wire.TargetDefinitionVersion != wire.BaseDefinitionVersion+1 ||
		!validTaskStateDigest(wire.BaseDefinitionDigest) ||
		!validTaskStateDigest(wire.TargetDefinitionDigest) ||
		wire.BaseDefinitionDigest == wire.TargetDefinitionDigest ||
		!validTaskStateDigest(wire.RequestDigest) ||
		(wire.OriginalState != "active" && wire.OriginalState != "paused") {
		return wire, preparedResearchTaskScheduleV3StoreView{},
			preparedResearchTaskScheduleIdentityV3StoreView{},
			preparedResearchTaskScheduleV3StoreView{},
			preparedResearchTaskScheduleIdentityV3StoreView{},
			taskDefinitionEditIntegrity()
	}
	canonical, err := json.Marshal(wire)
	if err != nil || !bytes.Equal(canonical, raw) {
		return wire, preparedResearchTaskScheduleV3StoreView{},
			preparedResearchTaskScheduleIdentityV3StoreView{},
			preparedResearchTaskScheduleV3StoreView{},
			preparedResearchTaskScheduleIdentityV3StoreView{},
			taskDefinitionEditIntegrity()
	}
	wantDigest := wire.RequestDigest
	wire.RequestDigest = ""
	withoutDigest, err := json.Marshal(wire)
	if err != nil || sha256HexTaskDefinitionEdit(withoutDigest) != wantDigest {
		return wire, preparedResearchTaskScheduleV3StoreView{},
			preparedResearchTaskScheduleIdentityV3StoreView{},
			preparedResearchTaskScheduleV3StoreView{},
			preparedResearchTaskScheduleIdentityV3StoreView{},
			taskDefinitionEditIntegrity()
	}
	wire.RequestDigest = wantDigest
	base, baseIdentity, err := decodePreparedResearchTaskScheduleV3Store(wire.Base)
	if err != nil {
		return wire, base, baseIdentity, preparedResearchTaskScheduleV3StoreView{},
			preparedResearchTaskScheduleIdentityV3StoreView{}, err
	}
	target, targetIdentity, err := decodePreparedResearchTaskScheduleV3Store(wire.Target)
	if err != nil || baseIdentity.TenantID != targetIdentity.TenantID ||
		baseIdentity.UserID != targetIdentity.UserID ||
		baseIdentity.TaskID != targetIdentity.TaskID ||
		baseIdentity.OperationID != targetIdentity.OperationID ||
		baseIdentity.PreparedDigest != wire.BaseDefinitionDigest ||
		targetIdentity.PreparedDigest != wire.TargetDefinitionDigest ||
		base.Input.ActionAuthorizationToken != target.Input.ActionAuthorizationToken ||
		base.ActionAuthorizationDigest != target.ActionAuthorizationDigest {
		return wire, base, baseIdentity, target, targetIdentity,
			taskDefinitionEditIntegrity()
	}
	return wire, base, baseIdentity, target, targetIdentity, nil
}

func decodeResearchTaskEditSnapshotV3Store(
	raw []byte,
	prepared preparedResearchTaskEditV3StoreView,
	taskID string,
) (researchTaskEditSnapshotV3StoreView, error) {
	var snapshot researchTaskEditSnapshotV3StoreView
	if len(raw) == 0 || len(raw) > 16<<10 ||
		strictjson.DecodeExact(raw, &snapshot) != nil ||
		snapshot.WireVersion != researchTaskEditSnapshotV3StoreWire ||
		snapshot.TaskID != taskID || snapshot.Revision == "" {
		return snapshot, taskDefinitionEditIntegrity()
	}
	canonical, err := json.Marshal(snapshot)
	if err != nil || !bytes.Equal(canonical, raw) {
		return snapshot, taskDefinitionEditIntegrity()
	}
	var digest string
	var paused bool
	switch snapshot.Phase {
	case "base_original":
		digest = prepared.BaseDefinitionDigest
		paused = prepared.OriginalState == "paused"
	case "base_paused":
		digest, paused = prepared.BaseDefinitionDigest, true
	case "target_paused":
		digest, paused = prepared.TargetDefinitionDigest, true
	case "target_final":
		digest = prepared.TargetDefinitionDigest
		paused = prepared.OriginalState == "paused"
	default:
		return snapshot, taskDefinitionEditIntegrity()
	}
	if snapshot.DefinitionDigest != digest || snapshot.Paused != paused {
		return snapshot, taskDefinitionEditIntegrity()
	}
	return snapshot, nil
}

type researchTaskDefinitionEditProposalV3Wire struct {
	WireVersion             string `json:"wire_version"`
	OperationID             string `json:"operation_id"`
	TenantID                int64  `json:"tenant_id"`
	UserID                  int64  `json:"user_id"`
	TaskID                  string `json:"task_id"`
	SessionID               int64  `json:"session_id"`
	OriginalStatus          string `json:"original_status"`
	BaseDefinitionVersion   int64  `json:"base_definition_version"`
	BaseDefinitionDigest    string `json:"base_definition_digest"`
	TargetDefinitionVersion int64  `json:"target_definition_version"`
	TargetDefinitionDigest  string `json:"target_definition_digest"`
	PreparedEditDigest      string `json:"prepared_edit_digest"`
	BaseSnapshotDigest      string `json:"base_snapshot_digest"`
}

func (s *Store) beginResearchTaskDefinitionEditTxV3(
	ctx context.Context,
	tenantID, userID int64,
) (pgx.Tx, error) {
	if tenantID <= 0 || userID <= 0 {
		return nil, taskDefinitionEditValidation("native V3 edit scope is invalid")
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, taskDefinitionEditDatabaseError("begin native V3 edit", err)
	}
	for key, value := range map[string]string{
		"app.tenant_id": strconv.FormatInt(tenantID, 10),
		"app.user_id":   strconv.FormatInt(userID, 10),
	} {
		if _, err := tx.Exec(ctx, `SELECT set_config($1,$2,true)`, key, value); err != nil {
			rollbackTaskDefinitionEditTx(ctx, tx)
			return nil, taskDefinitionEditDatabaseError("set native V3 edit scope", err)
		}
	}
	return tx, nil
}

// LoadResearchTaskDefinitionEditBasisV3 returns only an exact native V3 head
// owned by the authenticated owner. It also recovers the current prepared
// Schedule from the successful native creation or latest completed V3 edit.
func (s *Store) LoadResearchTaskDefinitionEditBasisV3(
	ctx context.Context,
	tenantID, userID int64,
	taskID string,
) (*types.ResearchTaskDefinitionEditBasisV3, error) {
	if tenantID <= 0 || userID <= 0 ||
		!validTaskDefinitionEditReference(taskID, 255) {
		return nil, taskDefinitionEditValidation("native V3 edit basis scope is invalid")
	}
	tx, err := s.beginResearchTaskDefinitionEditTxV3(ctx, tenantID, userID)
	if err != nil {
		return nil, err
	}
	defer rollbackTaskDefinitionEditTx(ctx, tx)
	if err := lockTaskScheduleMutation(ctx, tx, taskID); err != nil {
		return nil, err
	}
	basis, err := loadResearchTaskDefinitionEditBasisV3Tx(
		ctx, tx, tenantID, userID, taskID, true)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, taskDefinitionEditDatabaseError("commit native V3 edit basis", err)
	}
	return basis, nil
}

func loadResearchTaskDefinitionEditBasisV3Tx(
	ctx context.Context,
	tx pgx.Tx,
	tenantID, userID int64,
	taskID string,
	lock bool,
) (*types.ResearchTaskDefinitionEditBasisV3, error) {
	_ = lock
	var status string
	var version, generation int64
	var digest, targetActionDigest, authorizationDigest string
	var payload []byte
	var scheduleName string
	var scheduleSpec []byte
	var manual string
	var provenanceKind string
	var provenance []byte
	err := tx.QueryRow(ctx, `
		SELECT * FROM load_native_research_v3_edit_basis_v1($1,$2,$3)`,
		tenantID, userID, taskID).Scan(
		&status, &version, &digest, &payload, &scheduleName, &scheduleSpec,
		&manual, &generation, &targetActionDigest, &authorizationDigest,
		&provenanceKind, &provenance)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, taskDefinitionEditNotFound()
	}
	if err != nil {
		return nil, taskDefinitionEditDatabaseError("load native V3 edit head", err)
	}
	definition, err := taskstate.DecodeApprovedDefinitionV3(payload)
	if err != nil {
		return nil, taskDefinitionEditIntegrity()
	}
	canonical, err := taskstate.EncodeApprovedDefinitionV3(definition)
	if err != nil || !bytes.Equal(canonical, payload) ||
		definition.TenantID != tenantID || definition.UserID != userID ||
		definition.TaskID != taskID || definition.TaskName != scheduleName ||
		definition.TaskManual != manual || !jsonBytesSemanticEqual(definition.SpecJSON, scheduleSpec) {
		return nil, taskDefinitionEditIntegrity()
	}
	wantDigest, err := taskstate.DigestApprovedDefinitionV3(definition)
	if err != nil || wantDigest != digest {
		return nil, taskDefinitionEditIntegrity()
	}
	preparedBytes := bytes.Clone(provenance)
	if provenanceKind == "edit" {
		preparedEdit, _, _, _, _, decodeErr :=
			decodePreparedResearchTaskEditV3Store(provenance)
		if decodeErr != nil || preparedEdit.TargetDefinitionVersion != version ||
			preparedEdit.TargetDefinitionDigest != digest {
			return nil, taskDefinitionEditIntegrity()
		}
		preparedBytes = bytes.Clone(preparedEdit.Target)
	} else if provenanceKind != "creation" || version != 1 {
		return nil, taskDefinitionEditIntegrity()
	}
	prepared, preparedIdentity, err := decodePreparedResearchTaskScheduleV3Store(
		preparedBytes)
	if err != nil || preparedIdentity.TenantID != tenantID ||
		preparedIdentity.UserID != userID || preparedIdentity.TaskID != taskID ||
		preparedIdentity.PreparedDigest != digest ||
		prepared.TargetActionDigest != targetActionDigest ||
		prepared.ActionAuthorizationDigest != authorizationDigest {
		return nil, taskDefinitionEditIntegrity()
	}
	return &types.ResearchTaskDefinitionEditBasisV3{
		TenantID: tenantID, UserID: userID, TaskID: taskID,
		Status: types.ScheduleStatus(status), DefinitionVersion: version,
		DefinitionDigest: digest, DefinitionPayload: bytes.Clone(payload),
		PreparedSchedule:    bytes.Clone(preparedBytes),
		AuthorityGeneration: generation, TargetActionDigest: targetActionDigest,
		ActionAuthorizationDigest: authorizationDigest,
	}, nil
}

func jsonBytesSemanticEqual(left, right []byte) bool {
	var a, b any
	return json.Unmarshal(left, &a) == nil && json.Unmarshal(right, &b) == nil &&
		reflect.DeepEqual(a, b)
}

// CreateResearchTaskDefinitionEditOperationV3 seals a native V3 edit against
// the exact current owner/head/authority/provenance snapshot.
func (s *Store) CreateResearchTaskDefinitionEditOperationV3(
	ctx context.Context,
	p types.CreateResearchTaskDefinitionEditOperationV3Params,
) (*types.TaskDefinitionEditOperation, error) {
	if !validTaskDefinitionEditReference(p.ID, 255) || p.TenantID <= 0 ||
		p.UserID <= 0 || !validTaskDefinitionEditReference(p.TaskID, 255) ||
		p.SessionID <= 0 || p.ExpiresAt.IsZero() || p.BaseVersion <= 0 ||
		p.TargetVersion != p.BaseVersion+1 {
		return nil, taskDefinitionEditValidation("native V3 edit proposal is incomplete")
	}
	base, err := taskstate.DecodeApprovedDefinitionV3(p.BaseDefinition)
	if err != nil {
		return nil, taskDefinitionEditValidation("native V3 edit base is invalid")
	}
	target, err := taskstate.DecodeApprovedDefinitionV3(p.TargetDefinition)
	if err != nil {
		return nil, taskDefinitionEditValidation("native V3 edit target is invalid")
	}
	baseBytes, baseDigest, err := canonicalResearchDefinitionV3(base)
	if err != nil || !bytes.Equal(baseBytes, p.BaseDefinition) {
		return nil, taskDefinitionEditValidation("native V3 edit base is not canonical")
	}
	targetBytes, targetDigest, err := canonicalResearchDefinitionV3(target)
	if err != nil || !bytes.Equal(targetBytes, p.TargetDefinition) {
		return nil, taskDefinitionEditValidation("native V3 edit target is not canonical")
	}
	for _, definition := range []taskstate.ApprovedDefinitionV3{base, target} {
		if definition.TenantID != p.TenantID || definition.UserID != p.UserID ||
			definition.TaskID != p.TaskID {
			return nil, taskDefinitionEditValidation("native V3 edit definition scope differs")
		}
	}
	prepared, preparedBase, preparedBaseIdentity,
		_, _, err := decodePreparedResearchTaskEditV3Store(p.PreparedEdit)
	if err != nil || prepared.OperationID != p.ID ||
		prepared.BaseDefinitionVersion != p.BaseVersion ||
		prepared.BaseDefinitionDigest != baseDigest ||
		prepared.TargetDefinitionVersion != p.TargetVersion ||
		prepared.TargetDefinitionDigest != targetDigest ||
		preparedBaseIdentity.TaskID != p.TaskID {
		return nil, taskDefinitionEditValidation("native V3 prepared edit differs")
	}
	baseSnapshot, err := decodeResearchTaskEditSnapshotV3Store(
		p.BaseSnapshot, prepared, p.TaskID)
	if err != nil || baseSnapshot.Phase != "base_original" {
		return nil, taskDefinitionEditValidation("native V3 base snapshot differs")
	}
	originalStatus := types.ScheduleStatusActive
	if prepared.OriginalState == "paused" {
		originalStatus = types.ScheduleStatusPaused
	}
	preparedDigest := sha256HexTaskDefinitionEdit(p.PreparedEdit)
	snapshotDigest := sha256HexTaskDefinitionEdit(p.BaseSnapshot)
	proposal := researchTaskDefinitionEditProposalV3Wire{
		WireVersion: researchTaskDefinitionEditProposalV3,
		OperationID: p.ID, TenantID: p.TenantID, UserID: p.UserID,
		TaskID: p.TaskID, SessionID: p.SessionID,
		OriginalStatus:        string(originalStatus),
		BaseDefinitionVersion: p.BaseVersion, BaseDefinitionDigest: baseDigest,
		TargetDefinitionVersion: p.TargetVersion, TargetDefinitionDigest: targetDigest,
		PreparedEditDigest: preparedDigest, BaseSnapshotDigest: snapshotDigest,
	}
	proposalBytes, err := json.Marshal(proposal)
	if err != nil {
		return nil, taskDefinitionEditIntegrity()
	}
	proposalDigest := sha256HexTaskDefinitionEdit(proposalBytes)
	tx, err := s.beginResearchTaskDefinitionEditTxV3(ctx, p.TenantID, p.UserID)
	if err != nil {
		return nil, err
	}
	defer rollbackTaskDefinitionEditTx(ctx, tx)
	if err := lockTaskScheduleMutation(ctx, tx, p.TaskID); err != nil {
		return nil, err
	}
	basis, err := loadResearchTaskDefinitionEditBasisV3Tx(
		ctx, tx, p.TenantID, p.UserID, p.TaskID, true)
	if err != nil {
		return nil, err
	}
	basePrepared := bytes.Clone(prepared.Base)
	if basis.Status != originalStatus ||
		basis.DefinitionVersion != p.BaseVersion || basis.DefinitionDigest != baseDigest ||
		!bytes.Equal(basis.DefinitionPayload, p.BaseDefinition) ||
		!bytes.Equal(basis.PreparedSchedule, basePrepared) ||
		basis.TargetActionDigest != preparedBase.TargetActionDigest ||
		basis.ActionAuthorizationDigest != preparedBase.ActionAuthorizationDigest {
		return nil, taskDefinitionEditConflict("native V3 edit base changed")
	}
	expiresAt := p.ExpiresAt.UTC().Truncate(time.Microsecond)
	var op types.TaskDefinitionEditOperation
	err = scanTaskDefinitionEditOperation(tx.QueryRow(ctx, `
		SELECT `+taskDefinitionEditOperationColumns+`
		  FROM seal_native_research_v3_edit_v1(
		   $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,
		   $19,$20,$21,$22)`,
		p.ID, p.TenantID, p.UserID, p.TaskID, p.SessionID, expiresAt,
		originalStatus, p.BaseVersion, baseDigest, p.BaseDefinition,
		p.TargetVersion, targetDigest, p.TargetDefinition, proposalBytes,
		proposalDigest, p.PreparedEdit, preparedDigest, p.BaseSnapshot,
		snapshotDigest, basePrepared, preparedBase.TargetActionDigest,
		preparedBase.ActionAuthorizationDigest), &op)
	if err != nil {
		return nil, taskDefinitionEditDatabaseError("create native V3 edit operation", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, taskDefinitionEditDatabaseError("commit native V3 edit operation", err)
	}
	return cloneTaskDefinitionEditOperation(&op), nil
}

func canonicalResearchDefinitionV3(
	definition taskstate.ApprovedDefinitionV3,
) ([]byte, string, error) {
	payload, err := taskstate.EncodeApprovedDefinitionV3(definition)
	if err != nil {
		return nil, "", err
	}
	sum := sha256.Sum256(payload)
	return payload, hex.EncodeToString(sum[:]), nil
}

func (s *Store) LoadResearchTaskDefinitionEditOperationV3(
	ctx context.Context,
	scope types.TaskDefinitionEditScope,
) (*types.TaskDefinitionEditOperation, error) {
	return s.loadResearchTaskDefinitionEditOperationV3(ctx, scope)
}

func (s *Store) loadResearchTaskDefinitionEditOperationV3(
	ctx context.Context,
	scope types.TaskDefinitionEditScope,
) (*types.TaskDefinitionEditOperation, error) {
	if !validTaskDefinitionEditScope(scope) {
		return nil, taskDefinitionEditValidation("native V3 edit scope is invalid")
	}
	tx, err := s.beginResearchTaskDefinitionEditTxV3(
		ctx, scope.TenantID, scope.UserID)
	if err != nil {
		return nil, err
	}
	defer rollbackTaskDefinitionEditTx(ctx, tx)
	var op types.TaskDefinitionEditOperation
	err = scanTaskDefinitionEditOperation(tx.QueryRow(ctx, `SELECT `+
		taskDefinitionEditOperationColumns+`
		 FROM load_native_research_v3_edit_operation_v1($1,$2,$3,$4)`,
		scope.ID, scope.TenantID, scope.UserID, scope.TaskID), &op)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, taskDefinitionEditNotFound()
	}
	if err != nil {
		return nil, taskDefinitionEditDatabaseError("load native V3 edit operation", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, taskDefinitionEditDatabaseError("commit native V3 edit load", err)
	}
	return cloneTaskDefinitionEditOperation(&op), nil
}

// AcquireResearchTaskDefinitionEditOperationV3 starts or takes over the exact
// native V3 operation through the protocol-specific capability boundary.
func (s *Store) AcquireResearchTaskDefinitionEditOperationV3(
	ctx context.Context,
	p types.AcquireTaskDefinitionEditOperationParams,
) (*types.TaskDefinitionEditOperation, error) {
	if err := validateTaskDefinitionEditAcquire(p); err != nil {
		return nil, err
	}
	if p.Scope.TargetTenantID != p.Scope.TenantID ||
		p.Scope.TargetUserID != p.Scope.UserID {
		return nil, taskDefinitionEditValidation("native V3 edit target scope differs")
	}
	tx, err := s.beginResearchTaskDefinitionEditTxV3(
		ctx, p.Scope.TenantID, p.Scope.UserID)
	if err != nil {
		return nil, err
	}
	defer rollbackTaskDefinitionEditTx(ctx, tx)
	var op types.TaskDefinitionEditOperation
	err = scanTaskDefinitionEditOperation(tx.QueryRow(ctx, `SELECT `+
		taskDefinitionEditOperationColumns+`
		 FROM acquire_native_research_v3_edit_v1($1,$2,$3,$4,$5,$6,$7,$8)`,
		p.Scope.ID, p.Scope.TenantID, p.Scope.UserID, p.Scope.TaskID,
		p.LeaseOwner, p.LeaseDuration.Microseconds(), p.ReceiptProvider,
		p.ReceiptTarget), &op)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, taskDefinitionEditNotFound()
	}
	if err != nil {
		return nil, taskDefinitionEditDatabaseError("acquire native V3 edit", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, taskDefinitionEditDatabaseError("commit native V3 edit acquisition", err)
	}
	copy := cloneTaskDefinitionEditOperation(&op)
	if taskDefinitionEditOperationIsTerminal(op.Status) || op.TombstonedAt != nil {
		return copy, taskDefinitionEditTerminal()
	}
	if op.Status != types.TaskDefinitionEditOperationStatusExecuting {
		return copy, taskDefinitionEditIntegrity()
	}
	if op.LeaseOwner != p.LeaseOwner {
		return copy, taskDefinitionEditBusy()
	}
	return copy, nil
}

func decodeResearchTaskDefinitionEditOperationV3(
	op *types.TaskDefinitionEditOperation,
) (preparedResearchTaskEditV3StoreView,
	preparedResearchTaskScheduleV3StoreView,
	preparedResearchTaskScheduleV3StoreView, error) {
	if op == nil || op.Protocol != types.TaskDefinitionEditProtocolResearchV3 {
		return preparedResearchTaskEditV3StoreView{},
			preparedResearchTaskScheduleV3StoreView{},
			preparedResearchTaskScheduleV3StoreView{}, taskDefinitionEditIntegrity()
	}
	prepared, base, baseIdentity, target, targetIdentity, err :=
		decodePreparedResearchTaskEditV3Store(op.PreparedEdit)
	if err != nil || prepared.OperationID != op.ID ||
		prepared.BaseDefinitionVersion != op.BaseDefinitionVersion ||
		prepared.BaseDefinitionDigest != op.BaseDefinitionDigest ||
		prepared.TargetDefinitionVersion != op.TargetDefinitionVersion ||
		prepared.TargetDefinitionDigest != op.TargetDefinitionDigest ||
		baseIdentity.TenantID != op.TenantID || baseIdentity.UserID != op.UserID ||
		baseIdentity.TaskID != op.TaskID || targetIdentity.TenantID != op.TenantID ||
		targetIdentity.UserID != op.UserID || targetIdentity.TaskID != op.TaskID ||
		sha256HexTaskDefinitionEdit(op.PreparedEdit) != op.PreparedEditDigest {
		return prepared, base, target, taskDefinitionEditIntegrity()
	}
	return prepared, base, target, nil
}

func (s *Store) QuiesceResearchTaskDefinitionEditV3(
	ctx context.Context, lease types.TaskDefinitionEditLease,
) error {
	if err := validateTaskDefinitionEditLease(lease); err != nil {
		return err
	}
	op, err := s.loadResearchTaskDefinitionEditOperationV3(ctx,
		types.TaskDefinitionEditScope{ID: lease.ID, TenantID: lease.TenantID,
			UserID: lease.UserID, TargetTenantID: lease.TargetTenantID,
			TargetUserID: lease.TargetUserID, TaskID: lease.TaskID})
	if err != nil {
		return err
	}
	_, base, _, err := decodeResearchTaskDefinitionEditOperationV3(op)
	if err != nil {
		return err
	}
	tx, err := s.beginResearchTaskDefinitionEditTxV3(
		ctx, lease.TenantID, lease.UserID)
	if err != nil {
		return err
	}
	defer rollbackTaskDefinitionEditTx(ctx, tx)
	_, err = tx.Exec(ctx, `SELECT quiesce_native_research_v3_edit_v1(
		$1,$2,$3,$4,$5,$6,$7,$8)`, lease.ID, lease.TenantID, lease.UserID,
		lease.TaskID, lease.LeaseOwner, lease.Fence, base.TargetActionDigest,
		base.ActionAuthorizationDigest)
	if err != nil {
		return taskDefinitionEditDatabaseError("quiesce native V3 edit", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return taskDefinitionEditDatabaseError("commit native V3 quiesce", err)
	}
	return nil
}

func (s *Store) AuthorizeResearchTaskDefinitionEditRemotePhaseV3(
	ctx context.Context, lease types.TaskDefinitionEditLease,
	expectedPhase types.TaskDefinitionEditPhase,
) (*types.TaskDefinitionEditOperation, error) {
	switch expectedPhase {
	case types.TaskDefinitionEditPhaseDBQuiesced,
		types.TaskDefinitionEditPhaseDefinitionCommitted,
		types.TaskDefinitionEditPhaseTemporalTargetApplied:
	default:
		return nil, taskDefinitionEditValidation("native V3 remote phase is invalid")
	}
	op, err := s.loadResearchTaskDefinitionEditOperationV3(ctx,
		types.TaskDefinitionEditScope{ID: lease.ID, TenantID: lease.TenantID,
			UserID: lease.UserID, TargetTenantID: lease.TargetTenantID,
			TargetUserID: lease.TargetUserID, TaskID: lease.TaskID})
	if err != nil {
		return nil, err
	}
	_, _, target, err := decodeResearchTaskDefinitionEditOperationV3(op)
	if err != nil {
		return nil, err
	}
	tx, err := s.beginResearchTaskDefinitionEditTxV3(
		ctx, lease.TenantID, lease.UserID)
	if err != nil {
		return nil, err
	}
	defer rollbackTaskDefinitionEditTx(ctx, tx)
	var authorized types.TaskDefinitionEditOperation
	err = scanTaskDefinitionEditOperation(tx.QueryRow(ctx, `SELECT `+
		taskDefinitionEditOperationColumns+`
		 FROM authorize_native_research_v3_edit_remote_v1(
		 $1,$2,$3,$4,$5,$6,$7,$8,$9)`, lease.ID, lease.TenantID,
		lease.UserID, lease.TaskID, lease.LeaseOwner, lease.Fence,
		expectedPhase, target.TargetActionDigest,
		target.ActionAuthorizationDigest), &authorized)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, taskDefinitionEditLeaseLost()
	}
	if err != nil {
		return nil, taskDefinitionEditDatabaseError("authorize native V3 remote phase", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, taskDefinitionEditDatabaseError("commit native V3 remote authority", err)
	}
	return cloneTaskDefinitionEditOperation(&authorized), nil
}

type researchTaskDefinitionEditCheckpointV3 struct {
	from, to      types.TaskDefinitionEditPhase
	expectedPhase string
	get           func(*types.TaskDefinitionEditOperation) ([]byte, string)
	column        string
}

func (s *Store) CheckpointResearchTaskDefinitionEditBasePausedV3(
	ctx context.Context, lease types.TaskDefinitionEditLease, snapshot []byte,
) error {
	return s.checkpointResearchTaskDefinitionEditV3(ctx, lease, snapshot,
		researchTaskDefinitionEditCheckpointV3{
			from:          types.TaskDefinitionEditPhaseDBQuiesced,
			to:            types.TaskDefinitionEditPhaseTemporalBasePaused,
			expectedPhase: "base_paused", column: "pause",
			get: func(op *types.TaskDefinitionEditOperation) ([]byte, string) {
				return op.PauseSnapshot, op.PauseSnapshotDigest
			},
		})
}

func (s *Store) CheckpointResearchTaskDefinitionEditTargetAppliedV3(
	ctx context.Context, lease types.TaskDefinitionEditLease, snapshot []byte,
) error {
	return s.checkpointResearchTaskDefinitionEditV3(ctx, lease, snapshot,
		researchTaskDefinitionEditCheckpointV3{
			from:          types.TaskDefinitionEditPhaseDefinitionCommitted,
			to:            types.TaskDefinitionEditPhaseTemporalTargetApplied,
			expectedPhase: "target_paused", column: "apply",
			get: func(op *types.TaskDefinitionEditOperation) ([]byte, string) {
				return op.ApplySnapshot, op.ApplySnapshotDigest
			},
		})
}

func (s *Store) CheckpointResearchTaskDefinitionEditTargetRestoredV3(
	ctx context.Context, lease types.TaskDefinitionEditLease, snapshot []byte,
) error {
	return s.checkpointResearchTaskDefinitionEditV3(ctx, lease, snapshot,
		researchTaskDefinitionEditCheckpointV3{
			from:          types.TaskDefinitionEditPhaseTemporalTargetApplied,
			to:            types.TaskDefinitionEditPhaseTemporalTargetRestored,
			expectedPhase: "target_final", column: "restore",
			get: func(op *types.TaskDefinitionEditOperation) ([]byte, string) {
				return op.RestoreSnapshot, op.RestoreSnapshotDigest
			},
		})
}

func (s *Store) checkpointResearchTaskDefinitionEditV3(
	ctx context.Context, lease types.TaskDefinitionEditLease, snapshot []byte,
	cp researchTaskDefinitionEditCheckpointV3,
) error {
	if len(snapshot) == 0 {
		return taskDefinitionEditValidation("native V3 checkpoint is empty")
	}
	op, err := s.loadResearchTaskDefinitionEditOperationV3(ctx,
		types.TaskDefinitionEditScope{ID: lease.ID, TenantID: lease.TenantID,
			UserID: lease.UserID, TargetTenantID: lease.TargetTenantID,
			TargetUserID: lease.TargetUserID, TaskID: lease.TaskID})
	if err != nil {
		return err
	}
	prepared, _, _, err := decodeResearchTaskDefinitionEditOperationV3(op)
	if err != nil {
		return err
	}
	decoded, decodeErr := decodeResearchTaskEditSnapshotV3Store(snapshot, prepared, op.TaskID)
	if decodeErr != nil || decoded.Phase != cp.expectedPhase {
		return taskDefinitionEditValidation("native V3 checkpoint evidence differs")
	}
	digest := sha256HexTaskDefinitionEdit(snapshot)
	tx, err := s.beginResearchTaskDefinitionEditTxV3(ctx, lease.TenantID, lease.UserID)
	if err != nil {
		return err
	}
	defer rollbackTaskDefinitionEditTx(ctx, tx)
	_, err = tx.Exec(ctx, `SELECT checkpoint_native_research_v3_edit_v1(
		$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`, lease.ID, lease.TenantID,
		lease.UserID, lease.TaskID, lease.LeaseOwner, lease.Fence, cp.from,
		cp.to, cp.column, snapshot, digest)
	if err != nil {
		return taskDefinitionEditDatabaseError("write native V3 checkpoint", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return taskDefinitionEditDatabaseError("commit native V3 checkpoint", err)
	}
	return nil
}

func (s *Store) CommitResearchTaskDefinitionEditDefinitionV3(
	ctx context.Context, lease types.TaskDefinitionEditLease,
) error {
	if err := validateTaskDefinitionEditLease(lease); err != nil {
		return err
	}
	op, err := s.loadResearchTaskDefinitionEditOperationV3(ctx,
		types.TaskDefinitionEditScope{ID: lease.ID, TenantID: lease.TenantID,
			UserID: lease.UserID, TargetTenantID: lease.TargetTenantID,
			TargetUserID: lease.TargetUserID, TaskID: lease.TaskID})
	if err != nil {
		return err
	}
	_, base, target, err := decodeResearchTaskDefinitionEditOperationV3(op)
	if err != nil {
		return err
	}
	tx, err := s.beginResearchTaskDefinitionEditTxV3(ctx, lease.TenantID, lease.UserID)
	if err != nil {
		return err
	}
	defer rollbackTaskDefinitionEditTx(ctx, tx)
	_, err = tx.Exec(ctx, `SELECT commit_native_research_v3_edit_definition_v1(
		$1,$2,$3,$4,$5,$6,$7,$8,$9)`, lease.ID, lease.TenantID,
		lease.UserID, lease.TaskID, lease.LeaseOwner, lease.Fence,
		base.TargetActionDigest, target.TargetActionDigest,
		target.ActionAuthorizationDigest)
	if err != nil {
		return taskDefinitionEditDatabaseError("commit native V3 definition", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return taskDefinitionEditDatabaseError("commit native V3 definition", err)
	}
	return nil
}

func (s *Store) CompleteResearchTaskDefinitionEditOperationV3(
	ctx context.Context, lease types.TaskDefinitionEditLease, result json.RawMessage,
) error {
	canonicalResult, err := canonicalTaskDefinitionEditResult(result)
	if err != nil {
		return err
	}
	op, err := s.loadResearchTaskDefinitionEditOperationV3(ctx,
		types.TaskDefinitionEditScope{ID: lease.ID, TenantID: lease.TenantID,
			UserID: lease.UserID, TargetTenantID: lease.TargetTenantID,
			TargetUserID: lease.TargetUserID, TaskID: lease.TaskID})
	if err != nil {
		return err
	}
	_, _, target, err := decodeResearchTaskDefinitionEditOperationV3(op)
	if err != nil {
		return err
	}
	tx, err := s.beginResearchTaskDefinitionEditTxV3(ctx, lease.TenantID, lease.UserID)
	if err != nil {
		return err
	}
	defer rollbackTaskDefinitionEditTx(ctx, tx)
	_, err = tx.Exec(ctx, `SELECT finish_native_research_v3_edit_v1(
		'complete',$1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb,'','')`, lease.ID,
		lease.TenantID, lease.UserID, lease.TaskID, lease.LeaseOwner,
		lease.Fence, target.TargetActionDigest, target.ActionAuthorizationDigest,
		string(canonicalResult))
	if err != nil {
		return taskDefinitionEditDatabaseError("finish native V3 edit", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return taskDefinitionEditDatabaseError("commit native V3 edit completion", err)
	}
	return nil
}

func (s *Store) BlockResearchTaskDefinitionEditOperationV3(
	ctx context.Context, lease types.TaskDefinitionEditLease,
	reason types.TaskDefinitionEditBlockReason,
) error {
	message, ok := taskDefinitionEditBlockText(reason)
	if !ok {
		return taskDefinitionEditValidation("native V3 block reason is invalid")
	}
	tx, err := s.beginResearchTaskDefinitionEditTxV3(ctx, lease.TenantID, lease.UserID)
	if err != nil {
		return err
	}
	defer rollbackTaskDefinitionEditTx(ctx, tx)
	_, err = tx.Exec(ctx, `SELECT finish_native_research_v3_edit_v1(
		'block',$1,$2,$3,$4,$5,$6,'','',NULL,$7,$8)`, lease.ID,
		lease.TenantID, lease.UserID, lease.TaskID, lease.LeaseOwner,
		lease.Fence, string(reason), message)
	if err != nil {
		return taskDefinitionEditDatabaseError("block native V3 edit", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return taskDefinitionEditDatabaseError("commit native V3 edit quarantine", err)
	}
	return nil
}

func (s *Store) ClaimStaleResearchTaskDefinitionEditOperationV3(
	ctx context.Context, before time.Time, leaseOwner string, leaseDuration time.Duration,
) (*types.TaskDefinitionEditOperation, error) {
	if before.IsZero() || !validTaskDefinitionEditReference(leaseOwner, 255) ||
		leaseDuration <= 0 || leaseDuration > maxTaskDefinitionEditLease ||
		leaseDuration.Microseconds() <= 0 {
		return nil, taskDefinitionEditValidation("native V3 recovery claim is invalid")
	}
	if s == nil || s.beginEditRecoveryTx == nil {
		return nil, errNativeV3EditRecoveryRuntimeUnavailable
	}
	tx, err := s.beginEditRecoveryTx(ctx, pgx.TxOptions{})
	if err != nil {
		if errors.Is(err, errNativeV3EditRecoveryRuntimeUnavailable) {
			return nil, errNativeV3EditRecoveryRuntimeUnavailable
		}
		return nil, taskDefinitionEditDatabaseError("begin native V3 recovery claim", err)
	}
	defer rollbackTaskDefinitionEditTx(ctx, tx)
	var operationID, taskID, claimedOwner string
	var tenantID, userID, fence int64
	err = tx.QueryRow(ctx, `SELECT * FROM
		claim_stale_native_research_v3_edit_v1($1,$2,$3)`, before,
		leaseOwner, leaseDuration.Microseconds()).Scan(
		&operationID, &tenantID, &userID, &taskID, &claimedOwner, &fence)
	if errors.Is(err, pgx.ErrNoRows) {
		if commitErr := tx.Commit(ctx); commitErr != nil {
			return nil, taskDefinitionEditDatabaseError("commit empty native V3 recovery claim", commitErr)
		}
		return nil, nil
	}
	if err != nil {
		return nil, taskDefinitionEditDatabaseError("claim stale native V3 edit", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, taskDefinitionEditDatabaseError("commit native V3 recovery claim", err)
	}
	operation, err := s.loadResearchTaskDefinitionEditOperationV3(ctx,
		types.TaskDefinitionEditScope{ID: operationID, TenantID: tenantID,
			UserID: userID, TargetTenantID: tenantID, TargetUserID: userID,
			TaskID: taskID})
	if err != nil {
		return nil, err
	}
	if operation.Status != types.TaskDefinitionEditOperationStatusExecuting ||
		operation.LeaseOwner != claimedOwner || operation.Fence != fence {
		return nil, taskDefinitionEditIntegrity()
	}
	return operation, nil
}
