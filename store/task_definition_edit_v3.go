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

	"github.com/YouToco/vane/internal/strictjson"
	"github.com/YouToco/vane/taskstate"
	"github.com/YouToco/vane/types"
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
	if _, err := tx.Exec(ctx, `SET LOCAL ROLE vane_edit_coordinator`); err != nil {
		rollbackTaskDefinitionEditTx(ctx, tx)
		return nil, taskDefinitionEditDatabaseError("enter native V3 edit role", err)
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
	if err := validateResearchTaskDefinitionEditActiveOwnerV3(
		ctx, tx, tenantID, userID); err != nil {
		return nil, err
	}
	lockClause := ""
	if lock {
		lockClause = " FOR UPDATE OF schedule FOR SHARE OF definition,playbook,authority"
	}
	var status string
	var version, generation int64
	var digest, targetActionDigest, authorizationDigest string
	var payload []byte
	var scheduleName string
	var scheduleSpec []byte
	var manual string
	err := tx.QueryRow(ctx, `
		SELECT schedule.status,schedule.approved_definition_version,
		       schedule.approved_definition_digest,definition.payload,
		       schedule.nl_description,schedule.spec_json::text::bytea,
		       playbook.content,authority.generation,
		       authority.target_action_digest,
		       authority.action_authorization_digest
		  FROM schedules schedule
		  JOIN schedule_playbooks playbook ON playbook.schedule_id=schedule.id
		  JOIN task_approved_definition_versions definition
		    ON definition.tenant_id=schedule.tenant_id
		   AND definition.user_id=schedule.user_id
		   AND definition.task_id=schedule.id
		   AND definition.version=schedule.approved_definition_version
		   AND definition.definition_digest=schedule.approved_definition_digest
		  JOIN research_v3_delivery_authorities authority
		    ON authority.tenant_id=schedule.tenant_id
		   AND authority.user_id=schedule.user_id
		   AND authority.task_id=schedule.id
		   AND authority.definition_version=schedule.approved_definition_version
		   AND authority.definition_digest=schedule.approved_definition_digest
		   AND authority.status='enabled'
		 WHERE schedule.tenant_id=$1 AND schedule.user_id=$2 AND schedule.id=$3
		   AND schedule.execution_mode='discover_at_run'
		   AND schedule.status IN ('active','paused')
		   AND schedule.scope_json='{}'::jsonb
		   AND playbook.fetch_plan='{}'::jsonb
		   AND schedule.definition_edit_operation_id IS NULL
		   AND schedule.definition_edit_fence IS NULL
		   AND definition.schema_version='vane.task-approved-definition/v3'`+lockClause,
		tenantID, userID, taskID).Scan(
		&status, &version, &digest, &payload, &scheduleName, &scheduleSpec,
		&manual, &generation, &targetActionDigest, &authorizationDigest)
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
	preparedBytes, err := loadCurrentResearchPreparedScheduleV3Tx(
		ctx, tx, tenantID, userID, taskID, version, digest)
	if err != nil {
		return nil, err
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

func loadCurrentResearchPreparedScheduleV3Tx(
	ctx context.Context,
	tx pgx.Tx,
	tenantID, userID int64,
	taskID string,
	version int64,
	digest string,
) ([]byte, error) {
	var preparedEdit []byte
	err := tx.QueryRow(ctx, `
		SELECT prepared_edit
		  FROM task_definition_edit_operations
		 WHERE operation_protocol=$1 AND tenant_id=$2 AND user_id=$3
		   AND task_id=$4 AND target_definition_version=$5
		   AND target_definition_digest=$6 AND status='completed'
		   AND phase='temporal_target_restored' AND tombstoned_at IS NOT NULL
		 ORDER BY created_at DESC,id DESC LIMIT 1`,
		types.TaskDefinitionEditProtocolResearchV3, tenantID, userID,
		taskID, version, digest).Scan(&preparedEdit)
	if err == nil {
		prepared, _, _, _, _, decodeErr := decodePreparedResearchTaskEditV3Store(
			preparedEdit)
		if decodeErr != nil || prepared.TargetDefinitionVersion != version ||
			prepared.TargetDefinitionDigest != digest {
			return nil, taskDefinitionEditIntegrity()
		}
		return bytes.Clone(prepared.Target), nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, taskDefinitionEditDatabaseError("load completed native V3 edit provenance", err)
	}
	var preparedSchedule []byte
	err = tx.QueryRow(ctx, `
		SELECT prepared_schedule
		  FROM task_creation_operations
		 WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3
		   AND execution_version=$4 AND tool_name='manage_tasks'
		   AND status='executed' AND phase='completed'
		   AND tombstoned_at IS NOT NULL AND compiled_digest=$5
		 ORDER BY created_at DESC,id DESC LIMIT 1`, tenantID, userID, taskID,
		types.TaskCreationExecutionVersionV2, digest).Scan(&preparedSchedule)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, taskDefinitionEditConflict(
			"native V3 prepared Schedule provenance is unavailable")
	}
	if err != nil {
		return nil, taskDefinitionEditDatabaseError("load native V3 creation provenance", err)
	}
	_, preparedIdentity, err := decodePreparedResearchTaskScheduleV3Store(
		preparedSchedule)
	if err != nil || version != 1 || preparedIdentity.PreparedDigest != digest {
		return nil, taskDefinitionEditIntegrity()
	}
	return bytes.Clone(preparedSchedule), nil
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
	var sessionActive bool
	if err := tx.QueryRow(ctx, `
		SELECT true FROM agent_sessions
		 WHERE id=$1 AND tenant_id=$2 AND user_id=$3 AND status='active'
		 FOR SHARE`, p.SessionID, p.TenantID, p.UserID).Scan(&sessionActive); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, taskDefinitionEditValidation("native V3 edit session is inactive")
		}
		return nil, taskDefinitionEditDatabaseError("validate native V3 edit session", err)
	}
	expiresAt := p.ExpiresAt.UTC().Truncate(time.Microsecond)
	var op types.TaskDefinitionEditOperation
	err = scanTaskDefinitionEditOperation(tx.QueryRow(ctx, `
		INSERT INTO task_definition_edit_operations (
		 id,operation_protocol,tenant_id,user_id,target_tenant_id,target_user_id,
		 task_id,session_id,operation_ref,expires_at,original_status,
		 base_definition_version,base_definition_digest,base_definition,
		 target_definition_version,target_definition_digest,target_definition,
		 canonical_proposal,proposal_digest,prepared_edit,prepared_edit_digest,
		 base_snapshot,base_snapshot_digest)
		VALUES ($1,$2,$3,$4,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,
		        $16,$17,$18,$19,$20,$21)
		RETURNING `+taskDefinitionEditOperationColumns,
		p.ID, types.TaskDefinitionEditProtocolResearchV3, p.TenantID, p.UserID,
		p.TaskID, p.SessionID, "agent_auto/v3:"+p.ID, expiresAt, originalStatus,
		p.BaseVersion, baseDigest, p.BaseDefinition, p.TargetVersion, targetDigest,
		p.TargetDefinition, proposalBytes, proposalDigest, p.PreparedEdit,
		preparedDigest, p.BaseSnapshot, snapshotDigest), &op)
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
		 FROM task_definition_edit_operations
		 WHERE id=$1 AND operation_protocol=$2 AND tenant_id=$3 AND user_id=$4
		   AND target_tenant_id=$3 AND target_user_id=$4 AND task_id=$5`,
		scope.ID, types.TaskDefinitionEditProtocolResearchV3,
		scope.TenantID, scope.UserID, scope.TaskID), &op)
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
	tx, err := s.beginResearchTaskDefinitionEditTxV3(
		ctx, lease.TenantID, lease.UserID)
	if err != nil {
		return err
	}
	defer rollbackTaskDefinitionEditTx(ctx, tx)
	if err := lockTaskScheduleMutation(ctx, tx, lease.TaskID); err != nil {
		return err
	}
	schedule, err := lockTaskDefinitionEditScheduleForUpdate(
		ctx, tx, lease.TargetTenantID, lease.TargetUserID, lease.TaskID)
	if err != nil {
		return err
	}
	op, _, err := loadLeasedTaskDefinitionEditOperationProtocol(
		ctx, tx, lease, types.TaskDefinitionEditProtocolResearchV3)
	if err != nil {
		return err
	}
	_, base, _, err := decodeResearchTaskDefinitionEditOperationV3(op)
	if err != nil {
		return err
	}
	if op.Phase == types.TaskDefinitionEditPhaseDBQuiesced {
		if assessTaskDefinitionEditScheduleProtocol(op, schedule,
			types.TaskDefinitionEditProtocolResearchV3) != taskDefinitionEditScheduleExact {
			return taskDefinitionEditConflict("native V3 quiesce replay differs")
		}
		return verifyResearchTaskDefinitionEditAuthorityV3(
			ctx, tx, op, op.BaseDefinitionVersion, op.BaseDefinitionDigest,
			base.TargetActionDigest, base.ActionAuthorizationDigest, "revoked")
	}
	if op.Phase != types.TaskDefinitionEditPhaseProposalSealed ||
		assessTaskDefinitionEditScheduleProtocol(op, schedule,
			types.TaskDefinitionEditProtocolResearchV3) != taskDefinitionEditScheduleExact {
		return taskDefinitionEditConflict("native V3 edit is not quiesce-ready")
	}
	if err := validateResearchTaskDefinitionEditActiveOwnerV3(
		ctx, tx, op.TenantID, op.UserID); err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `
		UPDATE research_v3_delivery_authorities
		   SET status='revoked',revoked_at=clock_timestamp()
		 WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3
		   AND definition_version=$4 AND definition_digest=$5
		   AND target_action_digest=$6 AND action_authorization_digest=$7
		   AND status='enabled' AND enabled_at IS NOT NULL AND revoked_at IS NULL`,
		op.TenantID, op.UserID, op.TaskID, op.BaseDefinitionVersion,
		op.BaseDefinitionDigest, base.TargetActionDigest,
		base.ActionAuthorizationDigest)
	if err != nil {
		return taskDefinitionEditDatabaseError("revoke native V3 edit authority", err)
	}
	if tag.RowsAffected() != 1 {
		return taskDefinitionEditConflict("native V3 base authority changed")
	}
	tag, err = tx.Exec(ctx, `
		UPDATE schedules
		   SET status=$7,definition_edit_operation_id=$8,
		       definition_edit_fence=$9,updated_at=clock_timestamp()
		 WHERE tenant_id=$1 AND user_id=$2 AND id=$3 AND status=$4
		   AND execution_mode=$5 AND approved_definition_version=$6
		   AND approved_definition_digest=$10
		   AND definition_edit_operation_id IS NULL AND definition_edit_fence IS NULL`,
		op.TenantID, op.UserID, op.TaskID, op.OriginalStatus,
		types.ExecutionModeDiscoverAtRun, op.BaseDefinitionVersion,
		types.ScheduleStatusPaused, op.ID, op.Fence, op.BaseDefinitionDigest)
	if err != nil || tag.RowsAffected() != 1 {
		if err == nil {
			return taskDefinitionEditConflict("native V3 schedule changed during quiesce")
		}
		return taskDefinitionEditDatabaseError("quiesce native V3 schedule", err)
	}
	tag, err = tx.Exec(ctx, `
		UPDATE task_definition_edit_operations SET phase=$7,updated_at=clock_timestamp()
		 WHERE id=$1 AND tenant_id=$2 AND user_id=$3
		   AND target_tenant_id=$4 AND target_user_id=$5 AND task_id=$6
		   AND operation_protocol=$8 AND status=$9 AND phase=$10
		   AND tombstoned_at IS NULL AND lease_owner=$11 AND fence=$12
		   AND lease_until>clock_timestamp()`, op.ID, op.TenantID, op.UserID,
		op.TargetTenantID, op.TargetUserID, op.TaskID,
		types.TaskDefinitionEditPhaseDBQuiesced,
		types.TaskDefinitionEditProtocolResearchV3,
		types.TaskDefinitionEditOperationStatusExecuting,
		types.TaskDefinitionEditPhaseProposalSealed, lease.LeaseOwner, lease.Fence)
	if err != nil || tag.RowsAffected() != 1 {
		if err == nil {
			return taskDefinitionEditLeaseLost()
		}
		return taskDefinitionEditDatabaseError("checkpoint native V3 quiesce", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return taskDefinitionEditDatabaseError("commit native V3 quiesce", err)
	}
	return nil
}

func verifyResearchTaskDefinitionEditAuthorityV3(
	ctx context.Context, tx pgx.Tx, op *types.TaskDefinitionEditOperation,
	version int64, digest, targetActionDigest, authorizationDigest, status string,
) error {
	var count int
	if err := tx.QueryRow(ctx, `
		SELECT count(*) FROM research_v3_delivery_authorities
		 WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3
		   AND definition_version=$4 AND definition_digest=$5
		   AND target_action_digest=$6 AND action_authorization_digest=$7
		   AND status=$8`, op.TenantID, op.UserID, op.TaskID, version, digest,
		targetActionDigest, authorizationDigest, status).Scan(&count); err != nil {
		return taskDefinitionEditDatabaseError("verify native V3 edit authority", err)
	}
	if count != 1 {
		return taskDefinitionEditConflict("native V3 edit authority differs")
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
	tx, err := s.beginResearchTaskDefinitionEditTxV3(
		ctx, lease.TenantID, lease.UserID)
	if err != nil {
		return nil, err
	}
	defer rollbackTaskDefinitionEditTx(ctx, tx)
	schedule, err := lockTaskDefinitionEditScheduleForUpdate(
		ctx, tx, lease.TargetTenantID, lease.TargetUserID, lease.TaskID)
	if err != nil {
		return nil, err
	}
	op, _, err := loadLeasedTaskDefinitionEditOperationProtocol(
		ctx, tx, lease, types.TaskDefinitionEditProtocolResearchV3)
	if err != nil {
		return nil, err
	}
	_, _, target, err := decodeResearchTaskDefinitionEditOperationV3(op)
	if err != nil || op.Phase != expectedPhase ||
		assessTaskDefinitionEditScheduleProtocol(op, schedule,
			types.TaskDefinitionEditProtocolResearchV3) != taskDefinitionEditScheduleExact {
		return nil, taskDefinitionEditConflict("native V3 remote authority changed")
	}
	if expectedPhase != types.TaskDefinitionEditPhaseDBQuiesced {
		if err := verifyResearchTaskDefinitionEditAuthorityV3(ctx, tx, op,
			op.TargetDefinitionVersion, op.TargetDefinitionDigest,
			target.TargetActionDigest, target.ActionAuthorizationDigest, "staged"); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, taskDefinitionEditDatabaseError("commit native V3 remote authority", err)
	}
	return cloneTaskDefinitionEditOperation(op), nil
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
	tx, err := s.beginResearchTaskDefinitionEditTxV3(ctx, lease.TenantID, lease.UserID)
	if err != nil {
		return err
	}
	defer rollbackTaskDefinitionEditTx(ctx, tx)
	schedule, err := lockTaskDefinitionEditScheduleForUpdate(
		ctx, tx, lease.TargetTenantID, lease.TargetUserID, lease.TaskID)
	if err != nil {
		return err
	}
	op, _, err := loadLeasedTaskDefinitionEditOperationProtocol(
		ctx, tx, lease, types.TaskDefinitionEditProtocolResearchV3)
	if err != nil {
		return err
	}
	prepared, _, _, err := decodeResearchTaskDefinitionEditOperationV3(op)
	decoded, decodeErr := decodeResearchTaskEditSnapshotV3Store(snapshot, prepared, op.TaskID)
	digest := sha256HexTaskDefinitionEdit(snapshot)
	existing, existingDigest := cp.get(op)
	if decodeErr != nil || decoded.Phase != cp.expectedPhase {
		return s.blockInvalidTaskDefinitionEditCheckpoint(ctx, tx, op, lease)
	}
	if op.Phase == cp.to {
		if !bytes.Equal(existing, snapshot) || existingDigest != digest ||
			assessTaskDefinitionEditScheduleProtocol(op, schedule,
				types.TaskDefinitionEditProtocolResearchV3) != taskDefinitionEditScheduleExact {
			return taskDefinitionEditConflict("native V3 checkpoint replay differs")
		}
		return nil
	}
	if op.Phase != cp.from || len(existing) != 0 || existingDigest != "" ||
		assessTaskDefinitionEditScheduleProtocol(op, schedule,
			types.TaskDefinitionEditProtocolResearchV3) != taskDefinitionEditScheduleExact {
		return taskDefinitionEditConflict("native V3 checkpoint transition differs")
	}
	query := `UPDATE task_definition_edit_operations SET ` + cp.column +
		`_snapshot=$7,` + cp.column + `_snapshot_digest=$8,phase=$9,
		 updated_at=clock_timestamp()
		 WHERE id=$1 AND tenant_id=$2 AND user_id=$3
		   AND target_tenant_id=$4 AND target_user_id=$5 AND task_id=$6
		   AND operation_protocol=$10 AND status=$11 AND phase=$12
		   AND tombstoned_at IS NULL AND lease_owner=$13 AND fence=$14
		   AND lease_until>clock_timestamp() AND ` + cp.column +
		`_snapshot IS NULL AND ` + cp.column + `_snapshot_digest=''`
	tag, err := tx.Exec(ctx, query, op.ID, op.TenantID, op.UserID,
		op.TargetTenantID, op.TargetUserID, op.TaskID, snapshot, digest, cp.to,
		types.TaskDefinitionEditProtocolResearchV3,
		types.TaskDefinitionEditOperationStatusExecuting, cp.from,
		lease.LeaseOwner, lease.Fence)
	if err != nil || tag.RowsAffected() != 1 {
		if err == nil {
			return taskDefinitionEditLeaseLost()
		}
		return taskDefinitionEditDatabaseError("write native V3 checkpoint", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return taskDefinitionEditDatabaseError("commit native V3 checkpoint", err)
	}
	return nil
}

func verifyCommittedResearchTaskDefinitionEditV3(
	ctx context.Context, tx pgx.Tx, op *types.TaskDefinitionEditOperation,
	target preparedResearchTaskScheduleV3StoreView,
) error {
	var schema string
	var mode types.ExecutionMode
	var digest, operationRef, status, name, manual string
	var payload, spec, scope, fetchPlan []byte
	err := tx.QueryRow(ctx, `
		SELECT definition.schema_version,definition.execution_mode,
		       definition.definition_digest,definition.payload,
		       definition.operation_ref,schedule.status,schedule.nl_description,
		       schedule.spec_json::text::bytea,schedule.scope_json::text::bytea,
		       playbook.content,playbook.fetch_plan::text::bytea
		  FROM task_approved_definition_versions definition
		  JOIN schedules schedule ON schedule.tenant_id=definition.tenant_id
		   AND schedule.user_id=definition.user_id AND schedule.id=definition.task_id
		  JOIN schedule_playbooks playbook ON playbook.schedule_id=schedule.id
		 WHERE definition.tenant_id=$1 AND definition.user_id=$2
		   AND definition.task_id=$3 AND definition.version=$4`,
		op.TenantID, op.UserID, op.TaskID, op.TargetDefinitionVersion).Scan(
		&schema, &mode, &digest, &payload, &operationRef, &status, &name,
		&spec, &scope, &manual, &fetchPlan)
	if errors.Is(err, pgx.ErrNoRows) {
		return taskDefinitionEditConflict("native V3 target definition is missing")
	}
	if err != nil {
		return taskDefinitionEditDatabaseError("verify native V3 target definition", err)
	}
	definition, decodeErr := taskstate.DecodeApprovedDefinitionV3(payload)
	if decodeErr != nil || schema != taskstate.ApprovedDefinitionSchemaVersionV3 ||
		mode != types.ExecutionModeDiscoverAtRun || digest != op.TargetDefinitionDigest ||
		operationRef != "definition-edit-v3/"+op.ID ||
		!bytes.Equal(payload, op.TargetDefinition) || name != definition.TaskName ||
		manual != definition.TaskManual || !jsonBytesSemanticEqual(spec, definition.SpecJSON) ||
		!jsonBytesSemanticEqual(scope, []byte(`{}`)) ||
		!jsonBytesSemanticEqual(fetchPlan, []byte(`{}`)) {
		return taskDefinitionEditIntegrity()
	}
	if status != string(types.ScheduleStatusPaused) {
		return taskDefinitionEditConflict("native V3 target schedule is not paused")
	}
	return verifyResearchTaskDefinitionEditAuthorityV3(ctx, tx, op,
		op.TargetDefinitionVersion, op.TargetDefinitionDigest,
		target.TargetActionDigest, target.ActionAuthorizationDigest, "staged")
}

func (s *Store) CommitResearchTaskDefinitionEditDefinitionV3(
	ctx context.Context, lease types.TaskDefinitionEditLease,
) error {
	if err := validateTaskDefinitionEditLease(lease); err != nil {
		return err
	}
	tx, err := s.beginResearchTaskDefinitionEditTxV3(ctx, lease.TenantID, lease.UserID)
	if err != nil {
		return err
	}
	defer rollbackTaskDefinitionEditTx(ctx, tx)
	if err := lockTaskScheduleMutation(ctx, tx, lease.TaskID); err != nil {
		return err
	}
	schedule, err := lockTaskDefinitionEditScheduleForUpdate(
		ctx, tx, lease.TargetTenantID, lease.TargetUserID, lease.TaskID)
	if err != nil {
		return err
	}
	op, _, err := loadLeasedTaskDefinitionEditOperationProtocol(
		ctx, tx, lease, types.TaskDefinitionEditProtocolResearchV3)
	if err != nil {
		return err
	}
	_, base, target, err := decodeResearchTaskDefinitionEditOperationV3(op)
	if err != nil {
		return err
	}
	if taskDefinitionEditPhaseHasCommittedDefinition(op.Phase) {
		if assessTaskDefinitionEditScheduleProtocol(op, schedule,
			types.TaskDefinitionEditProtocolResearchV3) != taskDefinitionEditScheduleExact {
			return taskDefinitionEditConflict("native V3 definition replay differs")
		}
		if err := verifyCommittedResearchTaskDefinitionEditV3(
			ctx, tx, op, target); err != nil {
			return err
		}
		return nil
	}
	if op.Phase != types.TaskDefinitionEditPhaseTemporalBasePaused ||
		assessTaskDefinitionEditScheduleProtocol(op, schedule,
			types.TaskDefinitionEditProtocolResearchV3) != taskDefinitionEditScheduleExact {
		return taskDefinitionEditConflict("native V3 definition is not commit-ready")
	}
	definition, err := taskstate.DecodeApprovedDefinitionV3(op.TargetDefinition)
	if err != nil {
		return taskDefinitionEditIntegrity()
	}
	canonical, digest, err := canonicalResearchDefinitionV3(definition)
	if err != nil || !bytes.Equal(canonical, op.TargetDefinition) ||
		digest != op.TargetDefinitionDigest || definition.TenantID != op.TenantID ||
		definition.UserID != op.UserID || definition.TaskID != op.TaskID ||
		definition.ExecutionMode != types.ExecutionModeDiscoverAtRun {
		return taskDefinitionEditIntegrity()
	}
	if err := verifyResearchTaskDefinitionEditAuthorityV3(ctx, tx, op,
		op.BaseDefinitionVersion, op.BaseDefinitionDigest,
		base.TargetActionDigest, base.ActionAuthorizationDigest, "revoked"); err != nil {
		return err
	}
	var baseGeneration int64
	if err := tx.QueryRow(ctx, `
		SELECT generation FROM research_v3_delivery_authorities
		 WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3
		   AND definition_version=$4 AND definition_digest=$5
		   AND target_action_digest=$6 AND action_authorization_digest=$7
		   AND status='revoked' FOR SHARE`, op.TenantID, op.UserID, op.TaskID,
		op.BaseDefinitionVersion, op.BaseDefinitionDigest,
		base.TargetActionDigest, base.ActionAuthorizationDigest).Scan(&baseGeneration); err != nil {
		return taskDefinitionEditDatabaseError("load native V3 base generation", err)
	}
	if baseGeneration <= 0 {
		return taskDefinitionEditIntegrity()
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO task_approved_definition_versions
		 (tenant_id,user_id,task_id,version,schema_version,execution_mode,
		  definition_digest,payload,operation_ref)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`, op.TenantID, op.UserID, op.TaskID,
		op.TargetDefinitionVersion, taskstate.ApprovedDefinitionSchemaVersionV3,
		types.ExecutionModeDiscoverAtRun, op.TargetDefinitionDigest,
		op.TargetDefinition, "definition-edit-v3/"+op.ID); err != nil {
		return taskDefinitionEditDatabaseError("append native V3 target definition", err)
	}
	tag, err := tx.Exec(ctx, `
		UPDATE schedules SET nl_description=$4,spec_json=$5::jsonb,
		 approved_definition_version=$6,approved_definition_digest=$7,
		 updated_at=clock_timestamp()
		 WHERE tenant_id=$1 AND user_id=$2 AND id=$3 AND status='paused'
		   AND execution_mode='discover_at_run'
		   AND approved_definition_version=$8 AND approved_definition_digest=$9
		   AND definition_edit_operation_id=$10 AND definition_edit_fence=$11
		   AND scope_json='{}'::jsonb`, op.TenantID, op.UserID, op.TaskID,
		definition.TaskName, string(definition.SpecJSON), op.TargetDefinitionVersion,
		op.TargetDefinitionDigest, op.BaseDefinitionVersion, op.BaseDefinitionDigest,
		op.ID, op.Fence)
	if err != nil || tag.RowsAffected() != 1 {
		if err == nil {
			return taskDefinitionEditConflict("native V3 schedule head changed")
		}
		return taskDefinitionEditDatabaseError("advance native V3 schedule head", err)
	}
	tag, err = tx.Exec(ctx, `
		UPDATE schedule_playbooks SET content=$2
		 WHERE schedule_id=$1 AND fetch_plan='{}'::jsonb`, op.TaskID,
		definition.TaskManual)
	if err != nil || tag.RowsAffected() != 1 {
		if err == nil {
			return taskDefinitionEditConflict("native V3 task manual changed")
		}
		return taskDefinitionEditDatabaseError("update native V3 task manual", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO research_v3_delivery_authorities
		 (tenant_id,user_id,task_id,generation,definition_version,
		  definition_digest,target_action_digest,action_authorization_digest,status)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'staged')`, op.TenantID, op.UserID,
		op.TaskID, baseGeneration+1, op.TargetDefinitionVersion,
		op.TargetDefinitionDigest, target.TargetActionDigest,
		target.ActionAuthorizationDigest); err != nil {
		return taskDefinitionEditDatabaseError("stage native V3 target authority", err)
	}
	tag, err = tx.Exec(ctx, `
		UPDATE task_definition_edit_operations SET phase=$7,updated_at=clock_timestamp()
		 WHERE id=$1 AND tenant_id=$2 AND user_id=$3
		   AND target_tenant_id=$4 AND target_user_id=$5 AND task_id=$6
		   AND operation_protocol=$8 AND status=$9 AND phase=$10
		   AND tombstoned_at IS NULL AND lease_owner=$11 AND fence=$12
		   AND lease_until>clock_timestamp()`, op.ID, op.TenantID, op.UserID,
		op.TargetTenantID, op.TargetUserID, op.TaskID,
		types.TaskDefinitionEditPhaseDefinitionCommitted,
		types.TaskDefinitionEditProtocolResearchV3,
		types.TaskDefinitionEditOperationStatusExecuting,
		types.TaskDefinitionEditPhaseTemporalBasePaused,
		lease.LeaseOwner, lease.Fence)
	if err != nil || tag.RowsAffected() != 1 {
		if err == nil {
			return taskDefinitionEditLeaseLost()
		}
		return taskDefinitionEditDatabaseError("checkpoint native V3 definition", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return taskDefinitionEditDatabaseError("commit native V3 definition", err)
	}
	return nil
}

func researchTaskDefinitionEditCompletedScheduleExactV3(
	op *types.TaskDefinitionEditOperation, schedule *taskDefinitionEditScheduleRow,
) bool {
	return schedule != nil && schedule.Status == op.OriginalStatus &&
		schedule.Mode == types.ExecutionModeDiscoverAtRun &&
		schedule.Version != nil && *schedule.Version == op.TargetDefinitionVersion &&
		schedule.Digest != nil && *schedule.Digest == op.TargetDefinitionDigest &&
		schedule.OperationID == nil && schedule.Fence == nil
}

func (s *Store) CompleteResearchTaskDefinitionEditOperationV3(
	ctx context.Context, lease types.TaskDefinitionEditLease, result json.RawMessage,
) error {
	canonicalResult, err := canonicalTaskDefinitionEditResult(result)
	if err != nil {
		return err
	}
	tx, err := s.beginResearchTaskDefinitionEditTxV3(ctx, lease.TenantID, lease.UserID)
	if err != nil {
		return err
	}
	defer rollbackTaskDefinitionEditTx(ctx, tx)
	if err := lockTaskScheduleMutation(ctx, tx, lease.TaskID); err != nil {
		return err
	}
	schedule, err := lockTaskDefinitionEditScheduleForUpdate(
		ctx, tx, lease.TargetTenantID, lease.TargetUserID, lease.TaskID)
	if err != nil {
		return err
	}
	op, err := loadTaskDefinitionEditOperationForUpdateProtocol(ctx, tx,
		types.TaskDefinitionEditScope{
			ID: lease.ID, TenantID: lease.TenantID, UserID: lease.UserID,
			TargetTenantID: lease.TargetTenantID,
			TargetUserID:   lease.TargetUserID, TaskID: lease.TaskID,
		}, types.TaskDefinitionEditProtocolResearchV3)
	if err != nil {
		return err
	}
	_, _, target, err := decodeResearchTaskDefinitionEditOperationV3(op)
	if err != nil {
		return err
	}
	if op.Status == types.TaskDefinitionEditOperationStatusCompleted {
		storedResult, storedErr := canonicalTaskDefinitionEditResult(op.Result)
		if op.Phase != types.TaskDefinitionEditPhaseTemporalTargetRestored ||
			op.Fence != lease.Fence || op.TombstonedAt == nil || op.LeaseOwner != "" ||
			op.LeaseUntil != nil || op.TakeoverNotBefore != nil || storedErr != nil ||
			!bytes.Equal(storedResult, canonicalResult) ||
			!researchTaskDefinitionEditCompletedScheduleExactV3(op, schedule) {
			return taskDefinitionEditConflict("native V3 completion replay differs")
		}
		if err := verifyResearchTaskDefinitionEditAuthorityV3(ctx, tx, op,
			op.TargetDefinitionVersion, op.TargetDefinitionDigest,
			target.TargetActionDigest, target.ActionAuthorizationDigest, "enabled"); err != nil {
			return err
		}
		return verifyTaskDefinitionEditReceiptForTerminal(
			ctx, tx, op.ID, op.TenantID, op.UserID)
	}
	if op.Status != types.TaskDefinitionEditOperationStatusExecuting ||
		op.Phase != types.TaskDefinitionEditPhaseTemporalTargetRestored ||
		assessTaskDefinitionEditScheduleProtocol(op, schedule,
			types.TaskDefinitionEditProtocolResearchV3) != taskDefinitionEditScheduleExact {
		return taskDefinitionEditConflict("native V3 operation is not completion-ready")
	}
	databaseNow, err := taskDefinitionEditDatabaseClock(ctx, tx)
	if err != nil {
		return err
	}
	if err := validateLoadedTaskDefinitionEditLease(op, databaseNow, lease); err != nil {
		return err
	}
	if err := verifyResearchTaskDefinitionEditAuthorityV3(ctx, tx, op,
		op.TargetDefinitionVersion, op.TargetDefinitionDigest,
		target.TargetActionDigest, target.ActionAuthorizationDigest, "staged"); err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `
		UPDATE schedules SET status=$4,definition_edit_operation_id=NULL,
		 definition_edit_fence=NULL,updated_at=clock_timestamp()
		 WHERE tenant_id=$1 AND user_id=$2 AND id=$3 AND status='paused'
		   AND definition_edit_operation_id=$5 AND definition_edit_fence=$6
		   AND execution_mode='discover_at_run'
		   AND approved_definition_version=$7 AND approved_definition_digest=$8`,
		op.TenantID, op.UserID, op.TaskID, op.OriginalStatus, op.ID, op.Fence,
		op.TargetDefinitionVersion, op.TargetDefinitionDigest)
	if err != nil || tag.RowsAffected() != 1 {
		if err == nil {
			return taskDefinitionEditConflict("native V3 schedule changed during completion")
		}
		return taskDefinitionEditDatabaseError("restore native V3 schedule status", err)
	}
	tag, err = tx.Exec(ctx, `
		UPDATE research_v3_delivery_authorities
		   SET status='enabled',enabled_at=clock_timestamp()
		 WHERE tenant_id=$1 AND user_id=$2 AND task_id=$3
		   AND definition_version=$4 AND definition_digest=$5
		   AND target_action_digest=$6 AND action_authorization_digest=$7
		   AND status='staged' AND enabled_at IS NULL AND revoked_at IS NULL`,
		op.TenantID, op.UserID, op.TaskID, op.TargetDefinitionVersion,
		op.TargetDefinitionDigest, target.TargetActionDigest,
		target.ActionAuthorizationDigest)
	if err != nil || tag.RowsAffected() != 1 {
		if err == nil {
			return taskDefinitionEditConflict("native V3 target authority changed")
		}
		return taskDefinitionEditDatabaseError("enable native V3 target authority", err)
	}
	if _, err := terminateTaskDefinitionEditTx(ctx, tx, op,
		types.TaskDefinitionEditOperationStatusCompleted, "", "",
		canonicalResult, true, &lease); err != nil {
		return err
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
	op, err := loadTaskDefinitionEditOperationForUpdateProtocol(ctx, tx,
		types.TaskDefinitionEditScope{
			ID: lease.ID, TenantID: lease.TenantID, UserID: lease.UserID,
			TargetTenantID: lease.TargetTenantID,
			TargetUserID:   lease.TargetUserID, TaskID: lease.TaskID,
		}, types.TaskDefinitionEditProtocolResearchV3)
	if err != nil {
		return err
	}
	if op.Status == types.TaskDefinitionEditOperationStatusBlocked {
		if op.Fence != lease.Fence || op.ErrorCode != string(reason) ||
			op.ErrorMessage != message || op.TombstonedAt == nil ||
			op.LeaseOwner != "" || op.LeaseUntil != nil ||
			op.TakeoverNotBefore != nil || len(op.Result) != 0 {
			return taskDefinitionEditConflict("native V3 block replay differs")
		}
		return verifyTaskDefinitionEditReceiptForTerminal(
			ctx, tx, op.ID, op.TenantID, op.UserID)
	}
	if taskDefinitionEditOperationIsTerminal(op.Status) {
		return taskDefinitionEditTerminal()
	}
	databaseNow, err := taskDefinitionEditDatabaseClock(ctx, tx)
	if err != nil {
		return err
	}
	if err := validateLoadedTaskDefinitionEditLease(op, databaseNow, lease); err != nil {
		return err
	}
	if _, err := terminateTaskDefinitionEditTx(ctx, tx, op,
		types.TaskDefinitionEditOperationStatusBlocked, string(reason), message,
		nil, false, &lease); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return taskDefinitionEditDatabaseError("commit native V3 edit quarantine", err)
	}
	return nil
}

func (s *Store) ListStaleResearchTaskDefinitionEditTenantIDsV3(
	ctx context.Context, before time.Time, afterTenantID int64, limit int,
) ([]int64, error) {
	if before.IsZero() || afterTenantID < 0 || limit <= 0 || limit > 1000 {
		return nil, taskDefinitionEditValidation("native V3 stale tenant query is invalid")
	}
	tx, err := s.beginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, taskDefinitionEditDatabaseError("begin native V3 stale tenant scan", err)
	}
	defer rollbackTaskDefinitionEditTx(ctx, tx)
	rows, err := tx.Query(ctx, `
		SELECT DISTINCT tenant_id FROM task_definition_edit_operations
		 WHERE operation_protocol=$1 AND status=$2 AND tombstoned_at IS NULL
		   AND tenant_id>$3 AND lease_owner<>'' AND fence>0 AND attempt>0
		   AND lease_until IS NOT NULL AND takeover_not_before IS NOT NULL
		   AND lease_until<=clock_timestamp()
		   AND takeover_not_before<=LEAST($4,clock_timestamp())
		 ORDER BY tenant_id LIMIT $5`, types.TaskDefinitionEditProtocolResearchV3,
		types.TaskDefinitionEditOperationStatusExecuting, afterTenantID, before, limit)
	if err != nil {
		return nil, taskDefinitionEditDatabaseError("list native V3 stale tenants", err)
	}
	defer rows.Close()
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, taskDefinitionEditDatabaseError("scan native V3 stale tenant", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, taskDefinitionEditDatabaseError("iterate native V3 stale tenants", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, taskDefinitionEditDatabaseError("commit native V3 stale tenant scan", err)
	}
	return ids, nil
}

func (s *Store) ListStaleResearchTaskDefinitionEditOperationsV3(
	ctx context.Context, tenantID int64, before time.Time, limit int,
) ([]types.TaskDefinitionEditOperation, error) {
	if tenantID <= 0 || before.IsZero() || limit <= 0 || limit > 1000 {
		return nil, taskDefinitionEditValidation("native V3 stale operation query is invalid")
	}
	tx, err := s.beginTaskDefinitionEditTx(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	defer rollbackTaskDefinitionEditTx(ctx, tx)
	rows, err := tx.Query(ctx, `SELECT `+taskDefinitionEditOperationColumns+`
		 FROM task_definition_edit_operations
		 WHERE tenant_id=$1 AND operation_protocol=$2 AND status=$3
		   AND tombstoned_at IS NULL AND lease_owner<>'' AND fence>0 AND attempt>0
		   AND lease_until IS NOT NULL AND takeover_not_before IS NOT NULL
		   AND lease_until<=clock_timestamp()
		   AND takeover_not_before<=LEAST($4,clock_timestamp())
		 ORDER BY takeover_not_before,id LIMIT $5`, tenantID,
		types.TaskDefinitionEditProtocolResearchV3,
		types.TaskDefinitionEditOperationStatusExecuting, before, limit)
	if err != nil {
		return nil, taskDefinitionEditDatabaseError("list native V3 stale operations", err)
	}
	defer rows.Close()
	operations := make([]types.TaskDefinitionEditOperation, 0)
	for rows.Next() {
		var op types.TaskDefinitionEditOperation
		if err := scanTaskDefinitionEditOperation(rows, &op); err != nil {
			return nil, taskDefinitionEditDatabaseError("scan native V3 stale operation", err)
		}
		operations = append(operations, *cloneTaskDefinitionEditOperation(&op))
	}
	if err := rows.Err(); err != nil {
		return nil, taskDefinitionEditDatabaseError("iterate native V3 stale operations", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, taskDefinitionEditDatabaseError("commit native V3 stale operation scan", err)
	}
	return operations, nil
}
