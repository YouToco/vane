package store

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/YouToco/vane/types"
)

func validateResearchGatewayConnection(ctx context.Context, conn *pgx.Conn) error {
	if conn == nil {
		return errors.New("gateway connection is nil")
	}
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin gateway authority probe: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	var session string
	var login, super, bypass, createdb, createrole, replication, inherit bool
	if err := tx.QueryRow(ctx, `SELECT rolname,rolcanlogin,rolsuper,rolbypassrls,
		rolcreatedb,rolcreaterole,rolreplication,rolinherit FROM pg_roles
		WHERE rolname=session_user`).Scan(&session, &login, &super, &bypass,
		&createdb, &createrole, &replication, &inherit); err != nil {
		return fmt.Errorf("read gateway identity: %w", err)
	}
	if session != researchGatewayLoginRole || !login || super || bypass || createdb ||
		createrole || replication || inherit {
		return fmt.Errorf("gateway login %q has unsafe role attributes", session)
	}
	var gatewayMember, executorMember, appMember bool
	if err := tx.QueryRow(ctx, `SELECT pg_has_role(session_user,$1,'MEMBER'),
		pg_has_role(session_user,$2,'MEMBER'),pg_has_role(session_user,'vane_app','MEMBER')`,
		researchGatewayCapabilityRole, researchRuntimeCapabilityRole,
	).Scan(&gatewayMember, &executorMember, &appMember); err != nil {
		return fmt.Errorf("inspect gateway memberships: %w", err)
	}
	if !gatewayMember || executorMember || appMember {
		return fmt.Errorf("gateway login %q has unsafe memberships", session)
	}
	memberships, err := tx.Query(ctx, `SELECT role.rolname FROM pg_roles role
		WHERE role.rolname<>session_user AND pg_has_role(session_user,role.oid,'MEMBER')
		ORDER BY role.rolname`)
	if err != nil {
		return fmt.Errorf("inspect gateway membership set: %w", err)
	}
	var membershipNames []string
	for memberships.Next() {
		var name string
		if err := memberships.Scan(&name); err != nil {
			memberships.Close()
			return err
		}
		membershipNames = append(membershipNames, name)
	}
	memberships.Close()
	if len(membershipNames) != 1 || membershipNames[0] != researchGatewayCapabilityRole {
		return fmt.Errorf("gateway login has unexpected memberships: %v", membershipNames)
	}
	var capLogin, capSuper, capBypass, capDB, capRole, capReplication, capInherit, capExecutor, capApp bool
	if err := tx.QueryRow(ctx, `SELECT rolcanlogin,rolsuper,rolbypassrls,rolcreatedb,
		rolcreaterole,rolreplication,rolinherit,pg_has_role(oid,$1,'MEMBER'),
		pg_has_role(oid,'vane_app','MEMBER') FROM pg_roles WHERE rolname=$2`,
		researchRuntimeCapabilityRole, researchGatewayCapabilityRole).Scan(&capLogin, &capSuper,
		&capBypass, &capDB, &capRole, &capReplication, &capInherit, &capExecutor, &capApp); err != nil {
		return err
	}
	if capLogin || capSuper || capBypass || capDB || capRole || capReplication || capInherit || capExecutor || capApp {
		return errors.New("gateway capability role is unsafe")
	}
	var ownedCount int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM pg_class class JOIN pg_namespace ns
		ON ns.oid=class.relnamespace WHERE ns.nspname='public' AND class.relowner=(
		SELECT oid FROM pg_roles WHERE rolname=$1)`, researchGatewayCapabilityRole).Scan(&ownedCount); err != nil {
		return err
	}
	if ownedCount != 0 {
		return errors.New("gateway capability owns public relations")
	}
	if _, err := tx.Exec(ctx, `SET LOCAL ROLE `+researchGatewayCapabilityRole); err != nil {
		return fmt.Errorf("enter gateway capability: %w", err)
	}
	var directDML int
	var canCreatePublic bool
	if err := tx.QueryRow(ctx, `SELECT count(*) FILTER (WHERE
		has_table_privilege(current_user,relation,'INSERT') OR
		has_table_privilege(current_user,relation,'UPDATE') OR
		has_table_privilege(current_user,relation,'DELETE') OR
		has_table_privilege(current_user,relation,'TRUNCATE') OR
		has_any_column_privilege(current_user,relation,'INSERT') OR
		has_any_column_privilege(current_user,relation,'UPDATE')),
		has_schema_privilege(current_user,'public','CREATE') FROM unnest($1::text[]) relation`,
		[]string{"tenants", "memberships", "schedules", "task_run_snapshots",
			"research_run_plans", "research_run_steps", "research_run_evidence",
			"research_brief_syntheses", "research_run_step_spend_reservations",
			"research_run_step_spend_settlements", "research_run_llm_spend_reservations",
			"research_run_llm_spend_settlements", "research_llm_gateway_verifier_keys",
			"research_llm_gateway_attempts", "research_llm_gateway_frozen_requests",
			"research_llm_process_gateway_settlements", "provider_price_rules", "llm_calls", "tool_calls"},
	).Scan(&directDML, &canCreatePublic); err != nil {
		return fmt.Errorf("inspect gateway direct grants: %w", err)
	}
	if directDML != 0 || canCreatePublic {
		return fmt.Errorf("gateway capability has direct mutation grants: count=%d create=%v", directDML, canCreatePublic)
	}
	var claimV2, settleV2, recoverV2, signedSettle, oldSettle, activeKey, signPayload, readsSecret, readsCalls bool
	if err := tx.QueryRow(ctx, `SELECT
		has_function_privilege(current_user,
		 'claim_research_llm_gateway_request_v2(bigint,text,text)','EXECUTE'),
		has_function_privilege(current_user,
		 'settle_research_llm_gateway_request_v2(bigint,text,text,text,text,integer,integer,integer,integer,integer,integer,boolean,text,boolean,boolean,boolean,text,text)','EXECUTE'),
		has_function_privilege(current_user,
		 'recover_research_llm_gateway_request_v2(bigint,text,text)','EXECUTE'),
		has_function_privilege(current_user,
		 'settle_signed_research_run_llm_spend_v3(bigint,bigint,text,bigint,bigint,text,text,text,text,text,text,integer,integer,integer,integer,integer,integer,boolean,real,integer,boolean,text,boolean,boolean,boolean,text,text,text,bigint,bytea)','EXECUTE'),
		has_function_privilege(current_user,
		 'settle_research_run_llm_spend_v3(bigint,bigint,text,bigint,bigint,text,text,text,text,integer,integer,integer,integer,integer,integer,boolean,real,integer,boolean,text,boolean,boolean,boolean,text,text)','EXECUTE'),
		has_function_privilege(current_user,'active_research_llm_gateway_key_id_v1()','EXECUTE'),
		has_function_privilege(current_user,'sign_research_llm_gateway_payload_v1(text,bytea,bigint,text,boolean)','EXECUTE'),
		has_column_privilege(current_user,'research_llm_gateway_verifier_keys','secret','SELECT'),
		has_table_privilege(current_user,'llm_calls','SELECT')`,
	).Scan(&claimV2, &settleV2, &recoverV2, &signedSettle, &oldSettle, &activeKey,
		&signPayload, &readsSecret, &readsCalls); err != nil {
		return fmt.Errorf("inspect gateway privileges: %w", err)
	}
	if !claimV2 || !settleV2 || !recoverV2 || signedSettle || oldSettle || activeKey ||
		signPayload || readsSecret || readsCalls {
		return errors.New("gateway capability privileges are unsafe")
	}
	rows, err := tx.Query(ctx, `SELECT proc.proname FROM pg_proc proc JOIN pg_namespace ns
		ON ns.oid=proc.pronamespace WHERE ns.nspname='public' AND proc.prosecdef
		AND has_function_privilege(current_user,proc.oid,'EXECUTE') ORDER BY proc.proname`)
	if err != nil {
		return err
	}
	defer rows.Close()
	var definers []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return err
		}
		definers = append(definers, name)
	}
	expected := []string{"claim_research_llm_gateway_request_v2", "recover_research_llm_gateway_request_v2", "settle_research_llm_gateway_request_v2"}
	if fmt.Sprint(definers) != fmt.Sprint(expected) {
		return fmt.Errorf("gateway SECURITY DEFINER allowlist differs: %v", definers)
	}
	rows.Close()
	for _, forbidden := range []string{researchRuntimeCapabilityRole, "vane_app"} {
		if _, err := tx.Exec(ctx, `SAVEPOINT gateway_forbidden_role`); err != nil {
			return err
		}
		_, roleErr := tx.Exec(ctx, `SET LOCAL ROLE `+forbidden)
		if roleErr == nil || !isPermissionDeniedV3(roleErr) {
			return fmt.Errorf("gateway can SET ROLE %s: %v", forbidden, roleErr)
		}
		if _, err := tx.Exec(ctx, `ROLLBACK TO SAVEPOINT gateway_forbidden_role`); err != nil {
			return err
		}
	}
	if _, err := tx.Exec(ctx, `RESET ROLE`); err != nil {
		return err
	}
	var resetUser string
	if err := tx.QueryRow(ctx, `SELECT current_user`).Scan(&resetUser); err != nil {
		return err
	}
	if resetUser != researchGatewayLoginRole {
		return fmt.Errorf("gateway RESET ROLE escaped login: %s", resetUser)
	}
	var resetPrivilege bool
	if err := tx.QueryRow(ctx, `SELECT
		has_table_privilege(current_user,'provider_price_rules','UPDATE') OR
		has_table_privilege(current_user,'llm_calls','SELECT') OR
		has_column_privilege(current_user,'research_llm_gateway_verifier_keys','secret','SELECT')`).Scan(&resetPrivilege); err != nil {
		return err
	}
	if resetPrivilege {
		return errors.New("gateway login retains privilege after RESET ROLE")
	}
	return nil
}

// ValidateResearchGatewayConnectionV1 is shared by the isolated gateway
// process. Keeping one strict probe prevents authority drift between binaries.
func ValidateResearchGatewayConnectionV1(ctx context.Context, conn *pgx.Conn) error {
	return validateResearchGatewayConnection(ctx, conn)
}

func (s *Store) beginScopedResearchGatewayTransactionV3(
	ctx context.Context, options pgx.TxOptions, identity types.RunIdentity, snapshotID int64,
) (pgx.Tx, types.ResearchRunSnapshotRefV3, error) {
	ref, err := s.loadControlResearchRunSnapshotRefV3(ctx, identity, snapshotID)
	if err != nil {
		return nil, types.ResearchRunSnapshotRefV3{}, err
	}
	capability, err := s.resolveResearchRunCapabilityV1(ctx, ref)
	if err != nil {
		return nil, types.ResearchRunSnapshotRefV3{}, err
	}
	tx, err := s.beginGatewayTx(ctx, options)
	if err != nil {
		return nil, types.ResearchRunSnapshotRefV3{}, err
	}
	fail := func(stage string, cause error) (pgx.Tx, types.ResearchRunSnapshotRefV3, error) {
		_ = tx.Rollback(context.WithoutCancel(ctx))
		return nil, types.ResearchRunSnapshotRefV3{}, researchRunDatabaseError(stage, cause)
	}
	if _, err := tx.Exec(ctx, `SET LOCAL search_path=pg_catalog,public,pg_temp`); err != nil {
		return fail("set gateway search path", err)
	}
	if _, err := tx.Exec(ctx, `SELECT set_config('app.research_run_capability_v1',$1,true),
		set_config('app.tenant_id',$2,true),set_config('app.user_id',$3,true)`,
		hex.EncodeToString(capability.raw[:]), fmt.Sprint(identity.TenantID),
		fmt.Sprint(identity.UserID)); err != nil {
		return fail("install gateway run capability", err)
	}
	if _, err := tx.Exec(ctx, `SET LOCAL ROLE `+researchGatewayCapabilityRole); err != nil {
		return fail("enter gateway role", err)
	}
	if _, err := tx.Exec(ctx, `SELECT require_research_run_capability_v1($1,$2,$3,$4,$5,$6,$7)`,
		ref.SnapshotID, ref.ReferenceDigest, identity.TenantID, identity.UserID,
		identity.TaskID, identity.TemporalWorkflowID, identity.TemporalRunID); err != nil {
		return fail("verify gateway run capability", err)
	}
	return tx, ref, nil
}

// FinalizeMeasuredResearchLLMGatewayReceiptV3 signs only a receipt whose
// request and attempt state were frozen at the gateway boundary.
func (s *Store) FinalizeMeasuredResearchLLMGatewayReceiptV3(
	ctx context.Context, binding types.ResearchLLMGatewayCallBindingV3,
	receipt types.ResearchLLMGatewayReceiptV3,
) (types.ResearchLLMGatewayReceiptV3, error) {
	if receipt.ReservationID != binding.ReservationID ||
		receipt.RequestDigest != binding.RequestDigest {
		return types.ResearchLLMGatewayReceiptV3{}, researchRunValidationError("gateway receipt binding differs")
	}
	tx, err := s.beginGatewayTx(ctx, pgx.TxOptions{})
	if err != nil {
		return types.ResearchLLMGatewayReceiptV3{}, err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if _, err := tx.Exec(ctx, `SET LOCAL ROLE `+researchGatewayCapabilityRole); err != nil {
		return types.ResearchLLMGatewayReceiptV3{}, err
	}
	if err := tx.QueryRow(ctx, `SELECT active_research_llm_gateway_key_id_v1()`).Scan(&receipt.KeyID); err != nil {
		return types.ResearchLLMGatewayReceiptV3{}, err
	}
	payload, err := receipt.CanonicalPayload()
	if err != nil {
		return types.ResearchLLMGatewayReceiptV3{}, err
	}
	if err := tx.QueryRow(ctx, `SELECT sign_research_llm_gateway_payload_v1($1,$2,$3,$4,$5)`,
		receipt.KeyID, payload, binding.ReservationID, binding.RequestDigest,
		receipt.Attempted).Scan(&receipt.Signature); err != nil {
		return types.ResearchLLMGatewayReceiptV3{}, err
	}
	if len(receipt.Signature) != 32 {
		return types.ResearchLLMGatewayReceiptV3{}, researchRunIntegrityError()
	}
	if err := tx.Commit(ctx); err != nil {
		return types.ResearchLLMGatewayReceiptV3{}, err
	}
	return receipt, nil
}

func (s *Store) PrepareResearchLLMGatewayReceiptV3(
	ctx context.Context, binding types.ResearchLLMGatewayCallBindingV3,
) error {
	if binding.ReservationID <= 0 || binding.RequestDigest == "" ||
		binding.SnapshotRef.ValidateFor(binding.Identity) != nil {
		return researchRunValidationError("gateway call binding is invalid")
	}
	tx, _, err := s.beginScopedResearchGatewayTransactionV3(ctx, pgx.TxOptions{},
		binding.Identity, binding.SnapshotRef.SnapshotID)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	var keyID string
	if err := tx.QueryRow(ctx, `SELECT active_research_llm_gateway_key_id_v1()`).Scan(&keyID); err != nil {
		return err
	}
	if keyID == "" {
		return researchRunIntegrityError()
	}
	return tx.Commit(ctx)
}

func (s *Store) MarkResearchLLMGatewaySendStartedV3(
	ctx context.Context, intent types.ResearchLLMGatewaySendIntentV3,
) (bool, error) {
	return s.markResearchLLMGatewayIntentV3(ctx, intent, true)
}

func (s *Store) markResearchLLMGatewayIntentV3(ctx context.Context,
	intent types.ResearchLLMGatewaySendIntentV3, sendStarted bool) (bool, error) {
	binding := intent.Binding
	if binding.ReservationID <= 0 || binding.RequestDigest == "" ||
		binding.SnapshotRef.ValidateFor(binding.Identity) != nil {
		return false, researchRunValidationError("gateway send binding is invalid")
	}
	tx, ref, err := s.beginScopedResearchGatewayTransactionV3(ctx, pgx.TxOptions{},
		binding.Identity, binding.SnapshotRef.SnapshotID)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if ref != binding.SnapshotRef {
		return false, researchRunIntegrityError()
	}
	call := intent.Call
	if call.Temperature == nil || call.MaxTokens == nil {
		return false, researchRunValidationError("gateway send intent is incomplete")
	}
	var first bool
	args := []any{binding.ReservationID, binding.RequestDigest, call.SystemPrompt,
		call.UserPrompt, call.Provider, call.Model, *call.Temperature, *call.MaxTokens,
		intent.DisableThinking}
	query := `SELECT mark_research_llm_gateway_pre_send_rejected_v1(
		$1,$2,$3,$4,$5,$6,$7,$8,$9)`
	if sendStarted {
		query = `SELECT mark_research_llm_gateway_send_started_v1(
			$1,$2,$3,$4,$5,$6,$7,$8,$9)`
	}
	if err := tx.QueryRow(ctx, query, args...).Scan(&first); err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return first, nil
}

func (s *Store) MarkResearchLLMGatewayPreSendRejectedV3(
	ctx context.Context, intent types.ResearchLLMGatewaySendIntentV3,
) (bool, error) {
	return s.markResearchLLMGatewayIntentV3(ctx, intent, false)
}

func (s *Store) ResearchLLMGatewayAttemptStartedV3(
	ctx context.Context, binding types.ResearchLLMGatewayCallBindingV3,
) (types.ResearchLLMGatewayAttemptStateV3, error) {
	if binding.ReservationID <= 0 || binding.RequestDigest == "" ||
		binding.SnapshotRef.ValidateFor(binding.Identity) != nil {
		return types.ResearchLLMGatewayAttemptNoneV3, researchRunValidationError("gateway attempt binding is invalid")
	}
	tx, ref, err := s.beginScopedResearchGatewayTransactionV3(ctx,
		pgx.TxOptions{AccessMode: pgx.ReadOnly}, binding.Identity,
		binding.SnapshotRef.SnapshotID)
	if err != nil {
		return types.ResearchLLMGatewayAttemptNoneV3, err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if ref != binding.SnapshotRef {
		return types.ResearchLLMGatewayAttemptNoneV3, researchRunIntegrityError()
	}
	var state types.ResearchLLMGatewayAttemptStateV3
	if err := tx.QueryRow(ctx, `SELECT research_llm_gateway_attempt_started_v1($1,$2)`,
		binding.ReservationID, binding.RequestDigest).Scan(&state); err != nil {
		return types.ResearchLLMGatewayAttemptNoneV3, err
	}
	if err := tx.Commit(ctx); err != nil {
		return types.ResearchLLMGatewayAttemptNoneV3, err
	}
	if state != types.ResearchLLMGatewayAttemptNoneV3 && state != types.ResearchLLMGatewayAttemptSendStartedV3 && state != types.ResearchLLMGatewayAttemptPreSendRejectedV3 {
		return types.ResearchLLMGatewayAttemptNoneV3, researchRunIntegrityError()
	}
	return state, nil
}

// SignConservativeResearchLLMGatewayRecoveryV3 creates the only receipt
// allowed after the normal reservation time window. Its exact request skeleton
// comes from the durable pre-send marker, never from caller-supplied facts.
func (s *Store) SignConservativeResearchLLMGatewayRecoveryV3(
	ctx context.Context, binding types.ResearchLLMGatewayCallBindingV3,
) (types.ResearchLLMGatewayReceiptV3, error) {
	return s.signResearchLLMGatewayRecoveryV3(ctx, binding,
		types.ResearchLLMGatewayAttemptSendStartedV3)
}

func (s *Store) SignConfirmedZeroResearchLLMGatewayRecoveryV3(
	ctx context.Context, binding types.ResearchLLMGatewayCallBindingV3,
) (types.ResearchLLMGatewayReceiptV3, error) {
	return s.signResearchLLMGatewayRecoveryV3(ctx, binding,
		types.ResearchLLMGatewayAttemptPreSendRejectedV3)
}

func (s *Store) signResearchLLMGatewayRecoveryV3(ctx context.Context,
	binding types.ResearchLLMGatewayCallBindingV3,
	state types.ResearchLLMGatewayAttemptStateV3) (types.ResearchLLMGatewayReceiptV3, error) {
	tx, ref, err := s.beginScopedResearchGatewayTransactionV3(ctx, pgx.TxOptions{},
		binding.Identity, binding.SnapshotRef.SnapshotID)
	if err != nil {
		return types.ResearchLLMGatewayReceiptV3{}, err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if ref != binding.SnapshotRef {
		return types.ResearchLLMGatewayReceiptV3{}, researchRunIntegrityError()
	}
	var call types.LLMCall
	var disable bool
	var temperature float32
	var maxTokens int
	var stage string
	if err := tx.QueryRow(ctx, `SELECT out_system_prompt,out_user_prompt,out_provider,
		out_model,out_temperature,out_max_tokens,out_disable_thinking,out_trace_id,out_stage
		FROM load_research_llm_gateway_recovery_intent_v1($1,$2,$3)`,
		binding.ReservationID, binding.RequestDigest, string(state)).Scan(&call.SystemPrompt, &call.UserPrompt,
		&call.Provider, &call.Model, &temperature, &maxTokens, &disable, &call.TraceID, &stage); err != nil {
		return types.ResearchLLMGatewayReceiptV3{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return types.ResearchLLMGatewayReceiptV3{}, err
	}
	tenantID, userID, snapshotID := binding.Identity.TenantID, binding.Identity.UserID,
		binding.SnapshotRef.SnapshotID
	call.RunSnapshotID = &snapshotID
	call.TenantID = &tenantID
	call.UserID = &userID
	call.RefType = types.RefType(researchRunLLMRefTypeV3)
	call.RefID = &snapshotID
	call.SpanName = researchRunLLMSpanV3(stage)
	call.Temperature = &temperature
	call.MaxTokens = &maxTokens
	call.Error = "gateway recovery: provider outcome unavailable"
	receipt := types.ResearchLLMGatewayReceiptV3{
		SchemaVersion:      types.ResearchLLMGatewayReceiptSchemaV3,
		SignedAtUnixMillis: time.Now().UTC().UnixMilli(), ReservationID: binding.ReservationID,
		RequestDigest: binding.RequestDigest, Call: call, DisableThinking: disable,
		Attempted: true, UsageKnown: false, DefinitelyZeroUsage: false,
		Outcome: "indeterminate", ErrorCode: string(types.CodeLLMUnavailable),
	}
	if state == types.ResearchLLMGatewayAttemptPreSendRejectedV3 {
		call.Error = "gateway recovery: pre-send rejection"
		receipt.Call = call
		receipt.Attempted = false
		receipt.DefinitelyZeroUsage = true
		receipt.Outcome = "failed"
		receipt.ErrorCode = string(types.CodeLLMBadRequest)
	}
	return s.FinalizeMeasuredResearchLLMGatewayReceiptV3(ctx, binding, receipt)
}

func isPermissionDeniedV3(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "42501"
}
