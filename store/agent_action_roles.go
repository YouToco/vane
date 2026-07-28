package store

import (
	"context"
	"fmt"
	"slices"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/types"
)

const (
	agentActionOperatorRole    = "vane_agent_action_operator"
	agentActionContinuatorRole = "vane_agent_action_continuator"
	agentActionProposerRole    = "vane_agent_action_proposer"
)

func validateAgentActionProposer(ctx context.Context, tx pgx.Tx) error {
	if err := validateAgentActionRole(
		ctx, tx, agentActionProposerRole,
		agentActionProposerSelectColumns(),
		agentActionProposerInsertColumns(),
		nil,
		nil,
		[]string{
			"agent_action_continuation_authority_events_id_seq:USAGE",
		},
	); err != nil {
		return err
	}
	var unrelated bool
	if err := tx.QueryRow(ctx, `
		SELECT NOT EXISTS (
		         SELECT 1
		           FROM pg_auth_members am
		           JOIN pg_roles granted_role ON granted_role.oid=am.roleid
		           JOIN pg_roles member_role ON member_role.oid=am.member
		          WHERE (
		                   granted_role.rolname=$1
		               AND member_role.rolname=ANY($2::name[])
		                ) OR (
		                   member_role.rolname=$1
		               AND granted_role.rolname=ANY($2::name[])
		                )
		       )`,
		agentActionProposerRole,
		[]string{
			"vane_app",
			agentActionOperatorRole,
			agentActionContinuatorRole,
		},
	).Scan(&unrelated); err != nil || !unrelated {
		return agentActionRoleDrift(
			agentActionProposerRole,
			fmt.Sprintf("peer roles unrelated=%v err=%v", unrelated, err),
		)
	}
	return nil
}

func validateAgentActionOperator(ctx context.Context, tx pgx.Tx) error {
	return validateAgentActionRole(
		ctx, tx, agentActionOperatorRole,
		agentActionOperatorSelectColumns(),
		agentActionOperatorInsertColumns(),
		agentActionOperatorUpdateColumns(),
		nil,
		[]string{
			"agent_action_continuation_authority_events_id_seq:USAGE",
		},
	)
}

func validateAgentActionContinuator(ctx context.Context, tx pgx.Tx) error {
	return validateAgentActionRole(
		ctx, tx, agentActionContinuatorRole,
		agentActionContinuatorSelectColumns(),
		agentActionContinuatorInsertColumns(),
		agentActionContinuatorUpdateColumns(),
		[]string{"subscriptions"},
		[]string{"agent_events_id_seq:USAGE"},
	)
}

func validateAgentActionRole(
	ctx context.Context,
	tx pgx.Tx,
	roleName string,
	expectedSelect, expectedInsert, expectedUpdate, expectedDelete,
	expectedSequences []string,
) error {
	var boundaryValid bool
	err := tx.QueryRow(ctx, `
		SELECT NOT r.rolsuper AND NOT r.rolcreatedb AND
		       NOT r.rolcreaterole AND NOT r.rolcanlogin AND
		       NOT r.rolinherit AND NOT r.rolreplication AND
		       NOT r.rolbypassrls AND
		       r.rolconfig=ARRAY['search_path=pg_catalog, public']::text[] AND
		       pg_has_role(CURRENT_USER,r.oid,'SET') AND
		       NOT pg_has_role('vane_app',r.oid,'MEMBER') AND
		       NOT pg_has_role(r.oid,'vane_app','MEMBER') AND
		       NOT pg_has_role(
		           'vane_agent_action_operator',
		           'vane_agent_action_continuator','MEMBER'
		       ) AND
		       NOT pg_has_role(
		           'vane_agent_action_continuator',
		           'vane_agent_action_operator','MEMBER'
		       ) AND
		       has_schema_privilege(r.oid,'public','USAGE') AND
		       NOT has_schema_privilege(r.oid,'public','CREATE') AND
		       NOT EXISTS (
		         SELECT 1 FROM pg_auth_members am
		          WHERE am.roleid=r.oid
		            AND am.member<>(SELECT oid FROM pg_roles
		                              WHERE rolname=CURRENT_USER)
		       ) AND
		       EXISTS (
		         SELECT 1 FROM pg_auth_members am
		          WHERE am.roleid=r.oid
		            AND am.member=(SELECT oid FROM pg_roles
		                             WHERE rolname=CURRENT_USER)
		       ) AND
		       NOT EXISTS (
		         SELECT 1 FROM pg_auth_members am WHERE am.member=r.oid
		       ) AND
		       NOT EXISTS (
		         SELECT 1 FROM pg_namespace n
		          WHERE n.nspname<>'public'
		            AND n.nspname<>'information_schema'
		            AND n.nspname NOT LIKE 'pg_%'
		            AND (
		              n.nspowner=r.oid OR
		              has_schema_privilege(r.oid,n.oid,'USAGE') OR
		              has_schema_privilege(r.oid,n.oid,'CREATE')
		            )
		       ) AND
		       NOT EXISTS (
		         SELECT 1 FROM pg_class c
		         JOIN pg_namespace n ON n.oid=c.relnamespace
		          WHERE n.nspname='public'
		            AND c.relkind IN ('r','p','v','m','f')
		            AND (
		              c.relowner=r.oid OR
		              has_table_privilege(
		                r.oid,c.oid,
		                'TRUNCATE,REFERENCES,TRIGGER,MAINTAIN'
		              )
		            )
		       ) AND
		       NOT EXISTS (
		         SELECT 1 FROM pg_proc p
		         JOIN pg_namespace n ON n.oid=p.pronamespace
		          WHERE n.nspname='public' AND p.prosecdef
		            AND has_function_privilege(r.oid,p.oid,'EXECUTE')
		       )
		  FROM pg_roles r WHERE r.rolname=$1`,
		roleName,
	).Scan(&boundaryValid)
	if err != nil || !boundaryValid {
		return agentActionRoleDrift(
			roleName, fmt.Sprintf("boundary valid=%v err=%v", boundaryValid, err),
		)
	}

	for privilege, expected := range map[string][]string{
		"SELECT":     expectedSelect,
		"INSERT":     expectedInsert,
		"UPDATE":     expectedUpdate,
		"REFERENCES": {},
	} {
		actual, err := agentActionRoleColumnPrivileges(
			ctx, tx, roleName, privilege,
		)
		if err != nil || !slices.Equal(actual, expected) {
			return agentActionRoleDrift(roleName, fmt.Sprintf(
				"%s actual=%v expected=%v err=%v",
				privilege, actual, expected, err,
			))
		}
	}
	deleteTables, err := agentActionRoleTablePrivileges(
		ctx, tx, roleName, "DELETE",
	)
	if err != nil || !slices.Equal(deleteTables, expectedDelete) {
		return agentActionRoleDrift(roleName, fmt.Sprintf(
			"DELETE actual=%v expected=%v err=%v",
			deleteTables, expectedDelete, err,
		))
	}
	sequences, err := agentActionRoleSequencePrivileges(
		ctx, tx, roleName,
	)
	if err != nil || !slices.Equal(sequences, expectedSequences) {
		return agentActionRoleDrift(roleName, fmt.Sprintf(
			"sequences actual=%v expected=%v err=%v",
			sequences, expectedSequences, err,
		))
	}
	return nil
}

func agentActionRoleTablePrivileges(
	ctx context.Context,
	tx pgx.Tx,
	roleName string,
	privilege string,
) ([]string, error) {
	rows, err := tx.Query(ctx, `
		SELECT c.relname
		  FROM pg_class c
		  JOIN pg_namespace n ON n.oid=c.relnamespace
		 WHERE n.nspname='public'
		   AND c.relkind IN ('r','p','v','m','f')
		   AND has_table_privilege($1::name,c.oid,$2)
		 ORDER BY c.relname`,
		roleName, privilege,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	values := make([]string, 0)
	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, rows.Err()
}

func agentActionRoleDrift(roleName, detail string) error {
	return types.NewAppError(
		types.CodeInternal,
		fmt.Sprintf("Agent action role %s capability drift: %s", roleName, detail),
		nil,
	)
}

func agentActionRoleColumnPrivileges(
	ctx context.Context,
	tx pgx.Tx,
	roleName string,
	privilege string,
) ([]string, error) {
	rows, err := tx.Query(ctx, `
		SELECT c.relname||'.'||a.attname
		  FROM pg_attribute a
		  JOIN pg_class c ON c.oid=a.attrelid
		  JOIN pg_namespace n ON n.oid=c.relnamespace
		 WHERE n.nspname='public'
		   AND c.relkind IN ('r','p','v','m','f')
		   AND a.attnum>0 AND NOT a.attisdropped
		   AND has_column_privilege($1::name,a.attrelid,a.attname,$2)
		 ORDER BY c.relname,a.attname`,
		roleName, privilege,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	values := make([]string, 0)
	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, rows.Err()
}

func agentActionRoleSequencePrivileges(
	ctx context.Context,
	tx pgx.Tx,
	roleName string,
) ([]string, error) {
	rows, err := tx.Query(ctx, `
		SELECT c.relname,
		       has_sequence_privilege($1::name,c.oid,'USAGE'),
		       has_sequence_privilege($1::name,c.oid,'SELECT'),
		       has_sequence_privilege($1::name,c.oid,'UPDATE')
		  FROM pg_class c
		  JOIN pg_namespace n ON n.oid=c.relnamespace
		 WHERE n.nspname='public' AND c.relkind='S'
		 ORDER BY c.relname`,
		roleName,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	values := make([]string, 0)
	for rows.Next() {
		var name string
		var usage, selectPrivilege, update bool
		if err := rows.Scan(
			&name, &usage, &selectPrivilege, &update,
		); err != nil {
			return nil, err
		}
		if usage {
			values = append(values, name+":USAGE")
		}
		if selectPrivilege {
			values = append(values, name+":SELECT")
		}
		if update {
			values = append(values, name+":UPDATE")
		}
	}
	return values, rows.Err()
}

func prefixedColumns(table string, columns ...string) []string {
	values := make([]string, 0, len(columns))
	for _, column := range columns {
		values = append(values, fmt.Sprintf("%s.%s", table, column))
	}
	return values
}

func mergeSortedColumns(groups ...[]string) []string {
	values := make([]string, 0)
	for _, group := range groups {
		values = append(values, group...)
	}
	slices.Sort(values)
	return values
}

func withoutColumns(values []string, excluded ...string) []string {
	exclusion := make(map[string]struct{}, len(excluded))
	for _, value := range excluded {
		exclusion[value] = struct{}{}
	}
	result := make([]string, 0, len(values))
	for _, value := range values {
		if _, omit := exclusion[value]; !omit {
			result = append(result, value)
		}
	}
	return result
}

func agentActionOperatorRootSelectColumns() []string {
	return prefixedColumns(
		"pending_actions",
		"args", "execution_version", "expires_at", "id", "session_id",
		"status", "tenant_id", "tool_name", "user_id",
	)
}

func agentActionContinuatorRootSelectColumns() []string {
	return prefixedColumns(
		"pending_actions",
		"execution_version", "id", "session_id", "status", "tenant_id",
		"user_id",
	)
}

func agentActionContinuationSelectColumns() []string {
	return prefixedColumns(
		"agent_action_continuations",
		"action_id", "adapter_version", "args_digest", "attempt_count",
		"blocked_reason", "canonical_args", "completed_at", "confirmed_at",
		"created_at", "lease_expires_at", "lease_fence", "lease_owner",
		"next_attempt_at", "not_found_digest", "not_found_messages",
		"session_id", "source_id", "status", "success_digest",
		"success_messages", "tenant_id", "terminal_code", "tool_name",
		"tool_policy", "tool_policy_digest", "tool_policy_version",
		"tool_spec", "tool_spec_digest", "tool_spec_version", "updated_at",
		"user_id",
	)
}

func agentActionAuthoritySelectColumns() []string {
	return prefixedColumns(
		"agent_action_continuation_authority_events",
		"action_id", "evidence", "generation", "mode", "tenant_id", "user_id",
	)
}

func agentActionContinuatorAuthoritySelectColumns() []string {
	return prefixedColumns(
		"agent_action_continuation_authority_events",
		"action_id", "generation", "mode", "tenant_id",
	)
}

func agentActionOperatorSelectColumns() []string {
	return mergeSortedColumns(
		agentActionOperatorRootSelectColumns(),
		agentActionContinuationSelectColumns(),
		agentActionAuthoritySelectColumns(),
	)
}

func agentActionProposerSelectColumns() []string {
	return mergeSortedColumns(
		prefixedColumns(
			"pending_actions",
			"args", "execution_version", "expires_at", "id", "session_id",
			"status", "summary", "tenant_id", "tool_name", "user_id",
		),
		withoutColumns(
			agentActionContinuationSelectColumns(),
			"agent_action_continuations.created_at",
			"agent_action_continuations.updated_at",
		),
		prefixedColumns(
			"agent_action_continuation_authority_events",
			"action_id", "evidence", "generation", "mode",
		),
	)
}

func agentActionProposerInsertColumns() []string {
	return mergeSortedColumns(
		prefixedColumns(
			"pending_actions",
			"args", "execution_version", "expires_at", "id", "session_id",
			"status", "summary", "tenant_id", "tool_name", "user_id",
		),
		prefixedColumns(
			"agent_action_continuations",
			"action_id", "adapter_version", "args_digest", "canonical_args",
			"next_attempt_at",
			"not_found_digest", "not_found_messages", "session_id", "source_id",
			"success_digest", "success_messages", "tenant_id", "tool_name",
			"tool_policy", "tool_policy_digest", "tool_policy_version",
			"tool_spec", "tool_spec_digest", "tool_spec_version", "user_id",
		),
		prefixedColumns(
			"agent_action_continuation_authority_events",
			"action_id", "evidence", "generation", "mode", "tenant_id",
			"user_id",
		),
	)
}

func agentActionOperatorInsertColumns() []string {
	return mergeSortedColumns(
		prefixedColumns(
			"agent_action_continuation_authority_events",
			"action_id", "evidence", "generation", "mode", "tenant_id",
			"user_id",
		),
		prefixedColumns(
			"agent_action_continuations",
			"action_id", "adapter_version", "args_digest", "canonical_args",
			"not_found_digest", "not_found_messages", "session_id", "source_id",
			"success_digest", "success_messages", "tenant_id", "tool_name",
			"tool_policy", "tool_policy_digest", "tool_policy_version",
			"tool_spec", "tool_spec_digest", "tool_spec_version", "user_id",
		),
	)
}

func agentActionOperatorUpdateColumns() []string {
	return mergeSortedColumns(
		prefixedColumns(
			"agent_action_continuations", "status", "updated_at",
		),
		prefixedColumns("pending_actions", "execution_version"),
	)
}

func agentActionContinuatorSelectColumns() []string {
	return mergeSortedColumns(
		agentActionContinuatorRootSelectColumns(),
		agentActionContinuationSelectColumns(),
		agentActionContinuatorAuthoritySelectColumns(),
		prefixedColumns(
			"agent_events",
			"batch_digest", "batch_idempotency_key", "batch_index",
			"batch_size", "created_at", "id", "kind", "payload",
			"payload_digest", "schema_version", "sequence", "session_id",
			"tenant_id", "user_id",
		),
		prefixedColumns(
			"agent_session_projection_authority_events",
			"action", "created_at", "generation", "id", "ledger_digest",
			"ledger_head_sequence", "legacy_digest", "session_id",
			"tenant_id", "user_id",
		),
		prefixedColumns(
			"agent_sessions",
			"activated_tools", "id", "messages", "tenant_id", "turn_count",
			"user_id",
		),
		prefixedColumns("schedule_sources", "schedule_id", "source_id"),
		prefixedColumns("schedules", "id", "tenant_id", "user_id"),
		prefixedColumns("sources", "id"),
		prefixedColumns(
			"subscriptions", "source_id", "status", "tenant_id", "user_id",
		),
	)
}

func agentActionContinuatorInsertColumns() []string {
	return prefixedColumns(
		"agent_events",
		"batch_digest", "batch_idempotency_key", "batch_index",
		"batch_size", "kind", "payload", "payload_digest", "schema_version",
		"sequence", "session_id", "tenant_id", "user_id",
	)
}

func agentActionContinuatorUpdateColumns() []string {
	return mergeSortedColumns(
		prefixedColumns(
			"agent_action_continuations",
			"attempt_count", "blocked_reason", "completed_at", "confirmed_at",
			"lease_expires_at", "lease_fence", "lease_owner",
			"next_attempt_at", "status", "terminal_code", "updated_at",
		),
		prefixedColumns("agent_sessions", "messages"),
		prefixedColumns(
			"pending_actions", "executed_at", "status", "updated_at",
		),
		prefixedColumns(
			"sources", "fail_count", "next_fetch_at", "status", "updated_at",
		),
	)
}
