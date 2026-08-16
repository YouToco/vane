package store

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"
)

const serverRuntimeLoginRole = "vane_server_runtime"
const nativeV3EditRecoveryRuntimeLoginRole = "vane_native_v3_edit_recovery_runtime"
const nativeV3EditRecoveryCapabilityRole = "vane_native_v3_edit_recovery"
const workspaceMemoryRuntimeCapabilityRole = "vane_workspace_memory_editor"

// serverRuntimeCapabilityRoles is the exact always-on set. Workspace memory is
// deliberately optional for one bridge release: the migration creates its
// isolated role, but the bridge keeps provisioning v129 so the previous binary
// can still start on the upgraded schema. A later release may activate the
// separately verified optional role without making this bridge unsafe as a
// rollback binary. Paid research execution and retired/provider roles do not
// belong in the long-lived application process.
var serverRuntimeCapabilityRoles = []string{
	"vane_agent_session_projection_operator",
	"vane_app",
	"vane_brief_reader",
	"vane_brief_synthesis_recovery",
	"vane_brief_synthesis_writer",
	"vane_brief_writer",
	"vane_edit_coordinator",
	"vane_edit_receipt",
	"vane_intelligence_reader",
	"vane_memory_editor",
	"vane_periodic_brief_writer",
	"vane_profile_claim_editor",
	"vane_profile_editor",
	"vane_profile_epoch_editor",
	"vane_push_batch_authority",
	"vane_push_effect_coordinator",
	"vane_push_effect_operator",
	"vane_push_effect_receipt",
	"vane_run_outcome_recovery",
	"vane_schedule_commander",
	"vane_snapshot_cutover_operator",
}

var serverRuntimeForbiddenRoles = []string{
	"vane_agent_session_fact_projector",
	"vane_research_llm_gateway",
	"vane_research_llm_gateway_runtime",
	"vane_research_runtime",
	"vane_research_v3_executor",
}

var serverRuntimeProtectedRelations = []string{
	"public.research_llm_gateway_attempts",
	"public.research_llm_gateway_frozen_requests",
	"public.research_llm_gateway_verifier_keys",
	"public.research_run_capabilities",
	"public.research_run_llm_spend_reservations",
	"public.research_run_llm_spend_settlements",
	"public.research_run_step_spend_settlements",
}

// Vane_app historically reads the LLM spend reservation/settlement ledger for
// user-facing accounting. Capability and frozen-request bytes are a stricter
// subset: no long-lived server capability may read them directly.
var serverRuntimeForbiddenReadRelations = []string{
	"public.research_llm_gateway_frozen_requests",
	"public.research_run_capabilities",
	"public.tenant_quota",
}

// ProvisionServerRuntime installs the cluster-global runtime shell only after
// the target database schema has migrated completely. This bridge release
// intentionally provisions v129 and leaves workspace memory dark; activating
// v138 requires a later release whose rollback base accepts the optional role.
// dbURL must authenticate directly as the migration/schema owner.
func ProvisionServerRuntime(ctx context.Context, dbURL string) error {
	return callServerRuntimeProvisioner(
		ctx, dbURL, "provision_vane_server_runtime_v129")
}

// DeprovisionServerRuntime accepts either bridge state and removes the optional
// v138 edge before delegating to v129's exact teardown. The shell must already
// be NOLOGIN. A dependency error is intentional evidence of drift and is never
// papered over with DROP OWNED.
func DeprovisionServerRuntime(ctx context.Context, dbURL string) error {
	return callServerRuntimeProvisioner(
		ctx, dbURL, "deprovision_vane_server_runtime_v138")
}

// ProvisionNativeV3EditRecoveryRuntime creates only the independent recovery
// login shell and binds its one opaque claim capability. Credential/login
// activation remains an operator/systemd responsibility.
func ProvisionNativeV3EditRecoveryRuntime(ctx context.Context, dbURL string) error {
	conn, err := pgx.Connect(ctx, dbURL)
	if err != nil {
		return fmt.Errorf("store: connect native V3 edit recovery provisioner: %w", err)
	}
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()
	_, err = conn.Exec(ctx, `DO $$ BEGIN
		IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='vane_native_v3_edit_recovery_runtime') THEN
			CREATE ROLE vane_native_v3_edit_recovery_runtime NOLOGIN NOSUPERUSER
				NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS;
		END IF;
		ALTER ROLE vane_native_v3_edit_recovery_runtime NOSUPERUSER NOCREATEDB
			NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS;
		IF EXISTS (SELECT 1 FROM pg_auth_members edge
			JOIN pg_roles granted ON granted.oid=edge.roleid
			JOIN pg_roles member ON member.oid=edge.member
			WHERE member.rolname='vane_native_v3_edit_recovery_runtime'
			  AND granted.rolname<>'vane_native_v3_edit_recovery') THEN
			RAISE EXCEPTION 'unsafe native V3 edit recovery runtime membership';
		END IF;
		IF EXISTS (SELECT 1 FROM pg_auth_members edge
			JOIN pg_roles granted ON granted.oid=edge.roleid
			JOIN pg_roles member ON member.oid=edge.member
			WHERE granted.rolname='vane_native_v3_edit_recovery'
			  AND member.rolname<>'vane_native_v3_edit_recovery_runtime') THEN
			RAISE EXCEPTION 'unsafe native V3 edit recovery capability member';
		END IF;
		REVOKE vane_native_v3_edit_recovery FROM vane_native_v3_edit_recovery_runtime;
		GRANT vane_native_v3_edit_recovery TO vane_native_v3_edit_recovery_runtime
			WITH ADMIN FALSE, SET TRUE, INHERIT FALSE;
	END $$`)
	if err != nil {
		return fmt.Errorf("store: provision native V3 edit recovery runtime: %w", err)
	}
	return nil
}

func DeprovisionNativeV3EditRecoveryRuntime(ctx context.Context, dbURL string) error {
	conn, err := pgx.Connect(ctx, dbURL)
	if err != nil {
		return fmt.Errorf("store: connect native V3 edit recovery deprovisioner: %w", err)
	}
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()
	_, err = conn.Exec(ctx, `DO $$ BEGIN
		IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='vane_native_v3_edit_recovery_runtime') THEN
			IF (SELECT rolcanlogin FROM pg_roles WHERE rolname='vane_native_v3_edit_recovery_runtime') THEN
				RAISE EXCEPTION 'native V3 edit recovery runtime must be NOLOGIN before deprovision';
			END IF;
			REVOKE vane_native_v3_edit_recovery FROM vane_native_v3_edit_recovery_runtime;
			DROP ROLE vane_native_v3_edit_recovery_runtime;
		END IF;
	END $$`)
	if err != nil {
		return fmt.Errorf("store: deprovision native V3 edit recovery runtime: %w", err)
	}
	return nil
}

func callServerRuntimeProvisioner(
	ctx context.Context, dbURL, functionName string,
) error {
	conn, err := pgx.Connect(ctx, dbURL)
	if err != nil {
		return fmt.Errorf("store: connect server runtime provisioner: %w", err)
	}
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()
	// functionName is selected only by the two closed current-version wrappers.
	// Historical schema tests invoke their exact provisioner directly; current
	// binaries must never silently fall back across a migration boundary.
	if _, err := conn.Exec(ctx, "SELECT public."+functionName+"()"); err != nil {
		return fmt.Errorf("store: %s: %w", functionName, err)
	}
	return nil
}

// NewServerRuntime creates the primary Store pool under the non-owner server
// login introduced by migration 098. Every physical connection is probed
// before use, then enters vane_app as its least-privilege default role. Store
// methods may still SET LOCAL ROLE to one of the exact capability memberships;
// RESET ROLE can only return to the inert vane_server_runtime session user.
func NewServerRuntime(ctx context.Context, runtimeURL string) (*Store, error) {
	pool, err := newStorePool(ctx, runtimeURL, initializeServerRuntimeConnection)
	if err != nil {
		return nil, fmt.Errorf("store: server runtime: %w", err)
	}
	store := newStore(pool, nil)
	store.readinessProbe = store.verifyServerRuntimeQuotaProjection
	return store, nil
}

// NewServerRuntimeWithResearchRuntimeCapability is the server-runtime
// equivalent of NewWithResearchRuntimeCapability. The primary pool must pass
// the migration-098 server boundary, while paid V3 execution remains on its
// existing independent research runtime and capability keyring. It never
// creates or accepts a provider-gateway pool.
func NewServerRuntimeWithResearchRuntimeCapability(
	ctx context.Context, runtimeURL, researchRuntimeURL string,
	capability ResearchRunCapabilityConfigV1,
) (*Store, error) {
	pool, err := newStorePool(ctx, runtimeURL, initializeServerRuntimeConnection)
	if err != nil {
		return nil, fmt.Errorf("store: server runtime: %w", err)
	}
	if strings.TrimSpace(researchRuntimeURL) == "" {
		store := newStore(pool, nil)
		store.readinessProbe = store.verifyServerRuntimeQuotaProjection
		if capability.ActiveKeyID != "" || capability.ActiveKeyHex != "" ||
			capability.RetiredKeys != "" {
			if err := store.configureResearchRunCapabilityV1(capability); err != nil {
				store.Close()
				return nil, fmt.Errorf("store: V3 research capability: %w", err)
			}
		}
		return store, nil
	}

	researchPool, err := newStorePool(
		ctx, researchRuntimeURL, validateResearchRuntimeConnection)
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("store: V3 research runtime: %w", err)
	}
	store := newStore(pool, researchPool)
	store.readinessProbe = store.verifyServerRuntimeQuotaProjection
	if capability.ActiveKeyID == "" || capability.ActiveKeyHex == "" {
		store.Close()
		return nil, errors.New("store: V3 research capability key is required")
	}
	if err := store.configureResearchRunCapabilityV1(capability); err != nil {
		store.Close()
		return nil, fmt.Errorf("store: V3 research capability: %w", err)
	}
	return store, nil
}

func NewServerRuntimeWithResearchRuntimeCapabilityAndEditRecovery(
	ctx context.Context, runtimeURL, researchRuntimeURL, editRecoveryRuntimeURL string,
	capability ResearchRunCapabilityConfigV1,
) (*Store, error) {
	store, err := NewServerRuntimeWithResearchRuntimeCapability(
		ctx, runtimeURL, researchRuntimeURL, capability)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(editRecoveryRuntimeURL) == "" {
		store.Close()
		return nil, errNativeV3EditRecoveryRuntimeUnavailable
	}
	recoveryPool, err := newStorePool(
		ctx, editRecoveryRuntimeURL, initializeNativeV3EditRecoveryRuntimeConnection)
	if err != nil {
		store.Close()
		return nil, fmt.Errorf("store: native V3 edit recovery runtime: %w", err)
	}
	store.editRecoveryPool = recoveryPool
	store.beginEditRecoveryTx = recoveryPool.BeginTx
	return store, nil
}

func initializeNativeV3EditRecoveryRuntimeConnection(ctx context.Context, conn *pgx.Conn) error {
	if conn == nil {
		return errors.New("native V3 edit recovery runtime connection is nil")
	}
	var sessionUser, currentUser string
	var login, super, bypass, createDB, createRole, inherit, replication bool
	if err := conn.QueryRow(ctx, `SELECT session_user,current_user,role.rolcanlogin,
		role.rolsuper,role.rolbypassrls,role.rolcreatedb,role.rolcreaterole,
		role.rolinherit,role.rolreplication FROM pg_roles role
		WHERE role.rolname=session_user`).Scan(&sessionUser, &currentUser, &login,
		&super, &bypass, &createDB, &createRole, &inherit, &replication); err != nil {
		return fmt.Errorf("read native V3 edit recovery identity: %w", err)
	}
	if sessionUser != nativeV3EditRecoveryRuntimeLoginRole ||
		currentUser != nativeV3EditRecoveryRuntimeLoginRole || !login || super || bypass ||
		createDB || createRole || inherit || replication {
		return errors.New("native V3 edit recovery runtime identity is unsafe")
	}
	var memberships, exactSafeMemberships, otherCapabilityMembers int
	if err := conn.QueryRow(ctx, `SELECT count(*),count(*) FILTER (
		WHERE granted.rolname='vane_native_v3_edit_recovery'
		  AND NOT edge.admin_option AND edge.set_option AND NOT edge.inherit_option),
		(SELECT count(*) FROM pg_auth_members capability_edge
		 JOIN pg_roles capability ON capability.oid=capability_edge.roleid
		 JOIN pg_roles capability_member ON capability_member.oid=capability_edge.member
		 WHERE capability.rolname='vane_native_v3_edit_recovery'
		   AND capability_member.rolname<>session_user)
		FROM pg_auth_members edge
		JOIN pg_roles granted ON granted.oid=edge.roleid
		JOIN pg_roles member ON member.oid=edge.member
		WHERE member.rolname=session_user`).Scan(
		&memberships, &exactSafeMemberships, &otherCapabilityMembers); err != nil {
		return fmt.Errorf("verify native V3 edit recovery memberships: %w", err)
	}
	if memberships != 1 || exactSafeMemberships != 1 || otherCapabilityMembers != 0 {
		return errors.New("native V3 edit recovery runtime membership set is unsafe")
	}
	var runtimeDirectACLs, capabilityMemberships int
	if err := conn.QueryRow(ctx, `SELECT
		(SELECT count(*) FROM (
			SELECT 1 FROM pg_proc object
			CROSS JOIN LATERAL aclexplode(COALESCE(object.proacl,
				acldefault('f',object.proowner))) acl
			JOIN pg_roles grantee ON grantee.oid=acl.grantee
			WHERE grantee.rolname=session_user
			UNION ALL
			SELECT 1 FROM pg_class object
			CROSS JOIN LATERAL aclexplode(COALESCE(object.relacl,
				acldefault(CASE WHEN object.relkind='S' THEN 's'::"char" ELSE 'r'::"char" END,
				object.relowner))) acl
			JOIN pg_roles grantee ON grantee.oid=acl.grantee
			WHERE grantee.rolname=session_user
			UNION ALL
			SELECT 1 FROM pg_attribute object
			CROSS JOIN LATERAL aclexplode(object.attacl) acl
			JOIN pg_roles grantee ON grantee.oid=acl.grantee
			WHERE grantee.rolname=session_user
			UNION ALL
			SELECT 1 FROM pg_namespace object
			CROSS JOIN LATERAL aclexplode(COALESCE(object.nspacl,
				acldefault('n',object.nspowner))) acl
			JOIN pg_roles grantee ON grantee.oid=acl.grantee
			WHERE grantee.rolname=session_user) direct_acl),
		(SELECT count(*) FROM pg_auth_members edge
		 JOIN pg_roles member ON member.oid=edge.member
		 WHERE member.rolname='vane_native_v3_edit_recovery')`).Scan(
		&runtimeDirectACLs, &capabilityMemberships); err != nil {
		return fmt.Errorf("verify native V3 edit recovery direct authority: %w", err)
	}
	if runtimeDirectACLs != 0 || capabilityMemberships != 0 {
		return errors.New("native V3 edit recovery direct authority is unsafe")
	}
	if _, err := conn.Exec(ctx, `SET ROLE vane_native_v3_edit_recovery`); err != nil {
		return fmt.Errorf("enter native V3 edit recovery capability: %w", err)
	}
	var member, claim, directOperationRead, directScheduleWrite bool
	var capabilityLogin, capabilitySuper, capabilityBypass, capabilityCreateDB,
		capabilityCreateRole, capabilityInherit, capabilityReplication bool
	var directFunctions, directSchemas, directRelations int
	if err := conn.QueryRow(ctx, `SELECT
		pg_has_role(session_user,'vane_native_v3_edit_recovery','MEMBER'),
		has_function_privilege(current_user,
		 'claim_stale_native_research_v3_edit_v1(timestamptz,text,bigint)','EXECUTE'),
		has_table_privilege(current_user,'task_definition_edit_operations','SELECT'),
		has_table_privilege(current_user,'schedules','UPDATE'),
		role.rolcanlogin,role.rolsuper,role.rolbypassrls,role.rolcreatedb,
		role.rolcreaterole,role.rolinherit,role.rolreplication,
		(SELECT count(*) FROM pg_proc object
		 CROSS JOIN LATERAL aclexplode(COALESCE(object.proacl,
			acldefault('f',object.proowner))) acl
		 WHERE acl.grantee=role.oid AND acl.privilege_type='EXECUTE'),
		(SELECT count(*) FROM pg_namespace object
		 CROSS JOIN LATERAL aclexplode(COALESCE(object.nspacl,
			acldefault('n',object.nspowner))) acl
		 WHERE acl.grantee=role.oid AND acl.privilege_type='USAGE'),
		(SELECT count(*) FROM (
			SELECT acl.grantee FROM pg_class object
			CROSS JOIN LATERAL aclexplode(COALESCE(object.relacl,
				acldefault(CASE WHEN object.relkind='S' THEN 's'::"char" ELSE 'r'::"char" END,
				object.relowner))) acl
			UNION ALL
			SELECT acl.grantee FROM pg_attribute object
			CROSS JOIN LATERAL aclexplode(object.attacl) acl) grants
		 WHERE grants.grantee=role.oid)
		FROM pg_roles role WHERE role.rolname=current_user`).Scan(
		&member, &claim, &directOperationRead, &directScheduleWrite,
		&capabilityLogin, &capabilitySuper, &capabilityBypass, &capabilityCreateDB,
		&capabilityCreateRole, &capabilityInherit, &capabilityReplication,
		&directFunctions, &directSchemas, &directRelations); err != nil {
		return fmt.Errorf("verify native V3 edit recovery authority: %w", err)
	}
	if !member || !claim || directOperationRead || directScheduleWrite ||
		capabilityLogin || capabilitySuper || capabilityBypass || capabilityCreateDB ||
		capabilityCreateRole || capabilityInherit || capabilityReplication ||
		directFunctions != 1 || directSchemas != 1 || directRelations != 0 {
		return errors.New("native V3 edit recovery authority is unsafe")
	}
	return nil
}

func initializeServerRuntimeConnection(ctx context.Context, conn *pgx.Conn) error {
	if err := validateServerRuntimeConnection(ctx, conn); err != nil {
		return err
	}
	if _, err := conn.Exec(ctx, `SET ROLE vane_app`); err != nil {
		return fmt.Errorf("enter default server capability: %w", err)
	}
	var sessionUser, currentUser string
	if err := conn.QueryRow(ctx, `SELECT session_user,current_user`).Scan(
		&sessionUser, &currentUser); err != nil {
		return fmt.Errorf("verify default server capability: %w", err)
	}
	if sessionUser != serverRuntimeLoginRole || currentUser != "vane_app" {
		return errors.New("server runtime default capability is unsafe")
	}
	if err := verifyServerRuntimeQuotaProjection(ctx, conn); err != nil {
		return err
	}
	return nil
}

type serverRuntimeQuotaProjectionQuerier interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func verifyServerRuntimeQuotaProjection(
	ctx context.Context, querier serverRuntimeQuotaProjectionQuerier,
) error {
	authorityRoles := append([]string{serverRuntimeLoginRole},
		serverRuntimeCapabilityRoles...)
	var quotaResolver, directQuotaRead bool
	if err := querier.QueryRow(ctx, `
		SELECT has_function_privilege(current_user,
		         'public.resolve_research_quota_rule_v1(bigint,bigint,text,text)',
		         'EXECUTE'),
		       COALESCE((SELECT bool_or(
		         has_table_privilege(role_name,'public.tenant_quota','SELECT') OR
		         has_any_column_privilege(role_name,'public.tenant_quota','SELECT'))
		         FROM unnest($1::text[]) AS role_name),false)`, authorityRoles,
	).Scan(&quotaResolver, &directQuotaRead); err != nil {
		return fmt.Errorf("verify server quota projection: %w", err)
	}
	if !quotaResolver || directQuotaRead {
		return errors.New("server runtime quota projection is unsafe")
	}
	return nil
}

func (s *Store) verifyServerRuntimeQuotaProjection(ctx context.Context) error {
	if s == nil || s.pool == nil {
		return errors.New("server runtime quota projection Store is unavailable")
	}
	return verifyServerRuntimeQuotaProjection(ctx, s.pool)
}

func validateServerRuntimeConnection(ctx context.Context, conn *pgx.Conn) error {
	if conn == nil {
		return errors.New("server runtime connection is nil")
	}
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin server runtime authority probe: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	var (
		sessionUser                                string
		login, super, bypass, createDB, createRole bool
		replication, inherit                       bool
	)
	if err := tx.QueryRow(ctx, `
		SELECT rolname,rolcanlogin,rolsuper,rolbypassrls,rolcreatedb,
		       rolcreaterole,rolreplication,rolinherit
		  FROM pg_catalog.pg_roles WHERE rolname=session_user`,
	).Scan(&sessionUser, &login, &super, &bypass, &createDB, &createRole,
		&replication, &inherit); err != nil {
		return fmt.Errorf("read server runtime identity: %w", err)
	}
	if sessionUser != serverRuntimeLoginRole || !login || super || bypass ||
		createDB || createRole || replication || inherit {
		return fmt.Errorf("server runtime login %q has unsafe attributes", sessionUser)
	}

	memberships, err := reachableServerRuntimeMemberships(ctx, tx)
	if err != nil {
		return err
	}
	wantMemberships := slices.Clone(serverRuntimeCapabilityRoles)
	workspaceMemoryEnabled := slices.Contains(
		memberships, workspaceMemoryRuntimeCapabilityRole)
	if workspaceMemoryEnabled {
		wantMemberships = append(wantMemberships, workspaceMemoryRuntimeCapabilityRole)
	}
	slices.Sort(wantMemberships)
	if !slices.Equal(memberships, wantMemberships) {
		return fmt.Errorf("server runtime memberships differ: got=%v want=%v",
			memberships, wantMemberships)
	}
	if err := verifyMemoryRuntimeAuthority(ctx, tx); err != nil {
		return err
	}
	if workspaceMemoryEnabled {
		if err := verifyWorkspaceMemoryRuntimeAuthority(ctx, tx); err != nil {
			return err
		}
	}

	for _, role := range serverRuntimeForbiddenRoles {
		var member bool
		if err := tx.QueryRow(ctx,
			`SELECT pg_has_role(session_user,$1,'MEMBER')`, role).Scan(&member); err != nil {
			return fmt.Errorf("inspect forbidden server membership %s: %w", role, err)
		}
		if member {
			return fmt.Errorf("server runtime can enter forbidden role %s", role)
		}
	}

	// Probe the inert login and every role the application can explicitly
	// enter. A direct grant to vane_app (or another allowlisted capability) is
	// just as dangerous as a grant to the NOINHERIT session user.
	authorityRoles := append([]string{serverRuntimeLoginRole},
		serverRuntimeCapabilityRoles...)
	if workspaceMemoryEnabled {
		authorityRoles = append(authorityRoles, workspaceMemoryRuntimeCapabilityRole)
	}
	for _, role := range authorityRoles {
		if err := validateServerRuntimeAuthorityRole(ctx, tx, role); err != nil {
			return err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit server runtime authority probe: %w", err)
	}
	return nil
}

// verifyMemoryRuntimeAuthority is the read-only startup half of migration
// 129's owner-only exact validator. Deployment/provisioning rejects both extra
// and missing authority; every new server connection independently rechecks
// that all fourteen required direct ACL entries still exist, come from each
// object's owner, and are not delegable.
func verifyMemoryRuntimeAuthority(
	ctx context.Context, querier serverRuntimeQuotaProjectionQuerier,
) error {
	var roleSafe, membershipSafe bool
	if err := querier.QueryRow(ctx, `
		WITH memory_role AS (
		  SELECT oid,rolcanlogin,rolsuper,rolcreatedb,rolcreaterole,rolinherit,
		         rolreplication,rolbypassrls,rolconfig
		    FROM pg_catalog.pg_roles WHERE rolname='vane_memory_editor'
		), object_owner AS (
		  SELECT owner.oid,owner.rolname
		    FROM pg_catalog.pg_class relation
		    JOIN pg_catalog.pg_roles owner ON owner.oid=relation.relowner
		   WHERE relation.oid='memory_authorizations'::pg_catalog.regclass
		)
		SELECT
		  EXISTS (SELECT 1 FROM memory_role
		           WHERE NOT rolcanlogin AND NOT rolsuper AND NOT rolcreatedb
		             AND NOT rolcreaterole AND NOT rolinherit AND NOT rolreplication
		             AND NOT rolbypassrls AND
		             rolconfig=ARRAY['search_path=pg_catalog, public, pg_temp']::TEXT[]),
		  (SELECT count(*)=2 FROM pg_catalog.pg_auth_members edge
		    JOIN memory_role role ON role.oid=edge.roleid
		    JOIN pg_catalog.pg_roles member ON member.oid=edge.member
		    CROSS JOIN object_owner owner
		   WHERE member.rolname IN (owner.rolname,'vane_server_runtime') AND
		         NOT edge.admin_option AND NOT edge.inherit_option AND edge.set_option) AND
		  NOT EXISTS (
		    SELECT 1 FROM pg_catalog.pg_auth_members edge
		    JOIN memory_role role ON role.oid=edge.roleid
		    JOIN pg_catalog.pg_roles member ON member.oid=edge.member
		    CROSS JOIN object_owner owner
		    WHERE member.rolname NOT IN (owner.rolname,'vane_server_runtime') OR
		          edge.admin_option OR edge.inherit_option OR NOT edge.set_option) AND
		  NOT EXISTS (
		    SELECT 1 FROM pg_catalog.pg_auth_members edge
		    JOIN memory_role role ON role.oid=edge.member)
	`).Scan(&roleSafe, &membershipSafe); err != nil {
		return fmt.Errorf("verify memory runtime role contract: %w", err)
	}
	if !roleSafe || !membershipSafe {
		return fmt.Errorf("memory runtime role contract is unsafe: attributes=%v memberships=%v",
			roleSafe, membershipSafe)
	}
	var required, unexpected int
	if err := querier.QueryRow(ctx, `
		SELECT
		  (SELECT count(*) FROM pg_catalog.pg_namespace object
		   CROSS JOIN LATERAL pg_catalog.aclexplode(COALESCE(object.nspacl,
		       pg_catalog.acldefault('n',object.nspowner))) acl
		   JOIN pg_catalog.pg_roles role ON role.oid=acl.grantee
		   WHERE role.rolname='vane_memory_editor' AND
		         object.nspname='public' AND acl.privilege_type='USAGE' AND
		         acl.grantor=object.nspowner AND NOT acl.is_grantable) +
		  (SELECT count(*) FROM pg_catalog.pg_class object
		   JOIN pg_catalog.pg_namespace namespace ON namespace.oid=object.relnamespace
		   CROSS JOIN LATERAL pg_catalog.aclexplode(COALESCE(object.relacl,
		       pg_catalog.acldefault(CASE WHEN object.relkind='S' THEN 's'::"char"
		                                 ELSE 'r'::"char" END,object.relowner))) acl
		   JOIN pg_catalog.pg_roles role ON role.oid=acl.grantee
		   WHERE role.rolname='vane_memory_editor' AND
		         namespace.nspname='public' AND acl.grantor=object.relowner AND
		         NOT acl.is_grantable AND (
		           (object.relname IN ('memory_authorizations','memory_records',
		                               'memory_events','memory_receipts') AND
		            acl.privilege_type IN ('SELECT','INSERT')) OR
		           (object.relname IN ('memory_records_id_seq','memory_events_id_seq') AND
		            acl.privilege_type IN ('USAGE','SELECT')))) +
		  (SELECT count(*) FROM pg_catalog.pg_attribute object
		   JOIN pg_catalog.pg_class relation ON relation.oid=object.attrelid
		   JOIN pg_catalog.pg_namespace namespace ON namespace.oid=relation.relnamespace
		   CROSS JOIN LATERAL pg_catalog.aclexplode(object.attacl) acl
		   JOIN pg_catalog.pg_roles role ON role.oid=acl.grantee
		   WHERE role.rolname='vane_memory_editor' AND
		         namespace.nspname='public' AND
		         relation.relname='memory_authorizations' AND
		         object.attname='consumed_event_id' AND
		         acl.privilege_type='UPDATE' AND acl.grantor=relation.relowner AND
		         NOT acl.is_grantable)`,
	).Scan(&required); err != nil {
		return fmt.Errorf("verify memory runtime authority: %w", err)
	}
	if err := querier.QueryRow(ctx, `
		WITH memory_role AS (
		  SELECT oid FROM pg_catalog.pg_roles WHERE rolname='vane_memory_editor'
		)
		SELECT
		  (SELECT count(*) FROM pg_catalog.pg_shdepend dependency,memory_role
		   WHERE dependency.refclassid='pg_authid'::pg_catalog.regclass
		     AND dependency.refobjid=memory_role.oid
		     AND dependency.deptype IN ('a','o')
		     AND (dependency.dbid=0 OR (
		       dependency.dbid=(SELECT oid FROM pg_catalog.pg_database
		                          WHERE datname=pg_catalog.current_database()) AND
		       NOT (
		         (dependency.classid='pg_namespace'::pg_catalog.regclass AND
		          dependency.objid='public'::pg_catalog.regnamespace AND
		          dependency.objsubid=0) OR
		         (dependency.classid='pg_class'::pg_catalog.regclass AND
		          dependency.objid IN (
		            'memory_authorizations'::pg_catalog.regclass,
		            'memory_records'::pg_catalog.regclass,
		            'memory_events'::pg_catalog.regclass,
		            'memory_receipts'::pg_catalog.regclass,
		            'memory_records_id_seq'::pg_catalog.regclass,
		            'memory_events_id_seq'::pg_catalog.regclass) AND
		          dependency.objsubid=0) OR
		         (dependency.classid='pg_class'::pg_catalog.regclass AND
		          dependency.objid='memory_authorizations'::pg_catalog.regclass AND
		          dependency.objsubid=(SELECT attnum FROM pg_catalog.pg_attribute
		             WHERE attrelid='memory_authorizations'::pg_catalog.regclass
		               AND attname='consumed_event_id' AND NOT attisdropped))
		       )
		     ))) +
		  (SELECT count(*) FROM pg_catalog.pg_namespace object,memory_role
		   CROSS JOIN LATERAL pg_catalog.aclexplode(COALESCE(object.nspacl,
		       pg_catalog.acldefault('n',object.nspowner))) acl
		   WHERE acl.grantee=memory_role.oid AND NOT (
		       object.nspname='public' AND acl.privilege_type='USAGE' AND
		       acl.grantor=object.nspowner AND NOT acl.is_grantable)) +
		  (SELECT count(*) FROM pg_catalog.pg_class object
		   JOIN pg_catalog.pg_namespace namespace ON namespace.oid=object.relnamespace
		   CROSS JOIN memory_role
		   CROSS JOIN LATERAL pg_catalog.aclexplode(COALESCE(object.relacl,
		       pg_catalog.acldefault(CASE WHEN object.relkind='S' THEN 's'::"char"
		                                 ELSE 'r'::"char" END,object.relowner))) acl
		   WHERE acl.grantee=memory_role.oid AND NOT (
		     namespace.nspname='public' AND (
		       (object.relname IN ('memory_authorizations','memory_records',
		                           'memory_events','memory_receipts') AND
		        acl.privilege_type IN ('SELECT','INSERT') AND
		        acl.grantor=object.relowner AND NOT acl.is_grantable) OR
		       (object.relname IN ('memory_records_id_seq','memory_events_id_seq') AND
		        acl.privilege_type IN ('USAGE','SELECT') AND
		        acl.grantor=object.relowner AND NOT acl.is_grantable)))) +
		  (SELECT count(*) FROM pg_catalog.pg_attribute object
		   JOIN pg_catalog.pg_class relation ON relation.oid=object.attrelid
		   JOIN pg_catalog.pg_namespace namespace ON namespace.oid=relation.relnamespace
		   CROSS JOIN memory_role
		   CROSS JOIN LATERAL pg_catalog.aclexplode(object.attacl) acl
		   WHERE acl.grantee=memory_role.oid AND NOT (
		     namespace.nspname='public' AND relation.relname='memory_authorizations' AND
		     object.attname='consumed_event_id' AND acl.privilege_type='UPDATE' AND
		     acl.grantor=relation.relowner AND NOT acl.is_grantable)) +
		  (SELECT count(*) FROM pg_catalog.pg_proc object,memory_role
		   CROSS JOIN LATERAL pg_catalog.aclexplode(COALESCE(object.proacl,
		       pg_catalog.acldefault('f',object.proowner))) acl
		   WHERE acl.grantee=memory_role.oid) +
		  (SELECT count(*) FROM pg_catalog.pg_default_acl object,memory_role
		   CROSS JOIN LATERAL pg_catalog.aclexplode(object.defaclacl) acl
		   WHERE acl.grantee=memory_role.oid) +
		  (SELECT count(*) FROM pg_catalog.pg_database object,memory_role
		   WHERE object.datdba=memory_role.oid) +
		  (SELECT count(*) FROM pg_catalog.pg_namespace object,memory_role
		   WHERE object.nspowner=memory_role.oid) +
		  (SELECT count(*) FROM pg_catalog.pg_class object,memory_role
		   WHERE object.relowner=memory_role.oid) +
		  (SELECT count(*) FROM pg_catalog.pg_proc object,memory_role
		   WHERE object.proowner=memory_role.oid) +
		  (SELECT count(*) FROM pg_catalog.pg_type object,memory_role
		   WHERE object.typowner=memory_role.oid)`,
	).Scan(&unexpected); err != nil {
		return fmt.Errorf("verify unexpected memory runtime authority: %w", err)
	}
	if unexpected != 0 {
		return fmt.Errorf("memory runtime retains %d unexpected authorities", unexpected)
	}
	if required != 14 {
		return fmt.Errorf("memory runtime has %d of 14 required authorities", required)
	}
	return nil
}

// verifyWorkspaceMemoryRuntimeAuthority is the startup guard for migration
// 138's independent team ledger. It rejects missing/extra ACLs, role edges,
// unsafe role attributes, and any loss of FORCE RLS before a connection can
// serve traffic.
func verifyWorkspaceMemoryRuntimeAuthority(
	ctx context.Context, querier serverRuntimeQuotaProjectionQuerier,
) error {
	var roleSafe, edgesSafe, rlsSafe bool
	if err := querier.QueryRow(ctx, `
        WITH role_row AS (
          SELECT oid,rolcanlogin,rolsuper,rolcreatedb,rolcreaterole,rolinherit,
                 rolreplication,rolbypassrls,rolconfig
            FROM pg_catalog.pg_roles
           WHERE rolname='vane_workspace_memory_editor'
        ), owner_row AS (
          SELECT owner.oid,owner.rolname
            FROM pg_catalog.pg_class relation
            JOIN pg_catalog.pg_roles owner ON owner.oid=relation.relowner
           WHERE relation.oid='workspace_memory_authorizations'::pg_catalog.regclass
        )
        SELECT
          EXISTS(SELECT 1 FROM role_row WHERE NOT rolcanlogin AND NOT rolsuper
            AND NOT rolcreatedb AND NOT rolcreaterole AND NOT rolinherit
            AND NOT rolreplication AND NOT rolbypassrls
            AND rolconfig=ARRAY['search_path=pg_catalog, public, pg_temp']::text[]),
          (SELECT count(*)=2 FROM pg_catalog.pg_auth_members edge
            JOIN role_row role ON role.oid=edge.roleid
            JOIN pg_catalog.pg_roles member ON member.oid=edge.member
            CROSS JOIN owner_row owner
           WHERE member.rolname IN(owner.rolname,'vane_server_runtime')
             AND NOT edge.admin_option AND NOT edge.inherit_option AND edge.set_option)
          AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_auth_members edge
            JOIN role_row role ON role.oid=edge.roleid
            JOIN pg_catalog.pg_roles member ON member.oid=edge.member
            CROSS JOIN owner_row owner
           WHERE member.rolname NOT IN(owner.rolname,'vane_server_runtime')
              OR edge.admin_option OR edge.inherit_option OR NOT edge.set_option)
          AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_auth_members edge
            JOIN role_row role ON role.oid=edge.member),
          (SELECT bool_and(relation.relrowsecurity AND relation.relforcerowsecurity)
             FROM pg_catalog.pg_class relation
            WHERE relation.oid IN(
              'workspace_memory_authorizations'::pg_catalog.regclass,
              'workspace_memory_records'::pg_catalog.regclass,
              'workspace_memory_events'::pg_catalog.regclass,
              'workspace_memory_receipts'::pg_catalog.regclass))
    `).Scan(&roleSafe, &edgesSafe, &rlsSafe); err != nil {
		return fmt.Errorf("verify workspace memory runtime contract: %w", err)
	}
	if !roleSafe || !edgesSafe || !rlsSafe {
		return fmt.Errorf("workspace memory runtime contract is unsafe: role=%v edges=%v rls=%v",
			roleSafe, edgesSafe, rlsSafe)
	}
	var policiesSafe bool
	if err := querier.QueryRow(ctx, `
        WITH role_row AS (SELECT oid FROM pg_catalog.pg_roles
          WHERE rolname='vane_workspace_memory_editor'), policies AS (
          SELECT relation.relname,policy.polname,policy.polpermissive,policy.polcmd,
                 policy.polroles,pg_catalog.pg_get_expr(policy.polqual,policy.polrelid) AS using_expr,
                 pg_catalog.pg_get_expr(policy.polwithcheck,policy.polrelid) AS check_expr
            FROM pg_catalog.pg_policy policy
            JOIN pg_catalog.pg_class relation ON relation.oid=policy.polrelid
           WHERE relation.relname IN('workspace_memory_authorizations',
             'workspace_memory_records','workspace_memory_events','workspace_memory_receipts')
        )
        SELECT count(*)=6
          AND array_agg(relname||':'||polname ORDER BY relname,polname)=ARRAY[
            'workspace_memory_authorizations:workspace_memory_authorization_insert',
            'workspace_memory_authorizations:workspace_memory_authorization_select',
            'workspace_memory_authorizations:workspace_memory_authorization_update',
            'workspace_memory_events:workspace_memory_event_tenant',
            'workspace_memory_receipts:workspace_memory_receipt_actor',
            'workspace_memory_records:workspace_memory_record_tenant']::text[]
          AND bool_and(polpermissive
            AND polroles=ARRAY[(SELECT oid FROM role_row)]::oid[])
          AND md5(string_agg(relname||'|'||polname||'|'||polpermissive::text||'|'||
            polcmd::text||'|'||COALESCE(using_expr,'<null>')||'|'||
            COALESCE(check_expr,'<null>'),E'\n' ORDER BY relname,polname))=
            '6917d270023b8fb464af8bc03d56ba2f'
          FROM policies
    `).Scan(&policiesSafe); err != nil {
		return fmt.Errorf("verify workspace memory RLS policies: %w", err)
	}
	if !policiesSafe {
		return errors.New("workspace memory RLS policy contract differs")
	}

	var required, unexpected int
	if err := querier.QueryRow(ctx, `
        WITH role_row AS (SELECT oid FROM pg_catalog.pg_roles
          WHERE rolname='vane_workspace_memory_editor')
        SELECT
          (SELECT count(*) FROM pg_catalog.pg_namespace object
            CROSS JOIN LATERAL pg_catalog.aclexplode(COALESCE(object.nspacl,
              pg_catalog.acldefault('n',object.nspowner))) acl
            CROSS JOIN role_row WHERE acl.grantee=role_row.oid
              AND object.nspname='public' AND acl.privilege_type='USAGE'
              AND acl.grantor=object.nspowner AND NOT acl.is_grantable) +
          (SELECT count(*) FROM pg_catalog.pg_class object
            JOIN pg_catalog.pg_namespace namespace ON namespace.oid=object.relnamespace
            CROSS JOIN LATERAL pg_catalog.aclexplode(COALESCE(object.relacl,
              pg_catalog.acldefault(CASE WHEN object.relkind='S' THEN 's'::"char"
                ELSE 'r'::"char" END,object.relowner))) acl
            CROSS JOIN role_row WHERE acl.grantee=role_row.oid
              AND namespace.nspname='public' AND acl.grantor=object.relowner
              AND NOT acl.is_grantable AND (
                (object.relname IN('workspace_memory_authorizations',
                  'workspace_memory_records','workspace_memory_events',
                  'workspace_memory_receipts') AND acl.privilege_type IN('SELECT','INSERT')) OR
                (object.relname IN('workspace_memory_records_id_seq',
                  'workspace_memory_events_id_seq') AND acl.privilege_type IN('USAGE','SELECT')))) +
          (SELECT count(*) FROM pg_catalog.pg_attribute object
            JOIN pg_catalog.pg_class relation ON relation.oid=object.attrelid
            JOIN pg_catalog.pg_namespace namespace ON namespace.oid=relation.relnamespace
            CROSS JOIN LATERAL pg_catalog.aclexplode(object.attacl) acl
            CROSS JOIN role_row WHERE acl.grantee=role_row.oid
              AND namespace.nspname='public'
              AND relation.relname='workspace_memory_authorizations'
              AND object.attname='consumed_event_id'
              AND acl.privilege_type='UPDATE' AND acl.grantor=relation.relowner
              AND NOT acl.is_grantable),
          (SELECT count(*) FROM pg_catalog.pg_shdepend dependency
            CROSS JOIN role_row
           WHERE dependency.refclassid='pg_authid'::pg_catalog.regclass
             AND dependency.refobjid=role_row.oid
             AND dependency.deptype IN('a','o')
             AND (dependency.dbid=0 OR (
               dependency.dbid=(SELECT oid FROM pg_catalog.pg_database
                 WHERE datname=pg_catalog.current_database()) AND NOT(
                 (dependency.classid='pg_namespace'::pg_catalog.regclass
                   AND dependency.objid='public'::pg_catalog.regnamespace
                   AND dependency.objsubid=0) OR
                 (dependency.classid='pg_class'::pg_catalog.regclass
                   AND dependency.objid IN(
                     'workspace_memory_authorizations'::pg_catalog.regclass,
                     'workspace_memory_records'::pg_catalog.regclass,
                     'workspace_memory_events'::pg_catalog.regclass,
                     'workspace_memory_receipts'::pg_catalog.regclass,
                     'workspace_memory_records_id_seq'::pg_catalog.regclass,
                     'workspace_memory_events_id_seq'::pg_catalog.regclass)
                   AND dependency.objsubid=0) OR
                 (dependency.classid='pg_class'::pg_catalog.regclass
                   AND dependency.objid='workspace_memory_authorizations'::pg_catalog.regclass
                   AND dependency.objsubid=(SELECT attnum FROM pg_catalog.pg_attribute
                     WHERE attrelid='workspace_memory_authorizations'::pg_catalog.regclass
                       AND attname='consumed_event_id' AND NOT attisdropped)))))) +
          (SELECT count(*) FROM pg_catalog.pg_database object
            CROSS JOIN role_row
            CROSS JOIN LATERAL pg_catalog.aclexplode(COALESCE(object.datacl,
              pg_catalog.acldefault('d',object.datdba))) acl
           WHERE acl.grantee=role_row.oid) +
          (SELECT count(*) FROM pg_catalog.pg_namespace object
            CROSS JOIN LATERAL pg_catalog.aclexplode(COALESCE(object.nspacl,
              pg_catalog.acldefault('n',object.nspowner))) acl
            CROSS JOIN role_row WHERE acl.grantee=role_row.oid AND NOT(
              object.nspname='public' AND acl.privilege_type='USAGE'
              AND acl.grantor=object.nspowner AND NOT acl.is_grantable)) +
          (SELECT count(*) FROM pg_catalog.pg_class object
            JOIN pg_catalog.pg_namespace namespace ON namespace.oid=object.relnamespace
            CROSS JOIN LATERAL pg_catalog.aclexplode(COALESCE(object.relacl,
              pg_catalog.acldefault(CASE WHEN object.relkind='S' THEN 's'::"char"
                ELSE 'r'::"char" END,object.relowner))) acl
            CROSS JOIN role_row WHERE acl.grantee=role_row.oid AND NOT(
              namespace.nspname='public' AND acl.grantor=object.relowner
              AND NOT acl.is_grantable AND (
                (object.relname IN('workspace_memory_authorizations',
                  'workspace_memory_records','workspace_memory_events',
                  'workspace_memory_receipts') AND acl.privilege_type IN('SELECT','INSERT')) OR
                (object.relname IN('workspace_memory_records_id_seq',
                  'workspace_memory_events_id_seq') AND acl.privilege_type IN('USAGE','SELECT'))))) +
          (SELECT count(*) FROM pg_catalog.pg_attribute object
            JOIN pg_catalog.pg_class relation ON relation.oid=object.attrelid
            JOIN pg_catalog.pg_namespace namespace ON namespace.oid=relation.relnamespace
            CROSS JOIN LATERAL pg_catalog.aclexplode(object.attacl) acl
            CROSS JOIN role_row WHERE acl.grantee=role_row.oid AND NOT(
              namespace.nspname='public'
              AND relation.relname='workspace_memory_authorizations'
              AND object.attname='consumed_event_id'
              AND acl.privilege_type='UPDATE' AND acl.grantor=relation.relowner
              AND NOT acl.is_grantable)) +
          (SELECT count(*) FROM pg_catalog.pg_proc object CROSS JOIN role_row
            CROSS JOIN LATERAL pg_catalog.aclexplode(COALESCE(object.proacl,
              pg_catalog.acldefault('f',object.proowner))) acl
            WHERE acl.grantee=role_row.oid) +
          (SELECT count(*) FROM pg_catalog.pg_default_acl object CROSS JOIN role_row
            CROSS JOIN LATERAL pg_catalog.aclexplode(object.defaclacl) acl
            WHERE acl.grantee=role_row.oid) +
          (SELECT count(*) FROM pg_catalog.pg_database object CROSS JOIN role_row
            WHERE object.datdba=role_row.oid) +
          (SELECT count(*) FROM pg_catalog.pg_namespace object CROSS JOIN role_row
            WHERE object.nspowner=role_row.oid) +
          (SELECT count(*) FROM pg_catalog.pg_class object CROSS JOIN role_row
            WHERE object.relowner=role_row.oid) +
          (SELECT count(*) FROM pg_catalog.pg_proc object CROSS JOIN role_row
            WHERE object.proowner=role_row.oid) +
          (SELECT count(*) FROM pg_catalog.pg_type object CROSS JOIN role_row
            WHERE object.typowner=role_row.oid)
    `).Scan(&required, &unexpected); err != nil {
		return fmt.Errorf("verify workspace memory runtime ACL: %w", err)
	}
	if required != 14 || unexpected != 0 {
		return fmt.Errorf("workspace memory runtime ACL differs: required=%d/14 unexpected=%d",
			required, unexpected)
	}
	return nil
}

func validateServerRuntimeAuthorityRole(
	ctx context.Context, tx pgx.Tx, role string,
) error {
	var (
		super, bypass, createDB, createRole, replication bool
		ownsDatabase, ownsObject, canCreatePublic        bool
	)
	if err := tx.QueryRow(ctx, `
		SELECT role.rolsuper,role.rolbypassrls,role.rolcreatedb,
		       role.rolcreaterole,role.rolreplication,
		       EXISTS (SELECT 1 FROM pg_catalog.pg_database
		                WHERE datname=current_database() AND datdba=role.oid),
		       EXISTS (
		         SELECT 1 FROM pg_catalog.pg_namespace WHERE nspowner=role.oid
		         UNION ALL SELECT 1 FROM pg_catalog.pg_class WHERE relowner=role.oid
		         UNION ALL SELECT 1 FROM pg_catalog.pg_proc WHERE proowner=role.oid
		         UNION ALL SELECT 1 FROM pg_catalog.pg_type WHERE typowner=role.oid
		       ),
		       has_schema_privilege(role.rolname,'public','CREATE')
		  FROM pg_catalog.pg_roles role WHERE role.rolname=$1`, role,
	).Scan(&super, &bypass, &createDB, &createRole, &replication,
		&ownsDatabase, &ownsObject, &canCreatePublic); err != nil {
		return fmt.Errorf("inspect server authority role %s: %w", role, err)
	}
	if super || bypass || createDB || createRole || replication ||
		ownsDatabase || ownsObject || canCreatePublic {
		return fmt.Errorf("server authority role %s is unsafe: super=%v bypass=%v createdb=%v createrole=%v replication=%v db_owner=%v object_owner=%v public_create=%v",
			role, super, bypass, createDB, createRole, replication,
			ownsDatabase, ownsObject, canCreatePublic)
	}

	var protectedMutation, protectedRead, readsGatewaySecret bool
	if err := tx.QueryRow(ctx, `
		SELECT
		  COALESCE(bool_or(
		    has_table_privilege($1,relation_name,'INSERT') OR
		    has_table_privilege($1,relation_name,'UPDATE') OR
		    has_table_privilege($1,relation_name,'DELETE') OR
		    has_table_privilege($1,relation_name,'TRUNCATE')
		  ),false),
		  COALESCE((SELECT bool_or(
		    has_table_privilege($1,relation_name,'SELECT') OR
		    has_any_column_privilege($1,relation_name,'SELECT')
		  ) FROM unnest($3::text[]) AS forbidden_read(relation_name)),false),
		  has_column_privilege($1,
		    'public.research_llm_gateway_verifier_keys','secret','SELECT')
		FROM unnest($2::text[]) AS relation_name`,
		role, serverRuntimeProtectedRelations, serverRuntimeForbiddenReadRelations,
	).Scan(&protectedMutation, &protectedRead, &readsGatewaySecret); err != nil {
		return fmt.Errorf("inspect protected privileges for %s: %w", role, err)
	}
	if protectedMutation || protectedRead || readsGatewaySecret {
		return fmt.Errorf("server authority role %s has direct protected data privileges", role)
	}

	var gatewayFunctionCount, binderFunctionCount int
	if err := tx.QueryRow(ctx, `
		SELECT count(*),count(*) FILTER (WHERE
		       proc.proname='bind_research_llm_process_gateway_v1' AND
		       pg_catalog.pg_get_function_identity_arguments(proc.oid)=
		       'requested_tenant_id bigint, requested_user_id bigint, requested_task_id text, requested_workflow_id text, requested_temporal_run_id text, requested_snapshot_id bigint, requested_reference_digest text, requested_reservation_id bigint')
		  FROM pg_catalog.pg_proc proc
		  JOIN pg_catalog.pg_namespace ns ON ns.oid=proc.pronamespace
		 WHERE ns.nspname='public'
		   AND (proc.proname LIKE '%research_llm_gateway%'
		        OR proc.proname LIKE '%research_llm_process_gateway%'
		        OR proc.proname LIKE 'settle_signed_research_run_llm_spend%'
		        OR proc.proname LIKE 'sign_research_llm_gateway%')
		   AND has_function_privilege($1,proc.oid,'EXECUTE')`, role,
	).Scan(&gatewayFunctionCount, &binderFunctionCount); err != nil {
		return fmt.Errorf("inspect gateway function privileges for %s: %w", role, err)
	}
	wantGatewayFunctions := 0
	if role == serverRuntimeLoginRole {
		wantGatewayFunctions = 1
	}
	if gatewayFunctionCount != wantGatewayFunctions ||
		binderFunctionCount != wantGatewayFunctions {
		return fmt.Errorf("server authority role %s can execute %d provider gateway functions",
			role, gatewayFunctionCount)
	}
	return nil
}

func reachableServerRuntimeMemberships(
	ctx context.Context, tx pgx.Tx,
) ([]string, error) {
	rows, err := tx.Query(ctx, `
		SELECT role.rolname
		  FROM pg_catalog.pg_roles role
		 WHERE role.rolname<>session_user
		   AND pg_has_role(session_user,role.oid,'MEMBER')
		 ORDER BY role.rolname`)
	if err != nil {
		return nil, fmt.Errorf("inspect server runtime memberships: %w", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var role string
		if err := rows.Scan(&role); err != nil {
			return nil, fmt.Errorf("scan server runtime membership: %w", err)
		}
		out = append(out, role)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read server runtime memberships: %w", err)
	}
	return out, nil
}
