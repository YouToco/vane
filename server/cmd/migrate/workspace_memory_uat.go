package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/YouToco/vane/server/internal/releaseinfo"
	"github.com/YouToco/vane/server/store"
	"github.com/YouToco/vane/server/types"
)

const (
	workspaceMemoryUATCommand      = "workspace-memory-uat"
	workspaceMemoryUATConfirmation = "vane.workspace-memory-runtime-uat/v1"
	workspaceMemoryUATPrefix       = "vane-runtime-uat:"
	workspaceMemoryUATTimeout      = 3 * time.Minute
	workspaceMemoryUATCleanupTime  = 2 * time.Minute
)

var lowerRevision = regexp.MustCompile(`^[0-9a-f]{40}$`)

type workspaceMemoryUATOptions struct {
	operationID      string
	expectedRevision string
	confirmation     string
}

type workspaceMemoryUATFixture struct {
	creatorUserID int64
	memberUserID  int64
	personalID    int64
	teamID        int64
	personalSID   int64
	teamSID       int64
}

type workspaceMemoryUATReport struct {
	Schema                    string `json:"schema"`
	Revision                  string `json:"revision"`
	OperationID               string `json:"operation_id"`
	RuntimeBoundaryVerified   bool   `json:"runtime_boundary_verified"`
	PersonalWriteVerified     bool   `json:"personal_write_verified"`
	TeamWriteVerified         bool   `json:"team_write_verified"`
	CrossMemberRecallVerified bool   `json:"cross_member_recall_verified"`
	PersonalExcludedFromTeam  bool   `json:"personal_excluded_from_team"`
	TeamExcludedFromPersonal  bool   `json:"team_excluded_from_personal"`
	CleanupVerified           bool   `json:"cleanup_verified"`
	PersonalEvidenceDigest    string `json:"personal_evidence_digest"`
	TeamEvidenceDigest        string `json:"team_evidence_digest"`
}

type workspaceMemoryRuntime interface {
	PrepareMemoryAuthorization(context.Context, int64, int64, int64, types.MemoryAction) (string, error)
	ApplyMemoryAction(context.Context, int64, int64, string, types.MemoryAction) (*types.MemoryActionResult, error)
	PrepareWorkspaceMemoryAuthorization(context.Context, int64, int64, int64, types.MemoryAction) (string, error)
	ApplyWorkspaceMemoryAction(context.Context, int64, int64, string, types.MemoryAction) (*types.MemoryActionResult, error)
	RecallMemories(context.Context, int64, int64, types.MemoryRecallQuery) (*types.MemoryRecallResult, error)
	RecallWorkspaceMemories(context.Context, int64, int64, types.MemoryRecallQuery) (*types.MemoryRecallResult, error)
}

func runWorkspaceMemoryUATCommand(arguments []string) error {
	return executeWorkspaceMemoryUATCommand(arguments, os.Stdout,
		releaseinfo.Revision, runWorkspaceMemoryRuntimeUAT)
}

func executeWorkspaceMemoryUATCommand(
	arguments []string,
	output io.Writer,
	revisionAuthority func() (string, bool),
	runner func(context.Context, string, string, string, string) (*workspaceMemoryUATReport, error),
) error {
	set := flag.NewFlagSet(workspaceMemoryUATCommand, flag.ContinueOnError)
	set.SetOutput(os.Stderr)
	var options workspaceMemoryUATOptions
	set.StringVar(&options.operationID, "operation-id", "", "one-time canonical UUID")
	set.StringVar(&options.expectedRevision, "expected-revision", "", "exact release revision")
	set.StringVar(&options.confirmation, "confirm", "", "exact mutation authority phrase")
	if err := set.Parse(arguments); err != nil {
		return err
	}
	if set.NArg() != 0 || options.confirmation != workspaceMemoryUATConfirmation ||
		!lowerRevision.MatchString(options.expectedRevision) {
		return errors.New("workspace memory UAT authority is invalid")
	}
	parsedOperation, err := uuid.Parse(options.operationID)
	if err != nil || parsedOperation == uuid.Nil || parsedOperation.String() != options.operationID {
		return errors.New("workspace memory UAT operation ID is invalid")
	}
	revision, ok := revisionAuthority()
	if !ok || revision != options.expectedRevision {
		return errors.New("workspace memory UAT binary is not the expected clean release")
	}
	ownerURL, err := migrationDatabaseURL()
	if err != nil {
		return err
	}
	runtimeURL := strings.TrimSpace(os.Getenv("VANE_DB_URL"))
	if runtimeURL == "" || runtimeURL == ownerURL {
		return errors.New("workspace memory UAT requires distinct owner and runtime database authorities")
	}
	ctx, cancel := context.WithTimeout(context.Background(), workspaceMemoryUATTimeout)
	defer cancel()
	report, err := runner(ctx, ownerURL, runtimeURL,
		options.operationID, revision)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(output)
	encoder.SetEscapeHTML(false)
	return encoder.Encode(report)
}

func runWorkspaceMemoryRuntimeUAT(
	ctx context.Context, ownerURL, runtimeURL, operationID, revision string,
) (_ *workspaceMemoryUATReport, returnErr error) {
	ownerPool, err := pgxpool.New(ctx, ownerURL)
	if err != nil {
		return nil, fmt.Errorf("open workspace memory UAT owner pool: %w", err)
	}
	defer ownerPool.Close()
	ownerStore, err := store.New(ctx, ownerURL)
	if err != nil {
		return nil, fmt.Errorf("open workspace memory UAT cleanup store: %w", err)
	}
	defer ownerStore.Close()
	if err := verifyWorkspaceMemoryUATOwner(ctx, ownerPool); err != nil {
		return nil, err
	}

	lockConn, err := ownerPool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire workspace memory UAT lock connection: %w", err)
	}
	defer lockConn.Release()
	if _, err := lockConn.Exec(ctx, `SELECT pg_catalog.pg_advisory_lock(
		pg_catalog.hashtextextended('vane/workspace-memory-runtime-uat/v1',0))`); err != nil {
		return nil, fmt.Errorf("lock workspace memory UAT: %w", err)
	}
	defer func() {
		unlockCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = lockConn.Exec(unlockCtx, `SELECT pg_catalog.pg_advisory_unlock(
			pg_catalog.hashtextextended('vane/workspace-memory-runtime-uat/v1',0))`)
	}()

	if err := cleanupStaleWorkspaceMemoryUAT(ctx, ownerPool, ownerStore); err != nil {
		return nil, err
	}
	fixture, err := createWorkspaceMemoryUATFixture(ctx, ownerPool, operationID)
	if err != nil {
		return nil, err
	}
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), workspaceMemoryUATCleanupTime)
		defer cancel()
		cleanupErr := cleanupWorkspaceMemoryUATFixture(cleanupCtx, ownerPool, ownerStore, fixture)
		if cleanupErr != nil {
			returnErr = errors.Join(returnErr, cleanupErr)
		}
	}()

	runtimeStore, err := store.NewServerRuntime(ctx, runtimeURL)
	if err != nil {
		return nil, fmt.Errorf("open exact workspace memory runtime: %w", err)
	}
	defer runtimeStore.Close()

	personalText, teamText, err := exerciseWorkspaceMemoryRuntime(
		ctx, runtimeStore, fixture, operationID)
	if err != nil {
		return nil, err
	}

	report := &workspaceMemoryUATReport{
		Schema: workspaceMemoryUATConfirmation, Revision: revision,
		OperationID: operationID, RuntimeBoundaryVerified: true,
		PersonalWriteVerified: true, TeamWriteVerified: true,
		CrossMemberRecallVerified: true, PersonalExcludedFromTeam: true,
		TeamExcludedFromPersonal: true,
		PersonalEvidenceDigest:   digestString(personalText),
		TeamEvidenceDigest:       digestString(teamText),
	}
	cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), workspaceMemoryUATCleanupTime)
	defer cleanupCancel()
	if err := cleanupWorkspaceMemoryUATFixture(cleanupCtx, ownerPool, ownerStore, fixture); err != nil {
		return nil, err
	}
	report.CleanupVerified = true
	fixture = workspaceMemoryUATFixture{}
	return report, nil
}

func exerciseWorkspaceMemoryRuntime(
	ctx context.Context, runtime workspaceMemoryRuntime,
	fixture workspaceMemoryUATFixture, operationID string,
) (string, string, error) {
	personalText := "vaneuatpersonal" + digestString(operationID + ":personal")[:20]
	teamText := "vaneuatteam" + digestString(operationID + ":team")[:20]
	personalAction := workspaceMemoryUATAction(operationID, "personal", personalText)
	personalAuthorization, err := runtime.PrepareMemoryAuthorization(
		ctx, fixture.personalID, fixture.creatorUserID, fixture.personalSID, personalAction)
	if err != nil {
		return "", "", fmt.Errorf("prepare personal memory UAT: %w", err)
	}
	personalAction.Evidence.AuthorizationID = personalAuthorization
	if _, err := runtime.ApplyMemoryAction(ctx, fixture.personalID,
		fixture.creatorUserID, digestString(operationID+":personal-idempotency"),
		personalAction); err != nil {
		return "", "", fmt.Errorf("apply personal memory UAT: %w", err)
	}

	teamAction := workspaceMemoryUATAction(operationID, "team", teamText)
	teamAuthorization, err := runtime.PrepareWorkspaceMemoryAuthorization(
		ctx, fixture.teamID, fixture.creatorUserID, fixture.teamSID, teamAction)
	if err != nil {
		return "", "", fmt.Errorf("prepare team memory UAT: %w", err)
	}
	teamAction.Evidence.AuthorizationID = teamAuthorization
	if _, err := runtime.ApplyWorkspaceMemoryAction(ctx, fixture.teamID,
		fixture.creatorUserID, digestString(operationID+":team-idempotency"), teamAction); err != nil {
		return "", "", fmt.Errorf("apply team memory UAT: %w", err)
	}

	personalRecall, err := runtime.RecallMemories(ctx, fixture.personalID,
		fixture.creatorUserID, types.MemoryRecallQuery{Query: personalText, Limit: 5})
	if err != nil {
		return "", "", fmt.Errorf("recall personal memory UAT: %w", err)
	}
	teamRecall, err := runtime.RecallWorkspaceMemories(ctx, fixture.teamID,
		fixture.memberUserID, types.MemoryRecallQuery{Query: teamText, Limit: 5})
	if err != nil {
		return "", "", fmt.Errorf("recall team memory UAT as second member: %w", err)
	}
	teamPersonalProbe, err := runtime.RecallWorkspaceMemories(ctx, fixture.teamID,
		fixture.memberUserID, types.MemoryRecallQuery{Query: personalText, Limit: 5})
	if err != nil {
		return "", "", fmt.Errorf("probe personal exclusion from team memory: %w", err)
	}
	personalTeamProbe, err := runtime.RecallMemories(ctx, fixture.personalID,
		fixture.creatorUserID, types.MemoryRecallQuery{Query: teamText, Limit: 5})
	if err != nil {
		return "", "", fmt.Errorf("probe team exclusion from personal memory: %w", err)
	}
	if !recallContainsExactly(personalRecall, personalText) ||
		!recallContainsExactly(teamRecall, teamText) ||
		recallContains(teamPersonalProbe, personalText) ||
		recallContains(personalTeamProbe, teamText) {
		return "", "", errors.New("workspace memory UAT isolation result is inconsistent")
	}
	return personalText, teamText, nil
}

func verifyWorkspaceMemoryUATOwner(ctx context.Context, pool *pgxpool.Pool) error {
	var exact bool
	err := pool.QueryRow(ctx, `SELECT current_user=session_user
		AND current_user=pg_catalog.pg_get_userbyid(c.relowner)
		AND current_user<>'vane_server_runtime'
		FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
		WHERE n.nspname='public' AND c.relname='tenants'`).Scan(&exact)
	if err != nil || !exact {
		return errors.New("workspace memory UAT owner authority is invalid")
	}
	return nil
}

func createWorkspaceMemoryUATFixture(
	ctx context.Context, pool *pgxpool.Pool, operationID string,
) (workspaceMemoryUATFixture, error) {
	prefix := workspaceMemoryUATPrefix + operationID
	var fixture workspaceMemoryUATFixture
	err := pool.QueryRow(ctx, `WITH creator AS (
		INSERT INTO public.users(feishu_open_id,name) VALUES($1||':creator',$1||':creator')
		RETURNING id), member_user AS (
		INSERT INTO public.users(feishu_open_id,name) VALUES($1||':member',$1||':member')
		RETURNING id), personal AS (
		INSERT INTO public.tenants(status,plan,display_name,workspace_kind,
			personal_owner_user_id,seat_limit)
		SELECT 'active','free',$1||':personal','personal',creator.id,1 FROM creator
		RETURNING id), team AS (
		INSERT INTO public.tenants(status,plan,display_name,workspace_kind,seat_limit)
		VALUES('active','free',$1||':team','team',2) RETURNING id), member_rows AS (
		INSERT INTO public.memberships(tenant_id,user_id,role)
		SELECT personal.id,creator.id,'owner' FROM personal,creator UNION ALL
		SELECT team.id,creator.id,'member' FROM team,creator UNION ALL
		SELECT team.id,member_user.id,'member' FROM team,member_user),
		personal_session AS (
		INSERT INTO public.agent_sessions(tenant_id,user_id)
		SELECT personal.id,creator.id FROM personal,creator RETURNING id),
		team_session AS (
		INSERT INTO public.agent_sessions(tenant_id,user_id)
		SELECT team.id,creator.id FROM team,creator RETURNING id)
		SELECT creator.id,member_user.id,personal.id,team.id,
			personal_session.id,team_session.id
		FROM creator,member_user,personal,team,personal_session,team_session`, prefix).Scan(
		&fixture.creatorUserID, &fixture.memberUserID, &fixture.personalID,
		&fixture.teamID, &fixture.personalSID, &fixture.teamSID)
	if err != nil {
		return workspaceMemoryUATFixture{}, fmt.Errorf("create atomic workspace memory UAT fixture: %w", err)
	}
	return fixture, nil
}

func cleanupStaleWorkspaceMemoryUAT(
	ctx context.Context, pool *pgxpool.Pool, ownerStore *store.Store,
) error {
	var tenants []int64
	err := pool.QueryRow(ctx, `SELECT COALESCE(array_agg(candidate.id ORDER BY candidate.id),
		'{}'::bigint[]) FROM (SELECT DISTINCT t.id FROM public.tenants t
		WHERE t.display_name LIKE $1 AND (
			t.personal_owner_user_id IN(SELECT u.id FROM public.users u
				WHERE u.feishu_open_id LIKE $1) OR EXISTS(
				SELECT 1 FROM public.memberships m JOIN public.users u ON u.id=m.user_id
				WHERE m.tenant_id=t.id AND u.feishu_open_id LIKE $1))) candidate`,
		workspaceMemoryUATPrefix+"%").Scan(&tenants)
	if err != nil {
		return fmt.Errorf("list stale workspace memory UAT fixtures: %w", err)
	}
	for _, tenantID := range tenants {
		if _, err := ownerStore.PurgeTenant(ctx, tenantID, false); err != nil {
			return fmt.Errorf("purge stale workspace memory UAT tenant: %w", err)
		}
	}
	if _, err := pool.Exec(ctx, `DELETE FROM public.users u
		WHERE u.feishu_open_id LIKE $1
		AND NOT EXISTS(SELECT 1 FROM public.memberships m WHERE m.user_id=u.id)`,
		workspaceMemoryUATPrefix+"%"); err != nil {
		return fmt.Errorf("delete stale workspace memory UAT users: %w", err)
	}
	return nil
}

func cleanupWorkspaceMemoryUATFixture(
	ctx context.Context, pool *pgxpool.Pool, ownerStore *store.Store,
	fixture workspaceMemoryUATFixture,
) error {
	if fixture.teamID == 0 && fixture.personalID == 0 {
		return nil
	}
	for _, tenantID := range []int64{fixture.teamID, fixture.personalID} {
		if tenantID <= 0 {
			continue
		}
		if _, err := ownerStore.PurgeTenant(ctx, tenantID, false); err != nil {
			return fmt.Errorf("purge workspace memory UAT tenant: %w", err)
		}
	}
	tag, err := pool.Exec(ctx, `DELETE FROM public.users WHERE id=ANY($1::bigint[])`,
		[]int64{fixture.creatorUserID, fixture.memberUserID})
	if err != nil {
		return fmt.Errorf("delete workspace memory UAT users: %w", err)
	}
	if tag.RowsAffected() != 2 {
		return fmt.Errorf("workspace memory UAT cleanup removed %d users, want 2", tag.RowsAffected())
	}
	var remaining int
	if err := pool.QueryRow(ctx, `SELECT
		(SELECT count(*) FROM public.tenants WHERE id=ANY($1::bigint[]))+
		(SELECT count(*) FROM public.users WHERE id=ANY($2::bigint[]))`,
		[]int64{fixture.teamID, fixture.personalID},
		[]int64{fixture.creatorUserID, fixture.memberUserID}).Scan(&remaining); err != nil {
		return fmt.Errorf("verify workspace memory UAT cleanup: %w", err)
	}
	if remaining != 0 {
		return errors.New("workspace memory UAT cleanup is incomplete")
	}
	return nil
}

func workspaceMemoryUATAction(operationID, scope, text string) types.MemoryAction {
	return types.MemoryAction{Action: types.MemoryActionRemember, Text: text,
		Evidence: types.MemoryEvidence{
			SourceType: types.MemoryEvidenceOwnerExplicitAgentTurn,
			SourceID: uuid.NewSHA1(uuid.NameSpaceOID,
				[]byte(operationID+":"+scope+":source")).String(),
			OwnerRequest:        "explicit workspace memory runtime production UAT",
			AuthorizationDigest: digestString(operationID + ":" + scope + ":authorization"),
		}}
}

func recallContainsExactly(result *types.MemoryRecallResult, text string) bool {
	return result != nil && len(result.Memories) == 1 && result.Memories[0].Memory.Text == text
}

func recallContains(result *types.MemoryRecallResult, text string) bool {
	if result == nil {
		return false
	}
	for _, item := range result.Memories {
		if item.Memory.Text == text {
			return true
		}
	}
	return false
}

func digestString(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}
