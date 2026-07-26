// cmd/runtimeadmin contains database-direct runtime maintenance operations.
// It is intentionally not mounted on HTTP, Agent, A2A, or Temporal ingress:
// only an operator with the deployment database configuration can run it.
//
// Baseline examples:
//
//	runtimeadmin baseline -mode dry-run -limit 100
//	runtimeadmin baseline -mode apply -confirm-apply -limit 100
//	runtimeadmin baseline -mode verify -after-tenant 1 -after-user 2 -after-task push-123
//	runtimeadmin agent-session-cutover -tenant 1 -user 2 -session 3 -action status
//	runtimeadmin agent-session-cutover -tenant 1 -user 2 -session 3 -action activate -confirm-cutover
//	runtimeadmin agent-action-cutover -tenant 1 -user 2 -action-id action-3 -operation status
//	runtimeadmin agent-action-cutover -tenant 1 -user 2 -action-id action-3 -operation activate -evidence ticket-123 -confirm-cutover
//
// Verify returns 0 only when every item in the page is verified and Next is
// absent. Exit 1 means a non-verified item, 2 means the command failed, and 3
// means the JSON Next cursor must be supplied to a subsequent verify call.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/YouToco/vane/config"
	"github.com/YouToco/vane/feedback"
	"github.com/YouToco/vane/feishu"
	"github.com/YouToco/vane/store"
	"github.com/YouToco/vane/types"
)

const (
	exitOK                        = 0
	exitVerifyFailed              = 1
	exitFailure                   = 2
	exitVerifyMorePages           = 3
	runTimeout                    = 2 * time.Minute
	maxSnapshotShadowAuditItems   = 1000
	maxLegacyBatch63EvidenceBytes = 1 << 20
)

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	if len(args) > 0 && args[0] == "legacy-batch63-repair" {
		return runLegacyBatch63Repair(args[1:])
	}
	if len(args) > 0 && args[0] == "snapshot-shadow" {
		return runSnapshotShadow(args[1:])
	}
	if len(args) > 0 && args[0] == "snapshot-cutover" {
		return runSnapshotCutover(args[1:])
	}
	if len(args) > 0 && args[0] == "agent-session-cutover" {
		return runAgentSessionCutover(args[1:])
	}
	if len(args) > 0 && args[0] == "agent-action-cutover" {
		return runAgentActionCutover(args[1:])
	}
	if len(args) == 0 || args[0] != "baseline" {
		fmt.Fprintln(os.Stderr,
			"用法: runtimeadmin baseline ... | snapshot-shadow ... | "+
				"snapshot-cutover ... | agent-session-cutover ... | "+
				"agent-action-cutover ... | "+
				"legacy-batch63-repair ...")
		return exitFailure
	}
	fs := flag.NewFlagSet("runtimeadmin baseline", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	rawMode := fs.String("mode", "dry-run", "dry-run、apply 或 verify")
	limit := fs.Int("limit", 100, "每页任务数（1-1000）")
	afterTenant := fs.Int64("after-tenant", 0, "exclusive cursor tenant id")
	afterUser := fs.Int64("after-user", 0, "exclusive cursor user id")
	afterTask := fs.String("after-task", "", "exclusive cursor task id")
	confirmApply := fs.Bool("confirm-apply", false, "确认 apply 会写 immutable v1 head")
	if err := fs.Parse(args[1:]); err != nil {
		return exitFailure
	}

	mode, ok := parseBaselineMode(*rawMode)
	if !ok {
		fmt.Fprintln(os.Stderr, "runtimeadmin: -mode 仅支持 dry-run、apply、verify")
		return exitFailure
	}
	if mode == store.TaskDefinitionBaselineApply && !*confirmApply {
		fmt.Fprintln(os.Stderr, "runtimeadmin: apply 必须显式提供 -confirm-apply")
		return exitFailure
	}

	cfg, err := config.Load("")
	if err != nil {
		fmt.Fprintln(os.Stderr, "runtimeadmin: 加载配置失败")
		return exitFailure
	}
	ctx, stop := signal.NotifyContext(
		context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, runTimeout)
	defer cancel()

	st, err := store.New(ctx, cfg.DB.URL)
	if err != nil {
		fmt.Fprintln(os.Stderr, "runtimeadmin: 连接数据库失败")
		return exitFailure
	}
	defer st.Close()

	page, err := st.ReconcileTaskDefinitionBaselines(
		ctx, mode, store.TaskDefinitionBaselineCursor{
			TenantID: *afterTenant,
			UserID:   *afterUser,
			TaskID:   *afterTask,
		}, *limit)
	return finishBaselineRun(os.Stdout, os.Stderr, mode, page, err)
}

type legacyBatch63RepairAction string

const (
	legacyBatch63RepairPreview legacyBatch63RepairAction = "preview"
	legacyBatch63RepairApply   legacyBatch63RepairAction = "apply"
	legacyBatch63RepairVerify  legacyBatch63RepairAction = "verify"
	legacyBatch63RepairAbort   legacyBatch63RepairAction = "abort"
)

type legacyBatch63RepairOptions struct {
	Action       legacyBatch63RepairAction
	EvidencePath string
	PlanDigest   string
	ExpiresAt    time.Time
	ConfirmApply bool
	ConfirmAbort bool
}

type legacyBatch63RepairOutput struct {
	Phase      string     `json:"phase"`
	PlanDigest string     `json:"plan_digest"`
	EnableBy   *time.Time `json:"enable_by"`
	ExpiresAt  *time.Time `json:"expires_at"`
	Remaining  int64      `json:"remaining"`
}

type legacyBatch63RepairStore interface {
	PreviewLegacyBatch63Repair(
		context.Context,
		store.LegacyBatch63RepairEvidence,
		time.Time,
		store.LegacyBatch63CardBuilder,
	) (store.LegacyBatch63RepairPlan, error)
	FinalizeLegacyBatch63Repair(
		context.Context,
		string,
		store.LegacyBatch63RepairEvidence,
		time.Time,
		store.LegacyBatch63CardBuilder,
	) (store.LegacyBatch63RepairStatus, error)
	VerifyLegacyBatch63Repair(
		context.Context,
	) (store.LegacyBatch63RepairStatus, error)
	AbortLegacyBatch63Repair(
		context.Context,
		string,
	) (store.LegacyBatch63RepairStatus, error)
}

func runLegacyBatch63Repair(args []string) int {
	opts, ok := parseLegacyBatch63RepairOptions(args, os.Stderr)
	if !ok {
		return exitFailure
	}
	var evidenceBytes []byte
	var err error
	if opts.Action == legacyBatch63RepairPreview ||
		opts.Action == legacyBatch63RepairApply {
		evidenceBytes, err = readLegacyBatch63Evidence(opts.EvidencePath)
		if err != nil {
			return finishLegacyBatch63RepairRun(
				os.Stdout, os.Stderr, legacyBatch63RepairOutput{},
				types.NewAppError(
					types.CodeValidation, "repair evidence file is invalid", nil))
		}
	}
	cfg, err := config.Load("")
	if err != nil {
		fmt.Fprintln(os.Stderr, "runtimeadmin: 加载配置失败")
		return exitFailure
	}
	ctx, stop := signal.NotifyContext(
		context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, runTimeout)
	defer cancel()
	st, err := store.New(ctx, cfg.DB.URL)
	if err != nil {
		fmt.Fprintln(os.Stderr, "runtimeadmin: 连接数据库失败")
		return exitFailure
	}
	defer st.Close()
	output, err := executeLegacyBatch63Repair(
		ctx, st, opts, evidenceBytes, time.Now().UTC())
	return finishLegacyBatch63RepairRun(os.Stdout, os.Stderr, output, err)
}

func executeLegacyBatch63Repair(
	ctx context.Context,
	st legacyBatch63RepairStore,
	opts legacyBatch63RepairOptions,
	evidenceBytes []byte,
	now time.Time,
) (legacyBatch63RepairOutput, error) {
	evidence := store.LegacyBatch63RepairEvidence{
		CanonicalBytes: evidenceBytes,
	}
	switch opts.Action {
	case legacyBatch63RepairPreview:
		plan, err := st.PreviewLegacyBatch63Repair(
			ctx, evidence, opts.ExpiresAt, buildLegacyBatch63Card)
		if err != nil {
			return legacyBatch63RepairOutput{}, err
		}
		enableBy, expiresAt := plan.EnableBy.UTC(), plan.ExpiresAt.UTC()
		return legacyBatch63RepairOutput{
			Phase:      string(legacyBatch63RepairPreview),
			PlanDigest: plan.PlanDigest,
			EnableBy:   &enableBy,
			ExpiresAt:  &expiresAt,
			Remaining: legacyBatch63Remaining(
				&expiresAt, plan.DatabaseNow),
		}, nil
	case legacyBatch63RepairApply:
		status, err := st.FinalizeLegacyBatch63Repair(
			ctx, opts.PlanDigest, evidence, opts.ExpiresAt,
			buildLegacyBatch63Card)
		return legacyBatch63OutputFromStatus(status, now), err
	case legacyBatch63RepairVerify:
		status, err := st.VerifyLegacyBatch63Repair(ctx)
		return legacyBatch63OutputFromStatus(status, now), err
	case legacyBatch63RepairAbort:
		status, err := st.AbortLegacyBatch63Repair(
			ctx, opts.PlanDigest)
		return legacyBatch63OutputFromStatus(status, now), err
	default:
		return legacyBatch63RepairOutput{}, types.NewAppError(
			types.CodeValidation, "legacy repair action is invalid", nil)
	}
}

func buildLegacyBatch63Card(input store.LegacyBatch63CardInput) string {
	items := make([]feedback.CardInput, len(input.Items))
	for index, item := range input.Items {
		items[index] = feedback.CardInput{
			BodyMD: item.BodyMD, DeliveryID: item.DeliveryID,
			Title: item.Title, Score: item.Score, URL: item.URL,
			SourceTitle: item.SourceTitle,
			Platform:    types.Platform(item.Platform),
			PublishedAt: item.PublishedAt,
		}
	}
	headerTitle, headerTemplate :=
		feishu.AggHeaderForTask(input.TaskTitle, len(items))
	return feishu.BuildAggregateCard(feedback.AggregateCardInput{
		HeaderTitle: headerTitle, HeaderTemplate: headerTemplate,
		EffectID: input.EffectID, Items: items,
	})
}

func legacyBatch63OutputFromStatus(
	status store.LegacyBatch63RepairStatus,
	_ time.Time,
) legacyBatch63RepairOutput {
	return legacyBatch63RepairOutput{
		Phase:      status.Phase,
		PlanDigest: status.PlanDigest,
		EnableBy:   status.EnableBy,
		ExpiresAt:  status.ExpiresAt,
		Remaining: legacyBatch63Remaining(
			status.ExpiresAt, status.DatabaseNow),
	}
}

func parseLegacyBatch63RepairOptions(
	args []string,
	stderr io.Writer,
) (legacyBatch63RepairOptions, bool) {
	fs := flag.NewFlagSet("runtimeadmin legacy-batch63-repair", flag.ContinueOnError)
	fs.SetOutput(stderr)
	rawAction := fs.String(
		"action", string(legacyBatch63RepairPreview),
		"preview、apply、verify 或 abort")
	evidencePath := fs.String(
		"evidence", "", "canonical evidence JSON file (preview/apply only)")
	planDigest := fs.String(
		"plan-digest", "", "exact preview plan digest (apply/abort only)")
	rawExpiresAt := fs.String(
		"expires-at", "", "exact RFC3339 provider UUID expiry (preview/apply only)")
	confirmApply := fs.Bool(
		"confirm-apply", false, "confirm the exact plan will be finalized")
	confirmAbort := fs.Bool(
		"confirm-abort", false, "confirm the exact unclaimed plan will be blocked")
	if err := fs.Parse(args); err != nil {
		return legacyBatch63RepairOptions{}, false
	}
	if fs.NArg() != 0 {
		fmt.Fprintln(stderr,
			"runtimeadmin: legacy-batch63-repair 不接受位置参数")
		return legacyBatch63RepairOptions{}, false
	}
	opts := legacyBatch63RepairOptions{
		Action:       legacyBatch63RepairAction(*rawAction),
		EvidencePath: *evidencePath,
		PlanDigest:   *planDigest,
		ConfirmApply: *confirmApply,
		ConfirmAbort: *confirmAbort,
	}
	if *rawExpiresAt != "" {
		expiresAt, err := time.Parse(time.RFC3339, *rawExpiresAt)
		if err != nil {
			fmt.Fprintln(stderr,
				"runtimeadmin: -expires-at 必须是 RFC3339 时间")
			return legacyBatch63RepairOptions{}, false
		}
		opts.ExpiresAt = expiresAt.UTC()
	}
	switch opts.Action {
	case legacyBatch63RepairPreview:
		if opts.EvidencePath == "" || opts.ExpiresAt.IsZero() ||
			opts.PlanDigest != "" ||
			opts.ConfirmApply || opts.ConfirmAbort {
			fmt.Fprintln(stderr,
				"runtimeadmin: preview 需要 -evidence 与 -expires-at")
			return legacyBatch63RepairOptions{}, false
		}
	case legacyBatch63RepairApply:
		if opts.EvidencePath == "" || !validLegacyBatch63PlanDigest(
			opts.PlanDigest) || opts.ExpiresAt.IsZero() ||
			!opts.ConfirmApply || opts.ConfirmAbort {
			fmt.Fprintln(stderr,
				"runtimeadmin: apply 需要 -evidence、-expires-at、-plan-digest 与 -confirm-apply")
			return legacyBatch63RepairOptions{}, false
		}
	case legacyBatch63RepairVerify:
		if opts.EvidencePath != "" || opts.PlanDigest != "" ||
			!opts.ExpiresAt.IsZero() ||
			opts.ConfirmApply || opts.ConfirmAbort {
			fmt.Fprintln(stderr,
				"runtimeadmin: verify 不接受 evidence、digest 或 confirm 参数")
			return legacyBatch63RepairOptions{}, false
		}
	case legacyBatch63RepairAbort:
		if opts.EvidencePath != "" || !validLegacyBatch63PlanDigest(
			opts.PlanDigest) || !opts.ExpiresAt.IsZero() ||
			opts.ConfirmApply || !opts.ConfirmAbort {
			fmt.Fprintln(stderr,
				"runtimeadmin: abort 需要 -plan-digest 与 -confirm-abort")
			return legacyBatch63RepairOptions{}, false
		}
	default:
		fmt.Fprintln(stderr,
			"runtimeadmin: legacy-batch63-repair -action 仅支持 preview、apply、verify、abort")
		return legacyBatch63RepairOptions{}, false
	}
	return opts, true
}

func validLegacyBatch63PlanDigest(value string) bool {
	if len(value) != 64 || value != strings.ToLower(value) {
		return false
	}
	for _, char := range value {
		if (char < '0' || char > '9') && (char < 'a' || char > 'f') {
			return false
		}
	}
	return true
}

func readLegacyBatch63Evidence(path string) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, errors.New("evidence file is unavailable")
	}
	defer file.Close()
	raw, err := io.ReadAll(io.LimitReader(
		file, maxLegacyBatch63EvidenceBytes+1))
	if err != nil {
		return nil, errors.New("evidence file is unavailable")
	}
	if len(raw) == 0 || len(raw) > maxLegacyBatch63EvidenceBytes {
		return nil, errors.New("evidence file size is invalid")
	}
	return raw, nil
}

func finishLegacyBatch63RepairRun(
	stdout io.Writer,
	stderr io.Writer,
	output legacyBatch63RepairOutput,
	runErr error,
) int {
	if runErr != nil {
		fmt.Fprintln(stderr, "runtimeadmin: "+safeError(
			runErr, "legacy batch 63 repair failed"))
		return exitFailure
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(output); err != nil {
		fmt.Fprintln(stderr, "runtimeadmin: 输出结果失败")
		return exitFailure
	}
	return exitOK
}

func legacyBatch63Remaining(expiresAt *time.Time, now time.Time) int64 {
	if expiresAt == nil {
		return 0
	}
	remaining := expiresAt.Sub(now.UTC())
	if remaining <= 0 {
		return 0
	}
	return int64(remaining / time.Second)
}

type agentSessionCutoverAction string

const (
	agentSessionCutoverStatus   agentSessionCutoverAction = "status"
	agentSessionCutoverActivate agentSessionCutoverAction = "activate"
	agentSessionCutoverRollback agentSessionCutoverAction = "rollback"
)

type agentSessionCutoverOptions struct {
	TenantID  int64
	UserID    int64
	SessionID int64
	Action    agentSessionCutoverAction
	Confirm   bool
}

type agentSessionCutoverOutput struct {
	TenantID           int64  `json:"tenant_id"`
	UserID             int64  `json:"user_id"`
	SessionID          int64  `json:"session_id"`
	Route              string `json:"route"`
	Generation         int64  `json:"generation"`
	EventID            int64  `json:"event_id,omitempty"`
	LedgerHeadSequence int64  `json:"ledger_head_sequence,omitempty"`
}

type agentSessionCutoverStore interface {
	GetAgentSessionProjectionAuthorityStatus(
		context.Context, int64, int64, int64,
	) (store.AgentSessionProjectionAuthorityStatus, error)
	ControlAgentSessionProjectionAuthority(
		context.Context, int64, int64, int64,
		store.AgentSessionProjectionAuthorityAction,
	) (store.AgentSessionProjectionAuthorityStatus, error)
}

func runAgentSessionCutover(args []string) int {
	opts, ok := parseAgentSessionCutoverOptions(args, os.Stderr)
	if !ok {
		return exitFailure
	}
	cfg, err := config.Load("")
	if err != nil {
		fmt.Fprintln(os.Stderr, "runtimeadmin: 加载配置失败")
		return exitFailure
	}
	ctx, stop := signal.NotifyContext(
		context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, runTimeout)
	defer cancel()
	st, err := store.New(ctx, cfg.DB.URL)
	if err != nil {
		fmt.Fprintln(os.Stderr, "runtimeadmin: 连接数据库失败")
		return exitFailure
	}
	defer st.Close()
	output, err := executeAgentSessionCutover(
		ctx, st, opts)
	return finishAgentSessionCutoverRun(os.Stdout, os.Stderr, output, err)
}

func parseAgentSessionCutoverOptions(
	args []string,
	stderr io.Writer,
) (agentSessionCutoverOptions, bool) {
	fs := flag.NewFlagSet(
		"runtimeadmin agent-session-cutover", flag.ContinueOnError)
	fs.SetOutput(stderr)
	tenantID := fs.Int64("tenant", 0, "exact tenant id")
	userID := fs.Int64("user", 0, "exact session owner user id")
	sessionID := fs.Int64("session", 0, "exact agent session id")
	rawAction := fs.String(
		"action", string(agentSessionCutoverStatus),
		"status、activate 或 rollback")
	confirm := fs.Bool(
		"confirm-cutover", false,
		"确认 activate/rollback 会持久化 session read authority")
	if err := fs.Parse(args); err != nil {
		return agentSessionCutoverOptions{}, false
	}
	if fs.NArg() != 0 {
		fmt.Fprintln(stderr,
			"runtimeadmin: agent-session-cutover 不接受位置参数")
		return agentSessionCutoverOptions{}, false
	}
	action := agentSessionCutoverAction(*rawAction)
	if *tenantID <= 0 || *userID <= 0 || *sessionID <= 0 {
		fmt.Fprintln(stderr,
			"runtimeadmin: agent-session-cutover requires positive "+
				"-tenant/-user/-session")
		return agentSessionCutoverOptions{}, false
	}
	switch action {
	case agentSessionCutoverStatus:
		if *confirm {
			fmt.Fprintln(stderr,
				"runtimeadmin: status 不接受 -confirm-cutover")
			return agentSessionCutoverOptions{}, false
		}
	case agentSessionCutoverActivate, agentSessionCutoverRollback:
		if !*confirm {
			fmt.Fprintln(stderr,
				"runtimeadmin: activate/rollback 必须显式提供 -confirm-cutover")
			return agentSessionCutoverOptions{}, false
		}
	default:
		fmt.Fprintln(stderr,
			"runtimeadmin: agent-session-cutover -action 仅支持 "+
				"status、activate、rollback")
		return agentSessionCutoverOptions{}, false
	}
	return agentSessionCutoverOptions{
		TenantID: *tenantID, UserID: *userID, SessionID: *sessionID,
		Action: action, Confirm: *confirm,
	}, true
}

func executeAgentSessionCutover(
	ctx context.Context,
	st agentSessionCutoverStore,
	opts agentSessionCutoverOptions,
) (agentSessionCutoverOutput, error) {
	if opts.TenantID <= 0 || opts.UserID <= 0 || opts.SessionID <= 0 {
		return agentSessionCutoverOutput{}, types.NewAppError(
			types.CodeValidation,
			"agent session cutover requires exact positive scope", nil,
		)
	}
	switch opts.Action {
	case agentSessionCutoverStatus:
		if opts.Confirm {
			return agentSessionCutoverOutput{}, types.NewAppError(
				types.CodeValidation,
				"agent session cutover status rejects write confirmation", nil,
			)
		}
	case agentSessionCutoverActivate, agentSessionCutoverRollback:
		if !opts.Confirm {
			return agentSessionCutoverOutput{}, types.NewAppError(
				types.CodeValidation,
				"agent session cutover mutation requires confirmation", nil,
			)
		}
	default:
		return agentSessionCutoverOutput{}, types.NewAppError(
			types.CodeValidation,
			"agent session cutover action is invalid", nil,
		)
	}
	var (
		status store.AgentSessionProjectionAuthorityStatus
		err    error
	)
	if opts.Action == agentSessionCutoverStatus {
		status, err = st.GetAgentSessionProjectionAuthorityStatus(
			ctx, opts.TenantID, opts.UserID, opts.SessionID)
	} else {
		action := store.AgentSessionProjectionAuthorityActivate
		if opts.Action == agentSessionCutoverRollback {
			action = store.AgentSessionProjectionAuthorityRollback
		}
		status, err = st.ControlAgentSessionProjectionAuthority(
			ctx, opts.TenantID, opts.UserID, opts.SessionID, action)
	}
	if err != nil {
		return agentSessionCutoverOutput{}, err
	}
	return agentSessionCutoverOutput{
		TenantID: status.TenantID, UserID: status.UserID,
		SessionID: status.SessionID, Route: string(status.Route),
		Generation: status.Generation, EventID: status.EventID,
		LedgerHeadSequence: status.LedgerHeadSequence,
	}, nil
}

func finishAgentSessionCutoverRun(
	stdout io.Writer,
	stderr io.Writer,
	output agentSessionCutoverOutput,
	runErr error,
) int {
	if runErr != nil {
		fmt.Fprintln(stderr, "runtimeadmin: "+safeError(
			runErr, "agent session cutover operation failed"))
		return exitFailure
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(output); err != nil {
		fmt.Fprintln(stderr, "runtimeadmin: 输出结果失败")
		return exitFailure
	}
	return exitOK
}

type agentActionCutoverOperation string

const (
	agentActionCutoverStatus   agentActionCutoverOperation = "status"
	agentActionCutoverActivate agentActionCutoverOperation = "activate"
	agentActionCutoverRollback agentActionCutoverOperation = "rollback"
)

type agentActionCutoverOptions struct {
	TenantID  int64
	UserID    int64
	ActionID  string
	Operation agentActionCutoverOperation
	Evidence  string
	Confirm   bool
}

type agentActionCutoverOutput struct {
	TenantID           int64      `json:"tenant_id"`
	UserID             int64      `json:"user_id"`
	ActionID           string     `json:"action_id"`
	SessionID          int64      `json:"session_id"`
	SourceID           int64      `json:"source_id"`
	ExecutionVersion   int        `json:"execution_version"`
	Route              string     `json:"route"`
	Generation         int64      `json:"generation"`
	Status             string     `json:"status"`
	TerminalCode       *string    `json:"terminal_code,omitempty"`
	AttemptCount       int        `json:"attempt_count"`
	LeaseOwner         *string    `json:"lease_owner,omitempty"`
	LeaseFence         int64      `json:"lease_fence"`
	LeaseExpiresAt     *time.Time `json:"lease_expires_at,omitempty"`
	NextAttemptAt      *time.Time `json:"next_attempt_at,omitempty"`
	ConfirmedAt        *time.Time `json:"confirmed_at,omitempty"`
	CompletedAt        *time.Time `json:"completed_at,omitempty"`
	BlockedReason      *string    `json:"blocked_reason,omitempty"`
	ActivationEligible bool       `json:"activation_eligible"`
	RollbackEligible   bool       `json:"rollback_eligible"`
}

type agentActionCutoverStore interface {
	GetAgentActionContinuationStatus(
		context.Context, int64, int64, string,
	) (store.AgentActionContinuationStatus, error)
	ActivateAgentActionContinuation(
		context.Context, int64, int64, string, string,
	) (int64, error)
	RollbackAgentActionContinuation(
		context.Context, int64, int64, string, string,
	) (int64, error)
}

func runAgentActionCutover(args []string) int {
	opts, ok := parseAgentActionCutoverOptions(args, os.Stderr)
	if !ok {
		return exitFailure
	}
	cfg, err := config.Load("")
	if err != nil {
		fmt.Fprintln(os.Stderr, "runtimeadmin: 加载配置失败")
		return exitFailure
	}
	ctx, stop := signal.NotifyContext(
		context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, runTimeout)
	defer cancel()
	st, err := store.New(ctx, cfg.DB.URL)
	if err != nil {
		fmt.Fprintln(os.Stderr, "runtimeadmin: 连接数据库失败")
		return exitFailure
	}
	defer st.Close()
	output, err := executeAgentActionCutover(ctx, st, opts)
	return finishAgentActionCutoverRun(os.Stdout, os.Stderr, output, err)
}

func parseAgentActionCutoverOptions(
	args []string,
	stderr io.Writer,
) (agentActionCutoverOptions, bool) {
	fs := flag.NewFlagSet(
		"runtimeadmin agent-action-cutover", flag.ContinueOnError)
	fs.SetOutput(stderr)
	tenantID := fs.Int64("tenant", 0, "exact tenant id")
	userID := fs.Int64("user", 0, "exact action owner user id")
	actionID := fs.String("action-id", "", "exact pending action id")
	rawOperation := fs.String(
		"operation", string(agentActionCutoverStatus),
		"status、activate 或 rollback")
	evidence := fs.String(
		"evidence", "", "operator evidence bound to activate/rollback")
	confirm := fs.Bool(
		"confirm-cutover", false,
		"确认 activate/rollback 会持久化 exact action authority")
	if err := fs.Parse(args); err != nil {
		return agentActionCutoverOptions{}, false
	}
	if fs.NArg() != 0 {
		fmt.Fprintln(stderr,
			"runtimeadmin: agent-action-cutover 不接受位置参数")
		return agentActionCutoverOptions{}, false
	}
	if *tenantID <= 0 || *userID <= 0 || *actionID == "" ||
		strings.TrimSpace(*actionID) != *actionID {
		fmt.Fprintln(stderr,
			"runtimeadmin: agent-action-cutover requires positive "+
				"-tenant/-user and exact -action-id")
		return agentActionCutoverOptions{}, false
	}
	operation := agentActionCutoverOperation(*rawOperation)
	switch operation {
	case agentActionCutoverStatus:
		if *confirm || *evidence != "" {
			fmt.Fprintln(stderr,
				"runtimeadmin: status 不接受 -confirm-cutover 或 -evidence")
			return agentActionCutoverOptions{}, false
		}
	case agentActionCutoverActivate, agentActionCutoverRollback:
		if !*confirm || *evidence == "" ||
			strings.TrimSpace(*evidence) != *evidence ||
			len(*evidence) > 512 {
			fmt.Fprintln(stderr,
				"runtimeadmin: activate/rollback 必须显式提供 "+
					"-confirm-cutover 与 exact -evidence")
			return agentActionCutoverOptions{}, false
		}
	default:
		fmt.Fprintln(stderr,
			"runtimeadmin: agent-action-cutover -operation 仅支持 "+
				"status、activate、rollback")
		return agentActionCutoverOptions{}, false
	}
	return agentActionCutoverOptions{
		TenantID:  *tenantID,
		UserID:    *userID,
		ActionID:  *actionID,
		Operation: operation,
		Evidence:  *evidence,
		Confirm:   *confirm,
	}, true
}

func executeAgentActionCutover(
	ctx context.Context,
	st agentActionCutoverStore,
	opts agentActionCutoverOptions,
) (agentActionCutoverOutput, error) {
	if opts.TenantID <= 0 || opts.UserID <= 0 || opts.ActionID == "" ||
		strings.TrimSpace(opts.ActionID) != opts.ActionID {
		return agentActionCutoverOutput{}, types.NewAppError(
			types.CodeValidation,
			"agent action cutover requires exact positive scope", nil,
		)
	}
	switch opts.Operation {
	case agentActionCutoverStatus:
		if opts.Confirm || opts.Evidence != "" {
			return agentActionCutoverOutput{}, types.NewAppError(
				types.CodeValidation,
				"agent action cutover status rejects mutation evidence", nil,
			)
		}
	case agentActionCutoverActivate, agentActionCutoverRollback:
		if !opts.Confirm || opts.Evidence == "" ||
			strings.TrimSpace(opts.Evidence) != opts.Evidence ||
			len(opts.Evidence) > 512 {
			return agentActionCutoverOutput{}, types.NewAppError(
				types.CodeValidation,
				"agent action cutover mutation requires exact confirmation", nil,
			)
		}
	default:
		return agentActionCutoverOutput{}, types.NewAppError(
			types.CodeValidation,
			"agent action cutover operation is invalid", nil,
		)
	}
	if opts.Operation == agentActionCutoverActivate {
		if _, err := st.ActivateAgentActionContinuation(
			ctx, opts.TenantID, opts.UserID, opts.ActionID, opts.Evidence,
		); err != nil {
			return agentActionCutoverOutput{}, err
		}
	}
	if opts.Operation == agentActionCutoverRollback {
		if _, err := st.RollbackAgentActionContinuation(
			ctx, opts.TenantID, opts.UserID, opts.ActionID, opts.Evidence,
		); err != nil {
			return agentActionCutoverOutput{}, err
		}
	}
	status, err := st.GetAgentActionContinuationStatus(
		ctx, opts.TenantID, opts.UserID, opts.ActionID)
	if err != nil {
		return agentActionCutoverOutput{}, err
	}
	return agentActionCutoverOutput{
		TenantID:           status.TenantID,
		UserID:             status.UserID,
		ActionID:           status.ActionID,
		SessionID:          status.SessionID,
		SourceID:           status.SourceID,
		ExecutionVersion:   status.ExecutionVersion,
		Route:              status.Route,
		Generation:         status.Generation,
		Status:             status.Status,
		TerminalCode:       status.TerminalCode,
		AttemptCount:       status.AttemptCount,
		LeaseOwner:         status.LeaseOwner,
		LeaseFence:         status.LeaseFence,
		LeaseExpiresAt:     status.LeaseExpiresAt,
		NextAttemptAt:      status.NextAttemptAt,
		ConfirmedAt:        status.ConfirmedAt,
		CompletedAt:        status.CompletedAt,
		BlockedReason:      status.BlockedReason,
		ActivationEligible: status.ActivationEligible,
		RollbackEligible:   status.RollbackEligible,
	}, nil
}

func finishAgentActionCutoverRun(
	stdout io.Writer,
	stderr io.Writer,
	output agentActionCutoverOutput,
	runErr error,
) int {
	if runErr != nil {
		fmt.Fprintln(stderr, "runtimeadmin: "+safeError(
			runErr, "agent action cutover operation failed"))
		return exitFailure
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(output); err != nil {
		fmt.Fprintln(stderr, "runtimeadmin: 输出结果失败")
		return exitFailure
	}
	return exitOK
}

func runSnapshotCutover(args []string) int {
	fs := flag.NewFlagSet("runtimeadmin snapshot-cutover", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	tenantID := fs.Int64("tenant", 0, "exact tenant id")
	userID := fs.Int64("user", 0, "exact task owner user id")
	taskID := fs.String("task", "", "exact task id")
	rawAction := fs.String(
		"action", "status", "status、activate 或 rollback")
	confirm := fs.Bool(
		"confirm-cutover", false,
		"确认 activate/rollback 会持久化 cutover event 与任务 pointer")
	if err := fs.Parse(args); err != nil {
		return exitFailure
	}
	if fs.NArg() != 0 {
		fmt.Fprintln(os.Stderr,
			"runtimeadmin: snapshot-cutover 不接受位置参数")
		return exitFailure
	}
	if *tenantID <= 0 || *userID <= 0 || *taskID == "" {
		fmt.Fprintln(os.Stderr,
			"runtimeadmin: snapshot-cutover requires positive -tenant/-user "+
				"and exact -task")
		return exitFailure
	}
	var action store.TaskRunSnapshotCutoverAction
	switch *rawAction {
	case "status":
	case string(store.TaskRunSnapshotCutoverActivate):
		action = store.TaskRunSnapshotCutoverActivate
	case string(store.TaskRunSnapshotCutoverRollback):
		action = store.TaskRunSnapshotCutoverRollback
	default:
		fmt.Fprintln(os.Stderr,
			"runtimeadmin: snapshot-cutover -action 仅支持 status、activate、rollback")
		return exitFailure
	}
	if *rawAction != "status" && !*confirm {
		fmt.Fprintln(os.Stderr,
			"runtimeadmin: activate/rollback 必须显式提供 -confirm-cutover")
		return exitFailure
	}

	cfg, err := config.Load("")
	if err != nil {
		fmt.Fprintln(os.Stderr, "runtimeadmin: 加载配置失败")
		return exitFailure
	}
	ctx, stop := signal.NotifyContext(
		context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, runTimeout)
	defer cancel()
	st, err := store.New(ctx, cfg.DB.URL)
	if err != nil {
		fmt.Fprintln(os.Stderr, "runtimeadmin: 连接数据库失败")
		return exitFailure
	}
	defer st.Close()

	var output any
	if *rawAction == "status" {
		output, err = st.GetTaskRunSnapshotCutoverStatus(
			ctx, *tenantID, *userID, *taskID)
	} else {
		output, err = st.ControlTaskRunSnapshotCutover(
			ctx, *tenantID, *userID, *taskID, action)
	}
	return finishSnapshotCutoverRun(os.Stdout, os.Stderr, output, err)
}

func finishSnapshotCutoverRun(
	stdout io.Writer,
	stderr io.Writer,
	output any,
	runErr error,
) int {
	if runErr != nil {
		fmt.Fprintln(stderr, "runtimeadmin: "+safeError(
			runErr, "snapshot cutover operation failed"))
		return exitFailure
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(output); err != nil {
		fmt.Fprintln(stderr, "runtimeadmin: 输出结果失败")
		return exitFailure
	}
	return exitOK
}

func runSnapshotShadow(args []string) int {
	fs := flag.NewFlagSet("runtimeadmin snapshot-shadow", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	taskID := fs.String("task", "", "exact canary task id")
	rawSince := fs.String("since", "", "inclusive RFC3339 canary start")
	expectedCount := fs.Int("expected-count", 0, "exact number of rows required")
	limit := fs.Int("limit", 100, "page size (1-1000)")
	if err := fs.Parse(args); err != nil {
		return exitFailure
	}
	since, err := time.Parse(time.RFC3339, *rawSince)
	if err != nil || *taskID == "" || *expectedCount <= 0 {
		fmt.Fprintln(os.Stderr,
			"runtimeadmin: snapshot-shadow requires -task, RFC3339 -since, "+
				"and -expected-count")
		return exitFailure
	}
	cfg, err := config.Load("")
	if err != nil {
		fmt.Fprintln(os.Stderr, "runtimeadmin: 加载配置失败")
		return exitFailure
	}
	ctx, stop := signal.NotifyContext(
		context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, runTimeout)
	defer cancel()
	st, err := store.New(ctx, cfg.DB.URL)
	if err != nil {
		fmt.Fprintln(os.Stderr, "runtimeadmin: 连接数据库失败")
		return exitFailure
	}
	defer st.Close()
	scope, err := st.FreezeTaskRunSnapshotShadowAuditScope(ctx, *taskID, since)
	if err != nil {
		return finishSnapshotShadowRun(
			os.Stdout, os.Stderr, store.TaskRunSnapshotShadowAuditPage{},
			*expectedCount, err)
	}
	if scope.Count <= 0 || scope.Count > maxSnapshotShadowAuditItems ||
		int64(*expectedCount) != scope.Count {
		return finishSnapshotShadowRun(
			os.Stdout, os.Stderr, store.TaskRunSnapshotShadowAuditPage{},
			*expectedCount, types.NewAppError(types.CodeValidation,
				"snapshot shadow audit scope does not match the asserted count", nil))
	}
	page, err := collectSnapshotShadowAudit(
		ctx, *taskID, since, scope.ThroughID, *limit, int(scope.Count),
		st.AuditTaskRunSnapshotShadowsV2Through)
	return finishSnapshotShadowRun(
		os.Stdout, os.Stderr, page, *expectedCount, err)
}

type snapshotShadowPageLoader func(
	context.Context, string, time.Time, int64, int64, int,
) (store.TaskRunSnapshotShadowAuditPage, error)

// collectSnapshotShadowAudit always starts at zero and follows Store-issued
// cursors through the complete frozen interval. The CLI deliberately exposes
// no after-id escape hatch: an operator cannot present a clean suffix while
// skipping a bad prefix.
func collectSnapshotShadowAudit(
	ctx context.Context,
	taskID string,
	since time.Time,
	throughID int64,
	limit int,
	frozenCount int,
	load snapshotShadowPageLoader,
) (store.TaskRunSnapshotShadowAuditPage, error) {
	collected := store.TaskRunSnapshotShadowAuditPage{
		Items: make([]store.TaskRunSnapshotShadowAuditItem, 0, frozenCount),
	}
	var afterID int64
	for {
		page, err := load(ctx, taskID, since, afterID, throughID, limit)
		if err != nil {
			return store.TaskRunSnapshotShadowAuditPage{}, err
		}
		collected.Items = append(collected.Items, page.Items...)
		if len(collected.Items) > frozenCount {
			return collected, nil
		}
		if page.Next == nil {
			return collected, nil
		}
		if *page.Next <= afterID || *page.Next >= throughID {
			return store.TaskRunSnapshotShadowAuditPage{},
				types.NewAppError(types.CodeValidation,
					"snapshot shadow audit cursor is invalid", nil)
		}
		afterID = *page.Next
	}
}

func finishSnapshotShadowRun(
	stdout io.Writer,
	stderr io.Writer,
	page store.TaskRunSnapshotShadowAuditPage,
	expectedCount int,
	runErr error,
) int {
	if runErr != nil {
		fmt.Fprintln(stderr, "runtimeadmin: "+safeError(
			runErr, "snapshot shadow verify failed"))
		return exitFailure
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(page); err != nil {
		fmt.Fprintln(stderr, "runtimeadmin: 输出结果失败")
		return exitFailure
	}
	if len(page.Items) == 0 || len(page.Items) != expectedCount {
		return exitVerifyFailed
	}
	for _, item := range page.Items {
		if item.Status != store.TaskRunSnapshotShadowMatch ||
			item.TypedAuditStatus != store.CompiledRunSnapshotV2AuditMatch ||
			!item.TypedEqual {
			return exitVerifyFailed
		}
	}
	if page.Next != nil {
		return exitVerifyMorePages
	}
	return exitOK
}

func finishBaselineRun(
	stdout io.Writer,
	stderr io.Writer,
	mode store.TaskDefinitionBaselineMode,
	page store.TaskDefinitionBaselinePage,
	runErr error,
) int {
	if runErr != nil {
		fmt.Fprintln(stderr, "runtimeadmin: "+safeError(
			runErr, "任务 definition baseline 执行失败"))
		return exitFailure
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(page); err != nil {
		fmt.Fprintln(stderr, "runtimeadmin: 输出结果失败")
		return exitFailure
	}
	return baselineExitCode(mode, page)
}

func baselineExitCode(
	mode store.TaskDefinitionBaselineMode,
	page store.TaskDefinitionBaselinePage,
) int {
	if mode != store.TaskDefinitionBaselineVerify {
		return exitOK
	}
	for _, item := range page.Items {
		if item.Status != store.TaskDefinitionBaselineVerified {
			return exitVerifyFailed
		}
	}
	if page.Next != nil {
		return exitVerifyMorePages
	}
	return exitOK
}

func parseBaselineMode(raw string) (store.TaskDefinitionBaselineMode, bool) {
	switch raw {
	case "dry-run":
		return store.TaskDefinitionBaselineDryRun, true
	case "apply":
		return store.TaskDefinitionBaselineApply, true
	case "verify":
		return store.TaskDefinitionBaselineVerify, true
	default:
		return "", false
	}
}

func safeError(err error, fallback string) string {
	var appErr *types.AppError
	if errors.As(err, &appErr) {
		return appErr.Message
	}
	return fallback
}
