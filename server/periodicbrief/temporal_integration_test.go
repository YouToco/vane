//go:build integration

package periodicbrief

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	sdklog "go.temporal.io/sdk/log"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	sdkworkflow "go.temporal.io/sdk/workflow"

	"github.com/YouToco/vane/server/store"
	"github.com/YouToco/vane/server/types"
)

type temporalPeriodicActivityProbe struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (p *temporalPeriodicActivityProbe) Synthesize(
	context.Context,
	SynthesizeInputV1,
) (types.PeriodicBriefReportV1, error) {
	p.once.Do(func() { close(p.started) })
	<-p.release
	return types.PeriodicBriefReportV1{}, nil
}

func TestPeriodicWorkflowExternalTerminationReplaysAndRecoveryConverges(
	t *testing.T,
) {
	workflowInput := WorkflowInputV1{
		IntentID: 9, TenantID: 4, UserID: 5,
	}
	var (
		postgresStore *store.Store
		postgresTask  string
	)
	if dbURL := os.Getenv("DATABASE_URL"); dbURL != "" {
		var setupErr error
		postgresStore, postgresTask, workflowInput, setupErr =
			preparePeriodicTemporalPostgresFixture(t, dbURL)
		if setupErr != nil {
			t.Fatal(setupErr)
		}
	}
	namespace := fmt.Sprintf(
		"vane-periodic-integration-%d", time.Now().UnixNano())
	startCtx, cancelStart := context.WithTimeout(
		t.Context(), 2*time.Minute)
	server, err := testsuite.StartDevServer(
		startCtx,
		testsuite.DevServerOptions{
			ExistingPath:  os.Getenv("VANE_TEMPORAL_CLI_PATH"),
			ClientOptions: &client.Options{Namespace: namespace},
			LogLevel:      "error",
		},
	)
	cancelStart()
	if err != nil {
		t.Fatalf("start Temporal dev server: %v", err)
	}
	t.Cleanup(func() {
		server.Client().Close()
		if stopErr := server.Stop(); stopErr != nil {
			t.Errorf("stop Temporal dev server: %v", stopErr)
		}
	})

	probe := &temporalPeriodicActivityProbe{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	taskQueue := "vane-periodic-integration"
	temporalWorker := worker.New(server.Client(), taskQueue,
		worker.Options{})
	temporalWorker.RegisterWorkflowWithOptions(
		WorkflowV1,
		sdkworkflow.RegisterOptions{Name: WorkflowNameV1},
	)
	temporalWorker.RegisterActivityWithOptions(
		probe.Synthesize,
		activity.RegisterOptions{Name: "SynthesizePeriodicBriefV1"},
	)
	if err := temporalWorker.Start(); err != nil {
		t.Fatalf("start Temporal worker: %v", err)
	}
	var releaseOnce sync.Once
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(probe.release) })
		temporalWorker.Stop()
	})

	workflowID := fmt.Sprintf(
		"periodic-external-terminate-%d", time.Now().UnixNano())
	run, err := server.Client().ExecuteWorkflow(
		t.Context(),
		client.StartWorkflowOptions{
			ID: workflowID, TaskQueue: taskQueue,
		},
		WorkflowNameV1,
		workflowInput,
	)
	if err != nil {
		t.Fatalf("start periodic workflow: %v", err)
	}
	if postgresStore != nil {
		if err := postgresStore.BindPeriodicBriefIntentRunV1(
			t.Context(), workflowInput.TenantID, workflowInput.UserID,
			workflowInput.IntentID, run.GetRunID()); err != nil {
			t.Fatalf("bind real PostgreSQL Temporal run: %v", err)
		}
	}
	select {
	case <-probe.started:
	case <-time.After(10 * time.Second):
		t.Fatal("periodic synthesis activity did not start")
	}
	if err := server.Client().TerminateWorkflow(
		t.Context(), run.GetID(), run.GetRunID(),
		"P2-D integration external termination",
	); err != nil {
		t.Fatalf("terminate periodic workflow: %v", err)
	}
	if err := run.Get(t.Context(), nil); err == nil {
		t.Fatal("externally terminated workflow returned success")
	}
	description, err := server.Client().DescribeWorkflowExecution(
		t.Context(), run.GetID(), run.GetRunID())
	if err != nil {
		t.Fatal(err)
	}
	if got := description.GetWorkflowExecutionInfo().GetStatus(); got != enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED {
		t.Fatalf("periodic workflow status=%s", got)
	}

	history := &historypb.History{}
	iterator := server.Client().GetWorkflowHistory(
		t.Context(), run.GetID(), run.GetRunID(), true,
		enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for iterator.HasNext() {
		event, historyErr := iterator.Next()
		if historyErr != nil {
			t.Fatalf("read terminated history: %v", historyErr)
		}
		history.Events = append(history.Events, event)
	}
	replayer := worker.NewWorkflowReplayer()
	replayer.RegisterWorkflowWithOptions(
		WorkflowV1,
		sdkworkflow.RegisterOptions{Name: WorkflowNameV1},
	)
	if err := replayer.ReplayWorkflowHistory(
		sdklog.NewStructuredLogger(slog.New(
			slog.NewTextHandler(io.Discard, nil))),
		history,
	); err != nil {
		t.Fatalf("replay terminated periodic history: %v", err)
	}

	if postgresStore != nil {
		runner, runnerErr := NewRecoveryRunner(
			postgresStore, server.Client(),
			&periodicRecoverySenderFake{},
			"https://vane.example", postgresTask, nil)
		if runnerErr != nil {
			t.Fatal(runnerErr)
		}
		if runnerErr = runner.recoverOne(
			t.Context(),
			store.PeriodicSynthesisRecoveryCandidateV1{
				Kind: "prepared", IntentID: workflowInput.IntentID,
				TenantID:   workflowInput.TenantID,
				UserID:     workflowInput.UserID,
				WorkflowID: run.GetID(), TemporalRunID: run.GetRunID(),
			},
		); runnerErr != nil {
			t.Fatalf("recover real PostgreSQL report: %v", runnerErr)
		}
		page, listErr := postgresStore.ListPeriodicBriefReportsV1(
			t.Context(), workflowInput.TenantID, workflowInput.UserID,
			postgresTask, store.PeriodicBriefReportQueryV1{
				Cadence:  store.BriefReportCadenceWeekly,
				PageSize: 20,
			})
		if listErr != nil || len(page.Items) != 1 ||
			page.Items[0].GenerationMode !=
				types.ExecutiveGenerationFallback ||
			page.Items[0].Processing !=
				types.RunCompletenessPartial {
			t.Fatalf("real PostgreSQL recovery page=%+v err=%v",
				page, listErr)
		}
		releaseOnce.Do(func() { close(probe.release) })
		return
	}

	brief := periodicRecoveryBriefFixture(t)
	fakeStore := &periodicRecoveryStoreFake{
		loaded: store.PeriodicBriefIntentInputsV1{
			Intent: store.PeriodicBriefIntentV1{
				ID: 9, TenantID: 4, UserID: 5, TaskID: "task-v1",
				Cadence: "weekly", Timezone: "UTC",
				PeriodStart: brief.GeneratedAt.AddDate(0, 0, -1),
				PeriodEnd:   brief.GeneratedAt.AddDate(0, 0, 1),
				InputDigest: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" +
					"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				RunOutcomeIDs: []int64{8},
				OutcomeDigest: "dddddddddddddddddddddddddddddddd" +
					"dddddddddddddddddddddddddddddddd",
				SourceCoverage: types.RunCompletenessComplete,
				Processing:     types.RunCompletenessComplete,
			},
			Briefs: []types.BriefV1{brief},
		},
	}
	runner, err := NewRecoveryRunner(
		fakeStore, server.Client(),
		&periodicRecoverySenderFake{},
		"https://vane.example", "task-v1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := runner.recoverOne(
		t.Context(),
		store.PeriodicSynthesisRecoveryCandidateV1{
			Kind:          "prepared",
			IntentID:      9,
			TenantID:      4,
			UserID:        5,
			WorkflowID:    run.GetID(),
			TemporalRunID: run.GetRunID(),
		},
	); err != nil {
		t.Fatalf("recover externally terminated report: %v", err)
	}
	if fakeStore.recovered == nil ||
		fakeStore.recovered.GenerationMode !=
			types.ExecutiveGenerationFallback ||
		fakeStore.recovered.Processing !=
			types.RunCompletenessPartial {
		t.Fatalf("terminated recovery draft=%+v", fakeStore.recovered)
	}
	releaseOnce.Do(func() { close(probe.release) })
}

func preparePeriodicTemporalPostgresFixture(
	t *testing.T,
	dbURL string,
) (*store.Store, string, WorkflowInputV1, error) {
	t.Helper()
	if err := store.Migrate(t.Context(), dbURL); err != nil {
		return nil, "", WorkflowInputV1{}, err
	}
	st, err := store.New(t.Context(), dbURL)
	if err != nil {
		return nil, "", WorkflowInputV1{}, err
	}
	raw, err := pgxpool.New(t.Context(), dbURL)
	if err != nil {
		st.Close()
		return nil, "", WorkflowInputV1{}, err
	}
	t.Cleanup(func() {
		raw.Close()
		st.Close()
	})
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	var tenantID, userID int64
	if err := raw.QueryRow(t.Context(),
		`INSERT INTO tenants DEFAULT VALUES RETURNING id`,
	).Scan(&tenantID); err != nil {
		return nil, "", WorkflowInputV1{}, err
	}
	if err := raw.QueryRow(t.Context(),
		`INSERT INTO users(feishu_open_id,name)
		 VALUES($1,'P2-D Temporal integration') RETURNING id`,
		"periodic-temporal-"+suffix).Scan(&userID); err != nil {
		return nil, "", WorkflowInputV1{}, err
	}
	if _, err := raw.Exec(t.Context(),
		`INSERT INTO memberships(tenant_id,user_id,role)
		 VALUES($1,$2,'owner')`,
		tenantID, userID); err != nil {
		return nil, "", WorkflowInputV1{}, err
	}
	taskID := "task-periodic-temporal-" + suffix
	if _, err := raw.Exec(t.Context(),
		`INSERT INTO schedules(
		    id,user_id,tenant_id,nl_description,spec_json,scope_json,
		    status,execution_mode
		 ) VALUES($1,$2,$3,'P2-D Temporal integration',
		          '{"every_seconds":86400,"tz":"UTC"}','{}',
		          'active','compiled')`,
		taskID, userID, tenantID); err != nil {
		return nil, "", WorkflowInputV1{}, err
	}
	end := time.Now().Round(0).UTC().Truncate(time.Microsecond).
		AddDate(0, 0, -1)
	start := end.AddDate(0, 0, -7)
	intent, err := st.PreparePeriodicBriefIntentV1(
		t.Context(), tenantID, userID, taskID,
		store.BriefReportCadenceWeekly, start, end)
	if err != nil {
		return nil, "", WorkflowInputV1{}, err
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(
			context.Background(), 10*time.Second)
		defer cancel()
		if _, cleanupErr := raw.Exec(cleanupCtx,
			`UPDATE tenants
			    SET status='deleting',
			        purge_after=clock_timestamp()-interval '1 second'
			  WHERE id=$1`,
			tenantID); cleanupErr == nil {
			_, _ = st.PurgeTenant(cleanupCtx, tenantID, false)
		}
	})
	return st, taskID, WorkflowInputV1{
		IntentID: intent.ID, TenantID: tenantID, UserID: userID,
	}, nil
}
