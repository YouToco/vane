package workflow

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"

	"github.com/YouToco/vane/server/types"
)

type runOutcomeWorkflowCase struct {
	fetchItems        int
	fetchCoverage     types.RunCompletenessV1
	observation       string
	scoreItems        int
	scoreProcessing   types.RunCompletenessV1
	scoreErr          error
	cardItems         int
	cardProcessing    types.RunCompletenessV1
	dedupErr          error
	pushErr           error
	finalizeErr       error
	cancelAtEvolve    bool
	canonical         bool
	canonicalEmpty    bool
	executive         bool
	synthesisErr      error
	synthesisFallback bool
	freezeErr         error
	onFreeze          func(ExecutiveBriefFreezeIn)
}

func executeRunOutcomeWorkflowCase(
	t *testing.T, tc runOutcomeWorkflowCase,
) (types.RunOutcomeClaimV1, error) {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetStartWorkflowOptions(client.StartWorkflowOptions{
		ID: "wf-run-outcome-matrix",
	})
	if tc.cancelAtEvolve {
		env.SetOnActivityStartedListener(func(
			info *activity.Info, _ context.Context, _ converter.EncodedValues,
		) {
			if info.ActivityType.Name == "EvolveProfile" {
				env.CancelWorkflow()
			}
		})
	}
	reg := func(name string, fn any) {
		env.RegisterActivityWithOptions(
			fn, activity.RegisterOptions{Name: name})
	}
	reg("PrepareRun", func(
		ctx context.Context, in PushParams,
	) (PrepareRunResult, error) {
		info := activity.GetInfo(ctx).WorkflowExecution
		identity := types.RunIdentity{
			TemporalWorkflowID: info.ID,
			TemporalRunID:      info.RunID, RunKind: types.RunSnapshotKindScheduled,
			TenantID: in.TenantID, UserID: in.UserID, TaskID: in.ScheduleID,
		}
		return PrepareRunResult{
			Authorized: true, Snapshot: mustCompiledRunRef(identity, 701),
		}, nil
	})
	reg("BeginRunOutcomeV1", func(
		_ context.Context, in RunOutcomeBeginIn,
	) (types.RunOutcomeMarkerV1, error) {
		return types.RunOutcomeMarkerV1{
			ID: 801, SchemaVersion: types.RunOutcomeSchemaVersionV1,
			RunSnapshotID: in.Run.Snapshot.SnapshotID,
			TenantID:      in.Run.TenantID, UserID: in.UserID,
			TaskID: in.Run.TaskID,
		}, nil
	})
	reg("EvolveProfile", func(context.Context, EvolveIn) error { return nil })
	reg("FetchOutcomeV1", func(
		context.Context, PushParams,
	) (FetchOutcomeResult, error) {
		return FetchOutcomeResult{
			Items:          items(tc.fetchItems),
			SourceCoverage: tc.fetchCoverage,
		}, nil
	})
	reg("Dedup", func(
		_ context.Context, in DedupIn,
	) ([]types.ContentItem, error) {
		if tc.dedupErr != nil {
			return nil, tc.dedupErr
		}
		return in.Items, nil
	})
	reg("QualifyEvents", func(
		_ context.Context, in QualifyEventsIn,
	) (QualifyEventsResult, error) {
		if tc.observation == "uncertain" {
			return QualifyEventsResult{Outcome: "uncertain"}, nil
		}
		return QualifyEventsResult{
			Items: in.Items, Outcome: "not_configured",
		}, nil
	})
	reg("ScoreOutcomeV1", func(
		context.Context, ScoreIn,
	) (ScoreOutcomeResult, error) {
		if tc.scoreErr != nil {
			return ScoreOutcomeResult{}, tc.scoreErr
		}
		return ScoreOutcomeResult{
			Items:      scoredItems(tc.scoreItems),
			Processing: tc.scoreProcessing,
		}, nil
	})
	reg("Select", func(
		_ context.Context, in SelectIn,
	) ([]types.ScoredItem, error) {
		return in.Scored, nil
	})
	reg("CardGenOutcomeV1", func(
		context.Context, CardGenIn,
	) (CardGenOutcomeResult, error) {
		return CardGenOutcomeResult{
			Cards:      cardsOf(tc.cardItems),
			Processing: tc.cardProcessing,
		}, nil
	})
	reg("CardGenOutcomeV3", func(
		context.Context, CardGenIn,
	) (CardGenOutcomeResult, error) {
		return CardGenOutcomeResult{
			Cards:      cardsOf(tc.cardItems),
			Processing: tc.cardProcessing,
		}, nil
	})
	if tc.canonical {
		reg("PrepareCanonicalBriefV1", func(
			_ context.Context, in CanonicalBriefPrepareIn,
		) (CanonicalBriefPrepareResult, error) {
			if tc.canonicalEmpty {
				return CanonicalBriefPrepareResult{
					BatchID: 901, Empty: true,
				}, nil
			}
			draft := types.BriefDraftV1{
				SchemaVersion: types.BriefSchemaVersionV1,
				RunOutcomeID:  in.Marker.ID,
				RunSnapshotID: in.Run.Snapshot.SnapshotID,
				PushBatchID:   901,
				TenantID:      in.Run.TenantID,
				UserID:        in.UserID,
				TaskID:        in.Run.TaskID,
				GeneratedAt:   in.GeneratedAt,
				Insights: []types.InsightV1{{
					ID: 1001, RankPosition: 1,
					Title: "title", BodyMD: "body",
					SourceTitle:  "source",
					SourceURL:    "https://example.com/item",
					DiscoveredAt: time.Unix(10, 0).UTC(),
				}},
			}
			return CanonicalBriefPrepareResult{
				Draft: &draft, BatchID: draft.PushBatchID,
			}, nil
		})
	}
	if tc.executive {
		reg("SynthesizeExecutiveBriefV1", func(
			_ context.Context, in ExecutiveBriefSynthesizeIn,
		) (ExecutiveBriefSynthesizeResult, error) {
			if tc.synthesisErr != nil {
				return ExecutiveBriefSynthesizeResult{}, tc.synthesisErr
			}
			mode := types.ExecutiveGenerationModel
			processing := types.RunCompletenessComplete
			if tc.synthesisFallback {
				mode = types.ExecutiveGenerationFallback
				processing = types.RunCompletenessPartial
			}
			return ExecutiveBriefSynthesizeResult{
				Fallback: tc.synthesisFallback,
				ArtifactDraft: types.ExecutiveBriefArtifactDraftV1{
					SchemaVersion: types.ExecutiveBriefSchemaVersionV1,
					RunOutcomeID:  in.Marker.ID,
					RunSnapshotID: in.Run.Snapshot.SnapshotID,
					PushBatchID:   in.Draft.PushBatchID,
					TenantID:      in.Run.TenantID, UserID: in.UserID,
					TaskID:         in.Run.TaskID,
					ProfileDigest:  strings.Repeat("a", 64),
					InputDigest:    strings.Repeat("b", 64),
					GenerationMode: mode, Processing: processing,
					GeneratedAt: time.Unix(15, 0).UTC(),
					Content: types.ExecutiveBriefContentV1{
						Headline: "headline", ExecutiveSummary: "summary",
						DecisionState: types.ExecutiveDecisionWatch,
						WhyForYou:     "relevant",
						Signals: []types.ExecutiveSignalV1{{
							Kind:  types.ExecutiveSignalChange,
							Title: "change", Summary: "summary",
							EvidenceRefs: []types.ExecutiveEvidenceRefV1{{
								InsightID: 1001, ClaimIndexes: []int{0},
							}},
						}},
					},
				},
			}, nil
		})
		reg("FreezeExecutiveBriefV1", func(
			_ context.Context, in ExecutiveBriefFreezeIn,
		) (types.ExecutiveBriefArtifactV1, error) {
			if tc.onFreeze != nil {
				tc.onFreeze(in)
			}
			return types.ExecutiveBriefArtifactV1{}, tc.freezeErr
		})
	}
	reg("Push", func(context.Context, PushIn) error { return tc.pushErr })
	reg("RecordEmptyBatch", func(context.Context, RecordEmptyIn) error {
		return nil
	})
	reg("NotifyEmptyResult", func(context.Context, NotifyEmptyIn) error {
		return nil
	})
	var mu sync.Mutex
	var finalized types.RunOutcomeClaimV1
	reg("FinalizeRunOutcomeV1", func(
		_ context.Context, in RunOutcomeFinalizeIn,
	) (types.RunOutcomeV1, error) {
		mu.Lock()
		finalized = in.Claim
		mu.Unlock()
		if tc.finalizeErr != nil {
			return types.RunOutcomeV1{}, tc.finalizeErr
		}
		return in.Claim.SealAt(time.Unix(1, 0).UTC())
	})
	runtimeVersion := CompiledRuntimeRunOutcomeV1
	if tc.canonical {
		runtimeVersion = CompiledRuntimeCanonicalBriefV1
	}
	if tc.executive {
		runtimeVersion = CompiledRuntimeExecutiveBriefV1
	}
	env.ExecuteWorkflow(PushPipelineWorkflow, PushParams{
		TenantID: 7, UserID: 9, RunKind: PushRunKindScheduled,
		ExecutionMode:  types.ExecutionModeCompiled,
		RuntimeVersion: runtimeVersion,
		ScheduleID:     "task-run-outcome-matrix",
	})
	mu.Lock()
	claim := finalized
	mu.Unlock()
	return claim, env.GetWorkflowError()
}

func TestPushPipelineWorkflow_CanonicalEmptyIsQuiet(t *testing.T) {
	claim, err := executeRunOutcomeWorkflowCase(
		t, runOutcomeWorkflowCase{
			fetchItems: 2, fetchCoverage: types.RunCompletenessComplete,
			scoreItems: 2, scoreProcessing: types.RunCompletenessComplete,
			cardItems: 2, cardProcessing: types.RunCompletenessComplete,
			canonical: true, canonicalEmpty: true,
		})
	if err != nil {
		t.Fatal(err)
	}
	if claim.Result != types.RunResultQuiet ||
		claim.SourceCoverage != types.RunCompletenessComplete ||
		claim.Processing != types.RunCompletenessComplete {
		t.Fatalf("canonical empty claim = %+v", claim)
	}
}

func TestPushPipelineWorkflow_CanonicalEmptyPushFailureIsFailed(t *testing.T) {
	pushErr := temporal.NewNonRetryableApplicationError(
		"empty receipt unavailable", string(types.CodeDatabase), nil)
	claim, err := executeRunOutcomeWorkflowCase(
		t, runOutcomeWorkflowCase{
			fetchItems: 2, fetchCoverage: types.RunCompletenessComplete,
			scoreItems: 2, scoreProcessing: types.RunCompletenessComplete,
			cardItems: 2, cardProcessing: types.RunCompletenessComplete,
			canonical: true, canonicalEmpty: true, pushErr: pushErr,
		})
	if err == nil {
		t.Fatal("canonical empty receipt failure completed workflow")
	}
	if claim.Result != types.RunResultFailed ||
		claim.Processing != types.RunCompletenessPartial {
		t.Fatalf("canonical empty failure claim = %+v", claim)
	}
}

func TestPushPipelineWorkflow_ExecutiveBriefFinalizesThenFreezes(
	t *testing.T,
) {
	var frozen ExecutiveBriefFreezeIn
	claim, err := executeRunOutcomeWorkflowCase(
		t, runOutcomeWorkflowCase{
			fetchItems: 1, fetchCoverage: types.RunCompletenessComplete,
			scoreItems: 1, scoreProcessing: types.RunCompletenessComplete,
			cardItems: 1, cardProcessing: types.RunCompletenessComplete,
			canonical: true, executive: true,
			onFreeze: func(in ExecutiveBriefFreezeIn) {
				frozen = in
			},
		})
	if err != nil {
		t.Fatal(err)
	}
	if claim.Result != types.RunResultContent ||
		claim.Processing != types.RunCompletenessComplete ||
		frozen.Draft.RunOutcomeID != claim.ID {
		t.Fatalf("claim=%+v freeze=%+v", claim, frozen)
	}
}

func TestPushPipelineWorkflow_ExecutiveFallbackMarksProcessingPartial(
	t *testing.T,
) {
	claim, err := executeRunOutcomeWorkflowCase(
		t, runOutcomeWorkflowCase{
			fetchItems: 1, fetchCoverage: types.RunCompletenessComplete,
			scoreItems: 1, scoreProcessing: types.RunCompletenessComplete,
			cardItems: 1, cardProcessing: types.RunCompletenessComplete,
			canonical: true, executive: true, synthesisFallback: true,
		})
	if err != nil {
		t.Fatal(err)
	}
	if claim.Result != types.RunResultContent ||
		claim.Processing != types.RunCompletenessPartial {
		t.Fatalf("fallback claim = %+v", claim)
	}
}

func TestPushPipelineWorkflow_ExecutiveSynthesisFailureStillPushes(
	t *testing.T,
) {
	synthesisErr := temporal.NewNonRetryableApplicationError(
		"receipt unavailable", string(types.CodeDatabase), nil)
	claim, err := executeRunOutcomeWorkflowCase(
		t, runOutcomeWorkflowCase{
			fetchItems: 1, fetchCoverage: types.RunCompletenessComplete,
			scoreItems: 1, scoreProcessing: types.RunCompletenessComplete,
			cardItems: 1, cardProcessing: types.RunCompletenessComplete,
			canonical: true, executive: true, synthesisErr: synthesisErr,
		})
	if err != nil {
		t.Fatal(err)
	}
	if claim.Result != types.RunResultContent ||
		claim.Processing != types.RunCompletenessPartial {
		t.Fatalf("synthesis failure claim = %+v", claim)
	}
}

func TestPushPipelineWorkflow_RunOutcomeFaultMatrix(t *testing.T) {
	quota := temporal.NewNonRetryableApplicationError(
		"quota", string(types.CodeQuotaExceeded), nil)
	cases := []struct {
		name           string
		input          runOutcomeWorkflowCase
		wantResult     types.RunResultV1
		wantCoverage   types.RunCompletenessV1
		wantProcessing types.RunCompletenessV1
		wantCode       string
		wantWFError    bool
	}{
		{
			name: "content complete",
			input: runOutcomeWorkflowCase{
				fetchItems: 2, fetchCoverage: types.RunCompletenessComplete,
				scoreItems: 2, scoreProcessing: types.RunCompletenessComplete,
				cardItems: 2, cardProcessing: types.RunCompletenessComplete,
			},
			wantResult:     types.RunResultContent,
			wantCoverage:   types.RunCompletenessComplete,
			wantProcessing: types.RunCompletenessComplete,
		},
		{
			name: "fetch quiet partial coverage",
			input: runOutcomeWorkflowCase{
				fetchCoverage: types.RunCompletenessPartial,
			},
			wantResult:     types.RunResultQuiet,
			wantCoverage:   types.RunCompletenessPartial,
			wantProcessing: types.RunCompletenessComplete,
		},
		{
			name: "observation uncertain",
			input: runOutcomeWorkflowCase{
				fetchItems: 2, fetchCoverage: types.RunCompletenessComplete,
				observation: "uncertain",
			},
			wantResult:     types.RunResultQuiet,
			wantCoverage:   types.RunCompletenessComplete,
			wantProcessing: types.RunCompletenessPartial,
		},
		{
			name: "score partial",
			input: runOutcomeWorkflowCase{
				fetchItems: 2, fetchCoverage: types.RunCompletenessComplete,
				scoreItems: 1, scoreProcessing: types.RunCompletenessPartial,
				cardItems: 1, cardProcessing: types.RunCompletenessComplete,
			},
			wantResult:     types.RunResultContent,
			wantCoverage:   types.RunCompletenessComplete,
			wantProcessing: types.RunCompletenessPartial,
		},
		{
			name: "quota failed",
			input: runOutcomeWorkflowCase{
				fetchItems: 2, fetchCoverage: types.RunCompletenessComplete,
				scoreErr: quota,
			},
			wantResult:     types.RunResultFailed,
			wantCoverage:   types.RunCompletenessComplete,
			wantProcessing: types.RunCompletenessPartial,
			wantCode:       string(types.CodeQuotaExceeded),
		},
		{
			name: "activity failure",
			input: runOutcomeWorkflowCase{
				fetchItems: 2, fetchCoverage: types.RunCompletenessComplete,
				dedupErr: errors.New("driver raw detail"),
			},
			wantResult:     types.RunResultFailed,
			wantCoverage:   types.RunCompletenessComplete,
			wantProcessing: types.RunCompletenessPartial,
			wantCode:       "workflow_failed", wantWFError: true,
		},
		{
			name: "push failure retains content",
			input: runOutcomeWorkflowCase{
				fetchItems: 2, fetchCoverage: types.RunCompletenessComplete,
				scoreItems: 2, scoreProcessing: types.RunCompletenessComplete,
				cardItems: 2, cardProcessing: types.RunCompletenessComplete,
				pushErr: errors.New("push provider raw detail"),
			},
			wantResult:     types.RunResultContent,
			wantCoverage:   types.RunCompletenessComplete,
			wantProcessing: types.RunCompletenessPartial,
			wantWFError:    true,
		},
		{
			name: "finalize failure fails normal workflow",
			input: runOutcomeWorkflowCase{
				fetchCoverage: types.RunCompletenessComplete,
				finalizeErr:   errors.New("database unavailable"),
			},
			wantResult:     types.RunResultQuiet,
			wantCoverage:   types.RunCompletenessComplete,
			wantProcessing: types.RunCompletenessComplete,
			wantWFError:    true,
		},
		{
			name: "workflow cancellation",
			input: runOutcomeWorkflowCase{
				fetchCoverage:  types.RunCompletenessComplete,
				cancelAtEvolve: true,
			},
			wantResult:     types.RunResultInterrupted,
			wantCoverage:   types.RunCompletenessPartial,
			wantProcessing: types.RunCompletenessPartial,
			wantCode:       "workflow_canceled",
			wantWFError:    true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			claim, workflowErr := executeRunOutcomeWorkflowCase(t, tc.input)
			if (workflowErr != nil) != tc.wantWFError {
				t.Fatalf("workflow error = %v, want error=%t",
					workflowErr, tc.wantWFError)
			}
			if claim.Result != tc.wantResult ||
				claim.SourceCoverage != tc.wantCoverage ||
				claim.Processing != tc.wantProcessing ||
				claim.FailureCode != tc.wantCode {
				t.Fatalf("claim = %+v", claim)
			}
			if tc.wantCode == "workflow_failed" &&
				claim.FailureMessage == "driver raw detail" {
				t.Fatal("raw activity error escaped into durable claim")
			}
		})
	}
}

func TestRetryableActivityErrorPreservesControlledOutcomeCode(t *testing.T) {
	err := retryableOrNot(types.NewAppError(
		types.CodeLLMUnavailable,
		"sanitized model failure",
		errors.New("provider raw detail"),
	))
	var application *temporal.ApplicationError
	if !errors.As(err, &application) {
		t.Fatalf("retryable error type = %T", err)
	}
	if application.NonRetryable() ||
		application.Type() != string(types.CodeLLMUnavailable) ||
		application.Message() != "sanitized model failure" {
		t.Fatalf("retryable ApplicationError = %+v", application)
	}
	terminal := terminalRunOutcomeForError(err)
	if terminal.failureCode != string(types.CodeLLMUnavailable) ||
		terminal.failureMessage != "sanitized model failure" {
		t.Fatalf("terminal outcome = %+v", terminal)
	}
}

func TestPushPipelineWorkflow_CanonicalBriefStagesBeforePush(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetStartWorkflowOptions(client.StartWorkflowOptions{
		ID: "wf-canonical-brief-order",
	})
	var mu sync.Mutex
	var sequence []string
	var preparedMarker types.RunOutcomeMarkerV1
	var pushed PushIn
	reg := func(name string, fn any) {
		env.RegisterActivityWithOptions(
			fn, activity.RegisterOptions{Name: name})
	}
	reg("PrepareRun", func(
		ctx context.Context, in PushParams,
	) (PrepareRunResult, error) {
		info := activity.GetInfo(ctx).WorkflowExecution
		identity := types.RunIdentity{
			TemporalWorkflowID: info.ID,
			TemporalRunID:      info.RunID,
			RunKind:            types.RunSnapshotKindScheduled,
			TenantID:           in.TenantID,
			UserID:             in.UserID,
			TaskID:             in.ScheduleID,
		}
		return PrepareRunResult{
			Authorized: true, Snapshot: mustCompiledRunRef(identity, 711),
		}, nil
	})
	reg("BeginRunOutcomeV1", func(
		_ context.Context, in RunOutcomeBeginIn,
	) (types.RunOutcomeMarkerV1, error) {
		preparedMarker = types.RunOutcomeMarkerV1{
			ID: 811, SchemaVersion: types.RunOutcomeSchemaVersionV1,
			RunSnapshotID: in.Run.Snapshot.SnapshotID,
			TenantID:      in.Run.TenantID, UserID: in.UserID,
			TaskID: in.Run.TaskID,
		}
		return preparedMarker, nil
	})
	reg("EvolveProfile", func(context.Context, EvolveIn) error { return nil })
	reg("FetchOutcomeV1", func(
		context.Context, PushParams,
	) (FetchOutcomeResult, error) {
		return FetchOutcomeResult{
			Items:          items(2),
			SourceCoverage: types.RunCompletenessComplete,
		}, nil
	})
	reg("Dedup", func(
		_ context.Context, in DedupIn,
	) ([]types.ContentItem, error) {
		return in.Items, nil
	})
	reg("QualifyEvents", func(
		_ context.Context, in QualifyEventsIn,
	) (QualifyEventsResult, error) {
		return QualifyEventsResult{
			Items: in.Items, Outcome: "not_configured",
		}, nil
	})
	reg("ScoreOutcomeV1", func(
		context.Context, ScoreIn,
	) (ScoreOutcomeResult, error) {
		return ScoreOutcomeResult{
			Items:      scoredItems(2),
			Processing: types.RunCompletenessComplete,
		}, nil
	})
	reg("Select", func(
		_ context.Context, in SelectIn,
	) ([]types.ScoredItem, error) {
		return in.Scored, nil
	})
	reg("CardGenOutcomeV1", func(
		context.Context, CardGenIn,
	) (CardGenOutcomeResult, error) {
		return CardGenOutcomeResult{
			Cards:      cardsOf(2),
			Processing: types.RunCompletenessComplete,
		}, nil
	})
	reg("PrepareCanonicalBriefV1", func(
		_ context.Context, in CanonicalBriefPrepareIn,
	) (CanonicalBriefPrepareResult, error) {
		mu.Lock()
		sequence = append(sequence, "stage")
		mu.Unlock()
		draft := types.BriefDraftV1{
			SchemaVersion: types.BriefSchemaVersionV1,
			RunOutcomeID:  in.Marker.ID,
			RunSnapshotID: in.Run.Snapshot.SnapshotID,
			PushBatchID:   901,
			TenantID:      in.Run.TenantID, UserID: in.UserID,
			TaskID: in.Run.TaskID, GeneratedAt: in.GeneratedAt,
			Insights: []types.InsightV1{{
				ID: 1001, RankPosition: 1,
				Title: "title", BodyMD: "body",
				SourceTitle:  "source",
				SourceURL:    "https://example.com/one",
				DiscoveredAt: time.Unix(10, 0).UTC(),
			}},
		}
		return CanonicalBriefPrepareResult{
			Draft: &draft, BatchID: draft.PushBatchID,
		}, nil
	})
	reg("Push", func(_ context.Context, in PushIn) error {
		mu.Lock()
		sequence = append(sequence, "push")
		pushed = in
		mu.Unlock()
		return nil
	})
	reg("FinalizeRunOutcomeV1", func(
		_ context.Context, in RunOutcomeFinalizeIn,
	) (types.RunOutcomeV1, error) {
		mu.Lock()
		sequence = append(sequence, "finalize")
		mu.Unlock()
		return in.Claim.SealAt(time.Unix(20, 0).UTC())
	})
	reg("RecordEmptyBatch", func(context.Context, RecordEmptyIn) error {
		return nil
	})
	reg("NotifyEmptyResult", func(context.Context, NotifyEmptyIn) error {
		return nil
	})

	env.ExecuteWorkflow(PushPipelineWorkflow, PushParams{
		TenantID: 7, UserID: 9, RunKind: PushRunKindScheduled,
		ExecutionMode:  types.ExecutionModeCompiled,
		RuntimeVersion: CompiledRuntimeCanonicalBriefV1,
		ScheduleID:     "task-canonical-brief",
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(sequence) != 3 ||
		sequence[0] != "stage" ||
		sequence[1] != "push" ||
		sequence[2] != "finalize" {
		t.Fatalf("canonical command order = %v", sequence)
	}
	if pushed.CanonicalOutcome == nil ||
		*pushed.CanonicalOutcome != preparedMarker ||
		pushed.CanonicalBrief == nil ||
		pushed.CanonicalBrief.RunOutcomeID != preparedMarker.ID {
		t.Fatalf("canonical Push input = %+v", pushed)
	}
}
