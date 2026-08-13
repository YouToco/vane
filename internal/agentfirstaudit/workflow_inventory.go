package agentfirstaudit

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

const (
	legacyPushWorkflowType         = "PushPipelineWorkflow"
	legacyVisibilityPageSize int32 = 1000
	maxLegacyVisibilityPages       = 4096
	maxLegacyWorkflowRuns          = 100000
	maxLegacyHistoryPages          = 4096
	maxLegacyHistoryEvents         = 500000
	maxLegacyHistoryBytes          = 128 << 20
)

type LegacyWorkflowReader interface {
	ListWorkflowExecutions(context.Context, *workflowservice.ListWorkflowExecutionsRequest,
		...grpc.CallOption) (*workflowservice.ListWorkflowExecutionsResponse, error)
	ListArchivedWorkflowExecutions(context.Context,
		*workflowservice.ListArchivedWorkflowExecutionsRequest,
		...grpc.CallOption) (*workflowservice.ListArchivedWorkflowExecutionsResponse, error)
	DescribeWorkflowExecution(context.Context,
		*workflowservice.DescribeWorkflowExecutionRequest,
		...grpc.CallOption) (*workflowservice.DescribeWorkflowExecutionResponse, error)
	GetWorkflowExecutionHistory(context.Context,
		*workflowservice.GetWorkflowExecutionHistoryRequest,
		...grpc.CallOption) (*workflowservice.GetWorkflowExecutionHistoryResponse, error)
}

type WorkflowHistoryReplayer func(*historypb.History) error

type LegacyWorkflowRun struct {
	WorkflowID      string
	RunID           string
	Status          string
	AuthorityDigest string
	HistoryDigest   string
	HistoryEvents   int
}

type LegacyWorkflowInventory struct {
	Archived bool
	Count    int
	Digest   string
	Runs     []LegacyWorkflowRun
}

type legacyWorkflowRunV1 struct {
	AuthorityDigest string `json:"authority_digest"`
	HistoryDigest   string `json:"history_digest"`
	HistoryEvents   int    `json:"history_events"`
	RunID           string `json:"run_id"`
	SchemaVersion   string `json:"schema_version"`
	Status          string `json:"status"`
	WorkflowID      string `json:"workflow_id"`
}

// VerifyLegacyExecutionsPhysicallyAbsent is the disabled-archival prepared
// Gate. Empty visibility is insufficient: every run retained by the baseline
// must be absent from both mutable-state Describe and direct History reads.
func VerifyLegacyExecutionsPhysicallyAbsent(
	ctx context.Context,
	reader LegacyWorkflowReader,
	namespace string,
	runs []LegacyWorkflowRun,
) error {
	if reader == nil ||
		!boundedCanonicalTemporalText(namespace, maxTemporalAuthorityTextBytes) {
		return fmt.Errorf("legacy physical-absence request is invalid")
	}
	seen := make(map[string]struct{}, len(runs))
	for _, run := range runs {
		if !boundedCanonicalTemporalText(run.WorkflowID, maxTemporalAuthorityTextBytes) ||
			!canonicalUUID(run.RunID) {
			return fmt.Errorf("legacy physical-absence execution is invalid")
		}
		key := run.WorkflowID + "\x00" + run.RunID
		if _, duplicate := seen[key]; duplicate {
			return fmt.Errorf("legacy physical-absence execution is duplicated")
		}
		seen[key] = struct{}{}
		_, err := reader.DescribeWorkflowExecution(ctx,
			&workflowservice.DescribeWorkflowExecutionRequest{
				Namespace: namespace,
				Execution: &commonpb.WorkflowExecution{WorkflowId: run.WorkflowID, RunId: run.RunID},
			})
		if _, absent := errors.AsType[*serviceerror.NotFound](err); !absent {
			if err != nil {
				return fmt.Errorf("negative-probe legacy workflow description: %w", err)
			}
			return fmt.Errorf("legacy workflow description remains readable")
		}
		_, err = reader.GetWorkflowExecutionHistory(ctx,
			&workflowservice.GetWorkflowExecutionHistoryRequest{
				Namespace:    namespace,
				Execution:    &commonpb.WorkflowExecution{WorkflowId: run.WorkflowID, RunId: run.RunID},
				WaitNewEvent: false, HistoryEventFilterType: enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT,
				SkipArchival: false,
			})
		if _, absent := errors.AsType[*serviceerror.NotFound](err); !absent {
			if err != nil {
				return fmt.Errorf("negative-probe legacy workflow history: %w", err)
			}
			return fmt.Errorf("legacy workflow history remains readable")
		}
	}
	return nil
}

func ReadStableLegacyWorkflowInventory(
	ctx context.Context,
	reader LegacyWorkflowReader,
	namespace string,
	archived bool,
	replay WorkflowHistoryReplayer,
) (LegacyWorkflowInventory, error) {
	first, err := ReadLegacyWorkflowInventory(ctx, reader, namespace, archived, replay)
	if err != nil {
		return LegacyWorkflowInventory{}, err
	}
	second, err := ReadLegacyWorkflowInventory(ctx, reader, namespace, archived, replay)
	if err != nil {
		return LegacyWorkflowInventory{}, err
	}
	if first.Count != second.Count || first.Digest != second.Digest {
		return LegacyWorkflowInventory{}, fmt.Errorf("legacy workflow inventory drifted between scans")
	}
	return second, nil
}

// RequireWorkflowVisible calibrates standard visibility against an execution
// whose exact ID/run/type are already bound by independent History evidence.
// It deliberately uses the same unfiltered, complete scan as legacy discovery.
func RequireWorkflowVisible(
	ctx context.Context,
	reader LegacyWorkflowReader,
	namespace string,
	workflowID string,
	runID string,
	workflowType string,
) error {
	if !boundedCanonicalTemporalText(workflowID, maxTemporalAuthorityTextBytes) ||
		!canonicalUUID(runID) ||
		!boundedCanonicalTemporalText(workflowType, maxTemporalAuthorityTextBytes) {
		return fmt.Errorf("visibility calibration execution is invalid")
	}
	all, err := listWorkflowExecutionsUnfiltered(ctx, reader, namespace, false)
	if err != nil {
		return err
	}
	matches := 0
	for _, info := range all {
		if info.GetExecution().GetWorkflowId() == workflowID &&
			info.GetExecution().GetRunId() == runID && info.GetType().GetName() == workflowType {
			matches++
		}
	}
	if matches != 1 {
		return fmt.Errorf("visibility calibration execution count is %d", matches)
	}
	return nil
}

// ReadLegacyWorkflowInventory reads one complete standard-visibility or
// archival inventory. It does not infer physical deletion from an empty list;
// callers compare two independent scans and, for disabled archival, directly
// negative-probe every run retained by the baseline.
func ReadLegacyWorkflowInventory(
	ctx context.Context,
	reader LegacyWorkflowReader,
	namespace string,
	archived bool,
	replay WorkflowHistoryReplayer,
) (LegacyWorkflowInventory, error) {
	if reader == nil || replay == nil ||
		!boundedCanonicalTemporalText(namespace, maxTemporalAuthorityTextBytes) {
		return LegacyWorkflowInventory{}, fmt.Errorf("legacy workflow inventory request is invalid")
	}
	listed, err := listLegacyWorkflowExecutions(ctx, reader, namespace, archived)
	if err != nil {
		return LegacyWorkflowInventory{}, err
	}
	runs := make([]legacyWorkflowRunV1, 0, len(listed))
	publicRuns := make([]LegacyWorkflowRun, 0, len(listed))
	for _, listedInfo := range listed {
		canonical, err := canonicalLegacyWorkflowInfo(listedInfo)
		if err != nil {
			return LegacyWorkflowInventory{}, err
		}
		execution := listedInfo.GetExecution()
		description, err := reader.DescribeWorkflowExecution(ctx,
			&workflowservice.DescribeWorkflowExecutionRequest{
				Namespace: namespace,
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: execution.GetWorkflowId(), RunId: execution.GetRunId(),
				},
			})
		if err != nil {
			return LegacyWorkflowInventory{}, fmt.Errorf("describe legacy workflow: %w", err)
		}
		if description == nil || len(description.GetPendingActivities()) != 0 ||
			len(description.GetPendingChildren()) != 0 || description.GetPendingWorkflowTask() != nil ||
			len(description.GetCallbacks()) != 0 || len(description.GetPendingNexusOperations()) != 0 {
			return LegacyWorkflowInventory{}, fmt.Errorf("legacy workflow description is incomplete or pending")
		}
		described, err := canonicalLegacyWorkflowInfo(description.GetWorkflowExecutionInfo())
		if err != nil || described != canonical {
			return LegacyWorkflowInventory{}, fmt.Errorf("legacy workflow list and description differ")
		}
		history, historyDigest, eventCount, err := readLegacyWorkflowHistory(
			ctx, reader, namespace, execution.GetWorkflowId(), execution.GetRunId(),
			listedInfo.GetTaskQueue(), listedInfo.GetStatus(), archived)
		if err != nil {
			return LegacyWorkflowInventory{}, err
		}
		if err := replay(history); err != nil {
			return LegacyWorkflowInventory{}, fmt.Errorf("replay retained legacy workflow: %w", err)
		}
		run := legacyWorkflowRunV1{
			SchemaVersion: "vane.agent-first-legacy-workflow-run/v1",
			WorkflowID:    execution.GetWorkflowId(), RunID: execution.GetRunId(),
			Status: listedInfo.GetStatus().String(), AuthorityDigest: canonical,
			HistoryDigest: historyDigest, HistoryEvents: eventCount,
		}
		runs = append(runs, run)
		publicRuns = append(publicRuns, LegacyWorkflowRun{
			WorkflowID: run.WorkflowID, RunID: run.RunID, Status: run.Status,
			AuthorityDigest: run.AuthorityDigest, HistoryDigest: run.HistoryDigest,
			HistoryEvents: run.HistoryEvents,
		})
	}
	sort.Slice(runs, func(i, j int) bool {
		if runs[i].WorkflowID == runs[j].WorkflowID {
			return runs[i].RunID < runs[j].RunID
		}
		return runs[i].WorkflowID < runs[j].WorkflowID
	})
	sort.Slice(publicRuns, func(i, j int) bool {
		if publicRuns[i].WorkflowID == publicRuns[j].WorkflowID {
			return publicRuns[i].RunID < publicRuns[j].RunID
		}
		return publicRuns[i].WorkflowID < publicRuns[j].WorkflowID
	})
	digest, err := digestCanonical(struct {
		Archived      bool                  `json:"archived"`
		Runs          []legacyWorkflowRunV1 `json:"runs"`
		SchemaVersion string                `json:"schema_version"`
	}{archived, runs, "vane.agent-first-legacy-workflow-inventory/v1"})
	if err != nil {
		return LegacyWorkflowInventory{}, err
	}
	return LegacyWorkflowInventory{
		Archived: archived, Count: len(runs), Digest: digest, Runs: publicRuns,
	}, nil
}

func listLegacyWorkflowExecutions(
	ctx context.Context,
	reader LegacyWorkflowReader,
	namespace string,
	archived bool,
) ([]*workflowpb.WorkflowExecutionInfo, error) {
	all, err := listWorkflowExecutionsUnfiltered(ctx, reader, namespace, archived)
	if err != nil {
		return nil, err
	}
	result := make([]*workflowpb.WorkflowExecutionInfo, 0)
	for _, info := range all {
		if info.GetType().GetName() == legacyPushWorkflowType {
			result = append(result, info)
		}
	}
	return result, nil
}

func listWorkflowExecutionsUnfiltered(
	ctx context.Context,
	reader LegacyWorkflowReader,
	namespace string,
	archived bool,
) ([]*workflowpb.WorkflowExecutionInfo, error) {
	if reader == nil ||
		!boundedCanonicalTemporalText(namespace, maxTemporalAuthorityTextBytes) {
		return nil, fmt.Errorf("workflow visibility request is invalid")
	}
	var result []*workflowpb.WorkflowExecutionInfo
	var token []byte
	seenTokens := map[string]struct{}{}
	seenRuns := map[string]struct{}{}
	observed := 0
	for page := 0; ; page++ {
		if page >= maxLegacyVisibilityPages {
			return nil, fmt.Errorf("legacy workflow visibility exceeds page limit")
		}
		var executions []*workflowpb.WorkflowExecutionInfo
		var next []byte
		if archived {
			response, err := reader.ListArchivedWorkflowExecutions(ctx,
				&workflowservice.ListArchivedWorkflowExecutionsRequest{
					Namespace: namespace, PageSize: legacyVisibilityPageSize,
					NextPageToken: token,
				})
			if err != nil {
				return nil, fmt.Errorf("list archived legacy workflows: %w", err)
			}
			if response == nil {
				return nil, fmt.Errorf("archived legacy workflow page is absent")
			}
			executions, next = response.GetExecutions(), response.GetNextPageToken()
		} else {
			response, err := reader.ListWorkflowExecutions(ctx,
				&workflowservice.ListWorkflowExecutionsRequest{
					Namespace: namespace, PageSize: legacyVisibilityPageSize,
					NextPageToken: token,
				})
			if err != nil {
				return nil, fmt.Errorf("list standard legacy workflows: %w", err)
			}
			if response == nil {
				return nil, fmt.Errorf("standard legacy workflow page is absent")
			}
			executions, next = response.GetExecutions(), response.GetNextPageToken()
		}
		for _, info := range executions {
			observed++
			if observed > maxLegacyWorkflowRuns {
				return nil, fmt.Errorf("Temporal workflow visibility exceeds item limit")
			}
			if info == nil || info.GetExecution() == nil {
				return nil, fmt.Errorf("legacy workflow visibility item is absent")
			}
			key := info.GetExecution().GetWorkflowId() + "\x00" + info.GetExecution().GetRunId()
			if _, duplicate := seenRuns[key]; duplicate {
				return nil, fmt.Errorf("legacy workflow visibility item is duplicated")
			}
			seenRuns[key] = struct{}{}
			result = append(result, info)
		}
		if len(next) == 0 {
			break
		}
		key := string(next)
		if bytes.Equal(next, token) {
			return nil, fmt.Errorf("legacy workflow visibility token cycles")
		}
		if _, duplicate := seenTokens[key]; duplicate {
			return nil, fmt.Errorf("legacy workflow visibility token cycles")
		}
		seenTokens[key] = struct{}{}
		token = bytes.Clone(next)
	}
	return result, nil
}

func canonicalLegacyWorkflowInfo(info *workflowpb.WorkflowExecutionInfo) (string, error) {
	if info == nil || info.GetExecution() == nil ||
		!boundedCanonicalTemporalText(info.GetExecution().GetWorkflowId(), maxTemporalAuthorityTextBytes) ||
		!canonicalUUID(info.GetExecution().GetRunId()) ||
		info.GetType().GetName() != legacyPushWorkflowType ||
		!boundedCanonicalTemporalText(info.GetTaskQueue(), maxTemporalAuthorityTextBytes) ||
		info.GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_UNSPECIFIED ||
		info.GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING ||
		info.GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW ||
		info.GetStartTime() == nil || info.GetStartTime().CheckValid() != nil ||
		info.GetExecutionTime() == nil || info.GetExecutionTime().CheckValid() != nil ||
		info.GetCloseTime() == nil || info.GetCloseTime().CheckValid() != nil ||
		info.GetHistoryLength() <= 0 || info.GetHistorySizeBytes() < 0 ||
		info.GetStateTransitionCount() < 0 || info.GetFirstRunId() != info.GetExecution().GetRunId() ||
		info.GetParentNamespaceId() != "" || info.GetParentExecution() != nil ||
		(info.GetRootExecution() != nil &&
			(info.GetRootExecution().GetWorkflowId() != info.GetExecution().GetWorkflowId() ||
				info.GetRootExecution().GetRunId() != info.GetExecution().GetRunId())) {
		return "", fmt.Errorf("legacy workflow visibility authority is invalid")
	}
	clone := proto.Clone(info).(*workflowpb.WorkflowExecutionInfo)
	clone.Memo = nil
	clone.SearchAttributes = nil
	raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(clone)
	if err != nil {
		return "", fmt.Errorf("marshal legacy workflow authority: %w", err)
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

func readLegacyWorkflowHistory(
	ctx context.Context,
	reader LegacyWorkflowReader,
	namespace string,
	workflowID string,
	runID string,
	taskQueue string,
	status enumspb.WorkflowExecutionStatus,
	archived bool,
) (*historypb.History, string, int, error) {
	var events []*historypb.HistoryEvent
	var token []byte
	seenTokens := map[string]struct{}{}
	totalBytes := 0
	for page := 0; ; page++ {
		if page >= maxLegacyHistoryPages {
			return nil, "", 0, fmt.Errorf("legacy workflow history exceeds page limit")
		}
		response, err := reader.GetWorkflowExecutionHistory(ctx,
			&workflowservice.GetWorkflowExecutionHistoryRequest{
				Namespace:     namespace,
				Execution:     &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: runID},
				NextPageToken: token, WaitNewEvent: false,
				HistoryEventFilterType: enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT,
				SkipArchival:           !archived,
			})
		if err != nil {
			return nil, "", 0, fmt.Errorf("read legacy workflow history: %w", err)
		}
		if response == nil || response.GetHistory() == nil || response.GetArchived() != archived {
			return nil, "", 0, fmt.Errorf("legacy workflow history authority differs")
		}
		totalBytes += proto.Size(response.GetHistory())
		if totalBytes > maxLegacyHistoryBytes {
			return nil, "", 0, fmt.Errorf("legacy workflow history exceeds byte limit")
		}
		events = append(events, response.GetHistory().GetEvents()...)
		if len(events) > maxLegacyHistoryEvents {
			return nil, "", 0, fmt.Errorf("legacy workflow history exceeds event limit")
		}
		next := response.GetNextPageToken()
		if len(next) == 0 {
			break
		}
		key := string(next)
		if bytes.Equal(next, token) {
			return nil, "", 0, fmt.Errorf("legacy workflow history token cycles")
		}
		if _, duplicate := seenTokens[key]; duplicate {
			return nil, "", 0, fmt.Errorf("legacy workflow history token cycles")
		}
		seenTokens[key] = struct{}{}
		token = bytes.Clone(next)
	}
	if len(events) == 0 {
		return nil, "", 0, fmt.Errorf("legacy workflow history is empty")
	}
	for index, event := range events {
		if event == nil || event.GetEventId() != int64(index+1) ||
			event.GetEventType() == enumspb.EVENT_TYPE_UNSPECIFIED ||
			event.GetEventTime() == nil || event.GetEventTime().CheckValid() != nil {
			return nil, "", 0, fmt.Errorf("legacy workflow history event %d is invalid", index+1)
		}
	}
	started := events[0].GetWorkflowExecutionStartedEventAttributes()
	if started == nil || started.GetWorkflowType().GetName() != legacyPushWorkflowType ||
		started.GetTaskQueue().GetName() != taskQueue ||
		!legacyWorkflowClosureMatches(events[len(events)-1].GetEventType(), status) {
		return nil, "", 0, fmt.Errorf("legacy workflow history start authority differs")
	}
	history := &historypb.History{Events: events}
	raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(history)
	if err != nil {
		return nil, "", 0, fmt.Errorf("marshal legacy workflow history: %w", err)
	}
	sum := sha256.Sum256(raw)
	return history, hex.EncodeToString(sum[:]), len(events), nil
}

func legacyWorkflowClosureMatches(
	eventType enumspb.EventType,
	status enumspb.WorkflowExecutionStatus,
) bool {
	want := map[enumspb.WorkflowExecutionStatus]enumspb.EventType{
		enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED:        enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_FAILED:           enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED,
		enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED:         enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED,
		enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED:       enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW,
		enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT:        enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT,
	}
	return want[status] == eventType
}
