package agentfirstaudit

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"

	schedulepb "go.temporal.io/api/schedule/v1"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	vaneworkflow "github.com/YouToco/vane/workflow"
)

const (
	scheduleInventoryPageSize int32 = 1000
	maxScheduleInventoryPages       = 4096
	maxScheduleInventoryItems       = 100000
)

type ScheduleInventoryReader interface {
	ListSchedules(context.Context, *workflowservice.ListSchedulesRequest,
		...grpc.CallOption) (*workflowservice.ListSchedulesResponse, error)
	DescribeSchedule(context.Context, *workflowservice.DescribeScheduleRequest,
		...grpc.CallOption) (*workflowservice.DescribeScheduleResponse, error)
}

type ScheduleInventory struct {
	Count  int
	Digest string
	Items  []ScheduleAuthority
}

type ScheduleAuthority struct {
	ScheduleID        string
	WorkflowType      string
	ActionDigest      string
	DescriptionDigest string
	Paused            bool
}

type ExpectedScheduleAuthority struct {
	ScheduleID         string
	TargetActionDigest string
	Paused             bool
}

type scheduleAuthorityV1 struct {
	ActionDigest      string `json:"action_digest"`
	DescriptionDigest string `json:"description_digest"`
	Paused            bool   `json:"paused"`
	ScheduleID        string `json:"schedule_id"`
	SchemaVersion     string `json:"schema_version"`
	WorkflowType      string `json:"workflow_type"`
}

func ValidateScheduleInventoryParity(
	temporal ScheduleInventory,
	expected []ExpectedScheduleAuthority,
) error {
	if temporal.Count != len(temporal.Items) || temporal.Count != len(expected) ||
		len(temporal.Digest) != sha256.Size*2 {
		return fmt.Errorf("Temporal and database schedule counts differ")
	}
	byID := make(map[string]ExpectedScheduleAuthority, len(expected))
	for _, item := range expected {
		if !boundedCanonicalTemporalText(item.ScheduleID, maxTemporalAuthorityTextBytes) ||
			!validLowerHex(item.TargetActionDigest, sha256.Size) {
			return fmt.Errorf("database schedule authority is invalid")
		}
		if _, duplicate := byID[item.ScheduleID]; duplicate {
			return fmt.Errorf("database schedule authority is duplicated")
		}
		byID[item.ScheduleID] = item
	}
	seen := make(map[string]struct{}, len(temporal.Items))
	for _, item := range temporal.Items {
		if _, duplicate := seen[item.ScheduleID]; duplicate {
			return fmt.Errorf("Temporal schedule authority is duplicated")
		}
		seen[item.ScheduleID] = struct{}{}
		want, found := byID[item.ScheduleID]
		if !found || item.WorkflowType != vaneworkflow.ResearchScheduledWorkflowV3Name ||
			item.ActionDigest != want.TargetActionDigest || item.Paused != want.Paused {
			return fmt.Errorf("Temporal schedule %q differs from database authority", item.ScheduleID)
		}
	}
	return nil
}

// ReadStableScheduleInventory performs two complete namespace scans. Every
// listed schedule is described from the authority endpoint; any legacy or
// unknown Action blocks the retention Gate even when paused or exhausted.
func ReadStableScheduleInventory(
	ctx context.Context,
	reader ScheduleInventoryReader,
	namespace string,
) (ScheduleInventory, error) {
	first, err := readScheduleInventory(ctx, reader, namespace)
	if err != nil {
		return ScheduleInventory{}, err
	}
	second, err := readScheduleInventory(ctx, reader, namespace)
	if err != nil {
		return ScheduleInventory{}, err
	}
	if first.Count != second.Count || first.Digest != second.Digest {
		return ScheduleInventory{}, fmt.Errorf("Temporal schedule inventory drifted between scans")
	}
	return second, nil
}

func readScheduleInventory(
	ctx context.Context,
	reader ScheduleInventoryReader,
	namespace string,
) (ScheduleInventory, error) {
	if reader == nil || !boundedCanonicalTemporalText(namespace, maxTemporalAuthorityTextBytes) {
		return ScheduleInventory{}, fmt.Errorf("schedule inventory request is invalid")
	}
	var entries []*schedulepb.ScheduleListEntry
	var token []byte
	seenTokens := map[string]struct{}{}
	seenIDs := map[string]struct{}{}
	for page := 0; ; page++ {
		if page >= maxScheduleInventoryPages {
			return ScheduleInventory{}, fmt.Errorf("Temporal schedules exceed page limit")
		}
		response, err := reader.ListSchedules(ctx, &workflowservice.ListSchedulesRequest{
			Namespace: namespace, MaximumPageSize: scheduleInventoryPageSize,
			NextPageToken: token,
		})
		if err != nil {
			return ScheduleInventory{}, fmt.Errorf("list Temporal schedules: %w", err)
		}
		if response == nil {
			return ScheduleInventory{}, fmt.Errorf("Temporal schedule page is absent")
		}
		for _, entry := range response.GetSchedules() {
			if entry == nil ||
				!boundedCanonicalTemporalText(entry.GetScheduleId(), maxTemporalAuthorityTextBytes) ||
				entry.GetInfo() == nil || entry.GetInfo().GetWorkflowType().GetName() !=
				vaneworkflow.ResearchScheduledWorkflowV3Name {
				return ScheduleInventory{}, fmt.Errorf("Temporal schedule list authority is invalid")
			}
			if _, duplicate := seenIDs[entry.GetScheduleId()]; duplicate {
				return ScheduleInventory{}, fmt.Errorf("Temporal schedule is duplicated")
			}
			seenIDs[entry.GetScheduleId()] = struct{}{}
			entries = append(entries, entry)
			if len(entries) > maxScheduleInventoryItems {
				return ScheduleInventory{}, fmt.Errorf("Temporal schedules exceed item limit")
			}
		}
		next := response.GetNextPageToken()
		if len(next) == 0 {
			break
		}
		key := string(next)
		if bytes.Equal(next, token) {
			return ScheduleInventory{}, fmt.Errorf("Temporal schedule token cycles")
		}
		if _, duplicate := seenTokens[key]; duplicate {
			return ScheduleInventory{}, fmt.Errorf("Temporal schedule token cycles")
		}
		seenTokens[key] = struct{}{}
		token = bytes.Clone(next)
	}
	authorities := make([]scheduleAuthorityV1, 0, len(entries))
	publicItems := make([]ScheduleAuthority, 0, len(entries))
	for _, entry := range entries {
		description, err := reader.DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
			Namespace: namespace, ScheduleId: entry.GetScheduleId(),
		})
		if err != nil {
			return ScheduleInventory{}, fmt.Errorf("describe Temporal schedule: %w", err)
		}
		if description == nil || description.GetSchedule() == nil ||
			description.GetInfo() == nil || len(description.GetConflictToken()) == 0 {
			return ScheduleInventory{}, fmt.Errorf("Temporal schedule description is incomplete")
		}
		start := description.GetSchedule().GetAction().GetStartWorkflow()
		if start == nil || start.GetWorkflowType().GetName() !=
			vaneworkflow.ResearchScheduledWorkflowV3Name ||
			entry.GetInfo().GetWorkflowType().GetName() != start.GetWorkflowType().GetName() {
			return ScheduleInventory{}, fmt.Errorf("Temporal schedule Action is legacy or unknown")
		}
		scheduleRaw, err := proto.MarshalOptions{Deterministic: true}.Marshal(description.GetSchedule())
		if err != nil {
			return ScheduleInventory{}, fmt.Errorf("marshal Temporal schedule authority: %w", err)
		}
		if len(scheduleRaw) == 0 {
			return ScheduleInventory{}, fmt.Errorf("Temporal schedule authority bytes are empty")
		}
		scheduleSum := sha256.Sum256(scheduleRaw)
		conflictSum := sha256.Sum256(description.GetConflictToken())
		descriptionDigest, err := digestCanonical(struct {
			ConflictTokenDigest string `json:"conflict_token_digest"`
			ScheduleDigest      string `json:"schedule_digest"`
			SchemaVersion       string `json:"schema_version"`
		}{
			ConflictTokenDigest: hex.EncodeToString(conflictSum[:]),
			ScheduleDigest:      hex.EncodeToString(scheduleSum[:]),
			SchemaVersion:       "vane.agent-first-schedule-description/v1",
		})
		if err != nil {
			return ScheduleInventory{}, err
		}
		actionRaw, err := proto.MarshalOptions{Deterministic: true}.Marshal(
			description.GetSchedule().GetAction())
		if err != nil {
			return ScheduleInventory{}, fmt.Errorf("marshal Temporal schedule Action: %w", err)
		}
		if len(actionRaw) == 0 {
			return ScheduleInventory{}, fmt.Errorf("Temporal schedule Action bytes are empty")
		}
		actionSum := sha256.Sum256(actionRaw)
		actionDigest := hex.EncodeToString(actionSum[:])
		paused := description.GetSchedule().GetState().GetPaused()
		authorities = append(authorities, scheduleAuthorityV1{
			SchemaVersion: "vane.agent-first-schedule-authority/v1",
			ScheduleID:    entry.GetScheduleId(), WorkflowType: start.GetWorkflowType().GetName(),
			ActionDigest: actionDigest, DescriptionDigest: descriptionDigest, Paused: paused,
		})
		publicItems = append(publicItems, ScheduleAuthority{
			ScheduleID: entry.GetScheduleId(), WorkflowType: start.GetWorkflowType().GetName(),
			ActionDigest: actionDigest, DescriptionDigest: descriptionDigest, Paused: paused,
		})
	}
	sort.Slice(authorities, func(i, j int) bool {
		return authorities[i].ScheduleID < authorities[j].ScheduleID
	})
	sort.Slice(publicItems, func(i, j int) bool {
		return publicItems[i].ScheduleID < publicItems[j].ScheduleID
	})
	digest, err := digestScheduleInventory(publicItems)
	if err != nil {
		return ScheduleInventory{}, err
	}
	return ScheduleInventory{
		Count: len(authorities), Digest: digest, Items: publicItems,
	}, nil
}

func digestScheduleInventory(items []ScheduleAuthority) (string, error) {
	authorities := make([]scheduleAuthorityV1, 0, len(items))
	for _, item := range items {
		authorities = append(authorities, scheduleAuthorityV1{
			SchemaVersion: "vane.agent-first-schedule-authority/v1",
			ScheduleID:    item.ScheduleID, WorkflowType: item.WorkflowType,
			ActionDigest: item.ActionDigest, DescriptionDigest: item.DescriptionDigest,
			Paused: item.Paused,
		})
	}
	sort.Slice(authorities, func(i, j int) bool {
		return authorities[i].ScheduleID < authorities[j].ScheduleID
	})
	return digestCanonical(struct {
		Schedules     []scheduleAuthorityV1 `json:"schedules"`
		SchemaVersion string                `json:"schema_version"`
	}{authorities, "vane.agent-first-schedule-inventory/v1"})
}
