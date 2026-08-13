package agentfirstaudit

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	replicationpb "go.temporal.io/api/replication/v1"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestReadTemporalAuthorityBindsClusterNamespaceAndArchivePolicy(t *testing.T) {
	reader := validNamespaceReader()
	authority, err := ReadTemporalAuthority(t.Context(), reader, "default")
	if err != nil {
		t.Fatal(err)
	}
	if authority.ClusterID != "cluster-id" || authority.ClusterName != "active" ||
		authority.NamespaceID != "123e4567-e89b-42d3-a456-426614174000" ||
		authority.RetentionSeconds != 86400 || authority.HistoryArchivalState != "disabled" ||
		authority.HistoryArchiveURIDigest != emptySHA256 || len(authority.ClusterDigest) != 64 ||
		len(authority.NamespacePolicyDigest) != 64 {
		t.Fatalf("authority=%+v", authority)
	}
	reordered := validNamespaceReader()
	reordered.namespace.ReplicationConfig.Clusters[0], reordered.namespace.ReplicationConfig.Clusters[1] =
		reordered.namespace.ReplicationConfig.Clusters[1], reordered.namespace.ReplicationConfig.Clusters[0]
	reorderedAuthority, err := ReadTemporalAuthority(t.Context(), reordered, "default")
	if err != nil {
		t.Fatal(err)
	}
	if reorderedAuthority.NamespacePolicyDigest != authority.NamespacePolicyDigest {
		t.Fatal("replication list order changed semantic digest")
	}
}

func TestReadTemporalAuthorityRejectsUnprovablePolicy(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*namespaceReaderFake)
	}{
		{"cluster rpc", func(f *namespaceReaderFake) { f.clusterErr = errors.New("denied") }},
		{"missing cluster id", func(f *namespaceReaderFake) { f.cluster.ClusterId = "" }},
		{"wrong namespace", func(f *namespaceReaderFake) { f.namespace.NamespaceInfo.Name = "other" }},
		{"noncanonical namespace id", func(f *namespaceReaderFake) {
			f.namespace.NamespaceInfo.Id = "123E4567-E89B-42D3-A456-426614174000"
		}},
		{"namespace deleted", func(f *namespaceReaderFake) {
			f.namespace.NamespaceInfo.State = enumspb.NAMESPACE_STATE_DELETED
		}},
		{"schedules unsupported", func(f *namespaceReaderFake) {
			f.namespace.NamespaceInfo.SupportsSchedules = false
		}},
		{"fractional retention", func(f *namespaceReaderFake) {
			f.namespace.Config.WorkflowExecutionRetentionTtl.Nanos = 1
		}},
		{"mixed archival", func(f *namespaceReaderFake) {
			f.namespace.Config.HistoryArchivalState = enumspb.ARCHIVAL_STATE_ENABLED
			f.namespace.Config.HistoryArchivalUri = "s3://history"
		}},
		{"disabled uri", func(f *namespaceReaderFake) {
			f.namespace.Config.HistoryArchivalUri = "s3://stale"
		}},
		{"unspecified archive", func(f *namespaceReaderFake) {
			f.namespace.Config.HistoryArchivalState = enumspb.ARCHIVAL_STATE_UNSPECIFIED
		}},
		{"duplicate cluster", func(f *namespaceReaderFake) {
			f.namespace.ReplicationConfig.Clusters[1].ClusterName = "active"
		}},
		{"active cluster absent", func(f *namespaceReaderFake) {
			f.namespace.ReplicationConfig.ActiveClusterName = "absent"
		}},
		{"passive cluster endpoint", func(f *namespaceReaderFake) {
			f.cluster.ClusterName = "standby"
		}},
		{"replication unspecified", func(f *namespaceReaderFake) {
			f.namespace.ReplicationConfig.State = enumspb.REPLICATION_STATE_UNSPECIFIED
		}},
		{"oversized archive uri", func(f *namespaceReaderFake) {
			f.namespace.Config.HistoryArchivalState = enumspb.ARCHIVAL_STATE_ENABLED
			f.namespace.Config.VisibilityArchivalState = enumspb.ARCHIVAL_STATE_ENABLED
			f.namespace.Config.HistoryArchivalUri = strings.Repeat("x", maxTemporalArchiveURIBytes+1)
			f.namespace.Config.VisibilityArchivalUri = "s3://visibility"
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := validNamespaceReader()
			tc.mutate(fixture)
			if _, err := ReadTemporalAuthority(t.Context(), fixture, "default"); err == nil {
				t.Fatal("unprovable Temporal policy accepted")
			}
		})
	}
}

func TestReadTemporalAuthorityHashesArchiveURIsWithoutExposingThem(t *testing.T) {
	fixture := validNamespaceReader()
	fixture.namespace.Config.HistoryArchivalState = enumspb.ARCHIVAL_STATE_ENABLED
	fixture.namespace.Config.VisibilityArchivalState = enumspb.ARCHIVAL_STATE_ENABLED
	fixture.namespace.Config.HistoryArchivalUri = "s3://secret-history-authority"
	fixture.namespace.Config.VisibilityArchivalUri = "s3://secret-visibility-authority"
	authority, err := ReadTemporalAuthority(t.Context(), fixture, "default")
	if err != nil {
		t.Fatal(err)
	}
	if authority.HistoryArchivalState != "enabled" ||
		authority.HistoryArchiveURIDigest == emptySHA256 ||
		authority.VisibilityArchiveURIDigest == emptySHA256 {
		t.Fatalf("authority=%+v", authority)
	}
}

type namespaceReaderFake struct {
	cluster      *workflowservice.GetClusterInfoResponse
	namespace    *workflowservice.DescribeNamespaceResponse
	clusterErr   error
	namespaceErr error
}

func validNamespaceReader() *namespaceReaderFake {
	return &namespaceReaderFake{
		cluster: &workflowservice.GetClusterInfoResponse{
			ClusterId: "cluster-id", ClusterName: "active", ServerVersion: "1.28.0",
			HistoryShardCount: 4, PersistenceStore: "postgresql", VisibilityStore: "postgresql",
			InitialFailoverVersion: 1, FailoverVersionIncrement: 10,
		},
		namespace: &workflowservice.DescribeNamespaceResponse{
			NamespaceInfo: &namespacepb.NamespaceInfo{
				Name: "default", Id: "123e4567-e89b-42d3-a456-426614174000",
				State: enumspb.NAMESPACE_STATE_REGISTERED, SupportsSchedules: true,
			},
			Config: &namespacepb.NamespaceConfig{
				WorkflowExecutionRetentionTtl: durationpb.New(24 * time.Hour),
				HistoryArchivalState:          enumspb.ARCHIVAL_STATE_DISABLED,
				VisibilityArchivalState:       enumspb.ARCHIVAL_STATE_DISABLED,
			},
			ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
				ActiveClusterName: "active", State: enumspb.REPLICATION_STATE_NORMAL,
				Clusters: []*replicationpb.ClusterReplicationConfig{
					{ClusterName: "active"}, {ClusterName: "standby"},
				},
			},
			FailoverVersion: 1,
		},
	}
}

func (f *namespaceReaderFake) GetClusterInfo(
	context.Context, *workflowservice.GetClusterInfoRequest, ...grpc.CallOption,
) (*workflowservice.GetClusterInfoResponse, error) {
	return proto.Clone(f.cluster).(*workflowservice.GetClusterInfoResponse), f.clusterErr
}

func (f *namespaceReaderFake) DescribeNamespace(
	context.Context, *workflowservice.DescribeNamespaceRequest, ...grpc.CallOption,
) (*workflowservice.DescribeNamespaceResponse, error) {
	return proto.Clone(f.namespace).(*workflowservice.DescribeNamespaceResponse), f.namespaceErr
}
