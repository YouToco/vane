package agentfirstaudit

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/google/uuid"
	enumspb "go.temporal.io/api/enums/v1"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
)

const (
	emptySHA256                   = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	maxTemporalAuthorityTextBytes = 4 << 10
	maxTemporalArchiveURIBytes    = 8 << 10
)

type NamespaceReader interface {
	GetClusterInfo(context.Context, *workflowservice.GetClusterInfoRequest,
		...grpc.CallOption) (*workflowservice.GetClusterInfoResponse, error)
	DescribeNamespace(context.Context, *workflowservice.DescribeNamespaceRequest,
		...grpc.CallOption) (*workflowservice.DescribeNamespaceResponse, error)
}

type TemporalAuthority struct {
	ClusterID                  string
	ClusterName                string
	Namespace                  string
	NamespaceID                string
	RetentionSeconds           int64
	HistoryArchivalState       string
	HistoryArchiveURIDigest    string
	VisibilityArchivalState    string
	VisibilityArchiveURIDigest string
	ClusterDigest              string
	NamespacePolicyDigest      string
}

type clusterAuthorityV1 struct {
	ClusterID                string `json:"cluster_id"`
	ClusterName              string `json:"cluster_name"`
	FailoverVersionIncrement int64  `json:"failover_version_increment"`
	HistoryShardCount        int32  `json:"history_shard_count"`
	InitialFailoverVersion   int64  `json:"initial_failover_version"`
	PersistenceStore         string `json:"persistence_store"`
	SchemaVersion            string `json:"schema_version"`
	ServerVersion            string `json:"server_version"`
	VisibilityStore          string `json:"visibility_store"`
}

type namespaceAuthorityV1 struct {
	ActiveCluster              string   `json:"active_cluster"`
	Clusters                   []string `json:"clusters"`
	FailoverVersion            int64    `json:"failover_version"`
	HistoryArchiveURIDigest    string   `json:"history_archive_uri_digest"`
	HistoryArchivalState       string   `json:"history_archival_state"`
	IsGlobal                   bool     `json:"is_global"`
	Namespace                  string   `json:"namespace"`
	NamespaceID                string   `json:"namespace_id"`
	ReplicationState           string   `json:"replication_state"`
	RetentionSeconds           int64    `json:"retention_seconds"`
	SchemaVersion              string   `json:"schema_version"`
	SupportsSchedules          bool     `json:"supports_schedules"`
	VisibilityArchiveURIDigest string   `json:"visibility_archive_uri_digest"`
	VisibilityArchivalState    string   `json:"visibility_archival_state"`
}

func ReadTemporalAuthority(
	ctx context.Context, reader NamespaceReader, namespace string,
) (TemporalAuthority, error) {
	if reader == nil || !boundedCanonicalTemporalText(namespace, maxTemporalAuthorityTextBytes) {
		return TemporalAuthority{}, fmt.Errorf("Temporal authority request is invalid")
	}
	cluster, err := reader.GetClusterInfo(ctx, &workflowservice.GetClusterInfoRequest{})
	if err != nil {
		return TemporalAuthority{}, fmt.Errorf("read Temporal cluster authority: %w", err)
	}
	description, err := reader.DescribeNamespace(ctx,
		&workflowservice.DescribeNamespaceRequest{Namespace: namespace})
	if err != nil {
		return TemporalAuthority{}, fmt.Errorf("read Temporal namespace authority: %w", err)
	}
	if cluster == nil ||
		!boundedCanonicalTemporalText(cluster.GetClusterId(), maxTemporalAuthorityTextBytes) ||
		!boundedCanonicalTemporalText(cluster.GetClusterName(), maxTemporalAuthorityTextBytes) ||
		!boundedCanonicalTemporalText(cluster.GetServerVersion(), maxTemporalAuthorityTextBytes) ||
		cluster.GetHistoryShardCount() <= 0 ||
		!boundedCanonicalTemporalText(cluster.GetPersistenceStore(), maxTemporalAuthorityTextBytes) ||
		!boundedCanonicalTemporalText(cluster.GetVisibilityStore(), maxTemporalAuthorityTextBytes) {
		return TemporalAuthority{}, fmt.Errorf("Temporal cluster authority is incomplete")
	}
	info, config, replication := description.GetNamespaceInfo(), description.GetConfig(),
		description.GetReplicationConfig()
	if info == nil || config == nil || replication == nil || info.GetName() != namespace ||
		!canonicalUUID(info.GetId()) || info.GetState() != enumspb.NAMESPACE_STATE_REGISTERED ||
		!info.GetSupportsSchedules() ||
		!boundedCanonicalTemporalText(replication.GetActiveClusterName(), maxTemporalAuthorityTextBytes) ||
		replication.GetState() != enumspb.REPLICATION_STATE_NORMAL {
		return TemporalAuthority{}, fmt.Errorf("Temporal namespace authority is incomplete")
	}
	retention := config.GetWorkflowExecutionRetentionTtl()
	if retention == nil || retention.CheckValid() != nil || retention.Seconds <= 0 ||
		retention.Nanos != 0 {
		return TemporalAuthority{}, fmt.Errorf("Temporal retention is not a positive whole second")
	}
	historyState, historyURI, err := archivalAuthority(
		config.GetHistoryArchivalState(), config.GetHistoryArchivalUri())
	if err != nil {
		return TemporalAuthority{}, fmt.Errorf("Temporal history archival authority: %w", err)
	}
	visibilityState, visibilityURI, err := archivalAuthority(
		config.GetVisibilityArchivalState(), config.GetVisibilityArchivalUri())
	if err != nil {
		return TemporalAuthority{}, fmt.Errorf("Temporal visibility archival authority: %w", err)
	}
	if historyState != visibilityState {
		return TemporalAuthority{}, fmt.Errorf("Temporal archival modes are mixed")
	}
	clusters := make([]string, 0, len(replication.GetClusters()))
	seen := make(map[string]struct{}, len(replication.GetClusters()))
	for _, current := range replication.GetClusters() {
		name := current.GetClusterName()
		if !boundedCanonicalTemporalText(name, maxTemporalAuthorityTextBytes) {
			return TemporalAuthority{}, fmt.Errorf("Temporal replication cluster is empty")
		}
		if _, duplicate := seen[name]; duplicate {
			return TemporalAuthority{}, fmt.Errorf("Temporal replication cluster is duplicated")
		}
		seen[name] = struct{}{}
		clusters = append(clusters, name)
	}
	if _, ok := seen[replication.GetActiveClusterName()]; !ok {
		return TemporalAuthority{}, fmt.Errorf("Temporal active cluster is not replicated")
	}
	sort.Strings(clusters)
	clusterCanonical := clusterAuthorityV1{
		SchemaVersion: "vane.agent-first-temporal-cluster/v1",
		ClusterID:     cluster.GetClusterId(), ClusterName: cluster.GetClusterName(),
		ServerVersion:     cluster.GetServerVersion(),
		HistoryShardCount: cluster.GetHistoryShardCount(),
		PersistenceStore:  cluster.GetPersistenceStore(), VisibilityStore: cluster.GetVisibilityStore(),
		InitialFailoverVersion:   cluster.GetInitialFailoverVersion(),
		FailoverVersionIncrement: cluster.GetFailoverVersionIncrement(),
	}
	namespaceCanonical := namespaceAuthorityV1{
		SchemaVersion: "vane.agent-first-temporal-namespace/v1",
		Namespace:     namespace, NamespaceID: info.GetId(), SupportsSchedules: true,
		RetentionSeconds: retention.Seconds, HistoryArchivalState: historyState,
		HistoryArchiveURIDigest: historyURI, VisibilityArchivalState: visibilityState,
		VisibilityArchiveURIDigest: visibilityURI, IsGlobal: description.GetIsGlobalNamespace(),
		FailoverVersion: description.GetFailoverVersion(), ActiveCluster: replication.GetActiveClusterName(),
		Clusters: clusters, ReplicationState: replication.GetState().String(),
	}
	clusterDigest, err := digestCanonical(clusterCanonical)
	if err != nil {
		return TemporalAuthority{}, err
	}
	namespaceDigest, err := digestCanonical(namespaceCanonical)
	if err != nil {
		return TemporalAuthority{}, err
	}
	return TemporalAuthority{
		ClusterID: clusterCanonical.ClusterID, ClusterName: clusterCanonical.ClusterName,
		Namespace: namespace, NamespaceID: namespaceCanonical.NamespaceID,
		RetentionSeconds: retention.Seconds, HistoryArchivalState: historyState,
		HistoryArchiveURIDigest: historyURI, VisibilityArchivalState: visibilityState,
		VisibilityArchiveURIDigest: visibilityURI, ClusterDigest: clusterDigest,
		NamespacePolicyDigest: namespaceDigest,
	}, nil
}

func archivalAuthority(state enumspb.ArchivalState, uri string) (string, string, error) {
	switch state {
	case enumspb.ARCHIVAL_STATE_DISABLED:
		if uri != "" {
			return "", "", fmt.Errorf("disabled archival retains a URI")
		}
		return "disabled", emptySHA256, nil
	case enumspb.ARCHIVAL_STATE_ENABLED:
		if !boundedCanonicalTemporalText(uri, maxTemporalArchiveURIBytes) {
			return "", "", fmt.Errorf("enabled archival URI is absent")
		}
		sum := sha256.Sum256([]byte(uri))
		return "enabled", hex.EncodeToString(sum[:]), nil
	default:
		return "", "", fmt.Errorf("archival state is unspecified")
	}
}

func boundedCanonicalTemporalText(value string, maxBytes int) bool {
	return value != "" && len(value) <= maxBytes && strings.TrimSpace(value) == value
}

func canonicalUUID(value string) bool {
	parsed, err := uuid.Parse(value)
	return err == nil && parsed.String() == value
}

func validLowerHex(value string, byteCount int) bool {
	if len(value) != byteCount*2 {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == byteCount && hex.EncodeToString(decoded) == value
}

func digestCanonical(value any) (string, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("marshal Temporal authority: %w", err)
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}
