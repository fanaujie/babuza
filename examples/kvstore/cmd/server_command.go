package cmd

import (
	"fmt"
	"github.com/fanaujie/babuza/examples/kvstore/server"
	"github.com/spf13/cobra"
	"io"
	"strconv"
	"strings"
)

var (
	kvStoreConfig       server.Config
	clusterPeersAddress string
)

func NewServerCommand(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	var serverCommand = &cobra.Command{
		Use:   "server",
		Short: "run kv store server",
		Long: `Run a key-value store server powered by Babuza distributed consensus framework.

Babuza is a framework that implements the Raft consensus algorithm (based on etcd's Raft library)
with additional customizable components for easily building distributed systems. This KV store
is a complete example application that demonstrates how to use Babuza's components.

You can configure various aspects of the KV store server including:
- Raft consensus protocol settings
- Communication and transport protocols
- Storage mechanisms (WAL, snapshots, state machine)
- Session management for idempotency`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := parseAndValidateServerParams(); err != nil {
				return err
			}
			return server.NewServer(kvStoreConfig).Start()
		},
	}

	// ==== KV HTTP Service Parameters ====
	serverCommand.Flags().StringVar(&kvStoreConfig.KvServiceHttpAddress, "kv-http-address", "localhost:24200",
		"HTTP Service Address: The HTTP listen address and port for the KV Store API service.")
	serverCommand.Flags().StringVar(&kvStoreConfig.HttpCertFile, "http-cert", "",
		"HTTP TLS Certificate: Path to X.509 certificate for the HTTP API service. Enables HTTPS when set together with --http-key.")
	serverCommand.Flags().StringVar(&kvStoreConfig.HttpKeyFile, "http-key", "",
		"HTTP TLS Key: Path to X.509 private key for the HTTP API service. Enables HTTPS when set together with --http-cert.")

	// ==== Raft Consensus Parameters ====
	serverCommand.Flags().Uint64Var(&kvStoreConfig.RaftClusterId, "raft-cluster-id", 100,
		"Raft Cluster ID: Identifies the entire Raft cluster. All nodes in the same cluster must use the same cluster ID.")
	serverCommand.Flags().Uint64Var(&kvStoreConfig.RaftLocalPeerId, "raft-local-peer-id", 1,
		"Raft Node ID: Unique ID for the current node within the cluster. Each node must use a different ID.")
	serverCommand.Flags().BoolVar(&kvStoreConfig.RaftVoterOrLearner, "raft-voter", true,
		"Raft Voting Rights: Set to true for voting member, false for learner node. Learners don't vote but receive log replication.")
	serverCommand.Flags().StringVar(&kvStoreConfig.RaftLocalPeerAddress, "raft-local-peer-address", "localhost:14200",
		"Raft Node Address: Network address of the current node for Raft protocol communication. Format is IP:PORT or hostname:PORT.")
	serverCommand.Flags().BoolVar(&kvStoreConfig.JoinRaftCluster, "raft-join-cluster", false,
		"Join Existing Cluster: Set to true if this node should attempt to join an existing cluster rather than create a new one.")
	serverCommand.Flags().StringVar(&clusterPeersAddress, "raft-cluster-peers-address", "1=localhost:14200",
		"Raft Cluster Node List: Format is 'ID1=address1,ID2=address2,...'. Example: '1=192.168.1.10:14200,2=192.168.1.11:14200'")

	// ==== Raft TLS Security Configuration ====
	serverCommand.Flags().BoolVar(&kvStoreConfig.RaftEncrypt, "raft-encrypt", false,
		"Raft TLS Encryption: Enable TLS encryption for Raft communication. Certificate and key parameters must be set when true.")
	serverCommand.Flags().StringVar(&kvStoreConfig.RaftTLSCertFile, "raft-peer-cert", "",
		"Raft TLS Certificate: Path to X.509 certificate for Raft communication. Required when TLS is enabled.")
	serverCommand.Flags().StringVar(&kvStoreConfig.RaftTLSKeyFile, "raft-peer-key", "",
		"Raft TLS Key: Path to X.509 private key for Raft communication. Required when TLS is enabled.")
	serverCommand.Flags().StringVar(&kvStoreConfig.RaftTLSRootCA, "raft-peer-root-ca", "",
		"Raft TLS Root CA: Path to root CA certificate for verifying peer certificates. Recommended when TLS is enabled.")

	// ==== Storage Configuration ====
	serverCommand.Flags().StringVar(&kvStoreConfig.RaftStorageDir, "raft-storage-dir", "./raft_storage",
		"Storage Directory: Root directory path for storing WAL logs, snapshots, and state machine data.")

	// ==== Babuza Framework Component Selection ====
	serverCommand.Flags().IntVar(&kvStoreConfig.BabuzaSession, "session-type", server.NoOpSession,
		`Session Management Implementation (for idempotency):
  1: No Operation Session (NoOp) - Default, no idempotency support
  2: Expire Session (Expire) - Session management with simple expiration mechanism
  3: LRU Session (LRU) - Session management based on Least Recently Used algorithm
Use cases: Choose 2 or 3 when clients need to ensure operations are not executed multiple times`)

	serverCommand.Flags().IntVar(&kvStoreConfig.BabuzaTransportProtocol, "transport-protocol", server.TcpTransport,
		`Transport Protocol Implementation:
  1: TCP Transport (TCP) - Default, basic TCP protocol with physical network I/O, low latency
  2: TCP Memory Transport (TCP Memory) - In-memory TCP implementation for testing
  3: HTTP Transport (HTTP) - Based on HTTP protocol, works through proxies and firewalls
  4: gRPC Transport (gRPC) - Based on gRPC protocol, supports bidirectional streaming and advanced features
Use cases: Choose 1 for production, 2 for testing or single-process clusters, 3 for cross-network scenarios, 4 for advanced RPC needs`)

	serverCommand.Flags().IntVar(&kvStoreConfig.BabuzaWal, "wal-type", server.BabuzaWal,
		`Write-Ahead Log (WAL) Implementation:
  1: Babuza WAL - Default, Babuza native implementation
  2: ETCD WAL - Uses etcd's WAL implementation
  3: LSTM WAL Disk - Disk-based WAL implementation with LSTM
  4: LSTM WAL Memory - Memory-based WAL implementation with LSTM (for testing only)
Use cases: Choose 1 for general purposes, 2 for high throughput scenarios, 3 for special durability features, 4 for testing`)

	serverCommand.Flags().IntVar(&kvStoreConfig.BabuzaSnapshot, "snapshot-type", server.DurableSnapshot,
		`Snapshot Implementation:
  1: Durable Snapshot (Durable) - Default, stores snapshots on local disk
  2: Volatile Snapshot (Volatile) - Stores snapshots in memory (for testing only)
  3: MinIO Snapshot (MinIO) - Stores snapshots using MinIO object storage service
Use cases: Recommend 1 for production, use 2 for testing, choose 3 for shared storage or cloud storage needs
Note: When choosing MinIO Snapshot (3), you must provide MinIO configuration parameters.`)

	serverCommand.Flags().IntVar(&kvStoreConfig.StateMachine, "state-machine", server.StateMachineMemory,
		`State Machine Type:
  1: Memory State Machine - Default, all data stored in memory
  2: Memory State Machine with Concurrent Snapshot - Doesn't block operations while creating snapshots
  3: Disk State Machine - Persists data to disk
Use cases: Choose 1 for high performance needs, 2 for large datasets with snapshot requirements, 3 for data persistence`)

	// ==== MinIO Configuration (for MinIO Snapshot) ====
	serverCommand.Flags().StringVar(&kvStoreConfig.MinIOEndpoint, "minio-endpoint", "",
		"MinIO Endpoint: The endpoint URL for the MinIO service. Example: 'play.min.io:9000'")
	serverCommand.Flags().StringVar(&kvStoreConfig.MinIOAccessKeyID, "minio-access-key", "",
		"MinIO Access Key ID: Access key for the MinIO service")
	serverCommand.Flags().StringVar(&kvStoreConfig.MinIOSecretAccessKey, "minio-secret-key", "",
		"MinIO Secret Access Key: Secret key for the MinIO service")
	serverCommand.Flags().BoolVar(&kvStoreConfig.MinIOUseSSL, "minio-use-ssl", true,
		"MinIO Use SSL: Set to true to enable SSL/TLS for MinIO connections")
	serverCommand.Flags().StringVar(&kvStoreConfig.MinIOBucket, "minio-bucket", "raft-snapshots",
		"MinIO Bucket: Name of the bucket to store Raft snapshots")
	serverCommand.Flags().StringVar(&kvStoreConfig.MinIOPrefix, "minio-prefix", "",
		"MinIO Prefix: Optional prefix (folder path) for snapshot objects in the bucket")

	// ==== Advanced Raft Parameters ====
	serverCommand.Flags().BoolVar(&kvStoreConfig.RaftDisableForwarding, "disable-forwarding", false,
		"Disable Proposal Forwarding: When true, follower nodes will not automatically forward proposals to the leader. Default is false, enabling forwarding.")

	serverCommand.Flags().BoolVar(&kvStoreConfig.RaftWalNoSync, "wal-no-sync", false,
		"WAL Non-Sync Mode: When true, WAL writes won't fsync immediately, increasing performance but reducing reliability. Not recommended for production.")

	serverCommand.SetOut(stderr)
	return serverCommand
}

func parseAndValidateServerParams() error {
	kvStoreConfig.RaftClusterVotersAddress = make(map[uint64]string)
	for _, pe := range strings.Split(clusterPeersAddress, ",") {
		peer := strings.Split(pe, "=")
		if len(peer) != 2 {
			return fmt.Errorf("invalid raft-cluster-peers-address (peer=%s)", peer)
		}
		id, err := strconv.ParseUint(peer[0], 10, 64)
		if err != nil {
			return fmt.Errorf("failed to parse peer=%s", peer)
		}
		_, ok := kvStoreConfig.RaftClusterVotersAddress[id]
		if ok {
			return fmt.Errorf("repeated peer id(%d)", id)
		}
		kvStoreConfig.RaftClusterVotersAddress[id] = peer[1]
	}

	// Validate Session option
	if kvStoreConfig.BabuzaSession < 1 || kvStoreConfig.BabuzaSession > 3 {
		return fmt.Errorf("invalid session option: %d (must be 1-3)", kvStoreConfig.BabuzaSession)
	}

	// Validate Transport option
	if kvStoreConfig.BabuzaTransportProtocol < 1 || kvStoreConfig.BabuzaTransportProtocol > 4 {
		return fmt.Errorf("invalid transport protocol option: %d (must be 1-4)", kvStoreConfig.BabuzaTransportProtocol)
	}

	// Validate WAL option
	if kvStoreConfig.BabuzaWal < 1 || kvStoreConfig.BabuzaWal > 4 {
		return fmt.Errorf("invalid WAL option: %d (must be 1-4)", kvStoreConfig.BabuzaWal)
	}

	// Validate Snapshot option
	if kvStoreConfig.BabuzaSnapshot < 1 || kvStoreConfig.BabuzaSnapshot > 3 {
		return fmt.Errorf("invalid snapshot option: %d (must be 1-3)", kvStoreConfig.BabuzaSnapshot)
	}

	// Validate State Machine option
	if kvStoreConfig.StateMachine < 1 || kvStoreConfig.StateMachine > 3 {
		return fmt.Errorf("invalid state machine option: %d (must be 1-3)", kvStoreConfig.StateMachine)
	}

	// Validate TLS settings
	if kvStoreConfig.RaftEncrypt {
		if kvStoreConfig.RaftTLSCertFile == "" || kvStoreConfig.RaftTLSKeyFile == "" {
			return fmt.Errorf("when raft-encrypt is true, both raft-peer-cert and raft-peer-key must be provided")
		}
	}

	// Validate MinIO settings when MinIO snapshot is selected
	if kvStoreConfig.BabuzaSnapshot == server.MinIOSnapshot {
		if kvStoreConfig.MinIOEndpoint == "" {
			return fmt.Errorf("minio-endpoint is required when using MinIO snapshot storage")
		}
		if kvStoreConfig.MinIOAccessKeyID == "" {
			return fmt.Errorf("minio-access-key is required when using MinIO snapshot storage")
		}
		if kvStoreConfig.MinIOSecretAccessKey == "" {
			return fmt.Errorf("minio-secret-key is required when using MinIO snapshot storage")
		}
		if kvStoreConfig.MinIOBucket == "" {
			return fmt.Errorf("minio-bucket is required when using MinIO snapshot storage")
		}
	}

	return nil
}
