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
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := parseAndValidateServerParams(); err != nil {
				return err
			}
			return server.NewServer(kvStoreConfig).Start()
		},
	}
	serverCommand.Flags().StringVar(&kvStoreConfig.KvServiceHttpAddress, "kv-http-address", "localhost:24200", "Defines the HTTP address for the key-value server.")
	serverCommand.Flags().StringVar(&kvStoreConfig.HttpCertFile, "http-cert", "", "Specifies the path to the HTTP key-value server's X.509 certificate.")
	serverCommand.Flags().StringVar(&kvStoreConfig.HttpKeyFile, "http-key", "", "Specifies the path to the HTTP key-value server's X.509 private key.")
	serverCommand.Flags().Uint64Var(&kvStoreConfig.RaftClusterId, "raft-cluster-id", 100, "Sets the Raft cluster ID. Default is 100.")
	serverCommand.Flags().Uint64Var(&kvStoreConfig.RaftLocalPeerId, "raft-local-peer-id", 1, "Sets the unique peer ID within the Raft cluster. Default is 1.")
	serverCommand.Flags().BoolVar(&kvStoreConfig.RaftVoterOrLearner, "raft-voter", true, "Configures the node as a voter. If not set, the node will be configured as a learner. Default is true.")
	serverCommand.Flags().StringVar(&kvStoreConfig.RaftLocalPeerAddress, "raft-local-peer-address", "localhost:14200", "Defines the Raft local peer address.")
	serverCommand.Flags().BoolVar(&kvStoreConfig.JoinRaftCluster, "raft-join-cluster", false, "If set to true, the node will join the Raft cluster. Default is false.")
	serverCommand.Flags().StringVar(&clusterPeersAddress, "raft-cluster-peers-address", "1=localhost:14200", "Provides a list of all Raft peer addresses in the cluster.")
	serverCommand.Flags().BoolVar(&kvStoreConfig.RaftEncrypt, "raft-encrypt", false, "If set to true, TLS will be enabled. Default is false.")
	serverCommand.Flags().StringVar(&kvStoreConfig.RaftTLSCertFile, "raft-peer-cert", "", "Specifies the path to the Raft address's X.509 certificate.")
	serverCommand.Flags().StringVar(&kvStoreConfig.RaftTLSKeyFile, "raft-peer-key", "", "Specifies the path to the Raft address's X.509 private key.")
	serverCommand.Flags().StringVar(&kvStoreConfig.RaftTLSRootCA, "raft-peer-root-ca", "", "Specifies the path to the root X.509 certificate for the Raft transport.")
	serverCommand.Flags().StringVar(&kvStoreConfig.RaftStorageDir, "raft-storage-dir", "./raft_storage", "Defines the path to the Raft storage directory. Default is the 'raft_storage' directory in the current directory.")
	serverCommand.Flags().StringVar(&kvStoreConfig.BabuzaSession, "babuza-session", server.NoOpSession,
		fmt.Sprintf("Selects the session implementation. If set to \"%s\" or \"%s\", the state machine will support idempotency. Options are: \"%s\", \"%s\", \"%s\".",
			server.ExpireSession, server.LRUSession, server.NoOpSession, server.ExpireSession, server.LRUSession))
	serverCommand.Flags().StringVar(&kvStoreConfig.BabuzaTransportProtocol, "babuza-transport-protocol", server.TcpTransport,
		fmt.Sprintf("Selects the transport protocol implementation. Options are: \"%s\", \"%s\".",
			server.TcpTransport, server.HttpTransport))
	serverCommand.Flags().StringVar(&kvStoreConfig.BabuzaWal, "babuza-wal", server.BabuzaWal,
		fmt.Sprintf("Selects the Write-Ahead Log (WAL) implementation. Options are: \"%s\", \"%s\".",
			server.BabuzaWal, server.ETCDWal))
	serverCommand.Flags().StringVar(&kvStoreConfig.BabuzaSnapshot, "babuza-snapshot", server.DurableSnapshot,
		fmt.Sprintf("Selects the snapshot implementation. Options are: \"%s\", \"%s\".",
			server.DurableSnapshot, server.VolatileSnapshot))
	serverCommand.Flags().StringVar(&kvStoreConfig.StateMachine, "state-machine", server.MemoryType,
		fmt.Sprintf("Selects the type of state machine for the key-value store. Options are: \"%s\", \"%s\", \"%s\".",
			server.MemoryType, server.MemoryWithConcurrentSnapshotType, server.DiskType))
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
			return fmt.Errorf("repeate peer id(%d)", id)
		}
		kvStoreConfig.RaftClusterVotersAddress[id] = peer[1]
	}
	match := false
	for _, s := range []string{server.NoOpSession, server.ExpireSession, server.LRUSession} {
		if s == kvStoreConfig.BabuzaSession {
			match = true
			break
		}
	}
	if !match {
		return fmt.Errorf("not support option of babuza-session(%s)", kvStoreConfig.BabuzaSession)
	}
	match = false
	for _, s := range []string{server.TcpTransport, server.HttpTransport} {
		if s == kvStoreConfig.BabuzaTransportProtocol {
			match = true
			break
		}
	}
	if !match {
		return fmt.Errorf("not support option of babuza-transport-protocol(%s)", kvStoreConfig.BabuzaTransportProtocol)
	}
	match = false
	for _, s := range []string{server.BabuzaWal, server.ETCDWal, server.VolatileWal} {
		if s == kvStoreConfig.BabuzaWal {
			match = true
			break
		}
	}
	if !match {
		return fmt.Errorf("not support option of babuza-wal(%s)", kvStoreConfig.BabuzaWal)
	}
	match = false
	for _, s := range []string{server.DurableSnapshot, server.VolatileSnapshot} {
		if s == kvStoreConfig.BabuzaSnapshot {
			match = true
			break
		}
	}
	if !match {
		return fmt.Errorf("not support option of babuza-snapshot(%s)", kvStoreConfig.BabuzaSnapshot)
	}
	match = false
	for _, s := range []string{server.MemoryType, server.MemoryWithConcurrentSnapshotType, server.DiskType} {
		if s == kvStoreConfig.StateMachine {
			match = true
			break
		}
	}
	if !match {
		return fmt.Errorf("not support option of state-machine(%s)", kvStoreConfig.StateMachine)
	}
	return nil
}
