package cmd

import (
	"github.com/fanaujie/babuza/examples/redis-cluster/pkg/server"
	"github.com/spf13/cobra"
)

var (
	nodeID         uint64
	clusterID      uint64
	listenAddr     string
	raftAddr       string
	dataDir        string
	joinExisting   bool
	initialShards  int
	peerAddrs      []string
	configFilePath string
)

var serverCmd = &cobra.Command{
	Use:   "server",
	Short: "Start a Redis cluster node",
	Long: `Start a Redis cluster node that implements Redis protocol interface 
with Multi-Raft consensus for distributed data management.
Each node can participate in multiple shards (Raft groups).`,
	RunE: func(cmd *cobra.Command, args []string) error {
		config := server.Config{
			NodeID:        nodeID,
			ClusterID:     clusterID,
			ListenAddr:    listenAddr,
			RaftAddr:      raftAddr,
			DataDir:       dataDir,
			JoinExisting:  joinExisting,
			InitialShards: initialShards,
			PeerAddrs:     peerAddrs,
		}

		s, err := server.NewServer(config)
		if err != nil {
			return err
		}

		return s.Run()
	},
}

func init() {
	rootCmd.AddCommand(serverCmd)

	serverCmd.Flags().Uint64Var(&nodeID, "node-id", 1, "Unique node ID")
	serverCmd.Flags().Uint64Var(&clusterID, "cluster-id", 10000, "Cluster ID")
	serverCmd.Flags().StringVar(&listenAddr, "redis-address", "localhost:6379", "Redis protocol listen address")
	serverCmd.Flags().StringVar(&raftAddr, "raft-address", "localhost:14200", "Raft transport listen address")
	serverCmd.Flags().StringVar(&dataDir, "data-dir", "./data", "Data directory for Raft logs and snapshots")
	serverCmd.Flags().BoolVar(&joinExisting, "join", false, "Join existing cluster")
	serverCmd.Flags().IntVar(&initialShards, "shards", 100, "Number of initial shards (Raft groups)")
	serverCmd.Flags().StringSliceVar(&peerAddrs, "initial-raft-peers", nil, "List of peer Raft addresses (format: id=addr, e.g., 1=localhost:14200)")
}
