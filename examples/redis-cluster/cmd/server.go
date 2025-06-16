package cmd

import (
	"github.com/fanaujie/babuza/examples/redis-cluster/pkg/server"
	"github.com/spf13/cobra"
)

var (
	storeID                          uint64
	clusterID                        uint64
	listenAddr                       string
	raftAddr                         string
	dataDir                          string
	initialShards                    int
	storeAddrs                       []string
	intervalHeartbeatStore           int
	intervalHeartbeatRaftGroupLeader int
	pdGRPCAddr                       string
)

var serverCmd = &cobra.Command{
	Use:   "server",
	Short: "Start a Redis cluster node",
	Long: `Start a Redis cluster node that implements Redis protocol interface 
with Multi-Raft consensus for distributed data management.
Each node can participate in multiple shards (Raft groups).`,
	RunE: func(cmd *cobra.Command, args []string) error {
		config := server.Config{
			StoreID:                          storeID,
			ClusterID:                        clusterID,
			ListenAddr:                       listenAddr,
			RaftAddr:                         raftAddr,
			DataDir:                          dataDir,
			InitialShards:                    initialShards,
			StoreAddrs:                       storeAddrs,
			IntervalHeartbeatStore:           intervalHeartbeatStore,
			IntervalHeartbeatRaftGroupLeader: intervalHeartbeatRaftGroupLeader,
			PdGRPCAddr:                       pdGRPCAddr,
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

	serverCmd.Flags().Uint64Var(&storeID, "store-id", 1, "Unique store ID")
	serverCmd.Flags().Uint64Var(&clusterID, "cluster-id", 10000, "Cluster ID")
	serverCmd.Flags().StringVar(&listenAddr, "redis-address", "localhost:6379", "Redis protocol listen address")
	serverCmd.Flags().StringVar(&raftAddr, "raft-address", "localhost:14200", "Raft transport listen address")
	serverCmd.Flags().StringVar(&dataDir, "data-dir", "./data", "Data directory for Raft logs and snapshots")
	serverCmd.Flags().IntVar(&initialShards, "shards", 100, "Number of initial shards (Raft groups)")
	serverCmd.Flags().StringSliceVar(&storeAddrs, "initial-raft-stores", nil, "List of store Raft addresses (format: id=addr, e.g., 1=localhost:14200)")
	serverCmd.Flags().IntVar(&intervalHeartbeatStore, "interval-heartbeat-store", 3, "Interval(sec): node heartbeat")
	serverCmd.Flags().IntVar(&intervalHeartbeatRaftGroupLeader, "interval-heartbeat-raft-group-leader", 5, "Interval(sec): Raft group leader heartbeat")
	serverCmd.Flags().StringVar(&pdGRPCAddr, "pd-address", "localhost:15001", "PD (Placement Driver) address for resource management")
}
