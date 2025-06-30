// Copyright 2025 Chen Chunchieh <junjie725@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


package cmd

import (
	"github.com/fanaujie/babuza/examples/redis-cluster/pkg/server"
	"github.com/spf13/cobra"
)

var (
	storeID                          uint64
	clusterID                        uint64
	redisListenAddr                  string
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
			RedisListenAddr:                  redisListenAddr,
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
	serverCmd.Flags().StringVar(&redisListenAddr, "redis-address", "localhost:6379", "Redis protocol listen address")
	serverCmd.Flags().StringVar(&raftAddr, "raft-address", "localhost:14200", "Raft transport listen address")
	serverCmd.Flags().StringVar(&dataDir, "data-dir", "./data", "Data directory for Raft logs and snapshots")
	serverCmd.Flags().IntVar(&initialShards, "shards", 100, "Number of initial shards (Raft groups)")
	serverCmd.Flags().StringSliceVar(&storeAddrs, "initial-raft-stores", nil, "List of store Raft addresses (format: id=addr, e.g., 1=localhost:14200)")
	serverCmd.Flags().IntVar(&intervalHeartbeatStore, "interval-heartbeat-store", 1, "Interval(sec): node heartbeat")
	serverCmd.Flags().IntVar(&intervalHeartbeatRaftGroupLeader, "interval-heartbeat-raft-group-leader", 3, "Interval(sec): Raft group leader heartbeat")
	serverCmd.Flags().StringVar(&pdGRPCAddr, "pd-address", "localhost:15001", "PD (Placement Driver) address for resource management")
}
