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
	"fmt"
	"github.com/fanaujie/babuza/test/kvbench/server/multi"
	"github.com/spf13/cobra"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var (
	multiShardCount      uint64
	initialRaftStoresRaw string
	initialGRPCStoresRaw string
)

// multiCmd represents the multi command
var multiCmd = &cobra.Command{
	Use:   "multi",
	Short: "Run a multi-raft KV benchmark server",
	Long: `Starts a multi-raft KV benchmark server with multiple Raft groups (shards) using the babuza MultiRaft implementation.

This command creates a KV server that uses multiple Raft groups, each managing a portion of the key space.
The number of Raft groups is specified by the --shards parameter, which must be greater than 0.

Each Raft group operates independently, allowing for better scalability and performance in distributed environments.`,
	Run: runMultiServerFunc,
}

func init() {
	serverCmd.AddCommand(multiCmd)

	// Add the same flags as the single command
	multiCmd.Flags().Uint64Var(&clusterID, "cluster-id", 1, "ID of the Raft cluster")
	multiCmd.Flags().StringVar(&dataDir, "data-dir", "./data", "Directory for storing server data")
	multiCmd.Flags().Uint64Var(&storeID, "store-id", 1, "ID of the local peer")
	multiCmd.Flags().StringVar(&grpcAddr, "grpc-address", "127.0.0.1:24200", "Address for the gRPC service")
	multiCmd.Flags().StringVar(&raftAddr, "raft-address", "127.0.0.1:14200", "Address for Raft communication")
	multiCmd.Flags().StringVar(&initialRaftStoresRaw, "initial-raft-stores", "", "List of initial raft stores to connect (e.g., 1=127.0.0.1:14200,2=127.0.0.1:14201)")
	multiCmd.Flags().StringVar(&initialGRPCStoresRaw, "initial-grpc-stores", "", "List of initial gRPC stores to connect (e.g., 1=127.0.0.1:24200,2=127.0.0.1:24201,3=127.0.0.1:24202)")
	multiCmd.Flags().Uint64Var(&multiShardCount, "shards", 1, "Number of Raft groups (shards) to create")
}

func runMultiServerFunc(cmd *cobra.Command, args []string) {
	// Validate shard count
	if multiShardCount <= 0 {
		fmt.Fprintf(os.Stderr, "Shard count must be greater than 0\n")
		os.Exit(1)
	}

	// Parse initial peers
	var err error
	initialRaftStores, err := parseInitialPeers(initialRaftStoresRaw)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to parse initial stores: %v\n", err)
		os.Exit(1)
	}
	initialGRPCStores, err := parseInitialPeers(initialGRPCStoresRaw)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to parse initial gRPC stores: %v\n", err)
		os.Exit(1)
	}

	// Create server configuration
	cfg := multi.Config{
		DataDir:           dataDir,
		ClusterID:         clusterID,
		StoreID:           storeID,
		GrpcAddress:       grpcAddr,
		RaftAddress:       raftAddr,
		InitialRaftStores: initialRaftStores,
		InitialGRPCStores: initialGRPCStores,
		ShardCount:        multiShardCount,
	}

	// Create the server
	srv, err := multi.NewServer(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create server: %v\n", err)
		os.Exit(1)
	}

	// Start the server
	fmt.Printf("Starting multi-raft server with %d shards, gRPC on %s and Raft on %s...\n",
		multiShardCount, cfg.GrpcAddress, cfg.RaftAddress)
	if err = srv.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to start server: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("Server started successfully.")

	// Wait for leadership (only for the first node in a cluster)

	fmt.Println("Waiting for leadership...")
	if err = srv.WaitForLeadership(30 * time.Second); err != nil {
		fmt.Fprintf(os.Stderr, "Leadership timeout: %v\n", err)
	} else {
		fmt.Println("Leadership established.")
	}

	// Wait for interrupt signal to gracefully shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	fmt.Println("Shutting down server...")
	if err = srv.Stop(); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to stop server gracefully: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("Server stopped.")
}
