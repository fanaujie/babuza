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
	multiShardCount uint
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
	multiCmd.Flags().Uint64Var(&localPeerID, "local-peer-id", 1, "ID of the local peer")
	multiCmd.Flags().StringVar(&grpcAddr, "grpc-address", "127.0.0.1:24200", "Address for the gRPC service")
	multiCmd.Flags().StringVar(&raftAddr, "raft-address", "127.0.0.1:14200", "Address for Raft communication")
	multiCmd.Flags().StringVar(&initialRaftPeersRaw, "initial-raft-peers", "", "List of initial raft peers to connect (e.g., 1=127.0.0.1:14200,2=127.0.0.1:14201)")
	multiCmd.Flags().StringVar(&initialGRPCPeersRaw, "initial-grpc-peers", "", "List of initial gRPC peers to connect (e.g., 1=127.0.0.1:24200,2=127.0.0.1:24201,3=127.0.0.1:24202)")
	multiCmd.Flags().UintVar(&multiShardCount, "shards", 1, "Number of Raft groups (shards) to create")
}

func runMultiServerFunc(cmd *cobra.Command, args []string) {
	// Validate shard count
	if multiShardCount <= 0 {
		fmt.Fprintf(os.Stderr, "Shard count must be greater than 0\n")
		os.Exit(1)
	}

	// Parse initial peers
	var err error
	initialRaftPeers, err := parseInitialPeers(initialRaftPeersRaw)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to parse initial peers: %v\n", err)
		os.Exit(1)
	}
	initialGRPCPeers, err := parseInitialPeers(initialGRPCPeersRaw)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to parse initial gRPC peers: %v\n", err)
		os.Exit(1)
	}

	// Create server configuration
	cfg := multi.Config{
		DataDir:          dataDir,
		ClusterID:        clusterID,
		LocalPeerID:      localPeerID,
		GrpcAddress:      grpcAddr,
		RaftAddress:      raftAddr,
		InitialRaftPeers: initialRaftPeers,
		InitialGRPCPeers: initialGRPCPeers,
		ShardCount:       multiShardCount,
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
