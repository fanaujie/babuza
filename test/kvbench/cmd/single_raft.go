package cmd

import (
	"fmt"
	"github.com/fanaujie/babuza/test/kvbench/single"
	"github.com/spf13/cobra"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
)

var (
	dataDir         string
	clusterID       uint64
	localPeerID     uint64
	grpcAddr        string
	raftAddr        string
	joinCluster     bool
	initialPeersRaw string
	initialPeers    map[uint64]string
)

// serverCmd represents the server command
var singleCmd = &cobra.Command{
	Use:   "single",
	Short: "Run a single node KV benchmark server",
	Long:  `Starts a single-node Raft KV benchmark server instance using the babuza Raft implementation.`,
	Run:   runServerFunc,
}

func init() {
	serverCmd.AddCommand(singleCmd)

	singleCmd.Flags().StringVar(&dataDir, "data-dir", "./data", "Directory for storing server data")
	singleCmd.Flags().Uint64Var(&localPeerID, "local-peer-id", 1, "ID of the local peer")
	singleCmd.Flags().StringVar(&grpcAddr, "grpc-address", "127.0.0.1:24200", "Address for the gRPC service")
	singleCmd.Flags().StringVar(&raftAddr, "raft-address", "127.0.0.1:14200", "Address for Raft communication")
	singleCmd.Flags().BoolVar(&joinCluster, "join", false, "Join an existing cluster")
	singleCmd.Flags().StringVar(&initialPeersRaw, "initial-peers", "", "List of initial peers to connect to when joining (e.g., 1=127.0.0.1:14200,2=127.0.0.1:14201)")
}

func parseInitialPeers(raw string) (map[uint64]string, error) {
	peers := make(map[uint64]string)
	if strings.TrimSpace(raw) == "" {
		return peers, nil
	}
	entries := strings.Split(raw, ",")
	for _, entry := range entries {
		parts := strings.Split(entry, "=")
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid initial peer format: %s", entry)
		}
		id, err := strconv.ParseUint(parts[0], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid peer ID: %s", parts[0])
		}
		addr := parts[1]
		peers[id] = addr
	}
	return peers, nil
}

func runServerFunc(cmd *cobra.Command, args []string) {
	var err error
	initialPeers, err = parseInitialPeers(initialPeersRaw)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to parse initial peers: %v\n", err)
		os.Exit(1)
	}

	cfg := single.Config{
		DataDir:      dataDir,
		ClusterID:    clusterID,
		LocalPeerID:  localPeerID,
		GrpcAddress:  grpcAddr,
		RaftAddress:  raftAddr,
		JoinCluster:  joinCluster,
		InitialPeers: initialPeers,
	}

	srv, err := single.NewServer(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create server: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Starting server with gRPC on %s and Raft on %s...\n", cfg.GrpcAddress, cfg.RaftAddress)
	if err = srv.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to start server: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("Server started successfully.")

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
