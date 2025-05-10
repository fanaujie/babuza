package cmd

import (
	"fmt"
	barPB "github.com/cheggaaa/pb/v3"
	"github.com/fanaujie/babuza/test/kvbench/client"
	"github.com/fanaujie/babuza/test/kvbench/report"
	"github.com/spf13/cobra"
	"math/rand"
	"os"
	"sync"
	"time"
)

var (
	// Common flags
	endpoints    []string
	totalClients uint
	dialTimeout  time.Duration
	reqTimeout   time.Duration

	// Shard related flags
	shardCount   int
	targetLeader bool

	// Benchmark related flags
	sample  bool
	precise bool

	// Progress tracking
	bar *barPB.ProgressBar
	wg  sync.WaitGroup

	// Reporting
	reporter *report.Reporter
)

// RootCmd represents the base command when called without any subcommands
var RootCmd = &cobra.Command{
	Use:   "kvbench",
	Short: "A benchmark tool for raft-based key-value services",
	Long: `kvbench is a benchmark tool for raft-based key-value services.
It supports benchmarking put, get, and mixed workloads with various
configuration options including sharding and leader targeting.`,
}

func init() {
	// Connection flags
	RootCmd.PersistentFlags().StringSliceVar(&endpoints, "endpoints", []string{"127.0.0.1:14200"}, "Service endpoints")
	RootCmd.PersistentFlags().UintVar(&totalClients, "clients", 1, "Total number of clients")
	RootCmd.PersistentFlags().DurationVar(&dialTimeout, "dial-timeout", 2*time.Second, "Timeout for establishing connections")
	RootCmd.PersistentFlags().DurationVar(&reqTimeout, "request-timeout", 10*time.Second, "Timeout for individual requests")

	// Shard related flags
	RootCmd.PersistentFlags().IntVar(&shardCount, "shards", 1, "Number of shards in the service")
	RootCmd.PersistentFlags().BoolVar(&targetLeader, "target-leader", false, "Send requests only to shard leaders")

	// Benchmark related flags
	RootCmd.PersistentFlags().BoolVar(&precise, "precise", false, "Use full floating point precision in reports")
	RootCmd.PersistentFlags().BoolVar(&sample, "sample", false, "Sample requests for every second")
}

// mustCreateClients creates the requested number of clients
func mustCreateClients(clientFactory client.Factory) []client.Client {
	clients := make([]client.Client, totalClients)

	// Base configuration for all clients
	cfg := client.Config{
		Endpoints:      endpoints,
		TargetLeader:   targetLeader,
		ShardCount:     shardCount,
		DialTimeout:    dialTimeout,
		RequestTimeout: reqTimeout,
	}

	// Create clients
	for i := range clients {
		var err error
		clients[i], err = clientFactory.NewClient(cfg)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to create client %d: %v\n", i, err)
			os.Exit(1)
		}
	}

	return clients
}

// mustRandBytes generates a slice of random bytes with the given size
func mustRandBytes(n int) []byte {
	b := make([]byte, n)
	_, err := rand.Read(b)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to generate random bytes: %v\n", err)
		os.Exit(1)
	}
	return b
}

// newReporter creates a new reporter instance
func newReporter() *report.Reporter {
	return report.NewReporter(sample, precise)
}
