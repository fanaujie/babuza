package cmd

import (
	"crypto/rand"
	"fmt"
	barPB "github.com/cheggaaa/pb/v3"
	"github.com/fanaujie/babuza/test/kvbench/report"
	"github.com/spf13/cobra"
	"os"
	"sync"
)

var (
	// Common flags
	clientClusterID uint64
	endpoints       []string
	connections     uint
	totalClients    uint
	// Shard related flags
	shardCount   uint
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

var ClientCmd = &cobra.Command{
	Use:   "client",
	Short: "Execute client-side benchmark operations",
	Long: `Manages client connections and executes benchmark operations against key-value services.

This command initializes client connections to target endpoints with configurable parameters including:
- Connection pooling and client concurrency settings
- Shard targeting strategies
- Performance measurement and reporting options
- Customizable workload parameters

Use subcommands to specify the operation type (put, get, delete) to benchmark.`,
}

func init() {
	RootCmd.AddCommand(ClientCmd)

	// Connection flags
	ClientCmd.PersistentFlags().StringSliceVar(&endpoints, "endpoints", []string{"127.0.0.1:24200"}, "Service endpoints")
	ClientCmd.PersistentFlags().UintVar(&connections, "connections", 1, "Total number of connection")
	ClientCmd.PersistentFlags().UintVar(&totalClients, "clients", 1, "Total number of clients")
	ClientCmd.PersistentFlags().Uint64Var(&clientClusterID, "cluster-id", 1, "ID of the Raft cluster")
	// Shard related flags
	ClientCmd.PersistentFlags().UintVar(&shardCount, "shards", 1, "Number of shards in the service")
	ClientCmd.PersistentFlags().BoolVar(&targetLeader, "target-leader", false, "Send requests only to shard leaders")

	// Benchmark related flags
	ClientCmd.PersistentFlags().BoolVar(&precise, "precise", false, "Use full floating point precision in reports")
	ClientCmd.PersistentFlags().BoolVar(&sample, "sample", false, "Sample requests for every second")
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
