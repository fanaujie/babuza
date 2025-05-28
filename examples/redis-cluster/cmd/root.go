package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "redis-cluster",
	Short: "A Redis cluster implementation using Babuza Multi-Raft",
	Long: `A Redis compatible server cluster using Babuza Multi-Raft for distributed consensus.
This implementation provides sharding instead of the traditional Redis slot mechanism.
Each shard corresponds to a separate Raft group for enhanced reliability and consistency.`,
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
