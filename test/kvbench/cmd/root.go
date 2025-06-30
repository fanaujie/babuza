package cmd

import (
	"github.com/spf13/cobra"
)

// RootCmd represents the base command when called without any subcommands
var RootCmd = &cobra.Command{
	Use:   "kvbench",
	Short: "High-performance benchmark tool for distributed key-value stores",
	Long: `kvbench is a comprehensive benchmarking utility designed specifically for raft-based key-value services.

It provides detailed performance metrics and supports both single-raft and multi-raft cluster configurations
with advanced options including sharding strategies, leader targeting, customizable workloads, 
and precise reporting capabilities.`,
}
