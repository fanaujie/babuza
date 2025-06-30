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
