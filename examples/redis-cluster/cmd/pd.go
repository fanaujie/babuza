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
	"github.com/fanaujie/babuza/examples/redis-cluster/pkg/pdserver"
	"github.com/spf13/cobra"
)

var (
	pdHttpAddr string
	pdGrpcAddr string
)

var pdCmd = &cobra.Command{
	Use:   "pd",
	Short: "Start a PD (Placement Driver) node for Redis cluster",
	Long: `Start a PD (Placement Driver) control node for Redis cluster.
This component is responsible for resource scheduling and management.
It provides both HTTP and gRPC interfaces for communication.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		config := pdserver.Config{
			HttpAddr: pdHttpAddr,
			GrpcAddr: pdGrpcAddr,
		}

		server, err := pdserver.NewPDServer(config)
		if err != nil {
			return err
		}

		return server.Run()
	},
}

func init() {
	rootCmd.AddCommand(pdCmd)

	pdCmd.Flags().StringVar(&pdHttpAddr, "http-address", "localhost:15000", "HTTP server listen address")
	pdCmd.Flags().StringVar(&pdGrpcAddr, "grpc-address", "localhost:15001", "gRPC server listen address")
}
