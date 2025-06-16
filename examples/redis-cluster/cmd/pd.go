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
