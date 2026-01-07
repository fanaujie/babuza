package cmd

import (
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/fanaujie/babuza/examples/distlock/server"
	"github.com/spf13/cobra"
)

var (
	serverConfig        server.Config
	clusterPeersAddress string
)

func NewServerCommand(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	var serverCommand = &cobra.Command{
		Use:   "server",
		Short: "run distributed lock server",
		Long:  `Run a distributed lock server powered by Babuza distributed consensus framework.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := parseServerParams(); err != nil {
				return err
			}
			return server.NewServer(serverConfig).Start()
		},
	}

	serverCommand.Flags().StringVar(&serverConfig.HttpAddress, "http-address", "localhost:24200",
		"HTTP Service Address for the lock API")
	serverCommand.Flags().Uint64Var(&serverConfig.RaftClusterId, "raft-cluster-id", 100,
		"Raft Cluster ID")
	serverCommand.Flags().Uint64Var(&serverConfig.RaftLocalPeerId, "raft-local-peer-id", 1,
		"Unique ID for this node")
	serverCommand.Flags().StringVar(&serverConfig.RaftLocalPeerAddress, "raft-local-peer-address", "localhost:14200",
		"Raft communication address")
	serverCommand.Flags().StringVar(&clusterPeersAddress, "raft-cluster-peers-address", "1=localhost:14200",
		"Cluster peers in format 'ID1=address1,ID2=address2'")
	serverCommand.Flags().StringVar(&serverConfig.RaftStorageDir, "raft-storage-dir", "./raft_storage",
		"Storage directory for Raft data")
	serverCommand.Flags().BoolVar(&serverConfig.JoinRaftCluster, "raft-join-cluster", false,
		"Join existing cluster")

	serverCommand.SetOut(stderr)
	return serverCommand
}

func parseServerParams() error {
	serverConfig.RaftClusterVotersAddress = make(map[uint64]string)
	for _, pe := range strings.Split(clusterPeersAddress, ",") {
		peer := strings.Split(pe, "=")
		if len(peer) != 2 {
			return fmt.Errorf("invalid raft-cluster-peers-address (peer=%s)", peer)
		}
		id, err := strconv.ParseUint(peer[0], 10, 64)
		if err != nil {
			return fmt.Errorf("failed to parse peer=%s", peer)
		}
		if _, ok := serverConfig.RaftClusterVotersAddress[id]; ok {
			return fmt.Errorf("repeated peer id(%d)", id)
		}
		serverConfig.RaftClusterVotersAddress[id] = peer[1]
	}
	return nil
}
