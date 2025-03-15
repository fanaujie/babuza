package cmd

import (
	"fmt"
	"github.com/fanaujie/babuza/examples/kvstore/client"
	"github.com/fanaujie/babuza/examples/kvstore/cmd/cmdprocessor"
	"github.com/spf13/cobra"
	"io"
	"strconv"
	"strings"
	"time"
)

var (
	clientConfig   client.Config
	clusterMembers string
)

func NewClientCommand(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	var cliCommand = &cobra.Command{
		Use:   "client",
		Short: "run a client of key-value server",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := parseAndValidateClientParams(); err != nil {
				return err
			}
			kvClient, err := client.CreateKvStoreClient(clientConfig,
				client.NewAutoIncrementSession())
			if err != nil {
				return err
			}
			processor := cmdprocessor.NewCommandProcessor()
			processor.AddCommand("exit", cmdprocessor.NewExitCommand())
			processor.AddCommand("join", cmdprocessor.NewJoinCommand(kvClient))
			processor.AddCommand("set", cmdprocessor.NewSetCommand(kvClient))
			processor.AddCommand("get", cmdprocessor.NewGetCommand(kvClient))
			return processor.StartCommandLoop()
		},
	}
	cliCommand.Flags().BoolVar(&clientConfig.EnableTLS, "enable-tls", false, "Enable TLS for the client.")
	cliCommand.Flags().StringVar(&clusterMembers, "cluster-members", "1=localhost:24200", "Define the members of the key-value store server cluster for the client. The format should be: id1=address1,id2=address2")
	cliCommand.Flags().DurationVar(&clientConfig.AutoSyncInterval, "auto-sync-interval", time.Second*5, "Specify the auto sync interval for the client.")
	return cliCommand
}

func parseAndValidateClientParams() error {
	clientConfig.KvStoreClusterMembers = make([]client.ClusterPeer, 0)
	for _, mem := range strings.Split(clusterMembers, ",") {
		member := strings.Split(mem, "=")
		if len(member) != 2 {
			return fmt.Errorf("invalid cluster-members (member=%s)", member)
		}
		id, err := strconv.ParseUint(member[0], 10, 64)
		if err != nil {
			return fmt.Errorf("failed to parse member=%s", member)
		}
		clientConfig.KvStoreClusterMembers = append(clientConfig.KvStoreClusterMembers, client.ClusterPeer{
			Id:               id,
			KvServiceAddress: member[1],
		})
	}
	return nil
}
