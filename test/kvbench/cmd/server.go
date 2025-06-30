package cmd

import (
	"github.com/spf13/cobra"
)

// serverCmd represents the server command
var serverCmd = &cobra.Command{
	Use:   "server",
	Short: "Run a kvbench server instance",
	Long:  `Starts a kvbench server instance based on the babuza Raft library.`,
	Run:   runServerFunc,
}

func init() {
	RootCmd.AddCommand(serverCmd)
}
