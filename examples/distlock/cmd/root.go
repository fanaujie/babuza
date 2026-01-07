package cmd

import (
	"io"

	"github.com/spf13/cobra"
)

func NewRootCommand(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	rootCmd := &cobra.Command{
		Use:   "distlock [command]",
		Short: "distlock is a distributed lock service built with Babuza.",
		Long: `distlock is a distributed lock service built with Babuza.
It provides both server and client functionality for distributed locking.
Use 'server' to start a distlock node and 'client' to interact with the service.`,
	}
	rootCmd.AddCommand(NewServerCommand(stdin, stdout, stderr))
	rootCmd.AddCommand(NewClientCommand(stdin, stdout, stderr))
	rootCmd.SetOut(stderr)
	return rootCmd
}
