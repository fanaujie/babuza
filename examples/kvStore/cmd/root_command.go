package cmd

import (
	"github.com/spf13/cobra"
	"io"
)

func NewRootCommand(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {

	rootCmd := &cobra.Command{
		Use:   "kvstore [command]",
		Short: "kvstore is a distributed key-value store built with Babuza."}
	rootCmd.AddCommand(NewServerCommand(stdin, stdout, stderr))
	rootCmd.AddCommand(NewClientCommand(stdin, stdout, stderr))
	rootCmd.SetOut(stderr)
	return rootCmd
}
