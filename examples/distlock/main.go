package main

import (
	"os"

	"github.com/fanaujie/babuza/examples/distlock/cmd"
)

func main() {
	rootCmd := cmd.NewRootCommand(os.Stdin, os.Stdout, os.Stderr)
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
