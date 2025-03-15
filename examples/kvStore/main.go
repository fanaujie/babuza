package main

import (
	"github.com/fanaujie/babuza/examples/kvstore/cmd"
	"os"
)

func main() {
	rootCmd := cmd.NewRootCommand(os.Stdin, os.Stdout, os.Stderr)
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
