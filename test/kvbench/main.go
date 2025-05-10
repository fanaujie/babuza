package main

import (
	"fmt"
	"github.com/fanaujie/babuza/test/kvbench/cmd"
	"os"
)

func main() {
	if err := cmd.RootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(-1)
	}
}
