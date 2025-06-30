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
	"github.com/spf13/cobra"
	"io"
)

func NewRootCommand(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {

	rootCmd := &cobra.Command{
		Use:   "kvstore [command]",
		Short: "kvstore is a distributed key-value store built with Babuza.",
		Long: `kvstore is a distributed key-value store built with Babuza.
It provides both server and client functionality for managing distributed data.
Use 'server' to start a kvstore node and 'client' to interact with the store.`,
	}
	rootCmd.AddCommand(NewServerCommand(stdin, stdout, stderr))
	rootCmd.AddCommand(NewClientCommand(stdin, stdout, stderr))
	rootCmd.SetOut(stderr)
	return rootCmd
}
