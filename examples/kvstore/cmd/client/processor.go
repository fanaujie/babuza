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


package client

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

type Command interface {
	Execute(args []string) error
	Help() string
}

type CommandProcessor struct {
	commands map[string]Command
}

func NewCommandProcessor() *CommandProcessor {
	cp := &CommandProcessor{}
	cp.commands = make(map[string]Command)
	return cp
}

func (cp *CommandProcessor) AddCommand(cmdName string, cmd Command) {
	cp.commands[cmdName] = cmd
}

func (cp *CommandProcessor) StartCommandLoop() error {
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print("KvStore> ")
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input)
		// Parse command and arguments
		parts := strings.Split(input, " ")
		command := parts[0]
		args := parts[1:]

		// Execute command
		if cmd, ok := cp.commands[command]; ok {
			err := cmd.Execute(args)
			if err != nil {
				fmt.Println(err)
				continue
			} else {
				if command == "exit" {
					return nil
				}
			}
		} else {
			fmt.Println("Error: unknown command")
			cp.ListCommands()
		}
	}
}

func (cp *CommandProcessor) ListCommands() {
	fmt.Println("Supported commands:")
	for _, command := range cp.commands {
		fmt.Println(command.Help())
	}
}
