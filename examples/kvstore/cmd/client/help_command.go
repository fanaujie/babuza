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
	"fmt"
	"sort"
	"strings"
)

type HelpCommand struct {
	processor *CommandProcessor
}

func NewHelpCommand(processor *CommandProcessor) *HelpCommand {
	return &HelpCommand{
		processor: processor,
	}
}

func (hc *HelpCommand) Execute(args []string) error {
	if len(args) > 1 {
		return fmt.Errorf("help command accepts at most 1 argument, got %d", len(args))
	}

	if len(args) == 1 {
		return hc.showSpecificCommandHelp(args[0])
	}

	return hc.showAllCommandsHelp()
}

func (hc *HelpCommand) showSpecificCommandHelp(cmdName string) error {
	cmd, exists := hc.processor.commands[cmdName]
	if !exists {
		return fmt.Errorf("unknown command: '%s'", cmdName)
	}

	fmt.Printf("Help for command '%s':\n", cmdName)
	fmt.Println(strings.Repeat("-", 40))
	fmt.Println(cmd.Help())
	return nil
}

func (hc *HelpCommand) showAllCommandsHelp() error {
	fmt.Println("KvStore CLI - Available Commands:")
	fmt.Println(strings.Repeat("=", 60))
	fmt.Println()

	var commandNames []string
	for name := range hc.processor.commands {
		commandNames = append(commandNames, name)
	}
	sort.Strings(commandNames)

	fmt.Printf("%-12s %s\n", "Command", "Description")
	fmt.Println(strings.Repeat("-", 60))

	for _, name := range commandNames {
		cmd := hc.processor.commands[name]
		helpText := cmd.Help()
		parts := strings.SplitN(helpText, " - ", 2)
		if len(parts) == 2 {
			fmt.Printf("%-12s %s\n", parts[0], parts[1])
		} else {
			fmt.Printf("%-12s %s\n", name, helpText)
		}
	}

	fmt.Println()
	fmt.Println("Usage:")
	fmt.Println("  Type 'help <command>' for detailed help on a specific command")
	fmt.Println("  Type 'exit' to quit the KvStore CLI")
	fmt.Println()

	return nil
}

func (hc *HelpCommand) Help() string {
	return "help [command] - Show help information for all commands or a specific command"
}