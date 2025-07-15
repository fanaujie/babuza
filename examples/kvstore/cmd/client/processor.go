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
	"io"
	"os"
)

type Command interface {
	Execute(args []string) error
	Help() string
}

type CommandProcessor struct {
	commands      map[string]Command
	inputHandler  InputHandler
	outputHandler OutputHandler
	parser        CommandParser
}

func NewCommandProcessor() *CommandProcessor {
	return NewCommandProcessorWithHandlers(
		NewStdinInputHandler(os.Stdin),
		NewStandardOutputHandler(os.Stdout, os.Stderr),
		NewDefaultCommandParser(),
	)
}

func NewCommandProcessorWithHandlers(
	inputHandler InputHandler,
	outputHandler OutputHandler,
	parser CommandParser,
) *CommandProcessor {
	return &CommandProcessor{
		commands:      make(map[string]Command),
		inputHandler:  inputHandler,
		outputHandler: outputHandler,
		parser:        parser,
	}
}

func (cp *CommandProcessor) AddCommand(cmdName string, cmd Command) {
	cp.commands[cmdName] = cmd
}

func (cp *CommandProcessor) showWelcomeMessage() error {
	if err := cp.outputHandler.WriteOutput("Welcome to KvStore CLI!"); err != nil {
		return err
	}
	if err := cp.outputHandler.WriteOutput("Type 'help' to see all available commands."); err != nil {
		return err
	}
	if err := cp.outputHandler.WriteOutput(""); err != nil {
		return err
	}
	return nil
}

func (cp *CommandProcessor) StartCommandLoop() error {
	// Show welcome message and available commands
	if err := cp.showWelcomeMessage(); err != nil {
		return fmt.Errorf("failed to show welcome message: %w", err)
	}
	
	for {
		if err := cp.outputHandler.WritePrompt(); err != nil {
			return fmt.Errorf("failed to write prompt: %w", err)
		}

		input, err := cp.inputHandler.ReadInput()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return fmt.Errorf("failed to read input: %w", err)
		}

		parsed, err := cp.parser.Parse(input)
		if err != nil {
			continue
		}

		if err = cp.executeCommand(parsed); err != nil {
			if writeErr := cp.outputHandler.WriteError(err); writeErr != nil {
				return fmt.Errorf("failed to write error: %w", writeErr)
			}
		}

		if parsed.Name == "exit" {
			return nil
		}
	}
}

func (cp *CommandProcessor) executeCommand(parsed *ParsedCommand) error {
	cmd, exists := cp.commands[parsed.Name]
	if !exists {
		if err := cp.outputHandler.WriteError(fmt.Errorf("unknown command: %s", parsed.Name)); err != nil {
			return err
		}
		return cp.outputHandler.WriteCommandList(cp.commands)
	}

	return cmd.Execute(parsed.Args)
}
