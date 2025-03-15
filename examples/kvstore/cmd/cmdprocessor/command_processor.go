package cmdprocessor

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
