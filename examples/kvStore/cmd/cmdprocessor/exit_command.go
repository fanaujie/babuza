package cmdprocessor

type ExitCommand struct {
}

func NewExitCommand() *ExitCommand {
	return &ExitCommand{}
}

func (jc *ExitCommand) Execute(args []string) error {
	return nil
}

func (jc *ExitCommand) Help() string {
	return "exit - terminate the command line session"
}
