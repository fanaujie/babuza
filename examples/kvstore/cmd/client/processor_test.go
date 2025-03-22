package client

import (
	"testing"
)

type MockCommand struct {
	ExecuteCalled bool
	HelpCalled    bool
}

func (m *MockCommand) Execute(args []string) error {
	m.ExecuteCalled = true
	return nil
}

func (m *MockCommand) Help() string {
	m.HelpCalled = true
	return "mock command help"
}

func TestCommandProcessor(t *testing.T) {
	cp := NewCommandProcessor()
	mockCommand := &MockCommand{}

	// Test AddCommand
	cp.AddCommand("mock", mockCommand)
	if _, ok := cp.commands["mock"]; !ok {
		t.Errorf("AddCommand failed, command not found")
	}

	// Test Execute command
	err := cp.commands["mock"].Execute([]string{})
	if err != nil {
		t.Errorf("Execute command failed: %v", err)
	}
	if !mockCommand.ExecuteCalled {
		t.Errorf("Execute method of mock command was not called")
	}

	// Test Help command
	help := cp.commands["mock"].Help()
	if help != "mock command help" {
		t.Errorf("Help command failed, expected: %v, got: %v", "mock command help", help)
	}
	if !mockCommand.HelpCalled {
		t.Errorf("Help method of mock command was not called")
	}
}
