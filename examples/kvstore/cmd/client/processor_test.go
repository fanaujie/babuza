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
