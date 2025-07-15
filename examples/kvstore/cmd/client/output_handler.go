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
)

type OutputHandler interface {
	WritePrompt() error
	WriteOutput(message string) error
	WriteError(err error) error
	WriteCommandList(commands map[string]Command) error
}

type StandardOutputHandler struct {
	stdout io.Writer
	stderr io.Writer
}

func NewStandardOutputHandler(stdout, stderr io.Writer) OutputHandler {
	return &StandardOutputHandler{
		stdout: stdout,
		stderr: stderr,
	}
}

func (h *StandardOutputHandler) WritePrompt() error {
	_, err := fmt.Fprint(h.stdout, "KvStore> ")
	return err
}

func (h *StandardOutputHandler) WriteOutput(message string) error {
	_, err := fmt.Fprintln(h.stdout, message)
	return err
}

func (h *StandardOutputHandler) WriteError(err error) error {
	_, writeErr := fmt.Fprintf(h.stdout, "Error: %v\n", err)
	return writeErr
}

func (h *StandardOutputHandler) WriteCommandList(commands map[string]Command) error {
	if err := h.WriteOutput("Supported commands:"); err != nil {
		return err
	}
	for _, command := range commands {
		if err := h.WriteOutput(command.Help()); err != nil {
			return err
		}
	}
	return nil
}