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
	"io"
	"strings"
)

type InputHandler interface {
	ReadInput() (string, error)
}

type StdinInputHandler struct {
	reader *bufio.Reader
}

func NewStdinInputHandler(input io.Reader) InputHandler {
	return &StdinInputHandler{
		reader: bufio.NewReader(input),
	}
}

func (h *StdinInputHandler) ReadInput() (string, error) {
	input, err := h.reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(input), nil
}