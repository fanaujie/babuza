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
	"strings"
)

type ParsedCommand struct {
	Name string
	Args []string
}

type CommandParser interface {
	Parse(input string) (*ParsedCommand, error)
}

type DefaultCommandParser struct{}

func NewDefaultCommandParser() CommandParser {
	return &DefaultCommandParser{}
}

func (p *DefaultCommandParser) Parse(input string) (*ParsedCommand, error) {
	if strings.TrimSpace(input) == "" {
		return nil, fmt.Errorf("empty command")
	}
	
	parts := strings.Fields(input)
	if len(parts) == 0 {
		return nil, fmt.Errorf("empty command")
	}
	
	return &ParsedCommand{
		Name: parts[0],
		Args: parts[1:],
	}, nil
}