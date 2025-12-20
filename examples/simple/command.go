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

package main

import "encoding/json"

const (
	CmdSet uint8 = 1
)

type Command struct {
	Type  uint8  `json:"type"`
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

func NewSetCommand(key, value string) *Command {
	return &Command{
		Type:  CmdSet,
		Key:   key,
		Value: value,
	}
}

func (c *Command) Encode() ([]byte, error) {
	return json.Marshal(c)
}

func DecodeCommand(data []byte) (*Command, error) {
	var cmd Command
	if err := json.Unmarshal(data, &cmd); err != nil {
		return nil, err
	}
	return &cmd, nil
}
