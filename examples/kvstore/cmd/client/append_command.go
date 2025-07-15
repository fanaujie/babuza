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
	"context"
	"fmt"
	"github.com/fanaujie/babuza/examples/kvstore/client"
)

type AppendCommand struct {
	kvClient *client.KvStoreClient
}

func NewAppendCommand(kvClient *client.KvStoreClient) *AppendCommand {
	return &AppendCommand{
		kvClient: kvClient,
	}
}

func (ac *AppendCommand) Execute(args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("append command requires exactly 2 arguments: <key> <value>")
	}
	if len(args) > 2 {
		return fmt.Errorf("append command accepts only 2 arguments, got %d", len(args))
	}
	
	key := args[0]
	value := args[1]
	
	if key == "" {
		return fmt.Errorf("key cannot be empty")
	}
	
	res, err := ac.kvClient.Append(context.Background(), key, value)
	if err != nil {
		return fmt.Errorf("failed to append value to key '%s': %w", key, err)
	}
	
	fmt.Printf("Value appended to key '%s' successfully\n", key)
	fmt.Printf("New value: %s\n", res.Value)
	fmt.Printf("Session ID: %d, Sequence Number: %d\n", res.SessionID, res.SequenceNumber)
	return nil
}

func (ac *AppendCommand) Help() string {
	return "append <key> <value> - Append value to a key (creates key if it doesn't exist)"
}