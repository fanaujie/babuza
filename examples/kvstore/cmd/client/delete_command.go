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

type DeleteCommand struct {
	kvClient *client.KvStoreClient
}

func NewDeleteCommand(kvClient *client.KvStoreClient) *DeleteCommand {
	return &DeleteCommand{
		kvClient: kvClient,
	}
}

func (dc *DeleteCommand) Execute(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("delete command requires exactly 1 argument: <key>")
	}
	if len(args) > 1 {
		return fmt.Errorf("delete command accepts only 1 argument, got %d", len(args))
	}
	
	key := args[0]
	if key == "" {
		return fmt.Errorf("key cannot be empty")
	}
	
	res, err := dc.kvClient.Delete(context.Background(), key)
	if err != nil {
		return fmt.Errorf("failed to delete key '%s': %w", key, err)
	}
	
	fmt.Printf("Key '%s' deleted successfully\n", key)
	fmt.Printf("Session ID: %d, Sequence Number: %d\n", res.SessionID, res.SequenceNumber)
	return nil
}

func (dc *DeleteCommand) Help() string {
	return "delete <key> - Delete a key from the store"
}