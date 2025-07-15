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

type GetCommand struct {
	kvClient *client.KvStoreClient
}

func NewGetCommand(kvClient *client.KvStoreClient) *GetCommand {
	return &GetCommand{
		kvClient: kvClient,
	}
}

func (gc *GetCommand) Execute(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("get command requires exactly 1 argument: <key>")
	}
	if len(args) > 1 {
		return fmt.Errorf("get command accepts only 1 argument, got %d", len(args))
	}
	
	key := args[0]
	if key == "" {
		return fmt.Errorf("key cannot be empty")
	}
	
	res, err := gc.kvClient.Get(context.Background(), key)
	if err != nil {
		return fmt.Errorf("failed to get value for key '%s': %w", key, err)
	}
	
	fmt.Printf("Key: %s\nValue: %s\n", key, res.Value)
	return nil
}

func (gc *GetCommand) Help() string {
	return "get <key> - Get the value of a key"
}
