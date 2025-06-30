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

type SetCommand struct {
	kvClient *client.KvStoreClient
}

func NewSetCommand(kvClient *client.KvStoreClient) *SetCommand {
	return &SetCommand{
		kvClient: kvClient,
	}
}

func (sc *SetCommand) Execute(args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("error: set command requires 2 arguments: key and value")
	}
	key := args[0]
	value := args[1]
	_, err := sc.kvClient.Set(context.Background(), key, value)
	if err != nil {
		return fmt.Errorf("error: failed to set %v", err)
	} else {
		fmt.Println("Successfully set")
	}
	return nil
}

func (sc *SetCommand) Help() string {
	return "set <key> <value> - Set a new key-value pair"
}
