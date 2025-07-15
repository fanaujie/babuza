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
	"strconv"
)

type RemoveCommand struct {
	kvClient *client.KvStoreClient
}

func NewRemoveCommand(kvClient *client.KvStoreClient) *RemoveCommand {
	return &RemoveCommand{
		kvClient: kvClient,
	}
}

func (rc *RemoveCommand) Execute(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("remove command requires exactly 1 argument: <peerID>")
	}
	if len(args) > 1 {
		return fmt.Errorf("remove command accepts only 1 argument, got %d", len(args))
	}
	
	peerIDStr := args[0]
	if peerIDStr == "" {
		return fmt.Errorf("peerID cannot be empty")
	}
	
	peerID, err := strconv.ParseUint(peerIDStr, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid peerID '%s': must be a valid positive integer", peerIDStr)
	}
	
	err = rc.kvClient.Remove(context.Background(), peerID)
	if err != nil {
		return fmt.Errorf("failed to remove peer %d from cluster: %w", peerID, err)
	}
	
	fmt.Printf("Successfully removed peer %d from cluster\n", peerID)
	return nil
}

func (rc *RemoveCommand) Help() string {
	return "remove <peerID> - Remove a peer from the cluster"
}