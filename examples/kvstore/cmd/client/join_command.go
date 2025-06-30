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

type JoinCommand struct {
	kvClient *client.KvStoreClient
}

func NewJoinCommand(kvClient *client.KvStoreClient) *JoinCommand {
	return &JoinCommand{
		kvClient: kvClient,
	}
}

func (jc *JoinCommand) Execute(args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("error: join command requires 2 arguments: peerID and raftListenAddr")
	}
	peerID, err := strconv.ParseUint(args[0], 10, 64)
	if err != nil {
		return fmt.Errorf("error: invalid peerID")
	}
	raftListenAddr := args[1]
	err = jc.kvClient.Join(context.Background(), peerID, raftListenAddr, false)
	if err != nil {
		return fmt.Errorf("error: failed to join %v", err)
	} else {
		fmt.Println("Successfully joined")
	}
	return nil
}

func (jc *JoinCommand) Help() string {
	return "join <peerID> <raftListenAddr> - Join a new peer to the cluster"
}
