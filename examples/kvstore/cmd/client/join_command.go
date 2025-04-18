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
