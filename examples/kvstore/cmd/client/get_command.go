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
		return fmt.Errorf("error: get command requires 1 argument: key")
	}
	key := args[0]
	res, err := gc.kvClient.Get(context.Background(), key)
	if err != nil {
		return err
	} else {
		fmt.Println("Value:", res.Value)
	}
	return nil
}

func (gc *GetCommand) Help() string {
	return "get <key> - Get the value of a key"
}
