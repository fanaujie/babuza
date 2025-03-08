package cmdprocessor

import (
	"context"
	"fmt"
	"github.com/fanaujie/babuza/examples/kvStore/client"
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
