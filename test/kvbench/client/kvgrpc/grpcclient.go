package kvgrpc

import (
	"context"
	"fmt"
	"google.golang.org/grpc"
	"time"

	"github.com/fanaujie/babuza/test/kvbench/client"
	"github.com/fanaujie/babuza/test/kvbench/kvbenchpb"
)

// Client implements the client.Client interface using KVServiceClient
type Client struct {
	shardCount      uint
	targetLeader    bool
	factory         *Factory
	memberConnAddr  []string
	nextMemberIndex int
}

func NewGRPCClientWithRouting(config client.Config, factory *Factory) *Client {

	memberConnAddr := make([]string, 0)
	factory.nodeClientMap.Range(func(addr string, clients []*grpc.ClientConn) bool {
		if len(clients) > 0 {
			memberConnAddr = append(memberConnAddr, addr)
		}
		return true
	})

	return &Client{
		shardCount:      config.ShardCount,
		targetLeader:    config.TargetLeader,
		factory:         factory,
		memberConnAddr:  memberConnAddr,
		nextMemberIndex: 0,
	}
}

func (c *Client) getClientForGroup(groupID uint64) (*grpc.ClientConn, error) {
	var addr string

	if c.targetLeader {
		v, ok := c.factory.groupLeaderMap.Load(groupID)
		if !ok {
			return nil, fmt.Errorf("group %d not found", groupID)
		}
		addr = v
	} else {
		addr = c.memberConnAddr[c.nextMemberIndex]
		c.nextMemberIndex = (c.nextMemberIndex + 1) % len(c.memberConnAddr)
	}
	if addr == "" {
		return nil, nil
	}

	return c.factory.getConnectionForClient(addr)
}

// Put puts a key-value pair into the store
func (c *Client) Put(ctx context.Context, groupID uint64, key, value []byte) client.Response {
	conn, err := c.getClientForGroup(groupID)
	if err != nil {
		return client.Response{Error: err, EndTime: time.Now()}
	}
	defer c.factory.releaseConnection(conn)
	req := &kvbenchpb.PutRequest{
		GroupID: groupID,
		Key:     key,
		Value:   value,
	}
	startTime := time.Now()
	_, err = kvbenchpb.NewKVServiceClient(conn).Put(ctx, req)
	endTime := time.Now()
	return client.Response{
		Error:     err,
		StartTime: startTime,
		EndTime:   endTime,
	}
}

// Get retrieves a value for the given key
func (c *Client) Get(ctx context.Context, groupID uint64, key []byte) client.Response {
	conn, err := c.getClientForGroup(groupID)
	if err != nil {
		return client.Response{Error: err, EndTime: time.Now()}
	}
	defer c.factory.releaseConnection(conn)
	req := &kvbenchpb.GetRequest{
		GroupID: groupID,
		Key:     key,
	}
	_, err = kvbenchpb.NewKVServiceClient(conn).Get(ctx, req)
	endTime := time.Now()
	return client.Response{
		Error:   err,
		EndTime: endTime,
	}
}

// Delete removes a key-value pair
func (c *Client) Delete(ctx context.Context, groupID uint64, key []byte) client.Response {
	conn, err := c.getClientForGroup(groupID)
	if err != nil {
		return client.Response{Error: err, EndTime: time.Now()}
	}
	req := &kvbenchpb.DeleteRequest{
		GroupID: groupID,
		Key:     key,
	}
	_, err = kvbenchpb.NewKVServiceClient(conn).Delete(ctx, req)
	endTime := time.Now()
	return client.Response{
		Error:   err,
		EndTime: endTime,
	}
}

// Close closes the client connection
func (c *Client) Close() error {
	return nil
}
