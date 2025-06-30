package dummy

import (
	"context"
	"github.com/fanaujie/babuza/test/kvbench/client"
	"math/rand"
	"time"
)

type Client struct {
	maxDelay time.Duration
	rnd      *rand.Rand
}

func NewDummyClient(maxDelay time.Duration) *Client {
	return &Client{
		maxDelay: maxDelay,
		rnd:      rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (c *Client) randomDelay() {
	if c.maxDelay > 0 {
		delay := time.Duration(c.rnd.Int63n(int64(c.maxDelay)))
		time.Sleep(delay)
	}
}

func (c *Client) execOperation(ctx context.Context) client.Response {
	select {
	case <-ctx.Done():
		return client.Response{
			Error:   ctx.Err(),
			EndTime: time.Now(),
		}
	default:
	}

	c.randomDelay()

	select {
	case <-ctx.Done():
		return client.Response{
			Error:   ctx.Err(),
			EndTime: time.Now(),
		}
	default:
	}

	return client.Response{
		EndTime: time.Now(),
	}
}

func (c *Client) Put(ctx context.Context, groupID uint64, key, value []byte) client.Response {
	return c.execOperation(ctx)
}

func (c *Client) Get(ctx context.Context, groupID uint64, key []byte) client.Response {
	return c.execOperation(ctx)
}

func (c *Client) Delete(ctx context.Context, groupID uint64, key []byte) client.Response {
	return c.execOperation(ctx)
}

func (c *Client) Close() error {
	return nil
}
