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
