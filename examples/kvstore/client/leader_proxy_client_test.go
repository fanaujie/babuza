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
	"github.com/stretchr/testify/assert"
	"net/url"
	"testing"
)

func TestNewLeaderProxyClient(t *testing.T) {
	gw := newLeaderProxyClient(false, nil)
	assert.Equal(t, "http", gw.httpScheme)
	gw = newLeaderProxyClient(true, nil)
	assert.Equal(t, "https", gw.httpScheme)
	assert.Equal(t, 0, gw.leaderIndex)
	assert.NotNil(t, gw.httpClient)
}

func TestLeaderProxyClient_CurrentLeaderUrl(t *testing.T) {
	t.Run("enable tls", func(t *testing.T) {
		gw := newLeaderProxyClient(true, []ClusterPeer{
			{
				Id:               1,
				KvServiceAddress: "localhost:1001",
			},
			{
				Id:               2,
				KvServiceAddress: "localhost:1002",
			},
		})
		leaderUrl, err := gw.currentLeaderUrl()
		assert.Nil(t, err)
		assert.Equal(t, "localhost:1001", leaderUrl.Host)
		assert.Equal(t, "https", leaderUrl.Scheme)
	})
	t.Run("disable tls", func(t *testing.T) {
		gw := newLeaderProxyClient(false, []ClusterPeer{
			{
				Id:               1,
				KvServiceAddress: "localhost:1001",
			},
			{
				Id:               2,
				KvServiceAddress: "localhost:1002",
			},
		})
		leaderUrl, err := gw.currentLeaderUrl()
		assert.Nil(t, err)
		assert.Equal(t, "localhost:1001", leaderUrl.Host)
		assert.Equal(t, "http", leaderUrl.Scheme)
	})
	t.Run("failure", func(t *testing.T) {
		gw := newLeaderProxyClient(false, nil)
		_, err := gw.currentLeaderUrl()
		assert.ErrorIs(t, err, errNoPeers)
		gw = newLeaderProxyClient(false, []ClusterPeer{
			{
				Id:               1,
				KvServiceAddress: "localhost:1001",
			},
			{
				Id:               2,
				KvServiceAddress: "localhost:1002",
			},
		})
		gw.leaderIndex = 2
		_, err = gw.currentLeaderUrl()
		assert.ErrorIs(t, err, errInvalidLeaderIndex)
		gw.leaderIndex = 3
		_, err = gw.currentLeaderUrl()
		assert.ErrorIs(t, err, errInvalidLeaderIndex)
	})
}

func TestLeaderProxyClient_MoveNextLeader(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		gw := newLeaderProxyClient(false, []ClusterPeer{
			{
				Id:               1,
				KvServiceAddress: "localhost:1001",
			},
			{
				Id:               2,
				KvServiceAddress: "localhost:1002",
			},
		})

		assert.Nil(t, gw.moveNextLeader())
		assert.Equal(t, 1, gw.leaderIndex)
		assert.Nil(t, gw.moveNextLeader())
		assert.Equal(t, 0, gw.leaderIndex)
	})

	t.Run("failure", func(t *testing.T) {
		gw := newLeaderProxyClient(false, nil)
		assert.ErrorIs(t, gw.moveNextLeader(), errNoPeers)
	})

}

func TestLeaderProxyClient_UpdateLeaderIndex(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		gw := newLeaderProxyClient(false, []ClusterPeer{
			{
				Id:               1,
				KvServiceAddress: "localhost:1001",
			},
			{
				Id:               2,
				KvServiceAddress: "localhost:1002",
			},
		})
		newLeaderUrl := url.URL{Scheme: "http", Host: "localhost:1002"}
		assert.Nil(t, gw.updateLeaderIndex(&newLeaderUrl))
		assert.Equal(t, 1, gw.leaderIndex)
		le, err := gw.currentLeaderUrl()
		assert.Nil(t, err)
		assert.Equal(t, le.Host, "localhost:1002")
		newLeaderUrl = url.URL{Scheme: "http", Host: "localhost:1001"}
		assert.Nil(t, gw.updateLeaderIndex(&newLeaderUrl))
		le, err = gw.currentLeaderUrl()
		assert.Nil(t, err)
		assert.Equal(t, le.Host, "localhost:1001")
		assert.Equal(t, 0, gw.leaderIndex)

	})

	t.Run("failure", func(t *testing.T) {
		gw := newLeaderProxyClient(false, []ClusterPeer{
			{
				Id:               1,
				KvServiceAddress: "localhost:1001",
			},
			{
				Id:               2,
				KvServiceAddress: "localhost:1002",
			},
		})
		gw.leaderIndex = 1
		assert.ErrorIs(t, gw.updateLeaderIndex(nil), errNewLeaderUrlNil)
		assert.Equal(t, 1, gw.leaderIndex)
		newLeaderUrl := url.URL{Scheme: "http", Host: "localhost:8080"}
		assert.ErrorIs(t, gw.updateLeaderIndex(&newLeaderUrl), errLeaderIndexNotFoundMatch)
		assert.Equal(t, 1, gw.leaderIndex)
	})
}
