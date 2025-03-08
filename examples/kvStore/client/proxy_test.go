package client

import (
	"github.com/stretchr/testify/assert"
	"net/url"
	"testing"
)

func TestNewGateway(t *testing.T) {
	gw := newProxy(false, nil)
	assert.Equal(t, "http", gw.httpScheme)
	gw = newProxy(true, nil)
	assert.Equal(t, "https", gw.httpScheme)
	assert.Equal(t, 0, gw.leaderIndex)
	assert.NotNil(t, gw.httpClient)
}

func TestGateway_CurrentLeaderUrl(t *testing.T) {
	t.Run("enable tls", func(t *testing.T) {
		gw := newProxy(true, []ClusterPeer{
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
		gw := newProxy(false, []ClusterPeer{
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
		gw := newProxy(false, nil)
		_, err := gw.currentLeaderUrl()
		assert.ErrorIs(t, err, errNoPeers)
		gw = newProxy(false, []ClusterPeer{
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

func TestGateway_MoveNextLeader(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		gw := newProxy(false, []ClusterPeer{
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
		gw := newProxy(false, nil)
		assert.ErrorIs(t, gw.moveNextLeader(), errNoPeers)
	})

}

func TestGateway_UpdateLeaderIndex(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		gw := newProxy(false, []ClusterPeer{
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
		gw := newProxy(false, []ClusterPeer{
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
