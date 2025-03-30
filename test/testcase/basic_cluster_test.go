package testcase

import (
	"context"
	"github.com/fanaujie/babuza/examples/kvstore/client"
	"github.com/fanaujie/babuza/examples/kvstore/embedapp"
	"github.com/fanaujie/babuza/test/testcluster"
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
)

type BasicCluster struct {
	t *testing.T
}

func (c *BasicCluster) Log(s string) {
	c.t.Log(s)
}

func (c *BasicCluster) CreateTestComponents() []BabuzaComponent {
	return basicClusterComponents(true)
}

func (c *BasicCluster) Run(tc *testcluster.BabuzaCluster) {
	wait := tc.RaftElectionTimeout() * 3
	peers, connectGroup := makeVotingStandardPeers(3)
	assert.Nil(c.t, tc.MakeCluster(wait, peers))
	_, err := tc.CheckOneLeader(wait, connectGroup.GetIds())
	assert.Nil(c.t, err)
	kvClient, err := embedapp.NewKvStoreClient(tc.GetAllAppServiceAddresses(), client.NewNoOpSession())
	assert.Nil(c.t, err)
	defer func() {
		_ = kvClient.Close()
	}()
	for i := 0; i < 64; i++ {
		s := strconv.Itoa(i)
		assert.Nil(c.t, runWithCtxTimeout(wait, func(ctx context.Context) error {
			_, err = kvClient.Set(ctx, s, s)
			return err
		}))
	}
	assert.Nil(c.t, tc.CheckPeersConsistency(wait, connectGroup.GetIds()))
}

func TestBasicCluster(t *testing.T) {
	RunTests(&BasicCluster{t: t})
}
