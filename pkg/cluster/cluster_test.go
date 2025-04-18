package cluster

import (
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/logger"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"os"
	"testing"
)

func TestCluster_ClusterId(t *testing.T) {
	l := logger.NewRaftLogger(zap.NewNop().Sugar())
	cl := NewCluster(l)
	cl.SetClusterID(1)
	assert.Equal(t, uint64(1), cl.ClusterID())
}

func TestCluster_LocalPeerId(t *testing.T) {
	l := logger.NewRaftLogger(zap.NewNop().Sugar())
	cl := NewCluster(l)
	cl.SetLocalPeerID(1)
	assert.Equal(t, uint64(1), cl.LocalPeerID())
}

func TestCluster_Add(t *testing.T) {
	l := logger.NewRaftLogger(zap.NewNop().Sugar())
	cl := NewCluster(l)
	cl.SetClusterID(1)
	cl.SetLocalPeerID(10)
	assert.Nil(t, cl.Add(babuzapb.RaftPeerAttribute{
		Id:             1,
		RaftListenAddr: "localhost:14200",
	}))
	_, ok := cl.store.Peers[1]
	assert.Equal(t, true, ok)

	// test remove id
	cl.store.RemovedIds[2] = true
	assert.Error(t, cl.Add(babuzapb.RaftPeerAttribute{
		Id:             2,
		RaftListenAddr: "localhost:14200",
	}))
}

func TestCluster_Remove(t *testing.T) {
	l := logger.NewRaftLogger(zap.NewNop().Sugar())
	cl := NewCluster(l)
	cl.SetClusterID(1)
	cl.SetLocalPeerID(10)
	assert.Nil(t, cl.Add(babuzapb.RaftPeerAttribute{
		Id:             1,
		RaftListenAddr: "localhost:14200",
	}))
	assert.Nil(t, cl.Remove(1))
	assert.Error(t, cl.Remove(1))
}

func TestCluster_Update(t *testing.T) {
	l := logger.NewRaftLogger(zap.NewNop().Sugar())
	cl := NewCluster(l)
	cl.SetClusterID(1)
	cl.SetLocalPeerID(10)
	assert.Nil(t, cl.Add(babuzapb.RaftPeerAttribute{
		Id:             1,
		RaftListenAddr: "localhost:14200",
	}))
	assert.Nil(t, cl.Update(babuzapb.RaftPeerAttribute{
		Id:             1,
		RaftListenAddr: "localhost:14201",
	}))
	assert.Equal(t, "localhost:14201", cl.store.Peers[1].RaftPeerAttr.RaftListenAddr)

	assert.Error(t, cl.Update(babuzapb.RaftPeerAttribute{
		Id:             2,
		RaftListenAddr: "localhost:14201",
	}))
}

func TestCluster_Promote(t *testing.T) {
	l := logger.NewRaftLogger(zap.NewNop().Sugar())
	cl := NewCluster(l)
	cl.SetClusterID(1)
	cl.SetLocalPeerID(10)
	assert.Nil(t, cl.Add(babuzapb.RaftPeerAttribute{
		Id:             1,
		RaftListenAddr: "localhost:14200",
		IsLearner:      true,
	}))
	assert.Nil(t, cl.Promote(1))
	assert.Error(t, cl.Promote(1))
	assert.Error(t, cl.Promote(10))
}

func TestCluster_Peers(t *testing.T) {
	l := logger.NewRaftLogger(zap.NewNop().Sugar())
	cl := NewCluster(l)
	cl.SetClusterID(1)
	cl.SetLocalPeerID(10)
	assert.Nil(t, cl.Add(babuzapb.RaftPeerAttribute{
		Id:             1,
		RaftListenAddr: "localhost:14200",
	}))
	assert.Nil(t, cl.Add(babuzapb.RaftPeerAttribute{
		Id:             2,
		RaftListenAddr: "localhost:14201",
		IsLearner:      true,
	}))

	result := cl.Peers()
	assert.Equal(t, 2, len(result))

	assert.Equal(t, uint64(1), result[0].RaftPeerAttr.Id)
	assert.Equal(t, uint64(2), result[1].RaftPeerAttr.Id)
	assert.Equal(t, "localhost:14200", result[0].RaftPeerAttr.RaftListenAddr)
	assert.Equal(t, "localhost:14201", result[1].RaftPeerAttr.RaftListenAddr)
	assert.Equal(t, false, result[0].RaftPeerAttr.IsLearner)
	assert.Equal(t, true, result[1].RaftPeerAttr.IsLearner)

}

func TestCluster_SnapshotRestore(t *testing.T) {
	l := logger.NewRaftLogger(zap.NewNop().Sugar())
	cl := NewCluster(l)
	cl.SetClusterID(1)
	cl.SetLocalPeerID(10)
	assert.Nil(t, cl.Add(babuzapb.RaftPeerAttribute{
		Id:             1,
		RaftListenAddr: "localhost:14200",
	}))
	assert.Nil(t, cl.Add(babuzapb.RaftPeerAttribute{
		Id:             2,
		RaftListenAddr: "localhost:14201",
		IsLearner:      true,
	}))
	assert.Nil(t, cl.Remove(2))
	assert.Nil(t, cl.Add(babuzapb.RaftPeerAttribute{
		Id:             3,
		RaftListenAddr: "localhost:14202",
	}))

	p, err := os.CreateTemp("", "Cluster")
	assert.Nil(t, err)
	defer os.RemoveAll(p.Name())
	assert.Nil(t, cl.Snapshot(p))
	assert.Nil(t, p.Close())
	p, err = os.Open(p.Name())
	assert.Nil(t, err)
	defer p.Close()
	resCl := NewCluster(l)
	assert.Nil(t, resCl.Restore(p))
	resCl.SetLocalPeerID(cl.LocalPeerID())
	assert.Equal(t, cl, resCl)

}

func TestCluster_Apply(t *testing.T) {
	l := logger.NewRaftLogger(zap.NewNop().Sugar())
	cl := NewCluster(l)
	cl.SetClusterID(1)
	cl.SetLocalPeerID(3)
	assert.Nil(t, cl.Add(babuzapb.RaftPeerAttribute{
		Id:             1,
		RaftListenAddr: "localhost:14200",
	}))
	assert.Nil(t, cl.Add(babuzapb.RaftPeerAttribute{
		Id:             2,
		RaftListenAddr: "localhost:14201",
		IsLearner:      true,
	}))
	assert.Nil(t, cl.Remove(2))
	assert.Nil(t, cl.Add(babuzapb.RaftPeerAttribute{
		Id:             3,
		RaftListenAddr: "localhost:14202",
	}))
	// Cluster: peers [1,3], remove[2]
	assert.Nil(t, cl.Add(babuzapb.RaftPeerAttribute{
		Id:             4,
		RaftListenAddr: "localhost:14204",
	}))
	// Cluster: peers [1,3,4], remove[2]
	// id exists
	assert.Error(t, cl.Add(babuzapb.RaftPeerAttribute{
		Id:             1,
		RaftListenAddr: "localhost:14200",
	}))
	//endpoint exists
	assert.Error(t, cl.Add(babuzapb.RaftPeerAttribute{
		Id:             5,
		RaftListenAddr: "localhost:14200",
	}))

	//add learner
	// Cluster: peers [1,3,4], remove[2], learner[10]
	assert.Nil(t, cl.Add(babuzapb.RaftPeerAttribute{
		Id:             10,
		RaftListenAddr: "localhost:14210",
		IsLearner:      true,
	}))

	//promote learner
	assert.Nil(t, cl.Promote(10))
	// Cluster: peers [1,3,4,10], remove[2]
	// learner does not exist
	assert.Error(t, cl.Promote(11))
	// not learner
	assert.Error(t, cl.Promote(1))
	assert.Nil(t, cl.Remove(1))
	// Cluster: peers [3,4,10], remove[1,2]
	// id does not found
	assert.Error(t, cl.Remove(100))

	// id has been removed
	assert.Error(t, cl.Remove(1))

	assert.Nil(t, cl.Update(babuzapb.RaftPeerAttribute{
		Id:             3,
		RaftListenAddr: "localhost:14203",
	}))

	// id does not found
	assert.Error(t, cl.Update(babuzapb.RaftPeerAttribute{
		Id:             2,
		RaftListenAddr: "localhost:14203",
	}))
}
