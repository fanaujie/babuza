package node

import (
	"context"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"math"
	"sort"
	"testing"
	"time"
)

func TestNode_Tick(t *testing.T) {
	n := newNode(nil)
	n.Tick()
	assert.Equal(t, uint64(1), n.tick)
}

func TestNode_Campaign(t *testing.T) {
	n := newNode(nil)
	assert.Nil(t, n.Campaign(context.Background()))
	msgs, _ := n.campaign.Get()
	assert.Equal(t, int(1), len(msgs))
	assert.Equal(t, raftpb.MsgHup, msgs[0])
}

func TestNode_TransferLeadership(t *testing.T) {
	n := newNode(nil)
	n.TransferLeadership(context.Background(), 1, 2)
	msgs, _ := n.transferLeader.Get()
	assert.Equal(t, int(1), len(msgs))
	assert.Equal(t, TransferLeaderMessage{
		Lead:       1,
		Transferee: 2,
	}, msgs[0])
}

func TestNode_ReadIndex(t *testing.T) {
	n := newNode(nil)
	rctx := []byte{1, 2, 3, 4}
	assert.Nil(t, n.ReadIndex(context.Background(), rctx))
	msgs, _ := n.readIndex.Get()
	assert.Equal(t, int(1), len(msgs))
	assert.Equal(t, rctx, msgs[0])
}

func TestNode_ReportUnreachable(t *testing.T) {
	n := newNode(nil)
	n.ReportUnreachable(1)
	msgs, _ := n.unreachable.Get()
	assert.Equal(t, int(1), len(msgs))
	assert.Equal(t, uint64(1), msgs[0])
}

func TestNode_ReportSnapshot(t *testing.T) {
	n := newNode(nil)
	n.ReportSnapshot(1, raft.SnapshotFailure)
	n.ReportSnapshot(2, raft.SnapshotFinish)
	msgs, _ := n.reportSnapshot.Get()
	assert.Equal(t, int(2), len(msgs))
	assert.Equal(t, ReportSnapshotMessage{
		Id:     1,
		Status: raft.SnapshotFailure,
	}, msgs[0])
	assert.Equal(t, ReportSnapshotMessage{
		Id:     2,
		Status: raft.SnapshotFinish,
	}, msgs[1])
}

func TestNode_Step(t *testing.T) {
	n := newNode(nil)
	var sortKey []int
	for i, _ := range raftpb.MessageType_name {
		sortKey = append(sortKey, int(i))

	}
	sort.Ints(sortKey)
	for i := 0; i < len(sortKey); i++ {
		msgt := raftpb.MessageType(i)
		assert.Nil(t, n.Step(context.Background(), raftpb.Message{Type: msgt}))
	}
	msgs, _ := n.step.Get()
	assert.Equal(t, len(msgs), len(sortKey))
	for i := 0; i < len(msgs); i++ {
		msgt := raftpb.MessageType(i)
		assert.Equal(t, raftpb.Message{Type: msgt}, msgs[i])
	}
}

type testReady struct {
	ticker            *time.Ticker
	ms                *raft.MemoryStorage
	n                 *RaftNode
	normalEntries     []raftpb.Entry
	confChangeEntries []raftpb.Entry
	readStates        []raft.ReadState
	stopCh            chan struct{}
	notifyLeaderCh    chan uint64
	leader            uint64
}

func newTestReady(ms *raft.MemoryStorage, n *RaftNode) *testReady {
	return &testReady{
		ticker:         time.NewTicker(100 * time.Millisecond),
		ms:             ms,
		n:              n,
		stopCh:         make(chan struct{}),
		notifyLeaderCh: make(chan uint64),
	}
}
func (tr *testReady) stop() {
	tr.stopCh <- struct{}{}
}
func (tr *testReady) run() {
	for {
		select {
		case <-tr.stopCh:
			return
		case <-tr.ticker.C:
			tr.n.Tick()
		case rd := <-tr.n.Ready():
			if rd.SoftState != nil {
				if rd.SoftState.Lead != tr.leader {
					tr.leader = rd.SoftState.Lead
					tr.notifyLeaderCh <- tr.leader
				}
			}
			if len(rd.ReadStates) > 0 {
				for i := 0; i < len(rd.ReadStates); i++ {
					tr.readStates = append(tr.readStates, rd.ReadStates[i])
				}
			}
			tr.ms.Append(rd.Entries)
			if len(rd.CommittedEntries) > 0 {
				for i := 0; i < len(rd.CommittedEntries); i++ {
					e := &rd.CommittedEntries[i]
					if e.Data == nil {
						continue
					}
					switch e.Type {
					case raftpb.EntryNormal:
						tr.normalEntries = append(tr.normalEntries, *e)
					case raftpb.EntryConfChange:
						var cc raftpb.ConfChange
						cc.Unmarshal(e.Data)
						tr.n.ApplyConfChange(cc)
						tr.confChangeEntries = append(tr.confChangeEntries, *e)
					}
				}
			}
			tr.n.Advance()
		}
	}
}

func TestNode_Propose(t *testing.T) {
	ms := raft.NewMemoryStorage()
	cfg := raft.Config{
		ID:              1,
		ElectionTick:    5,
		HeartbeatTick:   1,
		Storage:         ms,
		MaxSizePerMsg:   math.MaxUint64,
		MaxInflightMsgs: 256,
	}
	n := StartNode(Config{
		Peers:   []raft.Peer{{ID: 1}},
		RaftCfg: &cfg,
	})
	tr := newTestReady(ms, n)
	go tr.run()
	defer func() {
		n.Stop()
		tr.stop()
	}()
	<-tr.notifyLeaderCh
	command := []byte("hello")
	assert.Nil(t, n.Propose(context.Background(), command))
	assert.Nil(t, n.Propose(context.Background(), command))
	time.Sleep(time.Second)
	assert.Equal(t, command, tr.normalEntries[0].Data)
	assert.Equal(t, command, tr.normalEntries[1].Data)
}

func TestNode_ProposeConfChange(t *testing.T) {
	ms := raft.NewMemoryStorage()
	cfg := raft.Config{
		ID:              1,
		ElectionTick:    5,
		HeartbeatTick:   1,
		Storage:         ms,
		MaxSizePerMsg:   math.MaxUint64,
		MaxInflightMsgs: 256,
	}
	n := StartNode(Config{
		Peers:   []raft.Peer{{ID: 1}},
		RaftCfg: &cfg,
	})
	tr := newTestReady(ms, n)
	go tr.run()
	defer func() {
		n.Stop()
		tr.stop()
	}()
	<-tr.notifyLeaderCh
	cc := raftpb.ConfChange{Type: raftpb.ConfChangeAddNode, NodeID: 2}
	confData, err := cc.Marshal()
	assert.Nil(t, err)
	assert.Nil(t, n.ProposeConfChange(context.Background(), cc))
	time.Sleep(time.Second)
	assert.Equal(t, confData, tr.confChangeEntries[1].Data)
}

func TestNode_ReadIndex_Result(t *testing.T) {
	ms := raft.NewMemoryStorage()
	cfg := raft.Config{
		ID:              1,
		ElectionTick:    5,
		HeartbeatTick:   1,
		Storage:         ms,
		MaxSizePerMsg:   math.MaxUint64,
		MaxInflightMsgs: 256,
	}
	n := StartNode(Config{
		Peers:   []raft.Peer{{ID: 1}},
		RaftCfg: &cfg,
	})
	tr := newTestReady(ms, n)
	go tr.run()
	defer func() {
		n.Stop()
		tr.stop()
	}()
	<-tr.notifyLeaderCh
	ctx := []byte("hello")
	assert.Nil(t, n.ReadIndex(context.Background(), ctx))
	time.Sleep(time.Second)
	assert.Equal(t, uint64(2), tr.readStates[0].Index)
	assert.Equal(t, ctx, tr.readStates[0].RequestCtx)
}
func TestNode_Status(t *testing.T) {
	ms := raft.NewMemoryStorage()
	cfg := raft.Config{
		ID:              1,
		ElectionTick:    5,
		HeartbeatTick:   1,
		Storage:         ms,
		MaxSizePerMsg:   math.MaxUint64,
		MaxInflightMsgs: 256,
	}
	n := StartNode(Config{
		Peers:   []raft.Peer{{ID: 1}},
		RaftCfg: &cfg,
	})
	tr := newTestReady(ms, n)
	go tr.run()
	defer func() {
		n.Stop()
		tr.stop()
	}()
	<-tr.notifyLeaderCh
	s := n.Status()
	assert.Equal(t, uint64(1), s.ID)
	assert.Equal(t, uint64(2), s.Term)
	assert.Equal(t, uint64(1), s.Lead)
}
