package raft

import (
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/pkg/cluster"
	"github.com/fanaujie/babuza/pkg/logger"
	"github.com/fanaujie/babuza/pkg/status"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.uber.org/zap"
	"testing"
	"time"
)

func TestRaft_Ready(t *testing.T) {
	localPeerID := uint64(1)
	etcdRaftNode := &mockRaftNode{readyCh: make(chan raft.Ready)}
	tr := newTestRaft(localPeerID)
	tr.raftNode = etcdRaftNode
	tr.storage = &mockStorageMgr{}
	tr.status = status.New()
	tr.closer.Run(func() {
		tr.processRaftReady()
	})
	defer tr.closer.Close()

	t.Run("apply chan", func(t *testing.T) {
		etcdRaftNode.readyCh <- raft.Ready{
			CommittedEntries: []raftpb.Entry{
				{},
			},
		}
		select {
		case <-time.After(time.Second):
			assert.Fail(t, "wait time limit exceeded")
		case <-tr.applyCh:
		}
	})

	t.Run("read state chan", func(t *testing.T) {
		etcdRaftNode.readyCh <- raft.Ready{
			ReadStates: []raft.ReadState{{}},
		}
		select {
		case <-time.After(time.Second):
			assert.Fail(t, "wait time limit exceeded")
		case <-tr.readStateCh:
		}
	})
}

func TestRaft_SendRaftMessage(t *testing.T) {
	localPeerID := uint64(1)
	raftNode := &mockRaftNode{readyCh: make(chan raft.Ready)}
	tr := newTestRaft(localPeerID)
	tr.raftNode = raftNode
	trans := &mockTransport{}
	tr.trans = trans
	tr.storage = &mockStorageMgr{}
	tr.status = status.New()
	tr.cluster = cluster.NewCluster(&logger.Mock{})
	tr.cluster.SetLocalPeerID(localPeerID)
	tr.logger = logger.NewRaftLogger(zap.NewExample().Sugar())
	tr.closer.Run(func() {
		tr.processRaftReady()
	})
	defer tr.closer.Close()
	for _, tc := range []struct {
		isLeader        bool
		sendRaftMsg     []raftpb.Message
		expectedRaftMsg []raftpb.Message
		expectedSnapMsg []raftpb.Message
	}{
		{
			isLeader:        true,
			sendRaftMsg:     []raftpb.Message{{Type: raftpb.MsgApp, Index: 1}, {Type: raftpb.MsgApp, Index: 2}, {Type: raftpb.MsgApp, Index: 3}},
			expectedRaftMsg: []raftpb.Message{{Type: raftpb.MsgApp, Index: 1}, {Type: raftpb.MsgApp, Index: 2}, {Type: raftpb.MsgApp, Index: 3}},
			expectedSnapMsg: nil,
		},
		{
			isLeader:        true,
			sendRaftMsg:     []raftpb.Message{{Type: raftpb.MsgApp, Index: 1}, {Type: raftpb.MsgSnap, Index: 2}, {Type: raftpb.MsgSnap, Index: 3}},
			expectedRaftMsg: []raftpb.Message{{Type: raftpb.MsgApp, Index: 1}},
			expectedSnapMsg: []raftpb.Message{{Type: raftpb.MsgSnap, Index: 2}, {Type: raftpb.MsgSnap, Index: 3}},
		},
		{
			isLeader: false,
			sendRaftMsg: []raftpb.Message{
				{Type: raftpb.MsgAppResp, Index: 1}, {Type: raftpb.MsgHeartbeatResp}, {Type: raftpb.MsgAppResp, Index: 2},
				{Type: raftpb.MsgAppResp, Index: 3}, {Type: raftpb.MsgHeartbeatResp},
			},
			expectedRaftMsg: []raftpb.Message{{Type: raftpb.MsgHeartbeatResp}, {Type: raftpb.MsgHeartbeatResp}, {Type: raftpb.MsgAppResp, Index: 3}},
			expectedSnapMsg: nil,
		},
		{
			isLeader: false,
			sendRaftMsg: []raftpb.Message{
				{Type: raftpb.MsgAppResp, Index: 1}, {Type: raftpb.MsgAppResp, Index: 2, Reject: true}, {Type: raftpb.MsgHeartbeatResp},
			},
			expectedRaftMsg: []raftpb.Message{{Type: raftpb.MsgAppResp, Index: 2, Reject: true}, {Type: raftpb.MsgHeartbeatResp}, {Type: raftpb.MsgAppResp, Index: 1}},
			expectedSnapMsg: nil,
		},
	} {
		if tc.isLeader {
			raftNode.readyCh <- raft.Ready{
				SoftState: &raft.SoftState{
					Lead:      localPeerID,
					RaftState: raft.StateLeader,
				},
			}

		} else {
			raftNode.readyCh <- raft.Ready{
				SoftState: &raft.SoftState{
					Lead:      localPeerID + 1,
					RaftState: raft.StateFollower,
				},
			}
		}
		raftNode.readyCh <- raft.Ready{
			Messages: tc.sendRaftMsg,
		}
		time.Sleep(time.Millisecond * 300)
		assert.Equal(t, tc.expectedRaftMsg, trans.send)
		assert.Equal(t, tc.expectedSnapMsg, trans.snap)
		trans.send = nil
		trans.snap = nil
	}

}

func TestRaft_SaveStorage(t *testing.T) {
	localPeerID := uint64(1)
	etcdRaftNode := &mockRaftNode{readyCh: make(chan raft.Ready)}
	tr := newTestRaft(localPeerID)
	tr.raftNode = etcdRaftNode
	storage := &mockStorageMgr{}
	tr.storage = storage
	tr.status = status.New()
	tr.closer.Run(func() {
		tr.processRaftReady()
	})

	defer tr.closer.Close()
	for _, tc := range []struct {
		saveEntries []raftpb.Entry
		snapshot    raftpb.Snapshot
	}{
		{
			saveEntries: []raftpb.Entry{
				{
					Term:  1,
					Index: 1,
					Type:  raftpb.EntryNormal,
					Data:  []byte{1},
				},
			},
		},
		{
			saveEntries: []raftpb.Entry{
				{
					Term:  1,
					Index: 1,
					Type:  raftpb.EntryNormal,
					Data:  []byte{1},
				},
			},
			snapshot: raftpb.Snapshot{
				Data: nil,
				Metadata: raftpb.SnapshotMetadata{
					Index: 1,
					Term:  1,
				},
			},
		},
	} {

		etcdRaftNode.readyCh <- raft.Ready{
			Entries:  tc.saveEntries,
			Snapshot: tc.snapshot,
		}
		if !raft.IsEmptySnap(tc.snapshot) {
			<-tr.applyCh
		}
		time.Sleep(time.Millisecond * 300)
		assert.Equal(t, tc.saveEntries, storage.entries)
		assert.Equal(t, storage.entryStorage.appendEntries, storage.entries)
		assert.Equal(t, tc.snapshot, storage.snap)
		if !raft.IsEmptySnap(tc.snapshot) {
			assert.Equal(t, storage.snap, storage.entryStorage.applySnap)
			assert.Equal(t, storage.snap, storage.releaseSnap)
		}
	}
}

func TestRaft_UpdateLeaderShip(t *testing.T) {

	localPeerID := uint64(1)
	tr := newTestRaft(localPeerID)
	tr.status = status.New()
	tr.cluster = cluster.NewCluster(&logger.Mock{})
	tr.cluster.SetLocalPeerID(localPeerID)
	tr.raftListener = &mockRaftListener{
		leaderIDs: make(map[uint64]uint64),
	}
	tr.raftEventPublisher = newRaftEventPublisher()
	tr.updateLeadership(raft.SoftState{
		Lead:      raft.None,
		RaftState: raft.StateFollower,
	})
	assert.Equal(t, 0, len(tr.raftEventPublisher.ch))

	tr.updateLeadership(raft.SoftState{
		Lead:      raft.None,
		RaftState: raft.StatePreCandidate,
	})
	assert.Equal(t, 0, len(tr.raftEventPublisher.ch))

	tr.updateLeadership(raft.SoftState{
		Lead:      raft.None,
		RaftState: raft.StateCandidate,
	})
	assert.Equal(t, 0, len(tr.raftEventPublisher.ch))

	//acquire leadership
	tr.updateLeadership(raft.SoftState{
		Lead:      1,
		RaftState: raft.StateLeader,
	})
	assert.Equal(t, 2, len(tr.raftEventPublisher.ch))
	event := <-tr.raftEventPublisher.ch
	assert.Equal(t, ibabuza.AcquiredLeader, event.Event)
	assert.Equal(t, uint64(1), event.PeerID)
	assert.Equal(t, uint64(1), tr.getLeaderId())
	event = <-tr.raftEventPublisher.ch
	assert.Equal(t, ibabuza.LeaderChanged, event.Event)
	assert.Equal(t, uint64(1), event.PeerID)
	assert.Equal(t, uint64(1), tr.getLeaderId())

	//lose leadership
	tr.updateLeadership(raft.SoftState{
		Lead:      2,
		RaftState: raft.StateFollower,
	})
	assert.Equal(t, 2, len(tr.raftEventPublisher.ch))
	event = <-tr.raftEventPublisher.ch
	assert.Equal(t, ibabuza.LostLeader, event.Event)
	assert.Equal(t, uint64(1), event.PeerID)

	event = <-tr.raftEventPublisher.ch
	assert.Equal(t, ibabuza.LeaderChanged, event.Event)
	assert.Equal(t, uint64(2), event.PeerID)

	tr.updateLeadership(raft.SoftState{
		Lead:      raft.None,
		RaftState: raft.StateCandidate,
	})
	assert.Equal(t, 0, len(tr.raftEventPublisher.ch))

	tr.updateLeadership(raft.SoftState{
		Lead:      1,
		RaftState: raft.StateLeader,
	})
	assert.Equal(t, 2, len(tr.raftEventPublisher.ch))
	event = <-tr.raftEventPublisher.ch
	assert.Equal(t, ibabuza.AcquiredLeader, event.Event)
	assert.Equal(t, uint64(1), event.PeerID)
	assert.Equal(t, uint64(1), tr.getLeaderId())
	event = <-tr.raftEventPublisher.ch
	assert.Equal(t, ibabuza.LeaderChanged, event.Event)
	assert.Equal(t, uint64(1), event.PeerID)
	assert.Equal(t, uint64(1), tr.getLeaderId())
}

func TestRaft_LeadershipNotify(t *testing.T) {
	localPeerID := uint64(1)
	raftNode := &mockRaftNode{readyCh: make(chan raft.Ready)}
	tr := newTestRaft(localPeerID)
	tr.raftNode = raftNode
	tr.storage = &mockStorageMgr{}
	tr.status = status.New()
	tr.cluster = cluster.NewCluster(&logger.Mock{})
	tr.cluster.SetLocalPeerID(localPeerID)
	tr.closer.Run(func() {
		tr.processRaftReady()
	})
	mockListener := &mockRaftListener{
		leaderIDs: make(map[uint64]uint64),
	}
	tr.raftListener = mockListener
	tr.raftEventPublisher = newRaftEventPublisher()
	tr.closer.Run(func() {
		tr.handleListenerEvent()
	})
	defer tr.closer.Close()
	for _, tc := range []struct {
		ready    raft.Ready
		isLeader bool
	}{
		{
			ready: raft.Ready{
				SoftState: &raft.SoftState{
					Lead:      localPeerID,
					RaftState: raft.StateLeader,
				},
			},
			isLeader: true,
		},
		{
			ready: raft.Ready{
				SoftState: &raft.SoftState{
					Lead:      localPeerID + 1,
					RaftState: raft.StateFollower,
				},
			},
			isLeader: false,
		},
		{
			ready: raft.Ready{
				SoftState: &raft.SoftState{
					Lead:      raft.None,
					RaftState: raft.StatePreCandidate,
				},
			},
			isLeader: false,
		},
		{
			ready: raft.Ready{
				SoftState: &raft.SoftState{
					Lead:      localPeerID,
					RaftState: raft.StateLeader,
				},
			},
			isLeader: true,
		},
	} {
		raftNode.readyCh <- tc.ready
		<-time.After(time.Second)
		assert.Equal(t, tc.isLeader, mockListener.leaderIDs[0] == localPeerID)
	}
}
