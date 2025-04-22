package raft

import (
	"github.com/fanaujie/babuza/pkg/logger"
	"github.com/fanaujie/babuza/pkg/replier"
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

func TestRaft_Ready_FollowerWaitConfigChanged(t *testing.T) {
	localPeerID := uint64(1)
	etcdRaftNode := &mockRaftNode{readyCh: make(chan raft.Ready)}
	tr := newTestRaft(localPeerID)
	tr.raftNode = etcdRaftNode
	trans := &mockTransport{}
	tr.trans = trans
	tr.storage = &mockStorageMgr{}
	tr.status = status.New()
	tr.completionReplier = replier.NewCompletion()
	tr.closer.Run(func() {
		tr.processRaftReady()
	})

	defer tr.closer.Close()

	t.Run("follower: waiting for votingPeersCfg changed ", func(t *testing.T) {
		etcdRaftNode.readyCh <- raft.Ready{
			SoftState: &raft.SoftState{
				Lead:      localPeerID + 1,
				RaftState: raft.StateFollower,
			},
		}

		etcdRaftNode.readyCh <- raft.Ready{
			CommittedEntries: []raftpb.Entry{
				{
					Term:  1,
					Index: 5,
					Type:  raftpb.EntryConfChange,
					Data:  nil,
				},
				{
					Term:  1,
					Index: 6,
					Type:  raftpb.EntryConfChange,
					Data:  nil,
				},
			},
			Messages: []raftpb.Message{
				{
					Type: raftpb.MsgHeartbeatResp,
				},
			},
		}

		go func() {
			a := <-tr.applyCh
			lastApplyIndex := uint64(0)
			for _, e := range a.entries {
				lastApplyIndex = e.Index
			}
			tr.completionReplier.MarkCompleted(lastApplyIndex)
		}()
		<-time.After(time.Second)
		assert.Equal(t, raftpb.MsgHeartbeatResp, trans.send[0].Type)
	})

	t.Run("follower: no votingPeersCfg changed", func(t *testing.T) {
		etcdRaftNode.readyCh <- raft.Ready{
			SoftState: &raft.SoftState{
				Lead:      localPeerID + 1,
				RaftState: raft.StateFollower,
			},
		}

		etcdRaftNode.readyCh <- raft.Ready{
			CommittedEntries: []raftpb.Entry{
				{
					Term:  1,
					Index: 5,
					Type:  raftpb.EntryNormal,
					Data:  nil,
				},
				{
					Term:  1,
					Index: 6,
					Type:  raftpb.EntryNormal,
					Data:  nil,
				},
			},
			Messages: []raftpb.Message{
				{
					Type: raftpb.MsgHeartbeatResp,
				},
			},
		}
		<-time.After(time.Second)
		assert.Equal(t, raftpb.MsgHeartbeatResp, trans.send[0].Type)
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
	tr.logger = logger.NewRaftLogger(zap.NewExample().Sugar())
	tr.closer.Run(func() {
		tr.processRaftReady()
	})
	tr.closer.Run(func() {
		for {
			select {
			case <-tr.closer.CloseCh():
				return
			case <-tr.leaderCh:
			}
		}
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

	tr.updateLeadership(raft.SoftState{
		Lead:      raft.None,
		RaftState: raft.StateFollower,
	})
	assert.Equal(t, 0, len(tr.leaderCh))

	tr.updateLeadership(raft.SoftState{
		Lead:      raft.None,
		RaftState: raft.StatePreCandidate,
	})
	assert.Equal(t, 0, len(tr.leaderCh))

	tr.updateLeadership(raft.SoftState{
		Lead:      raft.None,
		RaftState: raft.StateCandidate,
	})
	assert.Equal(t, 0, len(tr.leaderCh))

	//acquire leadership
	tr.updateLeadership(raft.SoftState{
		Lead:      1,
		RaftState: raft.StateLeader,
	})
	assert.Equal(t, 1, len(tr.leaderCh))
	assert.Equal(t, true, <-tr.leaderCh)
	assert.Equal(t, uint64(1), tr.getLeaderId())

	//lose leadership
	tr.updateLeadership(raft.SoftState{
		Lead:      2,
		RaftState: raft.StateFollower,
	})
	assert.Equal(t, 1, len(tr.leaderCh))
	assert.Equal(t, false, <-tr.leaderCh)
	assert.Equal(t, uint64(2), tr.getLeaderId())

	tr.updateLeadership(raft.SoftState{
		Lead:      raft.None,
		RaftState: raft.StateCandidate,
	})
	assert.Equal(t, 0, len(tr.leaderCh))

	tr.updateLeadership(raft.SoftState{
		Lead:      1,
		RaftState: raft.StateLeader,
	})
	assert.Equal(t, 1, len(tr.leaderCh))
	assert.Equal(t, true, <-tr.leaderCh)
	assert.Equal(t, uint64(1), tr.getLeaderId())
}

func TestRaft_LeadershipNotify(t *testing.T) {
	localPeerID := uint64(1)
	raftNode := &mockRaftNode{readyCh: make(chan raft.Ready)}
	tr := newTestRaft(localPeerID)
	tr.raftNode = raftNode
	tr.storage = &mockStorageMgr{}
	tr.status = status.New()
	tr.closer.Run(func() {
		tr.processRaftReady()
	})
	defer tr.closer.Close()
	for _, tc := range []struct {
		ready    raft.Ready
		isLeader bool
		timeout  bool
	}{
		{
			ready: raft.Ready{
				SoftState: &raft.SoftState{
					Lead:      localPeerID,
					RaftState: raft.StateLeader,
				},
			},
			isLeader: true,
			timeout:  false,
		},
		{
			ready: raft.Ready{
				SoftState: &raft.SoftState{
					Lead:      localPeerID + 1,
					RaftState: raft.StateFollower,
				},
			},
			isLeader: false,
			timeout:  false,
		},
		{
			ready: raft.Ready{
				SoftState: &raft.SoftState{
					Lead:      localPeerID,
					RaftState: raft.StateLeader,
				},
			},
			isLeader: true,
			timeout:  false,
		},
		{
			ready: raft.Ready{
				SoftState: &raft.SoftState{
					Lead:      raft.None,
					RaftState: raft.StatePreCandidate,
				},
			},
			isLeader: false,
			timeout:  false,
		},
		{
			ready: raft.Ready{
				SoftState: &raft.SoftState{
					Lead:      raft.None,
					RaftState: raft.StateCandidate,
				},
			},
			isLeader: false,
			timeout:  true,
		},
		{
			ready: raft.Ready{
				SoftState: &raft.SoftState{
					Lead:      localPeerID,
					RaftState: raft.StateLeader,
				},
			},
			isLeader: true,
			timeout:  false,
		},
	} {
		raftNode.readyCh <- tc.ready
		if tc.timeout == false {
			select {
			case <-time.After(time.Second):
				assert.Fail(t, "wait time limit exceeded")
			case l := <-tr.LeaderCh():
				if l {
					assert.Equal(t, localPeerID, tr.getLeaderId())
				}
				assert.Equal(t, tc.isLeader, l)
			}
		} else {
			select {
			case <-time.After(time.Second):
			case <-tr.LeaderCh():
				assert.Fail(t, "expected timeout")
			}
		}
	}
}
