package raft

import (
	"github.com/fanaujie/babuza/pkg/idgenerator"
	"github.com/fanaujie/babuza/pkg/replier"
	"github.com/fanaujie/babuza/pkg/status"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/raft/v3"
	"testing"
	"time"
)

func TestRaft_WaitReadIndexResponse(t *testing.T) {

	localPeerID := uint64(1)
	raftNode := &mockRaftNode{readyCh: make(chan raft.Ready)}
	tr := newTestRaft(localPeerID)
	tr.config.LinearizedReadRequestTimeout = time.Second * 2
	tr.raftNode = raftNode
	tr.status = status.New()
	readCtx := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	t.Run("correctness", func(t *testing.T) {
		readState := raft.ReadState{
			Index:      1,
			RequestCtx: readCtx,
		}
		tr.readStateCh <- readState
		rs, err := tr.readIndexResponse(readCtx, tr.leaderChangeNotifier.Get())
		assert.Nil(t, err)
		assert.Equal(t, readState, rs)
	})

	t.Run("leader changed", func(t *testing.T) {
		resultCh := make(chan error)
		go func() {
			_, err := tr.readIndexResponse(readCtx, tr.leaderChangeNotifier.Get())
			resultCh <- err
		}()
		time.Sleep(tr.config.LinearizedReadRequestTimeout / 2)
		tr.updateLeadership(raft.SoftState{
			Lead:      localPeerID,
			RaftState: raft.StateLeader,
		})
		assert.ErrorIs(t, <-resultCh, ErrLeaderChange)
	})

	t.Run("internal request timeout", func(t *testing.T) {
		_, err := tr.readIndexResponse(readCtx, tr.leaderChangeNotifier.Get())
		assert.ErrorIs(t, err, errReadIndexRequestTimeout)
	})

	t.Run("raft stop", func(t *testing.T) {
		raftNode.readIndexFunc = func(rctx []byte) error {
			return raft.ErrStopped
		}
		_, err := tr.readIndexResponse(readCtx, tr.leaderChangeNotifier.Get())
		assert.ErrorIs(t, err, raft.ErrStopped)
		raftNode.readIndexFunc = nil
	})
}

func TestRaft_ProcessRaftLinearizedRead(t *testing.T) {
	localPeerID := uint64(1)
	etcdRaftNode := &mockRaftNode{readyCh: make(chan raft.Ready)}
	tr := newTestRaft(localPeerID)
	tr.config.LinearizedReadRequestTimeout = time.Second * 2
	tr.raftNode = etcdRaftNode
	tr.idGenerator = idgenerator.New(localPeerID, 10000)
	tr.status = status.New()
	defer tr.closer.Close()
	tr.closer.Run(func() {
		tr.processRaftLinearizedRead()
	})
	time.Sleep(time.Millisecond * 500)
	t.Run("correctness", func(t *testing.T) {
		etcdRaftNode.readIndexFunc = func(rctx []byte) error {
			tr.readStateCh <- raft.ReadState{Index: 10, RequestCtx: rctx}
			return nil
		}
		tr.status.SetAppliedIndex(10)
		n := tr.linearizeReqNotifier.Get()
		select {
		case tr.readIndexCh <- struct{}{}:
		default:
		}
		<-n.GetCh()
		assert.Nil(t, n.GetError())
		etcdRaftNode.readIndexFunc = nil
	})

	t.Run("waiting for apply index is equal to commit index", func(t *testing.T) {
		commitIndex := uint64(10)
		etcdRaftNode.readIndexFunc = func(rctx []byte) error {
			tr.readStateCh <- raft.ReadState{Index: commitIndex, RequestCtx: rctx}
			return nil
		}
		defer func() {
			etcdRaftNode.readIndexFunc = nil
		}()
		tr.status.SetAppliedIndex(5)
		n := tr.linearizeReqNotifier.Get()
		select {
		case tr.readIndexCh <- struct{}{}:
		default:
		}
		go func() {
			tr.status.SetAppliedIndex(10)
		}()
		<-n.GetCh()
		assert.Nil(t, n.GetError())
	})

	t.Run("request timeout", func(t *testing.T) {
		n := tr.linearizeReqNotifier.Get()
		select {
		case tr.readIndexCh <- struct{}{}:
		default:
		}
		<-n.GetCh()
		assert.ErrorIs(t, n.GetError(), errReadIndexRequestTimeout)
	})

	t.Run("leader changed", func(t *testing.T) {
		n := tr.linearizeReqNotifier.Get()
		select {
		case tr.readIndexCh <- struct{}{}:
		default:
		}
		tr.updateLeadership(raft.SoftState{
			Lead:      localPeerID,
			RaftState: raft.StateLeader,
		})
		<-n.GetCh()
		assert.ErrorIs(t, n.GetError(), ErrLeaderChange)
	})
}

func TestRaft_ProcessRaftLinearizedRead_Timeout_AppliedIndexIsNotEqualToCommittedIndex(t *testing.T) {
	localPeerID := uint64(1)
	etcdRaftNode := &mockRaftNode{readyCh: make(chan raft.Ready)}
	tr := newTestRaft(localPeerID)
	tr.raftNode = etcdRaftNode
	tr.idGenerator = idgenerator.New(localPeerID, 10000)
	tr.status = status.New()
	tr.completionReplier = replier.NewCompletion()
	defer tr.closer.Close()
	tr.closer.Run(func() {
		tr.processRaftLinearizedRead()
	})
	time.Sleep(time.Millisecond * 500)
	commitIndex := uint64(10)
	etcdRaftNode.readIndexFunc = func(rctx []byte) error {
		tr.readStateCh <- raft.ReadState{Index: commitIndex, RequestCtx: rctx}
		return nil
	}
	tr.status.SetAppliedIndex(5)
	n := tr.linearizeReqNotifier.Get()
	select {
	case tr.readIndexCh <- struct{}{}:
	default:
	}
	select {
	case <-time.After(time.Second * 2):
		return
	case <-n.GetCh():
		assert.Fail(t, "must be timeout")
	}
}
