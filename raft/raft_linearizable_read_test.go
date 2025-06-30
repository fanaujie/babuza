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


package raft

import (
	"github.com/fanaujie/babuza/pkg/cluster"
	"github.com/fanaujie/babuza/pkg/idgenerator"
	"github.com/fanaujie/babuza/pkg/logger"
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
	tr.cluster = cluster.NewCluster(&logger.Mock{})
	tr.cluster.SetLocalPeerID(localPeerID)
	readCtx := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	t.Run("correctness", func(t *testing.T) {
		readState := raft.ReadState{
			Index:      1,
			RequestCtx: readCtx,
		}
		tr.readStateCh <- readState
		rs, err := tr.readIndexResponse(readCtx, tr.leaderChangeNotifier.Channel())
		assert.Nil(t, err)
		assert.Equal(t, readState, rs)
	})

	t.Run("leader changed", func(t *testing.T) {
		resultCh := make(chan error)
		go func() {
			_, err := tr.readIndexResponse(readCtx, tr.leaderChangeNotifier.Channel())
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
		_, err := tr.readIndexResponse(readCtx, tr.leaderChangeNotifier.Channel())
		assert.ErrorIs(t, err, ErrReadIndexRequestTimeout)
	})

	t.Run("raft stop", func(t *testing.T) {
		raftNode.readIndexFunc = func(rctx []byte) error {
			return raft.ErrStopped
		}
		_, err := tr.readIndexResponse(readCtx, tr.leaderChangeNotifier.Channel())
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
	tr.cluster = cluster.NewCluster(&logger.Mock{})
	tr.cluster.SetLocalPeerID(localPeerID)
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
		n := tr.linearizeReqNotifier.Current()
		select {
		case tr.readIndexCh <- struct{}{}:
		default:
		}
		<-n.Channel()
		assert.Nil(t, n.Error())
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
		n := tr.linearizeReqNotifier.Current()
		select {
		case tr.readIndexCh <- struct{}{}:
		default:
		}
		go func() {
			tr.status.SetAppliedIndex(10)
		}()
		<-n.Channel()
		assert.Nil(t, n.Error())
	})

	t.Run("request timeout", func(t *testing.T) {
		n := tr.linearizeReqNotifier.Current()
		select {
		case tr.readIndexCh <- struct{}{}:
		default:
		}
		<-n.Channel()
		assert.ErrorIs(t, n.Error(), ErrReadIndexRequestTimeout)
	})

	t.Run("leader changed", func(t *testing.T) {
		n := tr.linearizeReqNotifier.Current()
		select {
		case tr.readIndexCh <- struct{}{}:
		default:
		}
		tr.updateLeadership(raft.SoftState{
			Lead:      localPeerID,
			RaftState: raft.StateLeader,
		})
		<-n.Channel()
		assert.ErrorIs(t, n.Error(), ErrLeaderChange)
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
	n := tr.linearizeReqNotifier.Current()
	select {
	case tr.readIndexCh <- struct{}{}:
	default:
	}
	select {
	case <-time.After(time.Second * 2):
		return
	case <-n.Channel():
		assert.Fail(t, "must be timeout")
	}
}
