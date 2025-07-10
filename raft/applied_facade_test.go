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
	"errors"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/logger"
	"github.com/fanaujie/babuza/pkg/session"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"io"
	"testing"
	"time"
)

// Mock implementations for the new interfaces

type mockAppliedFirstCommitInTermNotifier struct {
	reset bool
}

func (m *mockAppliedFirstCommitInTermNotifier) Reset() {
	m.reset = true
}

type mockAppliedSessionManager struct {
	sessions map[uint64]ibabuza.Session
}

func newMockAppliedSessionManager() *mockAppliedSessionManager {
	return &mockAppliedSessionManager{sessions: make(map[uint64]ibabuza.Session)}
}

func (m *mockAppliedSessionManager) Register(sessionID uint64, reqTime int64) error {
	if _, ok := m.sessions[sessionID]; ok {
		return errors.New("session already exists")
	}
	m.sessions[sessionID] = newMockAppliedSession(sessionID)
	return nil
}

func (m *mockAppliedSessionManager) UnRegister(sessionID uint64) error {
	if _, ok := m.sessions[sessionID]; !ok {
		return errors.New("session not found")
	}
	delete(m.sessions, sessionID)
	return nil
}

func (m *mockAppliedSessionManager) ExpireSession(reqTime int64) {
	// Mock implementation - no actual expiration logic
}

func (m *mockAppliedSessionManager) GetSession(sessionID uint64) (ibabuza.Session, error) {
	if sessionID == 0 {
		return &session.NoOPSession{}, nil
	}
	s, ok := m.sessions[sessionID]
	if !ok {
		return nil, errors.New("session expired")
	}
	return s, nil
}

type mockAppliedSession struct {
	id                    uint64
	lastActiveNanoseconds int64
	result                map[uint64]ibabuza.ApplyResult
}

func newMockAppliedSession(id uint64) *mockAppliedSession {
	return &mockAppliedSession{id: id, result: make(map[uint64]ibabuza.ApplyResult)}
}

func (m *mockAppliedSession) Id() uint64                   { return m.id }
func (m *mockAppliedSession) LastActiveNanoseconds() int64 { return m.lastActiveNanoseconds }
func (m *mockAppliedSession) ExpireAppliedResult(uint64)   {}
func (m *mockAppliedSession) RepeatSequenceNum(seqNum uint64) bool {
	_, ok := m.result[seqNum]
	return ok
}
func (m *mockAppliedSession) ClearResult(lowestSeqNumNotYetReplied uint64) {
	for seq := range m.result {
		if seq < lowestSeqNumNotYetReplied {
			delete(m.result, seq)
		}
	}
}
func (m *mockAppliedSession) AddResult(seqNum uint64, reqTime int64, result ibabuza.ApplyResult) error {
	if _, ok := m.result[seqNum]; ok {
		return errors.New("exist apply result")
	}
	m.result[seqNum] = result
	m.lastActiveNanoseconds = reqTime
	return nil
}

func (m *mockAppliedSession) GetResult(seqNum uint64) (ibabuza.ApplyResult, bool) {
	ar, ok := m.result[seqNum]
	return ar, ok
}

func (m *mockAppliedSession) Snapshot(io.Writer, ibabuza.ApplyResultSerializer) error { return nil }
func (m *mockAppliedSession) Restore(io.Reader, ibabuza.ApplyResultSerializer) error  { return nil }

type mockAppliedReplier struct {
	reply map[uint64]ibabuza.ApplyResult
}

func newMockAppliedReplier() *mockAppliedReplier {
	return &mockAppliedReplier{reply: make(map[uint64]ibabuza.ApplyResult)}
}

func (m *mockAppliedReplier) SendResult(replyID uint64, ar ibabuza.ApplyResult) {
	m.reply[replyID] = ar
}

type mockAppliedCluster struct {
	groupID    ibabuza.RaftGroupID
	clusterID  uint64
	localId    uint64
	peers      map[uint64]babuzapb.Peer
	removedIds map[uint64]struct{}
	hardTerm   uint64
}

func newMockAppliedCluster() *mockAppliedCluster {
	return &mockAppliedCluster{
		groupID:    1,
		clusterID:  1,
		localId:    1,
		peers:      make(map[uint64]babuzapb.Peer),
		removedIds: make(map[uint64]struct{}),
		hardTerm:   1,
	}
}

func (m *mockAppliedCluster) ClusterID() uint64              { return m.clusterID }
func (m *mockAppliedCluster) GroupID() ibabuza.RaftGroupID   { return m.groupID }
func (m *mockAppliedCluster) LocalPeerID() uint64            { return m.localId }
func (m *mockAppliedCluster) GetHardStateTerm() uint64       { return m.hardTerm }
func (m *mockAppliedCluster) SetAppliedIndex(index uint64)   {}
func (m *mockAppliedCluster) SetAppliedTerm(term uint64)     {}
func (m *mockAppliedCluster) SetConfState(cs raftpb.ConfState) {}

func (m *mockAppliedCluster) Peer(peerID uint64) (babuzapb.Peer, error) {
	p, ok := m.peers[peerID]
	if !ok {
		return babuzapb.Peer{}, errors.New("peer not found")
	}
	return p, nil
}

func (m *mockAppliedCluster) Add(peer babuzapb.RaftPeerAttribute) error {
	if _, ok := m.peers[peer.PeerID]; ok {
		return errors.New("ErrPeerIDExists")
	}
	if _, ok := m.removedIds[peer.PeerID]; ok {
		return errors.New("ErrPeerIDRemoved")
	}
	m.peers[peer.PeerID] = babuzapb.Peer{
		RaftPeerAttr: peer,
	}
	return nil
}

func (m *mockAppliedCluster) Update(peerID uint64, peer babuzapb.RaftPeerAttribute) error {
	p, ok := m.peers[peerID]
	if !ok {
		return errors.New("ErrPeerIDNotFound")
	}
	p.RaftPeerAttr.RaftListenAddr = peer.RaftListenAddr
	m.peers[peerID] = p
	return nil
}

func (m *mockAppliedCluster) Remove(peerID uint64) error {
	_, ok := m.peers[peerID]
	if !ok {
		return errors.New("ErrPeerIDNotFound")
	}
	delete(m.peers, peerID)
	m.removedIds[peerID] = struct{}{}
	return nil
}

func (m *mockAppliedCluster) Promote(peerID uint64) error {
	p, ok := m.peers[peerID]
	if !ok {
		return errors.New("ErrPeerIDNotFound")
	}
	if !p.RaftPeerAttr.IsLearner {
		return errors.New("ErrPeerNotLearner")
	}
	p.RaftPeerAttr.IsLearner = false
	m.peers[peerID] = p
	return nil
}

func (m *mockAppliedCluster) UpdateAppServiceAddresses(peerID uint64, addresses []string) error {
	return nil
}

type mockAppliedRaftNode struct {
}

func (m *mockAppliedRaftNode) ApplyConfChange(clusterID uint64, cc raftpb.ConfChangeI) (*raftpb.ConfState, error) {
	return &raftpb.ConfState{}, nil
}

type mockAppliedTransport struct {
	resolver map[uint64]string
}

func newMockAppliedTransport() *mockAppliedTransport {
	return &mockAppliedTransport{
		resolver: make(map[uint64]string),
	}
}

func (m *mockAppliedTransport) AddPeer(groupID ibabuza.RaftGroupID, peerID uint64, raftListenAddr string) {
	m.resolver[peerID] = raftListenAddr
}

func (m *mockAppliedTransport) UpdatePeer(groupID ibabuza.RaftGroupID, peerID uint64, raftListenAddr string) {
	m.resolver[peerID] = raftListenAddr
}

func (m *mockAppliedTransport) RemovePeer(groupID ibabuza.RaftGroupID, peerID uint64) {
	delete(m.resolver, peerID)
}

type mockAppliedPublishMemberEvent struct {
	events []ibabuza.RaftEvent
}

func newMockAppliedPublishMemberEvent() *mockAppliedPublishMemberEvent {
	return &mockAppliedPublishMemberEvent{events: make([]ibabuza.RaftEvent, 0)}
}

func (m *mockAppliedPublishMemberEvent) Publish(event ibabuza.RaftEvent) {
	m.events = append(m.events, event)
}

type mockMetricsCollector struct {
	isLearner int
}

func (m *mockMetricsCollector) SetHasLeader(hasLeader int64) {}
func (m *mockMetricsCollector) SetIsLeader(isLeader int64) {}
func (m *mockMetricsCollector) IncrementLeaderChanges() {}
func (m *mockMetricsCollector) SetIsLearner(isFollower int64) {
	m.isLearner = int(isFollower)
}
func (m *mockMetricsCollector) IncrementLearnerPromoteSucceed() {}
func (m *mockMetricsCollector) IncrementLearnerPromoteFailed() {}
func (m *mockMetricsCollector) RecordApplySec(duration float64) {}
func (m *mockMetricsCollector) RecordDoSnapshotSec(duration float64) {}
func (m *mockMetricsCollector) RecordApplySnapshotSec(duration float64) {}
func (m *mockMetricsCollector) SetSnapshotApplyInProgress(applying int64) {}
func (m *mockMetricsCollector) IncrementInflightSnapshots() {}
func (m *mockMetricsCollector) DecrementInflightSnapshots() {}
func (m *mockMetricsCollector) SetProposalCommited(commitedEntries uint64) {}
func (m *mockMetricsCollector) SetProposalAppliedIndex(appliedIndex uint64) {}
func (m *mockMetricsCollector) IncrementProposalPending() {}
func (m *mockMetricsCollector) DecrementProposalPending() {}
func (m *mockMetricsCollector) IncrementProposalFailed() {}
func (m *mockMetricsCollector) IncrementSlowReadIndex() {}
func (m *mockMetricsCollector) IncrementReadIndexFailed() {}

// Test cases

func TestAppliedFacade_DoExactlyOnce(t *testing.T) {
	sessionMgr := newMockAppliedSessionManager()
	replier := newMockAppliedReplier()
	a := appliedFacadeImpl{
		firstCommitNotifier: &mockAppliedFirstCommitInTermNotifier{},
		sessionManager:      sessionMgr,
		replier:             replier,
		cluster:             newMockAppliedCluster(),
		raftNode:            &mockAppliedRaftNode{},
		trans:               newMockAppliedTransport(),
		log:                 &logger.Mock{},
	}

	t.Run("find session and apply", func(t *testing.T) {
		s := newMockAppliedSession(1)
		sessionMgr.sessions[1] = s
		defer delete(sessionMgr.sessions, 1)
		
		toApply, ar := a.doExactlyOnce(1, babuzapb.RequestContext{
			SessionID:   1,
			SequenceNum: 1,
		}, s)
		
		assert.Equal(t, true, toApply)
		assert.Equal(t, uint64(0), ar.LogIndex)
		assert.Nil(t, ar.Error)
	})

	t.Run("session result not found", func(t *testing.T) {
		s := newMockAppliedSession(1)
		// Session exists but doesn't have the result for this sequence number
		toApply, ar := a.doExactlyOnce(100, babuzapb.RequestContext{
			SessionID:   1,
			SequenceNum: 2, // Different sequence number that doesn't exist
		}, s)
		
		assert.Equal(t, true, toApply) // Should proceed to apply
		assert.Equal(t, uint64(0), ar.LogIndex)
		assert.Nil(t, ar.Error)
	})

	t.Run("exactly-once: duplicate sequence number", func(t *testing.T) {
		s := newMockAppliedSession(1)
		existingResult := ibabuza.ApplyResult{LogIndex: 100, Response: "cached"}
		s.result[1] = existingResult
		sessionMgr.sessions[1] = s
		defer delete(sessionMgr.sessions, 1)
		
		toApply, ar := a.doExactlyOnce(101, babuzapb.RequestContext{
			SessionID:   1,
			SequenceNum: 1,
		}, s)
		
		assert.Equal(t, false, toApply)
		assert.Equal(t, existingResult, ar)
	})

	t.Run("clear old results", func(t *testing.T) {
		s := newMockAppliedSession(1)
		s.result[1] = ibabuza.ApplyResult{LogIndex: 1}
		sessionMgr.sessions[1] = s
		defer delete(sessionMgr.sessions, 1)
		
		toApply, ar := a.doExactlyOnce(100, babuzapb.RequestContext{
			SessionID:                 1,
			SequenceNum:               2,
			LowestSeqNumNotYetReplied: 2,
		}, s)
		
		assert.Equal(t, true, toApply)
		assert.Equal(t, uint64(0), ar.LogIndex)
		_, ok := s.result[1]
		assert.Equal(t, false, ok)
	})
}

func TestAppliedFacade_ApplyNilEntryInNewTerm(t *testing.T) {
	notifier := &mockAppliedFirstCommitInTermNotifier{}
	a := appliedFacadeImpl{
		firstCommitNotifier: notifier,
		sessionManager:      newMockAppliedSessionManager(),
		replier:             newMockAppliedReplier(),
		cluster:             newMockAppliedCluster(),
		raftNode:            &mockAppliedRaftNode{},
		trans:               newMockAppliedTransport(),
		log:                 &logger.Mock{},
	}

	a.ApplyNilEntryInNewTerm(1, 1)
	assert.Equal(t, true, notifier.reset)
}

func TestAppliedFacade_ApplyNormalEntry(t *testing.T) {
	a := appliedFacadeImpl{
		firstCommitNotifier: &mockAppliedFirstCommitInTermNotifier{},
		sessionManager:      newMockAppliedSessionManager(),
		replier:             newMockAppliedReplier(),
		cluster:             newMockAppliedCluster(),
		raftNode:            &mockAppliedRaftNode{},
		trans:               newMockAppliedTransport(),
		log:                 &logger.Mock{},
		metricsCollector:    &mockMetricsCollector{},
	}

	t.Run("register session", func(t *testing.T) {
		sessionMgr := newMockAppliedSessionManager()
		replier := newMockAppliedReplier()
		a.sessionManager = sessionMgr
		a.replier = replier

		req := babuzapb.NormalRequest{
			Context: babuzapb.RequestContext{
				ReplyID: 1,
			},
			Register: &babuzapb.RegisterSessionRequest{
				SessionID: 100,
			},
		}
		data, err := req.Marshal()
		assert.Nil(t, err)
		e := raftpb.Entry{
			Term:  1,
			Index: 100,
			Type:  raftpb.EntryNormal,
			Data:  data,
		}
		
		actualReq, ar, session := a.ApplyNormalEntry(e)
		assert.Equal(t, req, actualReq)
		assert.Equal(t, e.Index, ar.LogIndex)
		assert.Nil(t, ar.Error)
		
		// Check NoOPSession is returned
		assert.Equal(t, uint64(0), session.Id())
	})

	t.Run("publish app service", func(t *testing.T) {
		sessionMgr := newMockAppliedSessionManager()
		replier := newMockAppliedReplier()
		a.sessionManager = sessionMgr
		a.replier = replier

		req := babuzapb.NormalRequest{
			Context: babuzapb.RequestContext{
				ReplyID: 1,
			},
			PubAppService: &babuzapb.PubAppServiceRequest{
				PubServicePeerID:      1,
				AppServiceAddresses:   []string{"localhost:8080"},
			},
		}
		data, err := req.Marshal()
		assert.Nil(t, err)
		e := raftpb.Entry{
			Term:  1,
			Index: 100,
			Type:  raftpb.EntryNormal,
			Data:  data,
		}
		
		actualReq, ar, session := a.ApplyNormalEntry(e)
		assert.Equal(t, req, actualReq)
		assert.Equal(t, e.Index, ar.LogIndex)
		assert.Nil(t, ar.Error)
		
		// Check NoOPSession is returned
		assert.Equal(t, uint64(0), session.Id())
	})

	t.Run("valid session and sequence number", func(t *testing.T) {
		sessionMgr := newMockAppliedSessionManager()
		replier := newMockAppliedReplier()
		a.sessionManager = sessionMgr
		a.replier = replier

		sId := uint64(1)
		seqNum := uint64(1)
		sessionMgr.sessions[sId] = newMockAppliedSession(sId)

		req := babuzapb.NormalRequest{
			Context: babuzapb.RequestContext{
				ReplyID:     1,
				SessionID:   sId,
				SequenceNum: seqNum,
			},
		}
		data, err := req.Marshal()
		assert.Nil(t, err)
		e := raftpb.Entry{
			Term:  1,
			Index: 100,
			Type:  raftpb.EntryNormal,
			Data:  data,
		}
		
		actualReq, ar, session := a.ApplyNormalEntry(e)
		assert.Equal(t, req, actualReq)
		assert.Equal(t, uint64(0), ar.LogIndex) // Empty result means should apply
		assert.Nil(t, ar.Error)
		assert.Equal(t, sId, session.Id())
	})

	t.Run("session not found", func(t *testing.T) {
		sessionMgr := newMockAppliedSessionManager()
		replier := newMockAppliedReplier()
		a.sessionManager = sessionMgr
		a.replier = replier

		req := babuzapb.NormalRequest{
			Context: babuzapb.RequestContext{
				ReplyID:     1,
				SessionID:   999,
				SequenceNum: 1,
			},
		}
		data, err := req.Marshal()
		assert.Nil(t, err)
		e := raftpb.Entry{
			Term:  1,
			Index: 100,
			Type:  raftpb.EntryNormal,
			Data:  data,
		}
		
		actualReq, ar, session := a.ApplyNormalEntry(e)
		assert.Equal(t, req, actualReq)
		assert.Equal(t, e.Index, ar.LogIndex)
		assert.Error(t, ar.Error)
		
		// Check NoOPSession is returned
		assert.Equal(t, uint64(0), session.Id())
	})

	t.Run("exactly-once: duplicate sequence", func(t *testing.T) {
		sessionMgr := newMockAppliedSessionManager()
		replier := newMockAppliedReplier()
		a.sessionManager = sessionMgr
		a.replier = replier

		sId := uint64(1)
		seqNum := uint64(1)
		sess := newMockAppliedSession(sId)
		existingResult := ibabuza.ApplyResult{LogIndex: 50, Response: "cached"}
		sess.result[seqNum] = existingResult
		sessionMgr.sessions[sId] = sess

		req := babuzapb.NormalRequest{
			Context: babuzapb.RequestContext{
				ReplyID:     1,
				SessionID:   sId,
				SequenceNum: seqNum,
			},
		}
		data, err := req.Marshal()
		assert.Nil(t, err)
		e := raftpb.Entry{
			Term:  1,
			Index: 100,
			Type:  raftpb.EntryNormal,
			Data:  data,
		}
		
		actualReq, ar, session := a.ApplyNormalEntry(e)
		assert.Equal(t, req, actualReq)
		assert.Equal(t, existingResult, ar)
		assert.Equal(t, sId, session.Id())
	})
}

func TestAppliedFacade_ApplyConfChangeEntry(t *testing.T) {
	a := appliedFacadeImpl{
		firstCommitNotifier: &mockAppliedFirstCommitInTermNotifier{},
		sessionManager:      newMockAppliedSessionManager(),
		replier:             newMockAppliedReplier(),
		cluster:             newMockAppliedCluster(),
		raftNode:            &mockAppliedRaftNode{},
		trans:               newMockAppliedTransport(),
		memberEvent:         newMockAppliedPublishMemberEvent(),
		log:                 &logger.Mock{},
		metricsCollector:    &mockMetricsCollector{},
	}

	t.Run("add node with valid session", func(t *testing.T) {
		sessionMgr := newMockAppliedSessionManager()
		replier := newMockAppliedReplier()
		cluster := newMockAppliedCluster()
		trans := newMockAppliedTransport()
		memberEvent := newMockAppliedPublishMemberEvent()
		
		a.sessionManager = sessionMgr
		a.replier = replier
		a.cluster = cluster
		a.trans = trans
		a.memberEvent = memberEvent

		sId := uint64(1)
		seqNum := uint64(1)
		sess := newMockAppliedSession(sId)
		sessionMgr.sessions[sId] = sess

		addPeerId := uint64(2)
		replyID := uint64(1)
		req := babuzapb.ConfChangeRequest{
			Context: babuzapb.RequestContext{
				ReplyID:     replyID,
				SessionID:   sId,
				SequenceNum: seqNum,
			},
			GroupID: 1,
			RaftPeerAttr: babuzapb.RaftPeerAttribute{
				PeerID:         addPeerId,
				RaftListenAddr: "localhost:14200",
			},
		}
		data, err := req.Marshal()
		assert.Nil(t, err)

		cc := raftpb.ConfChange{Type: raftpb.ConfChangeAddNode, NodeID: addPeerId, Context: data}
		data, err = cc.Marshal()
		assert.Nil(t, err)
		e := raftpb.Entry{
			Term:  1,
			Index: 1,
			Type:  raftpb.EntryConfChange,
			Data:  data,
		}
		
		ctx, ar, removeSelf := a.ApplyConfChangeEntry(e)
		assert.Equal(t, req.Context, ctx)
		assert.Equal(t, false, removeSelf)
		assert.Equal(t, e.Index, ar.LogIndex)
		assert.Nil(t, ar.Error)
		
		// Check peer was added
		_, ok := cluster.peers[addPeerId]
		assert.True(t, ok)
		
		// Check transport was updated
		_, ok = trans.resolver[addPeerId]
		assert.True(t, ok)
		
		// Check member event was published
		assert.Equal(t, 1, len(memberEvent.events))
		assert.Equal(t, ibabuza.MemberJoined, memberEvent.events[0].Event)
	})

	t.Run("remove self", func(t *testing.T) {
		sessionMgr := newMockAppliedSessionManager()
		replier := newMockAppliedReplier()
		cluster := newMockAppliedCluster()
		trans := newMockAppliedTransport()
		memberEvent := newMockAppliedPublishMemberEvent()
		
		a.sessionManager = sessionMgr
		a.replier = replier
		a.cluster = cluster
		a.trans = trans
		a.memberEvent = memberEvent

		cluster.localId = 1
		cluster.Add(babuzapb.RaftPeerAttribute{PeerID: 1})

		sId := uint64(2)
		seqNum := uint64(1)
		sess := newMockAppliedSession(sId)
		sessionMgr.sessions[sId] = sess

		removePeerId := uint64(1) // self
		replyID := uint64(1)
		req := babuzapb.ConfChangeRequest{
			Context: babuzapb.RequestContext{
				ReplyID:     replyID,
				SessionID:   sId,
				SequenceNum: seqNum,
			},
			GroupID: 1,
			RaftPeerAttr: babuzapb.RaftPeerAttribute{
				PeerID: removePeerId,
			},
		}
		data, err := req.Marshal()
		assert.Nil(t, err)

		cc := raftpb.ConfChange{Type: raftpb.ConfChangeRemoveNode, NodeID: removePeerId, Context: data}
		data, err = cc.Marshal()
		assert.Nil(t, err)
		e := raftpb.Entry{
			Term:  1,
			Index: 1,
			Type:  raftpb.EntryConfChange,
			Data:  data,
		}
		
		ctx, ar, removeSelf := a.ApplyConfChangeEntry(e)
		assert.Equal(t, req.Context, ctx)
		assert.Equal(t, true, removeSelf)
		assert.Equal(t, e.Index, ar.LogIndex)
		assert.Nil(t, ar.Error)
		
		// Check peer was removed
		_, ok := cluster.peers[removePeerId]
		assert.False(t, ok)
		
		// Check member event was published
		assert.Equal(t, 1, len(memberEvent.events))
		assert.Equal(t, ibabuza.MemberRemoved, memberEvent.events[0].Event)
	})

	t.Run("session not found", func(t *testing.T) {
		sessionMgr := newMockAppliedSessionManager()
		replier := newMockAppliedReplier()
		a.sessionManager = sessionMgr
		a.replier = replier

		replyID := uint64(1)
		req := babuzapb.ConfChangeRequest{
			Context: babuzapb.RequestContext{
				ReplyID:     replyID,
				SessionID:   999,
				SequenceNum: 1,
			},
			GroupID: 1,
			RaftPeerAttr: babuzapb.RaftPeerAttribute{
				PeerID:         1,
				RaftListenAddr: "localhost:14200",
			},
		}
		data, err := req.Marshal()
		assert.Nil(t, err)

		cc := raftpb.ConfChange{Type: raftpb.ConfChangeAddNode, NodeID: 1, Context: data}
		data, err = cc.Marshal()
		assert.Nil(t, err)
		e := raftpb.Entry{
			Term:  1,
			Index: 1,
			Type:  raftpb.EntryConfChange,
			Data:  data,
		}

		ctx, ar, removeSelf := a.ApplyConfChangeEntry(e)
		assert.Equal(t, req.Context, ctx)
		assert.Equal(t, false, removeSelf)
		assert.Equal(t, e.Index, ar.LogIndex)
		assert.Error(t, ar.Error)
	})

	t.Run("exactly-once: duplicate sequence", func(t *testing.T) {
		sessionMgr := newMockAppliedSessionManager()
		replier := newMockAppliedReplier()
		cluster := newMockAppliedCluster()
		trans := newMockAppliedTransport()
		
		a.sessionManager = sessionMgr
		a.replier = replier
		a.cluster = cluster
		a.trans = trans

		sId := uint64(1)
		seqNum := uint64(1)
		sess := newMockAppliedSession(sId)
		existingResult := ibabuza.ApplyResult{LogIndex: 50, Response: "cached"}
		sess.result[seqNum] = existingResult
		sessionMgr.sessions[sId] = sess

		addPeerId := uint64(2)
		replyID := uint64(1)
		req := babuzapb.ConfChangeRequest{
			Context: babuzapb.RequestContext{
				ReplyID:     replyID,
				SessionID:   sId,
				SequenceNum: seqNum,
			},
			GroupID: 1,
			RaftPeerAttr: babuzapb.RaftPeerAttribute{
				PeerID: addPeerId,
			},
		}
		data, err := req.Marshal()
		assert.Nil(t, err)

		cc := raftpb.ConfChange{Type: raftpb.ConfChangeAddNode, NodeID: addPeerId, Context: data}
		data, err = cc.Marshal()
		assert.Nil(t, err)
		e := raftpb.Entry{
			Term:  1,
			Index: 100,
			Type:  raftpb.EntryConfChange,
			Data:  data,
		}

		ctx, ar, removeSelf := a.ApplyConfChangeEntry(e)
		assert.Equal(t, req.Context, ctx)
		assert.Equal(t, false, removeSelf)
		assert.Equal(t, existingResult, ar)
	})
}

func TestAppliedFacade_SendAppliedResult(t *testing.T) {
	replier := newMockAppliedReplier()
	a := appliedFacadeImpl{
		replier: replier,
	}

	replyID := uint64(1)
	ar := ibabuza.ApplyResult{
		LogIndex: 100,
		Response: "test",
	}

	a.SendAppliedResult(replyID, ar)

	result, ok := replier.reply[replyID]
	assert.True(t, ok)
	assert.Equal(t, ar, result)
}

func TestAppliedFacade_HandleSessionRegister(t *testing.T) {
	a := appliedFacadeImpl{
		sessionManager: newMockAppliedSessionManager(),
	}

	t.Run("register session", func(t *testing.T) {
		sessionMgr := newMockAppliedSessionManager()
		a.sessionManager = sessionMgr

		e := raftpb.Entry{
			Term:  1,
			Index: 100,
			Type:  raftpb.EntryNormal,
		}
		
		req := &babuzapb.RegisterSessionRequest{
			SessionID:  100,
			Unregister: false,
		}
		
		reqTime := time.Now().UnixNano()
		ar := a.handleSessionRegister(e, reqTime, req)
		
		assert.Equal(t, e.Index, ar.LogIndex)
		assert.Nil(t, ar.Error)
		
		// Check session was registered
		_, ok := sessionMgr.sessions[e.Index]
		assert.True(t, ok)
	})

	t.Run("unregister session", func(t *testing.T) {
		sessionMgr := newMockAppliedSessionManager()
		a.sessionManager = sessionMgr
		
		// First register a session
		sessionMgr.sessions[100] = newMockAppliedSession(100)

		e := raftpb.Entry{
			Term:  1,
			Index: 100,
			Type:  raftpb.EntryNormal,
		}
		
		req := &babuzapb.RegisterSessionRequest{
			SessionID:  100,
			Unregister: true,
		}
		
		reqTime := time.Now().UnixNano()
		ar := a.handleSessionRegister(e, reqTime, req)
		
		assert.Equal(t, e.Index, ar.LogIndex)
		assert.Nil(t, ar.Error)
		
		// Check session was unregistered
		_, ok := sessionMgr.sessions[100]
		assert.False(t, ok)
	})
}

func TestAppliedFacade_HandlePubAppService(t *testing.T) {
	cluster := newMockAppliedCluster()
	a := appliedFacadeImpl{
		cluster: cluster,
	}

	e := raftpb.Entry{
		Term:  1,
		Index: 100,
		Type:  raftpb.EntryNormal,
	}
	
	req := babuzapb.NormalRequest{
		PubAppService: &babuzapb.PubAppServiceRequest{
			PubServicePeerID:    1,
			AppServiceAddresses: []string{"localhost:8080", "localhost:8081"},
		},
	}
	
	ar := a.handlePubAppService(e, req)
	
	assert.Equal(t, e.Index, ar.LogIndex)
	assert.Nil(t, ar.Response) // Mock cluster returns nil
}

func TestAppliedFacade_ProcessConfChange(t *testing.T) {
	a := appliedFacadeImpl{
		cluster:          newMockAppliedCluster(),
		raftNode:         &mockAppliedRaftNode{},
		trans:            newMockAppliedTransport(),
		memberEvent:      newMockAppliedPublishMemberEvent(),
		metricsCollector: &mockMetricsCollector{},
	}

	t.Run("add node", func(t *testing.T) {
		cluster := newMockAppliedCluster()
		trans := newMockAppliedTransport()
		memberEvent := newMockAppliedPublishMemberEvent()
		
		a.cluster = cluster
		a.trans = trans
		a.memberEvent = memberEvent

		cc := raftpb.ConfChange{
			Type:   raftpb.ConfChangeAddNode,
			NodeID: 2,
		}
		
		req := babuzapb.ConfChangeRequest{
			GroupID: 1,
			RaftPeerAttr: babuzapb.RaftPeerAttribute{
				PeerID:         2,
				RaftListenAddr: "localhost:14200",
			},
		}

		confState, removeSelf, err := a.processConfChange(cc, req)
		
		assert.NotNil(t, confState)
		assert.False(t, removeSelf)
		assert.Nil(t, err)
		
		// Check peer was added
		_, ok := cluster.peers[2]
		assert.True(t, ok)
		
		// Check transport was updated
		_, ok = trans.resolver[2]
		assert.True(t, ok)
		
		// Check member event was published
		assert.Equal(t, 1, len(memberEvent.events))
		assert.Equal(t, ibabuza.MemberJoined, memberEvent.events[0].Event)
	})

	t.Run("cluster validation failure", func(t *testing.T) {
		cluster := newMockAppliedCluster()
		a.cluster = cluster

		// Try to add a peer that already exists
		cluster.peers[2] = babuzapb.Peer{
			RaftPeerAttr: babuzapb.RaftPeerAttribute{PeerID: 2},
		}

		cc := raftpb.ConfChange{
			Type:   raftpb.ConfChangeAddNode,
			NodeID: 2,
		}
		
		req := babuzapb.ConfChangeRequest{
			GroupID: 1,
			RaftPeerAttr: babuzapb.RaftPeerAttribute{
				PeerID:         2,
				RaftListenAddr: "localhost:14200",
			},
		}

		confState, removeSelf, err := a.processConfChange(cc, req)
		
		assert.Nil(t, confState)
		assert.False(t, removeSelf)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "ErrPeerIDExists")
	})
}

func TestAppliedTransportAdaptor(t *testing.T) {
	mockTrans := &mockTransportForAdaptor{
		resolver: make(map[uint64]string),
	}
	
	adaptor := &appliedTransportAdaptor{
		trans: mockTrans,
	}

	t.Run("add peer", func(t *testing.T) {
		adaptor.AddPeer(1, 2, "localhost:14200")
		assert.Equal(t, "localhost:14200", mockTrans.resolver[2])
	})

	t.Run("update peer", func(t *testing.T) {
		adaptor.UpdatePeer(1, 2, "localhost:14201")
		assert.Equal(t, "localhost:14201", mockTrans.resolver[2])
	})

	t.Run("remove peer", func(t *testing.T) {
		adaptor.RemovePeer(1, 2)
		_, ok := mockTrans.resolver[2]
		assert.False(t, ok)
	})
}

// Additional mock for testing transport adaptor
type mockTransportForAdaptor struct {
	resolver map[uint64]string
}

func (m *mockTransportForAdaptor) Start() error                                       { return nil }
func (m *mockTransportForAdaptor) Stop() error                                        { return nil }
func (m *mockTransportForAdaptor) SetupTransportConfig(ibabuza.TransportConfig) error { return nil }
func (m *mockTransportForAdaptor) SetupTransportRaft(ibabuza.RaftNodeHandler) error {
	return nil
}
func (m *mockTransportForAdaptor) Send(msg raftpb.Message) {}
func (m *mockTransportForAdaptor) SendSnapshot(snap raftpb.Message) {}
func (m *mockTransportForAdaptor) CreateTransportClient() (ibabuza.TransportClient, error) {
	return nil, nil
}
func (m *mockTransportForAdaptor) AddPeer(peerID uint64, endpoint string) {
	m.resolver[peerID] = endpoint
}
func (m *mockTransportForAdaptor) UpdatePeer(peerID uint64, endpoint string) {
	m.resolver[peerID] = endpoint
}
func (m *mockTransportForAdaptor) RemovePeer(peerID uint64) {
	delete(m.resolver, peerID)
}
func (m *mockTransportForAdaptor) RemovePeers() {}

func TestNewAppliedFacade(t *testing.T) {
	firstCommitNotifier := &mockAppliedFirstCommitInTermNotifier{}
	sessionMgr := newMockAppliedSessionManager()
	replier := newMockAppliedReplier()
	cluster := newMockAppliedCluster()
	raftNode := &mockAppliedRaftNode{}
	trans := newMockAppliedTransport()
	memberEvent := newMockAppliedPublishMemberEvent()
	log := &logger.Mock{}
	metricsCollector := &mockMetricsCollector{}

	facade := NewAppliedFacade(
		firstCommitNotifier,
		sessionMgr,
		replier,
		cluster,
		raftNode,
		trans,
		memberEvent,
		log,
		metricsCollector,
	)

	assert.NotNil(t, facade)
	
	// Test that the facade is properly initialized
	impl := facade.(*appliedFacadeImpl)
	assert.Equal(t, firstCommitNotifier, impl.firstCommitNotifier)
	assert.Equal(t, sessionMgr, impl.sessionManager)
	assert.Equal(t, replier, impl.replier)
	assert.Equal(t, cluster, impl.cluster)
	assert.Equal(t, raftNode, impl.raftNode)
	assert.Equal(t, trans, impl.trans)
	assert.Equal(t, memberEvent, impl.memberEvent)
	assert.Equal(t, log, impl.log)
	assert.Equal(t, metricsCollector, impl.metricsCollector)
}