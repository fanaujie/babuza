package raft

import (
	"errors"
	"fmt"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/logger"
	"github.com/fanaujie/babuza/pkg/session"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"io"
	"testing"
)

type mockApplyStateMachineAdaptor struct {
	index uint64
}

func (m *mockApplyStateMachineAdaptor) GetStateMachineAppliedIndex() uint64 {
	return m.index
}

func (m *mockApplyStateMachineAdaptor) SetStateMachineAppliedIndex(index uint64) {
	m.index = index
}

type mockApplyNotifier struct {
	close bool
}

func (m *mockApplyNotifier) CloseAndRenew() {
	m.close = true
}

type mockApplyRaftNode struct {
}

func (m *mockApplyRaftNode) ApplyConfChange(cc raftpb.ConfChangeI) *raftpb.ConfState {
	return &raftpb.ConfState{}
}

type mockApplySession struct {
	id                    uint64
	lastActiveNanoseconds int64
	result                map[uint64]ibabuza.ApplyResult
}

func newMockApplySession(id uint64) *mockApplySession {
	return &mockApplySession{id: id, result: make(map[uint64]ibabuza.ApplyResult)}
}

func (m *mockApplySession) Id() uint64                   { return m.id }
func (m *mockApplySession) LastActiveNanoseconds() int64 { return 0 }
func (m *mockApplySession) ExpireAppliedResult(uint64)   {}
func (m *mockApplySession) RepeatSequenceNum(seqNum uint64) bool {
	_, ok := m.result[seqNum]
	return ok
}
func (m *mockApplySession) ClearResult(lowestSeqNumNotYetReplied uint64) {
	for seq := range m.result {
		if seq < lowestSeqNumNotYetReplied {
			delete(m.result, seq)
		}
	}
}
func (m *mockApplySession) AddResult(seqNum uint64, reqTime int64, result ibabuza.ApplyResult) error {
	_, ok := m.result[seqNum]
	if ok {
		return errors.New("exist apply result")
	}
	m.result[seqNum] = result
	m.lastActiveNanoseconds = reqTime
	return nil
}

func (m *mockApplySession) GetResult(seqNum uint64) (ibabuza.ApplyResult, bool) {
	ar, ok := m.result[seqNum]
	return ar, ok
}

func (m *mockApplySession) Snapshot(io.Writer, ibabuza.ApplyResultSerializer) error { return nil }
func (m *mockApplySession) Restore(io.Reader, ibabuza.ApplyResultSerializer) error  { return nil }

type mockApplySessionMgr struct {
	sessions map[uint64]ibabuza.Session
}

func newMockApplySessionManager() *mockApplySessionMgr {
	return &mockApplySessionMgr{sessions: make(map[uint64]ibabuza.Session)}
}

func (m *mockApplySessionMgr) GetSession(sid uint64) (ibabuza.Session, error) {
	if sid == 0 {
		return &session.NoOPSession{}, nil
	}
	s, ok := m.sessions[sid]
	if !ok {
		return nil, errors.New("session expired")
	}
	return s, nil
}
func (m *mockApplySessionMgr) Register(sid uint64, reqTime int64) {
	_, ok := m.sessions[sid]
	if ok {
		return
	}
	m.sessions[sid] = newMockApplySession(sid)
}
func (m *mockApplySessionMgr) ExpireSession(int64) {}

type mockApplyTransport struct {
	resolver map[uint64]string
}

func newMockApplyTransport() *mockApplyTransport {
	return &mockApplyTransport{
		resolver: make(map[uint64]string),
	}
}

func (m *mockApplyTransport) AddPeer(id uint64, endpoint string) {
	m.resolver[id] = endpoint
}
func (m *mockApplyTransport) UpdatePeer(id uint64, endpoint string) {
	m.resolver[id] = endpoint
}
func (m *mockApplyTransport) RemovePeer(id uint64) {
	delete(m.resolver, id)
}

type mockApplyReplier struct {
	reply map[uint64]ibabuza.ApplyResult
}

func newMockApplyReplier() *mockApplyReplier {
	return &mockApplyReplier{reply: make(map[uint64]ibabuza.ApplyResult)}
}

func (m *mockApplyReplier) SendResult(replyID uint64, ar ibabuza.ApplyResult) {
	m.reply[replyID] = ar
}

type mockApplyStatus struct {
	index          uint64
	term           uint64
	state          raftpb.ConfState
	hardStateTerm  uint64
	pubServiceDone bool
}

func newMockApplyStatus() *mockApplyStatus {
	return &mockApplyStatus{}
}

func (m *mockApplyStatus) SetAppliedIndex(v uint64) {
	m.index = v
}
func (m *mockApplyStatus) SetAppliedTerm(v uint64) {
	m.term = v
}
func (m *mockApplyStatus) SetConfState(s raftpb.ConfState) {
	m.state = s
}
func (m *mockApplyStatus) GetHardStateTerm() uint64 {
	return m.hardStateTerm
}

func (m *mockApplyStatus) IsLocalPeerPublishServiceMarkDone() bool {
	return m.pubServiceDone
}

type mockApplyCluster struct {
	localId    uint64
	peers      map[uint64]babuzapb.Peer
	removedIds map[uint64]struct{}
}

func newMockApplyCluster() *mockApplyCluster {
	return &mockApplyCluster{
		peers:      make(map[uint64]babuzapb.Peer),
		removedIds: make(map[uint64]struct{})}
}

func (m *mockApplyCluster) LocalPeerID() uint64 { return m.localId }
func (m *mockApplyCluster) Peers() []babuzapb.Peer {
	var result []babuzapb.Peer
	for _, p := range m.peers {
		result = append(result, p)
	}
	return result
}

func (m *mockApplyCluster) Add(peer babuzapb.RaftPeerAttribute) error {
	if _, ok := m.peers[peer.Id]; ok {
		return errors.New("ErrPeerIDExists")
	}
	if _, ok := m.removedIds[peer.Id]; ok {
		return errors.New("ErrPeerIDRemoved")
	}
	m.peers[peer.Id] = babuzapb.Peer{
		RaftPeerAttr: peer,
	}
	return nil
}

func (m *mockApplyCluster) Update(peer babuzapb.RaftPeerAttribute) error {
	p, ok := m.peers[peer.Id]
	if !ok {
		return errors.New("ErrPeerIDNotFound")
	}
	p.RaftPeerAttr.RaftListenAddr = peer.RaftListenAddr
	m.peers[peer.Id] = p
	return nil
}

func (m *mockApplyCluster) UpdateAppServiceAddresses(peerID uint64, addresses []string) error {
	return nil
}

func (m *mockApplyCluster) Remove(peerID uint64) error {
	_, ok := m.peers[peerID]
	if !ok {
		return errors.New("ErrPeerIDNotFound")
	}
	delete(m.peers, peerID)
	m.removedIds[peerID] = struct{}{}
	return nil
}

func (m *mockApplyCluster) Promote(peerID uint64) error {
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

func (m *mockApplyCluster) ValidateAndApply(changeType raftpb.ConfChangeType, req babuzapb.ConfChangeRequest) error {
	switch changeType {
	case raftpb.ConfChangeAddNode, raftpb.ConfChangeAddLearnerNode:
		if req.PromoteLearner {
			return m.Promote(req.RaftPeerAttr.Id)
		} else {
			return m.Add(req.RaftPeerAttr)
		}

	case raftpb.ConfChangeRemoveNode:
		return m.Remove(req.RaftPeerAttr.Id)
	case raftpb.ConfChangeUpdateNode:
		return m.Update(req.RaftPeerAttr)
	}
	return fmt.Errorf("cluster: not support changeType(%d)", changeType)
}

func TestApplier_DoExactlyOnce(t *testing.T) {
	sessionMgr := newMockApplySessionManager()
	replier := newMockApplyReplier()
	a := appliedFacadeImpl{
		status:              &mockApplyStatus{},
		firstCommitNotifier: &mockApplyNotifier{},
		sessionMgr:          sessionMgr,
		replier:             replier,
		cluster:             &mockApplyCluster{},
		raftNode:            &mockApplyRaftNode{},
		trans:               &mockApplyTransport{},
		log:                 &logger.Mock{},
	}

	t.Run("find session and apply", func(t *testing.T) {
		s := newMockApplySession(1)
		sessionMgr.sessions[1] = s
		defer delete(sessionMgr.sessions, 1)
		toApply, getSession := a.doExactlyOnce(1, 1, babuzapb.RequestContext{
			SessionID:   1,
			SequenceNum: 1,
		})
		assert.Equal(t, s, getSession)
		assert.Equal(t, true, toApply)
	})

	t.Run("expire session: not found session ", func(t *testing.T) {
		toApply, getSession := a.doExactlyOnce(100, 1, babuzapb.RequestContext{
			ReplyID:     10,
			SessionID:   1,
			SequenceNum: 1,
		})
		assert.Nil(t, getSession)
		assert.Equal(t, false, toApply)
		ar, ok := replier.reply[10]
		assert.Equal(t, true, ok)
		assert.Equal(t, uint64(100), ar.LogIndex)
		assert.Error(t, ar.Error)
	})

	t.Run("find session and clear result", func(t *testing.T) {
		s := newMockApplySession(1)
		s.result[1] = ibabuza.ApplyResult{LogIndex: 1}
		sessionMgr.sessions[1] = s
		defer delete(sessionMgr.sessions, 1)
		toApply, getSession := a.doExactlyOnce(100, 1, babuzapb.RequestContext{
			ReplyID:                   10,
			SessionID:                 1,
			SequenceNum:               2,
			LowestSeqNumNotYetReplied: 2,
		})
		assert.NotNil(t, getSession)
		assert.Equal(t, true, toApply)
		_, ok := s.result[1]
		assert.Equal(t, false, ok)
	})

	t.Run("no operation session", func(t *testing.T) {
		toApply, getSession := a.doExactlyOnce(100, 1, babuzapb.RequestContext{
			ReplyID: 10,
		})
		_, ok := getSession.(*session.NoOPSession)
		assert.Equal(t, true, ok)
		assert.Equal(t, true, toApply)
	})

	t.Run("exactly-once", func(t *testing.T) {
		sid := uint64(1)
		reqSeqNum := uint64(1)
		s := newMockApplySession(sid)
		sessionMgr.sessions[sid] = s
		s.result[reqSeqNum] = ibabuza.ApplyResult{LogIndex: 100}
		defer delete(sessionMgr.sessions, sid)
		toApply, getSession := a.doExactlyOnce(101, 1, babuzapb.RequestContext{
			ReplyID:     1,
			SessionID:   sid,
			SequenceNum: reqSeqNum,
		})
		assert.Equal(t, false, toApply)
		assert.Nil(t, getSession)
		ar1, ok := s.GetResult(reqSeqNum)
		assert.Equal(t, true, ok)
		assert.Equal(t, uint64(100), ar1.LogIndex)
		ar2, ok := replier.reply[sid]
		assert.Equal(t, true, ok)
		assert.Equal(t, ar1, ar2)
	})
}

func TestApplier_ApplyNilEntryInNewTerm(t *testing.T) {
	notifier := &mockApplyNotifier{}
	status := newMockApplyStatus()
	a := appliedFacadeImpl{
		status:              status,
		firstCommitNotifier: notifier,
		sessionMgr:          newMockApplySessionManager(),
		replier:             newMockApplyReplier(),
		cluster:             newMockApplyCluster(),
		raftNode:            &mockApplyRaftNode{},
		trans:               newMockApplyTransport(),
		log:                 &logger.Mock{},
	}

	a.ApplyNilEntryInNewTerm(1, 1)
	assert.Equal(t, true, notifier.close)
	assert.Equal(t, uint64(1), status.index)
	assert.Equal(t, uint64(1), status.term)
}

func TestApplier_ApplyNormalEntry(t *testing.T) {

	a := appliedFacadeImpl{
		storage:             &mockApplyStateMachineAdaptor{},
		firstCommitNotifier: &mockApplyNotifier{},
		raftNode:            &mockApplyRaftNode{},
		cluster:             newMockApplyCluster(),
		trans:               newMockApplyTransport(),
		log:                 &logger.Mock{},
	}

	t.Run("register session", func(t *testing.T) {

		sessionMgr := newMockApplySessionManager()
		status := newMockApplyStatus()
		replier := newMockApplyReplier()

		a.status = status
		a.replier = replier
		a.sessionMgr = sessionMgr

		req := babuzapb.NormalRequest{
			Context: babuzapb.RequestContext{
				ReplyID: 1,
			},
			Register: &babuzapb.RegisterSessionRequest{},
		}
		data, err := req.Marshal()
		assert.Nil(t, err)
		e := raftpb.Entry{
			Term:  1,
			Index: 100,
			Type:  raftpb.EntryNormal,
			Data:  data,
		}
		entry := a.ApplyNormalEntry(e)
		assert.Nil(t, entry)

		assert.Equal(t, status.index, e.Index)
		assert.Equal(t, status.term, e.Term)

		ar, ok := replier.reply[req.Context.ReplyID]
		assert.Equal(t, true, ok)
		assert.Equal(t, e.Index, ar.LogIndex)

		_, ok = sessionMgr.sessions[ar.LogIndex]
		assert.Equal(t, true, ok)
	})

	t.Run("valid session and sequence number", func(t *testing.T) {
		sessionMgr := newMockApplySessionManager()
		status := newMockApplyStatus()
		replier := newMockApplyReplier()

		a.status = status
		a.replier = replier
		a.sessionMgr = sessionMgr

		sId := uint64(1)
		seqNum := uint64(1)
		sessionMgr.sessions[sId] = newMockApplySession(sId)

		req := babuzapb.NormalRequest{
			Context: babuzapb.RequestContext{
				ReplyID:     1,
				SessionID:   sId,
				SequenceNum: seqNum,
			},
			StateMachineLog: []byte{1},
		}
		data, err := req.Marshal()
		assert.Nil(t, err)
		e := raftpb.Entry{
			Term:  1,
			Index: 100,
			Type:  raftpb.EntryNormal,
			Data:  data,
		}
		entry := a.ApplyNormalEntry(e)
		assert.NotNil(t, entry)
		assert.Equal(t, e.Term, entry.Term())
		assert.Equal(t, e.Index, entry.Index())
		assert.Equal(t, []byte{1}, entry.Command())
	})

	t.Run("not found session", func(t *testing.T) {
		sessionMgr := newMockApplySessionManager()
		status := newMockApplyStatus()
		replier := newMockApplyReplier()

		a.status = status
		a.replier = replier
		a.sessionMgr = sessionMgr

		req := babuzapb.NormalRequest{
			Context: babuzapb.RequestContext{
				ReplyID:     1,
				SessionID:   1,
				SequenceNum: 1,
			},
			StateMachineLog: []byte{1},
		}
		data, err := req.Marshal()
		assert.Nil(t, err)
		e := raftpb.Entry{
			Term:  2,
			Index: 1,
			Type:  raftpb.EntryNormal,
			Data:  data,
		}
		entry := a.ApplyNormalEntry(e)
		assert.Nil(t, entry)
		assert.Equal(t, status.term, e.Term)
		assert.Equal(t, status.index, e.Index)

		ar, ok := replier.reply[req.Context.ReplyID]
		assert.Equal(t, true, ok)
		assert.Error(t, ar.Error)
	})

	t.Run("session: exactly-once", func(t *testing.T) {
		sessionMgr := newMockApplySessionManager()
		status := newMockApplyStatus()
		replier := newMockApplyReplier()

		a.status = status
		a.replier = replier
		a.sessionMgr = sessionMgr

		sId := uint64(1)
		seqNum := uint64(1)
		sess := newMockApplySession(sId)
		_ = sess.AddResult(seqNum, 1, ibabuza.ApplyResult{
			LogIndex: 1,
			Response: "hello",
		})
		sessionMgr.sessions[sId] = sess
		req := babuzapb.NormalRequest{
			Context: babuzapb.RequestContext{
				ReplyID:     1,
				SessionID:   sId,
				SequenceNum: seqNum,
			},
			StateMachineLog: []byte("hello"),
		}
		data, err := req.Marshal()
		assert.Nil(t, err)
		e := raftpb.Entry{
			Term:  1,
			Index: 2,
			Type:  raftpb.EntryNormal,
			Data:  data,
		}
		entry := a.ApplyNormalEntry(e)
		assert.Nil(t, entry)
		assert.Equal(t, status.term, e.Term)
		assert.Equal(t, status.index, e.Index)
		ar, ok := replier.reply[req.Context.ReplyID]
		assert.Equal(t, true, ok)
		assert.Equal(t, "hello", ar.Response.(string))
	})

	t.Run("no operation session", func(t *testing.T) {
		smAdaptor := &mockApplyStateMachineAdaptor{}
		sessionMgr := newMockApplySessionManager()
		status := newMockApplyStatus()
		replier := newMockApplyReplier()
		a.storage = smAdaptor
		a.status = status
		a.replier = replier
		a.sessionMgr = sessionMgr

		req := babuzapb.NormalRequest{
			Context: babuzapb.RequestContext{
				ReplyID: 1,
			},
			StateMachineLog: []byte{1},
		}
		data, err := req.Marshal()
		assert.Nil(t, err)
		e := raftpb.Entry{
			Term:  1,
			Index: 1,
			Type:  raftpb.EntryNormal,
			Data:  data,
		}
		entry := a.ApplyNormalEntry(e)
		assert.NotNil(t, entry)
		assert.Equal(t, e.Term, entry.Term())
		assert.Equal(t, e.Index, entry.Index())
		assert.Equal(t, []byte{1}, entry.Command())
	})
	t.Run("disk state machine: already applied", func(t *testing.T) {
		smAdaptor := &mockApplyStateMachineAdaptor{
			index: 10,
		}
		sessionMgr := newMockApplySessionManager()
		status := newMockApplyStatus()
		replier := newMockApplyReplier()
		a.storage = smAdaptor
		a.status = status
		a.replier = replier
		a.sessionMgr = sessionMgr

		req := babuzapb.NormalRequest{
			Context: babuzapb.RequestContext{
				ReplyID: 1,
			},
			StateMachineLog: []byte{1},
		}
		data, err := req.Marshal()
		assert.Nil(t, err)
		e := raftpb.Entry{
			Term:  1,
			Index: 1,
			Type:  raftpb.EntryNormal,
			Data:  data,
		}
		entry := a.ApplyNormalEntry(e)
		assert.Nil(t, entry)
		ne := NewEntry(11, 1, 0, 0, 0, nil, nil, nil)
		a.SendStateMachineAppliedResult(ne, ibabuza.ApplyResult{})
		assert.Equal(t, uint64(11), a.storage.GetStateMachineAppliedIndex())
		e.Index = 11
		assert.Nil(t, a.ApplyNormalEntry(e))
		e.Index = 12
		assert.NotNil(t, a.ApplyNormalEntry(e))
	})
}

func TestApplier_ApplyConfChangeEntry(t *testing.T) {
	a := appliedFacadeImpl{
		firstCommitNotifier: &mockApplyNotifier{},
		raftNode:            &mockApplyRaftNode{},
		log:                 &logger.Mock{},
	}
	closeCh := make(chan struct{})
	go func() {
		for {
			select {
			case <-closeCh:
				return
			}
		}
	}()

	t.Run("valid session and sequence number", func(t *testing.T) {

		sessionMgr := newMockApplySessionManager()
		status := newMockApplyStatus()
		replier := newMockApplyReplier()
		cl := newMockApplyCluster()
		trans := newMockApplyTransport()

		a.status = status
		a.replier = replier
		a.cluster = cl
		a.trans = trans
		a.sessionMgr = sessionMgr

		sId := uint64(1)
		seqNum := uint64(1)
		sess := newMockApplySession(sId)
		sessionMgr.sessions[sId] = sess

		addPeerId := uint64(2)
		replyID := uint64(1)
		req := babuzapb.ConfChangeRequest{
			Context: babuzapb.RequestContext{
				ReplyID:     replyID,
				SessionID:   sId,
				SequenceNum: seqNum,
			},
			RaftPeerAttr: babuzapb.RaftPeerAttribute{
				Id:             addPeerId,
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
		removeSelf := a.ApplyConfChangeEntry(e)
		assert.Equal(t, false, removeSelf)
		assert.Equal(t, status.index, e.Index)
		assert.Equal(t, status.term, e.Term)
		_, ok := cl.peers[addPeerId]
		assert.Equal(t, true, ok)
		_, ok = trans.resolver[addPeerId]
		assert.Equal(t, true, ok)
		ar1, ok := replier.reply[req.Context.ReplyID]
		assert.Equal(t, true, ok)
		ar2, ok := sess.GetResult(seqNum)
		assert.Equal(t, true, ok)
		assert.Equal(t, ar1, ar2)

	})
	t.Run("not found session", func(t *testing.T) {

		sessionMgr := newMockApplySessionManager()
		status := newMockApplyStatus()
		replier := newMockApplyReplier()
		cl := newMockApplyCluster()
		trans := newMockApplyTransport()

		a.status = status
		a.replier = replier
		a.cluster = cl
		a.trans = trans
		a.sessionMgr = sessionMgr

		replyID := uint64(1)
		req := babuzapb.ConfChangeRequest{
			Context: babuzapb.RequestContext{
				ReplyID:     replyID,
				SessionID:   1,
				SequenceNum: 1,
			},
			RaftPeerAttr: babuzapb.RaftPeerAttribute{
				Id:             1,
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

		removeSelf := a.ApplyConfChangeEntry(e)
		assert.Equal(t, false, removeSelf)

		assert.Equal(t, status.index, e.Index)
		assert.Equal(t, status.term, e.Term)
		ar, ok := replier.reply[req.Context.ReplyID]
		assert.Equal(t, true, ok)
		assert.Error(t, ar.Error)
	})

	t.Run("session: exactly-once", func(t *testing.T) {
		sessionMgr := newMockApplySessionManager()
		status := newMockApplyStatus()
		replier := newMockApplyReplier()
		cl := newMockApplyCluster()
		trans := newMockApplyTransport()

		a.status = status
		a.replier = replier
		a.cluster = cl
		a.trans = trans
		a.sessionMgr = sessionMgr

		sId := uint64(1)
		seqNum := uint64(1)
		sess := newMockApplySession(sId)
		addPeerId := uint64(2)
		_ = cl.Add(babuzapb.RaftPeerAttribute{
			Id: addPeerId,
		})
		_ = sess.AddResult(seqNum, 1, ibabuza.ApplyResult{
			Response: nil,
		})
		sessionMgr.sessions[sId] = sess

		replyID := uint64(1)
		req := babuzapb.ConfChangeRequest{
			Context: babuzapb.RequestContext{
				ReplyID:     replyID,
				SessionID:   sId,
				SequenceNum: seqNum,
			},
			RaftPeerAttr: babuzapb.RaftPeerAttribute{
				Id: addPeerId,
			},
		}
		data, err := req.Marshal()
		assert.Nil(t, err)

		cc := raftpb.ConfChange{Type: raftpb.ConfChangeAddNode, NodeID: addPeerId, Context: data}
		data, err = cc.Marshal()
		assert.Nil(t, err)
		e := raftpb.Entry{
			Term:  1,
			Index: 2,
			Type:  raftpb.EntryConfChange,
			Data:  data,
		}

		removeSelf := a.ApplyConfChangeEntry(e)

		assert.Equal(t, false, removeSelf)
		assert.Equal(t, status.index, e.Index)
		assert.Equal(t, status.term, e.Term)
		ar1, ok := replier.reply[req.Context.ReplyID]
		assert.Equal(t, true, ok)
		ar2, ok := sess.GetResult(seqNum)
		assert.Equal(t, true, ok)
		assert.Equal(t, ar1, ar2)
		assert.Nil(t, ar1.Response)
	})

	t.Run("no operation session", func(t *testing.T) {
		sessionMgr := newMockApplySessionManager()
		status := newMockApplyStatus()
		replier := newMockApplyReplier()
		cl := newMockApplyCluster()
		trans := newMockApplyTransport()

		a.status = status
		a.replier = replier
		a.cluster = cl
		a.trans = trans
		a.sessionMgr = sessionMgr

		addPeerId := uint64(2)
		replyID := uint64(1)
		req := babuzapb.ConfChangeRequest{
			Context: babuzapb.RequestContext{
				ReplyID: replyID,
			},
			RaftPeerAttr: babuzapb.RaftPeerAttribute{
				Id:             addPeerId,
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
		removeSelf := a.ApplyConfChangeEntry(e)
		assert.Equal(t, false, removeSelf)
		assert.Equal(t, status.index, e.Index)
		assert.Equal(t, status.term, e.Term)

		_, ok := cl.peers[addPeerId]
		assert.Equal(t, true, ok)
		_, ok = trans.resolver[addPeerId]
		assert.Equal(t, true, ok)
		_, ok = replier.reply[req.Context.ReplyID]
		assert.Equal(t, true, ok)

		// failure: verify cluster
		req.Context.ReplyID = 2
		data, err = req.Marshal()
		assert.Nil(t, err)

		cc = raftpb.ConfChange{Type: raftpb.ConfChangeAddNode, NodeID: addPeerId, Context: data}
		data, err = cc.Marshal()
		assert.Nil(t, err)
		e = raftpb.Entry{
			Term:  1,
			Index: 2,
			Type:  raftpb.EntryConfChange,
			Data:  data,
		}
		removeSelf = a.ApplyConfChangeEntry(e)
		assert.Equal(t, false, removeSelf)
		assert.Equal(t, status.index, e.Index)
		assert.Equal(t, status.term, e.Term)

		ar, ok := replier.reply[req.Context.ReplyID]
		assert.Equal(t, true, ok)
		assert.Equal(t, uint64(2), ar.LogIndex)
		assert.Error(t, ar.Error)
	})

	t.Run("remove self: no operation session", func(t *testing.T) {
		sessionMgr := newMockApplySessionManager()
		status := newMockApplyStatus()
		replier := newMockApplyReplier()
		cl := newMockApplyCluster()
		trans := newMockApplyTransport()

		a.status = status
		a.replier = replier
		a.cluster = cl
		a.trans = trans
		a.sessionMgr = sessionMgr

		cl.localId = 1
		_ = cl.Add(babuzapb.RaftPeerAttribute{
			Id: 1,
		})
		_ = cl.Add(babuzapb.RaftPeerAttribute{
			Id: 2,
		})
		removePeerId := uint64(1)
		replyID := uint64(1)
		req := babuzapb.ConfChangeRequest{
			Context: babuzapb.RequestContext{
				ReplyID: replyID,
			},
			RaftPeerAttr: babuzapb.RaftPeerAttribute{
				Id: removePeerId,
			},
		}
		data, err := req.Marshal()
		assert.Nil(t, err)

		cc := raftpb.ConfChange{Type: raftpb.ConfChangeRemoveNode, NodeID: removePeerId, Context: data}
		data, err = cc.Marshal()
		assert.Nil(t, err)
		e := raftpb.Entry{
			Term:  1,
			Index: 2,
			Type:  raftpb.EntryConfChange,
			Data:  data,
		}
		removeSelf := a.ApplyConfChangeEntry(e)
		assert.Equal(t, true, removeSelf)

		assert.Equal(t, status.index, e.Index)
		assert.Equal(t, status.term, e.Term)
		_, ok := cl.peers[removePeerId]
		assert.Equal(t, false, ok)
		_, ok = trans.resolver[removePeerId]
		assert.Equal(t, false, ok)
		_, ok = replier.reply[req.Context.ReplyID]
		assert.Equal(t, true, ok)
	})
	close(closeCh)
}
