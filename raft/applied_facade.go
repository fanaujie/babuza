package raft

import (
	"fmt"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"time"
)

type AppliedStorage interface {
	GetStateMachineAppliedIndex() uint64
	SetStateMachineAppliedIndex(index uint64)
}

type AppliedStatus interface {
	SetAppliedIndex(uint64)
	SetAppliedTerm(uint64)
	SetConfState(raftpb.ConfState)
	GetHardStateTerm() uint64
	IsPublishServiceMarkDone() bool
}

type AppliedFirstCommitInTermNotifier interface {
	CloseChanAndRenew()
}

type AppliedSessionManager interface {
	Register(uint64, int64)
	ExpireSession(int64)
	GetSession(uint64) (ibabuza.Session, error)
}

type AppliedReplier interface {
	SendResult(uint64, ibabuza.ApplyResult)
}

type AppliedCluster interface {
	LocalPeerID() uint64
	Add(babuzapb.RaftPeerAttribute) error
	Update(babuzapb.RaftPeerAttribute) error
	Remove(peerId uint64) error
	Promote(peerId uint64) error
	UpdateAppServiceAddresses(uint64, []string) error
}

type AppliedRaftNode interface {
	ApplyConfChange(cc raftpb.ConfChangeI) *raftpb.ConfState
}

type AppliedTransport interface {
	AddPeer(uint64, string)
	UpdatePeer(uint64, string)
	RemovePeer(uint64)
}

type appliedFacadeImpl struct {
	storage              AppliedStorage
	status               AppliedStatus
	firstCommitNotifier  AppliedFirstCommitInTermNotifier
	sessionMgr           AppliedSessionManager
	replier              AppliedReplier
	cluster              AppliedCluster
	raftNode             AppliedRaftNode
	trans                AppliedTransport
	clusterMemberEventCh chan ClusterMemberEvent
	log                  ibabuza.Logger
}

func newAppliedFacade(storage AppliedStorage, status AppliedStatus, firstCommitNotifier AppliedFirstCommitInTermNotifier,
	sessionMgr AppliedSessionManager, replier AppliedReplier, cluster AppliedCluster, raftNode AppliedRaftNode,
	trans AppliedTransport, clusterMemberEventCh chan ClusterMemberEvent, log ibabuza.Logger) *appliedFacadeImpl {

	return &appliedFacadeImpl{
		storage:              storage,
		status:               status,
		firstCommitNotifier:  firstCommitNotifier,
		sessionMgr:           sessionMgr,
		replier:              replier,
		cluster:              cluster,
		raftNode:             raftNode,
		trans:                trans,
		clusterMemberEventCh: clusterMemberEventCh,
		log:                  log,
	}
}

func newAppliedFacadeFromRaft(r *Raft) *appliedFacadeImpl {

	return &appliedFacadeImpl{
		storage:              r.storage,
		status:               r.status,
		firstCommitNotifier:  r.firstCommitInTermNotifier,
		sessionMgr:           r.sessionMgr,
		replier:              r.resultReplier,
		cluster:              r.cluster,
		raftNode:             r.raftNode,
		trans:                r.trans,
		clusterMemberEventCh: r.clusterMemberEventCh,
		log:                  r.logger,
	}
}

func (a *appliedFacadeImpl) ApplyNilEntryInNewTerm(index, term uint64) {
	a.firstCommitNotifier.CloseChanAndRenew()
	a.status.SetAppliedIndex(index)
	a.status.SetAppliedTerm(term)
}

func (a *appliedFacadeImpl) ApplyNormalEntry(e raftpb.Entry) ibabuza.Entry {
	var req babuzapb.NormalRequest
	if err := req.Unmarshal(e.Data); err != nil {
		a.log.Errorf("CRITICAL: Failed to unmarshal entry at index %d, term %d: %v",
			e.Index, e.Term, err)
		a.log.Errorf("Entry data (hex): %x", e.Data)
		panic(fmt.Errorf("critical unmarshal error at index %d, term %d: %w",
			e.Index, e.Term, err))
	}

	reqTime := time.Now().UnixNano()
	if req.Register != nil {
		a.handleSessionRegister(e, req, reqTime)
		return nil
	} else if req.PubAppService != nil {
		a.handlePubAppService(e, req)
		return nil
	}
	toApply, session := a.doExactlyOnce(e.Index, reqTime, req.Context)
	if !toApply || e.Index <= a.storage.GetStateMachineAppliedIndex() {
		a.updateAppliedIndexAndTerm(e.Index, e.Term)
		return nil
	}
	return NewEntry(
		e.Index,
		e.Term,
		req.Context.ReplyId,
		req.Context.SequenceNum,
		reqTime,
		req.StateMachineLog,
		a.status.IsPublishServiceMarkDone(),
		session,
		a,
	)
}

func (a *appliedFacadeImpl) ApplyConfChangeEntry(entry raftpb.Entry) bool {
	cc, confReq, err := a.parseConfChangeEntry(entry)
	if err != nil {
		a.log.Panicf("Failed to parse conf change: %v", err)
	}
	reqTime := time.Now().UnixNano()
	toApply, session := a.doExactlyOnce(entry.Index, reqTime, confReq.Context)
	if !toApply {
		a.updateAppliedIndexAndTerm(entry.Index, entry.Term)
		return false
	}
	removeSelf, result := a.processConfChange(cc, confReq)
	a.sendConfChangeResult(session, confReq.Context, entry.Index, result)
	a.updateAppliedIndexAndTerm(entry.Index, entry.Term)
	return removeSelf
}

func (a *appliedFacadeImpl) SendStateMachineAppliedResult(e *Entry, ar ibabuza.ApplyResult) {
	index := e.Index()
	a.replier.SendResult(e.ReplyId(), ar)
	a.status.SetAppliedTerm(e.Term())
	a.status.SetAppliedIndex(index)
	a.storage.SetStateMachineAppliedIndex(index)
	e.Release()
}

func (a *appliedFacadeImpl) doExactlyOnce(index uint64, requestTime int64, ctx babuzapb.RequestContext) (bool, ibabuza.Session) {
	a.sessionMgr.ExpireSession(requestTime)
	sess, err := a.sessionMgr.GetSession(ctx.SessionID)
	if err != nil {
		a.replier.SendResult(ctx.ReplyId, ibabuza.ApplyResult{
			LogIndex: index,
			Response: err,
		})
		return false, nil
	}
	defer sess.ClearResult(ctx.LowestSeqNumNotYetReplied)
	if sess.RepeatSequenceNum(ctx.SequenceNum) {
		if ar, ok := sess.GetResult(ctx.SequenceNum); ok == false {
			err = fmt.Errorf("seesion id(%d) seqence nume(%d): not found apply result", ctx.SessionID, ctx.SequenceNum)
			a.replier.SendResult(ctx.ReplyId, ibabuza.ApplyResult{
				LogIndex: index,
				Response: err,
			})
		} else {
			a.replier.SendResult(ctx.ReplyId, ar)
		}
		return false, nil
	}
	return true, sess
}

func (a *appliedFacadeImpl) clusterValidateAndApply(changeType raftpb.ConfChangeType, req babuzapb.ConfChangeRequest) error {

	switch changeType {
	case raftpb.ConfChangeAddNode, raftpb.ConfChangeAddLearnerNode:
		if req.PromoteLearner {
			return a.cluster.Promote(req.RaftPeerAttr.Id)
		} else {
			return a.cluster.Add(req.RaftPeerAttr)
		}

	case raftpb.ConfChangeRemoveNode:
		return a.cluster.Remove(req.RaftPeerAttr.Id)
	case raftpb.ConfChangeUpdateNode:
		return a.cluster.Update(req.RaftPeerAttr)
	}
	return fmt.Errorf("cluster: not support changeType(%d)", changeType)
}

func (a *appliedFacadeImpl) parseConfChangeEntry(entry raftpb.Entry) (raftpb.ConfChange, babuzapb.ConfChangeRequest, error) {
	var cc raftpb.ConfChange
	var confReq babuzapb.ConfChangeRequest

	if err := cc.Unmarshal(entry.Data); err != nil {
		return cc, confReq, fmt.Errorf("unmarshal conf change: %w", err)
	}

	if err := confReq.Unmarshal(cc.Context); err != nil {
		return cc, confReq, fmt.Errorf("unmarshal conf request: %w", err)
	}

	if cc.NodeID != confReq.RaftPeerAttr.Id {
		return cc, confReq, fmt.Errorf("node ID mismatch: %d != %d", cc.NodeID, confReq.RaftPeerAttr.Id)
	}

	return cc, confReq, nil
}

func (a *appliedFacadeImpl) processConfChange(cc raftpb.ConfChange, confReq babuzapb.ConfChangeRequest) (bool, error) {
	res := a.clusterValidateAndApply(cc.Type, confReq)
	if res != nil {
		cc.NodeID = raft.None
		a.raftNode.ApplyConfChange(cc)
		return false, res
	}

	pubServiceDone := a.status.IsPublishServiceMarkDone()
	a.status.SetConfState(*a.raftNode.ApplyConfChange(cc))

	var removeSelf bool

	switch cc.Type {
	case raftpb.ConfChangeAddNode, raftpb.ConfChangeAddLearnerNode:
		if !confReq.PromoteLearner && confReq.RaftPeerAttr.Id != a.cluster.LocalPeerID() {
			a.trans.AddPeer(confReq.RaftPeerAttr.Id, confReq.RaftPeerAttr.RaftListenAddr)
		}
		if pubServiceDone {
			a.clusterMemberEventCh <- ClusterMemberEvent{
				Event: MemberJoinEvent,
				Peer:  confReq.RaftPeerAttr,
			}
		}

	case raftpb.ConfChangeRemoveNode:
		if cc.NodeID == a.cluster.LocalPeerID() {
			removeSelf = true
		}
		a.trans.RemovePeer(confReq.RaftPeerAttr.Id)
		if pubServiceDone {
			a.clusterMemberEventCh <- ClusterMemberEvent{
				Event: MemberLeaveEvent,
				Peer:  confReq.RaftPeerAttr,
			}
		}

	case raftpb.ConfChangeUpdateNode:
		if confReq.RaftPeerAttr.Id != a.cluster.LocalPeerID() {
			a.trans.UpdatePeer(confReq.RaftPeerAttr.Id, confReq.RaftPeerAttr.RaftListenAddr)
		}
		if pubServiceDone {
			a.clusterMemberEventCh <- ClusterMemberEvent{
				Event: MemberUpdateEvent,
				Peer:  confReq.RaftPeerAttr,
			}
		}
	}

	return removeSelf, nil
}

func (a *appliedFacadeImpl) sendConfChangeResult(session ibabuza.Session, ctx babuzapb.RequestContext, index uint64, response error) {
	ar := ibabuza.ApplyResult{
		LogIndex: index,
		Response: response,
	}
	_ = session.AddResult(ctx.SequenceNum, time.Now().UnixNano(), ar)
	a.replier.SendResult(ctx.ReplyId, ar)
}

func (a *appliedFacadeImpl) handleSessionRegister(e raftpb.Entry, req babuzapb.NormalRequest, reqTime int64) {
	a.sessionMgr.Register(e.Index, reqTime)
	a.replier.SendResult(req.Context.ReplyId, ibabuza.ApplyResult{
		LogIndex: e.Index,
	})
	a.updateAppliedIndexAndTerm(e.Index, e.Term)
}

func (a *appliedFacadeImpl) handlePubAppService(e raftpb.Entry, req babuzapb.NormalRequest) {
	result := a.cluster.UpdateAppServiceAddresses(
		req.PubAppService.Id,
		req.PubAppService.AppServiceAddresses,
	)

	a.replier.SendResult(req.Context.ReplyId, ibabuza.ApplyResult{
		LogIndex: e.Index,
		Response: result,
	})
	a.updateAppliedIndexAndTerm(e.Index, e.Term)
}

func (a *appliedFacadeImpl) updateAppliedIndexAndTerm(index, term uint64) {
	a.status.SetAppliedIndex(index)
	a.status.SetAppliedTerm(term)
}
