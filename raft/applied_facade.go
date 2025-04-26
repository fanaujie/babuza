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
}

type AppliedFirstCommitInTermNotifier interface {
	CloseAndRenew()
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
	ClusterID() uint64
	LocalPeerID() uint64
	Add(babuzapb.RaftPeerAttribute) error
	Update(babuzapb.RaftPeerAttribute) error
	Remove(peerID uint64) error
	Promote(peerID uint64) error
	UpdateAppServiceAddresses(uint64, []string) error
}

type AppliedRaftNode interface {
	ApplyConfChange(clusterID uint64, cc raftpb.ConfChangeI) (*raftpb.ConfState, error)
}

type AppliedTransport interface {
	AddPeer(uint64, string)
	UpdatePeer(uint64, string)
	RemovePeer(uint64)
}

type appliedFacadeImpl struct {
	storage             AppliedStorage
	firstCommitNotifier AppliedFirstCommitInTermNotifier
	sessionMgr          AppliedSessionManager
	replier             AppliedReplier
	cluster             AppliedCluster
	raftNode            AppliedRaftNode
	trans               AppliedTransport
	log                 ibabuza.Logger
	metricsCollector    ibabuza.MetricsCollector
}

func NewAppliedFacade(storage AppliedStorage, firstCommitNotifier AppliedFirstCommitInTermNotifier,
	sessionMgr AppliedSessionManager, replier AppliedReplier, cluster AppliedCluster, raftNode AppliedRaftNode,
	trans AppliedTransport, log ibabuza.Logger, metricsCollector ibabuza.MetricsCollector) InternalAppliedFacade {
	return &appliedFacadeImpl{
		storage:             storage,
		firstCommitNotifier: firstCommitNotifier,
		sessionMgr:          sessionMgr,
		replier:             replier,
		cluster:             cluster,
		raftNode:            raftNode,
		trans:               trans,
		log:                 log,
		metricsCollector:    metricsCollector,
	}
}

func newAppliedFacadeFromRaft(r *Raft) *appliedFacadeImpl {

	return &appliedFacadeImpl{
		storage:             r.storage,
		firstCommitNotifier: r.firstCommitInTermNotifier,
		sessionMgr:          r.sessionMgr,
		replier:             r.resultReplier,
		cluster:             r.cluster,
		raftNode:            r,
		trans:               r.trans,
		log:                 r.logger,
		metricsCollector:    r.metricsCollector,
	}
}

func (a *appliedFacadeImpl) ApplyNilEntryInNewTerm(index, term uint64) {
	a.firstCommitNotifier.CloseAndRenew()
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
		return nil
	}
	return NewEntry(
		e.Index,
		e.Term,
		req.Context.ReplyID,
		req.Context.SequenceNum,
		reqTime,
		req.StateMachineLog,
		session,
		a,
	)
}

func (a *appliedFacadeImpl) ApplyConfChangeEntry(entry raftpb.Entry) (*raftpb.ConfState, bool) {
	cc, confReq, err := a.parseConfChangeEntry(entry)
	if err != nil {
		a.log.Panicf("Failed to parse conf change: %v", err)
	}
	reqTime := time.Now().UnixNano()
	toApply, session := a.doExactlyOnce(entry.Index, reqTime, confReq.Context)
	if !toApply {
		return nil, false
	}
	confChange, removeSelf, err := a.processConfChange(cc, confReq)
	a.sendConfChangeResult(session, confReq.Context, entry.Index, err)
	return confChange, removeSelf
}

func (a *appliedFacadeImpl) SendStateMachineAppliedResult(e *Entry, ar ibabuza.ApplyResult) {
	index := e.Index()
	a.replier.SendResult(e.ReplyId(), ar)
	a.storage.SetStateMachineAppliedIndex(index)
	e.Release()
}

func (a *appliedFacadeImpl) doExactlyOnce(index uint64, requestTime int64, ctx babuzapb.RequestContext) (bool, ibabuza.Session) {
	a.sessionMgr.ExpireSession(requestTime)
	sess, err := a.sessionMgr.GetSession(ctx.SessionID)
	if err != nil {
		a.replier.SendResult(ctx.ReplyID, ibabuza.ApplyResult{
			LogIndex: index,
			Error:    err,
		})
		return false, nil
	}
	defer sess.ClearResult(ctx.LowestSeqNumNotYetReplied)
	if sess.RepeatSequenceNum(ctx.SequenceNum) {
		if ar, ok := sess.GetResult(ctx.SequenceNum); ok == false {
			err = fmt.Errorf("seesion id(%d) seqence nume(%d): not found apply result", ctx.SessionID, ctx.SequenceNum)
			a.replier.SendResult(ctx.ReplyID, ibabuza.ApplyResult{
				LogIndex: index,
				Error:    err,
			})
		} else {
			a.replier.SendResult(ctx.ReplyID, ar)
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

func (a *appliedFacadeImpl) processConfChange(cc raftpb.ConfChange, confReq babuzapb.ConfChangeRequest) (*raftpb.ConfState, bool, error) {
	if err := a.clusterValidateAndApply(cc.Type, confReq); err != nil {
		cc.NodeID = raft.None
		_, _ = a.raftNode.ApplyConfChange(a.cluster.ClusterID(), cc)
		return nil, false, err
	}
	applyResult, err := a.raftNode.ApplyConfChange(a.cluster.ClusterID(), cc)
	if err != nil {
		return nil, false, err
	}
	var removeSelf bool

	switch cc.Type {
	case raftpb.ConfChangeAddNode, raftpb.ConfChangeAddLearnerNode:
		if !confReq.PromoteLearner && confReq.RaftPeerAttr.Id != a.cluster.LocalPeerID() {
			a.trans.AddPeer(confReq.RaftPeerAttr.Id, confReq.RaftPeerAttr.RaftListenAddr)
		}
		if confReq.RaftPeerAttr.Id == a.cluster.LocalPeerID() {
			if cc.Type == raftpb.ConfChangeAddLearnerNode {
				a.metricsCollector.SetIsLearner(1)
			} else {
				a.metricsCollector.SetIsLearner(0)
			}
		}

	case raftpb.ConfChangeRemoveNode:
		if cc.NodeID == a.cluster.LocalPeerID() {
			removeSelf = true
		} else {
			a.trans.RemovePeer(confReq.RaftPeerAttr.Id)
		}
	case raftpb.ConfChangeUpdateNode:
		if confReq.RaftPeerAttr.Id != a.cluster.LocalPeerID() {
			a.trans.UpdatePeer(confReq.RaftPeerAttr.Id, confReq.RaftPeerAttr.RaftListenAddr)
		}
	}

	return applyResult, removeSelf, nil
}

func (a *appliedFacadeImpl) sendConfChangeResult(session ibabuza.Session, ctx babuzapb.RequestContext, index uint64,
	err error) {
	ar := ibabuza.ApplyResult{
		LogIndex: index,
		Error:    err,
	}
	_ = session.AddResult(ctx.SequenceNum, time.Now().UnixNano(), ar)
	a.replier.SendResult(ctx.ReplyID, ar)
}

func (a *appliedFacadeImpl) handleSessionRegister(e raftpb.Entry, req babuzapb.NormalRequest, reqTime int64) {
	a.sessionMgr.Register(e.Index, reqTime)
	a.replier.SendResult(req.Context.ReplyID, ibabuza.ApplyResult{
		LogIndex: e.Index,
	})
}

func (a *appliedFacadeImpl) handlePubAppService(e raftpb.Entry, req babuzapb.NormalRequest) {
	result := a.cluster.UpdateAppServiceAddresses(
		req.PubAppService.PubServicePeerID,
		req.PubAppService.AppServiceAddresses,
	)

	a.replier.SendResult(req.Context.ReplyID, ibabuza.ApplyResult{
		LogIndex: e.Index,
		Response: result,
	})
}
