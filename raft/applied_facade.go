package raft

import (
	"fmt"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"time"
)

type AppliedStatus interface {
	SetAppliedIndex(uint64)
	SetAppliedTerm(uint64)
	SetConfState(raftpb.ConfState)
	GetHardStateTerm() uint64
}

type AppliedFirstCommitInTermNotifier interface {
	Reset()
}

type AppliedSessionManager interface {
	Register(uint64, int64) error
	UnRegister(uint64) error
	ExpireSession(int64)
	GetSession(uint64) (ibabuza.Session, error)
}

type AppliedReplier interface {
	SendResult(uint64, ibabuza.ApplyResult)
}

type AppliedCluster interface {
	ClusterID() uint64
	GroupID() ibabuza.RaftGroupID
	LocalPeerID() uint64
	Peer(peerID uint64) (babuzapb.Peer, error)
	Add(babuzapb.RaftPeerAttribute) error
	Update(uint64, babuzapb.RaftPeerAttribute) error
	Remove(peerID uint64) error
	Promote(peerID uint64) error
	UpdateAppServiceAddresses(uint64, []string) error
}

type AppliedRaftNode interface {
	ApplyConfChange(clusterID uint64, cc raftpb.ConfChangeI) (*raftpb.ConfState, error)
}

type AppliedTransport interface {
	AddPeer(ibabuza.RaftGroupID, uint64, string)
	UpdatePeer(ibabuza.RaftGroupID, uint64, string)
	RemovePeer(ibabuza.RaftGroupID, uint64)
}

type AppliedPublishMemberEvent interface {
	Publish(event ibabuza.RaftEvent)
}

type appliedTransportAdaptor struct {
	trans ibabuza.Transport
}

func (m *appliedTransportAdaptor) AddPeer(id ibabuza.RaftGroupID, peerID uint64, raftListenAddr string) {
	m.trans.AddPeer(peerID, raftListenAddr)
}

func (m *appliedTransportAdaptor) UpdatePeer(id ibabuza.RaftGroupID, peerID uint64, raftListenAddr string) {
	m.trans.UpdatePeer(peerID, raftListenAddr)
}

func (m *appliedTransportAdaptor) RemovePeer(id ibabuza.RaftGroupID, peerID uint64) {
	m.trans.RemovePeer(peerID)
}

type appliedFacadeImpl struct {
	firstCommitNotifier AppliedFirstCommitInTermNotifier
	sessionManager      AppliedSessionManager
	replier             AppliedReplier
	cluster             AppliedCluster
	raftNode            AppliedRaftNode
	trans               AppliedTransport
	memberEvent         AppliedPublishMemberEvent
	log                 ibabuza.Logger
	metricsCollector    ibabuza.MetricsCollector
}

func NewAppliedFacade(firstCommitNotifier AppliedFirstCommitInTermNotifier,
	sessionMgr AppliedSessionManager, replier AppliedReplier, cluster AppliedCluster, raftNode AppliedRaftNode,
	trans AppliedTransport, memberEvent AppliedPublishMemberEvent, log ibabuza.Logger,
	metricsCollector ibabuza.MetricsCollector) InternalAppliedFacade {

	return &appliedFacadeImpl{
		firstCommitNotifier: firstCommitNotifier,
		sessionManager:      sessionMgr,
		replier:             replier,
		cluster:             cluster,
		raftNode:            raftNode,
		trans:               trans,
		memberEvent:         memberEvent,
		log:                 log,
		metricsCollector:    metricsCollector,
	}
}

func newAppliedFacadeFromRaft(r *Raft) *appliedFacadeImpl {

	return &appliedFacadeImpl{
		firstCommitNotifier: r.firstCommitInTermNotifier,
		sessionManager:      r.sessionManager,
		replier:             r.resultReplier,
		cluster:             r.cluster,
		raftNode:            r,
		trans: &appliedTransportAdaptor{
			r.trans,
		},
		memberEvent:      r.raftEventPublisher,
		log:              r.logger,
		metricsCollector: r.metricsCollector,
	}
}

func (a *appliedFacadeImpl) ApplyNilEntryInNewTerm(index, term uint64) {
	a.firstCommitNotifier.Reset()
}

func (a *appliedFacadeImpl) ApplyNormalEntry(e raftpb.Entry) (babuzapb.NormalRequest, ibabuza.ApplyResult, ibabuza.Session) {
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
		noOpSession, _ := a.sessionManager.GetSession(0)
		return req, a.handleSessionRegister(e, reqTime, req.Register), noOpSession
	} else if req.PubAppService != nil {
		noOpSession, _ := a.sessionManager.GetSession(0)
		return req, a.handlePubAppService(e, req), noOpSession
	}
	a.sessionManager.ExpireSession(reqTime)
	session, err := a.sessionManager.GetSession(req.Context.SessionID)
	if err != nil {
		noOpSession, _ := a.sessionManager.GetSession(0)
		return req, ibabuza.ApplyResult{
			LogIndex: e.Index,
			Error:    err,
		}, noOpSession
	}
	toApply, ar := a.doExactlyOnce(e.Index, req.Context, session)
	if !toApply {
		return req, ar, session
	}
	return req, ibabuza.ApplyResult{}, session
}

func (a *appliedFacadeImpl) ApplyConfChangeEntry(entry raftpb.Entry) (babuzapb.RequestContext, ibabuza.ApplyResult, bool) {
	cc, confReq, err := a.parseConfChangeEntry(entry)
	if err != nil {
		a.log.Panicf("Failed to parse conf change: %v", err)
	}
	reqTime := time.Now().UnixNano()
	a.sessionManager.ExpireSession(reqTime)
	session, err := a.sessionManager.GetSession(confReq.Context.SessionID)
	if err != nil {
		return confReq.Context, ibabuza.ApplyResult{
			LogIndex: entry.Index,
			Error:    err,
		}, false
	}
	toApply, ar := a.doExactlyOnce(entry.Index, confReq.Context, session)
	if !toApply {
		return confReq.Context, ar, false
	}
	confChange, removeSelf, err := a.processConfChange(cc, confReq)
	ar = ibabuza.ApplyResult{
		LogIndex: entry.Index,
		Response: confChange,
		Error:    err,
	}
	if err = session.AddResult(confReq.Context.SequenceNum, reqTime, ar); err != nil {
		a.log.Panicf("Failed to add result: %v", err)
	}
	return confReq.Context, ar, removeSelf
}

func (a *appliedFacadeImpl) SendAppliedResult(replyID uint64, ar ibabuza.ApplyResult) {
	a.replier.SendResult(replyID, ar)
}

func (a *appliedFacadeImpl) doExactlyOnce(index uint64, ctx babuzapb.RequestContext, session ibabuza.Session) (bool, ibabuza.ApplyResult) {
	defer session.ClearResult(ctx.LowestSeqNumNotYetReplied)

	if session.RepeatSequenceNum(ctx.SequenceNum) {
		if ar, ok := session.GetResult(ctx.SequenceNum); ok == false {
			return false, ibabuza.ApplyResult{
				LogIndex: index,
				Error:    fmt.Errorf("seesion id(%d) seqence nume(%d): not found apply result", ctx.SessionID, ctx.SequenceNum),
			}
		} else {
			return false, ar
		}
	}
	return true, ibabuza.ApplyResult{}
}

func (a *appliedFacadeImpl) clusterValidateAndApply(changeType raftpb.ConfChangeType, req babuzapb.ConfChangeRequest) error {

	switch changeType {
	case raftpb.ConfChangeAddNode, raftpb.ConfChangeAddLearnerNode:
		if req.PromoteLearner {
			return a.cluster.Promote(req.RaftPeerAttr.PeerID)
		} else {
			return a.cluster.Add(req.RaftPeerAttr)
		}

	case raftpb.ConfChangeRemoveNode:
		return a.cluster.Remove(req.RaftPeerAttr.PeerID)
	case raftpb.ConfChangeUpdateNode:
		return a.cluster.Update(req.RaftPeerAttr.PeerID, req.RaftPeerAttr)
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

	if cc.NodeID != confReq.RaftPeerAttr.PeerID {
		return cc, confReq, fmt.Errorf("node ID mismatch: %d != %d", cc.NodeID,
			confReq.RaftPeerAttr.PeerID)
	}

	return cc, confReq, nil
}

func (a *appliedFacadeImpl) processConfChange(cc raftpb.ConfChange, confReq babuzapb.ConfChangeRequest) (*raftpb.ConfState, bool, error) {
	if err := a.clusterValidateAndApply(cc.Type, confReq); err != nil {
		cc.NodeID = raft.None
		_, _ = a.raftNode.ApplyConfChange(uint64(a.cluster.GroupID()), cc)
		return nil, false, err
	}
	confState, err := a.raftNode.ApplyConfChange(uint64(a.cluster.GroupID()), cc)
	if err != nil {
		return nil, false, err
	}
	var removeSelf bool
	a.processMemberEvent(cc.Type, confReq)
	switch cc.Type {
	case raftpb.ConfChangeAddNode, raftpb.ConfChangeAddLearnerNode:
		if !confReq.PromoteLearner && confReq.RaftPeerAttr.PeerID != a.cluster.LocalPeerID() {
			a.trans.AddPeer(ibabuza.RaftGroupID(confReq.GroupID), confReq.RaftPeerAttr.PeerID,
				confReq.RaftPeerAttr.RaftListenAddr)
		}
		if confReq.RaftPeerAttr.PeerID == a.cluster.LocalPeerID() {
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
			a.trans.RemovePeer(ibabuza.RaftGroupID(confReq.GroupID), confReq.RaftPeerAttr.PeerID)
		}
	case raftpb.ConfChangeUpdateNode:
		if confReq.RaftPeerAttr.PeerID != a.cluster.LocalPeerID() {
			a.trans.UpdatePeer(ibabuza.RaftGroupID(confReq.GroupID),
				confReq.RaftPeerAttr.PeerID, confReq.RaftPeerAttr.RaftListenAddr)
		}
	}

	return confState, removeSelf, nil
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

func (a *appliedFacadeImpl) handleSessionRegister(e raftpb.Entry, reqTime int64, req *babuzapb.RegisterSessionRequest) ibabuza.ApplyResult {
	var err error
	if req.Unregister {
		err = a.sessionManager.UnRegister(req.SessionID)
	} else {
		err = a.sessionManager.Register(e.Index, reqTime)
	}
	return ibabuza.ApplyResult{
		LogIndex: e.Index,
		Error:    err,
	}
}

func (a *appliedFacadeImpl) handlePubAppService(e raftpb.Entry, req babuzapb.NormalRequest) ibabuza.ApplyResult {
	result := a.cluster.UpdateAppServiceAddresses(
		req.PubAppService.PubServicePeerID,
		req.PubAppService.AppServiceAddresses,
	)
	return ibabuza.ApplyResult{
		LogIndex: e.Index,
		Response: result,
	}
}

func (a *appliedFacadeImpl) processMemberEvent(ccType raftpb.ConfChangeType, confReq babuzapb.ConfChangeRequest) {
	switch ccType {
	case raftpb.ConfChangeAddNode:
		if confReq.PromoteLearner {
			a.publishMemberEvent(ibabuza.LeanerPromoted, ibabuza.RaftGroupID(confReq.GroupID), confReq.RaftPeerAttr.PeerID)
		} else {
			a.publishMemberEvent(ibabuza.MemberJoined, ibabuza.RaftGroupID(confReq.GroupID), confReq.RaftPeerAttr.PeerID)
		}
	case raftpb.ConfChangeAddLearnerNode:
		a.publishMemberEvent(ibabuza.LeanerAdded, ibabuza.RaftGroupID(confReq.GroupID), confReq.RaftPeerAttr.PeerID)
	case raftpb.ConfChangeRemoveNode:
		a.publishMemberEvent(ibabuza.MemberRemoved, ibabuza.RaftGroupID(confReq.GroupID), confReq.RaftPeerAttr.PeerID)
	case raftpb.ConfChangeUpdateNode:
		a.publishMemberEvent(ibabuza.MemberUpdated, ibabuza.RaftGroupID(confReq.GroupID), confReq.RaftPeerAttr.PeerID)
	}
}

func (a *appliedFacadeImpl) publishMemberEvent(event int, groupID ibabuza.RaftGroupID, peerID uint64) {
	if a.memberEvent != nil {
		a.memberEvent.Publish(ibabuza.RaftEvent{
			Event:   event,
			GroupID: groupID,
			PeerID:  peerID,
		})
	}
}
