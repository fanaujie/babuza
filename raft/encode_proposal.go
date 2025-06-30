package raft

import (
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

func EncodeRegisterSessionRequest(replyID uint64, unregisterSessionID uint64) ([]byte, error) {
	req := babuzapb.NormalRequest{
		Context: babuzapb.RequestContext{
			ReplyID: replyID,
		},
		Register: &babuzapb.RegisterSessionRequest{},
	}
	if unregisterSessionID != 0 {
		req.Register.Unregister = true
		req.Register.SessionID = unregisterSessionID
	}
	data, err := req.Marshal()
	if err != nil {
		return nil, err
	}
	return data, nil
}

func EncodePubAppServiceAddressesRequest(replyID, peerID uint64, addresses []string) ([]byte, error) {
	req := babuzapb.NormalRequest{
		Context: babuzapb.RequestContext{
			ReplyID: replyID,
		},
		PubAppService: &babuzapb.PubAppServiceRequest{
			PubServicePeerID:    peerID,
			AppServiceAddresses: addresses,
		},
	}
	data, err := req.Marshal()
	if err != nil {
		return nil, err
	}
	return data, nil
}

func EncodeClusterConfigurationChange(replyID uint64, session ClientSession, changeType raftpb.ConfChangeType,
	groupID ibabuza.RaftGroupID, raftPeerAttr babuzapb.RaftPeerAttribute, promoteLearner bool) (raftpb.ConfChange, error) {
	req := babuzapb.ConfChangeRequest{
		Context: babuzapb.RequestContext{
			ReplyID: replyID,
		},
		GroupID:        uint64(groupID),
		RaftPeerAttr:   raftPeerAttr,
		PromoteLearner: promoteLearner,
	}
	if session.SessionID != 0 {
		req.Context.SessionID = session.SessionID
		req.Context.SequenceNum = session.SequenceNumber
		req.Context.LowestSeqNumNotYetReplied = session.LowestSequenceNumberNotYetReplied
	}
	data, err := req.Marshal()
	if err != nil {
		return raftpb.ConfChange{}, err
	}
	return raftpb.ConfChange{Type: changeType, NodeID: raftPeerAttr.PeerID, Context: data}, nil
}

func EncodeProposedLog(replyID uint64, session ClientSession, log []byte) ([]byte, error) {
	req := babuzapb.NormalRequest{
		Context: babuzapb.RequestContext{
			ReplyID: replyID,
		},
		StateMachineLog: log,
	}
	if session.SessionID != 0 {
		req.Context.SessionID = session.SessionID
		req.Context.SequenceNum = session.SequenceNumber
		req.Context.LowestSeqNumNotYetReplied = session.LowestSequenceNumberNotYetReplied
	}
	data, err := req.Marshal()
	if err != nil {
		return nil, err
	}
	return data, nil
}
