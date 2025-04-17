package raft

import (
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

func EncodeRegisterSessionRequest(replyId uint64) ([]byte, error) {
	req := babuzapb.NormalRequest{
		Context: babuzapb.RequestContext{
			ReplyId: replyId,
		},
		Register: &babuzapb.RegisterSessionRequest{},
	}
	data, err := req.Marshal()
	if err != nil {
		return nil, err
	}
	return data, nil
}

func EncodePubAppServiceAddressesRequest(replyId, peerId uint64, addresses []string) ([]byte, error) {
	req := babuzapb.NormalRequest{
		Context: babuzapb.RequestContext{
			ReplyId: replyId,
		},
		PubAppService: &babuzapb.PubAppServiceRequest{
			PubServicePeerId:    peerId,
			AppServiceAddresses: addresses,
		},
	}
	data, err := req.Marshal()
	if err != nil {
		return nil, err
	}
	return data, nil
}

func EncodeClusterConfigurationChange(replyId uint64, session ClientSession, changeType raftpb.ConfChangeType,
	raftPeerAttr babuzapb.RaftPeerAttribute, promoteLearner bool) (raftpb.ConfChange, error) {
	req := babuzapb.ConfChangeRequest{
		Context: babuzapb.RequestContext{
			ReplyId: replyId,
		},
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
	return raftpb.ConfChange{Type: changeType, NodeID: raftPeerAttr.Id, Context: data}, nil
}

func EncodeProposedLog(replyId uint64, session ClientSession, log []byte) ([]byte, error) {
	req := babuzapb.NormalRequest{
		Context: babuzapb.RequestContext{
			ReplyId: replyId,
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
