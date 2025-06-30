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
