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


package request

type Peer struct {
}

type JoinPeerRequest struct {
	SessionID                         uint64 `json:"session_id"`
	SequenceNumber                    uint64 `json:"sequence_number"`
	LowestSequenceNumberNotYetReplied uint64 `json:"lowest_sequence_number_not_yet_replied"`
	RaftPeerId                        uint64 `json:"raft_peer_id"`
	RaftListenAddr                    string `json:"raft_listen_addr"`
	IsLearner                         bool   `json:"is_learner"`
}

type UpdatePeerRequest struct {
	SessionID                         uint64 `json:"session_id"`
	SequenceNumber                    uint64 `json:"sequence_number"`
	LowestSequenceNumberNotYetReplied uint64 `json:"lowest_sequence_number_not_yet_replied"`
	RaftPeerId                        uint64 `json:"raft_peer_id"`
	RaftListenAddr                    string `json:"raft_listen_addr"`
}

type RemovePeerRequest struct {
	SessionID                         uint64 `json:"session_id"`
	SequenceNumber                    uint64 `json:"sequence_number"`
	LowestSequenceNumberNotYetReplied uint64 `json:"lowest_sequence_number_not_yet_replied"`
	RaftPeerId                        uint64 `json:"raft_peer_id"`
}

type PromoteLearnerRequest RemovePeerRequest

type TransferLeaderRequest struct {
	SessionID                         uint64 `json:"session_id"`
	SequenceNumber                    uint64 `json:"sequence_number"`
	LowestSequenceNumberNotYetReplied uint64 `json:"lowest_sequence_number_not_yet_replied"`
	Transferee                        uint64 `json:"transferee"`
}

type KvStoreSetRequest struct {
	SessionID                         uint64 `json:"session_id"`
	SequenceNumber                    uint64 `json:"sequence_number"`
	LowestSequenceNumberNotYetReplied uint64 `json:"lowest_sequence_number_not_yet_replied"`
	Key                               string `json:"key"`
	Value                             string `json:"value"`
}

type KvStoreAppendRequest KvStoreSetRequest
type KvStoreDeleteRequest struct {
	SessionID                         uint64 `json:"session_id"`
	SequenceNumber                    uint64 `json:"sequence_number"`
	LowestSequenceNumberNotYetReplied uint64 `json:"lowest_sequence_number_not_yet_replied"`
	Key                               string `json:"key"`
}
