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


package response

import "github.com/fanaujie/babuza/examples/kvstore/server/kvstore"

type RegisterSessionResponse struct {
	SessionId uint64 `json:"session_id"`
}

type UnregisterSessionResponse struct {
	SessionID      uint64 `json:"session_id"`
	IsUnregistered bool   `json:"is_unregistered"`
}

type ClusterPeer struct {
	Id                uint64 `json:"id"`
	RaftListenAddr    string `json:"raft_listen_addr"`
	IsLearner         bool   `json:"is_learner"`
	AppServiceAddress string `json:"app_service_address"`
}

type ClusterConfigurationResponse struct {
	SessionID      uint64        `json:"session_id"`
	SequenceNumber uint64        `json:"sequence_number"`
	LeaderID       uint64        `json:"leader_id"`
	Peers          []ClusterPeer `json:"peers"`
}

type TransferLeaderResponse struct{}

type KvStoreResponse struct {
	SessionID      uint64 `json:"session_id"`
	SequenceNumber uint64 `json:"sequence_number"`
	kvstore.KvResult
}
