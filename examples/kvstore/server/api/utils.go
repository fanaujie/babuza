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


package api

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/fanaujie/babuza/examples/kvstore/server/response"
	"github.com/fanaujie/babuza/raft"
	"net/http"
)

func processRaftProposeError(err error, w http.ResponseWriter) {
	if errors.Is(err, raft.ErrNotLeader) {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	} else if errors.Is(err, context.DeadlineExceeded) {
		http.Error(w, err.Error(), http.StatusGatewayTimeout)
		return
	}
	http.Error(w, err.Error(), http.StatusInternalServerError)
}

func writeHttpResponse(w http.ResponseWriter, response any) error {
	w.Header().Set("Content-Type", "application/json")
	return json.NewEncoder(w).Encode(response)
}

func convertRaftClusterPeersToResponse(babuza *raft.Raft, sessionId, sessionSeqNum uint64) *response.ClusterConfigurationResponse {
	babuzaResponse := babuza.ClusterInfo()
	res := &response.ClusterConfigurationResponse{
		SessionID:      sessionId,
		SequenceNumber: sessionSeqNum,
		LeaderID:       babuza.Status().LeaderID,
	}
	for _, peer := range babuzaResponse.Peers {
		r := response.ClusterPeer{
			Id:             peer.RaftPeerAttr.PeerID,
			RaftListenAddr: peer.RaftPeerAttr.RaftListenAddr,
			IsLearner:      peer.RaftPeerAttr.IsLearner,
		}
		if len(peer.AppServiceAddresses) == 1 {
			r.AppServiceAddress = peer.AppServiceAddresses[0]
		}
		res.Peers = append(res.Peers, r)

	}

	return res
}
