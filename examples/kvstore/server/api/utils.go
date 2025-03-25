package api

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/fanaujie/babuza/examples/kvstore/server/response"
	"github.com/fanaujie/babuza/raft"
	"net/http"
)

func processRaftProposeError(err error, w http.ResponseWriter, req *http.Request, redirectLeaderAddresses []string) {
	if errors.Is(err, raft.ErrNotLeader) {
		if redirectLeaderAddresses != nil {
			http.Redirect(w, req, "http://"+redirectLeaderAddresses[0], http.StatusMovedPermanently)
			return
		}
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
	babuzaResponse := babuza.ClusterConfiguration()
	res := &response.ClusterConfigurationResponse{
		SessionID:      sessionId,
		SequenceNumber: sessionSeqNum,
		LeaderID:       babuzaResponse.LeaderID,
	}
	for _, peer := range babuzaResponse.Peers {
		r := response.ClusterPeer{
			Id:             peer.RaftPeerAttr.Id,
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
