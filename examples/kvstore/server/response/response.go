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
