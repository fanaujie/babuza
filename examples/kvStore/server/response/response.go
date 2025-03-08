package response

import "github.com/fanaujie/babuza/examples/kvStore/server/kvstore"

type RegisterSessionResponse struct {
	SessionId uint64
}

type ClusterPeer struct {
	Id                uint64
	RaftListenAddr    string
	IsLearner         bool
	AppServiceAddress string
}

type ClusterConfigurationResponse struct {
	SessionID      uint64
	SequenceNumber uint64
	Peers          []ClusterPeer
}

type TransferLeaderResponse struct{}

type KvStoreResponse struct {
	SessionID      uint64
	SequenceNumber uint64
	kvstore.KvResult
}
