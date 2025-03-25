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
	Transferee uint64 `json:"transferee"`
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
