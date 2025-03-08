package request

type Peer struct {
}

type JoinPeerRequest struct {
	SessionID                         uint64
	SequenceNumber                    uint64
	LowestSequenceNumberNotYetReplied uint64
	RaftPeerId                        uint64
	RaftListenAddr                    string
	IsLearner                         bool
}

type UpdatePeerRequest struct {
	SessionID                         uint64
	SequenceNumber                    uint64
	LowestSequenceNumberNotYetReplied uint64
	RaftPeerId                        uint64
	RaftListenAddr                    string
}

type RemovePeerRequest struct {
	SessionID                         uint64
	SequenceNumber                    uint64
	LowestSequenceNumberNotYetReplied uint64
	RaftPeerId                        uint64
}

type PromoteLearnerRequest RemovePeerRequest

type TransferLeaderRequest struct {
	Transferee uint64
}

type KvStoreSetRequest struct {
	SessionID                         uint64
	SequenceNumber                    uint64
	LowestSequenceNumberNotYetReplied uint64
	Key                               string
	Value                             string
}

type KvStoreAppendRequest KvStoreSetRequest
type KvStoreDeleteRequest struct {
	SessionID                         uint64
	SequenceNumber                    uint64
	LowestSequenceNumberNotYetReplied uint64
	Key                               string
}
