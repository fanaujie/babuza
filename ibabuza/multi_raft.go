package ibabuza

type RaftGroupID uint64
type NodeID uint64

type RaftGroup struct {
	ID         RaftGroupID
	RaftPeerID uint64
	Metadata   []byte
}

type Scheduler interface {
	EnqueueBatchTickState(gids []RaftGroupID) error
	EnqueueState(gid RaftGroupID, state int) error
	Stop()
}

type ApplyJob func()

type ApplyJobQueue interface {
	Put(gid RaftGroupID, job ApplyJob) error
	Stop()
}

type RaftStateProcessor interface {
	ProcessTick(gid RaftGroupID)
	ProcessReady(gid RaftGroupID)
	ProcessStep(gid RaftGroupID)
	ProcessProposal(gid RaftGroupID)
	ProcessConfigChange(gid RaftGroupID)
}

type MultiStatus interface {
	Get(gid RaftGroupID) Status
	Set(gid RaftGroupID)
}
