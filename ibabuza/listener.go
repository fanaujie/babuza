package ibabuza

const (
	MemberJoined = iota + 1
	MemberUpdated
	MemberRemoved
	LeanerAdded
	LeanerPromoted
	LeaderChanged
	AcquiredLeader
	LostLeader
	RemoveSelf = iota + 1
)

type RaftEvent struct {
	Event   int
	GroupID RaftGroupID
	PeerID  uint64
}

type RaftListener interface {
	OnLeaderChange(term, leaderID uint64)
	OnAcquiredLeader(term, leaderID uint64)
	OnLostLeader(term, leaderID uint64)
	OnMemberChange(memberEvent int, term, peerID uint64)
	OnRaftShutdown()
}
