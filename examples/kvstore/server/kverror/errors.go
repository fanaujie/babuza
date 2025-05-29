package kverror

import "errors"

var (
	ErrKeyNotFound    = errors.New("key not found")
	ErrUnknownCommand = errors.New("unknown command")
	ErrInvalidKeyType = errors.New("invalid key type, must be string")
	//ErrNotLeader                  = errors.New("raft: not leader")
	//ErrLearnerNotReady            = errors.New("raft: can only promote a learner which is in sync with leader")
	//ErrLearnerNotSwitchLeaderShip = errors.New("raft: learner can not switch to leadership")
	//ErrLearnerNotVotingMember     = errors.New("raft: learner can not become voting member")
	//ErrVotingMemberNotLeaner      = errors.New("raft: voting member can not become a leaner")
	//ErrRaftStopped                = errors.New("raft: raft has stopped")
	//ErrPeerIDRemoved              = errors.New("cluster: node id removed")
	//ErrPeerIDExists               = errors.New("cluster: node id exists")
	//ErrPeerIDNotFound             = errors.New("cluster: node id not found")
	//ErrPeerRaftListenAddrExists   = errors.New("cluster: peer raft-advertise-address exists")
	//ErrPeerNotLearner             = errors.New("cluster: peer is not a learner")
)
