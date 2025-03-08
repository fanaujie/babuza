package cluster

import "errors"

var (
	ErrPeerIDRemoved            = errors.New("cluster: node id removed")
	ErrPeerIDExists             = errors.New("cluster: node id exists")
	ErrPeerIDNotFound           = errors.New("cluster: node id not found")
	ErrPeerRaftListenAddrExists = errors.New("cluster: peer raft-advertise-address exists")
	ErrPeerNotLearner           = errors.New("cluster: peer is not a learner")
)
