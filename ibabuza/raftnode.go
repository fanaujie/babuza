package ibabuza

import "go.etcd.io/etcd/raft/v3"

type RaftNode interface {
	Start(raft.Config, []raft.Peer) (raft.Node, error)
	Restart(raft.Config) (raft.Node, error)
}
