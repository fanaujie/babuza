package raftnode

import (
	"go.etcd.io/etcd/raft/v3"
)

type EtcdRaftNode struct {
}

func NewEtcdRaftNode() *EtcdRaftNode {
	return &EtcdRaftNode{}
}

func (n *EtcdRaftNode) Start(cfg raft.Config, peers []raft.Peer) (raft.Node, error) {
	cfg.Logger.Infof("EtcdRaftNode: Starting Raft Node")
	return raft.StartNode(&cfg, peers), nil
}

func (n *EtcdRaftNode) Restart(cfg raft.Config) (raft.Node, error) {
	cfg.Logger.Infof("EtcdRaftNode: Restarting Raft Node")
	return raft.RestartNode(&cfg), nil
}
