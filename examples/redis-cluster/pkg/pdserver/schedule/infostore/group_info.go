package infostore

import (
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
)

type GroupInfo struct {
	storeID  uint64
	groupID  uint64
	leaderID uint64
	peers    []babuzapb.RaftPeerAttribute
}

func CreateGroupInfo(storeID, groupID, leaderID uint64, peers []babuzapb.RaftPeerAttribute) GroupInfo {
	return GroupInfo{
		storeID:  storeID,
		groupID:  groupID,
		leaderID: leaderID,
		peers:    peers,
	}
}

func (g *GroupInfo) StoreID() uint64 {
	return g.storeID
}

func (g *GroupInfo) GroupID() uint64 {
	return g.groupID
}

func (g *GroupInfo) RandomFollower() (babuzapb.RaftPeerAttribute, bool) {
	if len(g.peers) == 0 {
		return babuzapb.RaftPeerAttribute{}, false
	}
	// excluding the leader
	for _, peer := range g.peers {
		if peer.PeerID != g.leaderID {
			return peer, true
		}
	}
	return babuzapb.RaftPeerAttribute{}, false
}

func (g *GroupInfo) Leader() (babuzapb.RaftPeerAttribute, bool) {
	for _, peer := range g.peers {
		if peer.PeerID == g.leaderID {
			return peer, true
		}
	}
	return babuzapb.RaftPeerAttribute{}, false
}

func (g *GroupInfo) PeerOnStore(storeID uint64) (babuzapb.RaftPeerAttribute, bool) {
	for _, peer := range g.peers {
		if peer.StoreID == storeID {
			return peer, true
		}
	}
	return babuzapb.RaftPeerAttribute{}, false
}

func (g *GroupInfo) Peers() []babuzapb.RaftPeerAttribute {
	return g.peers
}
