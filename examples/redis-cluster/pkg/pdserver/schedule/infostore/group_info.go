// Copyright 2025 Chen Chunchieh <junjie725@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


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
