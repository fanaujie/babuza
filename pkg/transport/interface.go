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


package transport

import (
	"github.com/fanaujie/babuza/ibabuza"
)

type PeerAction[Reporter any] interface {
	Stop()
	UpdateRaftReport(reporter Reporter)
}

type PeerManager[Peer PeerAction[Reporter], Reporter any] interface {
	GetPeer(groupID ibabuza.RaftGroupID, peerID uint64) (Peer, error)
	GetPeerByAddress(peerAddr string) (Peer, error)
	AddPeer(groupID ibabuza.RaftGroupID, peerID uint64, peerAddr string, factory PeerFactory[Peer]) error
	UpdatePeer(groupID ibabuza.RaftGroupID, peerID uint64, peerAddr string, factory PeerFactory[Peer]) error
	RemovePeer(groupID ibabuza.RaftGroupID, peerID uint64) error
	RemoveAllPeers()
	ResolvePeerAddress(groupID ibabuza.RaftGroupID, peerID uint64) (string, error)
	UpdatePeerRaftReport(Reporter)
}

type PeerFactory[Peer any] interface {
	CreatePeer(peerAddress string) Peer
}
