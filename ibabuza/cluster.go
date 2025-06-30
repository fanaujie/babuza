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


package ibabuza

import (
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"io"
)

type ClusterRestoreSnapshot interface {
	Cluster() (io.Reader, error)
}

type Cluster interface {
	SetClusterID(clusterID uint64)
	SetGroupID(groupID RaftGroupID)
	SetLocalPeerID(localPeerID uint64)
	Peer(peerID uint64) (babuzapb.Peer, error)
	Snapshot(io.Writer) error
	Restore(io.Reader) error
	Peers() []babuzapb.Peer
	ClusterID() uint64
	GroupID() RaftGroupID
	LocalPeerID() uint64
	Add(babuzapb.RaftPeerAttribute) error
	Update(peerID uint64, attr babuzapb.RaftPeerAttribute) error
	Remove(peerID uint64) error
	Promote(peerID uint64) error
	UpdateAppServiceAddresses(peerID uint64, addresses []string) error
}
