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


package peer

import (
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"io"
)

type TransportClientFactory interface {
	CreateTransportClient() (ibabuza.TransportClient, error)
}

type SnapshotFileReader interface {
	ForEachFile(visitor func(io.Reader, babuzapb.SnapshotFileDesc) error) error
	Metadata() babuzapb.SnapshotMetadata
}

type Peer interface {
	SendRaftMessage(msg raftpb.Message) error
	SendSnapshot(snapMsg raftpb.Message, snapReader SnapshotFileReader)
	UpdateRaftReport(report ibabuza.RaftStatusReporter)
	Stop()
}

type MultiRaftPeer interface {
	SendRaftMessage(msg *babuzapb.MultiRaftMessage) error
	SendSnapshot(snapMsg babuzapb.MultiRaftMessage, snapReader SnapshotFileReader)
	UpdateRaftReport(report ibabuza.MultiRaftStatusReporter)
	Stop()
}
