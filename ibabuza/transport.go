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
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

type RaftMessageHandler interface {
	ProcessBatchMessage(babuzapb.BatchMessage)
	ProcessSnapshotMessage(babuzapb.SnapshotMessage) babuzapb.SnapshotMessageResponse
	GetClusterPeer(babuzapb.GetClusterPeersRequest) babuzapb.GetClusterPeersResponse
	PublishApplicationService(babuzapb.PublishApplicationServiceRequest) babuzapb.PublishApplicationServiceResponse
}

type RaftStatusReporter interface {
	ReportUnreachable(peerID uint64)
	ReportSnapshot(peerID uint64, status raft.SnapshotStatus)
}

type SnapshotStorage interface {
	CreateSnapshotReader(snapshotIndex uint64) (SnapshotReader, error)
}

type RaftNodeHandler interface {
	RaftMessageHandler
	RaftStatusReporter
	SnapshotStorage
}

type TransportResolver interface {
	ResolvePeerAddress(peerID uint64) (string, error)
}

type Transport interface {
	SetupTransportConfig(cfg TransportConfig) error
	SetupTransportRaft(RaftNodeHandler) error
	Start() error
	Stop() error
	Send(raftpb.Message)
	SendSnapshot(raftpb.Message)
	CreateTransportClient() (TransportClient, error)
	AddPeer(uint64, string)
	UpdatePeer(uint64, string)
	RemovePeer(uint64)
	RemovePeers()
}

type TransportServer interface {
	Start() error
	Stop() error
}

type TransportClient interface {
	SendBatchMessage(babuzapb.BatchMessage) error
	SendSnapshotMessage(babuzapb.SnapshotMessage) (babuzapb.SnapshotMessageResponse, error)
	GetClusterPeers(babuzapb.GetClusterPeersRequest) (babuzapb.GetClusterPeersResponse, error)
	PublishApplicationService(babuzapb.PublishApplicationServiceRequest) (babuzapb.PublishApplicationServiceResponse, error)
	Close() error
}

type TLSConfig struct {
	EnableTLS bool
	MutualTLS bool
	TLSCert   string
	TLSKey    string
	TLSRootCA string
}

type TransportConfig struct {
	LocalNodeID uint64
	PeerAddress string
	TLSConfig
}

type TransportProtocol interface {
	Setup(TransportConfig) error
	CreateServer(RaftMessageHandler) (TransportServer, error)
	CreateClient(TransportResolver) (TransportClient, error)
	Close() error
}
