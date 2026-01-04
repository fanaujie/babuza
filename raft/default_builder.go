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

package raft

import (
	"fmt"

	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/pkg/builder"
)

type DefaultBuilder struct {
	clusterID    uint64
	peerID       uint64
	raftAddr     string
	dataDir      string
	stateMachine ibabuza.BaseStateMachine
	listener     ibabuza.RaftListener
	inMemory     bool
}

func NewDefaultBuilder() *DefaultBuilder {
	return &DefaultBuilder{
		clusterID: 1,
		peerID:    1,
		raftAddr:  "127.0.0.1:12380",
		inMemory:  true,
	}
}

func (b *DefaultBuilder) ClusterID(id uint64) *DefaultBuilder {
	b.clusterID = id
	return b
}

func (b *DefaultBuilder) PeerID(id uint64) *DefaultBuilder {
	b.peerID = id
	return b
}

func (b *DefaultBuilder) RaftAddr(addr string) *DefaultBuilder {
	b.raftAddr = addr
	return b
}

func (b *DefaultBuilder) DataDir(dir string) *DefaultBuilder {
	b.dataDir = dir
	return b
}

func (b *DefaultBuilder) StateMachine(sm ibabuza.BaseStateMachine) *DefaultBuilder {
	b.stateMachine = sm
	return b
}

// Listener sets an optional Raft event listener.
func (b *DefaultBuilder) Listener(l ibabuza.RaftListener) *DefaultBuilder {
	b.listener = l
	return b
}

func (b *DefaultBuilder) InMemory(enabled bool) *DefaultBuilder {
	b.inMemory = enabled
	return b
}

func (b *DefaultBuilder) Start() (*Raft, error) {
	if b.stateMachine == nil {
		return nil, fmt.Errorf("state machine is required")
	}
	if !b.inMemory && b.dataDir == "" {
		return nil, fmt.Errorf("data directory is required when not using in-memory mode")
	}

	dataDir := b.dataDir
	if dataDir == "" {
		dataDir = "/tmp/babuza-default"
	}

	var walType, snapshotType string
	if b.inMemory {
		walType = builder.BadgerWalMemory
		snapshotType = builder.VolatileSnapshot
	} else {
		walType = builder.BabuzaWal
		snapshotType = builder.DurableSnapshot
	}

	components := builder.NewBabuzaComponentBuilder(&builder.BabuzaComponentConfig{
		ClusterId:      b.clusterID,
		StorageRootDir: dataDir,
		SessionType:    builder.NoOpSession,
		TransportType:  builder.TcpTransport,
		WalType:        walType,
		SnapshotType:   snapshotType,
	}).Build()

	cfg := DefaultBabuzaConfig(b.clusterID, b.peerID, b.raftAddr)

	peersConfig := NewPeersConfiguration()
	if err := peersConfig.AddPeer(b.peerID, b.raftAddr, false); err != nil {
		return nil, fmt.Errorf("failed to add peer: %w", err)
	}

	bootstrap, err := NewBootstrapRaftCluster(
		cfg,
		*peersConfig,
		b.stateMachine,
		components.Cluster,
		components.RaftNode,
		components.SessionManager,
		components.SnapshotManager,
		components.WalManager,
		components.Transport,
		components.Logger,
		components.MetricsController,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to bootstrap cluster: %w", err)
	}
	r, err := NewRaft(cfg, bootstrap, b.listener)
	if err != nil {
		return nil, fmt.Errorf("failed to create raft: %w", err)
	}
	return r, nil
}
