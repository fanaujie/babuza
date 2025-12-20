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

package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/pkg/builder"
	"github.com/fanaujie/babuza/raft"
)

// SimpleListener implements ibabuza.RaftListener to handle Raft events
type SimpleListener struct{}

func (l *SimpleListener) OnLeaderChange(term, leaderID uint64) {
	fmt.Printf("[Event] Leader changed: term=%d, leaderID=%d\n", term, leaderID)
}

func (l *SimpleListener) OnAcquiredLeader() {
	fmt.Println("[Event] Acquired leadership")
}

func (l *SimpleListener) OnLostLeader() {
	fmt.Println("[Event] Lost leadership")
}

func (l *SimpleListener) OnMemberChange(memberEvent int, term, peerID uint64) {
	eventName := memberEventName(memberEvent)
	fmt.Printf("[Event] Member change: event=%s, term=%d, peerID=%d\n", eventName, term, peerID)
}

func (l *SimpleListener) OnRaftShutdown() {
	fmt.Println("[Event] Raft shutdown")
}

func memberEventName(event int) string {
	switch event {
	case ibabuza.MemberJoined:
		return "MemberJoined"
	case ibabuza.MemberUpdated:
		return "MemberUpdated"
	case ibabuza.MemberRemoved:
		return "MemberRemoved"
	case ibabuza.LeanerAdded:
		return "LearnerAdded"
	case ibabuza.LeanerPromoted:
		return "LearnerPromoted"
	default:
		return "Unknown"
	}
}

const (
	clusterID   = 1
	localPeerID = 1
	raftAddr    = "127.0.0.1:12380"
	dataDir     = "/tmp/babuza-simple-demo"
)

func main() {
	fmt.Println("Babuza Simple Quick Start Demo")
	fmt.Println("==============================")
	fmt.Println()

	// Clean up data directory for fresh start
	os.RemoveAll(dataDir)

	// Create state machine
	stateMachine := NewSimpleKVStore()

	// Configure Babuza with defaults
	babuzaCfg := raft.DefaultBabuzaConfig(clusterID, localPeerID, raftAddr)

	// Build components using in-memory/volatile options for simplicity
	fmt.Println("Initializing single-node Raft cluster...")
	components := builder.NewBabuzaComponentBuilder(&builder.BabuzaComponentConfig{
		ClusterId:      clusterID,
		StorageRootDir: dataDir,
		SessionType:    builder.NoOpSession,
		TransportType:  builder.TcpTransport,
		WalType:        builder.BadgerWalMemory,
		SnapshotType:   builder.VolatileSnapshot,
	}).Build()

	// Configure single peer
	peersConfig := raft.NewPeersConfiguration()
	if err := peersConfig.AddPeer(localPeerID, raftAddr, false); err != nil {
		fmt.Printf("Failed to add peer: %v\n", err)
		return
	}

	// Bootstrap Raft cluster
	bootstrap, err := raft.NewBootstrapRaftCluster(
		babuzaCfg,
		*peersConfig,
		stateMachine,
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
		fmt.Printf("Failed to bootstrap cluster: %v\n", err)
		return
	}

	// Create Raft instance with listener
	listener := &SimpleListener{}
	r, err := raft.NewRaft(babuzaCfg, bootstrap, listener)
	if err != nil {
		fmt.Printf("Failed to create Raft: %v\n", err)
		return
	}

	// Wait for leader election (single-node becomes leader quickly)
	fmt.Println("Waiting for leader election...")
	time.Sleep(2 * time.Second)

	status := r.Status()
	fmt.Printf("Raft is ready (Leader: %v)\n", status.IsLeader())
	fmt.Println()

	// Run demo operations
	fmt.Println("Demo operations:")
	ctx := context.Background()

	// Set 'hello' = 'world'
	if err := propose(ctx, r, "hello", "world"); err != nil {
		fmt.Printf("  Set 'hello' failed: %v\n", err)
	} else {
		fmt.Printf("  Set 'hello' = 'world' -> success\n")
	}

	// Get 'hello'
	if val, err := stateMachine.Query("hello"); err != nil {
		fmt.Printf("  Get 'hello' failed: %v\n", err)
	} else {
		fmt.Printf("  Get 'hello' -> '%v'\n", val)
	}

	// Set 'foo' = 'bar'
	if err := propose(ctx, r, "foo", "bar"); err != nil {
		fmt.Printf("  Set 'foo' failed: %v\n", err)
	} else {
		fmt.Printf("  Set 'foo' = 'bar' -> success\n")
	}

	// Get 'foo'
	if val, err := stateMachine.Query("foo"); err != nil {
		fmt.Printf("  Get 'foo' failed: %v\n", err)
	} else {
		fmt.Printf("  Get 'foo' -> '%v'\n", val)
	}

	// Get non-existent key
	if val, err := stateMachine.Query("nonexistent"); err != nil {
		fmt.Printf("  Get 'nonexistent' -> error: %v\n", err)
	} else {
		fmt.Printf("  Get 'nonexistent' -> '%v'\n", val)
	}

	fmt.Println()

	// Shutdown
	fmt.Println("Shutting down...")
	shutdownResult := r.Shutdown()
	if err := shutdownResult.Wait(); err != nil {
		fmt.Printf("Shutdown error: %v\n", err)
	} else {
		fmt.Println("Shutdown complete.")
	}
}

func propose(ctx context.Context, r *raft.Raft, key, value string) error {
	cmd := NewSetCommand(key, value)
	data, err := cmd.Encode()
	if err != nil {
		return err
	}

	result := r.Propose(ctx, raft.ClientSession{}, data)
	defer result.Release()

	applyResult := result.WaitForApplyResult()
	return applyResult.Error
}
