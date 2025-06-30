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


package server

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/tidwall/redcon"
	"net"
	"sync"
	"time"

	"testing"

	"github.com/fanaujie/babuza/ibabuza"
	babuza "github.com/fanaujie/babuza/raft"
)

// Mock implementations for testing
type mockMultiRaftStore struct{}

func (m *mockMultiRaftStore) Propose(ctx context.Context, groupID ibabuza.RaftGroupID, session babuza.ClientSession,
	log []byte) babuza.ProposedResult {
	return nil
}

func (m *mockMultiRaftStore) Query(groupID ibabuza.RaftGroupID, key any) (any, error) {
	return nil, nil
}

func TestRedirectToLeader(t *testing.T) {

}

func TestIsLocalLeaderForGroup(t *testing.T) {
	store := &mockMultiRaftStore{}
	cluster := newClusterMgr("127.0.0.1:6379", store)

	// Test with empty routing table
	if cluster.IsLocalLeaderForGroup(ibabuza.RaftGroupID(1)) {
		t.Error("Expected false for non-existent group")
	}

	// Set up routing table
	routingTable := map[uint64]string{
		1: "127.0.0.1:6379", // Local
		2: "127.0.0.1:6380", // Remote
	}
	cluster.UpdateRoutingTable(routingTable)

	// Test local leader
	if !cluster.IsLocalLeaderForGroup(ibabuza.RaftGroupID(1)) {
		t.Error("Expected true for local leader group")
	}

	// Test remote leader
	if cluster.IsLocalLeaderForGroup(ibabuza.RaftGroupID(2)) {
		t.Error("Expected false for remote leader group")
	}

	// Test non-existent group
	if cluster.IsLocalLeaderForGroup(ibabuza.RaftGroupID(999)) {
		t.Error("Expected false for non-existent group")
	}
}

func TestUpdateRoutingTable(t *testing.T) {
	store := &mockMultiRaftStore{}
	cluster := newClusterMgr("127.0.0.1:6379", store)

	// Initial routing table
	table1 := map[uint64]string{
		1: "127.0.0.1:6379",
		2: "127.0.0.1:6380",
	}
	cluster.UpdateRoutingTable(table1)

	if !cluster.IsLocalLeaderForGroup(ibabuza.RaftGroupID(1)) {
		t.Error("Expected group 1 to be local leader")
	}

	// Update routing table
	table2 := map[uint64]string{
		1: "127.0.0.1:6380", // Changed
		3: "127.0.0.1:6381", // New
	}
	cluster.UpdateRoutingTable(table2)

	// Group 1 should no longer be local leader
	if cluster.IsLocalLeaderForGroup(ibabuza.RaftGroupID(1)) {
		t.Error("Expected group 1 to no longer be local leader")
	}

	// Group 2 should no longer exist in routing table
	if cluster.IsLocalLeaderForGroup(ibabuza.RaftGroupID(2)) {
		t.Error("Expected group 2 to no longer exist")
	}

	// Group 3 should exist but not be local leader
	if cluster.IsLocalLeaderForGroup(ibabuza.RaftGroupID(3)) {
		t.Error("Expected group 3 to not be local leader")
	}
}

// Integration test with real redcon server
func TestRedirectToLeaderIntegration(t *testing.T) {
	// Start a simple Redis-like server for testing
	targetAddr := "127.0.0.1:16379"

	var wg sync.WaitGroup
	wg.Add(1)

	// Start target server
	targetServer := redcon.NewServer(targetAddr,
		func(conn redcon.Conn, cmd redcon.Command) {
			switch string(cmd.Args[0]) {
			case "PING":
				conn.WriteString("PONG")
			default:
				conn.WriteError("ERR unknown command")
			}
		},
		func(conn redcon.Conn) bool {
			return true
		},
		func(conn redcon.Conn, err error) {

		},
	)
	go func() {
		defer wg.Done()
		_ = targetServer.ListenAndServe()
	}()

	// Wait for server to start
	time.Sleep(100 * time.Millisecond)
	localAddr := "127.0.0.1:26379"
	// Create cluster
	store := &mockMultiRaftStore{}
	cluster := newClusterMgr("127.0.0.1:6380", store)

	redisServer := redcon.NewServer(localAddr,
		func(conn redcon.Conn, cmd redcon.Command) {
			cluster.RedirectToLeader(conn, cmd, ibabuza.RaftGroupID(1))
		},
		func(conn redcon.Conn) bool {
			return true
		},
		func(conn redcon.Conn, err error) {

		},
	)
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = redisServer.ListenAndServe()
	}()
	time.Sleep(100 * time.Millisecond)
	defer func() {
		_ = targetServer.Close()
		_ = redisServer.Close()
		_ = cluster.pool.Close()
		wg.Wait()
	}()
	// Set up routing table to point to our test server
	routingTable := map[uint64]string{
		1: targetAddr,
	}
	cluster.UpdateRoutingTable(routingTable)

	conn, err := net.Dial("tcp", localAddr)
	assert.NoError(t, err)
	defer conn.Close()
	_, err = conn.Write([]byte("*1\r\n$4\r\nPING\r\n"))
	assert.NoError(t, err)
	buf := make([]byte, 1024)
	_, err = conn.Read(buf)
	assert.NoError(t, err)
	assert.Equal(t, "+PONG\r\n", string(buf[:7]))
}
