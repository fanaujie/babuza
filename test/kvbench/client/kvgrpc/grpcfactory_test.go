package kvgrpc

import (
	"context"
	"github.com/stretchr/testify/assert"
	"net"
	"testing"

	"github.com/fanaujie/babuza/test/kvbench/client"
	"github.com/fanaujie/babuza/test/kvbench/kvbenchpb"
	"google.golang.org/grpc"
)

type mockServer struct {
	endpoint string
	resp     *kvbenchpb.ClusterPeersResponse
}

func (m *mockServer) Put(ctx context.Context, request *kvbenchpb.PutRequest) (*kvbenchpb.PutResponse, error) {
	return nil, nil
}

func (m *mockServer) Get(ctx context.Context, request *kvbenchpb.GetRequest) (*kvbenchpb.GetResponse, error) {
	return nil, nil
}

func (m *mockServer) Delete(ctx context.Context, request *kvbenchpb.DeleteRequest) (*kvbenchpb.DeleteResponse, error) {
	return nil, nil
}

func (m *mockServer) ClusterConfiguration(ctx context.Context, request *kvbenchpb.ClusterPeersRequest) (*kvbenchpb.ClusterPeersResponse, error) {
	return m.resp, nil
}

func startMockServer(t *testing.T, endpoint string, resp *kvbenchpb.ClusterPeersResponse) func() {

	server := grpc.NewServer()
	kvbenchpb.RegisterKVServiceServer(server, &mockServer{
		endpoint: endpoint,
		resp:     resp,
	})
	lis, err := net.Listen("tcp", endpoint)
	if err != nil {
		t.Fatalf("Failed to listen: %v", err)
	}
	go func() {
		_ = server.Serve(lis)
	}()

	cleanup := func() {
		server.Stop()
	}

	return cleanup
}

func TestNewGRPCFactory(t *testing.T) {
	fixedPeers := []*kvbenchpb.RaftPeerAttribute{
		{
			PeerID:         1,
			RaftListenAddr: "localhost:20050",
			GrpcListenAddr: "localhost:20051",
		},
		{
			PeerID:         2,
			RaftListenAddr: "localhost:30050",
			GrpcListenAddr: "localhost:30051",
		},
		{
			PeerID:         3,
			RaftListenAddr: "localhost:40050",
			GrpcListenAddr: "localhost:40051",
		},
	}
	fixedGroupMember := []string{
		"localhost:20051",
		"localhost:30051",
		"localhost:40051",
	}
	for _, tc := range []struct {
		name                     string
		config                   client.Config
		expectError              bool
		response                 *kvbenchpb.ClusterPeersResponse
		expectedGroupLeaderMap   map[uint64]string
		expectedGroupMemberMap   map[uint64][]string
		expectedNodeClientNumMap map[string]int
	}{
		{
			name: "basic configuration-round robin, 1 connection, 1 shard",
			config: client.Config{
				Endpoints:    []string{"localhost:20051", "localhost:30051", "localhost:40051"},
				Connections:  1,
				ShardCount:   1,
				TargetLeader: false,
			},
			expectError: false,
			response: &kvbenchpb.ClusterPeersResponse{
				ClusterID: 1,
				PeerID:    1,
				GroupPeers: []*kvbenchpb.GroupRaftPeerAttribute{
					{
						GroupID:  0,
						LeaderID: 1,
						Peers:    fixedPeers,
					},
				},
			},
			expectedGroupLeaderMap: map[uint64]string{
				0: "localhost:20051",
			},
			expectedGroupMemberMap: map[uint64][]string{
				0: fixedGroupMember,
			},
			expectedNodeClientNumMap: map[string]int{
				"localhost:20051": 1,
			},
		},
		{
			name: "basic configuration-target leader, 1 connection, 1 shard",
			config: client.Config{
				Endpoints:    []string{"localhost:20051", "localhost:30051", "localhost:40051"},
				Connections:  1,
				ShardCount:   1,
				TargetLeader: true,
			},
			expectError: false,
			response: &kvbenchpb.ClusterPeersResponse{
				ClusterID: 1,
				PeerID:    1,
				GroupPeers: []*kvbenchpb.GroupRaftPeerAttribute{
					{
						GroupID:  0,
						LeaderID: 1,
						Peers:    fixedPeers,
					},
				},
			},
			expectedGroupLeaderMap: map[uint64]string{
				0: "localhost:20051",
			},
			expectedGroupMemberMap: map[uint64][]string{
				0: fixedGroupMember,
			},
			expectedNodeClientNumMap: map[string]int{
				"localhost:20051": 1,
			},
		},
		{
			name: "multi connections-round robin, 3 connections, 1 shard",
			config: client.Config{
				Endpoints:    []string{"localhost:20051", "localhost:30051", "localhost:40051"},
				Connections:  3,
				ShardCount:   1,
				TargetLeader: false,
			},
			expectError: false,
			response: &kvbenchpb.ClusterPeersResponse{
				ClusterID: 1,
				PeerID:    1,
				GroupPeers: []*kvbenchpb.GroupRaftPeerAttribute{
					{
						GroupID:  0,
						LeaderID: 1,
						Peers:    fixedPeers,
					},
				},
			},
			expectedGroupLeaderMap: map[uint64]string{
				0: "localhost:20051",
			},
			expectedGroupMemberMap: map[uint64][]string{
				0: fixedGroupMember,
			},
			expectedNodeClientNumMap: map[string]int{
				"localhost:20051": 1,
				"localhost:30051": 1,
				"localhost:40051": 1,
			},
		},
		{
			name: "multi connections-target leader, 3 connections, 1 shard",
			config: client.Config{
				Endpoints:    []string{"localhost:20051", "localhost:30051", "localhost:40051"},
				Connections:  3,
				ShardCount:   1,
				TargetLeader: true,
			},
			expectError: false,
			response: &kvbenchpb.ClusterPeersResponse{
				ClusterID: 1,
				PeerID:    1,
				GroupPeers: []*kvbenchpb.GroupRaftPeerAttribute{
					{
						GroupID:  0,
						LeaderID: 1,
						Peers:    fixedPeers,
					},
				},
			},
			expectedGroupLeaderMap: map[uint64]string{
				0: "localhost:20051",
			},
			expectedGroupMemberMap: map[uint64][]string{
				0: fixedGroupMember,
			},
			expectedNodeClientNumMap: map[string]int{
				"localhost:20051": 3,
			},
		},
		{
			name: "multi shards-round robin, 3 connections, 2 shards",
			config: client.Config{
				Endpoints:    []string{"localhost:20051", "localhost:30051", "localhost:40051"},
				Connections:  3,
				ShardCount:   2,
				TargetLeader: false,
			},
			expectError: false,
			response: &kvbenchpb.ClusterPeersResponse{
				ClusterID: 1,
				PeerID:    1,
				GroupPeers: []*kvbenchpb.GroupRaftPeerAttribute{
					{
						GroupID:  0,
						LeaderID: 1,
						Peers:    fixedPeers,
					},
					{
						GroupID:  1,
						LeaderID: 2,
						Peers:    fixedPeers,
					},
				},
			},
			expectedGroupLeaderMap: map[uint64]string{
				0: "localhost:20051",
				1: "localhost:30051",
			},
			expectedGroupMemberMap: map[uint64][]string{
				0: fixedGroupMember,
				1: fixedGroupMember,
			},
			expectedNodeClientNumMap: map[string]int{
				"localhost:20051": 1,
				"localhost:30051": 1,
				"localhost:40051": 1,
			},
		},
		{
			name: "multi shards-target leader, 3 connections, 2 shards",
			config: client.Config{
				Endpoints:    []string{"localhost:20051", "localhost:30051", "localhost:40051"},
				Connections:  3,
				ShardCount:   2,
				TargetLeader: true,
			},
			expectError: false,
			response: &kvbenchpb.ClusterPeersResponse{
				ClusterID: 1,
				PeerID:    1,
				GroupPeers: []*kvbenchpb.GroupRaftPeerAttribute{
					{
						GroupID:  0,
						LeaderID: 1,
						Peers:    fixedPeers,
					},
					{
						GroupID:  1,
						LeaderID: 2,
						Peers:    fixedPeers,
					},
				},
			},
			expectedGroupLeaderMap: map[uint64]string{
				0: "localhost:20051",
				1: "localhost:30051",
			},
			expectedGroupMemberMap: map[uint64][]string{
				0: fixedGroupMember,
				1: fixedGroupMember,
			},
			expectedNodeClientNumMap: map[string]int{
				"localhost:20051": 2,
				"localhost:30051": 1,
			},
		},
		{
			name: "error case-shard count exceeds connections",
			config: client.Config{
				Endpoints:    []string{"localhost:20051", "localhost:30051", "localhost:40051"},
				Connections:  1,
				ShardCount:   2,
				TargetLeader: false,
			},
			expectError: true,
			response: &kvbenchpb.ClusterPeersResponse{
				ClusterID: 1,
				PeerID:    1,
				GroupPeers: []*kvbenchpb.GroupRaftPeerAttribute{
					{
						GroupID:  0,
						LeaderID: 1,
						Peers:    fixedPeers,
					},
				},
			},
			expectedGroupLeaderMap:   nil,
			expectedGroupMemberMap:   nil,
			expectedNodeClientNumMap: nil,
		},
		{
			name: "error case-no endpoints",
			config: client.Config{
				Endpoints:    []string{},
				Connections:  1,
				ShardCount:   1,
				TargetLeader: false,
			},
			expectError:              true,
			response:                 nil,
			expectedGroupLeaderMap:   nil,
			expectedGroupMemberMap:   nil,
			expectedNodeClientNumMap: nil,
		},
		{
			name: "different distribution-target leader, 10 connections, 3 shards with uneven leaders",
			config: client.Config{
				Endpoints:    []string{"localhost:20051", "localhost:30051", "localhost:40051"},
				Connections:  10,
				ShardCount:   3,
				TargetLeader: true,
			},
			expectError: false,
			response: &kvbenchpb.ClusterPeersResponse{
				ClusterID: 1,
				PeerID:    1,
				GroupPeers: []*kvbenchpb.GroupRaftPeerAttribute{
					{
						GroupID:  0,
						LeaderID: 1,
						Peers:    fixedPeers,
					},
					{
						GroupID:  1,
						LeaderID: 1,
						Peers:    fixedPeers,
					},
					{
						GroupID:  2,
						LeaderID: 3,
						Peers:    fixedPeers,
					},
				},
			},
			expectedGroupLeaderMap: map[uint64]string{
				0: "localhost:20051",
				1: "localhost:20051",
				2: "localhost:40051",
			},
			expectedGroupMemberMap: map[uint64][]string{
				0: fixedGroupMember,
				1: fixedGroupMember,
				2: fixedGroupMember,
			},
			expectedNodeClientNumMap: map[string]int{
				"localhost:20051": 6,
				"localhost:40051": 4,
			},
		},
	} {
		func(t *testing.T, name string, config client.Config, expectError bool,
			response *kvbenchpb.ClusterPeersResponse) {
			t.Run(name, func(t *testing.T) {
				if len(config.Endpoints) > 0 {
					cleanup := startMockServer(t, config.Endpoints[0], response)
					defer cleanup()
				}
				f, err := NewGRPCFactory(1, config)
				if expectError {
					assert.Error(t, err)
					return
				}
				assert.NoError(t, err)
				assert.NotNil(t, f)
				// validate
				f.groupLeaderMap.Range(func(key, value interface{}) bool {
					assert.Equal(t, tc.expectedGroupLeaderMap[key.(uint64)], value.(string))
					return true
				})
				f.groupMemberMap.Range(func(key, value interface{}) bool {
					assert.Equal(t, tc.expectedGroupMemberMap[key.(uint64)], value.([]string))
					return true
				})
				f.nodeClientMap.Range(func(key, value interface{}) bool {
					assert.Equal(t, tc.expectedNodeClientNumMap[key.(string)], len(value.([]*grpc.ClientConn)))
					return true
				})
			})
		}(t, tc.name, tc.config, tc.expectError, tc.response)
	}
}

func TestConnectionManagement(t *testing.T) {
	fixedPeers := []*kvbenchpb.RaftPeerAttribute{
		{
			PeerID:         1,
			RaftListenAddr: "localhost:20050",
			GrpcListenAddr: "localhost:20051",
		},
		{
			PeerID:         2,
			RaftListenAddr: "localhost:30050",
			GrpcListenAddr: "localhost:30051",
		},
		{
			PeerID:         3,
			RaftListenAddr: "localhost:40050",
			GrpcListenAddr: "localhost:40051",
		},
	}

	serverAddr := "localhost:20051"
	response := &kvbenchpb.ClusterPeersResponse{
		ClusterID: 1,
		PeerID:    1,
		GroupPeers: []*kvbenchpb.GroupRaftPeerAttribute{
			{
				GroupID:  0,
				LeaderID: 1,
				Peers:    fixedPeers,
			},
		},
	}

	cleanup := startMockServer(t, serverAddr, response)
	defer cleanup()

	config := client.Config{
		Endpoints:    []string{serverAddr},
		Connections:  3,
		ShardCount:   1,
		TargetLeader: false,
	}

	f, err := NewGRPCFactory(1, config)
	assert.NoError(t, err)
	assert.NotNil(t, f)
	defer f.Close()

	conn1, err := f.getConnectionForClient(serverAddr)
	assert.NoError(t, err)
	assert.NotNil(t, conn1)

	usage1, ok := f.connUsageStats.Load(conn1)
	assert.True(t, ok)
	assert.Equal(t, uint(1), usage1)

	conn2, err := f.getConnectionForClient(serverAddr)
	assert.NoError(t, err)

	usage2, ok := f.connUsageStats.Load(conn2)
	assert.True(t, ok)
	assert.Equal(t, uint(1), usage2)

	f.releaseConnection(conn1)

	usageAfterRelease, ok := f.connUsageStats.Load(conn1)
	assert.True(t, ok)
	assert.Equal(t, uint(0), usageAfterRelease)

	conn1_1, err := f.getConnectionForClient(serverAddr)
	assert.NoError(t, err)
	assert.Equal(t, conn1, conn1_1)

	usageAfterReuse, ok := f.connUsageStats.Load(conn1_1)
	assert.True(t, ok)
	assert.Equal(t, uint(1), usageAfterReuse)

	conn3, err := f.getConnectionForClient(serverAddr)
	assert.NoError(t, err)
	assert.NotNil(t, conn3)
	usage3, ok := f.connUsageStats.Load(conn3)
	assert.True(t, ok)
	assert.Equal(t, uint(1), usage3)

	_, err = f.getConnectionForClient("invalid-addr")
	assert.Error(t, err)
}
