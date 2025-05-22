package single

import (
	"context"
	babuza "github.com/fanaujie/babuza/raft"
	"github.com/fanaujie/babuza/test/kvbench/kvbenchpb"
	"github.com/fanaujie/babuza/test/kvbench/statemachine"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"net"
)

// KVServiceServer is the gRPC server for the KV service
type KVServiceServer struct {
	raft         *babuza.Raft
	stateMachine *statemachine.MemoryStore
}

// GrpcServer wraps the gRPC server
type GrpcServer struct {
	server       *grpc.Server
	kvService    *KVServiceServer
	raft         *babuza.Raft
	stateMachine *statemachine.MemoryStore
}

// NewGrpcServer creates a new gRPC server
func NewGrpcServer(raft *babuza.Raft, stateMachine *statemachine.MemoryStore) *GrpcServer {
	server := grpc.NewServer()
	kvService := &KVServiceServer{
		raft:         raft,
		stateMachine: stateMachine,
	}
	kvbenchpb.RegisterKVServiceServer(server, kvService)
	return &GrpcServer{
		server:       server,
		kvService:    kvService,
		raft:         raft,
		stateMachine: stateMachine,
	}
}

// Serve starts the gRPC server
func (s *GrpcServer) Serve(lis net.Listener) error {
	return s.server.Serve(lis)
}

// Stop stops the gRPC server
func (s *GrpcServer) Stop() {
	s.server.GracefulStop()
}

// Put implements the Put RPC
func (s *KVServiceServer) Put(ctx context.Context, req *kvbenchpb.PutRequest) (*kvbenchpb.PutResponse, error) {
	// Create a command
	cmd := kvbenchpb.KvOP{
		Command: kvbenchpb.KvCommand_PUT,
		Key:     req.Key,
		Value:   req.Value,
	}
	data, err := cmd.Marshal()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create command: %v", err)
	}

	// Propose the command to Raft
	result := s.raft.Propose(ctx, babuza.ClientSession{}, data)
	defer result.Release()

	// Wait for the result
	applyResult := result.WaitForApplyResult()
	if applyResult.Error != nil {
		return nil, status.Errorf(codes.Internal, "failed to apply command: %v", applyResult.Error)
	}
	sta := s.raft.Status()
	// Return the response
	return &kvbenchpb.PutResponse{
		Header: &kvbenchpb.ResponseHeader{
			ClusterID: sta.ClusterID,
			PeerID:    sta.LocalPeerID,
			RaftTerm:  sta.RaftTerm,
		},
	}, nil
}

// Get implements the Get RPC
func (s *KVServiceServer) Get(ctx context.Context, req *kvbenchpb.GetRequest) (*kvbenchpb.GetResponse, error) {
	// not implemented yet
	return &kvbenchpb.GetResponse{}, nil
}

// Delete implements the Delete RPC
func (s *KVServiceServer) Delete(ctx context.Context, req *kvbenchpb.DeleteRequest) (*kvbenchpb.DeleteResponse, error) {
	// not implemented yet
	return &kvbenchpb.DeleteResponse{}, nil
}

func (s *KVServiceServer) ClusterConfiguration(ctx context.Context, req *kvbenchpb.ClusterPeersRequest) (*kvbenchpb.ClusterPeersResponse, error) {
	var peerAttr []*kvbenchpb.RaftPeerAttribute
	cfg := s.raft.ClusterConfiguration()
	for _, peer := range cfg.Peers {
		peerAttr = append(peerAttr, &kvbenchpb.RaftPeerAttribute{
			PeerID:         peer.RaftPeerAttr.Id,
			RaftListenAddr: peer.RaftPeerAttr.RaftListenAddr,
			GrpcListenAddr: peer.AppServiceAddresses[0],
			IsLearner:      peer.RaftPeerAttr.IsLearner,
		})
	}
	return &kvbenchpb.ClusterPeersResponse{
		ClusterID: cfg.ClusterID,
		PeerID:    cfg.LocalPeerID,
		GroupPeers: []*kvbenchpb.GroupRaftPeerAttribute{
			{
				LeaderID: cfg.LeaderID,
				Peers:    peerAttr,
			},
		},
	}, nil
}
