package multi

import (
	"context"
	"github.com/fanaujie/babuza/ibabuza"
	babuza "github.com/fanaujie/babuza/raft"
	"github.com/fanaujie/babuza/raft/multiraft"
	"github.com/fanaujie/babuza/test/kvbench/kvbenchpb"
	"github.com/fanaujie/babuza/test/kvbench/statemachine"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"net"
)

// GrpcServer implements the KVService gRPC service
type GrpcServer struct {
	serverCfg     Config
	multiRaftNode *multiraft.Node
	stateMachines map[ibabuza.RaftGroupID]*statemachine.MemoryStore
	grpcServer    *grpc.Server
	logger        ibabuza.Logger
}

// NewGrpcServer creates a new gRPC server for the KV service
func NewGrpcServer(serverCfg Config, node *multiraft.Node, stores map[ibabuza.RaftGroupID]*statemachine.MemoryStore,
	logger ibabuza.Logger) *GrpcServer {
	grpcServer := grpc.NewServer()
	server := &GrpcServer{
		serverCfg:     serverCfg,
		multiRaftNode: node,
		stateMachines: stores,
		grpcServer:    grpcServer,
		logger:        logger,
	}
	kvbenchpb.RegisterKVServiceServer(grpcServer, server)
	return server
}

// Serve starts the gRPC server
func (s *GrpcServer) Serve(lis net.Listener) error {
	return s.grpcServer.Serve(lis)
}

// Stop stops the gRPC server
func (s *GrpcServer) Stop() {
	s.grpcServer.GracefulStop()
}

// Put implements the Put RPC
func (s *GrpcServer) Put(ctx context.Context, req *kvbenchpb.PutRequest) (*kvbenchpb.PutResponse, error) {
	// Validate the request
	if req.GroupID == 0 || len(req.Key) == 0 {
		return nil, status.Error(codes.InvalidArgument, "invalid put request")
	}

	// Check if the group exists
	groupID := ibabuza.RaftGroupID(req.GroupID)
	if !s.multiRaftNode.HasGroupID(groupID) {
		return nil, status.Errorf(codes.NotFound, "raft group %d not found", req.GroupID)
	}

	// Create the operation
	op := &kvbenchpb.KvOP{
		Command: kvbenchpb.KvCommand_PUT,
		Key:     req.Key,
		Value:   req.Value,
	}

	// Serialize the operation
	data, err := op.Marshal()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to marshal request: %v", err)
	}

	// Propose the change to the Raft group
	result := s.multiRaftNode.Propose(ctx, groupID, babuza.ClientSession{}, data)
	defer result.Release()
	applyResult := result.WaitForApplyResult()
	if applyResult.Error != nil {
		return nil, status.Errorf(codes.Internal, "failed to apply command: %v", applyResult.Error)
	}
	// Get the status to include in the response
	sta, _ := s.multiRaftNode.Status(groupID)
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
func (s *GrpcServer) Get(ctx context.Context, req *kvbenchpb.GetRequest) (*kvbenchpb.GetResponse, error) {
	// not implemented yet
	return &kvbenchpb.GetResponse{}, nil
}

// Delete implements the Delete RPC
func (s *GrpcServer) Delete(ctx context.Context, req *kvbenchpb.DeleteRequest) (*kvbenchpb.DeleteResponse, error) {
	// not implemented yet
	return &kvbenchpb.DeleteResponse{}, nil
}

// ClusterConfiguration implements the ClusterConfiguration RPC
func (s *GrpcServer) ClusterConfiguration(ctx context.Context, req *kvbenchpb.ClusterPeersRequest) (*kvbenchpb.ClusterPeersResponse, error) {
	groups := s.multiRaftNode.GetGroupIDs()
	if len(groups) == 0 {
		return nil, status.Error(codes.NotFound, "no raft groups found")
	}

	resp := &kvbenchpb.ClusterPeersResponse{
		ClusterID: req.ClusterID,
		PeerID:    s.serverCfg.LocalPeerID,
	}

	// Get configuration for each group
	for _, groupID := range groups {
		config, err := s.multiRaftNode.Configuration(groupID)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to get configuration for group %d: %v", groupID, err)
		}

		// Create peer attributes for this group
		peers := make([]*kvbenchpb.RaftPeerAttribute, 0, len(config.Peers))
		for _, peer := range config.Peers {
			peers = append(peers, &kvbenchpb.RaftPeerAttribute{
				PeerID:         peer.RaftPeerAttr.PeerID,
				RaftListenAddr: peer.RaftPeerAttr.RaftListenAddr,
				GrpcListenAddr: s.serverCfg.InitialGRPCPeers[peer.RaftPeerAttr.PeerID],
				IsLearner:      peer.RaftPeerAttr.IsLearner,
			})
		}

		// Add this group to the response
		resp.GroupPeers = append(resp.GroupPeers, &kvbenchpb.GroupRaftPeerAttribute{
			GroupID:  uint64(groupID),
			LeaderID: config.LeaderID,
			Peers:    peers,
		})
	}

	return resp, nil
}
