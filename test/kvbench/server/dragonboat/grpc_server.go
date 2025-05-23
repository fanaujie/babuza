package dragonboat

import (
	"context"
	"github.com/fanaujie/babuza/test/kvbench/kvbenchpb"
	"github.com/lni/dragonboat/v4"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"net"
)

// GrpcServer implements the KVService gRPC service
type GrpcServer struct {
	serverCfg  Config
	nh         *dragonboat.NodeHost
	grpcServer *grpc.Server
}

// NewGrpcServer creates a new gRPC server for the KV service
func NewGrpcServer(serverCfg Config, nh *dragonboat.NodeHost) *GrpcServer {
	grpcServer := grpc.NewServer()
	server := &GrpcServer{
		serverCfg:  serverCfg,
		nh:         nh,
		grpcServer: grpcServer,
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

	_, err = s.nh.SyncPropose(ctx, s.nh.GetNoOPSession(req.GroupID), data)
	if err != nil {
		return nil, err
	}

	return &kvbenchpb.PutResponse{
		Header: &kvbenchpb.ResponseHeader{
			ClusterID: s.serverCfg.ClusterID,
			PeerID:    s.serverCfg.LocalPeerID,
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

	resp := &kvbenchpb.ClusterPeersResponse{
		ClusterID: req.ClusterID,
		PeerID:    s.serverCfg.LocalPeerID,
	}

	info := s.nh.GetNodeHostInfo(dragonboat.DefaultNodeHostInfoOption)
	for _, shardInfo := range info.ShardInfoList {
		peers := make([]*kvbenchpb.RaftPeerAttribute, 0)
		for replicaID, raftAddr := range shardInfo.Replicas {
			peers = append(peers, &kvbenchpb.RaftPeerAttribute{
				PeerID:         replicaID,
				RaftListenAddr: raftAddr,
				GrpcListenAddr: s.serverCfg.InitialGRPCPeers[replicaID],
				IsLearner:      false,
			})
		}

		// Add this group to the response
		resp.GroupPeers = append(resp.GroupPeers, &kvbenchpb.GroupRaftPeerAttribute{
			GroupID:  shardInfo.ShardID,
			LeaderID: shardInfo.LeaderID,
			Peers:    peers,
		})
	}
	return resp, nil
}
