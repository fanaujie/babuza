package grpc

import (
	"context"
	"fmt"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/connpool"
	"github.com/fanaujie/babuza/pkg/transport/protocol/grpc/pb"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
	"net"
)

type RaftMsgServer struct {
	pb.RaftTransportServer
	cfg         ibabuza.TransportConfig
	options     connpool.Config
	grpcNetwork NetworkIO
	raft        ibabuza.RaftMessageHandler
	logger      ibabuza.Logger
	server      *grpc.Server
	listener    net.Listener
}

func NewRaftMsgServer(
	cfg ibabuza.TransportConfig,
	grpcNetwork NetworkIO,
	raft ibabuza.RaftMessageHandler,
	logger ibabuza.Logger,
) *RaftMsgServer {
	return &RaftMsgServer{
		cfg:         cfg,
		grpcNetwork: grpcNetwork,
		raft:        raft,
		logger:      logger,
	}
}

func (r *RaftMsgServer) Start() error {
	var err error
	r.logger.Infof("grpc[raft server] peerId(%d) Start", r.cfg.PeerId)

	r.server, err = r.grpcNetwork.NewServer(r.cfg.TLSConfig)
	if err != nil {
		return fmt.Errorf("failed to create gRPC server: %w", err)
	}

	pb.RegisterRaftTransportServer(r.server, r)

	r.listener, err = r.grpcNetwork.Listen(r.cfg.PeerAddress)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}

	go func() {
		if err = r.server.Serve(r.listener); err != nil {
			r.logger.Errorf("grpc[raft server]: failed to serve: %v", err)
		}
	}()
	return nil
}

func (r *RaftMsgServer) Stop() error {
	r.logger.Infof("grpc[raft server] peerId(%d) Stop", r.cfg.PeerId)

	r.server.GracefulStop()

	if r.listener != nil {
		if err := r.listener.Close(); err != nil {
			r.logger.Warningf("grpc[raft server]: failed to close listener. peerId(%d) endpoint(%s) err(%s)",
				r.cfg.PeerId, r.cfg.PeerAddress, err.Error())
		}
	}
	return nil
}

func (r *RaftMsgServer) SendBatchMessage(ctx context.Context, msg *babuzapb.BatchMessage) (*emptypb.Empty, error) {
	r.raft.ProcessBatchMessage(*msg)
	return &emptypb.Empty{}, nil
}

func (r *RaftMsgServer) SendSnapshotMessage(ctx context.Context, msg *babuzapb.SnapshotMessage) (*emptypb.Empty, error) {
	r.raft.ProcessSnapshotMessage(*msg)
	return &emptypb.Empty{}, nil
}

func (r *RaftMsgServer) GetClusterPeers(ctx context.Context, req *babuzapb.GetClusterPeersRequest) (*babuzapb.GetClusterPeersResponse, error) {
	res := r.raft.GetClusterPeersRequest(*req)
	return &res, nil
}

func (r *RaftMsgServer) PublishApplicationService(ctx context.Context, req *babuzapb.PublishApplicationServiceRequest) (*babuzapb.PublishApplicationServiceResponse, error) {
	res := r.raft.PublishApplicationServiceRequest(*req)
	return &res, nil
}
