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
	r.logger.Infof("grpc[raft server] peerID(%d) Start", r.cfg.LocalNodeID)

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
	r.logger.Infof("grpc[raft server] peerID(%d) Stop", r.cfg.LocalNodeID)

	r.server.Stop()

	if r.listener != nil {
		if err := r.listener.Close(); err != nil {
			r.logger.Warningf("grpc[raft server]: failed to close listener. peerID(%d) endpoint(%s) err(%s)",
				r.cfg.LocalNodeID, r.cfg.PeerAddress, err.Error())
		}
	}
	return nil
}

func (r *RaftMsgServer) SendBatchMessage(ctx context.Context, msg *babuzapb.BatchMessage) (*emptypb.Empty, error) {
	r.raft.ProcessBatchMessage(*msg)
	return &emptypb.Empty{}, nil
}

func (r *RaftMsgServer) SendSnapshotMessage(ctx context.Context, msg *babuzapb.SnapshotMessage) (*babuzapb.SnapshotMessageResponse, error) {
	res := r.raft.ProcessSnapshotMessage(*msg)
	return &res, nil
}

func (r *RaftMsgServer) GetClusterPeers(ctx context.Context, req *babuzapb.GetClusterPeersRequest) (*babuzapb.GetClusterPeersResponse, error) {
	res := r.raft.GetClusterPeer(*req)
	return &res, nil
}

func (r *RaftMsgServer) PublishApplicationService(ctx context.Context, req *babuzapb.PublishApplicationServiceRequest) (*babuzapb.PublishApplicationServiceResponse, error) {
	res := r.raft.PublishApplicationService(*req)
	return &res, nil
}
