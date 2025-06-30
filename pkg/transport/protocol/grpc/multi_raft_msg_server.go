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
	"github.com/fanaujie/babuza/pkg/transport/protocol/grpc/pb"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
	"io"
	"net"
)

type MultiRaftMsgServer struct {
	pb.MultiRaftTransportServer
	cfg         ibabuza.TransportConfig
	grpcNetwork NetworkIO
	raft        ibabuza.MultiRaftStoreHandler
	logger      ibabuza.Logger
	server      *grpc.Server
	listener    net.Listener
}

func NewMultiRaftMsgServer(
	cfg ibabuza.TransportConfig,
	grpcNetwork NetworkIO,
	raft ibabuza.MultiRaftStoreHandler,
	logger ibabuza.Logger,
) *MultiRaftMsgServer {
	return &MultiRaftMsgServer{
		cfg:         cfg,
		grpcNetwork: grpcNetwork,
		raft:        raft,
		logger:      logger,
	}
}

func (r *MultiRaftMsgServer) Start() error {
	var err error
	r.logger.Infof("grpc[multi-raft server] peerID(%d) Start", r.cfg.LocalNodeID)

	r.server, err = r.grpcNetwork.NewServer(r.cfg.TLSConfig)
	if err != nil {
		return fmt.Errorf("failed to create gRPC server: %w", err)
	}

	pb.RegisterMultiRaftTransportServer(r.server, r)

	r.listener, err = r.grpcNetwork.Listen(r.cfg.PeerAddress)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}

	go func() {
		if err = r.server.Serve(r.listener); err != nil {
			r.logger.Errorf("grpc[multi-raft server]: failed to serve: %v", err)
		}
	}()
	return nil
}

func (r *MultiRaftMsgServer) Stop() error {
	r.logger.Infof("grpc[multi-raft server] peerID(%d) Stop", r.cfg.LocalNodeID)

	r.server.Stop()

	if r.listener != nil {
		if err := r.listener.Close(); err != nil {
			r.logger.Warningf("grpc[multi-raft server]: failed to close listener. peerID(%d) endpoint(%s) err(%s)",
				r.cfg.LocalNodeID, r.cfg.PeerAddress, err.Error())
		}
	}
	return nil
}

func (r *MultiRaftMsgServer) SendMultiRaftMessage(stream pb.MultiRaftTransport_SendMultiRaftMessageServer) error {
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			r.logger.Errorf("grpc[multi-raft server]: failed to receive message: %v", err)
			return err
		}
		r.raft.ProcessMultiRaftMessage(*msg)

		if err = stream.Send(&emptypb.Empty{}); err != nil {
			r.logger.Errorf("grpc[multi-raft server]: failed to send response: %v", err)
			return err
		}
	}
}

func (r *MultiRaftMsgServer) SendSnapshotMessage(ctx context.Context, msg *babuzapb.SnapshotMessage) (*babuzapb.SnapshotMessageResponse, error) {
	res := r.raft.ProcessSnapshotMessage(*msg)
	return &res, nil
}

func (r *MultiRaftMsgServer) GetClusterPeers(ctx context.Context, req *babuzapb.GetClusterPeersRequest) (*babuzapb.GetClusterPeersResponse, error) {
	res := r.raft.GetClusterPeer(*req)
	return &res, nil
}
