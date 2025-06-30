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
	"time"
)

type ClientConfig struct {
	GrpcDeadline time.Duration
}

type RaftMsgClient struct {
	resolver ibabuza.TransportResolver
	pool     connpool.Pool[*grpc.ClientConn]
	cfg      ClientConfig
}

func NewRaftMsgClient(pool connpool.Pool[*grpc.ClientConn], resolver ibabuza.TransportResolver, cfg ClientConfig) *RaftMsgClient {
	return &RaftMsgClient{
		resolver: resolver,
		pool:     pool,
		cfg:      cfg,
	}
}

func (r *RaftMsgClient) getConnection(peerID uint64) (*grpc.ClientConn, error) {
	addr, err := r.resolver.ResolvePeerAddress(peerID)
	if err != nil {
		return nil, err
	}
	conn, err := r.pool.Get(addr)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func (r *RaftMsgClient) SendMultiRaftMessage(babuzapb.MultiRaftBatchMessage) error {
	// not supported
	return nil
}

func (r *RaftMsgClient) SendBatchMessage(batchMsg babuzapb.BatchMessage) error {
	if batchMsg.Messages == nil || len(batchMsg.Messages) == 0 {
		return fmt.Errorf("batch message is empty")
	}

	conn, err := r.getConnection(batchMsg.Messages[0].To)
	if err != nil {
		return err
	}
	defer func() {
		r.returnPool(conn, err)
	}()

	client := pb.NewRaftTransportClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), r.cfg.GrpcDeadline)
	defer cancel()
	_, err = client.SendBatchMessage(ctx, &batchMsg)
	if err != nil {
		return err
	}
	return nil
}

func (r *RaftMsgClient) SendSnapshotMessage(snapMsg babuzapb.SnapshotMessage) (babuzapb.SnapshotMessageResponse, error) {
	conn, err := r.getConnection(snapMsg.To)
	if err != nil {
		return babuzapb.SnapshotMessageResponse{}, err
	}
	defer func() {
		r.returnPool(conn, err)
	}()

	client := pb.NewRaftTransportClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), r.cfg.GrpcDeadline)
	defer cancel()

	res, err := client.SendSnapshotMessage(ctx, &snapMsg)
	if err != nil {
		return babuzapb.SnapshotMessageResponse{}, err
	}
	return *res, nil
}

func (r *RaftMsgClient) GetClusterPeers(request babuzapb.GetClusterPeersRequest) (babuzapb.GetClusterPeersResponse, error) {
	var res babuzapb.GetClusterPeersResponse

	conn, err := r.getConnection(request.To)
	if err != nil {
		return res, err
	}
	defer func() {
		r.returnPool(conn, err)
	}()

	client := pb.NewRaftTransportClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), r.cfg.GrpcDeadline)
	defer cancel()

	response, err := client.GetClusterPeers(ctx, &request)
	if err != nil {
		return res, err
	}
	return *response, nil
}

func (r *RaftMsgClient) PublishApplicationService(request babuzapb.PublishApplicationServiceRequest) (babuzapb.PublishApplicationServiceResponse, error) {
	var res babuzapb.PublishApplicationServiceResponse

	conn, err := r.getConnection(request.To)
	if err != nil {
		return res, err
	}
	defer func() {
		r.returnPool(conn, err)
	}()

	client := pb.NewRaftTransportClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), r.cfg.GrpcDeadline)
	defer cancel()

	response, err := client.PublishApplicationService(ctx, &request)
	if err != nil {
		return res, err
	}

	return *response, nil
}

func (r *RaftMsgClient) Close() error {
	return nil
}

func (r *RaftMsgClient) returnPool(c *grpc.ClientConn, err error) {
	if err == nil {
		r.pool.Put(c)
	} else {
		r.pool.Remove(c)
	}
}
