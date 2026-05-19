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
	"sync"
)

type MultiRaftMsgClient struct {
	resolver ibabuza.MultiRaftTransportResolver
	pool     connpool.Pool[*grpc.ClientConn]
	cfg      ClientConfig
	logger   ibabuza.Logger

	streamMu    sync.RWMutex
	streamCache map[string]pb.MultiRaftTransport_SendMultiRaftMessageClient
	streamConn  map[string]*grpc.ClientConn
}

func NewMultiRaftMsgClient(
	pool connpool.Pool[*grpc.ClientConn],
	resolver ibabuza.MultiRaftTransportResolver,
	cfg ClientConfig,
	logger ibabuza.Logger,
) *MultiRaftMsgClient {
	return &MultiRaftMsgClient{
		resolver:    resolver,
		pool:        pool,
		cfg:         cfg,
		logger:      logger,
		streamCache: make(map[string]pb.MultiRaftTransport_SendMultiRaftMessageClient),
		streamConn:  make(map[string]*grpc.ClientConn),
	}
}

func (r *MultiRaftMsgClient) getConnection(addr string) (*grpc.ClientConn, error) {
	conn, err := r.pool.Get(addr)
	if err != nil {
		return nil, fmt.Errorf("failed to get connection from pool: %w", err)
	}
	return conn, nil
}

func (r *MultiRaftMsgClient) getStream(addr string) (pb.MultiRaftTransport_SendMultiRaftMessageClient, error) {
	r.streamMu.RLock()

	if stream, ok := r.streamCache[addr]; ok {
		r.streamMu.RUnlock()
		return stream, nil
	}
	r.streamMu.RUnlock()

	conn, err := r.getConnection(addr)
	if err != nil {
		return nil, err
	}

	client := pb.NewMultiRaftTransportClient(conn)
	stream, err := client.SendMultiRaftMessage(context.TODO())
	if err != nil {
		r.pool.Remove(conn)
		return nil, fmt.Errorf("failed to create stream: %w", err)
	}
	r.streamMu.Lock()
	r.streamConn[addr] = conn
	r.streamMu.Unlock()
	r.streamCache[addr] = stream
	go r.receiveResponses(addr, stream)
	return stream, nil
}

func (r *MultiRaftMsgClient) receiveResponses(addr string, stream pb.MultiRaftTransport_SendMultiRaftMessageClient) {
	for {
		_, err := stream.Recv()
		if err != nil {
			r.logger.Errorf("Error receiving from stream for %s: %v", addr, err)
			r.closeStream(addr)
			return
		}
	}
}

func (r *MultiRaftMsgClient) closeStream(addr string) {
	r.streamMu.Lock()
	defer r.streamMu.Unlock()
	if s, ok := r.streamCache[addr]; ok {
		s.CloseSend()
		delete(r.streamCache, addr)
		if conn, ok := r.streamConn[addr]; ok {
			r.pool.Remove(conn)
			delete(r.streamConn, addr)
		}
	}

}

func (r *MultiRaftMsgClient) SendMultiRaftMessage(batchMsg babuzapb.MultiRaftBatchMessage) error {
	if batchMsg.Messages == nil || len(batchMsg.Messages) == 0 {
		return fmt.Errorf("batch message is empty")
	}

	// Get the first message to determine the peerID and groupID,
	// and use it to get the stream. Because the batch message is sent to the same node.
	groupID := ibabuza.RaftGroupID(batchMsg.Messages[0].GroupID)
	peerID := batchMsg.Messages[0].Message.To
	addr, err := r.resolver.ResolvePeerAddress(groupID, peerID)
	if err != nil {
		return fmt.Errorf("failed to resolve peer address: %w", err)
	}
	stream, err := r.getStream(addr)
	if err != nil {
		return err
	}

	if err = stream.Send(&batchMsg); err != nil {
		r.closeStream(addr)
		return fmt.Errorf("failed to send batch message: %w", err)
	}
	return nil
}

func (r *MultiRaftMsgClient) SendSnapshotMessage(snapMsg babuzapb.SnapshotMessage) (babuzapb.SnapshotMessageResponse, error) {
	addr, err := r.resolver.ResolvePeerAddress(ibabuza.RaftGroupID(snapMsg.GroupID), snapMsg.To)
	if err != nil {
		return babuzapb.SnapshotMessageResponse{}, fmt.Errorf("failed to resolve peer address: %w", err)
	}
	conn, err := r.getConnection(addr)
	if err != nil {
		return babuzapb.SnapshotMessageResponse{}, err
	}
	defer func() {
		r.returnPool(conn, err)
	}()

	client := pb.NewMultiRaftTransportClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), r.cfg.GrpcDeadline)
	defer cancel()

	res, err := client.SendSnapshotMessage(ctx, &snapMsg)
	if err != nil {
		return babuzapb.SnapshotMessageResponse{}, err
	}
	if res == nil {
		return babuzapb.SnapshotMessageResponse{}, fmt.Errorf("snapshot response is nil")
	}
	return *res, nil
}

func (r *MultiRaftMsgClient) GetClusterPeers(request babuzapb.GetClusterPeersRequest) (babuzapb.GetClusterPeersResponse, error) {
	var res babuzapb.GetClusterPeersResponse
	addr, err := r.resolver.ResolvePeerAddress(ibabuza.RaftGroupID(request.GroupID), request.To)
	if err != nil {
		return res, fmt.Errorf("failed to resolve peer address: %w", err)
	}
	conn, err := r.getConnection(addr)
	if err != nil {
		res.Status = babuzapb.FAILED
		res.Message = err.Error()
		return res, nil
	}
	defer func() {
		r.returnPool(conn, err)
	}()

	client := pb.NewMultiRaftTransportClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), r.cfg.GrpcDeadline)
	defer cancel()

	response, err := client.GetClusterPeers(ctx, &request)
	if err != nil {
		res.Status = babuzapb.FAILED
		res.Message = err.Error()
		return res, nil
	}

	return *response, nil
}

func (r *MultiRaftMsgClient) Close() error {
	r.streamMu.Lock()
	defer r.streamMu.Unlock()

	for _, stream := range r.streamCache {
		stream.CloseSend()
	}
	for _, conn := range r.streamConn {
		r.pool.Put(conn)
	}
	r.streamCache = make(map[string]pb.MultiRaftTransport_SendMultiRaftMessageClient)
	return nil
}

func (r *MultiRaftMsgClient) returnPool(c *grpc.ClientConn, err error) {
	if err == nil {
		r.pool.Put(c)
	} else {
		r.pool.Remove(c)
	}
}
