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
	resolver ibabuza.TransportResolver
	pool     connpool.Pool[*grpc.ClientConn]
	cfg      ClientConfig
	logger   ibabuza.Logger

	streamMu    sync.RWMutex
	streamCache map[uint64]pb.MultiRaftTransport_SendMultiRaftMessageClient
	streamConn  map[uint64]*grpc.ClientConn
}

func NewMultiRaftMsgClient(
	pool connpool.Pool[*grpc.ClientConn],
	resolver ibabuza.TransportResolver,
	cfg ClientConfig,
	logger ibabuza.Logger,
) *MultiRaftMsgClient {
	return &MultiRaftMsgClient{
		resolver:    resolver,
		pool:        pool,
		cfg:         cfg,
		logger:      logger,
		streamCache: make(map[uint64]pb.MultiRaftTransport_SendMultiRaftMessageClient),
		streamConn:  make(map[uint64]*grpc.ClientConn),
	}
}

func (r *MultiRaftMsgClient) getConnection(peerID uint64) (*grpc.ClientConn, error) {
	addr, err := r.resolver.ResolvePeerAddress(peerID)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve peer address: %w", err)
	}
	conn, err := r.pool.Get(addr)
	if err != nil {
		return nil, fmt.Errorf("failed to get connection from pool: %w", err)
	}
	return conn, nil
}

func (r *MultiRaftMsgClient) GetStream(peerID uint64) (pb.MultiRaftTransport_SendMultiRaftMessageClient, error) {
	r.streamMu.RLock()

	if stream, ok := r.streamCache[peerID]; ok {
		r.streamMu.RUnlock()
		return stream, nil
	}
	r.streamMu.RUnlock()

	conn, err := r.getConnection(peerID)
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
	r.streamConn[peerID] = conn
	r.streamMu.Unlock()
	r.streamCache[peerID] = stream
	go r.receiveResponses(peerID, stream)
	return stream, nil
}

func (r *MultiRaftMsgClient) receiveResponses(peerID uint64, stream pb.MultiRaftTransport_SendMultiRaftMessageClient) {
	for {
		_, err := stream.Recv()
		if err != nil {
			r.logger.Errorf("Error receiving from stream for peer %d: %v", peerID, err)
			r.closeStream(peerID)
			return
		}
	}
}

func (r *MultiRaftMsgClient) closeStream(peerID uint64) {
	r.streamMu.Lock()
	defer r.streamMu.Unlock()
	if s, ok := r.streamCache[peerID]; ok {
		s.CloseSend()
		delete(r.streamCache, peerID)
		if conn, ok := r.streamConn[peerID]; ok {
			r.pool.Remove(conn)
			delete(r.streamConn, peerID)
		}
	}

}

func (r *MultiRaftMsgClient) SendMultiRaftMessage(batchMsg babuzapb.MultiRaftBatchMessage) error {
	if batchMsg.Messages == nil || len(batchMsg.Messages) == 0 {
		return fmt.Errorf("batch message is empty")
	}

	peerID := batchMsg.Messages[0].Message.To
	stream, err := r.GetStream(peerID)
	if err != nil {
		return err
	}

	if err = stream.Send(&batchMsg); err != nil {
		r.closeStream(peerID)
		return fmt.Errorf("failed to send batch message: %w", err)
	}
	return nil
}

func (r *MultiRaftMsgClient) SendBatchMessage(batchMsg babuzapb.BatchMessage) error {
	return nil
}

func (r *MultiRaftMsgClient) SendSnapshotMessage(snapMsg babuzapb.SnapshotMessage) (babuzapb.SnapshotMessageResponse, error) {
	conn, err := r.getConnection(snapMsg.To)
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
	return *res, err
}

func (r *MultiRaftMsgClient) GetClusterPeers(request babuzapb.GetClusterPeersRequest) (babuzapb.GetClusterPeersResponse, error) {
	var res babuzapb.GetClusterPeersResponse

	conn, err := r.getConnection(request.To)
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

func (r *MultiRaftMsgClient) PublishApplicationService(request babuzapb.PublishApplicationServiceRequest) (babuzapb.PublishApplicationServiceResponse, error) {
	// No implementation needed
	return babuzapb.PublishApplicationServiceResponse{}, nil
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
	r.streamCache = make(map[uint64]pb.MultiRaftTransport_SendMultiRaftMessageClient)
	return nil
}

func (r *MultiRaftMsgClient) returnPool(c *grpc.ClientConn, err error) {
	if err == nil {
		r.pool.Put(c)
	} else {
		r.pool.Remove(c)
	}
}
