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
		return nil, fmt.Errorf("failed to resolve peer address: %w", err)
	}
	conn, err := r.pool.Get(addr)
	if err != nil {
		return nil, fmt.Errorf("failed to get connection from pool: %w", err)
	}
	return conn, nil
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
		return fmt.Errorf("failed to send batch message: %w", err)
	}
	return nil
}

func (r *RaftMsgClient) SendSnapshotMessage(snapMsg babuzapb.SnapshotMessage) error {
	conn, err := r.getConnection(snapMsg.To)
	if err != nil {
		return err
	}
	defer func() {
		r.returnPool(conn, err)
	}()

	client := pb.NewRaftTransportClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), r.cfg.GrpcDeadline)
	defer cancel()

	_, err = client.SendSnapshotMessage(ctx, &snapMsg)
	if err != nil {
		return fmt.Errorf("failed to send snapshot message: %w", err)
	}

	return nil
}

func (r *RaftMsgClient) GetClusterPeers(request babuzapb.GetClusterPeersRequest) babuzapb.GetClusterPeersResponse {
	var res babuzapb.GetClusterPeersResponse

	conn, err := r.getConnection(request.To)
	if err != nil {
		res.Status = babuzapb.FAILED
		res.Message = err.Error()
		return res
	}
	defer func() {
		r.returnPool(conn, err)
	}()

	client := pb.NewRaftTransportClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), r.cfg.GrpcDeadline)
	defer cancel()

	response, err := client.GetClusterPeers(ctx, &request)
	if err != nil {
		res.Status = babuzapb.FAILED
		res.Message = err.Error()
		return res
	}

	return *response
}

func (r *RaftMsgClient) PublishApplicationService(request babuzapb.PublishApplicationServiceRequest) babuzapb.PublishApplicationServiceResponse {
	var res babuzapb.PublishApplicationServiceResponse

	conn, err := r.getConnection(request.To)
	if err != nil {
		res.Status = babuzapb.FAILED
		res.Message = err.Error()
		return res
	}
	defer func() {
		r.returnPool(conn, err)
	}()

	client := pb.NewRaftTransportClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), r.cfg.GrpcDeadline)
	defer cancel()

	response, err := client.PublishApplicationService(ctx, &request)
	if err != nil {
		res.Status = babuzapb.FAILED
		res.Message = err.Error()
		return res
	}

	return *response
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
