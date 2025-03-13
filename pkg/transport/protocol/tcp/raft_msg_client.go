package tcp

import (
	"fmt"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/transport/protocol/tcp/connpool"
	"github.com/fanaujie/babuza/pkg/transport/protocol/tcp/connpool/frame"
)

type RaftMsgClient struct {
	resolver ibabuza.TransportResolver
	connPool *connpool.ConnectionPool
}

func NewRaftMsgClient(pool *connpool.ConnectionPool, resolver ibabuza.TransportResolver) *RaftMsgClient {
	return &RaftMsgClient{
		resolver: resolver,
		connPool: pool,
	}
}

func (r *RaftMsgClient) getConnection(peerId uint64) (*connpool.Connection, error) {
	addr, err := r.resolver.ResolvePeerAddress(peerId)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve peer address: %w", err)
	}
	conn, err := r.connPool.GetConnection(addr)
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
	return conn.SendFrame(frame.BatchMsgType, &batchMsg)
}

func (r *RaftMsgClient) SendSnapshotMessage(snapMsg babuzapb.SnapshotMessage) error {
	conn, err := r.getConnection(snapMsg.To)
	if err != nil {
		return err
	}
	return conn.SendFrame(frame.SnapshotMsgType, &snapMsg)
}

func (r *RaftMsgClient) GetClusterPeers(request babuzapb.GetClusterPeersRequest) babuzapb.GetClusterPeersResponse {
	var res babuzapb.GetClusterPeersResponse

	conn, err := r.getConnection(request.ToId)
	if err != nil {
		res.Status = babuzapb.FAILED
		res.Message = err.Error()
		return res
	}
	err = conn.SendFrame(frame.ClusterPeersReqType, &request)
	if err != nil {
		res.Status = babuzapb.FAILED
		res.Message = err.Error()
		return res
	}

	err = conn.ReadFrame(func(msgType frame.MessageType, msgBuf []byte) error {
		if msgType != frame.ClusterPeersResType {
			return fmt.Errorf("unexpected message type: %v", msgType)
		}
		return res.Unmarshal(msgBuf)
	})
	if err != nil {
		res.Status = babuzapb.FAILED
		res.Message = err.Error()
		return res
	}
	return res
}

func (r *RaftMsgClient) PublishApplicationService(request babuzapb.PublishApplicationServiceRequest) babuzapb.PublishApplicationServiceResponse {
	var res babuzapb.PublishApplicationServiceResponse
	conn, err := r.getConnection(request.ToId)
	if err != nil {
		res.Status = babuzapb.FAILED
		res.Message = err.Error()
		return res
	}
	err = conn.SendFrame(frame.PubAppServiceReqType, &request)
	if err != nil {
		res.Status = babuzapb.FAILED
		res.Message = err.Error()
		return res
	}
	err = conn.ReadFrame(func(msgType frame.MessageType, msgBuf []byte) error {
		if msgType != frame.PubAppServiceResType {
			return fmt.Errorf("unexpected message type: %v", msgType)
		}
		return res.Unmarshal(msgBuf)
	})
	if err != nil {
		res.Status = babuzapb.FAILED
		res.Message = err.Error()
		return res
	}
	return res
}

func (r *RaftMsgClient) Close() error {
	return nil
}
