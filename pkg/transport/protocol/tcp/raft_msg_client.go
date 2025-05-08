package tcp

import (
	"fmt"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/connpool"
	"github.com/fanaujie/babuza/pkg/transport/protocol/tcp/conn"
	"github.com/fanaujie/babuza/pkg/transport/protocol/tcp/conn/frame"
)

type FrameConnection interface {
	SendFrame(msgType frame.MessageType, msg frame.Message) (err error)
	ReadFrame(msgHandler func(msgType frame.MessageType, msgBuf []byte) error) (err error)
}

type RaftMsgClient struct {
	resolver ibabuza.TransportResolver
	pool     connpool.Pool[*conn.FrameConnection]
}

func NewRaftMsgClient(pool connpool.Pool[*conn.FrameConnection], resolver ibabuza.TransportResolver) *RaftMsgClient {
	return &RaftMsgClient{
		resolver: resolver,
		pool:     pool,
	}
}

func (r *RaftMsgClient) getConnection(peerID uint64) (*conn.FrameConnection, error) {
	addr, err := r.resolver.ResolvePeerAddress(peerID)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve peer address: %w", err)
	}
	c, err := r.pool.Get(addr)
	if err != nil {
		return nil, fmt.Errorf("failed to get connection from pool: %w", err)
	}
	return c, nil
}

func (r *RaftMsgClient) SendBatchMessage(batchMsg babuzapb.BatchMessage) error {
	if batchMsg.Messages == nil || len(batchMsg.Messages) == 0 {
		return fmt.Errorf("batch message is empty")
	}
	c, err := r.getConnection(batchMsg.Messages[0].To)
	if err != nil {
		return err
	}
	defer func() {
		r.returnPool(c, err)
	}()
	err = c.SendFrame(frame.BatchMsgType, &batchMsg)
	return err
}

func (r *RaftMsgClient) SendSnapshotMessage(snapMsg babuzapb.SnapshotMessage) (babuzapb.SnapshotMessageResponse, error) {
	var res babuzapb.SnapshotMessageResponse
	c, err := r.getConnection(snapMsg.To)
	if err != nil {
		return res, err
	}
	defer func() {
		r.returnPool(c, err)
	}()
	err = c.SendFrame(frame.SnapshotMsgReqType, &snapMsg)
	if err != nil {
		return res, nil
	}
	err = c.ReadFrame(func(msgType frame.MessageType, msgBuf []byte) error {
		if msgType != frame.SnapshotMsgResType {
			return fmt.Errorf("unexpected message type: %v", msgType)
		}
		return res.Unmarshal(msgBuf)
	})
	if err != nil {
		return res, err
	}
	return res, nil
}

func (r *RaftMsgClient) GetClusterPeers(request babuzapb.GetClusterPeersRequest) (babuzapb.GetClusterPeersResponse, error) {
	var res babuzapb.GetClusterPeersResponse

	c, err := r.getConnection(request.To)
	if err != nil {
		return res, err
	}
	defer func() {
		r.returnPool(c, err)
	}()
	err = c.SendFrame(frame.ClusterPeersReqType, &request)
	if err != nil {
		return res, err
	}

	err = c.ReadFrame(func(msgType frame.MessageType, msgBuf []byte) error {
		if msgType != frame.ClusterPeersResType {
			return fmt.Errorf("unexpected message type: %v", msgType)
		}
		return res.Unmarshal(msgBuf)
	})
	if err != nil {
		return res, err
	}
	return res, nil
}

func (r *RaftMsgClient) PublishApplicationService(request babuzapb.PublishApplicationServiceRequest) (babuzapb.PublishApplicationServiceResponse, error) {
	var res babuzapb.PublishApplicationServiceResponse
	c, err := r.getConnection(request.To)
	if err != nil {
		return res, err
	}
	defer func() {
		r.returnPool(c, err)
	}()

	err = c.SendFrame(frame.PubAppServiceReqType, &request)
	if err != nil {
		return res, err
	}
	err = c.ReadFrame(func(msgType frame.MessageType, msgBuf []byte) error {
		if msgType != frame.PubAppServiceResType {
			return fmt.Errorf("unexpected message type: %v", msgType)
		}
		return res.Unmarshal(msgBuf)
	})
	if err != nil {
		return res, err
	}
	return res, nil
}

func (r *RaftMsgClient) Close() error {
	return nil
}

func (r *RaftMsgClient) returnPool(c *conn.FrameConnection, err error) {
	if err == nil {
		r.pool.Put(c)
	} else {
		r.pool.Remove(c)
	}
}
