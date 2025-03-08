package tcp

import (
	"fmt"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/transport/protocol/tcp/frame"
	"github.com/fanaujie/babuza/pkg/utility/allocator"
	"net"
	"time"
)

type Options struct {
	WriteDeadline time.Duration
	ReadDeadline  time.Duration
	MaxBufferSize int
}

type RaftMsgClient struct {
	conn    net.Conn
	reader  *frame.Reader
	writer  *frame.Writer
	options Options
}

func NewRaftMsgClient(conn net.Conn, options Options) *RaftMsgClient {
	//TODO: connection pool?
	return &RaftMsgClient{
		conn:    conn,
		reader:  frame.NewReader(conn, options.MaxBufferSize),
		writer:  frame.NewWriter(conn),
		options: options,
	}

}

func (r *RaftMsgClient) SendBatchMessage(batchMsg babuzapb.BatchMessage) error {
	//TODO: retry if failed?
	if err := r.conn.SetWriteDeadline(time.Now().Add(r.options.WriteDeadline)); err != nil {
		return err
	}
	byteSlice := allocator.Acquire(r.options.MaxBufferSize)
	defer allocator.Release(byteSlice)
	return r.writer.Encode(byteSlice.Buffer, frame.BatchMsgType, &batchMsg)
}

func (r *RaftMsgClient) SendSnapshotMessage(snapMsg babuzapb.SnapshotMessage) error {
	//TODO: retry if failed?

	if err := r.conn.SetWriteDeadline(time.Now().Add(r.options.WriteDeadline)); err != nil {
		return err
	}
	byteSlice := allocator.Acquire(r.options.MaxBufferSize)
	defer allocator.Release(byteSlice)
	return r.writer.Encode(byteSlice.Buffer, frame.SnapshotMsgType, &snapMsg)
}
func (r *RaftMsgClient) GetClusterPeers(request babuzapb.GetClusterPeersRequest) (babuzapb.GetClusterPeersResponse, error) {
	var res babuzapb.GetClusterPeersResponse
	if err := r.conn.SetWriteDeadline(time.Now().Add(r.options.WriteDeadline)); err != nil {
		return res, err
	}
	byteSlice := allocator.Acquire(r.options.MaxBufferSize)
	defer allocator.Release(byteSlice)
	if err := r.writer.Encode(byteSlice.Buffer, frame.ClusterPeersReqType, &request); err != nil {
		return res, err
	}
	if err := r.conn.SetReadDeadline(time.Now().Add(r.options.ReadDeadline)); err != nil {
		return res, err
	}
	if err := r.reader.ReadFrame(func(msgType frame.MessageType, msgBuf []byte) error {
		if msgType != frame.ClusterPeersResType {
			return fmt.Errorf("unexpected message type: %v", msgType)
		}
		return res.Unmarshal(msgBuf)
	}); err != nil {
		return res, err
	}
	return res, nil
}
func (r *RaftMsgClient) PublishApplicationService(request babuzapb.PublishApplicationServiceRequest) (
	babuzapb.PublishApplicationServiceResponse, error) {

	var res babuzapb.PublishApplicationServiceResponse
	if err := r.conn.SetWriteDeadline(time.Now().Add(r.options.WriteDeadline)); err != nil {
		return res, err
	}
	byteSlice := allocator.Acquire(r.options.MaxBufferSize)
	defer allocator.Release(byteSlice)
	if err := r.writer.Encode(byteSlice.Buffer, frame.PubAppServiceReqType, &request); err != nil {
		return res, err
	}
	if err := r.conn.SetReadDeadline(time.Now().Add(r.options.ReadDeadline)); err != nil {
		return res, err
	}
	if err := r.reader.ReadFrame(func(msgType frame.MessageType, msgBuf []byte) error {
		if msgType != frame.PubAppServiceResType {
			return fmt.Errorf("unexpected message type: %v", msgType)
		}
		return res.Unmarshal(msgBuf)
	}); err != nil {
		return res, err
	}
	return res, nil
}
func (r *RaftMsgClient) Close() error {
	return r.conn.Close()
}
