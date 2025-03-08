package tcp

import (
	"errors"
	"fmt"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/transport/protocol/tcp/frame"
	"github.com/fanaujie/babuza/pkg/utility/allocator"
	"github.com/fanaujie/babuza/pkg/utility/syncutil"
	"net"
	"time"
)

type RaftMsgServer struct {
	cfg         ibabuza.TransportConfig
	options     Options
	tcpListener Listener
	raft        ibabuza.RaftMessageHandler
	logger      ibabuza.Logger
	closer      *syncutil.Closer
	listener    net.Listener
}

func NewRaftMsgServer(cfg ibabuza.TransportConfig, options Options, listener Listener, raft ibabuza.RaftMessageHandler,
	logger ibabuza.Logger) *RaftMsgServer {

	return &RaftMsgServer{
		cfg:         cfg,
		options:     options,
		tcpListener: listener,
		raft:        raft,
		logger:      logger,
		closer:      syncutil.NewCloser(),
	}
}

func (r *RaftMsgServer) Start() error {
	var err error
	r.logger.Infof("tcp[raft server] peerId(%d) Start", r.cfg.PeerId)
	r.listener, err = r.tcpListener.Listen(r.cfg.TLSConfig, r.cfg.PeerAddress)
	if err != nil {
		return err
	}
	r.closer.Run(func() {
		for {
			//TODO: retry if accept failed
			// refence:
			//if ne, ok := err.(net.Error); ok && ne.Temporary() {
			//	if tempDelay == 0 {
			//		tempDelay = 5 * time.Millisecond
			//	} else {
			//		tempDelay *= 2
			//	}
			//	if max := 1 * time.Second; tempDelay > max {
			//		tempDelay = max
			//	}
			//	srv.logf("http: Accept error: %v; retrying in %v", err, tempDelay)
			//	time.Sleep(tempDelay)
			//	continue
			//}

			conn, lErr := r.listener.Accept()
			if lErr != nil {
				select {
				case <-r.closer.CloseCh():
					return
				default:
				}
			} else {
				r.logger.Infof("tcp[raft server] peerId(%d) accept conn from %s", r.cfg.PeerId, conn.RemoteAddr().String())
				s := r.newSession(conn)
				r.closer.Run(func() {
					if sErr := s.start(); sErr != nil {
						r.logger.Warningf("tcp[raft server]: failed to decode session. peerId(%d) endpoint(%s) err(%s)",
							r.cfg.PeerId, r.cfg.PeerAddress, sErr.Error())
					}
				})
			}
		}
	})
	return nil
}

func (r *RaftMsgServer) Stop() error {
	if err := r.listener.Close(); err != nil {
		r.logger.Warningf("tcp[raft server]: failed to close. peerId(%d) endpoint(%s )err(%s)",
			r.cfg.PeerId, r.cfg.PeerAddress, err.Error())
	}
	r.closer.Close()
	return nil
}

func (r *RaftMsgServer) newSession(conn net.Conn) *session {
	return &session{
		options: r.options,
		conn:    conn,
		reader:  frame.NewReader(conn, r.options.MaxBufferSize),
		writer:  frame.NewWriter(conn),
		raft:    r.raft,
		closeCh: r.closer.CloseCh(),
	}
}

type session struct {
	options            Options
	conn               net.Conn
	reader             *frame.Reader
	writer             *frame.Writer
	batchMsg           babuzapb.BatchMessage
	snapshotMsg        babuzapb.SnapshotMessage
	getClusterPeersReq babuzapb.GetClusterPeersRequest
	pubAppServiceReq   babuzapb.PublishApplicationServiceRequest

	raft    ibabuza.RaftMessageHandler
	closeCh <-chan struct{}
}

func (s *session) messageHandler(msgType frame.MessageType, msgBuf []byte) error {
	switch msgType {
	case frame.BatchMsgType:
		if err := s.batchMsg.Unmarshal(msgBuf); err != nil {
			return err
		}
		s.raft.ProcessBatchMessage(s.batchMsg)
		s.batchMsg.Messages = nil
	case frame.SnapshotMsgType:
		if err := s.snapshotMsg.Unmarshal(msgBuf); err != nil {
			return err
		}
		s.raft.ProcessSnapshotMessage(s.snapshotMsg)
		s.snapshotMsg.Metadata = nil
		s.snapshotMsg.ChunkMessage = nil
		s.snapshotMsg.FinishMessage = nil
	case frame.ClusterPeersReqType:
		if err := s.getClusterPeersReq.Unmarshal(msgBuf); err != nil {
			return err
		}
		res := s.raft.GetClusterPeersRequest(s.getClusterPeersReq)
		if err := s.conn.SetWriteDeadline(time.Now().Add(s.options.WriteDeadline)); err != nil {
			return err
		}
		byteSlice := allocator.Acquire(s.options.MaxBufferSize)
		defer allocator.Release(byteSlice)
		return s.writer.Encode(byteSlice.Buffer, frame.ClusterPeersResType, &res)
	case frame.PubAppServiceReqType:
		if err := s.pubAppServiceReq.Unmarshal(msgBuf); err != nil {
			return err
		}
		res := s.raft.PublishApplicationServiceRequest(s.pubAppServiceReq)
		if err := s.conn.SetWriteDeadline(time.Now().Add(s.options.WriteDeadline)); err != nil {
			return err
		}
		byteSlice := allocator.Acquire(s.options.MaxBufferSize)
		defer allocator.Release(byteSlice)
		return s.writer.Encode(byteSlice.Buffer, frame.PubAppServiceResType, &res)
	default:
		return fmt.Errorf("tcp[raft server]: unsupported message type %d", msgType)
	}
	return nil
}
func (s *session) start() error {
	defer s.conn.Close()

	for {
		select {
		case <-s.closeCh:
			return errors.New("tcp server: close")
		default:
			if err := s.conn.SetReadDeadline(time.Now().Add(s.options.ReadDeadline)); err != nil {
				return err
			}
			if err := s.reader.ReadFrame(s.messageHandler); err != nil {
				return err
			}
		}
	}
}
