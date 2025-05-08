package tcp

import (
	"errors"
	"fmt"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/transport/protocol/tcp/conn"
	"github.com/fanaujie/babuza/pkg/transport/protocol/tcp/conn/frame"
	"github.com/fanaujie/babuza/pkg/utility/syncutil"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"net"
	"time"
)

type ServerConfig struct {
	ReadDeadline  time.Duration
	WriteDeadline time.Duration
}

type RaftMsgServer struct {
	cfg         ibabuza.TransportConfig
	config      ServerConfig
	tcpListener Listener
	raft        ibabuza.RaftMessageHandler
	logger      ibabuza.Logger
	closer      *syncutil.Closer
	listener    net.Listener
}

func NewRaftMsgServer(cfg ibabuza.TransportConfig, config ServerConfig, listener Listener, raft ibabuza.RaftMessageHandler,
	logger ibabuza.Logger) *RaftMsgServer {

	return &RaftMsgServer{
		cfg:         cfg,
		config:      config,
		tcpListener: listener,
		raft:        raft,
		logger:      logger,
		closer:      syncutil.NewCloser(),
	}
}

func (r *RaftMsgServer) Start() error {
	var err error
	r.logger.Infof("tcp[raft server] peerID(%d) Start", r.cfg.PeerId)
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

			c, lErr := r.listener.Accept()
			if lErr != nil {
				select {
				case <-r.closer.CloseCh():
					return
				default:
				}
			} else {
				r.logger.Infof("tcp[raft server] peerID(%d) accept conn from %s", r.cfg.PeerId, c.RemoteAddr().String())
				s := r.newSession(c)
				r.closer.Run(func() {
					if sErr := s.start(); sErr != nil {
						r.logger.Warningf("tcp[raft server]: failed to decode session. peerID(%d) endpoint(%s) err(%s)",
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
		r.logger.Warningf("tcp[raft server]: failed to close. peerID(%d) endpoint(%s )err(%s)",
			r.cfg.PeerId, r.cfg.PeerAddress, err.Error())
	}
	r.closer.Close()
	return nil
}

func (r *RaftMsgServer) newSession(c net.Conn) *session {
	return &session{
		config: r.config,
		conn:   c,
		frameConn: conn.NewConnection(c, conn.Config{
			ReadDeadline:  r.config.ReadDeadline,
			WriteDeadline: r.config.WriteDeadline,
		}),
		raft:    r.raft,
		closeCh: r.closer.CloseCh(),
	}
}

type session struct {
	config             ServerConfig
	conn               net.Conn
	frameConn          *conn.FrameConnection
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
	case frame.SnapshotMsgReqType:
		if err := s.snapshotMsg.Unmarshal(msgBuf); err != nil {
			return err
		}
		res := s.raft.ProcessSnapshotMessage(s.snapshotMsg)
		s.snapshotMsg.Metadata = babuzapb.SnapshotMetadata{}
		s.snapshotMsg.ChunkMessage = babuzapb.SnapshotChunkMessage{}
		s.snapshotMsg.FinishMessage = raftpb.Message{}
		if err := s.conn.SetWriteDeadline(time.Now().Add(s.config.WriteDeadline)); err != nil {
			return err
		}
		return s.frameConn.SendFrame(frame.SnapshotMsgResType, &res)
	case frame.ClusterPeersReqType:
		if err := s.getClusterPeersReq.Unmarshal(msgBuf); err != nil {
			return err
		}
		res := s.raft.GetClusterPeer(s.getClusterPeersReq)
		if err := s.conn.SetWriteDeadline(time.Now().Add(s.config.WriteDeadline)); err != nil {
			return err
		}
		return s.frameConn.SendFrame(frame.ClusterPeersResType, &res)
	case frame.PubAppServiceReqType:
		if err := s.pubAppServiceReq.Unmarshal(msgBuf); err != nil {
			return err
		}
		res := s.raft.PublishApplicationService(s.pubAppServiceReq)
		if err := s.conn.SetWriteDeadline(time.Now().Add(s.config.WriteDeadline)); err != nil {
			return err
		}
		return s.frameConn.SendFrame(frame.PubAppServiceResType, &res)
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
			if err := s.conn.SetReadDeadline(time.Now().Add(s.config.ReadDeadline)); err != nil {
				return err
			}
			if err := s.frameConn.ReadFrame(s.messageHandler); err != nil {
				return err
			}
		}
	}
}
