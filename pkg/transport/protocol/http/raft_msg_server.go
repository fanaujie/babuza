package http

import (
	"context"
	"crypto/tls"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/pkg/utility/allocator"
	"github.com/fanaujie/babuza/pkg/utility/netutil"
	"github.com/gogo/protobuf/proto"
	"io"
	"net"
	"net/http"
	"time"
)

type ServerConfig struct {
	WriteDeadline   time.Duration
	ReadDeadline    time.Duration
	ShutdownTimeout time.Duration
}

type RaftMsgServer struct {
	cfg    ibabuza.TransportConfig
	raft   ibabuza.RaftMessageHandler
	config ServerConfig
	logger ibabuza.Logger
	srv    *http.Server
}

func NewRaftMsgServer(cfg ibabuza.TransportConfig, config ServerConfig, raft ibabuza.RaftMessageHandler,
	logger ibabuza.Logger) *RaftMsgServer {

	r := &RaftMsgServer{
		cfg:    cfg,
		raft:   raft,
		config: config,
		logger: logger,
	}
	mux := http.NewServeMux()
	h := &handler{
		raft: raft,
	}
	mux.HandleFunc(raftBatchMsgPrefix, h.batchMessageFunc)
	mux.HandleFunc(raftSnapshotMsgPrefix, h.snapshotMessageFunc)
	mux.HandleFunc(raftClusterPeersPrefix, h.clusterPeersFunc)
	mux.HandleFunc(raftAppServiceUrlsPrefix, h.publishApplicationServiceFunc)
	r.srv = &http.Server{
		Addr:         cfg.PeerAddress,
		Handler:      mux,
		ReadTimeout:  config.ReadDeadline,
		WriteTimeout: config.WriteDeadline,
	}
	return r
}

func (r *RaftMsgServer) Start() error {
	var err error
	r.logger.Infof("http[raft server] peerId(%d) Start", r.cfg.PeerId)

	tlsCfg, err := netutil.GetServerTlsConfig(r.cfg.TLSConfig)
	if err != nil {
		return err
	}
	var listener net.Listener
	if tlsCfg == nil {
		listener, err = net.Listen("tcp", r.cfg.PeerAddress)
	} else {
		listener, err = tls.Listen("tcp", r.cfg.PeerAddress, tlsCfg)
	}
	go r.srv.Serve(listener)
	return nil
}

func (r *RaftMsgServer) Stop() error {
	ctx, cancel := context.WithTimeout(context.Background(), r.config.ShutdownTimeout)
	defer cancel()
	if err := r.srv.Shutdown(ctx); err != nil {
		return err
	}
	return nil
}

func (r *RaftMsgServer) decodeExpectedMessage(reader io.Reader, expectedSize int64, expectedMsg proto.Message) error {
	var byteSlice *allocator.ByteSlice
	byteSlice = allocator.Acquire(int(expectedSize))
	defer allocator.Release(byteSlice)
	buf := byteSlice.Buffer[:expectedSize]
	if _, err := io.ReadFull(reader, buf); err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
			return err
		}
		return err
	}
	return proto.Unmarshal(buf, expectedMsg)
}
