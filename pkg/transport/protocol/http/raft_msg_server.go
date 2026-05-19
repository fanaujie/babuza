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

type serverConnectionContextKey struct{}

type ServerConfig struct {
	WriteDeadline             time.Duration
	ReadDeadline              time.Duration
	ShutdownTimeout           time.Duration
	MessageStreamEnabled      bool
	StreamIdleTimeout         time.Duration
	SnapshotStreamIdleTimeout time.Duration
}

type RaftMsgServer struct {
	cfg              ibabuza.TransportConfig
	raft             ibabuza.RaftMessageHandler
	config           ServerConfig
	logger           ibabuza.Logger
	srv              *http.Server
	messageStreamHub *MessageStreamHub
}

func NewRaftMsgServer(cfg ibabuza.TransportConfig, config ServerConfig, raft ibabuza.RaftMessageHandler,
	logger ibabuza.Logger, hubs ...*MessageStreamHub) *RaftMsgServer {

	var messageHub *MessageStreamHub
	if len(hubs) > 0 {
		messageHub = hubs[0]
	}
	r := &RaftMsgServer{
		cfg:              cfg,
		raft:             raft,
		config:           config,
		logger:           logger,
		messageStreamHub: messageHub,
	}
	mux := http.NewServeMux()
	h := &handler{
		raft:             raft,
		config:           config,
		messageStreamHub: messageHub,
	}
	mux.HandleFunc(raftBatchMsgPrefix, h.batchMessageFunc)
	mux.HandleFunc(raftBatchMsgStreamPrefix, h.batchMessageStreamFunc)
	mux.HandleFunc(raftSnapshotMsgPrefix, h.snapshotMessageFunc)
	mux.HandleFunc(raftSnapshotStreamPrefix, h.snapshotMessageStreamFunc)
	mux.HandleFunc(raftClusterPeersPrefix, h.clusterPeersFunc)
	mux.HandleFunc(raftAppServiceUrlsPrefix, h.publishApplicationServiceFunc)
	readTimeout := config.ReadDeadline
	writeTimeout := config.WriteDeadline
	if config.MessageStreamEnabled {
		readTimeout = 0
		writeTimeout = 0
	}
	r.srv = &http.Server{
		Addr:         cfg.PeerAddress,
		Handler:      mux,
		ReadTimeout:  readTimeout,
		WriteTimeout: writeTimeout,
	}
	if config.MessageStreamEnabled {
		r.srv.ConnContext = func(ctx context.Context, conn net.Conn) context.Context {
			c, ok := conn.(*Connection)
			if !ok {
				return ctx
			}
			return context.WithValue(ctx, serverConnectionContextKey{}, c)
		}
	}
	return r
}

func (r *RaftMsgServer) Start() error {
	var err error
	r.logger.Infof("http[raft server] peerID(%d) Start", r.cfg.LocalNodeID)

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
	if err != nil {
		return err
	}
	if r.config.MessageStreamEnabled {
		listener = &deadlineListener{
			Listener:     listener,
			readTimeout:  r.config.ReadDeadline,
			writeTimeout: r.config.WriteDeadline,
		}
	}
	go r.srv.Serve(listener)
	return nil
}

type deadlineListener struct {
	net.Listener
	readTimeout  time.Duration
	writeTimeout time.Duration
}

func (l *deadlineListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return newConnection(c, l.readTimeout, l.writeTimeout), nil
}

func (r *RaftMsgServer) Stop() error {
	if r.messageStreamHub != nil {
		r.messageStreamHub.closeAll()
	}
	ctx, cancel := context.WithTimeout(context.Background(), r.config.ShutdownTimeout)
	defer cancel()
	if err := r.srv.Shutdown(ctx); err != nil {
		return err
	}
	return nil
}

func (r *RaftMsgServer) decodeExpectedMessage(reader io.Reader, expectedSize int64, expectedMsg proto.Message) error {
	if expectedSize == 0 {
		return proto.Unmarshal(nil, expectedMsg)
	}
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
