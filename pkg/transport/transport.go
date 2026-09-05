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

package transport

import (
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/transport/peer"
	"github.com/fanaujie/babuza/pkg/utility/breaker"
	"github.com/fanaujie/babuza/pkg/utility/limiter"
	"github.com/fanaujie/babuza/pkg/utility/multierror"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

type transportResolverAdaptor struct {
	peerManager PeerManager[peer.Peer, ibabuza.RaftStatusReporter]
}

func (t *transportResolverAdaptor) ResolvePeerAddress(peerID uint64) (string, error) {
	return t.peerManager.ResolvePeerAddress(0, peerID)
}

type peerFactory struct {
	t      *Transport
	peerID uint64
}

func (p *peerFactory) CreatePeer(peerRaftAddr string) peer.Peer {
	c, err := p.t.protocol.CreateClient(&transportResolverAdaptor{p.t.peerMgr})
	if err != nil {
		p.t.logger.Panicf("transport[local id=%d] failed to create client for peer address=%s err=%s",
			p.t.localNodeID, peerRaftAddr, err.Error())
	}
	if starter, ok := c.(interface{ StartMessageStream(uint64) }); ok {
		starter.StartMessageStream(p.peerID)
	}
	return peer.New(p.t.clusterID, p.t.localNodeID, peerRaftAddr, peer.RaftPeerConfig{
		LimiterMaxBatchMessageSize: p.t.options.PeerLimiterMaxBatchMessageSize,
		SnapshotChunkSize:          p.t.options.PeerSnapshotChunkSize,
		RaftMsgQueueSize:           p.t.options.PeerQueueSize,
		DialTimeout:                p.t.options.DialTimeout},
		p.t.raftProcessor, p.t.memoryLimiter, p.t.chunkRateLimiter, p.t.breaker, c, p.t.logger)
}

type Transport struct {
	clusterID        uint64
	localNodeID      uint64
	options          Options
	raftProcessor    ibabuza.RaftNodeHandler
	protocol         ibabuza.TransportProtocol
	server           ibabuza.TransportServer
	peerMgr          PeerManager[peer.Peer, ibabuza.RaftStatusReporter]
	memoryLimiter    limiter.ResourceLimiter
	chunkRateLimiter limiter.RateLimiter
	breaker          breaker.Breaker
	logger           ibabuza.Logger
	batchMessageCh   chan<- babuzapb.BatchMessage
	snapMessageCh    chan<- babuzapb.SnapshotMessage
}

func New(clusterID uint64, peerManager PeerManager[peer.Peer, ibabuza.RaftStatusReporter], memoryLimiter limiter.ResourceLimiter, chunkRateLimiter limiter.RateLimiter,
	breaker breaker.Breaker, protocol ibabuza.TransportProtocol, logger ibabuza.Logger, setOpts ...SetTransportOptions) *Transport {
	logger.Infof("transport: creating transport")

	opts := DefaultOptions()
	for _, setOpt := range setOpts {
		setOpt(&opts)
	}
	trans := &Transport{
		clusterID:        clusterID,
		options:          opts,
		protocol:         protocol,
		peerMgr:          peerManager,
		memoryLimiter:    memoryLimiter,
		chunkRateLimiter: chunkRateLimiter,
		breaker:          breaker,
		logger:           logger,
	}
	return trans
}

func (t *Transport) Start() error {
	return t.server.Start()
}

func (t *Transport) Stop() error {
	me := multierror.MultiError{}
	me.Append(t.server.Stop())
	t.peerMgr.RemoveAllPeers()
	me.Append(t.protocol.Close())
	return me.Get()
}

func (t *Transport) Send(msg raftpb.Message) {
	p, err := t.peerMgr.GetPeer(0, msg.To)
	if err != nil {
		t.logger.Warningf("transport[local id=%d] failed to get peerID=%d err=%s", t.localNodeID, msg.To, err.Error())
		return
	}
	if err := p.SendRaftMessage(msg); err != nil {
		t.logger.Warningf("transport[local id=%d] failed to send raft message to [id=%d] err=%s", t.localNodeID, msg.To, err.Error())
	}
}
func (t *Transport) SendSnapshot(snapMsg raftpb.Message) {
	p, err := t.peerMgr.GetPeer(0, snapMsg.To)
	if err != nil {
		t.logger.Warningf("transport[local id=%d] failed to get peerID=%d err=%s", t.localNodeID, snapMsg.To, err.Error())
		return
	}
	snapReader, err := t.raftProcessor.CreateSnapshotReader(snapMsg.Snapshot.Metadata.Index)
	if err != nil {
		t.logger.Panicf("transport[local id=%d] can not create snapshot reader (index=%d)", t.localNodeID, snapMsg.Snapshot.Metadata.Index)
	}
	p.SendSnapshot(snapMsg, snapReader)
}

func (t *Transport) SetupTransportConfig(cfg ibabuza.TransportConfig) error {
	t.localNodeID = cfg.LocalNodeID
	return t.protocol.Setup(cfg)
}

func (t *Transport) SetupTransportRaft(processor ibabuza.RaftNodeHandler) error {
	t.raftProcessor = processor
	t.peerMgr.UpdatePeerRaftReport(processor)
	s, err := t.protocol.CreateServer(processor)
	if err != nil {
		return err
	}
	t.server = s
	return nil
}

func (t *Transport) CreateTransportClient() (ibabuza.TransportClient, error) {
	return t.protocol.CreateClient(&transportResolverAdaptor{t.peerMgr})
}

func (t *Transport) AddPeer(peerID uint64, peerAddress string) {
	err := t.peerMgr.AddPeer(0, peerID, peerAddress, &peerFactory{t: t, peerID: peerID})
	if err != nil {
		t.logger.Warningf("transport[local id=%d] failed to add peerID=%d err=%s", t.localNodeID, peerID, err.Error())
	}
}

func (t *Transport) UpdatePeer(peerID uint64, peerAddress string) {
	err := t.peerMgr.UpdatePeer(0, peerID, peerAddress, &peerFactory{t: t, peerID: peerID})
	if err != nil {
		t.logger.Warningf("transport[local id=%d] failed to update peerID=%d err=%s", t.localNodeID, peerID, err.Error())
	}
}

func (t *Transport) RemovePeer(peerID uint64) {
	err := t.peerMgr.RemovePeer(0, peerID)
	if err != nil {
		t.logger.Warningf("transport[local id=%d] failed to remove peerID=%d", t.localNodeID, peerID)
	}
}

func (t *Transport) RemovePeers() {
	t.peerMgr.RemoveAllPeers()
}
