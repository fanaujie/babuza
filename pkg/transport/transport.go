package transport

import (
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/transport/peer"
	"github.com/fanaujie/babuza/pkg/utility/breaker"
	"github.com/fanaujie/babuza/pkg/utility/limiter"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

type peerFactory struct {
	t *Transport
}

func (p *peerFactory) CreatePeer(peerId uint64) peer.Peer {
	return peer.New(peerId, peer.RaftPeerConfig{
		LimiterMaxBatchMessageSize: p.t.options.PeerLimiterMaxBatchMessageSize,
		SnapshotChunkSize:          p.t.options.PeerSnapshotChunkSize,
		RaftMsgQueueSize:           p.t.options.PeerQueueSize,
		DialTimeout:                p.t.options.DialTimeout},
		p.t.raftProcessor, p.t.memoryLimiter, p.t.chunkRateLimiter, p.t.breaker, p.t, p.t.logger)
}

type Transport struct {
	localPeerId      uint64
	options          Options
	raftProcessor    ibabuza.RaftNodeHandler
	protocol         ibabuza.TransportProtocol
	server           ibabuza.TransportServer
	peerMgr          PeerManager
	memoryLimiter    limiter.ResourceLimiter
	chunkRateLimiter limiter.RateLimiter
	breaker          breaker.Breaker
	logger           ibabuza.Logger
	batchMessageCh   chan<- babuzapb.BatchMessage
	snapMessageCh    chan<- babuzapb.SnapshotMessage
}

func New(opts Options, peerManager PeerManager, memoryLimiter limiter.ResourceLimiter, chunkRateLimiter limiter.RateLimiter,
	breaker breaker.Breaker, protocol ibabuza.TransportProtocol, logger ibabuza.Logger) *Transport {
	logger.Infof("transport: creating transport")
	trans := &Transport{
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
	if err := t.server.Stop(); err != nil {
		return err
	}
	t.peerMgr.RemoveAllPeers()
	return t.protocol.Close()
}

func (t *Transport) Send(msg raftpb.Message) {
	p := t.peerMgr.GetPeer(msg.To)
	if p == nil {
		t.logger.Warningf("transport[local id=%d] not found peerId=%d", t.localPeerId, msg.To)
		return
	}
	if err := p.SendRaftMessage(&msg); err != nil {
		t.logger.Warningf("transport[local id=%d] failed to send raft message to [id=%d] err=%s", t.localPeerId, msg.To, err.Error())
	}
}
func (t *Transport) SendSnapshot(snapMsg raftpb.Message) {
	p := t.peerMgr.GetPeer(snapMsg.To)
	if p == nil {
		t.logger.Warningf("transport[local id=%d] not found peerId=%d", t.localPeerId, snapMsg.To)
		return
	}
	snapReader, err := t.raftProcessor.CreateSnapshotReader(snapMsg.Snapshot.Metadata.Index)
	if err != nil {
		t.logger.Panicf("transport[local id=%d] can not create snapshot reader (index=%d)", t.localPeerId, snapMsg.Snapshot.Metadata.Index)
	}
	p.SendSnapshot(&snapMsg, snapReader)
}

func (t *Transport) SetupTransportConfig(cfg ibabuza.TransportConfig) error {
	t.localPeerId = cfg.PeerId
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
	return t.protocol.CreateClient(t.peerMgr)
}

func (t *Transport) AddPeer(peerId uint64, peerAddress string) {
	err := t.peerMgr.AddPeer(peerId, peerAddress, &peerFactory{t})
	if err != nil {
		t.logger.Warningf("transport[local id=%d] failed to add peerId=%d err=%s", t.localPeerId, peerId, err.Error())
	}
}

func (t *Transport) UpdatePeer(peerId uint64, peerAddress string) {
	err := t.peerMgr.UpdatePeer(peerId, peerAddress)
	if err != nil {
		t.logger.Warningf("transport[local id=%d] failed to update peerId=%d err=%s", t.localPeerId, peerId, err.Error())
	}
}

func (t *Transport) RemovePeer(peerId uint64) {
	err := t.peerMgr.RemovePeer(peerId)
	if err != nil {
		t.logger.Warningf("transport[local id=%d] failed to remove peerId=%d", t.localPeerId, peerId)
	}
}

func (t *Transport) RemovePeers() {
	t.peerMgr.RemoveAllPeers()
}
