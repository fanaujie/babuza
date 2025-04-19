package transport

import (
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/transport/peer"
	"github.com/fanaujie/babuza/pkg/utility/breaker"
	"github.com/fanaujie/babuza/pkg/utility/limiter"
	"github.com/fanaujie/babuza/pkg/utility/multierror"
)

type multiRaftPeerFactory struct {
	t *MultiRaftTransport
}

func (p *multiRaftPeerFactory) CreatePeer(peerID uint64) peer.MultiRaftPeer {
	c, err := p.t.protocol.CreateClient(p.t.peerMgr)
	if err != nil {
		p.t.logger.Panicf("transport[local id=%d] failed to create client for peerID=%d err=%s",
			p.t.localPeerID, peerID, err.Error())
	}
	return peer.NewMultiRaftPeer(p.t.localPeerID, peerID, peer.MultiRaftPeerConfig{
		LimiterMaxBatchMessageSize: p.t.options.PeerLimiterMaxBatchMessageSize,
		SnapshotChunkSize:          p.t.options.PeerSnapshotChunkSize,
		RaftMsgQueueSize:           p.t.options.PeerQueueSize},
		p.t.raftProcessor, p.t.memoryLimiter, p.t.chunkRateLimiter, p.t.breaker, c, p.t.logger)
}

type MultiRaftTransport struct {
	clusterID        uint64
	localPeerID      uint64
	options          Options
	raftProcessor    ibabuza.MultiRaftNodeHandler
	protocol         ibabuza.TransportProtocol
	server           ibabuza.TransportServer
	peerMgr          MultiRaftPeerManager
	memoryLimiter    limiter.ResourceLimiter
	chunkRateLimiter limiter.RateLimiter
	breaker          breaker.Breaker
	logger           ibabuza.Logger
	batchMessageCh   chan<- babuzapb.BatchMessage
	snapMessageCh    chan<- babuzapb.SnapshotMessage
}

func NewMultiRaftTransport(clusterID uint64, peerManager MultiRaftPeerManager, memoryLimiter limiter.ResourceLimiter, chunkRateLimiter limiter.RateLimiter,
	breaker breaker.Breaker, protocol ibabuza.TransportProtocol, logger ibabuza.Logger, setOpts ...SetTransportOptions) *MultiRaftTransport {
	logger.Infof("transport: creating transport")

	opts := DefaultOptions()
	for _, setOpt := range setOpts {
		setOpt(&opts)
	}
	trans := &MultiRaftTransport{
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

func (t *MultiRaftTransport) Start() error {
	return t.server.Start()
}

func (t *MultiRaftTransport) Stop() error {
	me := multierror.MultiError{}
	me.Append(t.server.Stop())
	t.peerMgr.RemoveAllPeers()
	me.Append(t.protocol.Close())
	return me.Get()
}

func (t *MultiRaftTransport) Send(msg babuzapb.MultiRaftMessage) {
	p := t.peerMgr.GetPeer(msg.Message.To)
	if p == nil {
		t.logger.Warningf("transport[local id=%d] not found peerID=%d", t.localPeerID, msg.Message.To)
		return
	}
	if err := p.SendRaftMessage(msg); err != nil {
		t.logger.Warningf("transport[local id=%d] failed to send raft message to [id=%d] err=%s", t.localPeerID,
			msg.Message.To, err.Error())
	}
}

func (t *MultiRaftTransport) SendSnapshot(snapMsg babuzapb.MultiRaftMessage) {
	p := t.peerMgr.GetPeer(snapMsg.Message.To)
	if p == nil {
		t.logger.Warningf("transport[local id=%d] not found peerID=%d", t.localPeerID, snapMsg.Message.To)
		return
	}
	snapReader, err := t.raftProcessor.CreateSnapshotReader(ibabuza.RaftGroupID(snapMsg.GroupID),
		snapMsg.Message.Snapshot.Metadata.Index)
	if err != nil {
		t.logger.Panicf("transport[local id=%d] can not create snapshot reader (index=%d)",
			t.localPeerID, snapMsg.Message.Snapshot.Metadata.Index)
	}
	p.SendSnapshot(snapMsg, snapReader)
}

func (t *MultiRaftTransport) SetupTransportConfig(cfg ibabuza.TransportConfig) error {
	t.localPeerID = cfg.PeerId
	return t.protocol.Setup(cfg)
}

func (t *MultiRaftTransport) SetupTransportRaft(processor ibabuza.MultiRaftNodeHandler) error {
	t.raftProcessor = processor
	t.peerMgr.UpdatePeerRaftReport(processor)
	s, err := t.protocol.CreateServer(processor)
	if err != nil {
		return err
	}
	t.server = s
	return nil
}

func (t *MultiRaftTransport) CreateTransportClient() (ibabuza.TransportClient, error) {
	return t.protocol.CreateClient(t.peerMgr)
}

func (t *MultiRaftTransport) AddPeer(peerID uint64, peerAddress string) {
	err := t.peerMgr.AddPeer(peerID, peerAddress, &multiRaftPeerFactory{t})
	if err != nil {
		t.logger.Warningf("transport[local id=%d] failed to add peerID=%d err=%s", t.localPeerID, peerID, err.Error())
	}
}

func (t *MultiRaftTransport) UpdatePeer(peerID uint64, peerAddress string) {
	err := t.peerMgr.UpdatePeer(peerID, peerAddress)
	if err != nil {
		t.logger.Warningf("transport[local id=%d] failed to update peerID=%d err=%s", t.localPeerID, peerID, err.Error())
	}
}

func (t *MultiRaftTransport) RemovePeer(peerID uint64) {
	err := t.peerMgr.RemovePeer(peerID)
	if err != nil {
		t.logger.Warningf("transport[local id=%d] failed to remove peerID=%d", t.localPeerID, peerID)
	}
}

func (t *MultiRaftTransport) RemovePeers() {
	t.peerMgr.RemoveAllPeers()
}
