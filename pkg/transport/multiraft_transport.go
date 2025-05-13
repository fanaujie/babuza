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

type multiRaftPeerFactory struct {
	t *MultiRaftTransport
}

func (p *multiRaftPeerFactory) CreatePeer(peerID uint64) peer.MultiRaftPeer {
	c, err := p.t.protocol.CreateClient(p.t.peerMgr)
	if err != nil {
		p.t.logger.Panicf("transport[Node=%d] failed to create client for peerID=%d err=%s",
			p.t.localNodeID, peerID, err.Error())
	}
	return peer.NewMultiRaftPeer(p.t.clusterID, p.t.localNodeID, peerID, peer.MultiRaftPeerConfig{
		LimiterMaxBatchMessageSize: p.t.options.PeerLimiterMaxBatchMessageSize,
		SnapshotChunkSize:          p.t.options.PeerSnapshotChunkSize,
		RaftMsgQueueSize:           p.t.options.PeerQueueSize,
		HeartbeatBufferSize:        p.t.options.HeartbeatBufferSize},
		p.t.raftProcessor, p.t.memoryLimiter, p.t.chunkRateLimiter, p.t.breaker, c, p.t.logger)
}

type MultiRaftTransport struct {
	clusterID        uint64
	localNodeID      uint64
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

func (t *MultiRaftTransport) Send(groupID ibabuza.RaftGroupID, message raftpb.Message) {
	p, err := t.peerMgr.GetPeer(message.To)
	if err != nil {
		t.logger.Warningf("transport[Node=%d] %s", t.localNodeID, err)
		return
	}
	m := peer.GetMultiRaftMessage()
	m.GroupID = uint64(groupID)
	m.Message = message
	if err = p.SendRaftMessage(m); err != nil {
		t.logger.Warningf("transport[Node=%d] failed to send raft message to [id=%d] err=%s", t.localNodeID,
			message.To, err.Error())
	}
}

func (t *MultiRaftTransport) SendSnapshot(groupID ibabuza.RaftGroupID, snapMsg raftpb.Message) {
	p, err := t.peerMgr.GetPeer(snapMsg.To)
	if err != nil {
		t.logger.Warningf("transport[Node=%d] %s", t.localNodeID, err)
		return
	}
	snapReader, err := t.raftProcessor.CreateSnapshotReader(groupID, snapMsg.Snapshot.Metadata.Index)
	if err != nil {
		t.logger.Panicf("transport[Node=%d] can not create snapshot reader (index=%d)",
			t.localNodeID, snapMsg.Snapshot.Metadata.Index)
	}
	p.SendSnapshot(babuzapb.MultiRaftMessage{
		GroupID: uint64(groupID),
		Message: snapMsg,
	}, snapReader)
}

func (t *MultiRaftTransport) SendHeartbeat(to uint64, heartbeats []babuzapb.MultiRaftHeartbeatMessage, heartbeatResponse []babuzapb.MultiRaftHeartbeatMessage) {
	p, err := t.peerMgr.GetPeer(to)
	if err != nil {
		t.logger.Warningf("transport[Node=%d] %s", t.localNodeID, err)
		return
	}
	if len(heartbeats) > 0 {
		for _, heartbeat := range heartbeats {
			m := peer.GetMultiRaftHeartbeatMessage(1024)
			// Setting GroupID to 0 as it's not needed at container level;
			// each individual HeartbeatMessage already contains its own GroupID
			m.GroupID = 0
			m.Message.From = t.localNodeID
			m.Message.To = to
			m.HeartbeatMessages = append(m.HeartbeatMessages, heartbeat)
			if err = p.SendRaftMessage(m); err != nil {
				t.logger.Warningf("transport[Node=%d] failed to send heartbeat message to [id=%d] err=%s",
					t.localNodeID, m.Message.To, err.Error())
			}
		}
	}
	if len(heartbeatResponse) > 0 {
		for _, heartbeat := range heartbeatResponse {
			m := peer.GetMultiRaftHeartbeatResponseMessage(1024)
			// Setting GroupID to 0 as it's not needed at container level;
			// each individual HeartbeatMessageResponse already contains its own GroupID
			m.GroupID = 0
			m.Message.From = t.localNodeID
			m.Message.To = to
			m.HeartbeatResponseMessages = append(m.HeartbeatResponseMessages, heartbeat)
			if err = p.SendRaftMessage(m); err != nil {
				t.logger.Warningf("transport[Node=%d] failed to send heartbeat response message to [id=%d] err=%s",
					t.localNodeID, m.Message.To, err.Error())
			}
		}
	}
}

func (t *MultiRaftTransport) SetupTransportConfig(cfg ibabuza.TransportConfig) error {
	t.localNodeID = cfg.PeerId
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
		t.logger.Warningf("transport[Node=%d] failed to add peerID=%d err=%s", t.localNodeID, peerID, err.Error())
	}
}

func (t *MultiRaftTransport) UpdatePeer(peerID uint64, peerAddress string) {
	err := t.peerMgr.UpdatePeer(peerID, peerAddress)
	if err != nil {
		t.logger.Warningf("transport[Node=%d] failed to update peerID=%d err=%s", t.localNodeID, peerID, err.Error())
	}
}

func (t *MultiRaftTransport) RemovePeer(peerID uint64) {
	err := t.peerMgr.RemovePeer(peerID)
	if err != nil {
		t.logger.Warningf("transport[Node=%d] failed to remove peerID=%d", t.localNodeID, peerID)
	}
}

func (t *MultiRaftTransport) RemovePeers() {
	t.peerMgr.RemoveAllPeers()
}
