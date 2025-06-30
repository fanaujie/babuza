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

type multiRaftPeerFactory struct {
	t *MultiRaftTransport
}

func (p *multiRaftPeerFactory) CreatePeer(peerRaftAddr string) peer.MultiRaftPeer {
	c, err := p.t.protocol.CreateClient(p.t.peerMgr)
	if err != nil {
		p.t.logger.Panicf("transport[Node=%d] failed to create client for peer address=%s err=%s",
			p.t.localNodeID, peerRaftAddr, err.Error())
	}
	return peer.NewMultiRaftPeer(p.t.clusterID, p.t.localNodeID, peerRaftAddr, peer.MultiRaftPeerConfig{
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
	raftProcessor    ibabuza.MultiRaftStoreHandler
	protocol         ibabuza.MultiRaftTransportProtocol
	server           ibabuza.TransportServer
	peerMgr          PeerManager[peer.MultiRaftPeer, ibabuza.MultiRaftStatusReporter]
	memoryLimiter    limiter.ResourceLimiter
	chunkRateLimiter limiter.RateLimiter
	breaker          breaker.Breaker
	logger           ibabuza.Logger
	batchMessageCh   chan<- babuzapb.BatchMessage
	snapMessageCh    chan<- babuzapb.SnapshotMessage
}

func NewMultiRaftTransport(clusterID uint64, peerManager PeerManager[peer.MultiRaftPeer, ibabuza.MultiRaftStatusReporter], memoryLimiter limiter.ResourceLimiter,
	chunkRateLimiter limiter.RateLimiter, breaker breaker.Breaker, protocol ibabuza.MultiRaftTransportProtocol,
	logger ibabuza.Logger, setOpts ...SetTransportOptions) *MultiRaftTransport {

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
	p, err := t.peerMgr.GetPeer(groupID, message.To)
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
	p, err := t.peerMgr.GetPeer(groupID, snapMsg.To)
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

func (t *MultiRaftTransport) SendHeartbeat(toAddress string, heartbeats []babuzapb.MultiRaftHeartbeatMessage, heartbeatResponse []babuzapb.MultiRaftHeartbeatMessage) {
	p, err := t.peerMgr.GetPeerByAddress(toAddress)
	if err != nil {
		t.logger.Warningf("transport[Node=%d] no peer found for address=%s err=%s",
			t.localNodeID, toAddress, err.Error())
		return
	}
	if len(heartbeats) > 0 {
		m := peer.GetMultiRaftHeartbeatMessage(t.options.HeartbeatBufferSize)
		m.GroupID = heartbeats[0].GroupID
		m.Message.To = heartbeats[0].ToPeerID
		for _, heartbeat := range heartbeats {
			m.HeartbeatMessages = append(m.HeartbeatMessages, heartbeat)
		}
		if err = p.SendRaftMessage(m); err != nil {
			t.logger.Warningf("transport[Node=%d] failed to send heartbeat message to address=%s err=%s",
				t.localNodeID, toAddress, err.Error())
		}
	}
	if len(heartbeatResponse) > 0 {
		m := peer.GetMultiRaftHeartbeatResponseMessage(t.options.HeartbeatBufferSize)
		m.GroupID = heartbeatResponse[0].GroupID
		m.Message.To = heartbeatResponse[0].ToPeerID
		for _, heartbeat := range heartbeatResponse {
			m.HeartbeatResponseMessages = append(m.HeartbeatResponseMessages, heartbeat)
		}
		if err = p.SendRaftMessage(m); err != nil {
			t.logger.Warningf("transport[Node=%d] failed to send heartbeat response message to address=%s err=%s",
				t.localNodeID, toAddress, err.Error())
		}
	}
}

func (t *MultiRaftTransport) SetupTransportConfig(cfg ibabuza.TransportConfig) error {
	t.localNodeID = cfg.LocalNodeID
	return t.protocol.Setup(cfg)
}

func (t *MultiRaftTransport) SetupTransportRaft(processor ibabuza.MultiRaftStoreHandler) error {
	t.raftProcessor = processor
	t.peerMgr.UpdatePeerRaftReport(processor)
	s, err := t.protocol.CreateServer(processor)
	if err != nil {
		return err
	}
	t.server = s
	return nil
}

func (t *MultiRaftTransport) CreateTransportClient() (ibabuza.MultiRaftTransportClient, error) {
	return t.protocol.CreateClient(t.peerMgr)
}

func (t *MultiRaftTransport) AddPeer(groupID ibabuza.RaftGroupID, peerID uint64, peerAddress string) {
	err := t.peerMgr.AddPeer(groupID, peerID, peerAddress, &multiRaftPeerFactory{t})
	if err != nil {
		t.logger.Warningf("transport[Node=%d] failed to add peerID=%d err=%s", t.localNodeID, peerID, err.Error())
	}
}

func (t *MultiRaftTransport) UpdatePeer(groupID ibabuza.RaftGroupID, peerID uint64, peerAddress string) {
	err := t.peerMgr.UpdatePeer(groupID, peerID, peerAddress, &multiRaftPeerFactory{t})
	if err != nil {
		t.logger.Warningf("transport[Node=%d] failed to update peerID=%d err=%s", t.localNodeID, peerID, err.Error())
	}
}

func (t *MultiRaftTransport) RemovePeer(groupID ibabuza.RaftGroupID, peerID uint64) {
	err := t.peerMgr.RemovePeer(groupID, peerID)
	if err != nil {
		t.logger.Warningf("transport[Node=%d] failed to remove peerID=%d", t.localNodeID, peerID)
	}
}

func (t *MultiRaftTransport) RemovePeers() {
	t.peerMgr.RemoveAllPeers()
}

func (t *MultiRaftTransport) ResolvePeerAddress(groupID ibabuza.RaftGroupID, peerID uint64) (string, error) {
	return t.peerMgr.ResolvePeerAddress(groupID, peerID)
}
