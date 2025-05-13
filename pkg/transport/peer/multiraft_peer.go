package peer

import (
	"context"
	"errors"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/utility/breaker"
	"github.com/fanaujie/babuza/pkg/utility/limiter"
	"github.com/fanaujie/babuza/pkg/utility/syncutil"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"hash/crc32"
	"io"
	"sync"
)

var (
	ErrMultiRaftPeerStopped                  = errors.New("MultiRaftPeer: ErrMultiRaftPeerStopped")
	ErrMultiRaftPeerBreakerNotReady          = errors.New("MultiRaftPeer: ErrMultiRaftPeerBreakerNotReady")
	ErrMultiRaftPeerReachMaxTotalSendMsgSize = errors.New("MultiRaftPeer: ErrMultiRaftPeerReachMaxTotalSendMsgSize")
	ErrMultiRaftPeerQueueFull                = errors.New("MultiRaftPeer: ErrMultiRaftPeerQueueFull")
	ErrMultiRaftPeerCorruptedSnapshotFile    = errors.New("MultiRaftPeer: ErrMultiRaftPeerCorruptedSnapshotFile")
)

var (
	multiRaftMessagePool                  sync.Pool
	multiRaftHeartbeatMessagePool         sync.Pool
	multiRaftHeartbeatResponseMessagePool sync.Pool
)

func GetMultiRaftMessage() *babuzapb.MultiRaftMessage {
	m := multiRaftMessagePool.Get()
	if m == nil {
		return &babuzapb.MultiRaftMessage{}
	}
	return m.(*babuzapb.MultiRaftMessage)
}

func ReleaseMultiRaftMessage(msg *babuzapb.MultiRaftMessage) {
	if msg == nil {
		return
	}
	*msg = babuzapb.MultiRaftMessage{}
	multiRaftMessagePool.Put(msg)
}

func GetMultiRaftHeartbeatMessage(defaultBufferSize int) *babuzapb.MultiRaftMessage {
	m := multiRaftHeartbeatMessagePool.Get()
	if m == nil {
		return &babuzapb.MultiRaftMessage{
			Message: raftpb.Message{
				Type: raftpb.MsgHeartbeat,
			},
			HeartbeatMessages: make([]babuzapb.MultiRaftHeartbeatMessage, 0, defaultBufferSize),
		}
	}
	return m.(*babuzapb.MultiRaftMessage)
}

func ReleaseMultiRaftHeartbeatMessage(msg *babuzapb.MultiRaftMessage) {
	if msg == nil {
		return
	}
	msg.HeartbeatMessages = msg.HeartbeatMessages[:0]
	multiRaftHeartbeatMessagePool.Put(msg)
}

func GetMultiRaftHeartbeatResponseMessage(defaultBufferSize int) *babuzapb.MultiRaftMessage {
	m := multiRaftHeartbeatResponseMessagePool.Get()
	if m == nil {
		return &babuzapb.MultiRaftMessage{
			Message: raftpb.Message{
				Type: raftpb.MsgHeartbeatResp,
			},
			HeartbeatResponseMessages: make([]babuzapb.MultiRaftHeartbeatMessage, 0, defaultBufferSize),
		}
	}
	return m.(*babuzapb.MultiRaftMessage)
}

func ReleaseMultiRaftHeartbeatResponseMessage(msg *babuzapb.MultiRaftMessage) {
	if msg == nil {
		return
	}
	msg.HeartbeatResponseMessages = msg.HeartbeatResponseMessages[:0]
	multiRaftHeartbeatResponseMessagePool.Put(msg)
}

type MultiRaftPeerConfig struct {
	LimiterMaxBatchMessageSize int64
	SnapshotChunkSize          int64
	RaftMsgQueueSize           int64
	HeartbeatBufferSize        int
}

type MultiRaftPeerImpl struct {
	clusterID                  uint64
	localNodeID                uint64
	remotePeerID               uint64
	limiterMaxBatchMessageSize int64
	snapshotChunkSize          int64
	raftQueueSize              int64
	raftReport                 ibabuza.MultiRaftStatusReporter
	memoryLimiter              limiter.ResourceLimiter
	chunkRateLimiter           limiter.RateLimiter
	breaker                    breaker.Breaker
	transportClient            ibabuza.TransportClient
	logger                     ibabuza.Logger
	currentMsgCh               chan *babuzapb.MultiRaftMessage
	msgQueueCh                 chan chan *babuzapb.MultiRaftMessage
	msgQueueChPool             chan chan *babuzapb.MultiRaftMessage
	closer                     *syncutil.Closer
	mu                         sync.RWMutex
}

func NewMultiRaftPeer(clusterID, localNodeID, remotePeerID uint64, cfg MultiRaftPeerConfig, raftReport ibabuza.MultiRaftStatusReporter,
	memoryLimiter limiter.ResourceLimiter, chunkRateLimiter limiter.RateLimiter, breaker breaker.Breaker,
	transportClient ibabuza.TransportClient, logger ibabuza.Logger) *MultiRaftPeerImpl {
	closer := syncutil.NewCloser()
	r := &MultiRaftPeerImpl{
		clusterID:                  clusterID,
		localNodeID:                localNodeID,
		remotePeerID:               remotePeerID,
		limiterMaxBatchMessageSize: cfg.LimiterMaxBatchMessageSize,
		snapshotChunkSize:          cfg.SnapshotChunkSize,
		raftQueueSize:              cfg.RaftMsgQueueSize,
		raftReport:                 raftReport,
		memoryLimiter:              memoryLimiter,
		chunkRateLimiter:           chunkRateLimiter,
		breaker:                    breaker,
		transportClient:            transportClient,
		logger:                     logger,
		msgQueueCh:                 make(chan chan *babuzapb.MultiRaftMessage, 1),
		msgQueueChPool:             make(chan chan *babuzapb.MultiRaftMessage, cfg.RaftMsgQueueSize),
		closer:                     closer,
	}
	closer.Run(func() {
		r.processRaftMessage()
	})
	return r
}

func (p *MultiRaftPeerImpl) SendRaftMessage(msg *babuzapb.MultiRaftMessage) error {
	if !p.breaker.Ready() {
		p.reportUnreachable(msg)
		return ErrMultiRaftPeerBreakerNotReady
	}

	acquiredSize := int64(msg.Size())
	if !p.memoryLimiter.Allow(acquiredSize) {
		p.reportUnreachable(msg)
		return ErrMultiRaftPeerReachMaxTotalSendMsgSize
	}
	msgCh, err := p.getQueue()
	if err != nil {
		return err
	}
	select {
	case <-p.closer.CloseCh():
		return ErrMultiRaftPeerStopped
	case msgCh <- msg:
		p.memoryLimiter.Acquire(acquiredSize)
		return nil
	default:
		return ErrMultiRaftPeerQueueFull
	}
}

func (p *MultiRaftPeerImpl) SendSnapshot(snapMsg babuzapb.MultiRaftMessage, snapReader SnapshotFileReader) {
	p.closer.Run(func() {
		if err := p.sendSnapshotMessageLoop(snapMsg, snapReader); err != nil {
			if !errors.Is(err, ErrMultiRaftPeerStopped) {
				p.breaker.Fail()
				p.raftReport.ReportUnreachable(ibabuza.RaftGroupID(snapMsg.GroupID), p.remotePeerID)
				p.raftReport.ReportSnapshot(ibabuza.RaftGroupID(snapMsg.GroupID), p.remotePeerID, raft.SnapshotFailure)
			}
			p.logger.Errorf("Node[%d] send snapshot message to peerID=%d error: %v", p.localNodeID, p.remotePeerID, err)
			return
		}
		p.raftReport.ReportSnapshot(ibabuza.RaftGroupID(snapMsg.GroupID), p.remotePeerID, raft.SnapshotFinish)
	})
}

func (p *MultiRaftPeerImpl) UpdateRaftReport(report ibabuza.MultiRaftStatusReporter) {
	p.raftReport = report
}

func (p *MultiRaftPeerImpl) Stop() {
	_ = p.transportClient.Close()
	p.closer.Close()
}

func (p *MultiRaftPeerImpl) getQueue() (chan *babuzapb.MultiRaftMessage, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	msgCh := p.currentMsgCh
	if msgCh == nil {
		p.memoryLimiter.Reset()
		select {
		case <-p.closer.CloseCh():
			return nil, ErrPeerStopped
		case msgCh = <-p.msgQueueChPool:
		default:
			msgCh = make(chan *babuzapb.MultiRaftMessage, p.raftQueueSize)
		}
		p.currentMsgCh = msgCh
		select {
		case <-p.closer.CloseCh():
			return nil, ErrMultiRaftPeerStopped
		case p.msgQueueCh <- msgCh:
		}
	}
	return msgCh, nil
}

func (p *MultiRaftPeerImpl) processRaftMessage() {
	for {
		select {
		case <-p.closer.CloseCh():
			return
		case msgCh := <-p.msgQueueCh:
			if err := p.sendRaftMessageLoop(msgCh); err != nil {
				p.logger.Warningf("Node[%d] send multi-raft message to peerID=%d error: %v", p.localNodeID, p.remotePeerID, err)
			}
			p.mu.Lock()
			p.currentMsgCh = nil
			p.mu.Unlock()
			//drain the message channel
		DrainLoop:
			for {
				select {
				case <-msgCh:
				default:
					break DrainLoop
				}
			}
			// push the message channel to pool
			select {
			case p.msgQueueChPool <- msgCh:
			default:
				// if the pool is full, drop channel
				p.logger.Warningf("Node[%d] message channel pool is full, drop channel", p.localNodeID)
			}
		}
	}
}

func (p *MultiRaftPeerImpl) sendRaftMessageLoop(msgCh chan *babuzapb.MultiRaftMessage) error {
	msgBuf := make([]*babuzapb.MultiRaftMessage, 0, cap(msgCh))
	for {
		select {
		case <-p.closer.CloseCh():
			return ErrMultiRaftPeerStopped
		case msg := <-msgCh:
			msgBuf = append(msgBuf, msg)
			mSize := int64(msg.Size())
			remainSize := p.limiterMaxBatchMessageSize - mSize
			p.memoryLimiter.Release(mSize)
			for remainSize > 0 {
				select {
				case <-p.closer.CloseCh():
					return ErrMultiRaftPeerStopped
				case msg = <-msgCh:
					msgBuf = append(msgBuf, msg)
					mSize = int64(msg.Size())
					p.memoryLimiter.Release(mSize)
					remainSize -= mSize
				default:
					remainSize = -1
				}
			}
			if err := func() error {
				defer func() {
					msgBuf = releaseMultiRaftMessageBuffers(msgBuf)
				}()
				if err := p.transportClient.SendMultiRaftMessage(babuzapb.MultiRaftBatchMessage{
					ClusterID: p.clusterID,
					Messages:  msgBuf,
				}); err != nil {
					p.breaker.Fail()
					for _, m := range msgBuf {
						p.reportUnreachable(m)
					}
					return err
				}
				p.breaker.Success()
				return nil
			}(); err != nil {
				return err
			}

		}
	}
}

func (p *MultiRaftPeerImpl) sendSnapshotMessageLoop(snapMsg babuzapb.MultiRaftMessage,
	snapFileReader SnapshotFileReader) error {

	sendSnapshotMsg := func(msg babuzapb.SnapshotMessage) error {
		select {
		case <-p.closer.CloseCh():
			return ErrMultiRaftPeerStopped
		default:
			res, sErr := p.transportClient.SendSnapshotMessage(msg)
			if sErr != nil {
				return sErr
			}
			if res.Status != babuzapb.SUCCESS {
				return errors.New(res.Message)
			}
		}
		return nil
	}

	m := babuzapb.SnapshotMessage{
		ClusterID: p.clusterID,
		GroupID:   snapMsg.GroupID,
		From:      snapMsg.Message.From,
		To:        snapMsg.Message.To,
		Term:      snapMsg.Message.Snapshot.Metadata.Term,
		Index:     snapMsg.Message.Snapshot.Metadata.Index,
	}
	crcTable := crc32.MakeTable(crc32.Castagnoli)

	mt := snapFileReader.Metadata()
	m.Type = babuzapb.SnapshotMessageType_Metadata
	m.Metadata = mt
	if err := sendSnapshotMsg(m); err != nil {
		return err
	}
	p.breaker.Success()

	m.Metadata = babuzapb.SnapshotMetadata{}
	chunkBuf := make([]byte, p.snapshotChunkSize)
	if err := snapFileReader.ForEachFile(func(reader io.Reader, metadata babuzapb.SnapshotFileDesc) error {
		m.Type = babuzapb.SnapshotMessageType_Chunk
		m.ChunkMessage.FileTag = metadata.Tag
		m.ChunkMessage.FileType = metadata.FileType
		return p.sendSnapshotMsgWithChunk(reader, metadata.FileSize, m, chunkBuf, crcTable)
	}); err != nil {
		return err
	}

	m.ChunkMessage = babuzapb.SnapshotChunkMessage{}
	m.Type = babuzapb.SnapshotMessageType_Finish
	m.FinishMessage = snapMsg.Message
	if err := sendSnapshotMsg(m); err != nil {
		return err
	}
	p.breaker.Success()
	return nil
}

func (p *MultiRaftPeerImpl) sendSnapshotMsgWithChunk(reader io.Reader, fileSize int64,
	msg babuzapb.SnapshotMessage, chunkBuf []byte, crcTable *crc32.Table) error {

	var written int64
	for {
		nr, err := reader.Read(chunkBuf)
		if err != nil {
			if err == io.EOF {
				if fileSize != written {
					return ErrMultiRaftPeerCorruptedSnapshotFile
				}
				return nil
			}
			return err
		}
		if nr > 0 {
			msg.ChunkMessage.Id++
			msg.ChunkMessage.Data = chunkBuf[:nr]
			msg.ChunkMessage.LastChunk = fileSize == written+int64(nr)
			msg.ChunkMessage.ContinueCrc32 = crc32.Update(msg.ChunkMessage.ContinueCrc32, crcTable, msg.ChunkMessage.Data)
			written += int64(nr)
			select {
			case <-p.closer.CloseCh():
				return ErrMultiRaftPeerStopped
			default:
				if err = p.chunkRateLimiter.Wait(context.Background()); err != nil {
					return err
				}
				res, sErr := p.transportClient.SendSnapshotMessage(msg)
				if sErr != nil {
					return sErr
				}
				if res.Status != babuzapb.SUCCESS {
					return errors.New(res.Message)
				}
				p.breaker.Success()
			}
		}
	}
}

func (p *MultiRaftPeerImpl) reportUnreachable(msg *babuzapb.MultiRaftMessage) {
	switch msg.Message.Type {
	case raftpb.MsgHeartbeat, raftpb.MsgHeartbeatResp:
		for _, m := range msg.HeartbeatMessages {
			p.raftReport.ReportUnreachable(ibabuza.RaftGroupID(m.GroupID), msg.Message.To)
		}
		for _, m := range msg.HeartbeatResponseMessages {
			p.raftReport.ReportUnreachable(ibabuza.RaftGroupID(m.GroupID), msg.Message.To)
		}
	default:
		p.raftReport.ReportUnreachable(ibabuza.RaftGroupID(msg.GroupID), msg.Message.To)
	}
}
