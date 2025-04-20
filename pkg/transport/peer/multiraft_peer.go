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

type MultiRaftPeerConfig struct {
	LimiterMaxBatchMessageSize int64
	SnapshotChunkSize          int64
	RaftMsgQueueSize           int64
}

type MultiRaftPeerImpl struct {
	localPeerID                uint64
	peerID                     uint64
	limiterMaxBatchMessageSize int64
	snapshotChunkSize          int64
	raftQueueSize              int64
	raftReport                 ibabuza.RaftStatusReporter
	memoryLimiter              limiter.ResourceLimiter
	chunkRateLimiter           limiter.RateLimiter
	breaker                    breaker.Breaker
	transportClient            ibabuza.TransportClient
	logger                     ibabuza.Logger
	currentMsgCh               chan babuzapb.MultiRaftMessage
	msgQueueCh                 chan chan babuzapb.MultiRaftMessage
	closer                     *syncutil.Closer
	mu                         sync.RWMutex
}

func NewMultiRaftPeer(localPeerID, peerID uint64, cfg MultiRaftPeerConfig, raftReport ibabuza.RaftStatusReporter,
	memoryLimiter limiter.ResourceLimiter, chunkRateLimiter limiter.RateLimiter, breaker breaker.Breaker,
	transportClient ibabuza.TransportClient, logger ibabuza.Logger) *MultiRaftPeerImpl {
	closer := syncutil.NewCloser()
	r := &MultiRaftPeerImpl{
		localPeerID:                localPeerID,
		peerID:                     peerID,
		limiterMaxBatchMessageSize: cfg.LimiterMaxBatchMessageSize,
		snapshotChunkSize:          cfg.SnapshotChunkSize,
		raftQueueSize:              cfg.RaftMsgQueueSize,
		raftReport:                 raftReport,
		memoryLimiter:              memoryLimiter,
		chunkRateLimiter:           chunkRateLimiter,
		breaker:                    breaker,
		transportClient:            transportClient,
		logger:                     logger,
		msgQueueCh:                 make(chan chan babuzapb.MultiRaftMessage, 1),
		closer:                     closer,
	}
	closer.Run(func() {
		r.processRaftMessage()
	})
	return r
}

func (p *MultiRaftPeerImpl) SendRaftMessage(msg babuzapb.MultiRaftMessage) error {
	if !p.breaker.Ready() {
		p.raftReport.ReportUnreachable(msg.Message.To)
		return ErrMultiRaftPeerBreakerNotReady
	}

	acquiredSize := int64(msg.Size())
	if !p.memoryLimiter.Allow(acquiredSize) {
		p.raftReport.ReportUnreachable(msg.Message.To)
		return ErrMultiRaftPeerReachMaxTotalSendMsgSize
	}
	msgCh, err := p.getQueue()
	if err != nil {
		return err
	}
	select {
	case <-p.closer.CloseCh():
		p.raftReport.ReportUnreachable(msg.Message.To)
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
				p.raftReport.ReportUnreachable(p.peerID)
				p.raftReport.ReportSnapshot(p.peerID, raft.SnapshotFailure)
			}
			p.logger.Errorf("Local peer[%d] send snapshot message to peerID=%d error: %v", p.localPeerID, p.peerID, err)
			return
		}
		p.raftReport.ReportSnapshot(p.peerID, raft.SnapshotFinish)
	})
}

func (p *MultiRaftPeerImpl) UpdateRaftReport(report ibabuza.RaftStatusReporter) {
	p.raftReport = report
}

func (p *MultiRaftPeerImpl) Stop() {
	_ = p.transportClient.Close()
	p.closer.Close()
}

func (p *MultiRaftPeerImpl) getQueue() (chan babuzapb.MultiRaftMessage, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	msgCh := p.currentMsgCh
	if msgCh == nil {
		p.memoryLimiter.Reset()
		msgCh = make(chan babuzapb.MultiRaftMessage, p.raftQueueSize)
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
				if !errors.Is(err, ErrMultiRaftPeerStopped) {
					p.breaker.Fail()
					p.raftReport.ReportUnreachable(p.peerID)
				}
				p.logger.Warningf("Local peer[%d] send multi-raft message to peerID=%d error: %v", p.localPeerID, p.peerID, err)
			}
			p.mu.Lock()
			p.currentMsgCh = nil
			p.mu.Unlock()
		}
	}
}

func (p *MultiRaftPeerImpl) sendRaftMessageLoop(msgCh chan babuzapb.MultiRaftMessage) error {
	var nextBatch bool
	var maxBatchSize int64
	msgBuf := make([]babuzapb.MultiRaftMessage, 0, cap(msgCh))
	groupMsgBuf := make([]babuzapb.BatchMessage, 0, cap(msgCh))

	for {
		select {
		case <-p.closer.CloseCh():
			return ErrMultiRaftPeerStopped
		case msg := <-msgCh:
			msgBuf = append(msgBuf, msg)
			mSize := int64(msg.Size())
			maxBatchSize += mSize
			p.memoryLimiter.Release(mSize)
			for done := false; !done; {
				select {
				case <-p.closer.CloseCh():
					return ErrMultiRaftPeerStopped
				case msg = <-msgCh:
					msgBuf = append(msgBuf, msg)
					mSize = int64(msg.Size())
					p.memoryLimiter.Release(mSize)
					if maxBatchSize+mSize > p.limiterMaxBatchMessageSize {
						nextBatch = true
						done = true
					} else {
						maxBatchSize += mSize
					}
				default:
					done = true
				}
			}
			var firstBatch, secondBatch []babuzapb.MultiRaftMessage
			if nextBatch {
				firstBatch = msgBuf[:len(msgBuf)-1]
				secondBatch = msgBuf[len(msgBuf)-1:]
			} else {
				firstBatch = msgBuf
			}
			if len(firstBatch) > 0 {
				groupMsgBuf = p.groupMessagesByGroupID(firstBatch, groupMsgBuf)
				for _, batchMsg := range groupMsgBuf {
					if err := p.transportClient.SendBatchMessage(batchMsg); err != nil {
						return err
					}
					p.breaker.Success()
				}
				groupMsgBuf = releaseBatchMessageBuffers(groupMsgBuf)
				msgBuf = releaseMultiRaftMessageBuffers(firstBatch)
			}
			if len(secondBatch) > 0 {
				groupMsgBuf = p.groupMessagesByGroupID(secondBatch, groupMsgBuf)
				for _, batchMsg := range groupMsgBuf {
					if err := p.transportClient.SendBatchMessage(batchMsg); err != nil {
						return err
					}
					p.breaker.Success()
				}
				groupMsgBuf = releaseBatchMessageBuffers(groupMsgBuf)
				msgBuf = releaseMultiRaftMessageBuffers(secondBatch)
			}
			maxBatchSize = 0
			nextBatch = false
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
			if sErr := p.transportClient.SendSnapshotMessage(msg); sErr != nil {
				return sErr
			}
		}
		return nil
	}

	m := babuzapb.SnapshotMessage{
		ClusterID: snapMsg.GroupID,
		From:      snapMsg.Message.From,
		To:        snapMsg.Message.To,
		Term:      snapMsg.Message.Snapshot.Metadata.Term,
		Index:     snapMsg.Message.Snapshot.Metadata.Index,
	}
	crcTable := crc32.MakeTable(crc32.Castagnoli)

	mt := snapFileReader.Metadata()
	m.Metadata = mt
	if err := sendSnapshotMsg(m); err != nil {
		return err
	}
	p.breaker.Success()

	m.Metadata = babuzapb.SnapshotMetadata{}
	chunkBuf := make([]byte, p.snapshotChunkSize)
	if err := snapFileReader.ForEachFile(func(reader io.Reader, metadata babuzapb.SnapshotFileDesc) error {
		m.ChunkMessage.FileTag = metadata.Tag
		m.ChunkMessage.FileType = metadata.FileType
		return p.sendSnapshotMsgWithChunk(reader, metadata.FileSize, m, chunkBuf, crcTable)
	}); err != nil {
		return err
	}

	// 发送完成消息
	m.ChunkMessage = babuzapb.SnapshotChunkMessage{}
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
				if err = p.transportClient.SendSnapshotMessage(msg); err != nil {
					return err
				}
				p.breaker.Success()
			}
		}
	}
}

func (p *MultiRaftPeerImpl) groupMessagesByGroupID(messages []babuzapb.MultiRaftMessage, outputResult []babuzapb.BatchMessage) []babuzapb.BatchMessage {
	groupMap := make(map[uint64][]raftpb.Message)
	for _, msg := range messages {
		groupID := msg.GroupID
		groupMap[groupID] = append(groupMap[groupID], msg.Message)
	}

	for groupID, msgs := range groupMap {
		outputResult = append(outputResult, babuzapb.BatchMessage{
			ClusterID: groupID,
			Messages:  msgs,
		})
	}
	return outputResult
}
