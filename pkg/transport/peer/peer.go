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
	ErrPeerStopped                  = errors.New("RaftPeer: ErrPeerStopped")
	ErrPeerBreakerNotReady          = errors.New("RaftPeer: ErrPeerBreakerNotReady")
	ErrPeerReachMaxTotalSendMsgSize = errors.New("RaftPeer: ErrPeerReachMaxTotalSendMsgSize")
	ErrPeerQueueFull                = errors.New("RaftPeer: ErrPeerQueueFull")
	ErrPeerCorruptedSnapshotFile    = errors.New("RaftPeer: ErrPeerCorruptedSnapshotFile")
)

type RaftPeer struct {
	clusterID                  uint64
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
	currentMsgCh               chan raftpb.Message
	msgQueueCh                 chan chan raftpb.Message
	msgQueueChPool             chan chan raftpb.Message
	closer                     *syncutil.Closer
	mu                         sync.RWMutex
}

func New(clusterID, localPeerID, peerID uint64, cfg RaftPeerConfig, raftReport ibabuza.RaftStatusReporter,
	memoryLimiter limiter.ResourceLimiter, chunkRateLimiter limiter.RateLimiter, breaker breaker.Breaker,
	transportClient ibabuza.TransportClient, logger ibabuza.Logger) *RaftPeer {
	closer := syncutil.NewCloser()
	r := &RaftPeer{
		clusterID:                  clusterID,
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
		msgQueueCh:                 make(chan chan raftpb.Message, 1),
		msgQueueChPool:             make(chan chan raftpb.Message, cfg.QueuePoolSize),
		closer:                     closer,
	}
	closer.Run(func() {
		r.processRaftMessage()
	})
	return r
}

func (p *RaftPeer) SendRaftMessage(msg raftpb.Message) error {
	if !p.breaker.Ready() {
		p.raftReport.ReportUnreachable(msg.To)
		return ErrPeerBreakerNotReady
	}

	acquiredSize := int64(msg.Size())
	if !p.memoryLimiter.Allow(acquiredSize) {
		p.raftReport.ReportUnreachable(msg.To)
		return ErrPeerReachMaxTotalSendMsgSize
	}
	msgCh, err := p.getQueue()
	if err != nil {
		return err
	}
	select {
	case <-p.closer.CloseCh():
		p.raftReport.ReportUnreachable(msg.To)
		return ErrPeerStopped
	case msgCh <- msg:
		p.memoryLimiter.Acquire(acquiredSize)
		return nil
	default:
		return ErrPeerQueueFull
	}

}

func (p *RaftPeer) SendSnapshot(snapMsg raftpb.Message, snapReader SnapshotFileReader) {
	p.closer.Run(func() {
		if err := p.sendSnapshotMessageLoop(snapMsg, snapReader); err != nil {
			if !errors.Is(err, ErrPeerStopped) {
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

func (p *RaftPeer) UpdateRaftReport(report ibabuza.RaftStatusReporter) {
	p.raftReport = report
}

func (p *RaftPeer) Stop() {
	p.transportClient.Close()
	p.closer.Close()
}

func (p *RaftPeer) getQueue() (chan raftpb.Message, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	msgCh := p.currentMsgCh
	if msgCh == nil {
		p.memoryLimiter.Reset()
		select {
		case msgCh = <-p.msgQueueChPool:
		default:
			msgCh = make(chan raftpb.Message, p.raftQueueSize)
		}
		p.currentMsgCh = msgCh
		select {
		case <-p.closer.CloseCh():
			return nil, ErrPeerStopped
		case p.msgQueueCh <- msgCh:
		}
	}
	return msgCh, nil
}

func (p *RaftPeer) processRaftMessage() {
	for {
		select {
		case <-p.closer.CloseCh():
			return
		case msgCh := <-p.msgQueueCh:
			if err := p.sendRaftMessageLoop(msgCh); err != nil {
				if !errors.Is(err, ErrPeerStopped) {
					p.breaker.Fail()
					p.raftReport.ReportUnreachable(p.peerID)
				}
				p.logger.Warningf("Local peer[%d] send raft message to peerID=%d error: %v", p.localPeerID, p.peerID, err)
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
				p.logger.Warningf("Local peer[%d] message channel pool is full, drop channel", p.localPeerID)
			}

		}
	}
}

func (p *RaftPeer) sendRaftMessageLoop(msgCh chan raftpb.Message) error {
	var batchMsg = babuzapb.BatchMessage{
		ClusterID: p.clusterID,
	}
	var nextBatch bool
	var maxBatchSize int64
	msgBuf := make([]raftpb.Message, 0, cap(msgCh))
	for {
		select {
		case <-p.closer.CloseCh():
			return ErrPeerStopped
		case msg := <-msgCh:
			msgBuf = append(msgBuf, msg)
			mSize := int64(msg.Size())
			maxBatchSize += mSize
			p.memoryLimiter.Release(mSize)
			for done := false; !done; {
				select {
				case <-p.closer.CloseCh():
					return ErrPeerStopped
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
			var firstBatch, secondBatch []raftpb.Message
			if nextBatch {
				firstBatch = msgBuf[:len(msgBuf)-1]
				secondBatch = msgBuf[len(msgBuf)-1:]
			} else {
				firstBatch = msgBuf
			}
			if len(firstBatch) > 0 {
				batchMsg.Messages = firstBatch
				if err := p.transportClient.SendBatchMessage(batchMsg); err != nil {
					return err
				}
				p.breaker.Success()
				msgBuf = releaseRaftMessageBuffers(firstBatch)
			}
			if len(secondBatch) > 0 {
				batchMsg.Messages = secondBatch
				if err := p.transportClient.SendBatchMessage(batchMsg); err != nil {
					return err
				}
				p.breaker.Success()
				msgBuf = releaseRaftMessageBuffers(secondBatch)
			}
			nextBatch = false
			maxBatchSize = 0
		}
	}
}

func (p *RaftPeer) sendSnapshotMessageLoop(snapMsg raftpb.Message,
	snapFileReader SnapshotFileReader) error {

	sendSnapshotMsg := func(msg babuzapb.SnapshotMessage) error {
		select {
		case <-p.closer.CloseCh():
			return ErrPeerStopped
		default:
			if sErr := p.transportClient.SendSnapshotMessage(msg); sErr != nil {
				return sErr
			}
		}
		return nil
	}

	m := babuzapb.SnapshotMessage{
		ClusterID: p.clusterID,
		From:      snapMsg.From,
		To:        snapMsg.To,
		Term:      snapMsg.Snapshot.Metadata.Term,
		Index:     snapMsg.Snapshot.Metadata.Index,
	}
	crcTable := crc32.MakeTable(crc32.Castagnoli)

	//send metadata message
	mt := snapFileReader.Metadata()
	m.Metadata = mt
	if err := sendSnapshotMsg(m); err != nil {
		return err
	}
	p.breaker.Success()
	//send chunk message
	m.Metadata = babuzapb.SnapshotMetadata{}
	chunkBuf := make([]byte, p.snapshotChunkSize)
	if err := snapFileReader.ForEachFile(func(reader io.Reader, metadata babuzapb.SnapshotFileDesc) error {
		m.ChunkMessage.FileTag = metadata.Tag
		m.ChunkMessage.FileType = metadata.FileType
		return p.sendSnapshotMsgWithChunk(reader, metadata.FileSize, m, chunkBuf, crcTable)
	}); err != nil {
		return err
	}

	//send finish message
	m.ChunkMessage = babuzapb.SnapshotChunkMessage{}
	m.FinishMessage = snapMsg
	if err := sendSnapshotMsg(m); err != nil {
		return err
	}
	p.breaker.Success()
	return nil
}

func (p *RaftPeer) sendSnapshotMsgWithChunk(reader io.Reader, fileSize int64,
	msg babuzapb.SnapshotMessage, chunkBuf []byte, crcTable *crc32.Table) error {

	var written int64
	for {
		nr, err := reader.Read(chunkBuf)
		if err != nil {
			if err == io.EOF {
				if fileSize != written {
					return ErrPeerCorruptedSnapshotFile
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
				return ErrPeerStopped
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
