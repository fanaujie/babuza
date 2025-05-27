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
	ErrPeerStopped                  = errors.New("RaftPeerImpl: ErrPeerStopped")
	ErrPeerBreakerNotReady          = errors.New("RaftPeerImpl: ErrPeerBreakerNotReady")
	ErrPeerReachMaxTotalSendMsgSize = errors.New("RaftPeerImpl: ErrPeerReachMaxTotalSendMsgSize")
	ErrPeerQueueFull                = errors.New("RaftPeerImpl: ErrPeerQueueFull")
	ErrPeerCorruptedSnapshotFile    = errors.New("RaftPeerImpl: ErrPeerCorruptedSnapshotFile")
)

type RaftPeerImpl struct {
	clusterID                  uint64
	localNodeID                uint64
	remoteRaftAddr             string
	limiterMaxBatchMessageSize int64
	snapshotChunkSize          int64
	raftQueueSize              int64
	raftReport                 ibabuza.RaftStatusReporter
	memoryLimiter              limiter.ResourceLimiter
	chunkRateLimiter           limiter.RateLimiter
	breaker                    breaker.Breaker
	transportClient            ibabuza.TransportClient
	logger                     ibabuza.Logger
	closer                     *syncutil.Closer
	queueMu                    struct {
		sync.Mutex
		currentMsgCh chan raftpb.Message
		msgQueueCh   chan chan raftpb.Message
	}
}

func New(clusterID, localNodeID uint64, remoteRaftAddr string, cfg RaftPeerConfig,
	raftReport ibabuza.RaftStatusReporter, memoryLimiter limiter.ResourceLimiter,
	chunkRateLimiter limiter.RateLimiter, breaker breaker.Breaker, transportClient ibabuza.TransportClient,
	logger ibabuza.Logger) *RaftPeerImpl {
	closer := syncutil.NewCloser()
	r := &RaftPeerImpl{
		clusterID:                  clusterID,
		localNodeID:                localNodeID,
		remoteRaftAddr:             remoteRaftAddr,
		limiterMaxBatchMessageSize: cfg.LimiterMaxBatchMessageSize,
		snapshotChunkSize:          cfg.SnapshotChunkSize,
		raftQueueSize:              cfg.RaftMsgQueueSize,
		raftReport:                 raftReport,
		memoryLimiter:              memoryLimiter,
		chunkRateLimiter:           chunkRateLimiter,
		breaker:                    breaker,
		transportClient:            transportClient,
		logger:                     logger,
		closer:                     closer,
		queueMu: struct {
			sync.Mutex
			currentMsgCh chan raftpb.Message
			msgQueueCh   chan chan raftpb.Message
		}{
			msgQueueCh: make(chan chan raftpb.Message, 1),
		},
	}
	closer.Run(func() {
		r.processRaftMessage()
	})
	return r
}

func (p *RaftPeerImpl) SendRaftMessage(msg raftpb.Message) error {
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
		return ErrPeerStopped
	case msgCh <- msg:
		p.memoryLimiter.Acquire(acquiredSize)
		return nil
	default:
		return ErrPeerQueueFull
	}

}

func (p *RaftPeerImpl) SendSnapshot(snapMsg raftpb.Message, snapReader SnapshotFileReader) {
	p.closer.Run(func() {
		if err := p.sendSnapshotMessageLoop(snapMsg, snapReader); err != nil {
			if !errors.Is(err, ErrPeerStopped) {
				p.breaker.Fail()
				p.raftReport.ReportUnreachable(snapMsg.To)
				p.raftReport.ReportSnapshot(snapMsg.To, raft.SnapshotFailure)
			}
			p.logger.Errorf("Node[%d] send snapshot message to peerID=%d error: %v", p.localNodeID, snapMsg.To, err)
			return
		}
		p.raftReport.ReportSnapshot(snapMsg.To, raft.SnapshotFinish)
	})
}

func (p *RaftPeerImpl) UpdateRaftReport(report ibabuza.RaftStatusReporter) {
	p.raftReport = report
}

func (p *RaftPeerImpl) Stop() {
	p.transportClient.Close()
	p.closer.Close()
}

func (p *RaftPeerImpl) getQueue() (chan raftpb.Message, error) {
	p.queueMu.Lock()
	defer p.queueMu.Unlock()
	msgCh := p.queueMu.currentMsgCh
	if msgCh == nil {
		p.memoryLimiter.Reset()
		select {
		case <-p.closer.CloseCh():
			return nil, ErrPeerStopped
		default:
			msgCh = make(chan raftpb.Message, p.raftQueueSize)
		}
		p.queueMu.currentMsgCh = msgCh
		select {
		case <-p.closer.CloseCh():
			return nil, ErrPeerStopped
		case p.queueMu.msgQueueCh <- msgCh:
		}
	}
	return msgCh, nil
}

func (p *RaftPeerImpl) processRaftMessage() {
	for {
		select {
		case <-p.closer.CloseCh():
			return
		case msgCh := <-p.queueMu.msgQueueCh:
			if err := p.sendRaftMessageLoop(msgCh); err != nil {
				p.logger.Warningf("Node[%d] send raft message error: %v", p.localNodeID, err)
			}
			p.queueMu.Lock()
			p.queueMu.currentMsgCh = nil
			p.queueMu.Unlock()
		}
	}
}

func (p *RaftPeerImpl) sendRaftMessageLoop(msgCh chan raftpb.Message) error {
	var batchMsg = babuzapb.BatchMessage{
		ClusterID: p.clusterID,
	}
	msgBuf := make([]raftpb.Message, 0, cap(msgCh))
	for {
		select {
		case <-p.closer.CloseCh():
			return ErrPeerStopped
		case msg := <-msgCh:
			msgBuf = append(msgBuf, msg)
			mSize := int64(msg.Size())
			remainSize := p.limiterMaxBatchMessageSize - mSize
			p.memoryLimiter.Release(mSize)
			for remainSize > 0 {
				select {
				case <-p.closer.CloseCh():
					return ErrPeerStopped
				case msg = <-msgCh:
					msgBuf = append(msgBuf, msg)
					mSize = int64(msg.Size())
					p.memoryLimiter.Release(mSize)
					remainSize -= mSize
				default:
					remainSize = -1
				}
			}
			batchMsg.Messages = msgBuf
			if err := p.transportClient.SendBatchMessage(batchMsg); err != nil {
				p.breaker.Fail()
				for _, m := range msgBuf {
					p.raftReport.ReportUnreachable(m.To)
				}
				return err
			}
			p.breaker.Success()
			msgBuf = releaseRaftMessageBuffers(msgBuf)
			batchMsg.Messages = nil
		}
	}
}

func (p *RaftPeerImpl) sendSnapshotMessageLoop(snapMsg raftpb.Message,
	snapFileReader SnapshotFileReader) error {

	sendSnapshotMsg := func(msg babuzapb.SnapshotMessage) error {
		select {
		case <-p.closer.CloseCh():
			return ErrPeerStopped
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
		From:      snapMsg.From,
		To:        snapMsg.To,
		Term:      snapMsg.Snapshot.Metadata.Term,
		Index:     snapMsg.Snapshot.Metadata.Index,
	}
	crcTable := crc32.MakeTable(crc32.Castagnoli)

	//send metadata message
	mt := snapFileReader.Metadata()
	m.Type = babuzapb.SnapshotMessageType_Metadata
	m.Metadata = mt
	if err := sendSnapshotMsg(m); err != nil {
		return err
	}
	p.breaker.Success()
	//send chunk message
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

	//send finish message
	m.ChunkMessage = babuzapb.SnapshotChunkMessage{}
	m.Type = babuzapb.SnapshotMessageType_Finish
	m.FinishMessage = snapMsg
	if err := sendSnapshotMsg(m); err != nil {
		return err
	}
	p.breaker.Success()
	return nil
}

func (p *RaftPeerImpl) sendSnapshotMsgWithChunk(reader io.Reader, fileSize int64,
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
