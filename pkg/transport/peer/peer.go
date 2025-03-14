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
	clusterId                  uint64
	peerId                     uint64
	limiterMaxBatchMessageSize int64
	snapshotChunkSize          int64
	raftQueueSize              int64
	raftReport                 ibabuza.RaftStatusReporter
	memoryLimiter              limiter.ResourceLimiter
	chunkRateLimiter           limiter.RateLimiter
	breaker                    breaker.Breaker
	clientFactory              TransportClientFactory
	logger                     ibabuza.Logger
	msgQueue                   chan *raftpb.Message
	closer                     *syncutil.Closer
	mu                         sync.RWMutex
}

func New(clusterId, peerId uint64, cfg RaftPeerConfig, raftReport ibabuza.RaftStatusReporter,
	memoryLimiter limiter.ResourceLimiter, chunkRateLimiter limiter.RateLimiter, breaker breaker.Breaker,
	clientFactory TransportClientFactory, logger ibabuza.Logger) *RaftPeer {

	return &RaftPeer{
		clusterId:                  clusterId,
		peerId:                     peerId,
		limiterMaxBatchMessageSize: cfg.LimiterMaxBatchMessageSize,
		snapshotChunkSize:          cfg.SnapshotChunkSize,
		raftQueueSize:              cfg.RaftMsgQueueSize,
		raftReport:                 raftReport,
		memoryLimiter:              memoryLimiter,
		chunkRateLimiter:           chunkRateLimiter,
		breaker:                    breaker,
		clientFactory:              clientFactory,
		logger:                     logger,
		closer:                     syncutil.NewCloser(),
	}
}

func (p *RaftPeer) SendRaftMessage(msg *raftpb.Message) error {
	if !p.breaker.Ready() {
		p.raftReport.ReportUnreachable(msg.To)
		return ErrPeerBreakerNotReady
	}

	acquiredSize := int64(msg.Size())
	if !p.memoryLimiter.Allow(acquiredSize) {
		p.raftReport.ReportUnreachable(msg.To)
		return ErrPeerReachMaxTotalSendMsgSize
	}
	select {
	case <-p.closer.CloseCh():
		p.raftReport.ReportUnreachable(msg.To)
		return ErrPeerStopped
	default:
	}
	msgCh := p.getQueue()
	select {
	case msgCh <- msg:
		p.memoryLimiter.Acquire(acquiredSize)
		return nil
	default:
		return ErrPeerQueueFull
	}

}

func (p *RaftPeer) SendSnapshot(snapMsg *raftpb.Message, snapReader SnapshotFileReader) {
	p.closer.Run(func() {
		client, err := p.clientFactory.CreateTransportClient()
		if err != nil {
			p.logger.Errorf("RaftPeer[Id=%d] create transport client error: %v", p.peerId, err)
			p.breaker.Fail()
			return
		}
		defer client.Close()
		if err = p.sendSnapshotMessageLoop(client, snapMsg, snapReader); err != nil {
			if !errors.Is(err, ErrPeerStopped) {
				p.breaker.Fail()
				p.raftReport.ReportUnreachable(p.peerId)
				p.raftReport.ReportSnapshot(p.peerId, raft.SnapshotFailure)
			}
			p.logger.Errorf("RaftPeer[Id=%d] send snapshot error: %v", p.peerId, err)
			return
		}
		p.raftReport.ReportSnapshot(p.peerId, raft.SnapshotFinish)
	})
}

func (p *RaftPeer) UpdateRaftReport(report ibabuza.RaftStatusReporter) {
	p.raftReport = report
}

func (p *RaftPeer) Stop() {
	p.closer.Close()
}

func (p *RaftPeer) UpdatePeer() {
	p.Stop()
	//restart
}

func (p *RaftPeer) getQueue() chan *raftpb.Message {
	p.mu.RLock()
	msgCh := p.msgQueue
	p.mu.RUnlock()
	if msgCh == nil {
		p.memoryLimiter.Reset()
		p.mu.Lock()
		p.msgQueue = make(chan *raftpb.Message, p.raftQueueSize)
		msgCh = p.msgQueue
		p.mu.Unlock()
		p.closer.Run(func() {
			client, err := p.clientFactory.CreateTransportClient()
			if err != nil {
				p.logger.Errorf("RaftPeer[Id=%d] create transport client error: %v", p.peerId, err)
				p.breaker.Fail()
				return
			}
			defer client.Close()
			if err = p.sendRaftMessageLoop(client, msgCh); err != nil {
				if !errors.Is(err, ErrPeerStopped) {
					p.breaker.Fail()
					p.raftReport.ReportUnreachable(p.peerId)
				}
				p.logger.Errorf("RaftPeer[Id=%d] send raft message error: %v", p.peerId, err)
			}
			p.drainMsgQueue()
			p.logger.Infof("RaftPeer[Id=%d] sendRaftMessageLoop goroutine exit", p.peerId)
		})
		p.logger.Infof("RaftPeer[Id=%d] start new message queue", p.peerId)
	}
	return msgCh
}

func (p *RaftPeer) sendRaftMessageLoop(client ibabuza.TransportClient, msgCh chan *raftpb.Message) error {

	var batchMsg = babuzapb.BatchMessage{
		ClusterId: p.clusterId,
	}
	var nextBatch bool
	var maxBatchSize int64
	msgBuf := make([]raftpb.Message, 0, cap(msgCh))

	for {
		select {
		case <-p.closer.CloseCh():
			return ErrPeerStopped
		case msg := <-msgCh:
			msgBuf = append(msgBuf, *msg)
			mSize := int64(msg.Size())
			maxBatchSize += mSize
			p.memoryLimiter.Release(mSize)
			for done := false; !done; {
				select {
				case msg = <-p.msgQueue:
					msgBuf = append(msgBuf, *msg)
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
			batchMsg.Messages = msgBuf
			if nextBatch {
				batchMsg.Messages = msgBuf[:len(msgBuf)-1]
				if err := client.SendBatchMessage(batchMsg); err != nil {
					return err
				}
				p.breaker.Success()
				batchMsg.Messages = msgBuf[len(msgBuf)-1:]
			}
			if err := client.SendBatchMessage(batchMsg); err != nil {
				return err
			}

			p.breaker.Success()
			//TODO: optimise. record max length,  free when condition trigger
			for i := 0; i < len(msgBuf); i++ {
				msgBuf[i].Entries = nil
			}
			nextBatch = false
			maxBatchSize = 0
			msgBuf = msgBuf[:0]
		}
	}
}

func (p *RaftPeer) sendSnapshotMessageLoop(client ibabuza.TransportClient, snapMsg *raftpb.Message,
	snapFileReader SnapshotFileReader) error {

	sendSnapshotMsg := func(msg babuzapb.SnapshotMessage) error {
		select {
		case <-p.closer.CloseCh():
			return ErrPeerStopped
		default:
			if sErr := client.SendSnapshotMessage(msg); sErr != nil {
				return sErr
			}
		}
		return nil
	}

	m := babuzapb.SnapshotMessage{
		From:  snapMsg.From,
		To:    snapMsg.To,
		Term:  snapMsg.Snapshot.Metadata.Term,
		Index: snapMsg.Snapshot.Metadata.Index,
	}
	crcTable := crc32.MakeTable(crc32.Castagnoli)

	//send metadata message
	mt := snapFileReader.Metadata()
	m.Metadata = &mt
	if err := sendSnapshotMsg(m); err != nil {
		return err
	}
	p.breaker.Success()
	//send chunk message
	m.Metadata = nil
	chunkBuf := make([]byte, p.snapshotChunkSize)
	if err := snapFileReader.ForEachFile(func(reader io.Reader, metadata babuzapb.SnapshotFileDesc) error {
		m.ChunkMessage = &babuzapb.SnapshotChunkMessage{}
		m.ChunkMessage.FileTag = metadata.Tag
		m.ChunkMessage.FileType = metadata.FileType
		return p.sendSnapshotMsgWithChunk(reader, metadata.FileSize, client, m, chunkBuf, crcTable)
	}); err != nil {
		return err
	}

	//send finish message
	m.ChunkMessage = nil
	m.FinishMessage = snapMsg
	if err := sendSnapshotMsg(m); err != nil {
		return err
	}
	p.breaker.Success()
	return nil
}

func (p *RaftPeer) sendSnapshotMsgWithChunk(reader io.Reader, fileSize int64, client ibabuza.TransportClient,
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
				if err = client.SendSnapshotMessage(msg); err != nil {
					return err
				}
				p.breaker.Success()
			}
		}
	}
}

func (p *RaftPeer) drainMsgQueue() {
	p.mu.Lock()
	tmpCh := p.msgQueue
	p.msgQueue = nil
	p.mu.Unlock()
	for {
		select {
		case <-tmpCh:
		default:
			return
		}
	}
}
