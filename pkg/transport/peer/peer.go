package peer

import (
	"context"
	"errors"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/utility/breaker"
	"github.com/fanaujie/babuza/pkg/utility/limiter"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"hash/crc32"
	"io"
	"sync"
	"time"
)

var (
	ErrPeerStopped                  = errors.New("RaftPeer: ErrPeerStopped")
	ErrPeerBreakerNotReady          = errors.New("RaftPeer: ErrPeerBreakerNotReady")
	ErrPeerReachMaxTotalSendMsgSize = errors.New("RaftPeer: ErrPeerReachMaxTotalSendMsgSize")
	ErrPeerQueueFull                = errors.New("RaftPeer: ErrPeerQueueFull")
	ErrPeerCorruptedSnapshotFile    = errors.New("RaftPeer: ErrPeerCorruptedSnapshotFile")
)

type RaftPeer struct {
	id                         uint64
	limiterMaxBatchMessageSize int64
	snapshotChunkSize          int64
	raftQueueSize              int64
	dialTimeout                time.Duration
	raftReport                 ibabuza.RaftStatusReporter
	memoryLimiter              limiter.ResourceLimiter
	chunkRateLimiter           limiter.RateLimiter
	breaker                    breaker.Breaker
	dialer                     Dialer
	logger                     ibabuza.Logger
	msgQueue                   chan *raftpb.Message
	startRaftMsgCh             chan chan *raftpb.Message
	stopCh                     chan struct{}
	mu                         sync.RWMutex
}

func New(peerId uint64, cfg RaftPeerConfig, raftReport ibabuza.RaftStatusReporter,
	memoryLimiter limiter.ResourceLimiter, chunkRateLimiter limiter.RateLimiter, breaker breaker.Breaker, dialer Dialer,
	logger ibabuza.Logger) *RaftPeer {

	return &RaftPeer{
		id:                         peerId,
		limiterMaxBatchMessageSize: cfg.LimiterMaxBatchMessageSize,
		snapshotChunkSize:          cfg.SnapshotChunkSize,
		raftQueueSize:              cfg.RaftMsgQueueSize,
		dialTimeout:                cfg.DialTimeout,
		raftReport:                 raftReport,
		memoryLimiter:              memoryLimiter,
		chunkRateLimiter:           chunkRateLimiter,
		breaker:                    breaker,
		dialer:                     dialer,
		logger:                     logger,
		startRaftMsgCh:             make(chan chan *raftpb.Message),
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
	case <-p.stopCh:
		p.raftReport.ReportUnreachable(msg.To)
		return ErrPeerStopped
	default:
		p.mu.RLock()
		msgCh := p.msgQueue
		p.mu.RUnlock()

		if msgCh == nil {
			p.memoryLimiter.Reset()
			msgCh = make(chan *raftpb.Message, p.raftQueueSize)
			p.startRaftMsgCh <- msgCh
			p.logger.Infof("RaftPeer[Id=%d] start new message queue", p.id)
		}

		select {
		case msgCh <- msg:
			p.memoryLimiter.Acquire(acquiredSize)
			return nil
		default:
			p.raftReport.ReportUnreachable(msg.To)
			return ErrPeerQueueFull
		}
	}
}

func (p *RaftPeer) SendSnapshot(snapMsg *raftpb.Message, snapReader SnapshotFileReader) {

	if err := p.sendSnapshotMessageLoop(snapMsg, snapReader); err != nil {
		if !errors.Is(err, ErrPeerStopped) {
			p.breaker.Fail()
			p.raftReport.ReportUnreachable(p.id)
			p.raftReport.ReportSnapshot(p.id, raft.SnapshotFailure)
		}
		return
	}
	p.raftReport.ReportSnapshot(p.id, raft.SnapshotFinish)
	return
}

func (p *RaftPeer) UpdateRaftReport(report ibabuza.RaftStatusReporter) {
	p.raftReport = report
}

func (p *RaftPeer) Run() {
	p.stopCh = make(chan struct{})
	go func() {
		p.processRaftMsg()
	}()
}

func (p *RaftPeer) Stop() {
	close(p.stopCh)
}

func (p *RaftPeer) processRaftMsg() {
	for {
		select {
		case <-p.stopCh:
			return
		case msgCh := <-p.startRaftMsgCh:
			p.mu.Lock()
			p.msgQueue = msgCh
			p.mu.Unlock()
			if err := p.sendRaftMessageLoop(); err != nil {
				p.logger.Infof("RaftPeer[Id=%d] failed to send raft message err(%s)", p.id, err.Error())
				if !errors.Is(err, ErrPeerStopped) {
					p.breaker.Fail()
					p.raftReport.ReportUnreachable(p.id)
				}
				p.mu.Lock()
				p.msgQueue = nil
				p.mu.Unlock()
			}
		}
	}
}

func (p *RaftPeer) sendRaftMessageLoop() error {

	ctx, cancel := context.WithTimeout(context.Background(), p.dialTimeout)
	client, err := p.dialer.Dial(ctx, p.id)
	cancel()
	if err != nil {
		return err
	}
	defer client.Close()

	var batchMsg babuzapb.BatchMessage
	var nextBatch bool
	var maxBatchSize int64
	msgBuf := make([]raftpb.Message, 0, cap(p.msgQueue))
	for {
		select {
		//TODO: reqQ add context
		case <-p.stopCh:
			return ErrPeerStopped
		case msg := <-p.msgQueue:
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
				if err = client.SendBatchMessage(batchMsg); err != nil {
					return err
				}
				p.breaker.Success()
				batchMsg.Messages = msgBuf[len(msgBuf)-1:]
			}
			if err = client.SendBatchMessage(batchMsg); err != nil {
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

func (p *RaftPeer) sendSnapshotMessageLoop(snapMsg *raftpb.Message,
	snapFileReader SnapshotFileReader) error {

	if !p.breaker.Ready() {
		return ErrPeerBreakerNotReady
	}
	ctx, cancel := context.WithTimeout(context.Background(), p.dialTimeout)
	client, err := p.dialer.Dial(ctx, p.id)
	cancel()
	if err != nil {
		return err
	}
	defer client.Close()

	sendSnapshotMsg := func(msg babuzapb.SnapshotMessage) error {
		select {
		case <-p.stopCh:
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
	if err = snapFileReader.ForEachFile(func(reader io.Reader, metadata babuzapb.SnapshotFileDesc) error {
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
	if err = sendSnapshotMsg(m); err != nil {
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
			case <-p.stopCh:
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
