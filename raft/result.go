package raft

import (
	"context"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/utility/syncutil"
	"go.etcd.io/etcd/raft/v3"
	"math/rand"
	"sync"
	"time"
)

type ProposedResult interface {
	Wait() error
	Response() any
	LogIndex() uint64
	Release()
}

type ShutdownResult interface {
	Wait() error
}

type TransferLeaderResult interface {
	Wait() error
}

type ManualSnapshotResult interface {
	Wait() error
	SnapshotMetadata() babuzapb.SnapshotMetadata
	SnapshotFileReader() (ibabuza.SnapshotReader, error)
}

type PublishApplicationServiceResult interface {
	Wait() error
}

var (
	proposalResPool sync.Pool
)

type SnapshotResult struct {
	metadata babuzapb.SnapshotMetadata
	err      error
}

func NewSnapshotResult(metadata babuzapb.SnapshotMetadata, err error) SnapshotResult {
	return SnapshotResult{
		metadata: metadata,
		err:      err,
	}
}

type ErrorResult struct {
	e error
}

func NewErrorResult(err error) *ErrorResult {
	return &ErrorResult{e: err}
}

func (er *ErrorResult) Wait() error {
	return er.e
}

func (er *ErrorResult) Response() any {
	return nil
}

func (er *ErrorResult) LogIndex() uint64 {
	return 0
}

func (er *ErrorResult) Release() {
	er.e = nil
}

func (er *ErrorResult) SnapshotMetadata() babuzapb.SnapshotMetadata {
	return babuzapb.SnapshotMetadata{}
}

func (er *ErrorResult) SnapshotFileReader() (ibabuza.SnapshotReader, error) {
	return nil, er.e
}

type ProposalResult struct {
	ctx     context.Context
	closer  *syncutil.Closer
	resulCh chan ibabuza.ApplyResult
	ar      ibabuza.ApplyResult
}

func NewProposalResult(ctx context.Context, closer *syncutil.Closer, resultCh chan ibabuza.ApplyResult) ProposedResult {
	res := proposalResPool.Get()
	if res == nil {
		return &ProposalResult{
			ctx:     ctx,
			closer:  closer,
			resulCh: resultCh,
		}
	}
	pr := res.(*ProposalResult)
	pr.ctx = ctx
	pr.closer = closer
	pr.resulCh = resultCh
	return pr
}

func (p *ProposalResult) Wait() error {
	if p.resulCh == nil {
		panic("proposalResult already released")
	}
	if p.ar.LogIndex != 0 {
		return p.ar.Error
	}
	select {
	case <-p.closer.CloseCh():
		return raft.ErrStopped
	case <-p.ctx.Done():
		return p.ctx.Err()
	case result := <-p.resulCh:
		p.ar = result
		if result.Error != nil {
			return result.Error
		}
		return nil
	}
}

func (p *ProposalResult) Response() any {
	if p.resulCh == nil {
		panic("proposalResult already released")
	}
	if p.ar.LogIndex == 0 {
		panic("proposalResult not ready")
	}
	return p.ar.Response
}

func (p *ProposalResult) LogIndex() uint64 {
	if p.resulCh == nil {
		panic("proposalResult already released")
	}
	if p.ar.LogIndex == 0 {
		panic("proposalResult not ready")
	}
	return p.ar.LogIndex
}

func (p *ProposalResult) Release() {
	p.resulCh = nil
	p.ctx = nil
	p.closer = nil
	p.ar = ibabuza.ApplyResult{}
	proposalResPool.Put(p)
}

type SnapshotReader interface {
	CreateSnapshotReader(snapshotIndex uint64) (ibabuza.SnapshotReader, error)
}

type manualSnapshotResult struct {
	ctx     context.Context
	closer  *syncutil.Closer
	reader  SnapshotReader
	result  SnapshotResult
	done    bool
	resulCh chan SnapshotResult
}

func NewManualSnapshotResult(ctx context.Context, closer *syncutil.Closer, reader SnapshotReader,
	resultCh chan SnapshotResult) ManualSnapshotResult {
	return &manualSnapshotResult{
		ctx:     ctx,
		closer:  closer,
		reader:  reader,
		resulCh: resultCh,
	}
}

func (m *manualSnapshotResult) Wait() error {
	if m.result.err != nil || m.done {
		return m.result.err
	}
	select {
	case <-m.closer.CloseCh():
		m.result.err = raft.ErrStopped
		return m.result.err
	case <-m.ctx.Done():
		m.result.err = m.ctx.Err()
		return m.result.err
	case m.result = <-m.resulCh:
		m.done = true
		return m.result.err
	}
}

func (m *manualSnapshotResult) SnapshotMetadata() babuzapb.SnapshotMetadata {
	return m.result.metadata
}

func (m *manualSnapshotResult) SnapshotFileReader() (ibabuza.SnapshotReader, error) {
	return m.reader.CreateSnapshotReader(m.result.metadata.Snapshot.Metadata.Index)
}

type shutdownResult struct {
	resulCh chan struct{}
}

func NewShutdownResult(raftStop func()) ShutdownResult {
	r := &shutdownResult{
		resulCh: make(chan struct{}),
	}
	go func() {
		raftStop()
		close(r.resulCh)
	}()
	return r
}

func (r *shutdownResult) Wait() error {
	<-r.resulCh
	return nil
}

type transferLeaderResult struct {
	transferee            uint64
	ctx                   context.Context
	closer                *syncutil.Closer
	checkNewLeaderTimeout time.Duration
	getLeaderId           func() uint64
	err                   error
	done                  bool
	resulCh               chan error
}

func NewTransferLeaderResult(ctx context.Context, transferee uint64, closer *syncutil.Closer,
	checkNewLeaderTimeout time.Duration,
	getLeaderId func() uint64) TransferLeaderResult {
	r := &transferLeaderResult{
		transferee:            transferee,
		ctx:                   ctx,
		closer:                closer,
		checkNewLeaderTimeout: checkNewLeaderTimeout,
		getLeaderId:           getLeaderId,
		resulCh:               make(chan error, 1),
	}
	go r.do()
	return r
}

func (r *transferLeaderResult) do() {
	ticker := time.NewTicker(r.checkNewLeaderTimeout + time.Duration(rand.Int63n(int64(r.checkNewLeaderTimeout/10))))
	defer ticker.Stop()
	for r.getLeaderId() != r.transferee {
		select {
		case <-r.closer.CloseCh():
			r.resulCh <- raft.ErrStopped
			return
		case <-r.ctx.Done():
			r.resulCh <- r.ctx.Err()
			return
		case <-ticker.C:
		}
	}
	r.resulCh <- nil
}

func (r *transferLeaderResult) Wait() error {
	if r.err != nil {
		return r.err
	}
	if r.done {
		return nil
	}
	r.err = <-r.resulCh
	r.done = true
	return r.err

}
