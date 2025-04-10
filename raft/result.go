package raft

import (
	"context"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/utility/syncutil"
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

type errorResult struct {
	e error
}

func newErrorResult(err error) *errorResult {
	return &errorResult{e: err}
}

func (er *errorResult) Wait() error {
	return er.e
}

func (er *errorResult) Response() any {
	return nil
}

func (er *errorResult) LogIndex() uint64 {
	return 0
}

func (er *errorResult) Release() {
	er.e = nil
}

func (er *errorResult) SnapshotMetadata() babuzapb.SnapshotMetadata {
	return babuzapb.SnapshotMetadata{}
}

func (er *errorResult) SnapshotFileReader() (ibabuza.SnapshotReader, error) {
	return nil, er.e
}

type proposalResult struct {
	ctx     context.Context
	closer  *syncutil.Closer
	resulCh chan ibabuza.ApplyResult
	ar      ibabuza.ApplyResult
}

func newProposalResult(ctx context.Context, closer *syncutil.Closer, resultCh chan ibabuza.ApplyResult) *proposalResult {
	res := proposalResPool.Get()
	if res == nil {
		return &proposalResult{
			ctx:     ctx,
			closer:  closer,
			resulCh: resultCh,
		}
	}
	pr := res.(*proposalResult)
	pr.ctx = ctx
	pr.closer = closer
	pr.resulCh = resultCh
	return pr
}

func (p *proposalResult) Wait() error {
	if p.resulCh == nil {
		panic("proposalResult already released")
	}
	if p.ar.LogIndex != 0 {
		return p.ar.Error
	}
	select {
	case <-p.closer.CloseCh():
		return ErrStopped
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

func (p *proposalResult) Response() any {
	if p.resulCh == nil {
		panic("proposalResult already released")
	}
	if p.ar.LogIndex == 0 {
		panic("proposalResult not ready")
	}
	return p.ar.Response
}

func (p *proposalResult) LogIndex() uint64 {
	if p.resulCh == nil {
		panic("proposalResult already released")
	}
	if p.ar.LogIndex == 0 {
		panic("proposalResult not ready")
	}
	return p.ar.LogIndex
}

func (p *proposalResult) Release() {
	p.resulCh = nil
	p.ctx = nil
	p.closer = nil
	p.ar = ibabuza.ApplyResult{}
	proposalResPool.Put(p)
}

type manualSnapshotResult struct {
	ctx     context.Context
	closer  *syncutil.Closer
	storage InternalStorage
	result  snapshotResult
	done    bool
	resulCh chan snapshotResult
	babuza  *Raft
}

func (m *manualSnapshotResult) Wait() error {
	if m.result.err != nil || m.done {
		return m.result.err
	}
	select {
	case <-m.closer.CloseCh():
		m.result.err = ErrStopped
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
	return m.babuza.storage.CreateSnapshotReader(m.result.metadata.Snapshot.Metadata.Index)
}

type shutdownResult struct {
	resulCh chan struct{}
}

func newShutdownResult(raftStop func()) *shutdownResult {
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

func newTransferLeaderResult(ctx context.Context, transferee uint64, closer *syncutil.Closer, checkNewLeaderTimeout time.Duration,
	getLeaderId func() uint64) *transferLeaderResult {
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
			r.resulCh <- ErrStopped
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
