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


package raft

import (
	"context"
	"errors"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/utility/syncutil"
	"math/rand"
	"sync"
	"time"
)

type Result interface {
	Wait() error
}

type ProposedResult interface {
	WaitForApplyResult() ibabuza.ApplyResult
	Release()
}

type ShutdownResult interface {
	Result
}

type TransferLeaderResult interface {
	Result
}

type ManualSnapshotResult interface {
	Result
	SnapshotMetadata() (babuzapb.SnapshotMetadata, error)
	SnapshotFileReader() (ibabuza.SnapshotReader, error)
}

type PublishApplicationServiceResult interface {
	Result
}

type ErrorResult interface {
	ProposedResult
	ShutdownResult
	TransferLeaderResult
	ManualSnapshotResult
	PublishApplicationServiceResult
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

type errorResult struct {
	e error
}

func NewErrorResult(err error) ErrorResult {
	return &errorResult{e: err}
}

func (er *errorResult) Wait() error {
	return er.e
}

func (er *errorResult) WaitForApplyResult() ibabuza.ApplyResult {
	return ibabuza.ApplyResult{
		Error: er.e,
	}
}

func (er *errorResult) Release() {
	er.e = nil
}

func (er *errorResult) SnapshotMetadata() (babuzapb.SnapshotMetadata, error) {
	return babuzapb.SnapshotMetadata{}, er.e
}

func (er *errorResult) SnapshotFileReader() (ibabuza.SnapshotReader, error) {
	return nil, er.e
}

type proposalResult struct {
	ctx      context.Context
	closer   *syncutil.Closer
	resultCh chan ibabuza.ApplyResult
	ar       ibabuza.ApplyResult
}

func NewProposalResult(ctx context.Context, closer *syncutil.Closer, resultCh chan ibabuza.ApplyResult) ProposedResult {
	res := proposalResPool.Get()
	if res == nil {
		return &proposalResult{
			ctx:      ctx,
			closer:   closer,
			resultCh: resultCh,
		}
	}
	pr := res.(*proposalResult)
	pr.ctx = ctx
	pr.closer = closer
	pr.resultCh = resultCh
	return pr
}

func (p *proposalResult) WaitForApplyResult() ibabuza.ApplyResult {
	if p.resultCh == nil {
		panic("proposalResult already released")
	}
	if !p.ar.IsEmpty() {
		return p.ar
	}
	select {
	case <-p.closer.CloseCh():
		return ibabuza.ApplyResult{
			Error: ErrStopped,
		}
	case <-p.ctx.Done():
		return ibabuza.ApplyResult{
			Error: p.ctx.Err(),
		}
	case result := <-p.resultCh:
		p.ar = result
		return result
	}
}

func (p *proposalResult) Release() {
	*p = proposalResult{}
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
	return m.WaitForCompletion()
}

func (m *manualSnapshotResult) WaitForCompletion() error {
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

func (m *manualSnapshotResult) SnapshotMetadata() (babuzapb.SnapshotMetadata, error) {
	if m.done {
		if m.result.err == nil {
			return m.result.metadata, nil
		}
		return babuzapb.SnapshotMetadata{}, m.result.err
	}
	return babuzapb.SnapshotMetadata{}, errors.New("manualSnapshotResult not completed")
}

func (m *manualSnapshotResult) SnapshotFileReader() (ibabuza.SnapshotReader, error) {
	if m.done {
		if m.result.err == nil {
			return m.reader.CreateSnapshotReader(m.result.metadata.Snapshot.Metadata.Index)
		}
		return nil, m.result.err
	}
	return nil, errors.New("manualSnapshotResult not completed")
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
