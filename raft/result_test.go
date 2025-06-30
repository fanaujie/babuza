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
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"testing"
	"time"
)

func TestRaft_ProposalResult(t *testing.T) {
	ctx := context.Background()
	closer := syncutil.NewCloser()
	ch := make(chan ibabuza.ApplyResult, 1)

	t.Run("get proposalResult from pool", func(t *testing.T) {
		r := NewProposalResult(ctx, closer, ch).(*proposalResult)
		r.ar = ibabuza.ApplyResult{
			LogIndex: 100,
			Response: "bar",
		}
		r.Release()
		assert.Nil(t, r.resultCh)
		assert.Nil(t, r.closer)
		assert.Nil(t, r.ctx)
		r2 := NewProposalResult(ctx, closer, ch).(*proposalResult)
		assert.Nil(t, r2.ar.Response)
		r2.Release()
	})

	t.Run("success: get apply result", func(t *testing.T) {
		ch <- ibabuza.ApplyResult{
			LogIndex: 100,
			Response: "bar",
		}
		r := NewProposalResult(ctx, closer, ch)
		defer r.Release()
		ar := r.WaitForApplyResult()
		assert.Nil(t, ar.Error)
		assert.Equal(t, uint64(100), ar.LogIndex)
		assert.Equal(t, "bar", ar.Response.(string))
		ar = r.WaitForApplyResult()
		assert.Equal(t, uint64(100), ar.LogIndex)
		assert.Equal(t, "bar", ar.Response.(string))
	})

	t.Run("error: get apply result", func(t *testing.T) {
		e := errors.New("bar")
		ch <- ibabuza.ApplyResult{
			LogIndex: 100,
			Error:    e,
		}
		r := NewProposalResult(ctx, closer, ch)
		defer r.Release()
		ar := r.WaitForApplyResult()
		assert.Error(t, ar.Error)
		assert.Equal(t, uint64(100), ar.LogIndex)
		ar = r.WaitForApplyResult()
		assert.Error(t, ar.Error)
		assert.Equal(t, uint64(100), ar.LogIndex)

	})
	t.Run("close", func(t *testing.T) {
		r := NewProposalResult(ctx, closer, ch)
		closer.Close()
		ar := r.WaitForApplyResult()
		assert.Equal(t, ErrStopped, ar.Error)
		r.Release()
	})

}

func TestRaft_ManualSnapshotResult(t *testing.T) {
	ctx := context.Background()
	closer := syncutil.NewCloser()
	storage := &mockStorageMgr{
		snapMetadata: babuzapb.SnapshotMetadata{
			Version: 1,
			Snapshot: raftpb.Snapshot{
				Metadata: raftpb.SnapshotMetadata{
					Index: 100,
					Term:  10,
				},
			},
		},
	}
	ch := make(chan SnapshotResult, 1)
	t.Run("success", func(t *testing.T) {
		r := manualSnapshotResult{
			ctx:     ctx,
			closer:  closer,
			resulCh: ch,
		}

		ch <- SnapshotResult{
			metadata: storage.snapMetadata,
		}
		assert.Nil(t, r.WaitForCompletion())
		assert.Equal(t, true, r.done)
		m, err := r.SnapshotMetadata()
		assert.Nil(t, err)
		assert.Equal(t, uint64(10), m.Snapshot.Metadata.Term)
		assert.Equal(t, uint64(100), m.Snapshot.Metadata.Index)
		assert.Nil(t, r.WaitForCompletion()) // call wait() twice
	})
	t.Run("error", func(t *testing.T) {
		mr := manualSnapshotResult{
			ctx:     ctx,
			closer:  closer,
			resulCh: ch,
		}
		e := errors.New("bar")
		ch <- SnapshotResult{
			err: e,
		}
		assert.Equal(t, e, mr.WaitForCompletion())
		assert.Equal(t, true, mr.done)
		assert.Equal(t, e, mr.WaitForCompletion())
	})
	t.Run("close", func(t *testing.T) {
		r := manualSnapshotResult{
			ctx:     ctx,
			closer:  closer,
			resulCh: ch,
		}
		closer.Close()
		assert.Equal(t, ErrStopped, r.WaitForCompletion())
		assert.Equal(t, ErrStopped, r.WaitForCompletion()) // call wait() twice
	})
}

func TestRaft_ShutdownResult(t *testing.T) {
	r := NewShutdownResult(func() {
	})
	assert.Nil(t, r.Wait())
	assert.Nil(t, r.Wait()) // call wait() twice
}

func TestRaft_TransferLeaderResult(t *testing.T) {

	closer := syncutil.NewCloser()
	t.Run("success", func(t *testing.T) {
		ctx := context.Background()
		r := NewTransferLeaderResult(ctx, 1, closer, time.Millisecond*300, func() uint64 {
			return 1
		})
		assert.Nil(t, r.Wait())
		assert.Equal(t, true, r.(*transferLeaderResult).done)
		assert.Nil(t, r.Wait()) // call wait() twice
	})
	t.Run("deadline", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		r := NewTransferLeaderResult(ctx, 1, closer, time.Millisecond*300, func() uint64 {
			return 0
		})
		assert.Equal(t, context.DeadlineExceeded, r.Wait())
		assert.Equal(t, context.DeadlineExceeded, r.Wait()) // call wait() twice
	})
	t.Run("close", func(t *testing.T) {
		ctx := context.Background()
		r := NewTransferLeaderResult(ctx, 1, closer, time.Millisecond*300, func() uint64 {
			return 0
		})
		closer.Close()
		assert.Equal(t, ErrStopped, r.Wait())
		assert.Equal(t, ErrStopped, r.Wait()) // call wait() twice
	})
}
