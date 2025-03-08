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
		r := newProposalResult(ctx, closer, ch)
		r.err = errors.New("foo")
		r.ar = ibabuza.ApplyResult{
			LogIndex: 100,
			Response: "bar",
		}
		r.Release()
		r = newProposalResult(ctx, closer, ch)
		assert.Nil(t, r.err)
		assert.Nil(t, r.ar.Response)
		r.Release()
	})

	t.Run("success: get apply result", func(t *testing.T) {
		ch <- ibabuza.ApplyResult{
			LogIndex: 100,
			Response: "bar",
		}
		r := newProposalResult(ctx, closer, ch)
		assert.Nil(t, r.Wait())
		assert.Equal(t, uint64(100), r.LogIndex())
		assert.Equal(t, "bar", r.Response().(string))
		assert.Nil(t, r.Wait()) // call wait() twice
		r.Release()
	})

	t.Run("error: get apply result", func(t *testing.T) {
		e := errors.New("bar")
		ch <- ibabuza.ApplyResult{
			LogIndex: 100,
			Response: e,
		}
		r := newProposalResult(ctx, closer, ch)
		assert.Nil(t, r.Wait())
		assert.Equal(t, uint64(100), r.LogIndex())
		assert.Equal(t, e, r.Response().(error))
		assert.Nil(t, r.Wait()) // call wait() twice
		r.Release()
	})
	t.Run("close", func(t *testing.T) {
		r := newProposalResult(ctx, closer, ch)
		closer.Close()
		assert.Equal(t, ErrStopped, r.Wait())
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
	ch := make(chan snapshotResult, 1)
	t.Run("success", func(t *testing.T) {
		r := manualSnapshotResult{
			ctx:     ctx,
			closer:  closer,
			storage: storage,
			resulCh: ch,
		}

		ch <- snapshotResult{
			metadata: storage.snapMetadata,
		}
		assert.Nil(t, r.Wait())
		assert.Equal(t, true, r.done)
		m := r.SnapshotMetadata()
		assert.Equal(t, uint64(10), m.Snapshot.Metadata.Term)
		assert.Equal(t, uint64(100), m.Snapshot.Metadata.Index)
		assert.Nil(t, r.Wait()) // call wait() twice
	})
	t.Run("error", func(t *testing.T) {
		mr := manualSnapshotResult{
			ctx:     ctx,
			closer:  closer,
			storage: storage,
			resulCh: ch,
		}
		e := errors.New("bar")
		ch <- snapshotResult{
			err: e,
		}
		assert.Equal(t, e, mr.Wait())
		assert.Equal(t, true, mr.done)
		assert.Equal(t, e, mr.Wait())
	})
	t.Run("close", func(t *testing.T) {
		r := manualSnapshotResult{
			ctx:     ctx,
			closer:  closer,
			resulCh: ch,
		}
		closer.Close()
		assert.Equal(t, ErrStopped, r.Wait())
		assert.Equal(t, ErrStopped, r.Wait()) // call wait() twice
	})
}

func TestRaft_ShutdownResult(t *testing.T) {
	r := newShutdownResult(func() {
	})
	assert.Nil(t, r.Wait())
	assert.Nil(t, r.Wait()) // call wait() twice
}

func TestRaft_TransferLeaderResult(t *testing.T) {

	closer := syncutil.NewCloser()
	t.Run("success", func(t *testing.T) {
		ctx := context.Background()
		r := newTransferLeaderResult(ctx, 1, closer, time.Millisecond*300, func() uint64 {
			return 1
		})
		assert.Nil(t, r.Wait())
		assert.Equal(t, true, r.done)
		assert.Nil(t, r.Wait()) // call wait() twice
	})
	t.Run("deadline", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		r := newTransferLeaderResult(ctx, 1, closer, time.Millisecond*300, func() uint64 {
			return 0
		})
		assert.Equal(t, context.DeadlineExceeded, r.Wait())
		assert.Equal(t, context.DeadlineExceeded, r.Wait()) // call wait() twice
	})
	t.Run("close", func(t *testing.T) {
		ctx := context.Background()
		r := newTransferLeaderResult(ctx, 1, closer, time.Millisecond*300, func() uint64 {
			return 0
		})
		closer.Close()
		assert.Equal(t, ErrStopped, r.Wait())
		assert.Equal(t, ErrStopped, r.Wait()) // call wait() twice
	})
}
