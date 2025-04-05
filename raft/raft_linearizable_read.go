package raft

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"go.etcd.io/etcd/raft/v3"
	"time"
)

var (
	ErrLeaderChange            = errors.New("ErrLeaderChange")
	errReadIndexRequestTimeout = errors.New("errReadIndexRequestTimeout")
)

func (r *Raft) processRaftLinearizedRead() {
	readCtx := make([]byte, 8)
	for {
		leaderChangedCh := r.leaderChangeNotifier.Get()
		nextID := r.idGenerator.Next()
		select {
		case <-r.closer.CloseCh():
			return
		case <-leaderChangedCh:
			continue
		case <-r.readIndexCh:
			break
		}
		oldNotifier := r.linearizeReqNotifier.Renew()
		binary.BigEndian.PutUint64(readCtx, nextID)
		if err := r.raftReadIndexRequest(readCtx); err != nil {
			return
		}
		rs, err := r.readIndexResponse(readCtx, leaderChangedCh)
		if err != nil {
			if err == raft.ErrStopped || err == ErrStopped {
				return
			} else {
				oldNotifier.CloseChan(err)
				continue
			}
		}
		ai := r.status.GetAppliedIndex()
		if ai < rs.Index {
			select {
			case <-r.completionReplier.AcquireCompletionChan(rs.Index):
			case <-r.closer.CloseCh():
				return
			}
		}
		oldNotifier.CloseChan(nil)
	}
}

func (r *Raft) readIndexResponse(readCtx []byte, leaderChangedCh <-chan struct{}) (rs raft.ReadState, err error) {

	retryTimer := time.NewTimer(r.config.LinearizedReadRetryTimeout)
	defer retryTimer.Stop()
	requestTimer := time.NewTimer(r.config.LinearizedReadRequestTimeout)
	defer requestTimer.Stop()
	firstCommitNotifier := r.firstCommitInTermNotifier.Get()
	for {
		select {
		case rs = <-r.readStateCh:
			finish := bytes.Equal(rs.RequestCtx, readCtx)
			if !finish {

				// a previous request might time out. now we should ignore the response of it and
				// continue waiting for the response of the current requests.
				//id2 := uint64(0)
				//if len(rs.RequestCtx) == 8 {
				//	id2 = binary.BigEndian.Uint64(rs.RequestCtx)
				//}
				//lg.Warn(
				//	"ignored out-of-date read index response; local node read indexes queueing up and waiting to be in sync with leader",
				//	zap.Uint64("sent-request-Id", id1),
				//	zap.Uint64("receiver-request-Id", id2),
				//)
				//slowReadIndex.Inc()
				continue
			}
			return
		case <-leaderChangedCh:
			err = ErrLeaderChange
			return
		case <-firstCommitNotifier:
			firstCommitNotifier = r.firstCommitInTermNotifier.Get()
			if err = r.raftReadIndexRequest(readCtx); err != nil {
				return
			}
			retryTimer.Reset(r.config.LinearizedReadRetryTimeout)
		case <-retryTimer.C:
			if err = r.raftReadIndexRequest(readCtx); err != nil {
				return
			}
			retryTimer.Reset(r.config.LinearizedReadRetryTimeout)
		case <-requestTimer.C:
			err = errReadIndexRequestTimeout
			return
		case <-r.closer.CloseCh():
			err = ErrStopped
			return
		}
	}
}

func (r *Raft) raftReadIndexRequest(rctx []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	return r.raftNode.ReadIndex(ctx, rctx)
}
