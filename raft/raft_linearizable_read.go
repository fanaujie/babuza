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
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"go.etcd.io/etcd/raft/v3"
	"time"
)

var (
	ErrLeaderChange            = errors.New("ErrLeaderChange")
	ErrReadIndexRequestTimeout = errors.New("errReadIndexRequestTimeout")
)

func (r *Raft) processRaftLinearizedRead() {
	readCtx := make([]byte, 8)
	for {
		leaderChangedCh := r.leaderChangeNotifier.Channel()
		nextID := r.idGenerator.Next()
		select {
		case <-r.closer.CloseCh():
			return
		case <-leaderChangedCh:
			continue
		case <-r.readIndexCh:
			break
		}
		oldNotifier := r.linearizeReqNotifier.Swap()
		binary.BigEndian.PutUint64(readCtx, nextID)
		if err := r.raftReadIndexRequest(readCtx); err != nil {
			if errors.Is(err, raft.ErrStopped) {
				return
			}
			r.metricsCollector.IncrementReadIndexFailed()
			oldNotifier.CompleteWith(err)
			continue
		}
		rs, err := r.readIndexResponse(readCtx, leaderChangedCh)
		if err != nil {
			if errors.Is(err, raft.ErrStopped) || errors.Is(err, ErrStopped) {
				return
			} else {
				r.metricsCollector.IncrementReadIndexFailed()
				oldNotifier.CompleteWith(err)
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
		oldNotifier.CompleteWith(nil)
	}
}

func (r *Raft) readIndexResponse(readCtx []byte, leaderChangedCh <-chan struct{}) (rs raft.ReadState, err error) {

	retryTimer := time.NewTimer(r.config.LinearizedReadRetryTimeout)
	defer retryTimer.Stop()
	requestTimer := time.NewTimer(r.config.LinearizedReadRequestTimeout)
	defer requestTimer.Stop()
	firstCommitNotifier := r.firstCommitInTermNotifier.Channel()
	for {
		select {
		case rs = <-r.readStateCh:
			finish := bytes.Equal(rs.RequestCtx, readCtx)
			if !finish {

				// a previous request might time out. now we should ignore the response of it and
				// continue waiting for the response of the current requests.
				id1 := uint64(0)
				if len(readCtx) == 8 {
					id1 = binary.BigEndian.Uint64(readCtx)
				}
				id2 := uint64(0)
				if len(rs.RequestCtx) == 8 {
					id2 = binary.BigEndian.Uint64(rs.RequestCtx)
				}
				r.logger.Warningf("raft[%d] ignored out-of-date read index response; local node read indexes queueing up and waiting to be in sync with leader, id1: %d, id2: %d", r.cluster.ClusterID(), id1, id2)
				r.metricsCollector.IncrementSlowReadIndex()
				continue
			}
			return
		case <-leaderChangedCh:
			err = ErrLeaderChange
			return
		case <-firstCommitNotifier:
			firstCommitNotifier = r.firstCommitInTermNotifier.Channel()
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
			err = ErrReadIndexRequestTimeout
			r.metricsCollector.IncrementSlowReadIndex()
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
