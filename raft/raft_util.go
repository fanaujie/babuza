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
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"time"
)

func (r *Raft) propose(ctx context.Context, replyID uint64, proposalData []byte) (chan ibabuza.ApplyResult, error) {
	ch, err := r.resultReplier.AcquireResultChan(replyID)
	if err != nil {
		return nil, err
	}
	r.metricsCollector.IncrementProposalPending()
	defer r.metricsCollector.DecrementProposalPending()
	if err = r.raftNode.Propose(ctx, proposalData); err != nil {
		r.metricsCollector.IncrementProposalFailed()
		r.resultReplier.CancelResult(replyID)
		if errors.Is(err, raft.ErrProposalDropped) {
			err = ErrNotLeader
		} else if errors.Is(err, raft.ErrStopped) {
			err = ErrStopped
		}
		r.logger.Warningf("raft[%d] propose failed, err: %v", r.cluster.LocalPeerID(), err)
		return nil, err
	}
	return ch, nil
}

func (r *Raft) proposeConfChange(ctx context.Context, replyID uint64, confChange raftpb.ConfChangeI) (ProposedResult, error) {
	ch, err := r.resultReplier.AcquireResultChan(replyID)
	if err != nil {
		return nil, err
	}
	r.metricsCollector.IncrementProposalPending()
	defer r.metricsCollector.DecrementProposalPending()
	if err = r.raftNode.ProposeConfChange(ctx, confChange); err != nil {
		r.metricsCollector.IncrementProposalFailed()
		r.resultReplier.CancelResult(replyID)
		if errors.Is(err, raft.ErrProposalDropped) {
			err = ErrNotLeader
		} else if errors.Is(err, raft.ErrStopped) {
			err = ErrStopped
		}
		r.logger.Warningf("raft[%d] propose failed, err: %v", r.cluster.LocalPeerID(), err)
		return nil, err
	}
	return NewProposalResult(ctx, r.closer, ch), nil
}

func (r *Raft) learnerReady(learnerId uint64) error {
	rs := r.raftNode.Status()
	if rs.Progress == nil {
		return ErrNotLeader
	}
	var learnerMatch uint64
	found := false
	ClusterID := rs.ID
	for peerID, progress := range rs.Progress {
		if learnerId == peerID {
			learnerMatch = progress.Match
			found = true
			break
		}
	}
	if found {
		leaderMatch := rs.Progress[ClusterID].Match
		if float64(learnerMatch) < float64(leaderMatch)*r.config.LearnerReadyPercent {
			return ErrLearnerNotReady
		}
	}
	return nil
}

func (r *Raft) getLeaderId() uint64 {
	return r.status.CloneSoftState().Lead
}

func (r *Raft) waitForLeaderElectionTimeout() time.Duration {
	return time.Duration(r.config.RaftConfig.ElectionTicks*r.config.LogicalTickMs) * time.Millisecond * 3
}
