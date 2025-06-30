package experimental

import (
	"bytes"
	"encoding/binary"
	"errors"
	babuza "github.com/fanaujie/babuza/raft"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"time"
)

func (r *replica) processRaftLinearizedRead() {
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
			oldNotifier.CompleteWith(err)
			continue
		}
		rs, err := r.readIndexResponse(readCtx, leaderChangedCh)
		if err != nil {
			if errors.Is(err, babuza.ErrStopped) {
				return
			} else {
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

func (r *replica) readIndexResponse(readCtx []byte, leaderChangedCh <-chan struct{}) (rs raft.ReadState, err error) {

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
				r.logger.Warningf("groupID[%d] nodeID[%d] ignored out-of-date read index response; local node read indexes queueing up and waiting to be in sync with leader, id1: %d, id2: %d",
					r.cluster.LocalPeerID(), r.cluster.GroupID(), id1, id2)
				continue
			}
			return
		case <-leaderChangedCh:
			err = babuza.ErrLeaderChange
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
			err = babuza.ErrReadIndexRequestTimeout
			return
		case <-r.closer.CloseCh():
			err = babuza.ErrStopped
			return
		}
	}
}

func (r *replica) raftReadIndexRequest(readCtx []byte) error {
	if err := r.enqueueStepFunc(r.raftGroup.GroupID, raftpb.Message{
		Type: raftpb.MsgReadIndex,
		Entries: []raftpb.Entry{
			{
				Data: readCtx,
			},
		},
	}); err != nil {
		return err
	}
	return nil
}
