package raft

import (
	"context"
	"errors"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/pkg/cluster"
	"github.com/fanaujie/babuza/pkg/logger"
	"github.com/fanaujie/babuza/pkg/metrics"
	"github.com/fanaujie/babuza/pkg/replier"
	"github.com/fanaujie/babuza/pkg/status"
	"github.com/fanaujie/babuza/pkg/utility/syncutil"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/raft/v3"
	"testing"
	"time"
)

func TestRaft_PubAppService_FindLeader(t *testing.T) {

	t.Run("success: find leader", func(t *testing.T) {
		closer := syncutil.NewCloser()
		r := &Raft{
			status: status.New(),
			closer: closer,
		}
		r.status.SetSoftState(raft.SoftState{Lead: 1})
		leaderID, err := r.findLeader(context.Background(), time.Millisecond*100)
		assert.Nil(t, err)
		assert.Equal(t, uint64(1), leaderID)
	})

	t.Run("raft stop: find leader", func(t *testing.T) {
		closer := syncutil.NewCloser()
		r := &Raft{
			status: status.New(),
			closer: closer,
		}
		closer.Close()
		_, err := r.findLeader(context.Background(), time.Millisecond*100)
		assert.Equal(t, ErrStopped, err)
	})
}

func TestRaft_PubAppService_ProposalPubAppService(t *testing.T) {
	t.Run("success", func(t *testing.T) {

		closer := syncutil.NewCloser()
		r := &Raft{
			raftNode:         newMockRaftNode(),
			metricsCollector: metrics.NewMockMetricsCollector(),
			resultReplier:    replier.NewResult[ibabuza.ApplyResult](),
			status:           status.New(),
			closer:           closer,
		}
		res := r.proposalPubAppService(context.Background(), 1, nil)
		defer res.Release()
		r.resultReplier.SendResult(1, ibabuza.ApplyResult{
			LogIndex: uint64(100),
			Response: uint64(1000),
		})
		err := res.Wait()
		assert.Nil(t, err)
		assert.Equal(t, uint64(100), res.LogIndex())
		ar := res.Response()
		assert.Nil(t, err)
		assert.Equal(t, uint64(1000), ar.(uint64))
	})

	t.Run("raft stop", func(t *testing.T) {
		closer := syncutil.NewCloser()
		r := &Raft{
			raftNode:         newMockRaftNode(),
			metricsCollector: metrics.NewMockMetricsCollector(),
			resultReplier:    replier.NewResult[ibabuza.ApplyResult](),
			status:           status.New(),
			closer:           closer,
		}
		closer.Close()
		res := r.proposalPubAppService(context.Background(), 1, nil)
		defer res.Release()
		err := res.Wait()
		assert.Equal(t, ErrStopped, err)
	})

	t.Run("failure: raftNodeProposal ", func(t *testing.T) {
		closer := syncutil.NewCloser()
		m := newMockRaftNode()
		m.errorPropose = ErrNotLeader
		log := &logger.Mock{}
		cl := cluster.NewCluster(log)
		cl.SetClusterID(1)
		r := &Raft{
			raftNode:         m,
			cluster:          cl,
			metricsCollector: metrics.NewMockMetricsCollector(),
			logger:           log,
			resultReplier:    replier.NewResult[ibabuza.ApplyResult](),
			status:           status.New(),
			closer:           closer,
		}
		res := r.proposalPubAppService(context.Background(), 1, nil)
		defer res.Release()
		err := res.Wait()
		assert.Equal(t, ErrNotLeader, err)
	})
}

func TestRaft_PubAppService_SendPubAppServiceMsgToLeader(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		closer := syncutil.NewCloser()
		trans := &mockTransport{
			mockClient: &mockPubTransClient{},
		}
		r := &Raft{
			trans:         trans,
			resultReplier: replier.NewResult[ibabuza.ApplyResult](),
			status:        status.New(),
			closer:        closer,
		}
		resultCh := make(chan error)
		go func() {
			resultCh <- r.sendPubAppServiceMsgToLeader(context.Background(), 1, 1, []string{"foo"})
		}()
		time.Sleep(time.Millisecond * 100)
		r.resultReplier.SendResult(1, ibabuza.ApplyResult{})
		assert.Nil(t, <-resultCh)
	})
	t.Run("failure: cluster", func(t *testing.T) {
		closer := syncutil.NewCloser()
		trans := &mockTransport{
			mockClient: &mockPubTransClient{},
		}
		log := &logger.Mock{}
		cl := cluster.NewCluster(log)
		cl.SetClusterID(1)
		mockMetric := metrics.NewMockMetricsCollector()
		r := &Raft{
			cluster:          cl,
			logger:           log,
			metricsCollector: mockMetric,
			trans:            trans,
			resultReplier:    replier.NewResult[ibabuza.ApplyResult](),
			status:           status.New(),
			closer:           closer,
		}
		resultCh := make(chan error)
		go func() {
			resultCh <- r.sendPubAppServiceMsgToLeader(context.Background(), 1, 1, []string{"foo"})
		}()
		time.Sleep(time.Millisecond * 100)
		err := errors.New("foo")
		r.resultReplier.SendResult(1, ibabuza.ApplyResult{
			LogIndex: 10,
			Error:    err,
		})
		assert.Equal(t, err, <-resultCh)
	})
	t.Run("failure: transport ", func(t *testing.T) {
		closer := syncutil.NewCloser()
		err := errors.New("foo")
		trans := &mockTransport{
			mockClient: &mockPubTransClient{
				errMsg: err.Error(),
			},
		}
		log := &logger.Mock{}
		cl := cluster.NewCluster(log)
		cl.SetClusterID(1)
		mockMetric := metrics.NewMockMetricsCollector()
		r := &Raft{
			cluster:          cl,
			logger:           log,
			metricsCollector: mockMetric,
			trans:            trans,
			resultReplier:    replier.NewResult[ibabuza.ApplyResult](),
			status:           status.New(),
			closer:           closer,
		}
		resultCh := make(chan error)
		go func() {
			resultCh <- r.sendPubAppServiceMsgToLeader(context.Background(), 1, 1, []string{"foo"})
		}()
		time.Sleep(time.Millisecond * 100)

		r.resultReplier.SendResult(1, ibabuza.ApplyResult{
			LogIndex: 10,
			Error:    err,
		})
		assert.Equal(t, err, <-resultCh)
	})

	t.Run("raft stop", func(t *testing.T) {
		closer := syncutil.NewCloser()
		trans := &mockTransport{
			mockClient: &mockPubTransClient{},
		}
		r := &Raft{
			trans:         trans,
			resultReplier: replier.NewResult[ibabuza.ApplyResult](),
			status:        status.New(),
			closer:        closer,
		}
		resultCh := make(chan error)
		go func() {
			resultCh <- r.sendPubAppServiceMsgToLeader(context.Background(), 1, 1, []string{"foo"})
		}()
		closer.Close()
		assert.Equal(t, ErrStopped, <-resultCh)
	})
}

func TestRaft_ApplicationServiceStart_DisableProposalForwarding(t *testing.T) {
	t.Run("disableProposalForwarding: true, local peer is a leader", func(t *testing.T) {
		closer := syncutil.NewCloser()
		trans := &mockTransport{
			mockClient: &mockPubTransClient{},
		}
		log := &logger.Mock{}
		cl := cluster.NewCluster(log)
		cl.SetClusterID(1)
		mockMetric := metrics.NewMockMetricsCollector()
		r := &Raft{
			idGenerator: &mockIdGenerator{
				id: 1,
			},
			config: BabuzaConfig{
				LocalPeerID: 1,
				RaftConfig: RaftConfig{
					DisableProposalForwarding: true,
				}},
			cluster:          cl,
			logger:           log,
			metricsCollector: mockMetric,
			trans:            trans,
			raftNode:         newMockRaftNode(),
			resultReplier:    replier.NewResult[ibabuza.ApplyResult](),
			status:           status.New(),
			closer:           closer,
		}
		r.status.SetSoftState(raft.SoftState{Lead: 1})
		ch := make(chan error, 1)
		go func() {
			r.applicationServiceStart(context.Background(), time.Millisecond, nil, ch)
		}()
		time.Sleep(time.Millisecond * 100)
		r.resultReplier.SendResult(1, ibabuza.ApplyResult{})
		assert.Nil(t, <-ch)
	})

	t.Run("disableProposalForwarding: true, local peer is not a leader", func(t *testing.T) {
		closer := syncutil.NewCloser()
		trans := &mockTransport{
			mockClient: &mockPubTransClient{},
		}
		r := &Raft{
			idGenerator: &mockIdGenerator{
				id: 1,
			},
			config: BabuzaConfig{
				LocalPeerID: 10,
				RaftConfig: RaftConfig{
					DisableProposalForwarding: true,
				}},
			trans:         trans,
			raftNode:      newMockRaftNode(),
			resultReplier: replier.NewResult[ibabuza.ApplyResult](),
			status:        status.New(),
			closer:        closer,
		}
		r.status.SetSoftState(raft.SoftState{Lead: 1})
		ch := make(chan error, 1)
		go func() {
			r.applicationServiceStart(context.Background(), time.Millisecond, nil, ch)
		}()
		time.Sleep(time.Millisecond * 100)
		r.resultReplier.SendResult(1, ibabuza.ApplyResult{})
		assert.Nil(t, <-ch)
	})

	t.Run("disableProposalForwarding: false, local peer is a leader", func(t *testing.T) {
		closer := syncutil.NewCloser()
		trans := &mockTransport{
			mockClient: &mockPubTransClient{},
		}
		r := &Raft{
			idGenerator: &mockIdGenerator{
				id: 1,
			},
			config: BabuzaConfig{
				LocalPeerID: 1,
				RaftConfig: RaftConfig{
					DisableProposalForwarding: false,
				}},
			metricsCollector: metrics.NewMockMetricsCollector(),
			trans:            trans,
			raftNode:         newMockRaftNode(),
			resultReplier:    replier.NewResult[ibabuza.ApplyResult](),
			status:           status.New(),
			closer:           closer,
		}
		r.status.SetSoftState(raft.SoftState{Lead: 1})
		ch := make(chan error, 1)
		go func() {
			r.applicationServiceStart(context.Background(), time.Millisecond, nil, ch)
		}()
		time.Sleep(time.Millisecond * 100)
		r.resultReplier.SendResult(1, ibabuza.ApplyResult{})
		assert.Nil(t, <-ch)
	})

	t.Run("raft stop", func(t *testing.T) {
		closer := syncutil.NewCloser()
		trans := &mockTransport{
			mockClient: &mockPubTransClient{},
		}
		r := &Raft{
			idGenerator: &mockIdGenerator{
				id: 1,
			},
			config: BabuzaConfig{
				LocalPeerID: 1,
				RaftConfig: RaftConfig{
					DisableProposalForwarding: false,
				}},
			trans:         trans,
			raftNode:      newMockRaftNode(),
			resultReplier: replier.NewResult[ibabuza.ApplyResult](),
			status:        status.New(),
			closer:        closer,
		}
		r.status.SetSoftState(raft.SoftState{Lead: 1})
		ch := make(chan error, 1)
		closer.Close()
		go func() {
			r.applicationServiceStart(context.Background(), time.Millisecond, nil, ch)
		}()
		assert.Equal(t, ErrStopped, <-ch)
	})
}
