package node

import (
	"context"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"math"
	"testing"
	"time"
)

type benchReady struct {
	ticker         *time.Ticker
	ms             *raft.MemoryStorage
	n              raft.Node
	b              *testing.B
	stopCh         chan struct{}
	notifyLeaderCh chan uint64
	leader         uint64
}

func newBenchReady(ms *raft.MemoryStorage, n raft.Node, b *testing.B) *benchReady {
	return &benchReady{
		ticker:         time.NewTicker(100 * time.Millisecond),
		ms:             ms,
		n:              n,
		b:              b,
		stopCh:         make(chan struct{}),
		notifyLeaderCh: make(chan uint64),
	}
}
func (br *benchReady) stop() {
	br.stopCh <- struct{}{}
}
func (br *benchReady) run() {
	for {
		select {
		//case <-br.stopCh:
		//	return
		case <-br.ticker.C:
			br.n.Tick()
		case rd := <-br.n.Ready():
			if rd.SoftState != nil {
				if rd.SoftState.Lead != br.leader {
					br.leader = rd.SoftState.Lead
					br.notifyLeaderCh <- br.leader
				}
			}
			br.ms.Append(rd.Entries)
			// a reasonable disk sync latency
			time.Sleep(1 * time.Millisecond)
			if len(rd.CommittedEntries) > 0 {
				if rd.CommittedEntries[0].Type == raftpb.EntryConfChange {
					var cc raftpb.ConfChange
					cc.Unmarshal(rd.CommittedEntries[0].Data)
					br.n.ApplyConfChange(cc)
				}
			}
			br.n.Advance()
			if rd.HardState.Commit == uint64(br.b.N+2) {
				return
			}
		}
	}
}

func BenchmarkOneNode_BabuzaNode_Propose(b *testing.B) {
	ms := raft.NewMemoryStorage()
	cfg := raft.Config{
		ID:              1,
		ElectionTick:    5,
		HeartbeatTick:   1,
		Storage:         ms,
		MaxSizePerMsg:   math.MaxUint64,
		MaxInflightMsgs: 256,
	}
	n := StartNode(Config{
		Peers:   []raft.Peer{{ID: 1}},
		RaftCfg: &cfg,
	})
	ctx := context.TODO()
	br := newBenchReady(ms, n, b)
	defer n.Stop()
	go func() {
		<-br.notifyLeaderCh
		br.b.ResetTimer()
		for i := 0; i < b.N; i++ {
			n.Propose(ctx, []byte("foo"))
		}
	}()
	br.run()
}

func BenchmarkOneNode_ETCDNode_Propose(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ms := raft.NewMemoryStorage()
	cfg := &raft.Config{
		ID:              1,
		ElectionTick:    5,
		HeartbeatTick:   1,
		Storage:         ms,
		MaxSizePerMsg:   math.MaxUint64,
		MaxInflightMsgs: 256,
	}
	n := raft.StartNode(cfg, []raft.Peer{{ID: 1}})
	br := newBenchReady(ms, n, b)
	defer n.Stop()
	go func() {
		<-br.notifyLeaderCh
		br.b.ResetTimer()
		for i := 0; i < b.N; i++ {
			n.Propose(ctx, []byte("foo"))
		}
	}()
	br.run()
}
