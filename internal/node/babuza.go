package node

import (
	"context"
	"errors"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"runtime"
	"sync/atomic"
)

var (
	ErrStopped             = errors.New("")
	ErrRequestQueueStopped = errors.New("")
)

type Config struct {
	Peers   []raft.Peer
	RaftCfg *raft.Config
}

type RaftNode struct {
	tick uint64 // must use atomic operations to access; keep 64-bit aligned.

	rn                 *raft.RawNode
	proposal           *Queue[ProposalMessage]
	proposalConfChange *Queue[ProposeConfChangeMessage]
	applyConfChange    *Queue[ApplyConfChangeMessage]
	step               *Queue[raftpb.Message]
	campaign           *Queue[raftpb.MessageType]
	transferLeader     *Queue[TransferLeaderMessage]
	readIndex          *Queue[[]byte]
	unreachable        *Queue[uint64]
	reportSnapshot     *Queue[ReportSnapshotMessage]
	status             *Queue[chan raft.Status]

	readyCh       chan raft.Ready
	advanceCh     chan struct{}
	dirtyCh       chan struct{}
	stopCh        chan struct{}
	stoppedDoneCh chan struct{}
}

func StartNode(c Config) *RaftNode {
	if len(c.Peers) == 0 {
		panic("no peers given; use RestartNode instead")
	}
	rn, err := raft.NewRawNode(c.RaftCfg)
	if err != nil {
		panic(err)
	}
	if err = rn.Bootstrap(c.Peers); err != nil {
		panic(err)
	}
	n := newNode(rn)
	go n.run()
	return n
}

func RestartNode(c Config) *RaftNode {
	rn, err := raft.NewRawNode(c.RaftCfg)
	if err != nil {
		panic(err)
	}
	n := newNode(rn)
	go n.run()
	return nil
}

func newNode(rn *raft.RawNode) *RaftNode {

	return &RaftNode{
		tick:               0,
		rn:                 rn,
		proposal:           NewQueue[ProposalMessage](1024),
		proposalConfChange: NewQueue[ProposeConfChangeMessage](8),
		applyConfChange:    NewQueue[ApplyConfChangeMessage](8),
		step:               NewQueue[raftpb.Message](1024),
		campaign:           NewQueue[raftpb.MessageType](8),
		transferLeader:     NewQueue[TransferLeaderMessage](8),
		readIndex:          NewQueue[[]byte](64),
		unreachable:        NewQueue[uint64](16),
		reportSnapshot:     NewQueue[ReportSnapshotMessage](16),
		status:             NewQueue[chan raft.Status](8),
		readyCh:            make(chan raft.Ready),
		advanceCh:          make(chan struct{}),
		dirtyCh:            make(chan struct{}, 1),
		stopCh:             make(chan struct{}),
		stoppedDoneCh:      make(chan struct{}),
	}

}

func (n *RaftNode) Tick() {
	atomic.AddUint64(&n.tick, 1)
}

func (n *RaftNode) Campaign(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			retry, stop := n.campaign.Put(raftpb.MsgHup)
			if stop {
				return ErrRequestQueueStopped
			}
			if !retry {
				return nil
			}
			runtime.Gosched()
		}
	}
}

func (n *RaftNode) Propose(ctx context.Context, command []byte) error {
	pm := ProposalMessage{
		Command:  command,
		Ctx:      ctx,
		ResultCh: make(chan error, 1),
	}
Retry:
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			retry, stop := n.proposal.Put(pm)
			if stop {
				return ErrRequestQueueStopped
			}
			if !retry {
				break Retry
			}
			runtime.Gosched()
		}
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-pm.ResultCh:
		return err
	case <-n.stoppedDoneCh:
		return ErrStopped
	}
}

func (n *RaftNode) ProposeConfChange(ctx context.Context, cc raftpb.ConfChangeI) error {
	pm := ProposeConfChangeMessage{
		ConfChange: cc,
		Ctx:        ctx,
		ResultCh:   make(chan error, 1),
	}
Retry:
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			retry, stop := n.proposalConfChange.Put(pm)
			if stop {
				return ErrRequestQueueStopped
			}
			if !retry {
				break Retry
			}
			runtime.Gosched()
		}
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-pm.ResultCh:
		return err
	case <-n.stoppedDoneCh:
		return ErrStopped
	}
}

func (n *RaftNode) Step(ctx context.Context, msg raftpb.Message) error {

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			retry, stop := n.step.Put(msg)
			if stop {
				return ErrRequestQueueStopped
			}
			if !retry {
				return nil
			}
			runtime.Gosched()
		}
	}
}

func (n *RaftNode) Ready() <-chan raft.Ready {
	return n.readyCh
}

func (n *RaftNode) Advance() {
	select {
	case n.advanceCh <- struct{}{}:
	case <-n.stoppedDoneCh:
	}
}

func (n *RaftNode) ApplyConfChange(cc raftpb.ConfChangeI) *raftpb.ConfState {
	var state raftpb.ConfState
	pm := ApplyConfChangeMessage{
		ConfChange: cc.AsV2(),
		ResultCh:   make(chan raftpb.ConfState),
	}

Retry:
	for {
		retry, stop := n.applyConfChange.Put(pm)
		if stop {
			return &state
		}
		if !retry {
			break Retry
		}
		runtime.Gosched()
	}
	select {
	case state = <-pm.ResultCh:
		return &state
	case <-n.stoppedDoneCh:
		return &state
	}
}

func (n *RaftNode) TransferLeadership(ctx context.Context, lead, transferee uint64) {
	pm := TransferLeaderMessage{
		Lead:       lead,
		Transferee: transferee,
	}
	for {
		select {
		case <-ctx.Done():
			return
		default:
			retry, stop := n.transferLeader.Put(pm)
			if stop {
				return
			}
			if !retry {
				return
			}
			runtime.Gosched()
		}
	}
}

func (n *RaftNode) ReadIndex(ctx context.Context, rctx []byte) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			retry, stop := n.readIndex.Put(rctx)
			if stop {
				return ErrRequestQueueStopped
			}
			if !retry {
				return nil
			}
			runtime.Gosched()
		}
	}
}

func (n *RaftNode) Status() raft.Status {
	resultCh := make(chan raft.Status)
Retry:
	for {
		retry, stop := n.status.Put(resultCh)
		if stop {
			return raft.Status{}
		}
		if !retry {
			break Retry
		}
		runtime.Gosched()
	}
	select {
	case state := <-resultCh:
		return state
	case <-n.stoppedDoneCh:
		return raft.Status{}
	}
}

func (n *RaftNode) ReportUnreachable(id uint64) {
	for {
		retry, stop := n.unreachable.Put(id)
		if stop {
			return
		}
		if !retry {
			return
		}
		runtime.Gosched()
	}
}

func (n *RaftNode) ReportSnapshot(id uint64, status raft.SnapshotStatus) {
	pm := ReportSnapshotMessage{
		Id:     id,
		Status: status,
	}
	for {
		retry, stop := n.reportSnapshot.Put(pm)
		if stop {
			return
		}
		if !retry {
			return
		}
		runtime.Gosched()
	}
}

// Stop performs any necessary termination of the Node.
func (n *RaftNode) Stop() {
	select {
	case n.stopCh <- struct{}{}:
	case <-n.stoppedDoneCh:
		return
	}
	<-n.stoppedDoneCh
}

func (n *RaftNode) run() {

	var rd raft.Ready
	var advanceCh chan struct{}
	var readyCh chan raft.Ready

	for {
		n.processTick(atomic.SwapUint64(&n.tick, 0))
		if err := n.processAllRequest(); err != nil {
			return
		}
		if advanceCh == nil && readyCh == nil && n.rn.HasReady() {
			rd = n.rn.Ready()
			readyCh = n.readyCh
		}
		select {
		case <-n.stopCh:
			n.stopAllRequest()
			close(n.stoppedDoneCh)
			return
		case readyCh <- rd:
			readyCh = nil
			advanceCh = n.advanceCh
		case <-advanceCh:
			n.rn.Advance(rd)
			advanceCh = nil
			rd = raft.Ready{}
		default:
		}
		runtime.Gosched()
	}
}

func (n *RaftNode) processAllRequest() error {
	if err := n.processProposal(); err != nil {
		return err
	}
	if err := n.processStep(); err != nil {
		return err
	}
	if err := n.processProposalConfChange(); err != nil {
		return err
	}
	if err := n.processApplyConfChange(); err != nil {
		return err
	}
	if err := n.processReadIndex(); err != nil {
		return err
	}
	if err := n.processReportSnapshot(); err != nil {
		return err
	}
	if err := n.processReportUnreachable(); err != nil {
		return err
	}
	if err := n.processTransferLeader(); err != nil {
		return err
	}
	if err := n.processCampaign(); err != nil {
		return err
	}
	if err := n.processStatus(); err != nil {
		return err
	}
	return nil
}

func (n *RaftNode) stopAllRequest() {
	n.proposal.Stop()
	n.proposalConfChange.Stop()
	n.applyConfChange.Stop()
	n.step.Stop()
	n.campaign.Stop()
	n.transferLeader.Stop()
	n.readIndex.Stop()
	n.unreachable.Stop()
	n.reportSnapshot.Stop()
	n.status.Stop()
}

func (n *RaftNode) processTick(tick uint64) {
	for i := uint64(0); i < tick; i++ {
		n.rn.Tick()
	}
}

func (n *RaftNode) processCampaign() error {
	campaign, stop := n.campaign.Get()
	if stop {
		return ErrRequestQueueStopped
	}
	for _ = range campaign {
		if err := n.rn.Campaign(); err != nil {
			return err
		}
	}
	return nil
}

func (n *RaftNode) processProposal() error {
	proposal, stop := n.proposal.Get()
	if stop {
		return ErrRequestQueueStopped
	}
	for i := range proposal {
		pm := &proposal[i]
		err := n.rn.Propose(pm.Command)
		if pm.ResultCh != nil {
			pm.ResultCh <- err
		}
		//gc
		pm.Ctx = nil
		pm.Command = nil
		pm.ResultCh = nil
	}
	return nil
}
func (n *RaftNode) processProposalConfChange() error {
	proposal, stop := n.proposalConfChange.Get()
	if stop {
		return ErrRequestQueueStopped
	}
	for i := range proposal {
		pm := &proposal[i]
		err := n.rn.ProposeConfChange(pm.ConfChange)
		if pm.ResultCh != nil {
			pm.ResultCh <- err
		}
		//gc
		pm.ConfChange = nil
		pm.ResultCh = nil
	}
	return nil
}

func (n *RaftNode) processApplyConfChange() error {
	proposal, stop := n.applyConfChange.Get()
	if stop {
		return ErrRequestQueueStopped
	}
	for i := range proposal {
		pm := &proposal[i]
		cs := n.rn.ApplyConfChange(pm.ConfChange)
		if pm.ResultCh != nil {
			pm.ResultCh <- *cs
		}
		//gc
		pm.ConfChange = nil
		pm.ResultCh = nil
	}
	return nil
}

func (n *RaftNode) processStep() error {
	step, stop := n.step.Get()
	if stop {
		return ErrRequestQueueStopped
	}
	var err error
	for i := range step {
		s := &step[i]
		if err == nil {
			err = n.rn.Step(*s)
		}
		//gc
		s.Entries = nil
		s.Context = nil
	}
	return err
}

func (n *RaftNode) processTransferLeader() error {
	transferLeader, stop := n.transferLeader.Get()
	if stop {
		return ErrRequestQueueStopped
	}
	for _, t := range transferLeader {
		n.rn.TransferLeader(t.Transferee)
	}
	return nil
}

func (n *RaftNode) processReadIndex() error {
	readIndex, stop := n.readIndex.Get()
	if stop {
		return ErrRequestQueueStopped
	}
	for i := range readIndex {
		n.rn.ReadIndex(readIndex[i])
		readIndex[i] = nil
	}
	return nil
}

func (n *RaftNode) processStatus() error {
	status, stop := n.status.Get()
	if stop {
		return ErrRequestQueueStopped
	}
	if len(status) > 0 {
		s := n.rn.Status()
		for i := range status {
			status[i] <- s
			status[i] = nil
		}
	}
	return nil
}

func (n *RaftNode) processReportUnreachable() error {
	unreachable, stop := n.unreachable.Get()
	if stop {
		return ErrRequestQueueStopped
	}
	for _, id := range unreachable {
		n.rn.ReportUnreachable(id)
	}
	return nil
}

func (n *RaftNode) processReportSnapshot() error {
	reportSnapshot, stop := n.reportSnapshot.Get()
	if stop {
		return ErrRequestQueueStopped
	}
	for i := range reportSnapshot {
		m := &reportSnapshot[i]
		n.rn.ReportSnapshot(m.Id, m.Status)
	}
	return nil
}
