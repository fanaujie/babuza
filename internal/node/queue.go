package node

import (
	"context"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"sync"
)

type ProposeConfChangeMessage struct {
	ConfChange raftpb.ConfChangeI
	Ctx        context.Context
	ResultCh   chan error
}

type ApplyConfChangeMessage struct {
	ConfChange raftpb.ConfChangeI
	ResultCh   chan raftpb.ConfState
}

type ProposalMessage struct {
	Command  []byte
	Ctx      context.Context
	ResultCh chan error
}

type ReportSnapshotMessage struct {
	Id     uint64
	Status raft.SnapshotStatus
}

type TransferLeaderMessage struct {
	Lead       uint64
	Transferee uint64
}

type Value interface {
	ProposeConfChangeMessage | raftpb.MessageType | ApplyConfChangeMessage | ProposalMessage | raftpb.Message |
	[]byte | ReportSnapshotMessage | uint64 | chan raft.Status | TransferLeaderMessage
}

type Queue[V Value] struct {
	bufferA   []V
	bufferB   []V
	writeBuf  []V
	size      uint64
	tail      uint64
	isTargetA bool
	stop      bool
	mu        sync.Mutex
}

func NewQueue[V Value](size uint64) *Queue[V] {
	p := &Queue[V]{
		size:      size,
		bufferA:   make([]V, size),
		bufferB:   make([]V, size),
		isTargetA: true,
	}
	p.writeBuf = p.bufferA
	return p
}

func (p *Queue[V]) Stop() {
	p.mu.Lock()
	p.stop = true
	p.mu.Unlock()
}

func (p *Queue[V]) Put(element V) (retry bool, stopped bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stop {
		stopped = true
		return
	}
	retry = p.add(element)
	return
}

func (p *Queue[V]) Get() (result []V, stopped bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stop {
		stopped = true
		return
	}
	if p.tail == 0 {
		return
	}
	result = p.readAndSwap()
	return
}

func (p *Queue[V]) add(msg V) (retry bool) {
	if p.tail == p.size {
		retry = true
		return
	}
	p.writeBuf[p.tail] = msg
	p.tail++
	return false
}

func (p *Queue[V]) readAndSwap() []V {
	readBuf := p.bufferA
	if p.isTargetA {
		p.writeBuf = p.bufferB
	} else {
		readBuf = p.bufferB
		p.writeBuf = p.bufferA
	}
	p.isTargetA = !p.isTargetA
	readSize := p.tail
	p.tail = 0
	return readBuf[:readSize]
}
