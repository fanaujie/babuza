package raft

import (
	"github.com/fanaujie/babuza/ibabuza"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

type Applier interface {
	ApplyNilEntryInNewTerm(uint64, uint64)
	ApplyNormalEntry(raftpb.Entry) ibabuza.Entry
	ApplyConfChangeEntry(raftpb.Entry) bool
	SendStateMachineAppliedResult(e *Entry, ar ibabuza.ApplyResult)
}

type iterator struct {
	applier    Applier
	entries    []raftpb.Entry
	removeSelf bool
	pos        int
}

func newIterator(a Applier) *iterator {
	return &iterator{
		applier: a,
	}
}

func (it *iterator) SetEntries(entries []raftpb.Entry) {
	it.entries = entries
	it.pos = -1
}

func (it *iterator) ReleaseEntries() {
	it.entries = nil
	it.pos = -1
	it.removeSelf = false
}

func (it *iterator) HasRemovedSelf() bool {
	return it.removeSelf
}

func (it *iterator) Next() ibabuza.Entry {
	for it.pos++; it.pos < len(it.entries); it.pos++ {
		if it.removeSelf {
			return nil
		}
		entry := it.entries[it.pos]
		if len(entry.Data) == 0 {
			it.applier.ApplyNilEntryInNewTerm(entry.Index, entry.Term)
		} else {
			switch entry.Type {
			case raftpb.EntryNormal:
				stateMachineEntry := it.applier.ApplyNormalEntry(entry)
				if stateMachineEntry != nil {
					return stateMachineEntry
				}
			case raftpb.EntryConfChange:
				it.removeSelf = it.applier.ApplyConfChangeEntry(entry)
				if it.removeSelf {
					return nil
				}
			default:
				//fs.lg.Panic("[server] not support raft toApplyEntry type",zap.Uint64("type", uint64(e.Type)))
			}

		}
	}
	return nil
}
