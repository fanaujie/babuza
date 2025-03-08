package etcdwal

import (
	"go.etcd.io/etcd/raft/v3/raftpb"
)

type ReplayResult struct {
	metadata  []byte
	hardState raftpb.HardState
	entries   []raftpb.Entry
}

func NewReplayResult(metadata []byte, hardState raftpb.HardState, entries []raftpb.Entry) *ReplayResult {
	return &ReplayResult{
		metadata:  metadata,
		hardState: hardState,
		entries:   entries,
	}
}

func (r *ReplayResult) Metadata() []byte {
	return r.metadata
}
func (r *ReplayResult) HardState() raftpb.HardState {
	return r.hardState
}

func (r *ReplayResult) GetEntries() []raftpb.Entry {
	return r.entries
}

func (r *ReplayResult) ForEachConfChangeEntries(visitor func(raftpb.Entry) error) error {
	var confEntries []raftpb.Entry
	for i := range r.entries {
		e := &r.entries[i]
		if e.Type == raftpb.EntryConfChange {
			confEntries = append(confEntries, *e)
		}
	}
	for i := range confEntries {
		if err := visitor(confEntries[i]); err != nil {
			return err
		}
	}
	return nil
}

func (r *ReplayResult) DeleteUncommittedEntry(commitIndex uint64) error {
	var deleteFrom int
	entsLen := len(r.entries)
	for i := 0; i < entsLen; i++ {
		e := &r.entries[i]
		if e.Index > commitIndex {
			deleteFrom = i
			break
		}
	}
	if deleteFrom != 0 {
		r.entries = r.entries[:deleteFrom]
	}
	return nil
}
