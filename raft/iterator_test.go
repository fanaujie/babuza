package raft

import (
    "github.com/fanaujie/babuza/ibabuza"
    "github.com/fanaujie/babuza/ibabuza/babuzapb"
    "github.com/stretchr/testify/assert"
    "go.etcd.io/etcd/raft/v3/raftpb"
    "testing"
)

type mockApplier struct {
	newTerm struct {
		term  uint64
		index uint64
	}
	applyEntryIndex []uint64
	removeId        uint64
	sendRes         map[uint64]ibabuza.ApplyResult
}

func (m *mockApplier) ApplyNilEntryInNewTerm(index uint64, term uint64) {
	m.newTerm.term = term
	m.newTerm.index = index
}
func (m *mockApplier) ApplyNormalEntry(e raftpb.Entry) ibabuza.Entry {
	s := &mockSession{}
	for _, ex := range m.applyEntryIndex {
		if ex == e.Index {
			return &Entry{
				term:       e.Term,
				index:      e.Index,
				command:    e.Data,
				session:    s,
				sendResult: m,
			}
		}
	}
	return nil
}

func (m *mockApplier) ApplyConfChangeEntry(e raftpb.Entry) bool {
	var req = babuzapb.ConfChangeRequest{}
	_ = req.Unmarshal(e.Data)

	return m.removeId == req.RaftPeerAttr.Id
}

func (m *mockApplier) SendStateMachineAppliedResult(e *Entry, ar ibabuza.ApplyResult) {
	if m.sendRes == nil {
		m.sendRes = make(map[uint64]ibabuza.ApplyResult)
	}
	m.sendRes[e.index] = ar
}

func TestIterator_EmptyContent(t *testing.T) {
	m := mockApplier{}
	it := newIterator(&m)
	it.SetEntries([]raftpb.Entry{})
	assert.Nil(t, it.Next())
	assert.Equal(t, false, it.HasRemovedSelf())
}

func TestIterator_ApplyNilEntryInNewTerm(t *testing.T) {
	m := mockApplier{}
	it := newIterator(&m)
	it.SetEntries([]raftpb.Entry{
		{
			Term:  2,
			Index: 10,
			Type:  raftpb.EntryNormal,
		},
	})
	assert.Nil(t, it.Next())
	assert.Equal(t, false, it.HasRemovedSelf())
	assert.Equal(t, uint64(2), m.newTerm.term)
	assert.Equal(t, uint64(10), m.newTerm.index)
}

func TestIterator_ApplyNormalEntry(t *testing.T) {
	m := mockApplier{}
	it := newIterator(&m)
	it.SetEntries([]raftpb.Entry{
		{
			Term:  1,
			Index: 1,
			Type:  raftpb.EntryNormal,
			Data:  []byte{1},
		},
		{
			Term:  1,
			Index: 2,
			Type:  raftpb.EntryNormal,
			Data:  []byte{2},
		},
	})
	m.applyEntryIndex = append(m.applyEntryIndex, 1)
	m.applyEntryIndex = append(m.applyEntryIndex, 2)

	entry := it.Next()
	assert.NotNil(t, entry)
	assert.Equal(t, uint64(1), entry.Term())
	assert.Equal(t, uint64(1), entry.Index())
	assert.Equal(t, []byte{1}, entry.Command())
	entry.SendResponse(int64(100))

	assert.Equal(t, int64(100), m.sendRes[1].Response.(int64))
	entry = it.Next()
	assert.NotNil(t, entry)
	assert.Equal(t, uint64(1), entry.Term())
	assert.Equal(t, uint64(2), entry.Index())
	assert.Equal(t, []byte{2}, entry.Command())
	entry.SendResponse("hello")
	assert.Equal(t, "hello", m.sendRes[2].Response.(string))
	assert.Nil(t, it.Next())
}

func TestIterator_RemoveSelf(t *testing.T) {
	m := mockApplier{}
	it := newIterator(&m)
	req := babuzapb.ConfChangeRequest{
		RaftPeerAttr: babuzapb.RaftPeerAttribute{
			Id: 3,
		},
	}
	data, err := req.Marshal()
	assert.Nil(t, err)
	data, err = req.Marshal()
	assert.Nil(t, err)
	it.SetEntries([]raftpb.Entry{
		{
			Term:  1,
			Index: 1,
			Type:  raftpb.EntryNormal,
			Data:  []byte{1},
		},
		{
			Term:  1,
			Index: 2,
			Type:  raftpb.EntryNormal,
			Data:  []byte{2},
		},
		{
			Term:  1,
			Index: 3,
			Type:  raftpb.EntryConfChange,
			Data:  data,
		},
		{
			Term:  1,
			Index: 4,
			Type:  raftpb.EntryNormal,
			Data:  []byte{4},
		},
	})
	m.removeId = 3
	m.applyEntryIndex = append(m.applyEntryIndex, 1)
	m.applyEntryIndex = append(m.applyEntryIndex, 2)
	m.applyEntryIndex = append(m.applyEntryIndex, 4)

	e := it.Next()
	assert.NotNil(t, e)
	assert.Equal(t, false, it.HasRemovedSelf())
	e = it.Next()
	assert.NotNil(t, e)
	assert.Equal(t, false, it.HasRemovedSelf())
	assert.Nil(t, it.Next())
	assert.Equal(t, true, it.HasRemovedSelf())
	assert.Equal(t, nil, it.Next()) // index 4
	assert.Equal(t, true, it.HasRemovedSelf())
}

func TestIterator_RemoveOthers(t *testing.T) {
	m := mockApplier{}
	it := newIterator(&m)
	req := babuzapb.ConfChangeRequest{
		RaftPeerAttr: babuzapb.RaftPeerAttribute{
			Id: 3,
		},
	}
	data, err := req.Marshal()
	assert.Nil(t, err)
	it.SetEntries([]raftpb.Entry{
		{
			Term:  1,
			Index: 1,
			Type:  raftpb.EntryNormal,
			Data:  []byte{1},
		},
		{
			Term:  1,
			Index: 2,
			Type:  raftpb.EntryNormal,
			Data:  []byte{2},
		},
		{
			Term:  1,
			Index: 3,
			Type:  raftpb.EntryConfChange,
			Data:  data,
		},
		{
			Term:  1,
			Index: 4,
			Type:  raftpb.EntryNormal,
			Data:  []byte{4},
		},
	})
	m.applyEntryIndex = append(m.applyEntryIndex, 2)
	m.applyEntryIndex = append(m.applyEntryIndex, 4)

	entry := it.Next()
	assert.NotNil(t, entry)
	assert.Equal(t, false, it.HasRemovedSelf())
	assert.Equal(t, uint64(1), entry.Term())
	assert.Equal(t, uint64(2), entry.Index())
	assert.Equal(t, []byte{2}, entry.Command())
	entry = it.Next()
	assert.NotNil(t, entry)
	assert.Equal(t, uint64(1), entry.Term())
	assert.Equal(t, uint64(4), entry.Index())
	assert.Equal(t, []byte{4}, entry.Command())
	assert.Equal(t, false, it.HasRemovedSelf())
}
