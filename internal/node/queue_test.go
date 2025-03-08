package node

import (
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"testing"
)

func TestNewQueue(t *testing.T) {
	q1 := NewQueue[ProposeConfChangeMessage](10)
	assert.NotNil(t, q1)

	q2 := NewQueue[raftpb.MessageType](10)
	assert.NotNil(t, q2)

	q3 := NewQueue[ApplyConfChangeMessage](10)
	assert.NotNil(t, q3)

	q4 := NewQueue[ProposalMessage](10)
	assert.NotNil(t, q4)

	q5 := NewQueue[raftpb.Message](10)
	assert.NotNil(t, q5)

	q6 := NewQueue[[]byte](10)
	assert.NotNil(t, q6)

	q7 := NewQueue[ReportSnapshotMessage](10)
	assert.NotNil(t, q7)

	q8 := NewQueue[uint64](10)
	assert.NotNil(t, q8)

	q9 := NewQueue[chan raft.Status](10)
	assert.NotNil(t, q9)

	q10 := NewQueue[TransferLeaderMessage](10)
	assert.NotNil(t, q10)
}

func TestQueuePut(t *testing.T) {
	testSize := uint64(8)
	q := NewQueue[uint64](testSize)
	for i := uint64(0); i < testSize; i++ {
		retry, stop := q.Put(1)
		assert.Equal(t, false, retry)
		assert.Equal(t, false, stop)
		assert.Equal(t, true, q.isTargetA)
		assert.Equal(t, i+1, q.tail)
	}
	retry, stop := q.Put(testSize + 1)
	assert.Equal(t, false, stop)
	assert.Equal(t, true, retry)
}

func TestQueueGet(t *testing.T) {
	testSize := uint64(3)
	q := NewQueue[uint64](testSize)
	for i := uint64(0); i < testSize; i++ {
		q.Put(i)
	}
	result, stop := q.Get()
	assert.Equal(t, int(testSize), len(result))
	assert.Equal(t, false, stop)
	assert.Equal(t, false, q.isTargetA)
	for i, v := range result {
		assert.Equal(t, uint64(i), v)
	}
}

func TestQueueStop(t *testing.T) {

	q := NewQueue[uint64](1)
	q.Stop()
	_, stop := q.Put(1)
	assert.Equal(t, true, stop)
	_, stop = q.Get()
	assert.Equal(t, true, stop)
}
