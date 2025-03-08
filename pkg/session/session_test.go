package session

import (
	"errors"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"os"
	"testing"
)

func TestAddAppliedResult(t *testing.T) {
	s1 := NewSession(1, 0)
	assert.Nil(t, s1.AddResult(1, 10, ibabuza.ApplyResult{
		LogIndex: 100,
		Response: 100,
	}))
	assert.Equal(t, int64(10), s1.lastActiveNanoseconds)
	ar, ok := s1.result[1]
	assert.Equal(t, true, ok)
	assert.Equal(t, uint64(100), ar.LogIndex)
	assert.Equal(t, 100, ar.Response.(int))

	assert.Error(t, s1.AddResult(1, 10, ibabuza.ApplyResult{
		LogIndex: 100,
		Response: 100,
	}))
}

func TestRepeatSequenceNum(t *testing.T) {
	s1 := NewSession(1, 0)
	assert.Nil(t, s1.AddResult(2, 10, ibabuza.ApplyResult{
		LogIndex: 100,
		Response: 100,
	}))
	assert.Equal(t, false, s1.RepeatSequenceNum(1))
	assert.Equal(t, true, s1.RepeatSequenceNum(2))
	assert.Equal(t, false, s1.RepeatSequenceNum(3))
	assert.Nil(t, s1.AddResult(3, 10, ibabuza.ApplyResult{
		LogIndex: 100,
		Response: 100,
	}))
	assert.Equal(t, true, s1.RepeatSequenceNum(3))
}

func TestGetAppliedResult(t *testing.T) {
	s1 := NewSession(1, 0)

	_, ok := s1.GetResult(1)
	assert.Equal(t, false, ok)
	assert.Nil(t, s1.AddResult(2, 10, ibabuza.ApplyResult{
		LogIndex: 100,
		Response: 100,
	}))
	_, ok = s1.GetResult(2)
	assert.Equal(t, true, ok)

	assert.Nil(t, s1.AddResult(3, 10, ibabuza.ApplyResult{
		LogIndex: 100,
		Response: errors.New("error"),
	}))
	ar, ok := s1.GetResult(3)
	assert.Equal(t, true, ok)
	assert.Equal(t, "error", ar.Response.(error).Error())
}

func TestClearResult(t *testing.T) {
	s1 := NewSession(1, 0)
	assert.Nil(t, s1.AddResult(1, 10, ibabuza.ApplyResult{
		LogIndex: 100,
		Response: 100,
	}))
	assert.Nil(t, s1.AddResult(2, 11, ibabuza.ApplyResult{
		LogIndex: 101,
		Response: 100,
	}))
	assert.Nil(t, s1.AddResult(3, 12, ibabuza.ApplyResult{
		LogIndex: 102,
		Response: 100,
	}))
	assert.Nil(t, s1.AddResult(4, 13, ibabuza.ApplyResult{
		LogIndex: 103,
		Response: 100,
	}))

	s1.ClearResult(3)
	assert.Equal(t, 2, len(s1.result))
	_, ok := s1.result[3]
	assert.Equal(t, true, ok)
	_, ok = s1.result[4]
	assert.Equal(t, true, ok)
}

func TestSnapshotAndRestore(t *testing.T) {

	ars := newApplyResultSerializer(&mockJsonResultSerializer{})
	s1 := NewSession(1, 0)
	ar1 := ibabuza.ApplyResult{
		LogIndex: 1,
		Response: &mockResponseA{Value: 10},
	}
	assert.Nil(t, s1.AddResult(1, 10, ar1))
	ar2 := ibabuza.ApplyResult{
		LogIndex: 2,
		Response: &mockResponseB{Value: "hello"},
	}
	assert.Nil(t, s1.AddResult(2, 11, ar2))
	ar3 := ibabuza.ApplyResult{
		LogIndex: 3,
		Response: errors.New("error"),
	}
	assert.Nil(t, s1.AddResult(3, 12, ar3))
	ar4 := ibabuza.ApplyResult{
		LogIndex: 4,
	}
	assert.Nil(t, s1.AddResult(4, 13, ar4))

	w, err := ioutil.TempFile("", "")
	assert.Nil(t, err)
	assert.Nil(t, s1.Snapshot(w, ars))
	assert.Nil(t, w.Close())

	r, err := os.Open(w.Name())
	assert.Nil(t, err)
	s := NewSession(0, 0)
	assert.Nil(t, s.Restore(r, ars))
	assert.Equal(t, uint64(1), s.id)
	assert.Equal(t, int64(13), s.lastActiveNanoseconds)

	assert.Equal(t, 4, len(s.result))

	res := s.result[1].Response
	assert.Equal(t, uint64(1), s.result[1].LogIndex)
	assert.Equal(t, 10, res.(*mockResponseA).Value)

	res = s.result[2].Response
	assert.Equal(t, uint64(2), s.result[2].LogIndex)
	assert.Equal(t, "hello", res.(*mockResponseB).Value)

	assert.Equal(t, uint64(3), s.result[3].LogIndex)
	assert.Equal(t, "error", s.result[3].Response.(error).Error())

	assert.Equal(t, uint64(4), s.result[4].LogIndex)
	assert.Nil(t, s.result[4].Response)
}
