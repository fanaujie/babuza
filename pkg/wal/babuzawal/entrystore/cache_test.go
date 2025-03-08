package entrystore

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"testing"
)

func TestCacheAppend(t *testing.T) {

	type testCase struct {
		startPos      uint64
		init          []raftpb.Entry
		appendEntries []raftpb.Entry
		expect        IndexRange
	}

	tc := []testCase{
		{
			expect: IndexRange{
				Empty: true,
			},
		},
		{
			appendEntries: []raftpb.Entry{{Index: 1}, {Index: 2}},
			expect: IndexRange{
				First: 1,
				Last:  2,
			},
		},
		{
			init:          []raftpb.Entry{{Index: 1}, {Index: 2}, {Index: 3}},
			appendEntries: []raftpb.Entry{{Index: 4}, {Index: 5}},
			expect: IndexRange{
				First: 1,
				Last:  5,
			},
		},
		{
			init:          []raftpb.Entry{{Index: 5}, {Index: 6}, {Index: 7}},
			appendEntries: []raftpb.Entry{{Index: 3}, {Index: 4}},
			expect: IndexRange{
				First: 5,
				Last:  7,
			},
		},
		{
			init:          []raftpb.Entry{{Index: 5}, {Index: 6}, {Index: 7}},
			appendEntries: []raftpb.Entry{{Index: 6}, {Index: 7}, {Index: 8}},
			expect: IndexRange{
				First: 5,
				Last:  8,
			},
		},
		{
			startPos:      defaultBuffer - 1,
			init:          []raftpb.Entry{{Index: 5}, {Index: 6}, {Index: 7}},
			appendEntries: []raftpb.Entry{{Index: 6}, {Index: 7}, {Index: 8}},
			expect: IndexRange{
				First: 5,
				Last:  8,
			},
		},
		{
			startPos:      defaultBuffer - 1,
			init:          []raftpb.Entry{{Index: 5}, {Index: 6}, {Index: 7}},
			appendEntries: []raftpb.Entry{{Index: 8}, {Index: 9}, {Index: 10}},
			expect: IndexRange{
				First: 5,
				Last:  10,
			},
		},
		{
			startPos:      defaultBuffer - 1,
			appendEntries: []raftpb.Entry{{Index: 8}, {Index: 9}, {Index: 10}},
			expect: IndexRange{
				First: 8,
				Last:  10,
			},
		},
	}

	for k, c := range tc {
		cache := NewCache()
		cache.consumePos = c.startPos
		cache.appendPos = c.startPos
		if len(c.init) != 0 {
			cache.append(c.init)
		}
		cache.Append(c.appendEntries)
		r := cache.IndexRange()
		assert.Equal(t, r.Empty, c.expect.Empty, fmt.Sprintf("#case%d-expect empty=%v, but real empty=%v testCase={%v}", k, c.expect.Empty, r.Empty, c))
		assert.Equal(t, r.First, c.expect.First, fmt.Sprintf("#case%d-expect consumePos Index=%v, but real consumePos Index=%v testCase={%v}", k, c.expect.First, r.First, c))
		assert.Equal(t, r.Last, c.expect.Last, fmt.Sprintf("#case%d-expect consumePos Index=%v, but real consumePos Index=%v testCase={%v}", k, c.expect.Last, r.Last, c))
	}
}

func TestCacheConsume(t *testing.T) {
	type testCase struct {
		cacheSize      uint64
		startPos       uint64
		init           []raftpb.Entry
		consumeToIndex uint64
		expect         IndexRange
	}

	tc := []testCase{
		{
			expect: IndexRange{
				Empty: true,
			},
		},
		{
			init:           []raftpb.Entry{{Index: 5}, {Index: 6}, {Index: 7}},
			consumeToIndex: 5,
			expect: IndexRange{
				First: 6,
				Last:  7,
			},
		},
		{
			init:           []raftpb.Entry{{Index: 5}, {Index: 6}, {Index: 7}},
			consumeToIndex: 4,
			expect: IndexRange{
				First: 5,
				Last:  7,
			},
		},
		{
			init:           []raftpb.Entry{{Index: 5}, {Index: 6}, {Index: 7}},
			startPos:       defaultBuffer - 1,
			consumeToIndex: 7,
			expect: IndexRange{
				Empty: true,
			},
		},
	}

	for k, c := range tc {
		cache := NewCache()
		cache.consumePos = c.startPos
		cache.appendPos = c.startPos
		if len(c.init) != 0 {
			cache.append(c.init)
		}
		cache.Delete(c.consumeToIndex)
		r := cache.IndexRange()

		if r.Empty != c.expect.Empty {
			assert.Equal(t, r.Empty, c.expect.Empty, fmt.Sprintf("#case%d-expect empty=%v, but real empty=%v testCase={%v}", k, c.expect.Empty, r.Empty, c))
		}
		if r.First != c.expect.First {
			assert.Equal(t, r.First, c.expect.First, fmt.Sprintf("#case%d-expect first=%v, but real first=%v testCase={%v}", k, c.expect.First, r.First, c))
		}
		if r.Last != c.expect.Last {
			assert.Equal(t, r.Last, c.expect.Last, fmt.Sprintf("#case%d-expect last=%v, but real last=%v testCase={%v}", k, c.expect.Last, r.Last, c))
		}
	}
}

func TestCacheReadEntries(t *testing.T) {
	type testCase struct {
		startPos     uint64
		init         []raftpb.Entry
		fetchLoIndex uint64
		fetchHiIndex uint64
		hit          bool
		expect       []raftpb.Entry
	}

	tc := []testCase{
		{
			init:         []raftpb.Entry{{Index: 5}, {Index: 6}, {Index: 7}},
			fetchLoIndex: 1,
			fetchHiIndex: 2,
			hit:          false,
		},
		{
			init:         []raftpb.Entry{{Index: 5}, {Index: 6}, {Index: 7}},
			fetchLoIndex: 1,
			fetchHiIndex: 9,
			hit:          false,
		},
		{
			init:         []raftpb.Entry{{Index: 5}, {Index: 6}, {Index: 7}},
			fetchLoIndex: 5,
			fetchHiIndex: 9,
			hit:          false,
		},
		{
			init:         []raftpb.Entry{{Index: 5}, {Index: 6}, {Index: 7}},
			fetchLoIndex: 5,
			fetchHiIndex: 6,
			expect:       []raftpb.Entry{{Index: 5}, {Index: 6}},
			hit:          true,
		},
		{
			startPos:     defaultBuffer - 1,
			init:         []raftpb.Entry{{Index: 5}, {Index: 6}, {Index: 7}},
			fetchLoIndex: 5,
			fetchHiIndex: 8,
			hit:          false,
		},
		{
			startPos:     defaultBuffer - 3,
			init:         []raftpb.Entry{{Index: 5}, {Index: 6}, {Index: 7}},
			fetchLoIndex: 5,
			fetchHiIndex: 7,
			hit:          true,
			expect:       []raftpb.Entry{{Index: 5}, {Index: 6}, {Index: 7}},
		},
		{
			startPos:     defaultBuffer - 3,
			init:         []raftpb.Entry{{Index: 5}, {Index: 6}, {Index: 7}},
			fetchLoIndex: 5,
			fetchHiIndex: 6,
			hit:          true,
			expect:       []raftpb.Entry{{Index: 5}, {Index: 6}},
		},
		{
			startPos:     defaultBuffer - 3,
			init:         []raftpb.Entry{{Index: 5}, {Index: 6}, {Index: 7}},
			fetchLoIndex: 5,
			fetchHiIndex: 5,
			hit:          true,
			expect:       []raftpb.Entry{{Index: 5}},
		},
	}
	for k, c := range tc {
		cache := NewCache()
		cache.consumePos = c.startPos
		cache.appendPos = c.startPos
		if len(c.init) != 0 {
			cache.append(c.init)
		}
		readLen := c.fetchHiIndex - c.fetchLoIndex + 1
		out := make([]raftpb.Entry, readLen)
		for i := uint64(0); i < readLen; i++ {
			out[i].Index = c.fetchLoIndex
			c.fetchLoIndex++
		}
		assert.Equal(t, c.hit, cache.ReadEntriesData(out), fmt.Sprintf("#case%d-expect hit=%v, but real hit=%v testCase={%v}", k, c.expect, out, c))
		if c.expect != nil {
			assert.Equal(t, c.expect, out, fmt.Sprintf("#case%d-expect read entries=%v, but real entries=%v testCase={%v}", k, c.expect, out, c))
		}
	}
}

func TestCacheGrowBuffer(t *testing.T) {
	type testCase struct {
		startPos uint64
		growMax  uint64
	}

	var tc = []testCase{
		//{
		//	startPos: 0,
		//	growMax:  1,
		//},
		//{
		//	startPos: defaultBuffer-1,
		//	growMax:  1,
		//},
		{
			startPos: defaultBuffer - (defaultBuffer / 2),
			growMax:  2,
		},
	}
	for _, c := range tc {
		cache := NewCache()
		cache.consumePos = c.startPos
		cache.appendPos = c.startPos

		var i, count uint64
		endPos := defaultBuffer << c.growMax
		for ; count < c.growMax; count++ {
			for ; i < endPos-1; i++ {
				cache.append([]raftpb.Entry{{Index: i + 1}})
			}
		}
		assert.Equal(t, uint64(len(cache.ringBuf)), endPos, fmt.Sprintf("expect lenth of cache.ringBuf=%v, but real lenth of cache.ringBuf=%v testCase={%v}", endPos, len(cache.ringBuf), c))
		assert.Equal(t, uint64(cap(cache.ringBuf)), endPos, fmt.Sprintf("expect capacity of cache.ringBuf=%v, but real lenth of cache.ringBuf=%v testCase={%v}", endPos, cap(cache.ringBuf), c))
		for i = 1; i < endPos; i++ {
			assert.Equal(t, i, cache.ringBuf[cache.consumePos].Index, fmt.Sprintf("at consume pos=%v expect Index %v, but real Index=%v testCase={%v}", cache.consumePos, i, cache.ringBuf[cache.consumePos].Index, c))
			cache.consumePos++
		}
	}

}
