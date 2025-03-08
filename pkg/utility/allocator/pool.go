package allocator

import (
	"fmt"
	"sync"
)

type ByteSlice struct {
	Buffer []byte
}

type levelPool struct {
	levelSize int
	pool      sync.Pool
}

type ByteSlicePool struct {
	pools []*levelPool
}

var defaultByteSlicePool = NewByteSlicePool(64, 16*1024*1024, 2)

func Acquire(byteSize int) *ByteSlice {
	return defaultByteSlicePool.Acquire(byteSize)
}

func Release(b *ByteSlice) {
	defaultByteSlicePool.Release(b)
}

func NewByteSlicePool(minAllocByteSize, maxAllocByteSize int, factor float64) *ByteSlicePool {

	if minAllocByteSize == 0 || maxAllocByteSize == 0 {
		panic(fmt.Errorf("allocator: size is equal 0 (minAllocByteSize=%d, maxAllocByteSize=%d)", minAllocByteSize, maxAllocByteSize))
	}
	if minAllocByteSize > maxAllocByteSize {
		panic(fmt.Errorf("allocator: minAllocByteSize is greater than maxAllocByteSize (minAllocByteSize=%d, maxAllocByteSize=%d)",
			minAllocByteSize, maxAllocByteSize))
	}
	b := &ByteSlicePool{}
	for nextLevelSize := minAllocByteSize; nextLevelSize <= maxAllocByteSize; nextLevelSize = int(float64(nextLevelSize) * factor) {
		b.pools = append(b.pools, &levelPool{levelSize: nextLevelSize})
	}
	return b
}

func (p *ByteSlicePool) Acquire(byteSize int) *ByteSlice {
	if byteSize == 0 {
		panic("allocator: byteSize is equal to 0")
	}
	for _, bag := range p.pools {
		if byteSize <= bag.levelSize {
			v := bag.pool.Get()
			if v == nil {
				return &ByteSlice{Buffer: make([]byte, bag.levelSize)}
			}
			return v.(*ByteSlice)
		}
	}
	return &ByteSlice{Buffer: make([]byte, byteSize)}
}

func (p *ByteSlicePool) Release(b *ByteSlice) {
	capSize := cap(b.Buffer)
	for id, bag := range p.pools {
		if capSize == bag.levelSize {
			b.Buffer = b.Buffer[:bag.levelSize]
			p.pools[id].pool.Put(b)
			return
		}
		continue
	}
	//TODO: log size
}
