// Copyright 2025 Chen Chunchieh <junjie725@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


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
	if factor == 1 {
		panic(fmt.Errorf("allocator: factor is 1 (minAllocByteSize=%d)", minAllocByteSize))
	}
	b := &ByteSlicePool{}
	nextLevelSize := minAllocByteSize
	for ; nextLevelSize <= maxAllocByteSize; nextLevelSize = int(float64(nextLevelSize) * factor) {
		b.pools = append(b.pools, &levelPool{levelSize: nextLevelSize})
	}
	if nextLevelSize < maxAllocByteSize {
		b.pools = append(b.pools, &levelPool{levelSize: maxAllocByteSize})
	}
	return b
}

func (p *ByteSlicePool) Acquire(byteSize int) *ByteSlice {
	if byteSize <= 0 {
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
	panic(fmt.Sprintf("allocator: byteSize is greater than maxAllocByteSize (byteSize=%d, maxAllocByteSize=%d)", byteSize, p.pools[len(p.pools)-1].levelSize))
}

func (p *ByteSlicePool) Release(b *ByteSlice) {
	if b == nil || b.Buffer == nil {
		return
	}
	capSize := cap(b.Buffer)
	for id, bag := range p.pools {
		if capSize == bag.levelSize {
			b.Buffer = b.Buffer[:bag.levelSize]
			p.pools[id].pool.Put(b)
			return
		}
	}
	panic(fmt.Sprintf("allocator: byteSize is greater than maxAllocByteSize (byteSize=%d, maxAllocByteSize=%d)", capSize, p.pools[len(p.pools)-1].levelSize))
}
