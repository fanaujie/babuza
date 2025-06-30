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


package queue

import (
	"errors"
	"sync"
	"sync/atomic"
)

var (
	ErrBufferInUse = errors.New("buffer is currently in use")
	ErrBufferFull  = errors.New("buffer is full")
)

type SwapBufferQueue[T any] struct {
	bufferA   []T
	bufferB   []T
	writeBuf  []T
	size      uint64
	tail      uint64
	isTargetA bool
	stopped   bool
	mu        sync.Mutex

	bufferInUse bool
	onRelease   func([]T)
}

type BufferSlice[T any] struct {
	Data     []T
	swQueue  *SwapBufferQueue[T]
	released int32
}

func (bs *BufferSlice[T]) Release() {
	if bs.Data != nil {
		if atomic.CompareAndSwapInt32(&bs.released, 0, 1) {
			if bs.swQueue.onRelease != nil {
				bs.swQueue.onRelease(bs.Data)
			}
			bs.swQueue.mu.Lock()
			bs.swQueue.bufferInUse = false
			bs.swQueue.mu.Unlock()
		}
	}
}

func NewSwapBufferQueue[T any](size uint64, onRelease func([]T)) *SwapBufferQueue[T] {
	q := &SwapBufferQueue[T]{
		size:      size,
		bufferA:   make([]T, size),
		bufferB:   make([]T, size),
		isTargetA: true,
		onRelease: onRelease,
	}
	q.writeBuf = q.bufferA
	return q
}

func (q *SwapBufferQueue[T]) Dispose() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.stopped = true
}

func (q *SwapBufferQueue[T]) Disposed() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.stopped
}

func (q *SwapBufferQueue[T]) Len() uint64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.tail
}

func (q *SwapBufferQueue[T]) Put(element T) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.stopped {
		return ErrQueueDisposed
	}

	if q.tail == q.size {
		return ErrBufferFull
	}

	q.writeBuf[q.tail] = element
	q.tail++
	return nil
}

func (q *SwapBufferQueue[T]) Get() (BufferSlice[T], error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.stopped {
		var zero BufferSlice[T]
		return zero, ErrQueueDisposed
	}

	if q.bufferInUse {
		var zero BufferSlice[T]
		return zero, ErrBufferInUse
	}

	if q.tail == 0 {
		var zero BufferSlice[T]
		return zero, nil
	}

	q.bufferInUse = true

	data := q.swapBuffer()

	return BufferSlice[T]{
		Data:    data,
		swQueue: q,
	}, nil
}

func (q *SwapBufferQueue[T]) swapBuffer() []T {
	var readBuf []T
	if q.isTargetA {
		readBuf = q.bufferA[:q.tail]
		q.writeBuf = q.bufferB
	} else {
		readBuf = q.bufferB[:q.tail]
		q.writeBuf = q.bufferA
	}

	q.isTargetA = !q.isTargetA
	q.tail = 0

	return readBuf
}
