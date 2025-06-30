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


package limiter

import (
	"sync"
)

type MemorySizeLimiter struct {
	maxTotalSize int64
	currentSize  int64
	mu           sync.RWMutex
}

func NewMemorySizeLimiter(maxTotalSize int64) ResourceLimiter {
	return &MemorySizeLimiter{
		maxTotalSize: maxTotalSize,
	}
}

func (l *MemorySizeLimiter) Allow(amount int64) bool {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.currentSize+amount <= l.maxTotalSize
}

func (l *MemorySizeLimiter) Acquire(amount int64) bool {
	if !l.Allow(amount) {
		return false
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	l.currentSize += amount
	return true
}

func (l *MemorySizeLimiter) Release(amount int64) {
	l.mu.Lock()
	defer l.mu.Unlock()

	newSize := l.currentSize - amount
	if newSize < 0 {
		newSize = 0
	}
	l.currentSize = newSize
}

func (l *MemorySizeLimiter) Available() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()

	available := l.maxTotalSize - l.currentSize
	if available < 0 {
		return 0
	}
	return available
}

func (l *MemorySizeLimiter) Reset() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.currentSize = 0
}
