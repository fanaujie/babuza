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
