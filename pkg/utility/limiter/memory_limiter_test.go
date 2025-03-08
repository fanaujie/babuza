package limiter

import (
	"sync"
	"testing"
)

func TestNewMemorySizeLimiter(t *testing.T) {
	maxSize := int64(100)
	limiter := NewMemorySizeLimiter(maxSize)

	memLimiter, ok := limiter.(*MemorySizeLimiter)
	if !ok {
		t.Fatalf("Expected *MemorySizeLimiter, got %T", limiter)
	}

	if memLimiter.maxTotalSize != maxSize {
		t.Errorf("Expected maxTotalSize to be %d, got %d", maxSize, memLimiter.maxTotalSize)
	}

	if memLimiter.currentSize != 0 {
		t.Errorf("Expected currentSize to be 0, got %d", memLimiter.currentSize)
	}
}

func TestMemorySizeLimiter_Allow(t *testing.T) {
	maxSize := int64(100)
	limiter := NewMemorySizeLimiter(maxSize).(*MemorySizeLimiter)

	tests := []struct {
		name          string
		currentSize   int64
		requestSize   int64
		expectedAllow bool
	}{
		{"Allow when empty", 0, 50, true},
		{"Allow when partially full", 50, 50, true},
		{"Allow when exactly fits", 80, 20, true},
		{"Deny when exceeds", 80, 30, false},
		{"Deny when way too large", 0, 200, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			limiter.currentSize = tt.currentSize
			if got := limiter.Allow(tt.requestSize); got != tt.expectedAllow {
				t.Errorf("Allow(%d) = %v, want %v", tt.requestSize, got, tt.expectedAllow)
			}
		})
	}
}

func TestMemorySizeLimiter_Acquire(t *testing.T) {
	maxSize := int64(100)
	limiter := NewMemorySizeLimiter(maxSize).(*MemorySizeLimiter)

	tests := []struct {
		name            string
		initialSize     int64
		requestSize     int64
		expectedAcquire bool
		expectedSize    int64
	}{
		{"Acquire when empty", 0, 50, true, 50},
		{"Acquire when partially full", 50, 40, true, 90},
		{"Acquire when exactly fits", 80, 20, true, 100},
		{"Fail to acquire when exceeds", 80, 30, false, 80},
		{"Fail to acquire when way too large", 0, 200, false, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			limiter.currentSize = tt.initialSize
			if got := limiter.Acquire(tt.requestSize); got != tt.expectedAcquire {
				t.Errorf("Acquire(%d) = %v, want %v", tt.requestSize, got, tt.expectedAcquire)
			}
			if limiter.currentSize != tt.expectedSize {
				t.Errorf("After Acquire(%d), currentSize = %d, want %d",
					tt.requestSize, limiter.currentSize, tt.expectedSize)
			}
		})
	}
}

func TestMemorySizeLimiter_Release(t *testing.T) {
	maxSize := int64(100)
	limiter := NewMemorySizeLimiter(maxSize).(*MemorySizeLimiter)

	tests := []struct {
		name         string
		initialSize  int64
		releaseSize  int64
		expectedSize int64
	}{
		{"CompactAndReleaseSnapshot partial amount", 50, 20, 30},
		{"CompactAndReleaseSnapshot all", 50, 50, 0},
		{"CompactAndReleaseSnapshot more than current (should clamp to 0)", 50, 100, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			limiter.currentSize = tt.initialSize
			limiter.Release(tt.releaseSize)
			if limiter.currentSize != tt.expectedSize {
				t.Errorf("After CompactAndReleaseSnapshot(%d), currentSize = %d, want %d",
					tt.releaseSize, limiter.currentSize, tt.expectedSize)
			}
		})
	}
}

func TestMemorySizeLimiter_Available(t *testing.T) {
	maxSize := int64(100)
	limiter := NewMemorySizeLimiter(maxSize).(*MemorySizeLimiter)

	tests := []struct {
		name          string
		currentSize   int64
		expectedAvail int64
	}{
		{"All available when empty", 0, 100},
		{"Partial available", 40, 60},
		{"None available when full", 100, 0},
		{"None available when overcommitted", 120, 0}, // Edge case
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			limiter.currentSize = tt.currentSize
			if got := limiter.Available(); got != tt.expectedAvail {
				t.Errorf("Available() = %d, want %d", got, tt.expectedAvail)
			}
		})
	}
}

func TestMemorySizeLimiter_Reset(t *testing.T) {
	maxSize := int64(100)
	limiter := NewMemorySizeLimiter(maxSize).(*MemorySizeLimiter)

	limiter.currentSize = 75
	limiter.Reset()

	if limiter.currentSize != 0 {
		t.Errorf("After Reset(), currentSize = %d, want 0", limiter.currentSize)
	}
}

func TestMemorySizeLimiter_Concurrency(t *testing.T) {
	maxSize := int64(1000)
	limiter := NewMemorySizeLimiter(maxSize)

	// Test concurrent acquires and releases
	const goroutines = 10
	const operationsPerGoroutine = 100
	const amountPerOperation = 1

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < operationsPerGoroutine; j++ {
				success := limiter.Acquire(amountPerOperation)
				if success {
					// Simulate some work
					// Then release
					limiter.Release(amountPerOperation)
				}
			}
		}()
	}

	wg.Wait()

	// After all operations, available should be back to maxSize
	if available := limiter.Available(); available != maxSize {
		t.Errorf("After concurrent operations, Available() = %d, want %d", available, maxSize)
	}
}
