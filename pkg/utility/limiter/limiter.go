package limiter

import "context"

type ResourceLimiter interface {
	Allow(amount int64) bool
	Acquire(amount int64) bool
	Release(amount int64)
	Available() int64
	Reset()
}

type MemorySizeLimiterOptions struct {
	MaxTotalSize            int64
	MaxSingleAllocationSize int64
}

type RateLimiter interface {
	Wait(ctx context.Context) error
}
