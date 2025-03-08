package limiter

import "math"

type NoResourceLimiter struct{}

func NewNoResourceLimiter() ResourceLimiter {
	return &NoResourceLimiter{}
}

func (l *NoResourceLimiter) Allow(amount int64) bool {
	return true
}

func (l *NoResourceLimiter) Acquire(amount int64) bool {
	return true
}

func (l *NoResourceLimiter) Release(amount int64) {}

func (l *NoResourceLimiter) Available() int64 {
	return math.MaxInt64 // Return the maximum int64 value
}

func (l *NoResourceLimiter) Reset() {}
