package limiter

import "context"

type NoOpRateLimiter struct{}

func NewNoOpRateLimiter() RateLimiter {
	return &NoOpRateLimiter{}
}

func (l *NoOpRateLimiter) Wait(ctx context.Context) error {
	return nil
}
