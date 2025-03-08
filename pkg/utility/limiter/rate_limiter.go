package limiter

import (
	"context"
	"golang.org/x/time/rate"
)

type StandardRateLimiter struct {
	r *rate.Limiter
}

func NewStandardRateLimiter(r rate.Limit, b int) *StandardRateLimiter {
	return &StandardRateLimiter{
		r: rate.NewLimiter(r, b),
	}
}

func (s *StandardRateLimiter) Wait(ctx context.Context) error {
	return s.r.Wait(ctx)
}
