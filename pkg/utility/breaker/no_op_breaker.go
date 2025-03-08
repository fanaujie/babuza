package breaker

type NoOpBreaker struct{}

func NewNoOpBreaker() Breaker {
	return &NoOpBreaker{}
}

func (b *NoOpBreaker) Success() {}

func (b *NoOpBreaker) Fail() {}

func (b *NoOpBreaker) Ready() bool {
	return true
}

func (b *NoOpBreaker) Reset() {}
