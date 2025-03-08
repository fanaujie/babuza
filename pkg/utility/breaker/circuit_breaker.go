package breaker

import (
	"github.com/cenkalti/backoff"
	"github.com/facebookgo/clock"
	circuit "github.com/rubyist/circuitbreaker"
	"time"
)

type Config struct {
	InitialInterval     time.Duration
	RandomizationFactor float64
	Multiplier          float64
	MaxInterval         time.Duration
	FailureThreshold    int64
}

type CircuitBreaker struct {
	breaker *circuit.Breaker
}

func NewCircuitBreaker(config *Config) Breaker {
	if config == nil {
		config = &Config{
			InitialInterval:     1 * time.Second,
			RandomizationFactor: 0.5,
			Multiplier:          1.5,
			MaxInterval:         60 * time.Second,
			FailureThreshold:    5,
		}
	}

	c := clock.New()
	b := &backoff.ExponentialBackOff{
		InitialInterval:     config.InitialInterval,
		RandomizationFactor: config.RandomizationFactor,
		Multiplier:          config.Multiplier,
		MaxInterval:         config.MaxInterval,
		MaxElapsedTime:      0,
		Clock:               c,
	}

	return &CircuitBreaker{
		breaker: circuit.NewBreakerWithOptions(&circuit.Options{
			BackOff:    b,
			Clock:      c,
			ShouldTrip: circuit.ThresholdTripFunc(config.FailureThreshold),
		}),
	}
}

func (c *CircuitBreaker) Success() {
	c.breaker.Success()
}

func (c *CircuitBreaker) Fail() {
	c.breaker.Fail()
}

func (c *CircuitBreaker) Ready() bool {
	return c.breaker.Ready()
}

func (c *CircuitBreaker) Reset() {
	c.breaker.Reset()
}
