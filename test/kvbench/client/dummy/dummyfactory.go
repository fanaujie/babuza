package dummy

import (
	"github.com/fanaujie/babuza/test/kvbench/client"
	"time"
)

type Factory struct {
	maxDelay time.Duration
}

func NewDummyFactory(maxDelay time.Duration) *Factory {
	return &Factory{
		maxDelay: maxDelay,
	}
}

func (f *Factory) NewClient(config client.Config) (client.Client, error) {
	return NewDummyClient(f.maxDelay), nil
}
