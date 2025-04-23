package shard

import "runtime"

type Config struct {
	WorkerNum int
	QueueSize int64
	MaxTicks  int
}

func DefaultConfig(maxTicks int) *Config {
	return &Config{
		WorkerNum: runtime.NumCPU(),
		QueueSize: 256,
		MaxTicks:  maxTicks,
	}
}
