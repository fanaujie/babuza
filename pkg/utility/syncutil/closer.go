package syncutil

import (
	"context"
	"sync"
	"sync/atomic"
)

type Closer struct {
	wg         sync.WaitGroup
	ctx        context.Context
	cancelFunc context.CancelFunc
	count      uint64
}

func NewCloser() *Closer {
	c := &Closer{}
	c.ctx, c.cancelFunc = context.WithCancel(context.Background())
	return c
}

func (c *Closer) CloseCh() <-chan struct{} {
	return c.ctx.Done()
}

func (c *Closer) Cancel() {
	c.cancelFunc()
}

func (c *Closer) Close() {
	c.cancelFunc()
	c.wg.Wait()
}

func (c *Closer) Wait() {
	c.wg.Wait()
}

func (c *Closer) Count() uint64 {
	return atomic.LoadUint64(&c.count)
}

func (c *Closer) Run(f func()) {
	select {
	case <-c.ctx.Done():
		return
	default:
	}
	c.addOne()
	go func() {
		defer c.done()
		f()
	}()
}

func (c *Closer) addOne() {
	c.wg.Add(1)
	atomic.AddUint64(&c.count, 1)
}

func (c *Closer) done() {
	c.wg.Done()
	atomic.AddUint64(&c.count, ^uint64(0))
}
