package syncutil

import "sync"

type EventSignal struct {
	ch chan struct{}
	mu sync.Mutex
}

func NewEventSignal() *EventSignal {
	return &EventSignal{
		ch: make(chan struct{}, 1),
	}
}

func (r *EventSignal) Channel() chan struct{} {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.ch
}

func (r *EventSignal) Reset() {
	r.mu.Lock()
	oldCh := r.ch
	r.ch = make(chan struct{}, 1)
	r.mu.Unlock()
	close(oldCh)
}

type ResultSignal struct {
	ch  chan struct{}
	err error
}

func (c *ResultSignal) Channel() chan struct{} {
	return c.ch
}

func (c *ResultSignal) Error() error {
	return c.err
}

func (c *ResultSignal) CompleteWith(err error) {
	c.err = err
	close(c.ch)
}

type SignalManager struct {
	cw *ResultSignal
	mu sync.Mutex
}

func NewSignalManager() *SignalManager {
	return &SignalManager{
		cw: &ResultSignal{
			ch: make(chan struct{}, 1),
		},
	}
}

func (e *SignalManager) Current() *ResultSignal {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.cw
}

func (e *SignalManager) Swap() (old *ResultSignal) {
	e.mu.Lock()
	old = e.cw
	e.cw = &ResultSignal{
		ch: make(chan struct{}, 1),
	}
	e.mu.Unlock()
	return old
}
