package syncutil

import "sync"

type Notifier struct {
	ch chan struct{}
	mu sync.Mutex
}

type ChanWithErr struct {
	ch  chan struct{}
	err error
}

func (c *ChanWithErr) GetCh() chan struct{} {
	return c.ch
}

func (c *ChanWithErr) GetError() error {
	return c.err
}

type ErrNotifier struct {
	cw *ChanWithErr
	mu sync.Mutex
}

func NewNotifier() *Notifier {
	return &Notifier{
		ch: make(chan struct{}, 1),
	}
}

func (r *Notifier) Get() chan struct{} {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.ch
}

func (r *Notifier) CloseAndRenew() {
	r.mu.Lock()
	oldCh := r.ch
	r.ch = make(chan struct{}, 1)
	r.mu.Unlock()
	close(oldCh)
}

func NewErrNotifier() *ErrNotifier {
	return &ErrNotifier{
		cw: &ChanWithErr{
			ch: make(chan struct{}, 1),
		},
	}
}

func (e *ErrNotifier) Get() *ChanWithErr {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.cw
}

func (e *ErrNotifier) Renew() (old *ChanWithErr) {
	e.mu.Lock()
	old = e.cw
	e.cw = &ChanWithErr{
		ch: make(chan struct{}, 1),
	}
	e.mu.Unlock()
	return old
}

func (c *ChanWithErr) Close(err error) {
	c.err = err
	close(c.ch)
}
