package replier

import "sync"

type Completion struct {
	mu              sync.Mutex
	completions     map[uint64]chan struct{}
	lastCompletedID uint64
	closedCh        chan struct{}
}

func NewCompletion() *Completion {
	ch := make(chan struct{})
	close(ch)
	return &Completion{
		completions: make(map[uint64]chan struct{}),
		closedCh:    ch,
	}
}

func (c *Completion) AcquireCompletionChan(id uint64) chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lastCompletedID >= id {
		return c.closedCh
	}
	ch, ok := c.completions[id]
	if !ok {
		ch = make(chan struct{})
		c.completions[id] = ch
	}
	return ch
}

func (c *Completion) MarkCompleted(id uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lastCompletedID = id
	for k, v := range c.completions {
		if k <= id {
			delete(c.completions, k)
			close(v)
		}
	}
}
