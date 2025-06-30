// Copyright 2025 Chen Chunchieh <junjie725@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


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
