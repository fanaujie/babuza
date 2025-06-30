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


package replier

import (
	"errors"
	"sync"
)

type Result[T any] struct {
	mu      sync.Mutex
	results map[uint64]chan T
}

func NewResult[T any]() *Result[T] {
	return &Result[T]{
		results: make(map[uint64]chan T),
	}
}

func (r *Result[T]) AcquireResultChan(id uint64) (chan T, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.results[id]; !exists {
		c := make(chan T, 1)
		r.results[id] = c
		return c, nil
	}
	return nil, errors.New("id already exists")
}

func (r *Result[T]) CancelResult(id uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.results, id)
}

func (r *Result[T]) SendResult(id uint64, value T) {
	r.mu.Lock()
	c, ok := r.results[id]
	if !ok {
		r.mu.Unlock()
		return
	}
	delete(r.results, id)
	r.mu.Unlock()
	c <- value
	close(c)
}

func (r *Result[T]) IsWaiting(id uint64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.results[id]
	return ok
}
