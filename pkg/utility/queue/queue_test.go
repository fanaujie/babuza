/*
Copyright 2014 Workiva, LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Modifications Copyright 2025 [Chen Chunchieh]

Modifications:
- Updated all tests to use generic Queue[T] instead of interface{}
- Modified Get() and Poll() test methods to use pre-allocated slices as parameters
- Added tests for new GetOne() and PollOne() methods
- Changed error assertions to use new error names (ErrQueueDisposed, ErrQueueTimeout, ErrQueueEmpty)
- Added comparison benchmarks between generic and interface versions
- Updated TakeUntil() tests to use generic type parameters
- Modified Dispose() tests to work with typed slices instead of interface{} slices
- Updated ExecuteInParallel tests to use generic version
- Changed MultipleGetEmpty test to use GetOne() method
- Added _Generic and _Interface suffix to benchmark tests for comparison
*/
package queue

import (
	"github.com/Workiva/go-datastructures/queue"
	"github.com/stretchr/testify/assert"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestPut(t *testing.T) {
	q := New[string](10)

	q.Put(`test`)
	assert.Equal(t, int64(1), q.Len())

	result, err := q.GetOne()
	assert.Nil(t, err)

	assert.Equal(t, `test`, result)
	assert.True(t, q.Empty())

	q.Put(`test2`)
	assert.Equal(t, int64(1), q.Len())

	result, err = q.GetOne()
	assert.Nil(t, err)

	assert.Equal(t, `test2`, result)
	assert.True(t, q.Empty())
}

func TestGet(t *testing.T) {
	q := New[string](10)

	q.Put(`test`)
	pollItems := make([]string, 2)
	reads, err := q.Get(2, pollItems)
	if !assert.Nil(t, err) {
		return
	}

	assert.Equal(t, reads, int64(1))
	assert.Equal(t, `test`, pollItems[0])
	assert.Equal(t, int64(0), q.Len())

	q.Put(`1`)
	q.Put(`2`)

	reads, err = q.Get(1, pollItems[:1])
	if !assert.Nil(t, err) {
		return
	}

	assert.Equal(t, reads, int64(1))
	assert.Equal(t, `1`, pollItems[0])
	assert.Equal(t, int64(1), q.Len())

	reads, err = q.Get(2, pollItems)
	if !assert.Nil(t, err) {
		return
	}

	assert.Equal(t, `2`, pollItems[0])
}

func TestPoll(t *testing.T) {
	q := New[string](10)

	// should be able to Poll() before anything is present, without breaking future Puts
	q.PollOne(time.Millisecond)

	q.Put(`test`)
	pollItems := make([]string, 2)
	reads, err := q.Poll(2, pollItems, 0)
	if !assert.Nil(t, err) {
		return
	}

	assert.Equal(t, reads, int64(1))
	assert.Equal(t, `test`, pollItems[0])
	assert.Equal(t, int64(0), q.Len())

	q.Put(`1`)
	q.Put(`2`)

	reads, err = q.Poll(1, pollItems[:1], time.Millisecond)
	if !assert.Nil(t, err) {
		return
	}

	assert.Equal(t, reads, int64(1))
	assert.Equal(t, `1`, pollItems[0])
	assert.Equal(t, int64(1), q.Len())

	reads, err = q.Poll(2, pollItems, time.Millisecond)
	if !assert.Nil(t, err) {
		return
	}

	assert.Equal(t, `2`, pollItems[0])

	before := time.Now()
	_, err = q.Poll(1, pollItems[:1], 5*time.Millisecond)
	// This delta is normally 1-3 ms but running tests in CI with -race causes
	// this to run much slower. For now, just bump up the threshold.
	assert.InDelta(t, 5, time.Since(before).Seconds()*1000, 10)
	assert.Equal(t, ErrQueueTimeout, err)
}

func TestPollNoMemoryLeak(t *testing.T) {
	q := New[int](0)

	assert.Len(t, q.waiters, 0)
	pollItems := make([]int, 1)
	for i := 0; i < 10; i++ {
		// Poll() should cleanup waiters after timeout
		q.Poll(1, pollItems, time.Nanosecond)
		assert.Len(t, q.waiters, 0)
	}
}

func TestAddEmptyPut(t *testing.T) {
	q := New[int](10)

	q.Put()

	if q.Len() != 0 {
		t.Errorf(`Expected len: %d, received: %d`, 0, q.Len())
	}
}

func TestGetNonPositiveNumber(t *testing.T) {
	q := New[string](10)

	q.Put(`test`)

	pollItems := make([]string, 2)
	reads, err := q.Get(0, pollItems)
	if !assert.Nil(t, err) {
		return
	}

	if reads != 0 {
		t.Errorf(`Expected len: %d, received: %d`, 0, reads)
	}
}

func TestEmpty(t *testing.T) {
	q := New[string](10)

	if !q.Empty() {
		t.Errorf(`Expected empty queue.`)
	}

	q.Put(`test`)
	if q.Empty() {
		t.Errorf(`Expected non-empty queue.`)
	}
}

func TestGetEmpty(t *testing.T) {
	q := New[string](10)

	go func() {
		q.Put(`a`)
	}()

	poolItems := make([]string, 2)
	reads, err := q.Get(2, poolItems)
	if !assert.Nil(t, err) {
		return
	}

	assert.Equal(t, reads, int64(1))
	assert.Equal(t, `a`, poolItems[0])
}

func TestMultipleGetEmpty(t *testing.T) {
	q := New[string](10)
	var wg sync.WaitGroup
	wg.Add(2)
	results := make([]string, 2)

	go func() {
		wg.Done()
		local, err := q.GetOne()
		assert.Nil(t, err)
		results[0] = local
		wg.Done()
	}()

	go func() {
		wg.Done()
		local, err := q.GetOne()
		assert.Nil(t, err)
		results[1] = local
		wg.Done()
	}()

	wg.Wait()
	wg.Add(2)

	q.Put(`a`, `b`, `c`)
	wg.Wait()

	if assert.Len(t, results[0], 1) && assert.Len(t, results[1], 1) {
		assert.True(t, (results[0] == `a` && results[1] == `b`) ||
			(results[0] == `b` && results[1] == `a`),
			`The array should be a, b or b, a`)
	}
}

func TestDispose(t *testing.T) {
	// when the queue is empty
	q := New[string](10)
	itemsDisposed := q.Dispose()

	assert.Empty(t, itemsDisposed)

	// when the queue is not empty
	q = New[string](10)
	q.Put(`1`)
	itemsDisposed = q.Dispose()

	expected := []string{`1`}
	assert.Equal(t, expected, itemsDisposed)

	// when the queue has been disposed
	itemsDisposed = q.Dispose()
	assert.Nil(t, itemsDisposed)
}

func TestEmptyGetWithDispose(t *testing.T) {
	q := New[int](10)
	var wg sync.WaitGroup
	wg.Add(1)

	var err error

	go func() {
		wg.Done()
		_, err = q.GetOne()
		wg.Done()
	}()

	wg.Wait()
	wg.Add(1)

	q.Dispose()

	wg.Wait()

	assert.IsType(t, ErrQueueDisposed, err)
}

func TestDisposeAfterEmptyPoll(t *testing.T) {
	q := New[int](10)

	_, err := q.PollOne(time.Millisecond)
	assert.IsType(t, ErrQueueTimeout, err)

	// it should not hang
	q.Dispose()

	_, err = q.PollOne(time.Millisecond)
	assert.IsType(t, ErrQueueDisposed, err)
}

func TestGetPutDisposed(t *testing.T) {
	q := New[string](10)

	q.Dispose()

	_, err := q.GetOne()
	assert.IsType(t, ErrQueueDisposed, err)

	err = q.Put(`a`)
	assert.IsType(t, ErrQueueDisposed, err)
}

func BenchmarkQueue_Generic(b *testing.B) {
	q := New[string](int64(b.N))
	var wg sync.WaitGroup
	wg.Add(1)
	i := 0

	go func() {
		for {
			q.GetOne()
			i++
			if i == b.N {
				wg.Done()
				break
			}
		}
	}()

	for i := 0; i < b.N; i++ {
		q.Put(`a`)
	}

	wg.Wait()
}

func BenchmarkQueue_Interface(b *testing.B) {
	q := queue.New(int64(b.N))
	var wg sync.WaitGroup
	wg.Add(1)
	i := 0

	go func() {
		for {
			q.Get(1)
			i++
			if i == b.N {
				wg.Done()
				break
			}
		}
	}()

	for i := 0; i < b.N; i++ {
		q.Put(`a`)
	}

	wg.Wait()
}

func BenchmarkChannel(b *testing.B) {
	ch := make(chan interface{}, 1)
	var wg sync.WaitGroup
	wg.Add(1)
	i := 0

	go func() {
		for {
			<-ch
			i++
			if i == b.N {
				wg.Done()
				break
			}
		}
	}()

	for i := 0; i < b.N; i++ {
		ch <- `a`
	}

	wg.Wait()
}

func TestPeek(t *testing.T) {
	q := New[string](10)
	q.Put(`a`)
	q.Put(`b`)
	q.Put(`c`)
	peekResult, err := q.Peek()
	peekExpected := `a`
	assert.Nil(t, err)
	assert.Equal(t, q.Len(), int64(3))
	assert.Equal(t, peekExpected, peekResult)

	popResult, err := q.GetOne()
	assert.Nil(t, err)
	assert.Equal(t, peekResult, popResult)
	assert.Equal(t, q.Len(), int64(2))
}

func TestPeekOnDisposedQueue(t *testing.T) {
	q := New[int](10)
	q.Dispose()
	result, err := q.Peek()

	assert.Equal(t, 0, result)
	assert.IsType(t, ErrQueueDisposed, err)
}

func TestTakeUntil(t *testing.T) {
	q := New[string](10)
	q.Put(`a`, `b`, `c`)
	result, err := q.TakeUntil(func(item string) bool {
		return item != `c`
	})

	if !assert.Nil(t, err) {
		return
	}

	expected := []string{`a`, `b`}
	assert.Equal(t, expected, result)
}

func TestTakeUntilEmptyQueue(t *testing.T) {
	q := New[string](10)
	result, err := q.TakeUntil(func(item string) bool {
		return item != `c`
	})

	if !assert.Nil(t, err) {
		return
	}

	expected := []string{}
	assert.Equal(t, expected, result)
}

func TestTakeUntilThenGet(t *testing.T) {
	q := New[string](10)
	q.Put(`a`, `b`, `c`)
	takeItems, _ := q.TakeUntil(func(item string) bool {
		return item != `c`
	})

	poolItems := make([]string, 3)
	reads, _ := q.Get(3, poolItems)
	assert.Equal(t, []string{`a`, `b`}, takeItems)
	assert.Equal(t, []string{`c`}, poolItems[:reads])
}

func TestTakeUntilNoMatches(t *testing.T) {
	q := New[string](10)
	q.Put(`a`, `b`, `c`)
	takeItems, _ := q.TakeUntil(func(item string) bool {
		return item != `a`
	})

	poolItems := make([]string, 3)
	_, _ = q.Get(3, poolItems)
	assert.Equal(t, []string{}, takeItems)
	assert.Equal(t, []string{`a`, `b`, `c`}, poolItems)
}

func TestTakeUntilOnDisposedQueue(t *testing.T) {
	q := New[int](10)
	q.Dispose()
	result, err := q.TakeUntil(func(item int) bool {
		return true
	})

	assert.Nil(t, result)
	assert.IsType(t, ErrQueueDisposed, err)
}

func TestWaiters(t *testing.T) {
	s1, s2, s3, s4 := newSema(), newSema(), newSema(), newSema()

	w := waiters{}
	assert.Len(t, w, 0)

	//
	// test put()
	w.put(s1)
	assert.Equal(t, waiters{s1}, w)

	w.put(s2)
	w.put(s3)
	w.put(s4)
	assert.Equal(t, waiters{s1, s2, s3, s4}, w)

	//
	// test remove()
	//
	// remove from middle
	w.remove(s2)
	assert.Equal(t, waiters{s1, s3, s4}, w)

	// remove non-existing element
	w.remove(s2)
	assert.Equal(t, waiters{s1, s3, s4}, w)

	// remove from beginning
	w.remove(s1)
	assert.Equal(t, waiters{s3, s4}, w)

	// remove from end
	w.remove(s4)
	assert.Equal(t, waiters{s3}, w)

	// remove last element
	w.remove(s3)
	assert.Empty(t, w)

	// remove non-existing element
	w.remove(s3)
	assert.Empty(t, w)

	//
	// test get()
	//
	// start with 3 elements in list
	w.put(s1)
	w.put(s2)
	w.put(s3)
	assert.Equal(t, waiters{s1, s2, s3}, w)

	// get() returns each item in insertion order
	assert.Equal(t, s1, w.get())
	assert.Equal(t, s2, w.get())
	w.put(s4) // interleave a put(), item should go to the end
	assert.Equal(t, s3, w.get())
	assert.Equal(t, s4, w.get())
	assert.Empty(t, w)
	assert.Nil(t, w.get())
}

func TestExecuteInParallel(t *testing.T) {
	q := New[int](10)
	for i := 0; i < 10; i++ {
		q.Put(i)
	}

	numCalls := uint64(0)

	ExecuteInParallel(q, func(item int) {
		t.Logf("ExecuteInParallel called us with %+v", item)
		atomic.AddUint64(&numCalls, 1)
	})

	assert.Equal(t, uint64(10), numCalls)
	assert.True(t, q.Disposed())
}

func TestExecuteInParallelEmptyQueue(t *testing.T) {
	q := New[int](1)

	// basically just ensuring we don't deadlock here
	ExecuteInParallel(q, func(int) {
		t.Fail()
	})
}

func BenchmarkQueuePut_Generic(b *testing.B) {
	numItems := int64(1000)

	qs := make([]*Queue[int64], 0, b.N)

	for i := 0; i < b.N; i++ {
		q := New[int64](10)
		qs = append(qs, q)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q := qs[i]
		for j := int64(0); j < numItems; j++ {
			q.Put(j)
		}
	}
}

func BenchmarkQueuePut_Interface(b *testing.B) {
	numItems := int64(1000)

	qs := make([]*queue.Queue, 0, b.N)

	for i := 0; i < b.N; i++ {
		q := queue.New(10)
		qs = append(qs, q)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q := qs[i]
		for j := int64(0); j < numItems; j++ {
			q.Put(j)
		}
	}
}

func BenchmarkQueueGet_Generic(b *testing.B) {
	numItems := int64(1000)

	qs := make([]*Queue[int64], 0, b.N)

	for i := 0; i < b.N; i++ {
		q := New[int64](numItems)
		for j := int64(0); j < numItems; j++ {
			q.Put(j)
		}
		qs = append(qs, q)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		q := qs[i]
		for j := int64(0); j < numItems; j++ {
			q.GetOne()
		}
	}
}

func BenchmarkQueueGet_Interface(b *testing.B) {
	numItems := int64(1000)

	qs := make([]*queue.Queue, 0, b.N)

	for i := 0; i < b.N; i++ {
		q := queue.New(numItems)
		for j := int64(0); j < numItems; j++ {
			q.Put(j)
		}
		qs = append(qs, q)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		q := qs[i]
		for j := int64(0); j < numItems; j++ {
			q.Get(1)
		}
	}
}

func BenchmarkQueuePoll_Generic(b *testing.B) {
	numItems := int64(1000)

	qs := make([]*Queue[int64], 0, b.N)

	for i := 0; i < b.N; i++ {
		q := New[int64](numItems)
		for j := int64(0); j < numItems; j++ {
			q.Put(j)
		}
		qs = append(qs, q)
	}

	b.ResetTimer()

	for _, q := range qs {
		for j := int64(0); j < numItems; j++ {
			q.PollOne(time.Millisecond)
		}
	}
}

func BenchmarkQueuePoll_Interface(b *testing.B) {
	numItems := int64(1000)

	qs := make([]*queue.Queue, 0, b.N)

	for i := 0; i < b.N; i++ {
		q := queue.New(numItems)
		for j := int64(0); j < numItems; j++ {
			q.Put(j)
		}
		qs = append(qs, q)
	}

	b.ResetTimer()

	for _, q := range qs {
		for j := int64(0); j < numItems; j++ {
			q.Poll(1, time.Millisecond)
		}
	}
}

func BenchmarkExecuteInParallel_Generic(b *testing.B) {
	numItems := int64(1000)

	qs := make([]*Queue[int64], 0, b.N)

	for i := 0; i < b.N; i++ {
		q := New[int64](numItems)
		for j := int64(0); j < numItems; j++ {
			q.Put(j)
		}
		qs = append(qs, q)
	}

	var counter int64
	fn := func(c int64) {
		atomic.AddInt64(&counter, c)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		q := qs[i]
		ExecuteInParallel(q, fn)
	}
}

func BenchmarkExecuteInParallel_Interface(b *testing.B) {
	numItems := int64(1000)

	qs := make([]*queue.Queue, 0, b.N)

	for i := 0; i < b.N; i++ {
		q := queue.New(numItems)
		for j := int64(0); j < numItems; j++ {
			q.Put(j)
		}
		qs = append(qs, q)
	}

	var counter int64
	fn := func(ifc interface{}) {
		c := ifc.(int64)
		atomic.AddInt64(&counter, c)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		q := qs[i]
		queue.ExecuteInParallel(q, fn)
	}
}
