package queue

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func TestNewSwapBufferQueue(t *testing.T) {
	releaseCount := 0
	onRelease := func([]int) {
		releaseCount++
	}

	q := NewSwapBufferQueue[int](10, onRelease)

	if q == nil {
		t.Fatal("Expected non-nil queue")
	}

	if q.size != 10 {
		t.Errorf("Expected size 10, got %d", q.size)
	}

	if !q.isTargetA {
		t.Error("Expected isTargetA to be true")
	}

	if q.tail != 0 {
		t.Errorf("Expected tail 0, got %d", q.tail)
	}

	if len(q.bufferA) != 10 || len(q.bufferB) != 10 {
		t.Error("Buffers not initialized correctly")
	}
}

func TestPutAndGet(t *testing.T) {
	q := NewSwapBufferQueue[int](5, nil)

	// Put elements
	for i := 0; i < 3; i++ {
		err := q.Put(i + 1)
		if err != nil {
			t.Errorf("Error putting element: %v", err)
		}
	}

	// Get elements
	slice, err := q.Get()
	if err != nil {
		t.Errorf("Error getting elements: %v", err)
	}

	if len(slice.Data) != 3 {
		t.Errorf("Expected 3 elements, got %d", len(slice.Data))
	}

	for i := 0; i < 3; i++ {
		if slice.Data[i] != i+1 {
			t.Errorf("Expected %d at index %d, got %d", i+1, i, slice.Data[i])
		}
	}

	slice.Release()
}

func TestBufferSwapping(t *testing.T) {
	q := NewSwapBufferQueue[int](5, nil)

	// Put elements in first buffer
	for i := 0; i < 3; i++ {
		q.Put(i + 1)
	}

	// Get first buffer
	slice1, _ := q.Get()
	if len(slice1.Data) != 3 {
		t.Errorf("Expected 3 elements, got %d", len(slice1.Data))
	}

	initialTargetA := q.isTargetA

	// Put elements in second buffer
	for i := 0; i < 2; i++ {
		q.Put(i + 10)
	}

	// Verify buffer swapped
	if q.isTargetA == !initialTargetA {
		t.Error("Buffer should have swapped")
	}

	// Release first buffer
	slice1.Release()

	// Get second buffer
	slice2, _ := q.Get()
	if len(slice2.Data) != 2 {
		t.Errorf("Expected 2 elements, got %d", len(slice2.Data))
	}

	for i := 0; i < 2; i++ {
		if slice2.Data[i] != i+10 {
			t.Errorf("Expected %d at index %d, got %d", i+10, i, slice2.Data[i])
		}
	}

	slice2.Release()
}

func TestGetEmptyQueue(t *testing.T) {
	q := NewSwapBufferQueue[int](5, nil)

	slice, err := q.Get()
	if err == nil {
		t.Error("Expected not nil error, got nil")
	}
	if !errors.Is(err, ErrQueueEmpty) {
		t.Errorf("Expected ErrBufferInUse, got %v", err)
	}
	if len(slice.Data) != 0 {
		t.Errorf("Expected empty slice, got %d elements", len(slice.Data))
	}
}

func TestBufferFull(t *testing.T) {
	q := NewSwapBufferQueue[int](3, nil)

	// Fill buffer
	for i := 0; i < 3; i++ {
		err := q.Put(i)
		if err != nil {
			t.Errorf("Error putting element: %v", err)
		}
	}

	// Try to add one more
	err := q.Put(99)
	if !errors.Is(err, ErrBufferFull) {
		t.Errorf("Expected ErrBufferFull, got %v", err)
	}

	// Get and clear buffer
	slice, _ := q.Get()
	slice.Release()

	// Should be able to add more now
	err = q.Put(100)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
}

func TestBufferInUse(t *testing.T) {
	q := NewSwapBufferQueue[int](5, nil)

	// Put some elements
	q.Put(1)
	q.Put(2)

	// Get buffer but don't release
	_, err := q.Get()
	if err != nil {
		t.Errorf("Error getting elements: %v", err)
	}

	// Try to get buffer again
	_, err = q.Get()
	if !errors.Is(err, ErrBufferInUse) {
		t.Errorf("Expected ErrBufferInUse, got %v", err)
	}
}

func TestQueueStopped(t *testing.T) {
	q := NewSwapBufferQueue[int](5, nil)

	// Stop the queue
	q.Disposed()

	// Try to put element
	err := q.Put(1)
	if !errors.Is(err, ErrQueueDisposed) {
		t.Errorf("Expected ErrQueueStopped, got %v", err)
	}

	// Try to get elements
	_, err = q.Get()
	if !errors.Is(err, ErrQueueDisposed) {
		t.Errorf("Expected ErrQueueStopped, got %v", err)
	}
}

func TestOnReleaseCallback(t *testing.T) {
	released := false
	lastData := []int{}

	onRelease := func(data []int) {
		released = true
		lastData = data
	}

	q := NewSwapBufferQueue[int](5, onRelease)

	q.Put(1)
	q.Put(2)
	q.Put(3)

	slice, _ := q.Get()

	// Verify release hasn't been called yet
	if released {
		t.Error("onRelease should not have been called yet")
	}

	// Release the slice
	slice.Release()

	// Verify onRelease was called
	if !released {
		t.Error("onRelease should have been called")
	}

	if len(lastData) != 3 || lastData[0] != 1 || lastData[1] != 2 || lastData[2] != 3 {
		t.Errorf("onRelease received incorrect data: %v", lastData)
	}
}

func TestMultipleRelease(t *testing.T) {
	releaseCount := 0
	onRelease := func([]int) {
		releaseCount++
	}

	q := NewSwapBufferQueue[int](5, onRelease)

	q.Put(1)

	slice, _ := q.Get()

	// Release multiple times
	slice.Release()
	slice.Release()
	slice.Release()

	// onRelease should only be called once
	if releaseCount != 1 {
		t.Errorf("Expected releaseCount = 1, got %d", releaseCount)
	}
}

func TestConcurrentAccess(t *testing.T) {
	q := NewSwapBufferQueue[int](1000, nil)

	var wg sync.WaitGroup
	producers := 5
	itemsPerProducer := 100

	// Start producers
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(producerID int) {
			defer wg.Done()
			for i := 0; i < itemsPerProducer; i++ {
				// Retry until successful
				for {
					err := q.Put(producerID*1000 + i)
					if err == nil {
						break
					}
					// If buffer is full, wait a bit
					if errors.Is(err, ErrBufferFull) {
						time.Sleep(time.Millisecond)
						continue
					}
					t.Errorf("Unexpected error: %v", err)
					return
				}
			}
		}(p)
	}

	// Start consumer
	receivedItems := 0
	consumerDone := make(chan struct{})

	go func() {
		for receivedItems < producers*itemsPerProducer {
			slice, err := q.Get()
			if err != nil {
				if errors.Is(err, ErrQueueDisposed) {
					t.Errorf("Unexpected error: %v", err)
				}
				time.Sleep(time.Millisecond)
				continue
			}

			if len(slice.Data) > 0 {
				receivedItems += len(slice.Data)
			}
			slice.Release()
		}
		close(consumerDone)
	}()

	// Wait for producers
	wg.Wait()

	// Wait for consumer
	<-consumerDone

	if receivedItems != producers*itemsPerProducer {
		t.Errorf("Expected to receive %d items, got %d", producers*itemsPerProducer, receivedItems)
	}
}

func TestLen(t *testing.T) {
	q := NewSwapBufferQueue[int](5, nil)

	if q.Len() != 0 {
		t.Errorf("Empty queue should have length 0, got %d", q.Len())
	}

	for i := 0; i < 3; i++ {
		err := q.Put(i)
		if err != nil {
			t.Errorf("Error putting element: %v", err)
		}
	}

	if q.Len() != 3 {
		t.Errorf("Queue length should be 3, got %d", q.Len())
	}

	slice, err := q.Get()
	if err != nil {
		t.Errorf("Error getting elements: %v", err)
	}

	if q.Len() != 0 {
		t.Errorf("Queue length should be 0 after Get(), got %d", q.Len())
	}

	slice.Release()

	err = q.Put(42)
	if err != nil {
		t.Errorf("Error putting element: %v", err)
	}

	if q.Len() != 1 {
		t.Errorf("Queue length should be 1, got %d", q.Len())
	}
}
