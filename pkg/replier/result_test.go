package replier

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewResult(t *testing.T) {
	r := NewResult[int]()
	assert.NotNil(t, r)
	assert.NotNil(t, r.results)
}

func TestAcquireResultChan(t *testing.T) {
	r := NewResult[int]()
	ch, err := r.AcquireResultChan(1)
	assert.NoError(t, err)
	assert.NotNil(t, ch)

	_, err = r.AcquireResultChan(1)
	assert.EqualError(t, err, "id already exists")
}

func TestCancelResult(t *testing.T) {
	r := NewResult[int]()
	_, err := r.AcquireResultChan(1)
	assert.NoError(t, err)

	r.CancelResult(1)
	_, err = r.AcquireResultChan(1)
	assert.NoError(t, err)
}

func TestSendResult(t *testing.T) {
	r := NewResult[int]()
	ch, _ := r.AcquireResultChan(1)
	value := 42

	r.SendResult(1, value)
	received := <-ch
	assert.Equal(t, value, received)
}

func TestIsWaiting(t *testing.T) {
	r := NewResult[int]()
	_, _ = r.AcquireResultChan(1)
	assert.True(t, r.IsWaiting(1))

	r.CancelResult(1)
	assert.False(t, r.IsWaiting(1))
}
