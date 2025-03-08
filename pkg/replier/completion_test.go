package replier

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewCompletion(t *testing.T) {
	c := NewCompletion()
	assert.NotNil(t, c)
	assert.NotNil(t, c.completions)
	assert.NotNil(t, c.closedCh)
}

func TestAcquireCompletionChan(t *testing.T) {
	c := NewCompletion()
	ch := c.AcquireCompletionChan(1)
	assert.NotNil(t, ch)

	// Test acquiring a channel for an already completed ID
	c.MarkCompleted(1)
	ch = c.AcquireCompletionChan(1)
	select {
	case <-ch:
	default:
		t.Fatal("expected channel to be closed")
	}
}

func TestMarkCompleted(t *testing.T) {
	c := NewCompletion()
	ch1 := c.AcquireCompletionChan(1)
	ch2 := c.AcquireCompletionChan(2)

	c.MarkCompleted(1)
	select {
	case <-ch1:
	default:
		t.Fatal("expected channel to be closed")
	}

	select {
	case <-ch2:
		t.Fatal("expected channel to be open")
	default:
	}

	c.MarkCompleted(2)
	select {
	case <-ch2:
	default:
		t.Fatal("expected channel to be closed")
	}
}

func TestMarkCompleted2(t *testing.T) {
	c := NewCompletion()
	ch1 := c.AcquireCompletionChan(1)
	ch2 := c.AcquireCompletionChan(2)
	ch3 := c.AcquireCompletionChan(3)

	c.MarkCompleted(3)
	select {
	case <-ch1:
	default:
		t.Fatal("expected channel to be closed")
	}

	select {
	case <-ch2:
	default:
		t.Fatal("expected channel to be closed")
	}

	select {
	case <-ch3:
	default:
		t.Fatal("expected channel to be closed")
	}

}
