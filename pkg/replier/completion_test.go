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
