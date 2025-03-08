package multierror

import (
	"bytes"
	"sync"
)

var bufferPool = sync.Pool{}

func getBuffer() *bytes.Buffer {
	b := bufferPool.Get()
	if b == nil {
		return &bytes.Buffer{}
	}
	return b.(*bytes.Buffer)
}

func releaseBuffer(b *bytes.Buffer) {
	bufferPool.Put(b)
}
