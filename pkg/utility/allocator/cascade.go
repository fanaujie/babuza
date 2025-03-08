package allocator

type TwoLevelPool struct {
	firstFixedBufferSize int
	firstFixedBuffer     []byte
	secondPool           *ByteSlicePool
}

func NewDefaultTwoLevelPool(firstFixedBufferSize, secondPoolMaxBuffer int) *TwoLevelPool {
	bp := NewByteSlicePool(64, secondPoolMaxBuffer, 2)
	return NewTwoLevelPool(firstFixedBufferSize, bp)
}

func NewTwoLevelPool(firstFixedBufferSize int, secondPool *ByteSlicePool) *TwoLevelPool {
	if secondPool == nil {
		panic("secondPool cannot be nil")
	}
	return &TwoLevelPool{
		firstFixedBufferSize: firstFixedBufferSize,
		firstFixedBuffer:     make([]byte, firstFixedBufferSize),
		secondPool:           secondPool,
	}
}

func (c *TwoLevelPool) Acquire(byteSize int) (first []byte, second *ByteSlice) {
	if byteSize == 0 {
		panic("allocator: byteSize is equal to 0")
	}
	if byteSize <= c.firstFixedBufferSize {
		return c.firstFixedBuffer, nil
	}
	return nil, c.secondPool.Acquire(byteSize)
}

func (c *TwoLevelPool) Release(b *ByteSlice) {
	c.secondPool.Release(b)
}
