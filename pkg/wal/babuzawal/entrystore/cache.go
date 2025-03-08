package entrystore

import (
	"go.etcd.io/etcd/raft/v3/raftpb"
)

const (
	defaultBuffer uint64 = 128
)

type CacheEntryData struct {
	Index uint64
	Data  []byte
}

type IndexRange struct {
	First uint64
	Last  uint64
	Empty bool
}

func (i IndexRange) ToOffsetIndex(firstItemIndex uint64) (uint64, uint64) {
	if i.Empty {
		return 0, 0
	}
	return i.First - firstItemIndex, i.Last - firstItemIndex
}

type Cache struct {
	ringBuf    []CacheEntryData
	bufferSize uint64
	consumePos uint64
	appendPos  uint64
	length     uint64
}

func NewCache() *Cache {
	return &Cache{
		ringBuf:    make([]CacheEntryData, defaultBuffer),
		bufferSize: defaultBuffer}

}

func (c *Cache) IndexRange() IndexRange {
	if c.length == 0 {
		return IndexRange{Empty: true}
	}
	return IndexRange{
		First: c.ringBuf[c.consumePos].Index,
		Last:  c.ringBuf[c.dec(c.appendPos)].Index,
	}
}

func (c *Cache) Append(entries []raftpb.Entry) {

	if len(entries) == 0 {
		return
	}
	consumeIndex := c.ringBuf[c.consumePos].Index
	appendLast := entries[len(entries)-1].Index
	if consumeIndex > appendLast {
		return
	}
	appendFirst := entries[0].Index
	if appendFirst < consumeIndex {
		entries = entries[consumeIndex-appendFirst:]
	}
	offset := appendFirst - consumeIndex
	switch {
	case c.length > offset:
		c.truncate(appendFirst)
		c.append(entries)
	case c.length == offset || c.length == 0:
		c.append(entries)
	}
}

func (c *Cache) Delete(toIndex uint64) {

	if c.length == 0 {
		return
	}
	firstIndex := c.ringBuf[c.consumePos].Index
	if toIndex < firstIndex {
		return
	}
	for firstIndex <= toIndex {
		c.ringBuf[c.consumePos] = CacheEntryData{} //free
		c.consumePos = c.inc(c.consumePos)
		c.length--
		if c.consumePos == c.appendPos {
			break
		}
		firstIndex = c.ringBuf[c.consumePos].Index
	}
}

func (c *Cache) Clear() {
	c.Delete(c.ringBuf[c.dec(c.appendPos)].Index)
}

// ReadEntriesData
// a = ents[0].Index
// b = ents[len(ents)-1].Index
// ReadEntriesData fills data of entry of ents in the range [a,b].
// return ture if ents's range hits cache.
func (c *Cache) ReadEntriesData(ents []raftpb.Entry) (hitCache bool) {
	if c.length == 0 || len(ents) == 0 {
		return
	}
	entsLoIndex := ents[0].Index

	firstIndex := c.ringBuf[c.consumePos].Index
	if entsLoIndex < firstIndex {
		return
	}
	lastIndex := c.ringBuf[c.dec(c.appendPos)].Index
	entsHiIndex := ents[len(ents)-1].Index
	if entsHiIndex > lastIndex {
		return
	}

	var fetchStartPos = (c.consumePos + (entsLoIndex - firstIndex)) & (c.bufferSize - 1)
	hitCache = true
	i := 0
	for entsLoIndex <= entsHiIndex {
		ents[i].Data = c.ringBuf[fetchStartPos].Data
		fetchStartPos = c.inc(fetchStartPos)
		entsLoIndex++
		i++
	}
	return
}

func (c *Cache) truncate(toIndex uint64) {
	if c.length == 0 {
		return
	}
	lastItemPos := c.dec(c.appendPos)
	lastIndex := c.ringBuf[lastItemPos].Index
	if lastIndex < toIndex {
		return
	}
	for lastIndex >= toIndex {
		c.ringBuf[lastItemPos] = CacheEntryData{} //free
		c.appendPos = lastItemPos
		c.length--
		if c.consumePos == c.appendPos {
			break
		}
		lastItemPos = c.dec(c.appendPos)
		lastIndex = c.ringBuf[lastItemPos].Index
	}
}

func (c *Cache) append(entries []raftpb.Entry) {
	for i := range entries {
		e := &entries[i]
		c.ringBuf[c.appendPos] = CacheEntryData{
			Index: e.Index,
			Data:  e.Data,
		}
		c.appendPos = c.inc(c.appendPos)
		c.length++
		c.growBuf()
	}
}

func (c *Cache) inc(p uint64) uint64 {
	return (p + 1) & (c.bufferSize - 1)
}

func (c *Cache) dec(p uint64) uint64 {
	return (p - 1) & (c.bufferSize - 1)
}

func (c *Cache) shirkBuf() {

}

func (c *Cache) growBuf() {
	if c.consumePos == c.appendPos {
		extendSize := c.bufferSize << 1
		b := make([]CacheEntryData, extendSize)
		copy(b, c.ringBuf[c.consumePos:])
		copy(b[c.bufferSize-c.consumePos:], c.ringBuf[:c.consumePos])
		c.ringBuf = b
		c.consumePos = 0
		c.appendPos = c.bufferSize
		c.bufferSize = extendSize
	}
}
