package page

import (
	"github.com/fanaujie/babuza/pkg/utility/fileutil"
	"io"
	"os"
	"sync"
)

type Writer struct {
	segmentSize   int
	pageSize      int
	extendSegSize int
	currentOffset int
	bufWritePos   int
	bufSize       int
	buffer        []byte
	f             *os.File
	mu            *sync.RWMutex
}

func CreateWriter(segmentSize, pageSize, bufSize int, f *os.File) (*Writer, error) {
	currentOffset, err := f.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, err
	}
	return &Writer{
		segmentSize:   segmentSize,
		pageSize:      pageSize,
		extendSegSize: segmentSize,
		currentOffset: int(currentOffset),
		bufSize:       bufSize,
		buffer:        make([]byte, bufSize+pageSize), //extend one page size for alignment
		f:             f,
		mu:            &sync.RWMutex{},
	}, nil
}

func (p *Writer) copy(data []byte) error {
	dataLen := len(data)
	copy(p.buffer[p.bufWritePos:], data)
	p.bufWritePos += dataLen
	return nil
}

func (p *Writer) flush() error {
	if _, err := p.f.Write(p.buffer[:p.bufWritePos]); err != nil {
		return err
	}
	p.bufWritePos = 0
	return nil
}

func (p *Writer) CurrentOffset() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.currentOffset
}

func (p *Writer) Sync(enableSync bool) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.flush(); err != nil {
		return err
	}
	if enableSync {
		return fileutil.Datasync(p.f)
	}
	return nil
}

func (p *Writer) Truncate() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.f.Truncate(int64(p.currentOffset))
}

func (p *Writer) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.f.Close()
}

func (p *Writer) Write(data []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	var writeDataLen, total = len(data), 0
	if err := p.checkPreAllocFileSpace(writeDataLen); err != nil {
		return 0, err
	}

	if p.bufWritePos+writeDataLen <= p.bufSize {
		if err := p.copy(data); err != nil {
			return 0, err
		}
		p.currentOffset += writeDataLen
		return writeDataLen, nil
	}
	// alignment
	nextAlignmentSize := p.pageSize - (p.currentOffset % p.pageSize)
	if nextAlignmentSize != p.pageSize {
		if writeDataLen < nextAlignmentSize {
			if err := p.copy(data); err != nil {
				return 0, err
			}
			p.currentOffset += writeDataLen
			return writeDataLen, nil
		}
		if err := p.copy(data[:nextAlignmentSize]); err != nil {
			return 0, err
		}
		total += nextAlignmentSize
		data = data[nextAlignmentSize:]
		writeDataLen -= nextAlignmentSize
	}
	if err := p.flush(); err != nil {
		return total, err
	}
	if writeDataLen > p.pageSize {
		pages := writeDataLen / p.pageSize
		wSize := pages * p.pageSize
		if n, err := p.f.Write(data[:wSize]); err != nil {
			return total + n, err
		}
		total += wSize
		data = data[wSize:]
		writeDataLen -= wSize
	}
	if err := p.copy(data[:writeDataLen]); err != nil {
		return total, err
	}
	p.currentOffset += total + writeDataLen
	return total + writeDataLen, nil
}

func (p *Writer) CheckCycle() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.currentOffset > p.segmentSize
}

func (p *Writer) checkPreAllocFileSpace(dataSize int) error {
	//TODO: finish
	//if p.currentOffset+dataSize > p.extendSegSize {
	//	preAllocSize := 2 * segment.LogFixedBufSize
	//	if dataSize > preAllocSize {
	//		preAllocSize = dataSize
	//	}
	//	if err := p.handle.PreAllocateFileSpace(int64(p.extendSegSize), int64(preAllocSize)); err != nil {
	//		return err
	//	}
	//	p.extendSegSize += preAllocSize
	//}
	return nil
}
