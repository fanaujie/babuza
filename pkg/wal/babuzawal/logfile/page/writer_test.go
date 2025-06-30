package page

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"os"
	"testing"
)

func TestPageWriter_Copy(t *testing.T) {
	segmentSize := 4 * 1024 * 1024
	bufferSize := 32 * 1024
	pageSize := 256

	f, _ := os.CreateTemp("", "page-writer")
	defer os.RemoveAll(f.Name())
	pw, err := CreateWriter(segmentSize, pageSize, bufferSize, f)
	assert.Nil(t, err)
	defer f.Close()
	assert.Nil(t, err, pw.copy(make([]byte, 10)))
	assert.Equal(t, pw.bufWritePos, 10)
	assert.Nil(t, err, pw.copy(make([]byte, 100)))
	assert.Equal(t, pw.bufWritePos, 110)
	assert.Nil(t, err, pw.flush())
	assert.Equal(t, pw.bufWritePos, 0)
}

func TestPageWriter_Write(t *testing.T) {

	type testCase struct {
		segmentSize     int
		bufferSize      int
		pageSize        int
		writeCount      int
		dataMax         int
		dataMin         int
		flushAfterWrite bool
	}
	for index, tc := range []testCase{
		{
			segmentSize: 4 * 1024 * 1024,
			bufferSize:  32 * 1024,
			pageSize:    256,
			writeCount:  1024,
			dataMax:     1024,
			dataMin:     512,
		},
		{
			segmentSize: 4 * 1024 * 1024,
			bufferSize:  1024,
			pageSize:    512,
			writeCount:  8096,
			dataMax:     64,
			dataMin:     8,
		},
		{
			segmentSize:     4 * 1024 * 1024,
			bufferSize:      1024,
			pageSize:        64,
			writeCount:      8096 * 2,
			dataMax:         16,
			dataMin:         8,
			flushAfterWrite: true,
		},
	} {
		identity := fmt.Sprintf("test case(#%d): %v", index, tc)
		func() {
			f, _ := os.CreateTemp("", "page-writer")
			defer os.RemoveAll(f.Name())
			var expect []byte
			func() {
				pw, err := CreateWriter(tc.segmentSize, tc.pageSize, tc.bufferSize, f)
				assert.Nil(t, err, identity)
				defer f.Close()
				totalWrite := 0
				for i := 0; i < tc.writeCount; i++ {
					dataLen := rand.Intn(tc.dataMax-tc.dataMin+1) + tc.dataMin
					data := make([]byte, dataLen)
					rand.Read(data)
					n, err := pw.Write(data)
					assert.Nil(t, err, identity)
					assert.Equal(t, dataLen, n, identity)
					if tc.flushAfterWrite {
						assert.Nil(t, pw.flush(), identity)
					}
					totalWrite += dataLen
					assert.Equal(t, totalWrite, pw.currentOffset, identity)
					expect = append(expect, data...)
				}
				assert.Nil(t, pw.flush(), identity)
			}()
			data, err := os.ReadFile(f.Name())
			assert.Nil(t, err, identity)
			assert.Equal(t, expect, data, identity)
		}()
	}

}
