package crcfile

import (
	"github.com/stretchr/testify/assert"
	"hash/crc64"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"
)

func TestAll(t *testing.T) {

	tc := []int{1 << 10, 1 << 11, 1 << 12, 1 << 13, 1 << 14, 1 << 15, 1 << 16}
	h := crc64.New(Crc64Table)
	for _, size := range tc {
		func(testSize int) {
			data := make([]byte, testSize)
			rand.Read(data)
			h.Reset()
			h.Write(data)
			tmpFile, err := os.CreateTemp("", "snapshot-crc-file")
			assert.Nil(t, err)
			defer os.Remove(tmpFile.Name())
			cw := CreateWriter(tmpFile)
			n, eErr := cw.Write(data)
			assert.Nil(t, eErr)
			assert.Nil(t, cw.Close())
			assert.Equal(t, len(data), cw.FileSize())
			assert.Equal(t, len(data), n)
			fileInfo, err := os.Stat(tmpFile.Name())
			assert.Nil(t, err)
			assert.Equal(t, int(fileInfo.Size()), cw.FileSize())

			tmpFile, err = os.Open(tmpFile.Name())
			assert.Nil(t, err)
			cr := CreateReader(tmpFile)
			rData, err := ioutil.ReadAll(cr)
			assert.Nil(t, err)
			assert.Nil(t, cr.Close())
			assert.Equal(t, data, rData)
			assert.Equal(t, h.Sum64(), cr.Crc())
			assert.Equal(t, int(fileInfo.Size()), cr.FileSize())
		}(size)
	}
}
