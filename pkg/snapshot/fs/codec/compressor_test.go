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


package codec

import (
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/snapshot/fs/crcfile"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"
)

func TestCompressor(t *testing.T) {
	testCase := []babuzapb.SnapshotFileCompressionType{babuzapb.SnapshotFileCompression_None, babuzapb.SnapshotFileCompression_Snappy}
	for _, tc := range testCase {
		func(compressType babuzapb.SnapshotFileCompressionType) {
			tmpFile, err := os.CreateTemp("", "snapshot-compress")
			assert.Nil(t, err)
			defer os.Remove(tmpFile.Name())
			crcW := crcfile.CreateWriter(tmpFile)
			compressW, err := CreateCompressor(compressType, crcW)
			assert.Nil(t, err)
			jsonStr := `
{
    "glossary": {
        "title": "example glossary",
		"GlossDiv": {
            "title": "Start",
			"GlossList": {
                "GlossEntry": {
                    "ID": "SGML",
					"SortAs": "SGML",
					"GlossTerm": "Standard Generalized Markup Language",
					"Acronym": "SGML",
					"Abbrev": "ISO 8879:1986",
					"GlossDef": {
                        "para": "A meta-markup language, used to create markup languages such as DocBook.",
						"GlossSeeAlso": ["GML", "XML"]
                    },
					"GlossSee": "markup"
                }
            }
        }
    }
}
`
			data := []byte(jsonStr)
			_, err = compressW.Write(data)
			assert.Nil(t, err)
			assert.Nil(t, compressW.Close())
			//check size
			fileInfo, err := os.Stat(tmpFile.Name())
			assert.Nil(t, err)
			assert.Equal(t, fileInfo.Size(), int64(crcW.FileSize()))

			tmpFile, err = os.Open(tmpFile.Name())
			assert.Nil(t, err)
			crcR := crcfile.CreateReader(tmpFile)
			decompressR, eErr := CreateDeCompressor(compressType, crcR)
			assert.Nil(t, eErr)
			readData, eErr := ioutil.ReadAll(decompressR)
			assert.Nil(t, eErr)
			assert.Equal(t, data, readData)
			assert.Equal(t, fileInfo.Size(), int64(crcR.FileSize()))
		}(tc)
	}
}

func TestBufferWrapper(t *testing.T) {
	p, err := ioutil.TempFile("", "snapshot")
	assert.Nil(t, err)
	defer os.RemoveAll(p.Name())

	bw := newBufferWrapper(p)
	data := make([]byte, bufferSize+bufferSize/2)
	rand.Read(data)
	n, err := bw.Write(data)
	assert.Nil(t, err)
	assert.Equal(t, len(data), n)
	assert.Nil(t, bw.Close())

	pf, err := os.Open(p.Name())
	assert.Nil(t, err)
	defer pf.Close()
	readData, err := ioutil.ReadAll(pf)
	assert.Nil(t, err)
	assert.Equal(t, data, readData)
}

func TestSnappyWrapper(t *testing.T) {
	p, err := ioutil.TempFile("", "snapshot")
	assert.Nil(t, err)
	defer os.RemoveAll(p.Name())

	bw := newSnappyWrapper(p)
	data := make([]byte, bufferSize+bufferSize/2)
	rand.Read(data)
	n, err := bw.Write(data)
	assert.Nil(t, err)
	assert.Equal(t, len(data), n)
	assert.Nil(t, bw.Close())

	pf, err := os.Open(p.Name())
	assert.Nil(t, err)
	defer pf.Close()
	readData, err := ioutil.ReadAll(pf)
	assert.Nil(t, err)
	assert.NotEqual(t, data, readData)
}
