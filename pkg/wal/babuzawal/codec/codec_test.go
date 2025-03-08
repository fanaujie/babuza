package codec

import (
	"github.com/fanaujie/babuza/pkg/utility/allocator"
	"github.com/fanaujie/babuza/pkg/wal/babuzawal/pb"
	"github.com/stretchr/testify/assert"
	"hash/crc32"
	"os"
	"testing"
)

type mockDecoder struct {
	decodeLogType      pb.LogType
	decodeLogData      []byte
	logSizeWithPadding int64
}

func (m *mockDecoder) LogHandler(logType pb.LogType, logBuf []byte, logSizeWithPadding int64, logCrc uint32) error {
	m.decodeLogType = logType
	m.logSizeWithPadding = logSizeWithPadding
	m.decodeLogData = make([]byte, len(logBuf))
	copy(m.decodeLogData, logBuf)
	return nil
}

type mockLog struct {
	writeData          []byte
	encodeLastCrc      uint32
	encodeWriteDataCrc uint32
}

func (l *mockLog) Encode(buf []byte, logSize int, lastCrc uint32) (uint32, error) {
	copy(buf, l.writeData)
	l.encodeLastCrc = lastCrc
	l.encodeWriteDataCrc = crc32.Update(lastCrc, crc32Table, l.writeData)
	return l.encodeWriteDataCrc, nil
}

func TestEncoderDecoder(t *testing.T) {
	r, w, err := os.Pipe()
	assert.Nil(t, err)
	cp := allocator.NewDefaultTwoLevelPool(64, 1024)
	e := NewEncoder(w, cp, 100)
	m := &mockDecoder{}
	d := NewDecoder(r, cp, m.LogHandler)

	writeData := []byte{1, 2, 3, 4}
	mLog := &mockLog{
		writeData: writeData,
	}
	assert.Nil(t, Encode(e, pb.LogTypeConfChangeEntry, len(writeData), mLog))
	assert.Equal(t, uint32(100), mLog.encodeLastCrc)
	err = d.Decode()
	assert.Nil(t, err)
	assert.Equal(t, pb.LogTypeConfChangeEntry, m.decodeLogType)
	assert.Equal(t, writeData, m.decodeLogData)
	assert.Equal(t, mLog.encodeWriteDataCrc, crc32.Update(mLog.encodeLastCrc, crc32Table, m.decodeLogData))
}
