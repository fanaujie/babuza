package player

import (
	"bufio"
	"encoding/binary"
	"errors"
	"github.com/fanaujie/babuza/pkg/utility/allocator"
	"github.com/fanaujie/babuza/pkg/wal/babuzawal/codec"
	"github.com/fanaujie/babuza/pkg/wal/babuzawal/collection"
	"github.com/fanaujie/babuza/pkg/wal/babuzawal/iwal"
	"github.com/fanaujie/babuza/pkg/wal/babuzawal/pb"
	"github.com/fanaujie/babuza/pkg/wal/babuzawal/utility"
	"go.etcd.io/etcd/server/v3/wal/walpb"
	"hash/crc32"
)

var (
	ErrCRCMismatchForCrcMessage    = errors.New("corrupted crc message")
	ErrCRCMismatchForOthersMessage = errors.New("corrupted other message")
	ErrNotSupportMessage           = errors.New("not support message")
	ErrNotFoundNextEntry           = errors.New("not found next entry")
)

type Parser struct {
	result        iwal.ReplayWalResult
	startSnapshot walpb.Snapshot

	cascade       *allocator.TwoLevelPool
	parseEntry    bool
	findNextEntry bool
}

func NewParser(result iwal.ReplayWalResult, startSnapshot walpb.Snapshot,
	cascade *allocator.TwoLevelPool) *Parser {
	_, NotParseEntry := result.EntryCollection().(*collection.NopEntry)
	return &Parser{
		result:        result,
		startSnapshot: startSnapshot,
		cascade:       cascade,
		parseEntry:    !NotParseEntry,
	}
}

func (p *Parser) Parse(reader iwal.LogFileReader) error {
	p.result.SetLastLogFileDesc(reader.CurrentLogFileDesc())
	p.result.SetLastValidLogFileOffset(int64(utility.LogFileHeaderLength))
	p.findNextEntry = false

	defer reader.Close()
	logDecoder := codec.NewDecoder(bufio.NewReader(reader), p.cascade, p.logHandler)

	for {
		if err := logDecoder.Decode(); err != nil {
			return err
		}
	}
}

func (p *Parser) logHandler(logType pb.LogType, logBuf []byte, logSizeWithPadding int64, logCrc uint32) error {

	if logType == pb.LogTypeCrc {
		p.result.SetLastValidLogCrc(binary.LittleEndian.Uint32(logBuf))
		if p.result.LastValidLogCrc() != logCrc {
			return ErrCRCMismatchForCrcMessage
		}
		p.result.IncreaseLastValidLogFileOffset(logSizeWithPadding)
		return nil
	}
	p.result.SetLastValidLogCrc(crc32.Update(p.result.LastValidLogCrc(), iwal.Crc32Table, logBuf))
	if p.result.LastValidLogCrc() != logCrc {
		return ErrCRCMismatchForOthersMessage
	}
	switch logType {
	case pb.LogTypeNextEntry:
		if err := p.result.UnmarshalNextEntry(logBuf); err != nil {
			return err
		}
		p.findNextEntry = true
	case pb.LogTypeNormalEntry:
		fallthrough
	case pb.LogTypeConfChangeEntry:
		fallthrough
	case pb.LogTypeConfChangeV2Entry:
		if p.parseEntry {
			if p.findNextEntry {
				if err := p.result.EntryCollection().Decode(p.result.LastLogFileDesc().Id, p.startSnapshot.Index, logType,
					logBuf, logSizeWithPadding-codec.HeaderSize, p.result); err != nil {
					return err
				}
			} else {
				return ErrNotFoundNextEntry
			}
		}
	case pb.LogTypeMetadata:
		p.result.SetMetadata(logBuf)
	case pb.LogTypeHardState:
		if err := p.result.UnmarshalHardState(logBuf); err != nil {
			return err
		}
	case pb.LogTypeSnapshot:
		w := walpb.Snapshot{}
		if err := w.Unmarshal(logBuf); err != nil {
			return err
		}
		p.result.AppendWalSnapshots(w)
	default:
		return ErrNotSupportMessage
	}
	p.result.IncreaseLastValidLogFileOffset(logSizeWithPadding)
	return nil
}
