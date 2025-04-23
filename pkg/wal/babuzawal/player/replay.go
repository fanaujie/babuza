package player

import (
	"errors"
	"github.com/fanaujie/babuza/pkg/utility/allocator"
	"github.com/fanaujie/babuza/pkg/wal/babuzawal/iwal"
	"github.com/fanaujie/babuza/pkg/wal/babuzawal/utility"
	"go.etcd.io/etcd/server/v3/wal/walpb"
	"io"
	"path/filepath"
)

var (
	ErrNotMaximumRepairedFileId = errors.New("only repair the file whose id is the maximum")
)

type Player struct {
	walDir        string
	logFiles      utility.LogFileDescSlice
	startSnapshot walpb.Snapshot
	memPool       *allocator.TwoLevelPool
}

func Create(walDir string, startSnapshot walpb.Snapshot, memPool *allocator.TwoLevelPool) (*Player, error) {
	logFiles, err := utility.ReadValidLogFiles(walDir, startSnapshot)
	if err != nil {
		return nil, err
	}
	return &Player{
		walDir:        walDir,
		logFiles:      logFiles,
		startSnapshot: startSnapshot,
		memPool:       memPool,
	}, nil
}

func (p *Player) Replay(result iwal.ReplayWalResult, repair bool) error {

	if repair == false {
		if _, err := p.replay(result); err != nil {
			return err
		}
	} else {
		repairOnce := false
		for {
			if readErrFileDesc, err := p.replay(result); err != nil {
				// repair error: io.ErrUnexpectedEOF and parser.ErrCRCMismatchForOthersMessage
				// if err is ErrCRCMismatchForMessage, the segment file can not be repaired.
				if repairOnce == false && (err == io.ErrUnexpectedEOF || err == ErrCRCMismatchForOthersMessage) {
					lastDesc := p.logFiles[len(p.logFiles)-1]
					if readErrFileDesc.Id != p.logFiles[len(p.logFiles)-1].Id {
						return ErrNotMaximumRepairedFileId
					}
					if err = utility.RepairLogFile(filepath.Join(p.walDir, lastDesc.GetLogFileName()), result.LastValidLogOffset()); err != nil {
						return err
					}
					result.Reset()
					repairOnce = true
					continue
				}
				return err
			}
			break
		}
	}
	return nil
}

func (p *Player) replay(result iwal.ReplayWalResult) (iwal.LogFileDesc, error) {
	parser := NewParser(result, p.startSnapshot, p.memPool)
	lastLogFileDesc := iwal.LogFileDesc{}
	for _, desc := range p.logFiles {
		reader, err := utility.GetLogFileReader(p.walDir, desc)
		if err != nil {
			return desc, err
		}
		if err = parser.Parse(reader); err != io.EOF {
			return desc, err
		}
		lastLogFileDesc = desc
	}
	return lastLogFileDesc, nil
}
