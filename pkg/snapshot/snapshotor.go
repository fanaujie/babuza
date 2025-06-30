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


package snapshot

import (
	"errors"
	"fmt"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/snapshot/fs/api"
	"github.com/fanaujie/babuza/pkg/snapshot/fs/codec"
	"github.com/fanaujie/babuza/pkg/snapshot/io"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/wal/walpb"
	"sort"
	"sync"
)

type purgeRequest struct {
	groupID  ibabuza.RaftGroupID
	snapshot raftpb.Snapshot
}

type Config struct {
	SnapshotVersion uint64
	MaxSnapFiles    uint
	SnapshotDir     string
}

type Snapshotor struct {
	config            Config
	fs                api.SnapshotFileSystem
	installedSnapshot map[uint64]struct{}
	metadataCodec     *codec.Metadata
	fileValidator     *io.FileValidator
	logger            ibabuza.Logger
	mu                sync.Mutex
	purgeRequestCh    chan purgeRequest
	stopCh            chan struct{}
	once              sync.Once
}

func New(config Config, fs api.SnapshotFileSystem, logger ibabuza.Logger, purgeRequestCh chan purgeRequest) *Snapshotor {
	mc := &codec.Metadata{}
	s := &Snapshotor{
		config:            config,
		fs:                fs,
		metadataCodec:     mc,
		fileValidator:     io.NewFileValidator(fs, mc),
		logger:            logger,
		installedSnapshot: make(map[uint64]struct{}),
		stopCh:            make(chan struct{}),
	}
	if purgeRequestCh == nil {
		s.purgeRequestCh = make(chan purgeRequest, 1)
	} else {
		s.purgeRequestCh = purgeRequestCh
	}
	return s
}

func (s *Snapshotor) ScanInstalledSnapshots(removeUnfinishedSnapshotDir bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.scanInstalledSnapshot(); err != nil {
		return err
	}
	if removeUnfinishedSnapshotDir {
		if err := s.removeTempDir(); err != nil {
			return err
		}
	}
	return nil
}

func (s *Snapshotor) CreateInstalledSnapshotReader(snapshotIndex uint64, validateFsmFiles bool) (ibabuza.SnapshotReader, error) {
	m, err := s.getInstalledSnapshotMetadata(snapshotIndex)
	if err != nil {
		return nil, err
	}
	dir, err := s.fs.PathHelper().GenerateSnapshotFolderPath(s.config.SnapshotDir, babuzapb.SnapshotFolderType_InstallSnapshot, snapshotIndex)
	if err != nil {
		return nil, err
	}
	if validateFsmFiles {
		if err = s.fileValidator.ValidateSnapshotFiles(dir, m); err != nil {
			return nil, err
		}
	}
	return io.NewReader(s.fs, dir, m, s.metadataCodec), nil
}

func (s *Snapshotor) CreateAtomicSnapshotWriter(snapshotTerm, snapshotIndex uint64) (ibabuza.AtomicSnapshotWriter, error) {
	createdDir, err := s.fs.CreateDirAndTouch(s.config.SnapshotDir, babuzapb.SnapshotFolderType_TempWrite, snapshotIndex)
	if err != nil {
		return nil, err
	}
	return io.NewWriter(s.fs, createdDir, s.metadataCodec, s, snapshotIndex), nil
}

func (s *Snapshotor) CreateAtomicSnapshotReceiver(metadata babuzapb.SnapshotMetadata) (ibabuza.AtomicSnapshotReceiver, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.installedSnapshot[metadata.Snapshot.Metadata.Index]
	if ok {
		return nil, fmt.Errorf("snapshot: already register snapshot index=%d", metadata.Snapshot.Metadata.Index)
	}
	createdDir, err := s.fs.CreateDirAndTouch(s.config.SnapshotDir, babuzapb.SnapshotFolderType_TempReceive, metadata.Snapshot.Metadata.Index)
	if err != nil {
		return nil, err
	}
	return io.NewReceiver(s.fs, createdDir, metadata, s.metadataCodec, s, s.fileValidator), nil
}

func (s *Snapshotor) LoadLastValidSnapshot(walSnaps []walpb.Snapshot) (*raftpb.Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	installSnapshot := s.getInstalledSnapshotIndexSlice()
	if len(installSnapshot) > 0 {
		for index := len(installSnapshot) - 1; index >= 0; index-- {
			meta, err := s.getInstalledSnapshotMetadata(installSnapshot[index])
			if err != nil {
				return nil, err
			}
			for i := len(walSnaps) - 1; i >= 0; i-- {
				if meta.Snapshot.Metadata.Term == walSnaps[i].Term && meta.Snapshot.Metadata.Index == walSnaps[i].Index {
					m := meta.Snapshot
					return &m, nil
				}
			}
		}
	}
	return nil, nil
}

func (s *Snapshotor) Purger() ibabuza.SnapshotPurger {
	return &purger{
		Snapshotor: s,
	}
}

type purger struct {
	*Snapshotor
}

func (p *purger) Start() {
	p.once.Do(func() {
		go func() {
			for {
				select {
				case <-p.stopCh:
					return
				case req := <-p.purgeRequestCh:
					if err := p.purgeSnapshot(req.snapshot); err != nil {
						p.logger.Errorf("failed to purge snapshot index=%d: %v", req.snapshot.Metadata.Index, err)
					}
				}
			}
		}()
	})
}

func (s *Snapshotor) purgeSnapshot(snapshot raftpb.Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	installSnapshot := s.getInstalledSnapshotIndexSlice()
	for len(installSnapshot) > int(s.config.MaxSnapFiles) {
		if installSnapshot[0] < snapshot.Metadata.Index {
			dir, err := s.fs.PathHelper().GenerateSnapshotFolderPath(s.config.SnapshotDir, babuzapb.SnapshotFolderType_InstallSnapshot, installSnapshot[0])
			if err != nil {
				return err
			}
			if err = s.fs.RemoveDir(dir); err != nil {
				return err
			}
			delete(s.installedSnapshot, installSnapshot[0])
		}
		installSnapshot = installSnapshot[1:]
	}
	return nil
}

func (s *Snapshotor) Purge(snapshot raftpb.Snapshot) error {
	s.purgeRequestCh <- purgeRequest{
		snapshot: snapshot,
	}
	return nil
}

func (s *Snapshotor) Close() error {
	select {
	case <-s.stopCh:
		return nil
	default:
		close(s.stopCh)
	}
	return nil
}

func (s *Snapshotor) CommitSnapshot(folderType babuzapb.SnapshotFolderType, snapshotIndex uint64) error {
	return s.commitSnapshot(folderType, snapshotIndex)
}

func (s *Snapshotor) SnapshotVersion() uint64 {
	return s.config.SnapshotVersion
}

func (s *Snapshotor) getInstalledSnapshotMetadata(snapIndex uint64) (babuzapb.SnapshotMetadata, error) {
	_, ok := s.installedSnapshot[snapIndex]
	if !ok {
		return babuzapb.SnapshotMetadata{}, fmt.Errorf("snapshot: not found snapshot index=%d", snapIndex)
	}
	dir, err := s.fs.PathHelper().GenerateSnapshotFolderPath(s.config.SnapshotDir, babuzapb.SnapshotFolderType_InstallSnapshot, snapIndex)
	if err != nil {
		return babuzapb.SnapshotMetadata{}, err
	}
	metadataFilePath, err := s.fs.PathHelper().GenerateSnapshotFilePath(dir, babuzapb.SnapshotFileType_Metadata, snapIndex, "")
	if err != nil {
		return babuzapb.SnapshotMetadata{}, err
	}
	or, err := s.fs.FileRead(metadataFilePath)
	if err != nil {
		return babuzapb.SnapshotMetadata{}, err
	}
	defer or.Close()
	m, err := s.metadataCodec.Decode(or)
	if err != nil {
		return babuzapb.SnapshotMetadata{}, err
	}
	return m, nil
}

func (s *Snapshotor) removeTempDir() error {
	tmpWriter, tmpReceiver, err := s.fs.ScanTempSnapshotFolder(s.config.SnapshotDir)
	if err != nil {
		return err
	}
	for _, dir := range tmpWriter {
		if err = s.fs.RemoveDir(dir); err != nil {
			return err
		}
	}
	for _, dir := range tmpReceiver {
		if err = s.fs.RemoveDir(dir); err != nil {
			return err
		}
	}
	return nil
}

func (s *Snapshotor) scanInstalledSnapshot() error {

	installedFolder, err := s.fs.ScanInstalledSnapshot(s.config.SnapshotDir)
	if err != nil {
		return err
	}
	for _, snapshotIndex := range installedFolder {
		dir, err := s.fs.PathHelper().GenerateSnapshotFolderPath(s.config.SnapshotDir, babuzapb.SnapshotFolderType_InstallSnapshot, snapshotIndex)
		if err != nil {
			return err
		}
		if m, err := s.fileValidator.GetMetadataFile(dir); err != nil {
			//if err = os.Rename(installPath, filepath.Join(snapshotDir, snapshot.GetBrokenFolderName(snapshotIndex))); err != nil {
			//	return nil, err
			//}
			continue
		} else {
			if m.Version != s.config.SnapshotVersion {
				//TODO:
			}
			if err = s.fileValidator.ValidateSnapshotFiles(dir, m); err != nil {
				return err
			}
			s.installedSnapshot[snapshotIndex] = struct{}{}
		}
	}
	return nil
}

func (s *Snapshotor) getInstalledSnapshotIndexSlice() []uint64 {
	var install []uint64
	for index, _ := range s.installedSnapshot {
		install = append(install, index)
	}
	sort.Sort(uintSlice(install))
	return install
}

func (s *Snapshotor) commitSnapshot(folderType babuzapb.SnapshotFolderType, snapshotIndex uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.installedSnapshot[snapshotIndex]
	if ok {
		return errors.New(fmt.Sprintf("snapshot: the installed snapshot already exists. (snapshot index=%d)", snapshotIndex))
	}
	if err := s.fs.InstallSnapshotFromTempFolder(s.config.SnapshotDir, folderType, snapshotIndex); err != nil {
		return err
	}
	if err := s.fs.SyncDir(s.config.SnapshotDir); err != nil {
		return err
	}
	s.installedSnapshot[snapshotIndex] = struct{}{}
	return nil
}

type uintSlice []uint64

func (p uintSlice) Len() int           { return len(p) }
func (p uintSlice) Less(i, j int) bool { return p[i] < p[j] }
func (p uintSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
