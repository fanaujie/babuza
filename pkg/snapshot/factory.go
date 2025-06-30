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
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/pkg/snapshot/fs/cloudstorage"
	"github.com/fanaujie/babuza/pkg/snapshot/fs/durable"
	"github.com/fanaujie/babuza/pkg/snapshot/fs/volatile"
	"github.com/fanaujie/babuza/pkg/utility/fileutil"
)

type Options struct {
	snapshotVersion  uint64
	maxKeepSnapFiles uint
}

type SetOptions func(opt *Options)

func SetOptsWithMaxKeepSnapFiles(d uint) SetOptions {
	return func(opt *Options) {
		opt.maxKeepSnapFiles = d
	}
}

func SetOptsWithSnapshotVersion(d uint64) SetOptions {
	return func(opt *Options) {
		opt.snapshotVersion = d
	}
}

func NewDurableSnapshotManager(snapshotDir string, logger ibabuza.Logger, options ...SetOptions) ibabuza.SnapshotManager {
	defaultOpt := Options{
		snapshotVersion:  1,
		maxKeepSnapFiles: 3,
	}
	for _, setOpt := range options {
		setOpt(&defaultOpt)
	}
	if !fileutil.Exist(snapshotDir) {
		if err := fileutil.CreateDirAndTouch(snapshotDir); err != nil {
			logger.Panicf("failed to create snapshot dir %s: %v", snapshotDir, err)
			return nil
		}
	}
	fs := durable.NewSnapshotFS()
	logger.Infof("durable snapshot manager: creating durable snapshot manager with snapshotDir=%s", snapshotDir)
	return New(Config{
		SnapshotVersion: defaultOpt.snapshotVersion,
		MaxSnapFiles:    defaultOpt.maxKeepSnapFiles,
		SnapshotDir:     snapshotDir,
	}, fs, logger, nil)
}

func NewVolatileSnapshotManager(snapshotDir string, logger ibabuza.Logger, options ...SetOptions) ibabuza.SnapshotManager {
	defaultOpt := Options{
		snapshotVersion:  1,
		maxKeepSnapFiles: 3,
	}
	for _, setOpt := range options {
		setOpt(&defaultOpt)
	}
	fs := volatile.NewFileSystem()
	logger.Infof("volatile snapshot manager: creating volatile snapshot manager with snapshotDir=%s", snapshotDir)
	return New(Config{
		SnapshotVersion: defaultOpt.snapshotVersion,
		MaxSnapFiles:    defaultOpt.maxKeepSnapFiles,
		SnapshotDir:     snapshotDir,
	}, fs, logger, nil)
}

func NewMinIOSnapshotManager(snapshotDir string, config cloudstorage.Config, logger ibabuza.Logger,
	options ...SetOptions) ibabuza.SnapshotManager {
	defaultOpt := Options{
		snapshotVersion:  1,
		maxKeepSnapFiles: 3,
	}
	for _, setOpt := range options {
		setOpt(&defaultOpt)
	}
	fs, err := cloudstorage.NewMinioSnapshotFS(config)
	if err != nil {
		panic("failed to create minio snapshot fs: " + err.Error())
	}
	logger.Infof("volatile snapshot manager: creating volatile snapshot manager with snapshotDir=%s", snapshotDir)
	return New(Config{
		SnapshotVersion: defaultOpt.snapshotVersion,
		MaxSnapFiles:    defaultOpt.maxKeepSnapFiles,
		SnapshotDir:     snapshotDir,
	}, fs, logger, nil)
}
