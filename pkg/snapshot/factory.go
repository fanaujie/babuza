package snapshot

import (
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/pkg/snapshot/fs/durable"
	"github.com/fanaujie/babuza/pkg/snapshot/fs/volatile"
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
	fs := durable.NewFileSystem()
	logger.Infof("durable snapshot manager: creating durable snapshot manager with snapshotDir=%s", snapshotDir)
	return New(Config{
		SnapshotVersion: defaultOpt.snapshotVersion,
		MaxSnapFiles:    defaultOpt.maxKeepSnapFiles,
		SnapshotDir:     snapshotDir,
	}, fs, logger)
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
	}, fs, logger)
}
