package session

import (
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
)

type LruMgrOptions struct {
	maxSessions         int64
	snapshotCompression babuzapb.SnapshotFileCompressionType
}

type SetLruMgrOptions func(opt *LruMgrOptions)

func SetLruMgrOptionsWithMaxSessions(d int64) SetLruMgrOptions {
	return func(opt *LruMgrOptions) {
		opt.maxSessions = d
	}
}
func SetLruMgrOptionsWithSnapshotCompressionType(d babuzapb.SnapshotFileCompressionType) SetLruMgrOptions {
	return func(opt *LruMgrOptions) {
		opt.snapshotCompression = d
	}
}

type ExpiredMgrOptions struct {
	expiredNanoseconds  int64
	snapshotCompression babuzapb.SnapshotFileCompressionType
}

type SetExpiredMgrOptions func(opt *ExpiredMgrOptions)

func SetExpiredMgrOptionsWithExpiredNanoseconds(d int64) SetExpiredMgrOptions {
	return func(opt *ExpiredMgrOptions) {
		opt.expiredNanoseconds = d
	}
}

func SetExpiredMgrOptionsWithSnapshotCompressionType(d babuzapb.SnapshotFileCompressionType) SetExpiredMgrOptions {
	return func(opt *ExpiredMgrOptions) {
		opt.snapshotCompression = d
	}
}
