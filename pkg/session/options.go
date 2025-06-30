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


package session

import (
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"time"
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
	expiredTime         time.Duration
	snapshotCompression babuzapb.SnapshotFileCompressionType
}

type SetExpiredMgrOptions func(opt *ExpiredMgrOptions)

func SetExpiredMgrOptionsWithExpiredTime(d time.Duration) SetExpiredMgrOptions {
	return func(opt *ExpiredMgrOptions) {
		opt.expiredTime = d
	}
}

func SetExpiredMgrOptionsWithSnapshotCompressionType(d babuzapb.SnapshotFileCompressionType) SetExpiredMgrOptions {
	return func(opt *ExpiredMgrOptions) {
		opt.snapshotCompression = d
	}
}
