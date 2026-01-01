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


package builder

// Constants for component types, using descriptive string values
const (
	// Session types
	NoOpSession   = "noop"
	ExpireSession = "expire"
	LRUSession    = "lru"

	// Transport protocols
	TcpTransport       = "tcp"
	TcpMemoryTransport = "tcp-memory"
	HttpTransport      = "http"
	GRPCTransport      = "grpc"

	// WAL implementations
	BabuzaWal       = "babuza-wal"
	ETCDWal         = "etcd-wal"
	BadgerWalDisk   = "badger-wal"
	BadgerWalMemory = "badger-wal-memory"
	PebbleWalDisk   = "pebble-wal"
	PebbleWalMemory = "pebble-wal-memory"

	// Snapshot implementations
	DurableSnapshot  = "durable"
	VolatileSnapshot = "volatile"
	S3Snapshot       = "s3"

	// State machine types
	StateMachineMemory                           = "memory"
	StateMachineMemoryWithConcurrentSnapshotType = "memory-concurrent"
	StateMachineDisk                             = "disk"

	// Metrics implementations
	MetricsOtel       = "otel"
	MetricsPrometheus = "prometheus"
)
