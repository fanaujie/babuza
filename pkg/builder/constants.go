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
	BabuzaWal     = "babuza-wal"
	ETCDWal       = "etcd-wal"
	LsmtWalDisk   = "lsmt-wal"
	LsmtWalMemory = "lsmt-wal-memory"

	// Snapshot implementations
	DurableSnapshot  = "durable"
	VolatileSnapshot = "volatile"
	MinIOSnapshot    = "minio"

	// State machine types
	StateMachineMemory                           = "memory"
	StateMachineMemoryWithConcurrentSnapshotType = "memory-concurrent"
	StateMachineDisk                             = "disk"
)
