package connpool

type Connection interface {
	Close() error
}

type ConnectionCreator interface {
	Create(address string) (Connection, error)
}

type Pool interface {
	Get(address string) (Connection, error)
	Put(conn Connection) error
	Remove(conn Connection) error
	Close() error
	GetActiveConnectionCount(address string) int
	GetIdleConnectionCount(address string) int
}
