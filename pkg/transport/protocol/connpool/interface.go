package connpool

type Connection interface {
	Close() error
}

type ComparableConnection interface {
	Connection
	comparable
}

type ConnectionDialer[T ComparableConnection] interface {
	Dial(address string) (T, error)
}

type Pool[T ComparableConnection] interface {
	Get(address string) (T, error)
	Put(conn T) error
	Remove(conn T) error
	Close() error
	GetActiveConnectionCount(address string) int
	GetIdleConnectionCount(address string) int
}
