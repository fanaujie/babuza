package ibabuza

import (
	"go.etcd.io/etcd/raft/v3"
)

type Logger interface {
	raft.Logger
}
