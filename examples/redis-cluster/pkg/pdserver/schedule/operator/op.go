package operator

import "github.com/fanaujie/babuza/examples/redis-cluster/pkg/pdserver/schedule/infostore"

type Operator interface {
	RaftGroupID() uint64
	Finish(infostore.GroupInfo) bool
	Payload() any
}
