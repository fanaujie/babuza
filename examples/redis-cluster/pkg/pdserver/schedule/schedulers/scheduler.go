package schedulers

import (
	"github.com/fanaujie/babuza/examples/redis-cluster/pkg/pdserver/schedule/infostore"
	"github.com/fanaujie/babuza/examples/redis-cluster/pkg/pdserver/schedule/operator"
	"time"
)

const (
	maxScheduleRetries  = 12
	maxScheduleInterval = time.Second * 30
	minScheduleInterval = time.Millisecond * 10
)

type Scheduler interface {
	Name() string
	AllowSchedule() bool
	NextCheckInterval() time.Duration
	Schedule(manager infostore.InfoManager) operator.Operator
}
