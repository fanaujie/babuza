package pdserver

import (
	"context"
	"github.com/fanaujie/babuza/examples/redis-cluster/pkg/pb"
)

func (s *PDServer) StoreHeartbeat(ctx context.Context, req *pb.StoreHeartbeatReq) (*pb.StoreHeartbeatResp, error) {
	return s.coordinator.DoStoreHeartbeat(*req)
}

func (s *PDServer) RaftGroupLeaderHeartbeat(ctx context.Context, req *pb.RaftGroupLeaderHeartbeatReq) (*pb.RaftGroupLeaderHeartbeatResp, error) {
	return s.coordinator.DoRaftGroupLeaderHeartbeat(*req)
}
