package multiraft

import (
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"go.etcd.io/etcd/raft/v3"
)

type transportProcessor struct {
	*Node
}

func (d *transportProcessor) ProcessBatchMessage(msg babuzapb.BatchMessage) {

}

func (d *transportProcessor) ProcessSnapshotMessage(msg babuzapb.SnapshotMessage) {

}

func (d *transportProcessor) GetClusterPeer(req babuzapb.GetClusterPeersRequest) babuzapb.GetClusterPeersResponse {
	return babuzapb.GetClusterPeersResponse{}
}

func (d *transportProcessor) PublishApplicationService(req babuzapb.PublishApplicationServiceRequest) babuzapb.PublishApplicationServiceResponse {
	return babuzapb.PublishApplicationServiceResponse{}
}

func (d *transportProcessor) ReportUnreachable(id uint64) {

}
func (d *transportProcessor) ReportSnapshot(id uint64, status raft.SnapshotStatus) {

}

func (d *transportProcessor) CreateSnapshotReader(groupID ibabuza.RaftGroupID, snapshotIndex uint64) (ibabuza.SnapshotReader, error) {

	return nil, nil
}
