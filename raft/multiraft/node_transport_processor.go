package multiraft

import (
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"go.etcd.io/etcd/raft/v3"
)

type transportProcessor struct {
	*Node
}

func (d *transportProcessor) ProcessBatchMessage(batchMsg babuzapb.BatchMessage) {
	groupID := ibabuza.RaftGroupID(batchMsg.ClusterID)
	d.replicaSet.mu.RLock()
	r, ok := d.replicaSet.replica[groupID]
	d.replicaSet.mu.RUnlock()
	if ok {
		if err := r.EnqueueStep(batchMsg); err != nil {
			d.logger.Errorf("Node[%d] ProcessBatchMessage groupID[%d] enqueue step error: %v", d.config.NodeID, groupID, err)
		}
	} else {
		d.logger.Errorf("Node[%d] ProcessBatchMessage groupID[%d] not found", d.config.NodeID, groupID)
	}
}

func (d *transportProcessor) ProcessSnapshotMessage(msg babuzapb.SnapshotMessage) {

}

func (d *transportProcessor) GetClusterPeer(req babuzapb.GetClusterPeersRequest) babuzapb.GetClusterPeersResponse {
	groupID := ibabuza.RaftGroupID(req.ClusterID)
	d.replicaSet.mu.RLock()
	r, ok := d.replicaSet.replica[groupID]
	d.replicaSet.mu.RUnlock()
	if ok {
		return babuzapb.GetClusterPeersResponse{
			Status:  babuzapb.SUCCESS,
			Message: "success",
			Peers:   r.cluster.Peers()}
	} else {
		d.logger.Warningf("Node[%d] GetClusterPeer groupID[%d] not found", d.config.NodeID, groupID)
		return babuzapb.GetClusterPeersResponse{
			Status:  babuzapb.FAILED,
			Message: "group not found",
		}
	}
}

func (d *transportProcessor) PublishApplicationService(req babuzapb.PublishApplicationServiceRequest) babuzapb.PublishApplicationServiceResponse {
	return babuzapb.PublishApplicationServiceResponse{}
}

func (d *transportProcessor) ReportUnreachable(groupID ibabuza.RaftGroupID, nodeID uint64) {
	d.replicaSet.mu.RLock()
	r, ok := d.replicaSet.replica[groupID]
	d.replicaSet.mu.RUnlock()
	if ok {
		if err := r.EnqueueReportUnreachable(nodeID); err != nil {
			d.logger.Errorf("Node[%d] ReportUnreachable groupID[%d] enqueue unreachable error: %v", d.config.NodeID, groupID, err)
		}
	} else {
		d.logger.Errorf("Node[%d] ReportUnreachable groupID[%d] not found", d.config.NodeID, groupID)
	}

}
func (d *transportProcessor) ReportSnapshot(groupID ibabuza.RaftGroupID, nodeID uint64, status raft.SnapshotStatus) {
	d.replicaSet.mu.RLock()
	r, ok := d.replicaSet.replica[groupID]
	d.replicaSet.mu.RUnlock()
	if ok {
		if err := r.EnqueueReportSnapshot(nodeID, status); err != nil {
			d.logger.Errorf("Node[%d] ReportSnapshot groupID[%d] enqueue snapshot error: %v", d.config.NodeID, groupID, err)
		}
	} else {
		d.logger.Errorf("Node[%d] ReportSnapshot groupID[%d] not found", d.config.NodeID, groupID)
	}
}

func (d *transportProcessor) TransferLeader(groupID ibabuza.RaftGroupID, fromID, transfereeID uint64) {

}

func (d *transportProcessor) CreateSnapshotReader(groupID ibabuza.RaftGroupID, snapshotIndex uint64) (ibabuza.SnapshotReader, error) {

	return nil, nil
}
