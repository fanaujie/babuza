package multiraft

import (
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"go.etcd.io/etcd/raft/v3"
)

type transportProcessor struct {
	*Node
}

//TODO: verify message

func (d *transportProcessor) ProcessBatchMessage(batchMsg babuzapb.BatchMessage) {
	groupID := ibabuza.RaftGroupID(batchMsg.ClusterID)
	r, err := d.getReplica(groupID)
	if err != nil {
		d.logger.Errorf("Node[%d] ProcessBatchMessage groupID[%d] get replica error: %v", d.config.NodeID, groupID, err)
		return
	}
	if err = r.EnqueueStep(batchMsg); err != nil {
		d.logger.Errorf("Node[%d] ProcessBatchMessage groupID[%d] enqueue step error: %v", d.config.NodeID, groupID, err)
	}
}

func (d *transportProcessor) ProcessSnapshotMessage(msg babuzapb.SnapshotMessage) {
	groupID := ibabuza.RaftGroupID(msg.ClusterID)
	r, err := d.Node.getReplica(groupID)
	if err != nil {
		d.logger.Errorf("Node[%d] ProcessSnapshotMessage groupID[%d] get replica error: %v",
			d.config.NodeID, groupID, err)
		return
	}
	r.receivedSnapshotMsgCh <- msg
}

func (d *transportProcessor) GetClusterPeer(req babuzapb.GetClusterPeersRequest) babuzapb.GetClusterPeersResponse {
	groupID := ibabuza.RaftGroupID(req.ClusterID)
	r, err := d.getReplica(groupID)
	if err == nil {
		return babuzapb.GetClusterPeersResponse{
			Status:  babuzapb.SUCCESS,
			Message: "success",
			Peers:   r.cluster.Peers()}
	} else {
		d.logger.Errorf("Node[%d] GetClusterPeer groupID[%d] get replica error: %v", d.config.NodeID, groupID, err)
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
	r, err := d.getReplica(groupID)
	if err != nil {
		d.logger.Errorf("Node[%d] ReportUnreachable groupID[%d] get replica error: %v", d.config.NodeID, groupID, err)
		return
	}
	if err = r.EnqueueReportUnreachable(nodeID); err != nil {
		d.logger.Errorf("Node[%d] ReportUnreachable groupID[%d] enqueue unreachable error: %v", d.config.NodeID, groupID, err)
	}

}
func (d *transportProcessor) ReportSnapshot(groupID ibabuza.RaftGroupID, nodeID uint64, status raft.SnapshotStatus) {
	r, err := d.getReplica(groupID)
	if err != nil {
		d.logger.Errorf("Node[%d] ReportSnapshot groupID[%d] get replica error: %v", d.config.NodeID, groupID, err)
		return
	}
	if err = r.EnqueueReportSnapshot(nodeID, status); err != nil {
		d.logger.Errorf("Node[%d] ReportSnapshot groupID[%d] enqueue snapshot error: %v", d.config.NodeID, groupID, err)
	}
}

func (d *transportProcessor) CreateSnapshotReader(groupID ibabuza.RaftGroupID, snapshotIndex uint64) (ibabuza.SnapshotReader, error) {
	r, err := d.getReplica(groupID)
	if err != nil {
		return nil, err
	}
	snapReader, err := r.storage.CreateSnapshotReader(snapshotIndex)
	if err != nil {
		return nil, err
	}
	return snapReader, nil
}
