package multiraft

import (
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

type transportProcessor struct {
	*Node
}

func (d *transportProcessor) ProcessBatchMessage(batchMsg babuzapb.BatchMessage) {
	groupID := ibabuza.RaftGroupID(batchMsg.GroupID)
	r, err := d.getReplica(groupID)
	if err != nil {
		d.logger.Errorf("Node[%d] ProcessBatchMessage groupID[%d] get replica error: %v", d.config.NodeID, groupID, err)
		return
	}
	if batchMsg.ClusterID != r.cluster.ClusterID() {
		d.logger.Warningf("Node[%d] ProcessBatchMessage groupID[%d] cluster id %d not match %d",
			d.config.NodeID, groupID, batchMsg.ClusterID, r.cluster.ClusterID())
		return
	}
	if batchMsg.Messages[0].To != r.cluster.LocalPeerID() {
		d.logger.Warningf("Node[%d] ProcessBatchMessage groupID[%d] received batch message with different peer id(%d)",
			d.config.NodeID, groupID, batchMsg.Messages[0].To)
		return
	}
	if err = r.EnqueueStep(batchMsg); err != nil {
		d.logger.Warningf("Node[%d] ProcessBatchMessage groupID[%d] enqueue step error: %v", d.config.NodeID, groupID, err)
	}
}

func (d *transportProcessor) ProcessSnapshotMessage(msg babuzapb.SnapshotMessage) babuzapb.SnapshotMessageResponse {
	groupID := ibabuza.RaftGroupID(msg.GroupID)
	r, err := d.Node.getReplica(groupID)
	if err != nil {
		d.logger.Warningf("Node[%d] ProcessSnapshotMessage groupID[%d] get replica error: %v",
			d.config.NodeID, groupID, err)
		return babuzapb.SnapshotMessageResponse{
			Status:  babuzapb.REJECTED,
			Message: "Failed to get replica: " + err.Error(),
		}
	}
	if msg.ClusterID != r.cluster.ClusterID() {
		d.logger.Warningf("Node[%d] ProcessSnapshotMessage groupID[%d] cluster id %d not match %d",
			d.config.NodeID, groupID, msg.ClusterID, r.cluster.ClusterID())
		return babuzapb.SnapshotMessageResponse{
			Status:  babuzapb.REJECTED,
			Message: "Cluster ID mismatch",
		}
	}
	if msg.To != r.cluster.LocalPeerID() {
		d.logger.Warningf("Node[%d] ProcessSnapshotMessage groupID[%d] received snapshot message with different peer id(%d)",
			d.config.NodeID, groupID, msg.To)
		return babuzapb.SnapshotMessageResponse{
			Status:  babuzapb.REJECTED,
			Message: "Peer ID mismatch",
		}
	}
	switch msg.Type {
	case babuzapb.SnapshotMessageType_Metadata:
		if err = r.storage.MetadataSnapshotMessage(msg); err != nil {
			d.logger.Warningf("Node[%d] groupID[%d] failed to process snapshot metadata. err(%s)",
				r.cluster.LocalPeerID(), msg.GroupID, err.Error())
			return babuzapb.SnapshotMessageResponse{
				Status:  babuzapb.FAILED,
				Message: "Failed to receive snapshot metadata: " + err.Error(),
			}
		}
	case babuzapb.SnapshotMessageType_Chunk:
		if err = r.storage.ChunkSnapshotMessage(msg); err != nil {
			r.logger.Warningf("Node[%d] groupID[%d] failed to process snapshot chunk. err(%s)",
				r.cluster.LocalPeerID(), msg.GroupID, err.Error())
			return babuzapb.SnapshotMessageResponse{
				Status:  babuzapb.FAILED,
				Message: "Failed to receive snapshot chunk: " + err.Error(),
			}
		}
	case babuzapb.SnapshotMessageType_Finish:
		if err = r.storage.FinishSnapshotMessage(msg); err != nil {
			r.logger.Warningf("Node[%d] groupID[%d] failed to process snapshot finish. err(%s)",
				r.cluster.LocalPeerID(), msg.GroupID, err.Error())
			return babuzapb.SnapshotMessageResponse{
				Status:  babuzapb.FAILED,
				Message: "Failed to finish snapshot: " + err.Error(),
			}
		} else {
			if err = r.EnqueueStep(babuzapb.BatchMessage{
				ClusterID: msg.ClusterID,
				GroupID:   uint64(groupID),
				Messages: []raftpb.Message{
					msg.FinishMessage,
				},
			}); err != nil {
				r.logger.Warningf("Node[%d] groupID[%d] failed to enqueue finish message. err(%s)",
					r.cluster.LocalPeerID(), msg.GroupID, err.Error())
				return babuzapb.SnapshotMessageResponse{
					Status:  babuzapb.FAILED,
					Message: "Failed to enqueue finish message: " + err.Error(),
				}
			}
		}
	default:
		r.logger.Warningf("Node[%d] groupID[%d] unknown snapshot message type %d",
			r.cluster.LocalPeerID(), msg.GroupID, msg.Type)
		return babuzapb.SnapshotMessageResponse{
			Status:  babuzapb.REJECTED,
			Message: "Unknown snapshot message type",
		}
	}
	return babuzapb.SnapshotMessageResponse{
		Status:  babuzapb.SUCCESS,
		Message: "success",
	}
}

func (d *transportProcessor) GetClusterPeer(req babuzapb.GetClusterPeersRequest) babuzapb.GetClusterPeersResponse {
	groupID := ibabuza.RaftGroupID(req.GroupID)
	r, err := d.getReplica(groupID)
	if err == nil {
		if req.ClusterID != r.cluster.ClusterID() {
			d.logger.Warningf("Node[%d] GetClusterPeer groupID[%d] cluster id %d not match %d",
				d.config.NodeID, groupID, req.ClusterID, r.cluster.ClusterID())
			return babuzapb.GetClusterPeersResponse{
				Status:  babuzapb.REJECTED,
				Message: "cluster id not match",
			}
		}
		if req.To != r.cluster.LocalPeerID() {
			d.logger.Warningf("Node[%d] GetClusterPeer groupID[%d] received get cluster peer message with different peer id(%d)",
				d.config.NodeID, groupID, req.To)
			return babuzapb.GetClusterPeersResponse{
				Status:  babuzapb.REJECTED,
				Message: "peer id not match",
			}
		}
		return babuzapb.GetClusterPeersResponse{
			Status:  babuzapb.SUCCESS,
			Message: "success",
			Peers:   r.cluster.Peers()}
	}
	d.logger.Warningf("Node[%d] GetClusterPeer groupID[%d] get replica error: %v", d.config.NodeID, groupID, err)
	return babuzapb.GetClusterPeersResponse{
		Status:  babuzapb.REJECTED,
		Message: "group not found",
	}

}

func (d *transportProcessor) PublishApplicationService(req babuzapb.PublishApplicationServiceRequest) babuzapb.PublishApplicationServiceResponse {
	return babuzapb.PublishApplicationServiceResponse{}
}

func (d *transportProcessor) ReportUnreachable(groupID ibabuza.RaftGroupID, nodeID uint64) {
	r, err := d.getReplica(groupID)
	if err != nil {
		d.logger.Warningf("Node[%d] ReportUnreachable groupID[%d] get replica error: %v", d.config.NodeID, groupID, err)
		return
	}
	if err = r.ReportUnreachable(nodeID); err != nil {
		d.logger.Warningf("Node[%d] ReportUnreachable groupID[%d] enqueue unreachable error: %v", d.config.NodeID, groupID, err)
	}

}
func (d *transportProcessor) ReportSnapshot(groupID ibabuza.RaftGroupID, nodeID uint64, status raft.SnapshotStatus) {
	r, err := d.getReplica(groupID)
	if err != nil {
		d.logger.Warningf("Node[%d] ReportSnapshot groupID[%d] get replica error: %v", d.config.NodeID, groupID, err)
		return
	}
	if err = r.ReportSnapshot(nodeID, status); err != nil {
		d.logger.Warningf("Node[%d] ReportSnapshot groupID[%d] enqueue snapshot error: %v", d.config.NodeID, groupID, err)
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
