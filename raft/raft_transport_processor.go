package raft

import (
	"context"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"go.etcd.io/etcd/raft/v3"
)

type transportProcessor struct {
	*Raft
}

func (d *transportProcessor) ProcessBatchMessage(msg babuzapb.BatchMessage) {
	if msg.ClusterID != d.cluster.ClusterID() {
		d.logger.Warningf("raft[id=%d] received batch message with different cluster id(%d)", d.cluster.LocalPeerID(), msg.ClusterID)
		return
	}
	for i := 0; i < len(msg.Messages); i++ {
		if !d.isPeerInCluster(msg.Messages[i].From) {
			d.logger.Warningf("raft[id=%d] received batch message from unknown peer id(%d)", d.cluster.LocalPeerID(), msg.Messages[i].From)
			continue
		}
		if msg.Messages[i].To != d.cluster.LocalPeerID() {
			d.logger.Warningf("raft[id=%d] received batch message with local peer id(%d)", d.cluster.LocalPeerID(), msg.Messages[i].To)
			continue
		}
		if err := d.raftNode.Step(context.TODO(), msg.Messages[i]); err != nil {
			d.logger.Warningf("raft[id=%d] failed to step. err(%s)", d.cluster.LocalPeerID(), err.Error())
		}
	}
}

func (d *transportProcessor) ProcessSnapshotMessage(msg babuzapb.SnapshotMessage) {
	if msg.ClusterID != d.cluster.ClusterID() {
		d.logger.Warningf("raft[id=%d] received snapshot message with different cluster id(%d)", d.cluster.LocalPeerID(), msg.ClusterID)
		return
	}
	if !d.isPeerInCluster(msg.From) {
		d.logger.Warningf("raft[id=%d] received snapshot message from unknown peer id(%d)", d.cluster.LocalPeerID(), msg.From)
		return
	}
	if msg.To != d.cluster.LocalPeerID() {
		d.logger.Warningf("raft[id=%d] received snapshot message with different peer id(%d)", d.cluster.LocalPeerID(), msg.To)
		return
	}
	if bFinish, err := d.storage.ReceiveSnapshotMessage(msg); err != nil {
		d.logger.Warningf("raft[id=%d] failed to receiveSnapshotMessage. err(%s)", d.cluster.LocalPeerID(), err.Error())
	} else if bFinish {
		d.logger.Infof("raft[id=%d] received finish snapshot message (snapshot index=%d)", d.cluster.LocalPeerID(), msg.Index)
		if err = d.raftNode.Step(context.TODO(), msg.FinishMessage); err != nil {
			d.logger.Warningf("raft[id=%d] step err(%s)", d.cluster.LocalPeerID(), err.Error())
		}
	}
}

func (d *transportProcessor) GetClusterPeersRequest(req babuzapb.GetClusterPeersRequest) babuzapb.GetClusterPeersResponse {
	if req.ClusterID != d.cluster.ClusterID() {
		d.logger.Warningf("raft[id=%d] received get cluster peers request with different cluster id(%d)", d.cluster.LocalPeerID(), req.ClusterID)
		return babuzapb.GetClusterPeersResponse{
			Status:  babuzapb.FAILED,
			Message: "cluster id not match"}
	}
	if !d.isPeerInCluster(req.From) {
		d.logger.Warningf("raft[id=%d] received get cluster peers request from unknown peer id(%d)", d.cluster.LocalPeerID(), req.From)
		return babuzapb.GetClusterPeersResponse{
			Status:  babuzapb.FAILED,
			Message: "peer not in cluster"}
	}
	if req.To != d.cluster.LocalPeerID() {
		d.logger.Warningf("raft[id=%d] received get cluster peers request with different peer id(%d)", d.cluster.LocalPeerID(), req.To)
		return babuzapb.GetClusterPeersResponse{
			Status:  babuzapb.FAILED,
			Message: "peer id not match"}
	}
	return babuzapb.GetClusterPeersResponse{
		Status:  babuzapb.SUCCESS,
		Message: "success",
		Peers:   d.cluster.Peers()}
}

func (d *transportProcessor) PublishApplicationServiceRequest(req babuzapb.PublishApplicationServiceRequest) babuzapb.PublishApplicationServiceResponse {
	if req.ClusterID != d.cluster.ClusterID() {
		d.logger.Warningf("raft[id=%d] received publish application service request with different cluster id(%d)", d.cluster.LocalPeerID(), req.ClusterID)
		return babuzapb.PublishApplicationServiceResponse{
			Status:  babuzapb.FAILED,
			Message: "cluster id not match"}
	}
	if !d.isPeerInCluster(req.From) {
		d.logger.Warningf("raft[id=%d] received publish application service request from unknown peer id(%d)", d.cluster.LocalPeerID(), req.From)
		return babuzapb.PublishApplicationServiceResponse{
			Status:  babuzapb.FAILED,
			Message: "peer not in cluster"}
	}
	if req.To != d.cluster.LocalPeerID() {
		d.logger.Warningf("raft[id=%d] received publish application service request with different peer id(%d)", d.cluster.LocalPeerID(), req.To)
		return babuzapb.PublishApplicationServiceResponse{
			Status:  babuzapb.FAILED,
			Message: "peer id not match"}
	}

	normalRequest := babuzapb.NormalRequest{
		Context: babuzapb.RequestContext{
			ReplyID: req.ProposalReplyID,
		},
		PubAppService: &babuzapb.PubAppServiceRequest{
			PubServicePeerID:    req.From,
			AppServiceAddresses: req.AppServiceAddresses,
		},
	}

	data, err := normalRequest.Marshal()
	if err != nil {
		return babuzapb.PublishApplicationServiceResponse{
			Status:  babuzapb.FAILED,
			Message: err.Error()}
	}

	if err = d.raftNode.Propose(context.Background(), data); err != nil {
		return babuzapb.PublishApplicationServiceResponse{
			Status:  babuzapb.FAILED,
			Message: err.Error()}
	}
	return babuzapb.PublishApplicationServiceResponse{
		Status:  babuzapb.SUCCESS,
		Message: "success"}
}

func (d *transportProcessor) ReportUnreachable(id uint64) {
	d.raftNode.ReportUnreachable(id)
}
func (d *transportProcessor) ReportSnapshot(id uint64, status raft.SnapshotStatus) {
	d.status.AddInflightSnapshots(-1)
	d.metricsCollector.DecrementInflightSnapshots()
	if status == raft.SnapshotFinish {
		d.logger.Infof("raft[id=%d] finish to send snapshot to peer(id=%d)", d.cluster.LocalPeerID(), id)
	}
	d.raftNode.ReportSnapshot(id, status)
}

func (d *transportProcessor) CreateSnapshotReader(snapshotIndex uint64) (ibabuza.SnapshotReader, error) {
	snapReader, err := d.storage.CreateSnapshotReader(snapshotIndex)
	if err != nil {
		return nil, err
	}
	d.status.AddInflightSnapshots(1)
	d.metricsCollector.IncrementInflightSnapshots()
	return snapReader, nil
}

func (d *transportProcessor) isPeerInCluster(peerID uint64) bool {
	//TODO: implement this
	return true
}
