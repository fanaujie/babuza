package api

import (
	"encoding/json"
	"github.com/fanaujie/babuza/examples/kvStore/server/request"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/raft"
	"io"
	"net/http"
)

type ClusterPeerResourceHandler struct {
	r *raft.Raft
}

func NewClusterPeerResourceHandler(r *raft.Raft) *ClusterPeerResourceHandler {
	return &ClusterPeerResourceHandler{r: r}
}

func (h *ClusterPeerResourceHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		h.joinPeerFunc(w, r)
	case http.MethodPut:
		h.updatePeerFunc(w, r)
	case http.MethodDelete:
		h.removePeerFunc(w, r)
	case http.MethodGet:
		h.getPeersFunc(w, r)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (h *ClusterPeerResourceHandler) joinPeerFunc(w http.ResponseWriter, r *http.Request) {
	var req request.JoinPeerRequest
	b, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err = json.Unmarshal(b, &req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var joinRes raft.ProposedResult
	session := raft.ClientSession{
		SessionID:                         req.SessionID,
		SequenceNumber:                    req.SequenceNumber,
		LowestSequenceNumberNotYetReplied: req.LowestSequenceNumberNotYetReplied,
	}
	peerAttr := babuzapb.RaftPeerAttribute{
		Id:             req.RaftPeerId,
		RaftListenAddr: req.RaftListenAddr,
		IsLearner:      req.IsLearner,
	}
	if req.IsLearner {
		joinRes = h.r.AddLearner(r.Context(), session, peerAttr)
	} else {
		joinRes = h.r.AddVotingPeer(r.Context(), session, peerAttr)
	}
	defer joinRes.Release()
	if err = joinRes.Wait(); err != nil {
		processRaftProposeError(err, w, r, h.r.LeaderAppServiceAddresses())
		return
	}
	proposalRes := joinRes.Response()
	if err, ok := proposalRes.(error); ok {
		processRaftProposeError(err, w, r, h.r.LeaderAppServiceAddresses())
		return
	}
	res := convertRaftClusterPeersToResponse(h.r, session.SessionID, session.SequenceNumber)
	if err = writeHttpResponse(w, res); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (h *ClusterPeerResourceHandler) getPeersFunc(w http.ResponseWriter, r *http.Request) {
	if err := writeHttpResponse(w, convertRaftClusterPeersToResponse(h.r, 0, 0)); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (h *ClusterPeerResourceHandler) updatePeerFunc(w http.ResponseWriter, r *http.Request) {
	var req request.UpdatePeerRequest
	b, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err = json.Unmarshal(b, &req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	session := raft.ClientSession{
		SessionID:                         req.SessionID,
		SequenceNumber:                    req.SequenceNumber,
		LowestSequenceNumberNotYetReplied: req.LowestSequenceNumberNotYetReplied,
	}
	peerAttr := babuzapb.RaftPeerAttribute{
		Id:             req.RaftPeerId,
		RaftListenAddr: req.RaftListenAddr,
	}
	updateRes := h.r.UpdatePeer(r.Context(), session, peerAttr)
	defer updateRes.Release()
	if err = updateRes.Wait(); err != nil {
		processRaftProposeError(err, w, r, h.r.LeaderAppServiceAddresses())
		return
	}
	proposalRes := updateRes.Response()
	if err, ok := proposalRes.(error); ok {
		processRaftProposeError(err, w, r, h.r.LeaderAppServiceAddresses())
		return
	}
	res := convertRaftClusterPeersToResponse(h.r, session.SessionID, session.SequenceNumber)
	if err = writeHttpResponse(w, res); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (h *ClusterPeerResourceHandler) removePeerFunc(w http.ResponseWriter, r *http.Request) {
	var req request.RemovePeerRequest
	b, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err = json.Unmarshal(b, &req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	session := raft.ClientSession{
		SessionID:                         req.SessionID,
		SequenceNumber:                    req.SequenceNumber,
		LowestSequenceNumberNotYetReplied: req.LowestSequenceNumberNotYetReplied,
	}
	removeRes := h.r.RemovePeer(r.Context(), session, req.RaftPeerId)
	defer removeRes.Release()
	if err = removeRes.Wait(); err != nil {
		processRaftProposeError(err, w, r, h.r.LeaderAppServiceAddresses())
		return
	}
	proposalRes := removeRes.Response()
	if err, ok := proposalRes.(error); ok {
		processRaftProposeError(err, w, r, h.r.LeaderAppServiceAddresses())
		return
	}
	res := convertRaftClusterPeersToResponse(h.r, session.SessionID, session.SequenceNumber)
	if err = writeHttpResponse(w, res); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
