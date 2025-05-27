package api

import (
	"encoding/json"
	"fmt"
	"github.com/fanaujie/babuza/examples/kvstore/server/request"
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

// joinPeerFunc adds a new peer to the cluster
// @Summary Add a new peer to the cluster
// @Description Join a new peer (voting or learner) to the Raft cluster
// @Tags peers
// @Accept json
// @Produce json
// @Param request body request.JoinPeerRequest true "Peer join request"
// @Success 200 {object} response.ClusterConfigurationResponse "Current cluster configuration after join"
// @Failure 400 {object} string "Bad request"
// @Failure 500 {object} string "Internal server error"
// @Failure 503 {object} string "Service unavailable - not leader"
// @Router /peers [post]
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
		PeerID:         req.RaftPeerId,
		RaftListenAddr: req.RaftListenAddr,
		IsLearner:      req.IsLearner,
	}
	if req.IsLearner {
		joinRes = h.r.AddLearner(r.Context(), session, peerAttr)
	} else {
		joinRes = h.r.AddVotingPeer(r.Context(), session, peerAttr)
	}
	defer joinRes.Release()
	if ar := joinRes.WaitForApplyResult(); ar.Error != nil {
		processRaftProposeError(ar.Error, w, r, h.r.LeaderAppServiceAddresses())
		return
	}
	res := convertRaftClusterPeersToResponse(h.r, session.SessionID, session.SequenceNumber)
	if err = writeHttpResponse(w, res); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// getPeersFunc retrieves all cluster peers
// @Summary Get all cluster peers
// @Description Retrieve information about all peers in the Raft cluster
// @Tags peers
// @Produce json
// @Success 200 {object} response.ClusterConfigurationResponse "Cluster configuration"
// @Failure 500 {object} string "Internal server error"
// @Router /peers [get]
func (h *ClusterPeerResourceHandler) getPeersFunc(w http.ResponseWriter, r *http.Request) {
	if err := writeHttpResponse(w, convertRaftClusterPeersToResponse(h.r, 0, 0)); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// updatePeerFunc updates an existing peer's configuration
// @Summary Update peer configuration
// @Description Update an existing peer's configuration in the Raft cluster
// @Tags peers
// @Accept json
// @Produce json
// @Param request body request.UpdatePeerRequest true "Peer update request"
// @Success 200 {object} response.ClusterConfigurationResponse "Updated cluster configuration"
// @Failure 400 {object} string "Bad request"
// @Failure 500 {object} string "Internal server error"
// @Failure 503 {object} string "Service unavailable - not leader"
// @Router /peers [put]
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
		PeerID:         req.RaftPeerId,
		RaftListenAddr: req.RaftListenAddr,
	}
	updateRes := h.r.UpdatePeer(r.Context(), session, peerAttr)
	defer updateRes.Release()
	if ar := updateRes.WaitForApplyResult(); ar.Error != nil {
		processRaftProposeError(ar.Error, w, r, h.r.LeaderAppServiceAddresses())
		return
	}
	res := convertRaftClusterPeersToResponse(h.r, session.SessionID, session.SequenceNumber)
	if err = writeHttpResponse(w, res); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// removePeerFunc removes a peer from the cluster
// @Summary Remove a peer from the cluster
// @Description Remove an existing peer from the Raft cluster
// @Tags peers
// @Accept json
// @Produce json
// @Param request body request.RemovePeerRequest true "Peer removal request"
// @Success 200 {object} response.ClusterConfigurationResponse "Updated cluster configuration after removal"
// @Failure 400 {object} string "Bad request"
// @Failure 500 {object} string "Internal server error"
// @Failure 503 {object} string "Service unavailable - not leader"
// @Router /peers [delete]
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
	ar := removeRes.WaitForApplyResult()
	fmt.Printf("removeRes.WaitForApplyResult() = %v\n", ar)
	if ar.Error != nil {
		processRaftProposeError(ar.Error, w, r, h.r.LeaderAppServiceAddresses())
		return
	}
	res := convertRaftClusterPeersToResponse(h.r, session.SessionID, session.SequenceNumber)
	if err = writeHttpResponse(w, res); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
