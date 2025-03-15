package api

import (
	"encoding/json"
	"github.com/fanaujie/babuza/examples/kvstore/server/request"
	"github.com/fanaujie/babuza/raft"
	"io"
	"net/http"
)

type PromoteLearnerHandler struct {
	r *raft.Raft
}

func NewPromoteLearnerHandler(r *raft.Raft) *PromoteLearnerHandler {
	return &PromoteLearnerHandler{r: r}
}

func (h *PromoteLearnerHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req request.PromoteLearnerRequest
	b, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
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
	promoteRes := h.r.PromoteLearner(r.Context(), session, req.RaftPeerId)
	defer promoteRes.Release()
	if err = promoteRes.Wait(); err != nil {
		processRaftProposeError(err, w, r, h.r.LeaderAppServiceAddresses())
		return
	}
	proposalRes := promoteRes.Response()
	if err, ok := proposalRes.(error); ok {
		processRaftProposeError(err, w, r, h.r.LeaderAppServiceAddresses())
		return
	}
	res := convertRaftClusterPeersToResponse(h.r, session.SessionID, session.SequenceNumber)
	if err = writeHttpResponse(w, res); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
