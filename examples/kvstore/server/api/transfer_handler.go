package api

import (
	"encoding/json"
	"github.com/fanaujie/babuza/examples/kvstore/server/request"
	"github.com/fanaujie/babuza/examples/kvstore/server/response"
	"github.com/fanaujie/babuza/raft"
	"io"
	"net/http"
)

type TransferLeaderHandler struct {
	r *raft.Raft
}

func NewTransferLeaderHandler(r *raft.Raft) *TransferLeaderHandler {
	return &TransferLeaderHandler{r: r}
}

// ServeHTTP handles leadership transfer requests
// @Summary Transfer Raft leadership
// @Description Transfers leadership from the current leader to another node
// @Tags cluster-management
// @Accept json
// @Produce json
// @Param request body request.TransferLeaderRequest true "Leader transfer request"
// @Success 200 {object} response.TransferLeaderResponse "Leadership transfer successful"
// @Failure 400 {object} string "Bad request"
// @Failure 500 {object} string "Internal server error"
// @Failure 503 {object} string "Service unavailable - not leader"
// @Router /transfer-leader [put]
func (h *TransferLeaderHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req request.TransferLeaderRequest
	b, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err = json.Unmarshal(b, &req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err = h.r.TransferLeader(r.Context(), req.Transferee).Wait(); err != nil {
		processRaftProposeError(err, w)
		return
	}
	if err = writeHttpResponse(w, &response.TransferLeaderResponse{}); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
