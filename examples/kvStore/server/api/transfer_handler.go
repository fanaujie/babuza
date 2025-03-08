package api

import (
	"encoding/json"
	"github.com/fanaujie/babuza/examples/kvStore/server/request"
	"github.com/fanaujie/babuza/examples/kvStore/server/response"
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
		processRaftProposeError(err, w, r, h.r.LeaderAppServiceAddresses())
		return
	}
	if err = writeHttpResponse(w, &response.TransferLeaderResponse{}); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
