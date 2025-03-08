package api

import (
	"github.com/fanaujie/babuza/examples/kvStore/server/response"
	"github.com/fanaujie/babuza/raft"
	"net/http"
)

type SessionResourceHandler struct {
	r *raft.Raft
}

func NewSessionResourceHandler(r *raft.Raft) *SessionResourceHandler {
	return &SessionResourceHandler{r: r}
}

func (h *SessionResourceHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var res response.RegisterSessionResponse
	babuzaRes := h.r.RegisterSession(r.Context())
	defer babuzaRes.Release()
	if err := babuzaRes.Wait(); err != nil {
		processRaftProposeError(err, w, r, h.r.LeaderAppServiceAddresses())
		return
	}
	res.SessionId = babuzaRes.LogIndex()
	if err := writeHttpResponse(w, res); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
