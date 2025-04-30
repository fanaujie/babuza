package api

import (
	"github.com/fanaujie/babuza/examples/kvstore/server/response"
	"github.com/fanaujie/babuza/raft"
	"net/http"
)

type SessionResourceHandler struct {
	r *raft.Raft
}

func NewSessionResourceHandler(r *raft.Raft) *SessionResourceHandler {
	return &SessionResourceHandler{r: r}
}

// ServeHTTP handles session registration requests
// @Summary Register a new client session
// @Description Create a new client session for the Raft cluster
// @Tags sessions
// @Accept json
// @Produce json
// @Success 200 {object} response.RegisterSessionResponse "Session successfully registered"
// @Failure 400 {object} string "Bad request"
// @Failure 500 {object} string "Internal server error"
// @Failure 503 {object} string "Service unavailable - not leader"
// @Router /sessions [post]
func (h *SessionResourceHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var res response.RegisterSessionResponse
	babuzaRes := h.r.RegisterSession(r.Context())
	defer babuzaRes.Release()
	ar := babuzaRes.WaitForApplyResult()
	if ar.Error != nil {
		processRaftProposeError(ar.Error, w, r, h.r.LeaderAppServiceAddresses())
		return
	}
	res.SessionId = ar.LogIndex
	if err := writeHttpResponse(w, res); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
