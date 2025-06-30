// Copyright 2025 Chen Chunchieh <junjie725@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


package api

import (
	"github.com/fanaujie/babuza/examples/kvstore/server/response"
	"github.com/fanaujie/babuza/raft"
	"net/http"
	"strconv"
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
	if r.Method == http.MethodPost {
		var res response.RegisterSessionResponse
		babuzaRes := h.r.RegisterSession(r.Context())
		defer babuzaRes.Release()
		ar := babuzaRes.WaitForApplyResult()
		if ar.Error != nil {
			processRaftProposeError(ar.Error, w)
			return
		}
		res.SessionId = ar.LogIndex
		if err := writeHttpResponse(w, res); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	} else if r.Method == http.MethodDelete {
		var res response.UnregisterSessionResponse
		strSessionId := r.Header.Get(SessionIDHeader)
		if strSessionId == "" {
			http.Error(w, "session ID is required", http.StatusBadRequest)
			return
		}
		// string to uint64
		sessionIdUint64, err := strconv.ParseUint(strSessionId, 10, 64)
		if err != nil {
			http.Error(w, "invalid session ID", http.StatusBadRequest)
			return
		}
		babuzaRes := h.r.UnregisterSession(r.Context(), sessionIdUint64)
		defer babuzaRes.Release()
		ar := babuzaRes.WaitForApplyResult()
		if ar.Error != nil {
			processRaftProposeError(ar.Error, w)
			return
		}
		if ar.Error == nil {
			res.IsUnregistered = true
		}
		res.SessionID = sessionIdUint64
		if err = writeHttpResponse(w, res); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	} else {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

}
