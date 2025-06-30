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

// ServeHTTP handles learner promotion requests
// @Summary Promote a learner to a voting member
// @Description Promotes a learner node to a voting member in the Raft cluster
// @Tags cluster-management
// @Accept json
// @Produce json
// @Param request body request.PromoteLearnerRequest true "Learner promotion request"
// @Success 200 {object} response.ClusterConfigurationResponse "Updated cluster configuration after promotion"
// @Failure 400 {object} string "Bad request"
// @Failure 500 {object} string "Internal server error"
// @Failure 503 {object} string "Service unavailable - not leader"
// @Router /promote-learner [put]
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
	if ar := promoteRes.WaitForApplyResult(); ar.Error != nil {
		processRaftProposeError(ar.Error, w)
		return
	}
	res := convertRaftClusterPeersToResponse(h.r, session.SessionID, session.SequenceNumber)
	if err = writeHttpResponse(w, res); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
