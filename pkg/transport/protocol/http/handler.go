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

package http

import (
	"fmt"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/transport/protocol/tcp/conn/frame"
	"github.com/fanaujie/babuza/pkg/utility/allocator"
	"net/http"
	"strconv"
)

type handler struct {
	raft   ibabuza.RaftMessageHandler
	config ServerConfig
}

func (h *handler) batchMessageFunc(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	batchMsg := babuzapb.BatchMessage{}
	if err := decodeExpectedMessage(req.Body, req.ContentLength, &batchMsg); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	h.raft.ProcessBatchMessage(batchMsg)
	w.WriteHeader(http.StatusOK)
}

func (h *handler) batchMessageStreamFunc(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !h.config.MessageStreamEnabled {
		http.NotFound(w, req)
		return
	}
	defer req.Body.Close()

	reader := frame.NewReader(req.Body)
	for {
		eof, err := reader.ReadFrameOrEOF(func(msgType frame.MessageType, msgBuf []byte) error {
			if msgType != frame.BatchMsgType {
				return fmt.Errorf("unsupported message type: %d", msgType)
			}
			if len(msgBuf) == 0 {
				return fmt.Errorf("batch message is empty")
			}
			batchMsg := babuzapb.BatchMessage{}
			if err := batchMsg.Unmarshal(msgBuf); err != nil {
				return err
			}
			if len(batchMsg.Messages) == 0 {
				return fmt.Errorf("batch message is empty")
			}
			h.raft.ProcessBatchMessage(batchMsg)
			return nil
		})
		if eof {
			w.WriteHeader(http.StatusOK)
			return
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	}
}

func (h *handler) snapshotMessageFunc(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	snapMsg := babuzapb.SnapshotMessage{}
	if err := decodeExpectedMessage(req.Body, req.ContentLength, &snapMsg); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	res := h.raft.ProcessSnapshotMessage(snapMsg)
	writeProtoMessage[*babuzapb.SnapshotMessageResponse](w, &res)
}

func (h *handler) clusterPeersFunc(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	cId := req.URL.Query().Get("clusterID")
	if cId == "" {
		http.Error(w, "", http.StatusBadRequest)
		return
	}
	clusterID, err := strconv.ParseUint(cId, 10, 64)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	fId := req.URL.Query().Get("from")
	if fId == "" {
		http.Error(w, "", http.StatusBadRequest)
		return
	}
	fromId, err := strconv.ParseUint(fId, 10, 64)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	tId := req.URL.Query().Get("to")
	if tId == "" {
		http.Error(w, "", http.StatusBadRequest)
		return
	}
	toId, err := strconv.ParseUint(tId, 10, 64)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	res := h.raft.GetClusterPeer(babuzapb.GetClusterPeersRequest{
		ClusterID: clusterID,
		From:      fromId,
		To:        toId,
	})
	writeProtoMessage[*babuzapb.GetClusterPeersResponse](w, &res)
}

// TODO: add test case
func (h *handler) publishApplicationServiceFunc(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	appServiceUrlReq := babuzapb.PublishApplicationServiceRequest{}
	if err := decodeExpectedMessage(req.Body, req.ContentLength, &appServiceUrlReq); err != nil {
		fmt.Println(err.Error())
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	res := h.raft.PublishApplicationService(appServiceUrlReq)
	writeProtoMessage[*babuzapb.PublishApplicationServiceResponse](w, &res)
}

func writeProtoMessage[T interface {
	Size() int
	MarshalTo([]byte) (int, error)
}](w http.ResponseWriter, res T) {
	msgSize := res.Size()
	byteSlice := allocator.Acquire(msgSize)
	defer allocator.Release(byteSlice)

	n, err := res.MarshalTo(byteSlice.Buffer)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if _, err = w.Write(byteSlice.Buffer[:n]); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
