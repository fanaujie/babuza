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
	"errors"
	"fmt"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/transport/protocol/tcp/conn/frame"
	"github.com/fanaujie/babuza/pkg/utility/allocator"
	"io"
	"net/http"
	"strconv"
	"time"
)

type handler struct {
	raft             ibabuza.RaftMessageHandler
	config           ServerConfig
	messageStreamHub *MessageStreamHub
}

func (h *handler) batchMessageFunc(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	defer h.withRequestReadTimeout(req, h.config.ReadDeadline)()
	batchMsg := babuzapb.BatchMessage{}
	if err := decodeExpectedMessage(req.Body, req.ContentLength, &batchMsg); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	h.raft.ProcessBatchMessage(batchMsg)
	w.WriteHeader(http.StatusOK)
}

func (h *handler) batchMessageStreamFunc(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !h.config.MessageStreamEnabled {
		http.NotFound(w, req)
		return
	}
	if h.messageStreamHub == nil {
		http.Error(w, "message stream hub is not configured", http.StatusInternalServerError)
		return
	}
	from := req.URL.Query().Get("from")
	if from == "" {
		http.Error(w, "missing from", http.StatusBadRequest)
		return
	}
	fromID, err := strconv.ParseUint(from, 10, 64)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	stream := h.messageStreamHub.register(fromID)
	defer h.messageStreamHub.unregister(fromID, stream)

	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	controller := http.NewResponseController(w)
	if err := controller.Flush(); err != nil {
		return
	}

	for {
		select {
		case <-req.Context().Done():
			return
		case <-stream.done:
			return
		case frameBytes := <-stream.frames:
			if len(frameBytes) == 0 {
				continue
			}
			if _, err := w.Write(frameBytes); err != nil {
				return
			}
			if err := controller.Flush(); err != nil {
				return
			}
		}
	}
}

func (h *handler) snapshotMessageFunc(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	defer h.withRequestReadTimeout(req, h.config.ReadDeadline)()
	snapMsg := babuzapb.SnapshotMessage{}
	if err := decodeExpectedMessage(req.Body, req.ContentLength, &snapMsg); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	res := h.raft.ProcessSnapshotMessage(snapMsg)
	writeProtoMessage[*babuzapb.SnapshotMessageResponse](w, &res)
}

func (h *handler) snapshotMessageStreamFunc(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !h.config.MessageStreamEnabled {
		http.NotFound(w, req)
		return
	}
	defer h.withRequestReadTimeout(req, h.config.SnapshotStreamIdleTimeout)()
	defer req.Body.Close()
	if err := http.NewResponseController(w).EnableFullDuplex(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	reader := frame.NewReader(req.Body)
	frameCount := 0
	for {
		eof, err := reader.ReadFrameOrEOF(func(msgType frame.MessageType, msgBuf []byte) error {
			if msgType != frame.SnapshotMsgReqType {
				return fmt.Errorf("unsupported message type: %d", msgType)
			}
			if len(msgBuf) == 0 {
				return fmt.Errorf("snapshot message is empty")
			}
			snapMsg := babuzapb.SnapshotMessage{}
			if err := snapMsg.Unmarshal(msgBuf); err != nil {
				return err
			}
			frameCount++
			res := h.raft.ProcessSnapshotMessage(snapMsg)
			if snapMsg.Type == babuzapb.SnapshotMessageType_Finish || res.Status != babuzapb.SUCCESS {
				if err := writeSnapshotResponseFrame(w, &res); err != nil {
					return err
				}
				if err := http.NewResponseController(w).Flush(); err != nil {
					return err
				}
			}
			if snapMsg.Type == babuzapb.SnapshotMessageType_Finish || res.Status != babuzapb.SUCCESS {
				return errSnapshotStreamRejected
			}
			return nil
		})
		if err != nil {
			if errors.Is(err, errSnapshotStreamRejected) {
				return
			}
			if frameCount > 0 {
				return
			}
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if eof {
			if frameCount == 0 {
				http.Error(w, "snapshot stream is empty", http.StatusBadRequest)
				return
			}
			return
		}
	}
}

var errSnapshotStreamRejected = errors.New("snapshot stream rejected")

func writeSnapshotResponseFrame(w io.Writer, res *babuzapb.SnapshotMessageResponse) error {
	bufSize := frame.EncodeSize(res.Size())
	bufSlice := allocator.Acquire(bufSize)
	defer allocator.Release(bufSlice)
	return frame.NewWriter(w).Encode(bufSlice.Buffer[:bufSize], frame.SnapshotMsgResType, res)
}

func (h *handler) clusterPeersFunc(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	defer h.withRequestReadTimeout(req, h.config.ReadDeadline)()
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
	defer h.withRequestReadTimeout(req, h.config.ReadDeadline)()
	appServiceUrlReq := babuzapb.PublishApplicationServiceRequest{}
	if err := decodeExpectedMessage(req.Body, req.ContentLength, &appServiceUrlReq); err != nil {
		fmt.Println(err.Error())
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	res := h.raft.PublishApplicationService(appServiceUrlReq)
	writeProtoMessage[*babuzapb.PublishApplicationServiceResponse](w, &res)
}

func (h *handler) withRequestReadTimeout(req *http.Request, readTimeout time.Duration) func() {
	conn, ok := req.Context().Value(serverConnectionContextKey{}).(*Connection)
	if !ok || conn == nil {
		return func() {}
	}
	conn.SetReadTimeout(h.endpointReadTimeout(readTimeout))
	return func() {
		conn.SetReadTimeout(h.config.ReadDeadline)
	}
}

func (h *handler) endpointReadTimeout(readTimeout time.Duration) time.Duration {
	if readTimeout > 0 {
		return readTimeout
	}
	return h.config.ReadDeadline
}

func writeProtoMessage[T interface {
	Size() int
	MarshalTo([]byte) (int, error)
}](w http.ResponseWriter, res T) {
	msgSize := res.Size()
	if msgSize == 0 {
		w.WriteHeader(http.StatusOK)
		return
	}
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
