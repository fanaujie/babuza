package http

import (
	"fmt"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/utility/allocator"
	"net/http"
	"strconv"
)

type handler struct {
	raft ibabuza.RaftMessageHandler
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
