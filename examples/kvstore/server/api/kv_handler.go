package api

import (
	"encoding/json"
	"github.com/fanaujie/babuza/examples/kvstore/server/kverror"
	"github.com/fanaujie/babuza/examples/kvstore/server/kvstore"
	"github.com/fanaujie/babuza/examples/kvstore/server/request"
	"github.com/fanaujie/babuza/examples/kvstore/server/response"
	"github.com/fanaujie/babuza/raft"
	"io"
	"net/http"
)

type ReadKvStore interface {
	Load(key string) (string, error)
	Hash() uint32
}

type KvStoreResourceHandler struct {
	enableLinearizableRead bool
	r                      *raft.Raft
	store                  ReadKvStore
}

func NewKvStoreResourceHandler(enableLinearizableRead bool, r *raft.Raft, kvStore ReadKvStore) *KvStoreResourceHandler {
	return &KvStoreResourceHandler{
		enableLinearizableRead: enableLinearizableRead,
		r:                      r,
		store:                  kvStore,
	}
}

func (h *KvStoreResourceHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		h.setKvStoreFunc(w, r)
	case http.MethodPut:
		h.appendKvStoreFunc(w, r)
	case http.MethodDelete:
		h.deleteKvStoreFunc(w, r)
	case http.MethodGet:
		h.readKvStoreFunc(w, r)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (h *KvStoreResourceHandler) readKvStoreFunc(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "", http.StatusBadRequest)
		return
	}
	if h.enableLinearizableRead {
		if err := h.r.LinearizableRead(r.Context()); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	res := response.KvStoreResponse{
		KvResult: kvstore.KvResult{
			Command: kvstore.Read,
			Key:     key,
		},
	}
	var err error
	res.Value, err = h.store.Load(res.Key)
	if err != nil {
		if err == kverror.ErrKeyNotFound {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if err = writeHttpResponse(w, res); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (h *KvStoreResourceHandler) setKvStoreFunc(w http.ResponseWriter, r *http.Request) {
	var req request.KvStoreSetRequest
	b, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err = json.Unmarshal(b, &req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var kvCmd kvstore.KvCommand
	b, err = kvCmd.Set(req.Key, req.Value)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	session := raft.ClientSession{
		SessionID:                         req.SessionID,
		SequenceNumber:                    req.SequenceNumber,
		LowestSequenceNumberNotYetReplied: req.LowestSequenceNumberNotYetReplied,
	}
	cmdRes := h.r.Propose(r.Context(), session, b)
	defer cmdRes.Release()
	if err = cmdRes.Wait(); err != nil {
		processRaftProposeError(err, w, r, h.r.LeaderAppServiceAddresses())
		return
	}
	proposalRes := cmdRes.Response()
	if err, ok := proposalRes.(error); ok {
		processRaftProposeError(err, w, r, h.r.LeaderAppServiceAddresses())
		return
	}
	res := response.KvStoreResponse{
		SessionID:      req.SessionID,
		SequenceNumber: req.SequenceNumber,
		KvResult:       *(proposalRes.(*kvstore.KvResult)),
	}

	if err = writeHttpResponse(w, &res); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (h *KvStoreResourceHandler) appendKvStoreFunc(w http.ResponseWriter, r *http.Request) {
	var req request.KvStoreAppendRequest
	b, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err = json.Unmarshal(b, &req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var kvCmd kvstore.KvCommand
	b, err = kvCmd.Append(req.Key, req.Value)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	session := raft.ClientSession{
		SessionID:                         req.SessionID,
		SequenceNumber:                    req.SequenceNumber,
		LowestSequenceNumberNotYetReplied: req.LowestSequenceNumberNotYetReplied,
	}
	cmdRes := h.r.Propose(r.Context(), session, b)
	defer cmdRes.Release()
	if err = cmdRes.Wait(); err != nil {
		processRaftProposeError(err, w, r, h.r.LeaderAppServiceAddresses())
		return
	}
	proposalRes := cmdRes.Response()
	if err, ok := proposalRes.(error); ok {
		processRaftProposeError(err, w, r, h.r.LeaderAppServiceAddresses())
		return
	}
	res := response.KvStoreResponse{
		SessionID:      req.SessionID,
		SequenceNumber: req.SequenceNumber,
		KvResult:       *(proposalRes.(*kvstore.KvResult)),
	}
	if err = writeHttpResponse(w, &res); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (h *KvStoreResourceHandler) deleteKvStoreFunc(w http.ResponseWriter, r *http.Request) {
	var req request.KvStoreAppendRequest
	b, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if err = json.Unmarshal(b, &req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var kvCmd kvstore.KvCommand
	b, err = kvCmd.Delete(req.Key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	session := raft.ClientSession{
		SessionID:                         req.SessionID,
		SequenceNumber:                    req.SequenceNumber,
		LowestSequenceNumberNotYetReplied: req.LowestSequenceNumberNotYetReplied,
	}
	cmdRes := h.r.Propose(r.Context(), session, b)
	defer cmdRes.Release()
	if err = cmdRes.Wait(); err != nil {
		processRaftProposeError(err, w, r, h.r.LeaderAppServiceAddresses())
		return
	}
	proposalRes := cmdRes.Response()
	if err, ok := proposalRes.(error); ok {
		processRaftProposeError(err, w, r, h.r.LeaderAppServiceAddresses())
		return
	}
	res := response.KvStoreResponse{
		SessionID:      req.SessionID,
		SequenceNumber: req.SequenceNumber,
		KvResult:       *(proposalRes.(*kvstore.KvResult)),
	}
	if err = writeHttpResponse(w, &res); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
