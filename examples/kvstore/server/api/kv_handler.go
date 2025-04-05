package api

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/fanaujie/babuza/examples/kvstore/server/kverror"
	"github.com/fanaujie/babuza/examples/kvstore/server/kvstore"
	"github.com/fanaujie/babuza/examples/kvstore/server/request"
	"github.com/fanaujie/babuza/examples/kvstore/server/response"
	"github.com/fanaujie/babuza/raft"
	"io"
	"net/http"
	"time"
)

type ReadKvStore interface {
	Load(key string) (string, error)
	Hash() uint32
}

type KvStoreResourceHandler struct {
	r     *raft.Raft
	store ReadKvStore
}

func NewKvStoreResourceHandler(r *raft.Raft, kvStore ReadKvStore) *KvStoreResourceHandler {
	return &KvStoreResourceHandler{
		r:     r,
		store: kvStore,
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

// readKvStoreFunc retrieves a value from the key-value store
// @Summary Get a value by key
// @Description Retrieve a value from the key-value store by its key
// @Tags kv-store
// @Produce json
// @Param key query string true "Key to retrieve"
// @Success 200 {object} response.KvStoreResponse "Key-value pair"
// @Failure 400 {object} string "Bad request - missing key"
// @Failure 500 {object} string "Internal server error"
// @Router /kv [get]
func (h *KvStoreResourceHandler) readKvStoreFunc(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "", http.StatusBadRequest)
		return
	}
	linearized := r.Header.Get(LinearizableHeader)
	isLinearized := len(linearized) > 0 && linearized == "true"
	if isLinearized && h.r.Status().IsLeader() {
		ctx, cancel := context.WithTimeout(r.Context(), time.Second*2)
		defer cancel()
		if err := h.r.LinearizableRead(ctx); err != nil {
			if errors.Is(err, raft.ErrLeaderChange) {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			http.Error(w, err.Error(), http.StatusBadRequest)
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
		if errors.Is(err, kverror.ErrKeyNotFound) {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if err = writeHttpResponse(w, res); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// setKvStoreFunc sets a key-value pair
// @Summary Set a key-value pair
// @Description Set or replace a key-value pair in the store
// @Tags kv-store
// @Accept json
// @Produce json
// @Param request body request.KvStoreSetRequest true "Key-value set request"
// @Success 200 {object} response.KvStoreResponse "Result of the set operation"
// @Failure 400 {object} string "Bad request"
// @Failure 500 {object} string "Internal server error"
// @Failure 503 {object} string "Service unavailable - not leader"
// @Router /kv [post]
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

// appendKvStoreFunc appends to a value for a given key
// @Summary Append to a key's value
// @Description Append content to an existing key's value
// @Tags kv-store
// @Accept json
// @Produce json
// @Param request body request.KvStoreAppendRequest true "Key-value append request"
// @Success 200 {object} response.KvStoreResponse "Result of the append operation"
// @Failure 400 {object} string "Bad request"
// @Failure 500 {object} string "Internal server error"
// @Failure 503 {object} string "Service unavailable - not leader"
// @Router /kv [put]
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

// deleteKvStoreFunc deletes a key-value pair
// @Summary Delete a key-value pair
// @Description Remove a key-value pair from the store
// @Tags kv-store
// @Accept json
// @Produce json
// @Param request body request.KvStoreAppendRequest true "Key to delete"
// @Success 200 {object} response.KvStoreResponse "Result of the delete operation"
// @Failure 400 {object} string "Bad request"
// @Failure 500 {object} string "Internal server error"
// @Failure 503 {object} string "Service unavailable - not leader"
// @Router /kv [delete]
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
