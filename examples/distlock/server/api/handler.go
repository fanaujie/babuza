package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/fanaujie/babuza/examples/distlock/server/lockstore"
	"github.com/fanaujie/babuza/examples/distlock/server/request"
	"github.com/fanaujie/babuza/raft"
)

type ReadLockStore interface {
	Query(key any) (any, error)
	QueryLease(leaseID uint64) (*lockstore.LeaseResult, error)
	HasExpiredLeases(now int64) bool
	Hash() uint32
}

type pendingWait struct {
	ch        chan *lockstore.LockResult
	requestID string
	lockName  string
}

type LockResourceHandler struct {
	r            *raft.Raft
	store        ReadLockStore
	pendingWaits sync.Map
}

func NewLockResourceHandler(r *raft.Raft, store ReadLockStore) *LockResourceHandler {
	return &LockResourceHandler{
		r:     r,
		store: store,
	}
}

func (h *LockResourceHandler) NotifyWaiter(result *lockstore.LockResult) {
	if result.NextRequestID == "" {
		return
	}

	if pw, ok := h.pendingWaits.Load(result.NextRequestID); ok {
		pending := pw.(*pendingWait)
		select {
		case pending.ch <- &lockstore.LockResult{
			Command:      lockstore.CmdWait,
			LockName:     result.LockName,
			OwnerID:      result.NextOwnerID,
			FencingToken: result.NextToken,
			Acquired:     true,
			LeaseID:      result.NextLeaseID,
		}:
		default:
		}
	}
}

func (h *LockResourceHandler) NotifyWaiters(results []*lockstore.LockResult) {
	for _, result := range results {
		h.NotifyWaiter(result)
	}
}

func (h *LockResourceHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		h.acquireLock(w, r)
	case http.MethodDelete:
		h.releaseLock(w, r)
	case http.MethodGet:
		h.getLockStatus(w, r)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (h *LockResourceHandler) acquireLock(w http.ResponseWriter, r *http.Request) {
	var req request.LockAcquireRequest
	b, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err = json.Unmarshal(b, &req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if req.LockName == "" || req.OwnerID == "" || req.LeaseID == 0 {
		http.Error(w, "lock_name, owner_id and lease_id are required", http.StatusBadRequest)
		return
	}

	var cmd lockstore.LockCommand
	b, err = cmd.Acquire(req.LockName, req.OwnerID, req.LeaseID, time.Now().UnixNano())
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
	ar := cmdRes.WaitForApplyResult()
	if ar.Error != nil {
		processRaftProposeError(ar.Error, w)
		return
	}

	res := ar.Response.(*lockstore.LockResult)

	if !res.Acquired && req.WaitTimeoutSeconds > 0 {
		h.waitForLock(w, r, req)
		return
	}

	if err = writeHttpResponse(w, res); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (h *LockResourceHandler) waitForLock(w http.ResponseWriter, r *http.Request, req request.LockAcquireRequest) {
	if req.RequestID == "" {
		http.Error(w, "request_id is required for wait operations", http.StatusBadRequest)
		return
	}

	waitCh := make(chan *lockstore.LockResult, 1)
	requestID := req.RequestID
	pw := &pendingWait{
		ch:        waitCh,
		requestID: requestID,
		lockName:  req.LockName,
	}
	h.pendingWaits.Store(requestID, pw)
	defer func() {
		h.pendingWaits.Delete(requestID)
		close(waitCh)
	}()

	var cmd lockstore.LockCommand
	b, err := cmd.Wait(req.LockName, req.OwnerID, req.LeaseID, time.Now().UnixNano(), requestID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	session := raft.ClientSession{
		SessionID:                         req.SessionID,
		SequenceNumber:                    req.SequenceNumber + 1,
		LowestSequenceNumberNotYetReplied: req.LowestSequenceNumberNotYetReplied,
	}
	cmdRes := h.r.Propose(r.Context(), session, b)
	defer cmdRes.Release()
	ar := cmdRes.WaitForApplyResult()
	if ar.Error != nil {
		processRaftProposeError(ar.Error, w)
		return
	}

	res := ar.Response.(*lockstore.LockResult)

	if res.WaitStatus == lockstore.WaitStatusAcquired {
		if err = writeHttpResponse(w, res); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), time.Duration(req.WaitTimeoutSeconds)*time.Second)
	defer cancel()

	select {
	case result := <-waitCh:
		if err = writeHttpResponse(w, result); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	case <-ctx.Done():
		go h.cancelWait(req.LockName, requestID)
		http.Error(w, "wait timeout", http.StatusRequestTimeout)
	}
}

func (h *LockResourceHandler) releaseLock(w http.ResponseWriter, r *http.Request) {
	var req request.LockReleaseRequest
	b, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err = json.Unmarshal(b, &req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if req.LockName == "" || req.OwnerID == "" {
		http.Error(w, "lock_name and owner_id are required", http.StatusBadRequest)
		return
	}

	var cmd lockstore.LockCommand
	b, err = cmd.Release(req.LockName, req.OwnerID, req.FencingToken)
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
	ar := cmdRes.WaitForApplyResult()
	if ar.Error != nil {
		processRaftProposeError(ar.Error, w)
		return
	}

	res := ar.Response.(*lockstore.LockResult)
	h.NotifyWaiter(res)
	if err = writeHttpResponse(w, res); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (h *LockResourceHandler) getLockStatus(w http.ResponseWriter, r *http.Request) {
	lockName := r.URL.Query().Get("name")
	if lockName == "" {
		http.Error(w, "name query parameter is required", http.StatusBadRequest)
		return
	}

	linearized := r.Header.Get(LinearizableHeader)
	isLinearized := len(linearized) > 0 && linearized == "true"
	if isLinearized {
		ctx, cancel := context.WithTimeout(r.Context(), time.Second*5)
		defer cancel()
		if err := h.r.LinearizableRead(ctx); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
	}

	res, err := h.store.Query(lockName)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if err = writeHttpResponse(w, res); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (h *LockResourceHandler) cancelWait(lockName, requestID string) {
	var cmd lockstore.LockCommand
	b, err := cmd.CancelWait(lockName, requestID)
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	cmdRes := h.r.Propose(ctx, raft.ClientSession{}, b)
	defer cmdRes.Release()
	cmdRes.WaitForApplyResult()
}

type LeaseResourceHandler struct {
	r            *raft.Raft
	store        ReadLockStore
	lockHandler  *LockResourceHandler
}

func NewLeaseResourceHandler(r *raft.Raft, store ReadLockStore, lockHandler *LockResourceHandler) *LeaseResourceHandler {
	return &LeaseResourceHandler{
		r:           r,
		store:       store,
		lockHandler: lockHandler,
	}
}

func (h *LeaseResourceHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		h.grantLease(w, r)
	case http.MethodDelete:
		h.revokeLease(w, r)
	case http.MethodPut:
		h.keepAliveLease(w, r)
	case http.MethodGet:
		h.getLeaseStatus(w, r)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (h *LeaseResourceHandler) grantLease(w http.ResponseWriter, r *http.Request) {
	var req request.LeaseGrantRequest
	b, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err = json.Unmarshal(b, &req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if req.TTLSeconds <= 0 {
		http.Error(w, "ttl_seconds is required and must be positive", http.StatusBadRequest)
		return
	}

	var cmd lockstore.LockCommand
	b, err = cmd.LeaseGrant(req.TTLSeconds, time.Now().UnixNano())
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
	ar := cmdRes.WaitForApplyResult()
	if ar.Error != nil {
		processRaftProposeError(ar.Error, w)
		return
	}

	res := ar.Response.(*lockstore.LeaseResult)
	if err = writeHttpResponse(w, res); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (h *LeaseResourceHandler) revokeLease(w http.ResponseWriter, r *http.Request) {
	var req request.LeaseRevokeRequest
	b, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err = json.Unmarshal(b, &req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if req.LeaseID == 0 {
		http.Error(w, "lease_id is required", http.StatusBadRequest)
		return
	}

	var cmd lockstore.LockCommand
	b, err = cmd.LeaseRevoke(req.LeaseID)
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
	ar := cmdRes.WaitForApplyResult()
	if ar.Error != nil {
		processRaftProposeError(ar.Error, w)
		return
	}

	res := ar.Response.(*lockstore.LeaseResult)
	if err = writeHttpResponse(w, res); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (h *LeaseResourceHandler) keepAliveLease(w http.ResponseWriter, r *http.Request) {
	var req request.LeaseKeepAliveRequest
	b, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err = json.Unmarshal(b, &req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if req.LeaseID == 0 {
		http.Error(w, "lease_id is required", http.StatusBadRequest)
		return
	}

	var cmd lockstore.LockCommand
	b, err = cmd.LeaseKeepAlive(req.LeaseID, time.Now().UnixNano())
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
	ar := cmdRes.WaitForApplyResult()
	if ar.Error != nil {
		processRaftProposeError(ar.Error, w)
		return
	}

	res := ar.Response.(*lockstore.LeaseResult)
	if err = writeHttpResponse(w, res); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (h *LeaseResourceHandler) getLeaseStatus(w http.ResponseWriter, r *http.Request) {
	leaseIDStr := r.URL.Query().Get("id")
	if leaseIDStr == "" {
		http.Error(w, "id query parameter is required", http.StatusBadRequest)
		return
	}

	leaseID, err := strconv.ParseUint(leaseIDStr, 10, 64)
	if err != nil {
		http.Error(w, "invalid lease_id", http.StatusBadRequest)
		return
	}

	linearized := r.Header.Get(LinearizableHeader)
	isLinearized := len(linearized) > 0 && linearized == "true"
	if isLinearized {
		ctx, cancel := context.WithTimeout(r.Context(), time.Second*5)
		defer cancel()
		if err := h.r.LinearizableRead(ctx); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
	}

	res, err := h.store.QueryLease(leaseID)
	if err != nil {
		if errors.Is(err, lockstore.ErrLeaseNotFound) {
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

func (h *LeaseResourceHandler) ShouldTick() bool {
	return h.store.HasExpiredLeases(time.Now().UnixNano())
}

func (h *LeaseResourceHandler) ProposeTick() {
	var cmd lockstore.LockCommand
	b, err := cmd.Tick(time.Now().UnixNano())
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	cmdRes := h.r.Propose(ctx, raft.ClientSession{}, b)
	defer cmdRes.Release()
	ar := cmdRes.WaitForApplyResult()
	if ar.Error != nil {
		return
	}

	if tickRes, ok := ar.Response.(*lockstore.TickResult); ok {
		h.lockHandler.NotifyWaiters(tickRes.NotifyResults)
	}
}
