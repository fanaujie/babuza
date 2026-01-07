package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/fanaujie/babuza/raft"
)

const (
	LinearizableHeader = "X-Linearizable"
	LocksPath          = "/locks"
	LeasesPath         = "/leases"
)

func processRaftProposeError(err error, w http.ResponseWriter) {
	if errors.Is(err, raft.ErrNotLeader) {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	} else if errors.Is(err, context.DeadlineExceeded) {
		http.Error(w, err.Error(), http.StatusGatewayTimeout)
		return
	}
	http.Error(w, err.Error(), http.StatusInternalServerError)
}

func writeHttpResponse(w http.ResponseWriter, response any) error {
	w.Header().Set("Content-Type", "application/json")
	return json.NewEncoder(w).Encode(response)
}
