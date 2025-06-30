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


package pdserver

import (
	"encoding/json"
	"fmt"
	"github.com/fanaujie/babuza/examples/redis-cluster/pkg/pb"
	"github.com/fanaujie/babuza/examples/redis-cluster/pkg/pdserver/schedule"
	"github.com/fanaujie/babuza/examples/redis-cluster/pkg/pdserver/schedule/operator"
	"github.com/fanaujie/babuza/examples/redis-cluster/pkg/pdserver/schedule/schedulers"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"google.golang.org/grpc"
)

type Config struct {
	HttpAddr string
	GrpcAddr string
}

type LeaderDistributionResponse struct {
	Groups []GroupLeaderInfo `json:"groups"`
}

type GroupLeaderInfo struct {
	GroupID  uint64                       `json:"group_id"`
	StoreID  uint64                       `json:"store_id"`
	LeaderID uint64                       `json:"leader_id"`
	Peers    []babuzapb.RaftPeerAttribute `json:"peers"`
}

type TransferLeaderRequest struct {
	GroupID     uint64 `json:"group_id"`
	NewLeaderID uint64 `json:"new_leader_id"`
}

type TransferLeaderResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
}

type PDServer struct {
	config      Config
	coordinator *schedule.Coordinator

	httpServer *http.Server
	grpcServer *grpc.Server
}

func NewPDServer(config Config) (*PDServer, error) {
	server := &PDServer{
		config:      config,
		coordinator: schedule.NewCoordinator(),
	}
	err := server.coordinator.AddScheduleTask(schedulers.NewTransferLeaderScheduler("transfer-leader-scheduler"))
	if err != nil {
		return nil, fmt.Errorf("failed to add transfer leader scheduler: %w", err)
	}
	return server, nil
}

func (s *PDServer) Run() error {

	var err error
	if err = s.startHTTPServer(); err != nil {
		return err
	}

	if err = s.startGRPCServer(); err != nil {
		s.stopHTTPServer()
		return err
	}

	fmt.Printf("PD server started. HTTP at %s, gRPC at %s\n", s.config.HttpAddr, s.config.GrpcAddr)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	fmt.Println("Shutting down...")
	s.coordinator.Stop()
	s.stopGRPCServer()
	s.stopHTTPServer()
	return nil
}

func (s *PDServer) startHTTPServer() error {
	mux := http.NewServeMux()

	mux.HandleFunc("/api/v1/leaders", s.handleGetLeaders)
	mux.HandleFunc("/api/v1/transfer-leader", s.handleTransferLeader)

	server := &http.Server{
		Addr:    s.config.HttpAddr,
		Handler: mux,
	}
	s.httpServer = server

	listener, err := net.Listen("tcp", s.config.HttpAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on HTTP address: %v", err)
	}

	go func() {
		_ = server.Serve(listener)
	}()
	return nil
}

func (s *PDServer) stopHTTPServer() {
	if s.httpServer != nil {
		s.httpServer.Close()
	}
}

func (s *PDServer) startGRPCServer() error {
	server := grpc.NewServer()
	pb.RegisterPDServiceServer(server, s)

	s.grpcServer = server

	listener, err := net.Listen("tcp", s.config.GrpcAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on gRPC address: %v", err)
	}

	go func() {
		_ = server.Serve(listener)
	}()
	return nil
}

func (s *PDServer) stopGRPCServer() {
	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
	}
}

func (s *PDServer) handleGetLeaders(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	allGroups := s.coordinator.InfoManager().AllGroups()
	var groups []GroupLeaderInfo

	for _, groupInfo := range allGroups {
		leader, exists := groupInfo.Leader()
		if exists {
			groups = append(groups, GroupLeaderInfo{
				GroupID:  groupInfo.GroupID(),
				StoreID:  groupInfo.StoreID(),
				LeaderID: leader.PeerID,
				Peers:    groupInfo.Peers(),
			})
		}
	}

	response := LeaderDistributionResponse{Groups: groups}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (s *PDServer) handleTransferLeader(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req TransferLeaderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		response := TransferLeaderResponse{
			Success: false,
			Message: "Invalid request body: " + err.Error(),
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(response)
		return
	}

	groupInfo, exists := s.coordinator.InfoManager().RaftGroup(req.GroupID)
	if !exists {
		response := TransferLeaderResponse{
			Success: false,
			Message: "Raft group not found: " + strconv.FormatUint(req.GroupID, 10),
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(response)
		return
	}

	var newLeaderPeer babuzapb.RaftPeerAttribute
	var found bool
	for _, peer := range groupInfo.Peers() {
		if peer.PeerID == req.NewLeaderID {
			newLeaderPeer = peer
			found = true
			break
		}
	}

	if !found {
		response := TransferLeaderResponse{
			Success: false,
			Message: "New leader peer not found in group",
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(response)
		return
	}

	op := operator.NewTransferLeaderOperator(req.GroupID, newLeaderPeer)
	if success := s.coordinator.AddRaftGroupOp(op); !success {
		response := TransferLeaderResponse{
			Success: false,
			Message: "Failed to add transfer leader operation, group may already have an ongoing operation",
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		json.NewEncoder(w).Encode(response)
		return
	}

	response := TransferLeaderResponse{
		Success: true,
		Message: "Transfer leader operation initiated successfully",
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}
