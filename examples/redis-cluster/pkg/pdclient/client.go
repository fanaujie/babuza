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


package pdclient

import (
	"context"
	"github.com/fanaujie/babuza/examples/redis-cluster/pkg/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"sync"
)

type PDClient struct {
	mu     sync.RWMutex
	addr   string
	client pb.PDServiceClient
	conn   *grpc.ClientConn
}

func NewPDClient(addr string) (*PDClient, error) {
	pdClient := &PDClient{
		addr: addr,
	}

	if err := pdClient.connect(); err != nil {
		return nil, err
	}

	return pdClient, nil
}

func (pc *PDClient) connect() error {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	if pc.conn != nil {
		pc.conn.Close()
		pc.conn = nil
		pc.client = nil
	}

	conn, err := grpc.NewClient(pc.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	pc.conn = conn
	pc.client = pb.NewPDServiceClient(conn)
	return nil
}

func (pc *PDClient) Close() error {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	if pc.conn != nil {
		err := pc.conn.Close()
		pc.conn = nil
		pc.client = nil
		return err
	}

	return nil
}

func (pc *PDClient) StoreHeartbeat(ctx context.Context, req *pb.StoreHeartbeatReq) (*pb.StoreHeartbeatResp, error) {
	return pc.client.StoreHeartbeat(ctx, req)
}

func (pc *PDClient) RaftGroupLeaderHeartbeat(ctx context.Context, req *pb.RaftGroupLeaderHeartbeatReq) (*pb.RaftGroupLeaderHeartbeatResp, error) {
	return pc.client.RaftGroupLeaderHeartbeat(ctx, req)
}
