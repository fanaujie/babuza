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


package client

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/fanaujie/babuza/examples/kvstore/server/api"
	"github.com/fanaujie/babuza/examples/kvstore/server/kvstore"
	"github.com/fanaujie/babuza/examples/kvstore/server/response"
	"github.com/fanaujie/babuza/raft"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"net/http"
	"net/url"
	"testing"
	"time"
)

type sumRequest struct {
	NumA int
	NumB int
}

type sumResponse struct {
	Sum int
}

func TestSendHttpRequest_Session(t *testing.T) {
	m1 := http.NewServeMux()
	m1.HandleFunc("/sessions", func(writer http.ResponseWriter, request *http.Request) {
		res := &response.RegisterSessionResponse{
			SessionId: 100,
		}
		if err := json.NewEncoder(writer).Encode(res); err != nil {
			http.Error(writer, err.Error(), http.StatusInternalServerError)
		}
	})

	httpSrv1 := &http.Server{
		Addr:    "127.0.0.1:10000",
		Handler: m1,
	}

	go httpSrv1.ListenAndServe()
	defer func() {
		httpSrv1.Shutdown(context.Background())
	}()
	time.Sleep(time.Second) //waiting for server to start

	ms := NewManualIncrementSession()
	c1, err := CreateKvStoreClient(Config{
		AutoSyncInterval: time.Second,
		KvStoreClusterMembers: []ClusterPeer{
			{
				Id:               1,
				KvServiceAddress: "127.0.0.1:10000",
			},
		},
	}, ms)
	assert.Nil(t, err)
	defer c1.Close()
	s1, _ := c1.Session()
	assert.Equal(t, uint64(100), s1.SessionID)
	assert.Equal(t, uint64(0), s1.SequenceNumber)
	ms.SetSequenceNumber(5)
	s1, _ = c1.Session()
	assert.Equal(t, uint64(5), s1.SequenceNumber)

	c2, err := CreateKvStoreClient(Config{
		AutoSyncInterval: time.Second,
		KvStoreClusterMembers: []ClusterPeer{
			{
				Id:               1,
				KvServiceAddress: "127.0.0.1:10000",
			},
		},
	}, NewAutoIncrementSession())
	assert.Nil(t, err)
	s2, _ := c2.Session()
	assert.Equal(t, uint64(100), s2.SessionID)
	assert.Equal(t, uint64(1), s2.SequenceNumber)
	s2, _ = c2.Session()
	assert.Equal(t, uint64(2), s2.SequenceNumber)

	c3, err := CreateKvStoreClient(Config{
		AutoSyncInterval: time.Second,
		KvStoreClusterMembers: []ClusterPeer{
			{
				Id:               1,
				KvServiceAddress: "127.0.0.1:10000",
			},
		},
	}, NewNoOpSession())
	assert.Nil(t, err)
	s3, _ := c3.Session()
	assert.Equal(t, uint64(0), s3.SessionID)
	assert.Equal(t, uint64(0), s3.SequenceNumber)
	s3, _ = c3.Session()
	assert.Equal(t, uint64(0), s3.SequenceNumber)
}

func TestSendHttpRequest_AutoSync(t *testing.T) {
	m1 := http.NewServeMux()
	m1.HandleFunc(api.ClusterPeersHttpPath, func(writer http.ResponseWriter, request *http.Request) {
		res := &response.ClusterConfigurationResponse{
			Peers: []response.ClusterPeer{
				{
					Id:                1,
					RaftListenAddr:    "127.0.0.1:20000",
					AppServiceAddress: "127.0.0.1:10000",
				},
				{
					Id:                2,
					RaftListenAddr:    "127.0.0.1:20001",
					AppServiceAddress: "127.0.0.1:10001",
				},
			},
		}
		if err := json.NewEncoder(writer).Encode(res); err != nil {
			http.Error(writer, err.Error(), http.StatusInternalServerError)
		}
	})

	httpSrv1 := &http.Server{
		Addr:    "127.0.0.1:10000",
		Handler: m1,
	}

	go httpSrv1.ListenAndServe()
	defer func() {
		httpSrv1.Shutdown(context.Background())
	}()
	time.Sleep(time.Second) //waiting for server to start
	c, err := CreateKvStoreClient(Config{
		AutoSyncInterval: time.Second,
		KvStoreClusterMembers: []ClusterPeer{
			{
				Id:               1,
				KvServiceAddress: "127.0.0.1:10000",
			},
		},
	}, NewNoOpSession())
	assert.Nil(t, err)
	time.Sleep(time.Second * 2) //waiting for auto-sync
	assert.Nil(t, c.Close())
	expected := []ClusterPeer{
		{
			Id:               1,
			KvServiceAddress: "127.0.0.1:10000",
		},
		{
			Id:               2,
			KvServiceAddress: "127.0.0.1:10001",
		},
	}
	assert.Equal(t, expected, c.proxy.peers)
}

func TestSendHttpRequest_Redirect(t *testing.T) {

	m1 := http.NewServeMux()
	m1.HandleFunc("/test", func(writer http.ResponseWriter, request *http.Request) {
		//redirect to httpSrv2
		http.Redirect(writer, request, "http://127.0.0.1:10001", http.StatusMovedPermanently)
	})
	m1.HandleFunc("/get", func(writer http.ResponseWriter, request *http.Request) {
		b, _ := json.Marshal(&sumResponse{
			Sum: 10,
		})
		writer.Write(b)
	})

	m2 := http.NewServeMux()
	m2.HandleFunc("/test", func(writer http.ResponseWriter, request *http.Request) {
		s := sumRequest{}
		b, _ := ioutil.ReadAll(request.Body)
		_ = json.Unmarshal(b, &s)
		b, _ = json.Marshal(&sumResponse{
			Sum: s.NumA + s.NumB,
		})
		writer.Write(b)
	})

	httpSrv1 := &http.Server{
		Addr:    "127.0.0.1:10000",
		Handler: m1,
	}
	httpSrv2 := &http.Server{
		Addr:    "127.0.0.1:10001",
		Handler: m2,
	}

	go httpSrv1.ListenAndServe()
	go httpSrv2.ListenAndServe()
	defer func() {
		httpSrv1.Shutdown(context.Background())
		httpSrv2.Shutdown(context.Background())
	}()
	time.Sleep(time.Second) //waiting for server to start
	c, err := CreateKvStoreClient(Config{
		KvStoreClusterMembers: []ClusterPeer{
			{
				Id:               1,
				KvServiceAddress: "127.0.0.1:10000",
			},
			{
				Id:               2,
				KvServiceAddress: "127.0.0.1:10001",
			},
		},
	}, NewNoOpSession())
	assert.Nil(t, err)
	defer c.Close()
	req := sumRequest{
		NumA: 1,
		NumB: 2,
	}
	res := sumResponse{}
	assert.Nil(t, c.proxy.SendRequest(context.Background(), func(ctx context.Context, leaderUrl url.URL) (*http.Request, error) {
		leaderUrl.Path = "test"
		b, err := json.Marshal(&req)
		if err != nil {
			return nil, err
		}
		return http.NewRequest(http.MethodPost, leaderUrl.String(), bytes.NewReader(b))
	}, &res))
	assert.Equal(t, 3, res.Sum)
}

func TestSendHttpRequest_ServiceUnavailable(t *testing.T) {

	m1 := http.NewServeMux()
	m1.HandleFunc("/test", func(writer http.ResponseWriter, request *http.Request) {
		http.Error(writer, "", http.StatusServiceUnavailable)
	})
	m2 := http.NewServeMux()
	m2.HandleFunc("/test", func(writer http.ResponseWriter, request *http.Request) {
		s := sumRequest{}
		b, _ := ioutil.ReadAll(request.Body)
		_ = json.Unmarshal(b, &s)
		b, _ = json.Marshal(&sumResponse{
			Sum: s.NumA + s.NumB,
		})
		writer.Write(b)
	})
	httpSrv1 := &http.Server{
		Addr:    "127.0.0.1:10000",
		Handler: m1,
	}
	httpSrv2 := &http.Server{
		Addr:    "127.0.0.1:10001",
		Handler: m2,
	}

	go httpSrv1.ListenAndServe()
	go httpSrv2.ListenAndServe()
	defer func() {
		httpSrv1.Shutdown(context.Background())
		httpSrv2.Shutdown(context.Background())
	}()
	time.Sleep(time.Second) //waiting for server to start
	c, err := CreateKvStoreClient(Config{
		KvStoreClusterMembers: []ClusterPeer{
			{
				Id:               1,
				KvServiceAddress: "127.0.0.1:10000",
			},
			{
				Id:               2,
				KvServiceAddress: "127.0.0.1:10001",
			},
		},
	}, NewNoOpSession())
	assert.Nil(t, err)
	defer c.Close()
	req := sumRequest{
		NumA: 1,
		NumB: 2,
	}
	res := sumResponse{}
	assert.Nil(t, c.proxy.SendRequest(context.Background(), func(ctx context.Context, leaderUrl url.URL) (*http.Request, error) {
		leaderUrl.Path = "test"
		b, err := json.Marshal(&req)
		if err != nil {
			return nil, err
		}
		return http.NewRequest(http.MethodPost, leaderUrl.String(), bytes.NewReader(b))
	}, &res))
	assert.Equal(t, 3, res.Sum)
}

func TestSendHttpRequest_DirectKvStore(t *testing.T) {

	m1 := http.NewServeMux()
	m1.HandleFunc("/test", func(writer http.ResponseWriter, request *http.Request) {
		http.Error(writer, raft.ErrNotLeader.Error(), http.StatusServiceUnavailable)
	})
	m2 := http.NewServeMux()
	m2.HandleFunc("/test", func(writer http.ResponseWriter, request *http.Request) {
		json.NewEncoder(writer).Encode(&response.KvStoreResponse{
			KvResult: kvstore.KvResult{
				Command: 100,
				Key:     "a",
				Value:   "b",
			},
		})
	})
	m3 := http.NewServeMux()
	m3.HandleFunc("/test", func(writer http.ResponseWriter, request *http.Request) {
		http.Redirect(writer, request, "http://127.0.0.1:20000", http.StatusMovedPermanently)
	})
	httpSrv1 := &http.Server{
		Addr:    "127.0.0.1:10000",
		Handler: m1,
	}
	httpSrv2 := &http.Server{
		Addr:    "127.0.0.1:10001",
		Handler: m2,
	}
	httpSrv3 := &http.Server{
		Addr:    "127.0.0.1:10002",
		Handler: m3,
	}
	go httpSrv1.ListenAndServe()
	go httpSrv2.ListenAndServe()
	go httpSrv3.ListenAndServe()
	defer func() {
		httpSrv1.Shutdown(context.Background())
		httpSrv2.Shutdown(context.Background())
		httpSrv3.Shutdown(context.Background())
	}()
	time.Sleep(time.Second) //waiting for server to start
	c, err := CreateKvStoreClient(Config{
		KvStoreClusterMembers: []ClusterPeer{
			{
				Id:               1,
				KvServiceAddress: "127.0.0.1:10000",
			},
			{
				Id:               2,
				KvServiceAddress: "127.0.0.1:10001",
			},
			{
				Id:               3,
				KvServiceAddress: "127.0.0.1:10002",
			},
		},
	}, NewNoOpSession())
	assert.Nil(t, err)
	defer c.Close()

	res := response.KvStoreResponse{}
	assert.ErrorIs(t, raft.ErrNotLeader, c.SendRequestWithPeerId(3, func(leaderUrl url.URL) (*http.Request, error) {
		leaderUrl.Path = "test"
		return http.NewRequest(http.MethodPost, leaderUrl.String(), nil)
	}, &res))
	assert.Nil(t, c.SendRequestWithPeerId(2, func(leaderUrl url.URL) (*http.Request, error) {
		leaderUrl.Path = "test"
		return http.NewRequest(http.MethodPost, leaderUrl.String(), nil)
	}, &res))
	assert.Equal(t, uint64(100), res.Command)
	assert.Equal(t, "a", res.Key)
	assert.Equal(t, "b", res.Value)
}
