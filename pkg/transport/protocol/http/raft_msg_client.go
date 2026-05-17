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
	"bytes"
	"errors"
	"fmt"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/transport/protocol/tcp/conn/frame"
	"github.com/fanaujie/babuza/pkg/utility/allocator"
	"io"
	"net/http"
	"net/url"
	"sync"
)

const (
	raftBatchMsgPrefix       = "/raft/messages"
	raftBatchMsgStreamPrefix = "/raft/messages/stream"
	raftSnapshotMsgPrefix    = "/raft/snapshot"
	raftClusterPeersPrefix   = "/raft/peers"
	raftAppServiceUrlsPrefix = "/raft/app-service-urls"
)

type RaftMsgClient struct {
	client               *http.Client
	resolver             ibabuza.TransportResolver
	urlPool              *UrlPool
	messageStreamEnabled bool
	streamMu             sync.Mutex
	stream               *messageStream
}

type messageStream struct {
	body   *io.PipeWriter
	doneCh chan error
}

func NewRaftMsgClient(client *http.Client, resolver ibabuza.TransportResolver, enableTls bool, configs ...ServerConfig) *RaftMsgClient {
	var cfg ServerConfig
	if len(configs) > 0 {
		cfg = configs[0]
	}
	return &RaftMsgClient{
		client:               client,
		resolver:             resolver,
		urlPool:              NewUrlPool(enableTls),
		messageStreamEnabled: cfg.MessageStreamEnabled,
	}
}

func (r *RaftMsgClient) getUrl(peerID uint64, path string) (*url.URL, error) {
	addr, err := r.resolver.ResolvePeerAddress(peerID)
	if err != nil {
		return nil, err
	}
	u := r.urlPool.Acquire()
	u.Host = addr
	u.Path = path
	return u, nil
}

func (r *RaftMsgClient) SendMultiRaftMessage(babuzapb.MultiRaftBatchMessage) error {
	// not supported
	return nil
}
func (r *RaftMsgClient) SendBatchMessage(batchMsg babuzapb.BatchMessage) error {
	//TODO: retry if failed?
	if batchMsg.Messages == nil || len(batchMsg.Messages) == 0 {
		return fmt.Errorf("batch message is empty")
	}
	if r.messageStreamEnabled {
		return r.sendBatchMessageStream(batchMsg)
	}
	u, err := r.getUrl(batchMsg.Messages[0].To, raftBatchMsgPrefix)
	if err != nil {
		return err
	}
	defer r.urlPool.Release(u)
	msgSize := batchMsg.Size()

	bufSlice := allocator.Acquire(msgSize)
	defer allocator.Release(bufSlice)
	buf := bufSlice.Buffer[:msgSize]

	n, err := batchMsg.MarshalTo(buf)
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPost, u.String(), bytes.NewReader(buf[:n]))
	if err != nil {
		return err
	}
	req.ContentLength = int64(n)
	res, err := r.client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return errors.New("unexpected status code")
	}
	return nil
}

func (r *RaftMsgClient) sendBatchMessageStream(batchMsg babuzapb.BatchMessage) error {
	r.streamMu.Lock()
	defer r.streamMu.Unlock()

	if r.stream == nil {
		stream, err := r.openMessageStreamLocked(batchMsg.Messages[0].To)
		if err != nil {
			return err
		}
		r.stream = stream
	}
	select {
	case <-r.stream.doneCh:
		r.stream = nil
		stream, err := r.openMessageStreamLocked(batchMsg.Messages[0].To)
		if err != nil {
			return err
		}
		r.stream = stream
	default:
	}

	bufSize := frame.EncodeSize(batchMsg.Size())
	bufSlice := allocator.Acquire(bufSize)
	defer allocator.Release(bufSlice)

	err := frame.NewWriter(r.stream.body).Encode(bufSlice.Buffer[:bufSize], frame.BatchMsgType, &batchMsg)
	if err != nil {
		r.closeStreamLocked(r.stream, err)
		return err
	}
	return nil
}

func (r *RaftMsgClient) openMessageStreamLocked(peerID uint64) (*messageStream, error) {
	u, err := r.getUrl(peerID, raftBatchMsgStreamPrefix)
	if err != nil {
		return nil, err
	}
	defer r.urlPool.Release(u)

	pr, pw := io.Pipe()
	req, err := http.NewRequest(http.MethodPost, u.String(), pr)
	if err != nil {
		_ = pr.Close()
		_ = pw.Close()
		return nil, err
	}
	stream := &messageStream{
		body:   pw,
		doneCh: make(chan error, 1),
	}
	go r.monitorMessageStream(stream, req)
	return stream, nil
}

func (r *RaftMsgClient) monitorMessageStream(stream *messageStream, req *http.Request) {
	res, err := r.client.Do(req)
	if err == nil {
		if res.StatusCode != http.StatusOK {
			err = fmt.Errorf("unexpected status code: %d", res.StatusCode)
		}
		_ = res.Body.Close()
	}

	stream.doneCh <- err
	r.streamMu.Lock()
	defer r.streamMu.Unlock()
	if r.stream == stream {
		r.stream = nil
	}
}

func (r *RaftMsgClient) closeStreamLocked(stream *messageStream, err error) {
	if stream == nil {
		return
	}
	_ = stream.body.CloseWithError(err)
	if r.stream == stream {
		r.stream = nil
	}
}

func (r *RaftMsgClient) SendSnapshotMessage(snapMsg babuzapb.SnapshotMessage) (babuzapb.SnapshotMessageResponse, error) {
	//TODO: retry if failed?
	var resp babuzapb.SnapshotMessageResponse
	u, err := r.getUrl(snapMsg.To, raftSnapshotMsgPrefix)
	if err != nil {
		return resp, err
	}
	defer r.urlPool.Release(u)
	msgSize := snapMsg.Size()

	bufSlice := allocator.Acquire(msgSize)
	defer allocator.Release(bufSlice)
	buf := bufSlice.Buffer[:msgSize]

	n, err := snapMsg.MarshalTo(buf)
	req, err := http.NewRequest(http.MethodPost, u.String(), bytes.NewReader(buf[:n]))
	if err != nil {
		return resp, err
	}
	req.ContentLength = int64(n)
	res, err := r.client.Do(req)
	if err != nil {
		return resp, err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return resp, errors.New("unexpected status code")
	}
	if err = decodeExpectedMessage(res.Body, res.ContentLength, &resp); err != nil {
		return resp, err
	}
	return resp, nil
}
func (r *RaftMsgClient) GetClusterPeers(request babuzapb.GetClusterPeersRequest) (babuzapb.GetClusterPeersResponse, error) {
	var resp babuzapb.GetClusterPeersResponse
	u, err := r.getUrl(request.To, raftClusterPeersPrefix)
	if err != nil {
		return resp, err
	}
	defer r.urlPool.Release(u)
	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		return resp, err
	}
	q := req.URL.Query()
	q.Add("clusterID", fmt.Sprintf("%d", request.ClusterID))
	q.Add("from", fmt.Sprintf("%d", request.From))
	q.Add("to", fmt.Sprintf("%d", request.To))
	req.URL.RawQuery = q.Encode()
	res, err := r.client.Do(req)
	if err != nil {
		return resp, err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return resp, fmt.Errorf("unexpected status code: %d", res.StatusCode)
	}
	if err = decodeExpectedMessage(res.Body, res.ContentLength, &resp); err != nil {
		return resp, err
	}
	return resp, nil
}

func (r *RaftMsgClient) PublishApplicationService(request babuzapb.PublishApplicationServiceRequest) (babuzapb.PublishApplicationServiceResponse, error) {
	var resp babuzapb.PublishApplicationServiceResponse
	u, err := r.getUrl(request.To, raftAppServiceUrlsPrefix)
	if err != nil {
		return resp, nil
	}
	defer r.urlPool.Release(u)
	msgSize := request.Size()
	bufSlice := allocator.Acquire(msgSize)
	defer allocator.Release(bufSlice)
	buf := bufSlice.Buffer[:msgSize]

	n, err := request.MarshalTo(buf)
	req, err := http.NewRequest(http.MethodPost, u.String(),
		bytes.NewReader(buf[:n]))
	if err != nil {
		return resp, nil
	}
	req.ContentLength = int64(n)
	res, err := r.client.Do(req)
	if err != nil {
		return resp, nil
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return resp, fmt.Errorf("unexpected status code: %d", res.StatusCode)
	}
	if err = decodeExpectedMessage(res.Body, res.ContentLength, &resp); err != nil {
		return resp, nil
	}
	return resp, nil
}

func (r *RaftMsgClient) Close() error {
	r.streamMu.Lock()
	defer r.streamMu.Unlock()
	if r.stream == nil {
		return nil
	}
	err := r.stream.body.Close()
	r.stream = nil
	return err
}
