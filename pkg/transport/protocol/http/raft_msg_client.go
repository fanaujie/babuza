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
	"context"
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
	"time"
)

const (
	raftBatchMsgPrefix       = "/raft/messages"
	raftBatchMsgStreamPrefix = "/raft/messages/stream"
	raftSnapshotMsgPrefix    = "/raft/snapshot"
	raftSnapshotStreamPrefix = "/raft/snapshot/stream"
	raftClusterPeersPrefix   = "/raft/peers"
	raftAppServiceUrlsPrefix = "/raft/app-service-urls"
)

type RaftMsgClient struct {
	client               *http.Client
	snapshotStreamClient *http.Client
	resolver             ibabuza.TransportResolver
	urlPool              *UrlPool
	messageStreamEnabled bool
	messageStreamHub     *MessageStreamHub
	localNodeID          uint64
	streamHandler        ibabuza.RaftMessageHandler
	snapshotStreamMu     sync.Mutex
	snapshotStream       *snapshotStream
	receiveStreamMu      sync.Mutex
	receiveStreamCancel  context.CancelFunc
	receiveStreamDone    chan struct{}
	receiveStreamPeerID  uint64
}

type snapshotStream struct {
	peerID    uint64
	clusterID uint64
	groupID   uint64
	term      uint64
	index     uint64
	body      *io.PipeWriter
	doneCh    chan struct{}
	result    snapshotStreamResult
}

type snapshotStreamResult struct {
	resp babuzapb.SnapshotMessageResponse
	err  error
}

func NewRaftMsgClient(client *http.Client, resolver ibabuza.TransportResolver, enableTls bool, configs ...ServerConfig) *RaftMsgClient {
	return NewRaftMsgClientWithSnapshotStreamClient(client, client, resolver, enableTls, configs...)
}

func NewRaftMsgClientWithSnapshotStreamClient(client *http.Client, snapshotStreamClient *http.Client, resolver ibabuza.TransportResolver, enableTls bool, configs ...ServerConfig) *RaftMsgClient {
	var cfg ServerConfig
	if len(configs) > 0 {
		cfg = configs[0]
	}
	if snapshotStreamClient == nil {
		snapshotStreamClient = client
	}
	return &RaftMsgClient{
		client:               client,
		snapshotStreamClient: snapshotStreamClient,
		resolver:             resolver,
		urlPool:              NewUrlPool(enableTls),
		messageStreamEnabled: cfg.MessageStreamEnabled,
	}
}

func NewRaftMsgClientWithMessageStreamHub(client *http.Client, resolver ibabuza.TransportResolver, enableTls bool,
	config ServerConfig, hub *MessageStreamHub, localNodeID uint64, streamHandler ibabuza.RaftMessageHandler) *RaftMsgClient {
	r := NewRaftMsgClientWithSnapshotStreamClient(client, client, resolver, enableTls, config)
	r.messageStreamHub = hub
	r.localNodeID = localNodeID
	r.streamHandler = streamHandler
	return r
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
	peerID, err := batchMessagePeer(batchMsg)
	if err != nil {
		return err
	}
	if r.messageStreamEnabled && r.messageStreamHub != nil {
		err = r.messageStreamHub.send(peerID, batchMsg)
		if err == nil {
			return nil
		}
		if !errors.Is(err, errMessageStreamUnavailable) && !errors.Is(err, errMessageStreamBackpressure) {
			return err
		}
	}
	return r.sendBatchMessageShort(peerID, batchMsg)
}

func (r *RaftMsgClient) sendBatchMessageShort(peerID uint64, batchMsg babuzapb.BatchMessage) error {
	u, err := r.getUrl(peerID, raftBatchMsgPrefix)
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

func batchMessagePeer(batchMsg babuzapb.BatchMessage) (uint64, error) {
	if batchMsg.Messages == nil || len(batchMsg.Messages) == 0 {
		return 0, fmt.Errorf("batch message is empty")
	}
	peerID := batchMsg.Messages[0].To
	for i := 1; i < len(batchMsg.Messages); i++ {
		if batchMsg.Messages[i].To != peerID {
			return 0, fmt.Errorf("batch message contains multiple peers: %d and %d", peerID, batchMsg.Messages[i].To)
		}
	}
	return peerID, nil
}

func (r *RaftMsgClient) StartMessageStream(peerID uint64) {
	if !r.messageStreamEnabled || r.streamHandler == nil || r.localNodeID == 0 {
		return
	}

	r.receiveStreamMu.Lock()
	if r.receiveStreamCancel != nil {
		r.receiveStreamMu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	r.receiveStreamCancel = cancel
	r.receiveStreamDone = done
	r.receiveStreamPeerID = peerID
	r.receiveStreamMu.Unlock()

	go func() {
		defer close(done)
		r.receiveMessageStreamLoop(ctx, peerID)
	}()
}

func (r *RaftMsgClient) receiveMessageStreamLoop(ctx context.Context, peerID uint64) {
	backoff := 50 * time.Millisecond
	for {
		if ctx.Err() != nil {
			return
		}
		_ = r.receiveMessageStreamOnce(ctx, peerID)
		if ctx.Err() != nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff < time.Second {
			backoff *= 2
			if backoff > time.Second {
				backoff = time.Second
			}
		}
	}
}

func (r *RaftMsgClient) receiveMessageStreamOnce(ctx context.Context, peerID uint64) error {
	u, err := r.getUrl(peerID, raftBatchMsgStreamPrefix)
	if err != nil {
		return err
	}
	defer r.urlPool.Release(u)
	q := u.Query()
	q.Set("from", fmt.Sprintf("%d", r.localNodeID))
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return err
	}
	res, err := r.client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", res.StatusCode)
	}

	reader := frame.NewReader(res.Body)
	for {
		eof, err := reader.ReadFrameOrEOF(func(msgType frame.MessageType, msgBuf []byte) error {
			if msgType != frame.BatchMsgType {
				return fmt.Errorf("unsupported message type: %d", msgType)
			}
			if len(msgBuf) == 0 {
				return fmt.Errorf("batch message is empty")
			}
			batchMsg := babuzapb.BatchMessage{}
			if err := batchMsg.Unmarshal(msgBuf); err != nil {
				return err
			}
			if len(batchMsg.Messages) == 0 {
				return fmt.Errorf("batch message is empty")
			}
			r.streamHandler.ProcessBatchMessage(batchMsg)
			return nil
		})
		if eof {
			return io.EOF
		}
		if err != nil {
			return err
		}
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

func (r *RaftMsgClient) sendSnapshotMessageStream(snapMsg babuzapb.SnapshotMessage) (babuzapb.SnapshotMessageResponse, error) {
	switch snapMsg.Type {
	case babuzapb.SnapshotMessageType_Metadata:
		return r.sendSnapshotMetadataStream(snapMsg)
	case babuzapb.SnapshotMessageType_Chunk:
		return r.sendSnapshotChunkStream(snapMsg)
	case babuzapb.SnapshotMessageType_Finish:
		return r.finishSnapshotStream(snapMsg)
	default:
		return babuzapb.SnapshotMessageResponse{}, fmt.Errorf("unsupported snapshot message type: %s", snapMsg.Type)
	}
}

func (r *RaftMsgClient) sendSnapshotMetadataStream(snapMsg babuzapb.SnapshotMessage) (babuzapb.SnapshotMessageResponse, error) {
	r.snapshotStreamMu.Lock()
	defer r.snapshotStreamMu.Unlock()

	if r.snapshotStream != nil {
		if result, ok := r.finishedSnapshotStreamResultLocked(r.snapshotStream); ok {
			return result.resp, result.err
		}
		return babuzapb.SnapshotMessageResponse{}, fmt.Errorf("snapshot stream already active for peer %d", r.snapshotStream.peerID)
	}
	stream, err := r.openSnapshotStreamLocked(snapMsg)
	if err != nil {
		return babuzapb.SnapshotMessageResponse{}, err
	}
	r.snapshotStream = stream
	if err := r.writeSnapshotFrameLocked(stream, snapMsg); err != nil {
		r.closeSnapshotStreamLocked(stream, err)
		return babuzapb.SnapshotMessageResponse{}, err
	}
	return snapshotStreamSuccessResponse(), nil
}

func (r *RaftMsgClient) sendSnapshotChunkStream(snapMsg babuzapb.SnapshotMessage) (babuzapb.SnapshotMessageResponse, error) {
	r.snapshotStreamMu.Lock()
	defer r.snapshotStreamMu.Unlock()

	stream, result, err := r.activeSnapshotStreamLocked()
	if err != nil {
		return babuzapb.SnapshotMessageResponse{}, err
	}
	if result != nil {
		return result.resp, result.err
	}
	if err := stream.validateMessage(snapMsg); err != nil {
		return babuzapb.SnapshotMessageResponse{}, err
	}
	if err := r.writeSnapshotFrameLocked(stream, snapMsg); err != nil {
		if result, ok := r.finishedSnapshotStreamResultLocked(stream); ok {
			return result.resp, result.err
		}
		r.closeSnapshotStreamLocked(stream, err)
		return babuzapb.SnapshotMessageResponse{}, err
	}
	return snapshotStreamSuccessResponse(), nil
}

func (r *RaftMsgClient) finishSnapshotStream(snapMsg babuzapb.SnapshotMessage) (babuzapb.SnapshotMessageResponse, error) {
	r.snapshotStreamMu.Lock()
	defer r.snapshotStreamMu.Unlock()

	stream, result, err := r.activeSnapshotStreamLocked()
	if err != nil {
		return babuzapb.SnapshotMessageResponse{}, err
	}
	if result != nil {
		return result.resp, result.err
	}
	if err := stream.validateMessage(snapMsg); err != nil {
		return babuzapb.SnapshotMessageResponse{}, err
	}
	if err := r.writeSnapshotFrameLocked(stream, snapMsg); err != nil {
		if result, ok := r.finishedSnapshotStreamResultLocked(stream); ok {
			return result.resp, result.err
		}
		r.closeSnapshotStreamLocked(stream, err)
		return babuzapb.SnapshotMessageResponse{}, err
	}
	if err := stream.body.Close(); err != nil {
		if result, ok := r.finishedSnapshotStreamResultLocked(stream); ok {
			return result.resp, result.err
		}
		r.closeSnapshotStreamLocked(stream, err)
		return babuzapb.SnapshotMessageResponse{}, err
	}
	finalResult := r.readSnapshotResponseLocked(stream)
	if r.snapshotStream == stream {
		r.snapshotStream = nil
	}
	return finalResult.resp, finalResult.err
}

func (r *RaftMsgClient) openSnapshotStreamLocked(snapMsg babuzapb.SnapshotMessage) (*snapshotStream, error) {
	u, err := r.getUrl(snapMsg.To, raftSnapshotStreamPrefix)
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
	stream := &snapshotStream{
		peerID:    snapMsg.To,
		clusterID: snapMsg.ClusterID,
		groupID:   snapMsg.GroupID,
		term:      snapMsg.Term,
		index:     snapMsg.Index,
		body:      pw,
		doneCh:    make(chan struct{}),
	}
	go r.monitorSnapshotStream(stream, req)
	return stream, nil
}

func (r *RaftMsgClient) monitorSnapshotStream(stream *snapshotStream, req *http.Request) {
	var result snapshotStreamResult
	defer func() {
		if result.err != nil || result.resp.Status != babuzapb.SUCCESS {
			_ = stream.body.CloseWithError(result.err)
		}
		stream.result = result
		close(stream.doneCh)
	}()
	res, err := r.snapshotStreamClient.Do(req)
	if err != nil {
		result.err = err
		return
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		result.err = fmt.Errorf("unexpected status code: %d", res.StatusCode)
		return
	}
	reader := frame.NewReader(res.Body)
	for {
		eof, err := reader.ReadFrameOrEOF(func(msgType frame.MessageType, msgBuf []byte) error {
			if msgType != frame.SnapshotMsgResType {
				return fmt.Errorf("unsupported message type: %d", msgType)
			}
			return result.resp.Unmarshal(msgBuf)
		})
		if err != nil {
			result.err = err
			return
		}
		if eof {
			result.err = fmt.Errorf("snapshot stream closed before response")
			return
		}
		if result.resp.Status != babuzapb.SUCCESS {
			_ = stream.body.CloseWithError(fmt.Errorf("snapshot stream failed: %s", result.resp.Message))
		}
		return
	}
}

func (r *RaftMsgClient) activeSnapshotStreamLocked() (*snapshotStream, *snapshotStreamResult, error) {
	if r.snapshotStream == nil {
		return nil, nil, fmt.Errorf("snapshot stream is not active")
	}
	if result, ok := r.finishedSnapshotStreamResultLocked(r.snapshotStream); ok {
		return nil, &result, nil
	}
	return r.snapshotStream, nil, nil
}

func (r *RaftMsgClient) finishedSnapshotStreamResultLocked(stream *snapshotStream) (snapshotStreamResult, bool) {
	select {
	case <-stream.doneCh:
		r.closeSnapshotStreamLocked(stream, stream.result.err)
		return stream.result, true
	default:
		return snapshotStreamResult{}, false
	}
}

func (s *snapshotStream) validateMessage(snapMsg babuzapb.SnapshotMessage) error {
	if snapMsg.To != s.peerID {
		return fmt.Errorf("snapshot stream peer mismatch: active=%d message=%d", s.peerID, snapMsg.To)
	}
	if snapMsg.ClusterID != s.clusterID {
		return fmt.Errorf("snapshot stream cluster mismatch: active=%d message=%d", s.clusterID, snapMsg.ClusterID)
	}
	if snapMsg.GroupID != s.groupID {
		return fmt.Errorf("snapshot stream group mismatch: active=%d message=%d", s.groupID, snapMsg.GroupID)
	}
	if snapMsg.Term != s.term {
		return fmt.Errorf("snapshot stream term mismatch: active=%d message=%d", s.term, snapMsg.Term)
	}
	if snapMsg.Index != s.index {
		return fmt.Errorf("snapshot stream index mismatch: active=%d message=%d", s.index, snapMsg.Index)
	}
	return nil
}

func (r *RaftMsgClient) readSnapshotResponseLocked(stream *snapshotStream) snapshotStreamResult {
	<-stream.doneCh
	return stream.result
}

func (r *RaftMsgClient) writeSnapshotFrameLocked(stream *snapshotStream, snapMsg babuzapb.SnapshotMessage) error {
	bufSize := frame.EncodeSize(snapMsg.Size())
	bufSlice := allocator.Acquire(bufSize)
	defer allocator.Release(bufSlice)
	return frame.NewWriter(stream.body).Encode(bufSlice.Buffer[:bufSize], frame.SnapshotMsgReqType, &snapMsg)
}

func (r *RaftMsgClient) closeSnapshotStreamLocked(stream *snapshotStream, err error) {
	if stream == nil {
		return
	}
	_ = stream.body.CloseWithError(err)
	if r.snapshotStream == stream {
		r.snapshotStream = nil
	}
}

func snapshotStreamSuccessResponse() babuzapb.SnapshotMessageResponse {
	return babuzapb.SnapshotMessageResponse{Status: babuzapb.SUCCESS}
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
	var err error

	r.receiveStreamMu.Lock()
	receiveCancel := r.receiveStreamCancel
	receiveDone := r.receiveStreamDone
	r.receiveStreamCancel = nil
	r.receiveStreamDone = nil
	r.receiveStreamMu.Unlock()
	if receiveCancel != nil {
		receiveCancel()
		<-receiveDone
	}

	r.snapshotStreamMu.Lock()
	if r.snapshotStream != nil {
		if closeErr := r.snapshotStream.body.Close(); err == nil {
			err = closeErr
		}
		r.snapshotStream = nil
	}
	r.snapshotStreamMu.Unlock()

	return err
}
