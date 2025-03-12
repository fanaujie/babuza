package http

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/utility/allocator"
	"net/http"
	"net/url"
	"time"
)

const (
	raftBatchMsgPrefix       = "/raft/messages"
	raftSnapshotMsgPrefix    = "/raft/snapshot"
	raftClusterPeersPrefix   = "/raft/peers"
	raftAppServiceUrlsPrefix = "/raft/app-service-urls"
)

type Options struct {
	WriteDeadline   time.Duration
	ReadDeadline    time.Duration
	MaxBufferSize   int
	ShutdownTimeout time.Duration
}

type RaftMsgClient struct {
	byteSlice *allocator.ByteSlice
	client    *http.Client
	resolver  ibabuza.TransportResolver
	targetUrl url.URL
}

func NewRaftMsgClient(client *http.Client, options Options, u url.URL, resolver ibabuza.TransportResolver) *RaftMsgClient {
	return &RaftMsgClient{
		byteSlice: allocator.Acquire(options.MaxBufferSize),
		client:    client,
		resolver:  resolver,
		targetUrl: u,
	}
}

func (r *RaftMsgClient) getUrl(peerId uint64, path string) (url.URL, error) {
	addr, err := r.resolver.ResolvePeerAddress(peerId)
	if err != nil {
		return url.URL{}, err
	}
	result := r.targetUrl
	result.Host = addr
	result.Path = path
	return result, nil
}

func (r *RaftMsgClient) SendBatchMessage(batchMsg babuzapb.BatchMessage) error {
	//TODO: retry if failed?
	if batchMsg.Messages == nil || len(batchMsg.Messages) == 0 {
		return fmt.Errorf("batch message is empty")
	}
	u, err := r.getUrl(batchMsg.Messages[0].To, raftBatchMsgPrefix)
	if err != nil {
		return err
	}
	msgSize := batchMsg.Size()
	var buf []byte
	if msgSize > len(r.byteSlice.Buffer) {
		bufSlice := allocator.Acquire(msgSize)
		defer allocator.Release(bufSlice)
		buf = bufSlice.Buffer[:msgSize]
	} else {
		buf = r.byteSlice.Buffer[:msgSize]
	}
	n, err := batchMsg.MarshalTo(buf)
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPost, u.String(),
		bytes.NewReader(r.byteSlice.Buffer[:n]))
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
		return errors.New("")
	}
	return nil
}

func (r *RaftMsgClient) SendSnapshotMessage(snapMsg babuzapb.SnapshotMessage) error {
	//TODO: retry if failed?
	u, err := r.getUrl(snapMsg.To, raftSnapshotMsgPrefix)
	if err != nil {
		return err
	}
	msgSize := snapMsg.Size()
	var buf []byte
	if msgSize > len(r.byteSlice.Buffer) {
		bufSlice := allocator.Acquire(msgSize)
		defer allocator.Release(bufSlice)
		buf = bufSlice.Buffer[:msgSize]
	} else {
		buf = r.byteSlice.Buffer[:msgSize]
	}
	n, err := snapMsg.MarshalTo(buf)
	req, err := http.NewRequest(http.MethodPost, u.String(),
		bytes.NewReader(r.byteSlice.Buffer[:n]))
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
		return errors.New("")
	}
	return nil
}
func (r *RaftMsgClient) GetClusterPeers(request babuzapb.GetClusterPeersRequest) babuzapb.GetClusterPeersResponse {
	u, err := r.getUrl(request.ToId, raftClusterPeersPrefix)
	if err != nil {
		return babuzapb.GetClusterPeersResponse{
			Status:  babuzapb.FAILED,
			Message: err.Error(),
		}
	}
	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		return babuzapb.GetClusterPeersResponse{
			Status:  babuzapb.FAILED,
			Message: err.Error(),
		}
	}
	q := req.URL.Query()
	q.Add("clusterId", fmt.Sprintf("%d", request.ClusterId))
	q.Add("from", fmt.Sprintf("%d", request.FromId))
	q.Add("to", fmt.Sprintf("%d", request.ToId))
	req.URL.RawQuery = q.Encode()
	res, err := r.client.Do(req)
	if err != nil {
		return babuzapb.GetClusterPeersResponse{
			Status:  babuzapb.FAILED,
			Message: err.Error(),
		}
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return babuzapb.GetClusterPeersResponse{
			Status:  babuzapb.FAILED,
			Message: fmt.Sprintf("unexpected status code: %d", res.StatusCode),
		}
	}
	clusterPeersRes := babuzapb.GetClusterPeersResponse{}
	if err = decodeExpectedMessage(res.Body, res.ContentLength, &clusterPeersRes); err != nil {
		return babuzapb.GetClusterPeersResponse{
			Status:  babuzapb.FAILED,
			Message: err.Error(),
		}
	}
	return clusterPeersRes
}

func (r *RaftMsgClient) PublishApplicationService(request babuzapb.PublishApplicationServiceRequest) babuzapb.PublishApplicationServiceResponse {

	u, err := r.getUrl(request.ToId, raftClusterPeersPrefix)
	if err != nil {
		return babuzapb.PublishApplicationServiceResponse{
			Status:  babuzapb.FAILED,
			Message: err.Error(),
		}
	}
	msgSize := request.Size()
	var buf []byte
	if msgSize > len(r.byteSlice.Buffer) {
		bufSlice := allocator.Acquire(msgSize)
		defer allocator.Release(bufSlice)
		buf = bufSlice.Buffer[:msgSize]
	} else {
		buf = r.byteSlice.Buffer[:msgSize]
	}
	n, err := request.MarshalTo(buf)
	req, err := http.NewRequest(http.MethodPost, u.String(),
		bytes.NewReader(r.byteSlice.Buffer[:n]))
	if err != nil {
		return babuzapb.PublishApplicationServiceResponse{
			Status:  babuzapb.FAILED,
			Message: err.Error(),
		}
	}
	req.ContentLength = int64(n)
	res, err := r.client.Do(req)
	if err != nil {
		return babuzapb.PublishApplicationServiceResponse{
			Status:  babuzapb.FAILED,
			Message: err.Error(),
		}
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return babuzapb.PublishApplicationServiceResponse{
			Status:  babuzapb.FAILED,
			Message: fmt.Sprintf("unexpected status code: %d", res.StatusCode),
		}
	}
	var pubAppServiceUrlsRes babuzapb.PublishApplicationServiceResponse
	if err = decodeExpectedMessage(res.Body, res.ContentLength, &pubAppServiceUrlsRes); err != nil {
		return babuzapb.PublishApplicationServiceResponse{
			Status:  babuzapb.FAILED,
			Message: err.Error(),
		}
	}
	return pubAppServiceUrlsRes
}

func (r *RaftMsgClient) Close() error {
	allocator.Release(r.byteSlice)
	return nil
}
