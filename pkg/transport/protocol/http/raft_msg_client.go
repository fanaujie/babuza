package http

import (
	"bytes"
	"errors"
	"fmt"
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
	byteSlice           *allocator.ByteSlice
	raftBatchMsgUrl     string
	raftSnapshotMsgUrl  string
	raftClusterPeersUrl string
	raftAppServiceUrls  string
	client              *http.Client
}

func NewRaftMsgClient(client *http.Client, options Options, u url.URL) *RaftMsgClient {
	u.Path = raftBatchMsgPrefix
	raftBatchMsgUrl := u.String()
	u.Path = raftSnapshotMsgPrefix
	raftSnapshotMsgUrl := u.String()
	u.Path = raftClusterPeersPrefix
	raftClusterPeersUrl := u.String()
	u.Path = raftAppServiceUrlsPrefix
	raftAppServiceUrls := u.String()
	return &RaftMsgClient{
		byteSlice:           allocator.Acquire(options.MaxBufferSize),
		raftBatchMsgUrl:     raftBatchMsgUrl,
		raftSnapshotMsgUrl:  raftSnapshotMsgUrl,
		raftClusterPeersUrl: raftClusterPeersUrl,
		raftAppServiceUrls:  raftAppServiceUrls,
		client:              client,
	}
}

func (r *RaftMsgClient) SendBatchMessage(batchMsg babuzapb.BatchMessage) error {
	//TODO: retry if failed?
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
	req, err := http.NewRequest(http.MethodPost, r.raftBatchMsgUrl,
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
	req, err := http.NewRequest(http.MethodPost, r.raftSnapshotMsgUrl,
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
func (r *RaftMsgClient) GetClusterPeers(request babuzapb.GetClusterPeersRequest) (babuzapb.GetClusterPeersResponse, error) {
	req, err := http.NewRequest(http.MethodGet, r.raftClusterPeersUrl, nil)
	if err != nil {
		return babuzapb.GetClusterPeersResponse{}, err
	}
	q := req.URL.Query()
	q.Add("clusterId", fmt.Sprintf("%d", request.ClusterId))
	q.Add("from", fmt.Sprintf("%d", request.From))
	req.URL.RawQuery = q.Encode()
	res, err := r.client.Do(req)
	if err != nil {
		return babuzapb.GetClusterPeersResponse{}, err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return babuzapb.GetClusterPeersResponse{}, errors.New("")
	}
	clusterPeersRes := babuzapb.GetClusterPeersResponse{}
	if err = decodeExpectedMessage(res.Body, res.ContentLength, &clusterPeersRes); err != nil {
		return babuzapb.GetClusterPeersResponse{}, err
	}
	return clusterPeersRes, nil
}

func (r *RaftMsgClient) PublishApplicationService(request babuzapb.PublishApplicationServiceRequest) (
	babuzapb.PublishApplicationServiceResponse, error) {

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
	req, err := http.NewRequest(http.MethodPost, r.raftAppServiceUrls,
		bytes.NewReader(r.byteSlice.Buffer[:n]))
	if err != nil {
		return babuzapb.PublishApplicationServiceResponse{}, err
	}
	req.ContentLength = int64(n)
	res, err := r.client.Do(req)
	if err != nil {
		return babuzapb.PublishApplicationServiceResponse{}, err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return babuzapb.PublishApplicationServiceResponse{}, errors.New("")
	}
	var pubAppServiceUrlsRes babuzapb.PublishApplicationServiceResponse
	if err = decodeExpectedMessage(res.Body, res.ContentLength, &pubAppServiceUrlsRes); err != nil {
		return babuzapb.PublishApplicationServiceResponse{}, err
	}
	return pubAppServiceUrlsRes, nil
}

func (r *RaftMsgClient) Close() error {
	allocator.Release(r.byteSlice)
	return nil
}
