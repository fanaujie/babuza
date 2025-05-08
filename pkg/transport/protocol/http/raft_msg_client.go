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
)

const (
	raftBatchMsgPrefix       = "/raft/messages"
	raftSnapshotMsgPrefix    = "/raft/snapshot"
	raftClusterPeersPrefix   = "/raft/peers"
	raftAppServiceUrlsPrefix = "/raft/app-service-urls"
)

type RaftMsgClient struct {
	client   *http.Client
	resolver ibabuza.TransportResolver
	urlPool  *UrlPool
}

func NewRaftMsgClient(client *http.Client, resolver ibabuza.TransportResolver, enableTls bool) *RaftMsgClient {
	return &RaftMsgClient{
		client:   client,
		resolver: resolver,
		urlPool:  NewUrlPool(enableTls),
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

func (r *RaftMsgClient) SendBatchMessage(batchMsg babuzapb.BatchMessage) error {
	//TODO: retry if failed?
	if batchMsg.Messages == nil || len(batchMsg.Messages) == 0 {
		return fmt.Errorf("batch message is empty")
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
	return nil
}
