package embedapp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/fanaujie/babuza/examples/distlock/server/lockstore"
	"github.com/fanaujie/babuza/examples/distlock/server/request"
)

type DistLockClient struct {
	addresses []string
	client    *http.Client
	current   int
}

func NewDistLockClient(addresses []string) *DistLockClient {
	return &DistLockClient{
		addresses: addresses,
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
		current: 0,
	}
}

func (c *DistLockClient) nextAddress() string {
	addr := c.addresses[c.current]
	c.current = (c.current + 1) % len(c.addresses)
	return addr
}

func (c *DistLockClient) LeaseGrant(ctx context.Context, ttlSeconds int64) (*lockstore.LeaseResult, error) {
	req := request.LeaseGrantRequest{
		TTLSeconds: ttlSeconds,
	}
	return c.doLeaseRequest(ctx, http.MethodPost, req)
}

func (c *DistLockClient) LeaseRevoke(ctx context.Context, leaseID uint64) (*lockstore.LeaseResult, error) {
	req := request.LeaseRevokeRequest{
		LeaseID: leaseID,
	}
	return c.doLeaseRequest(ctx, http.MethodDelete, req)
}

func (c *DistLockClient) LeaseKeepAlive(ctx context.Context, leaseID uint64) (*lockstore.LeaseResult, error) {
	req := request.LeaseKeepAliveRequest{
		LeaseID: leaseID,
	}
	return c.doLeaseRequest(ctx, http.MethodPut, req)
}

func (c *DistLockClient) doLeaseRequest(ctx context.Context, method string, reqBody any) (*lockstore.LeaseResult, error) {
	for i := 0; i < len(c.addresses); i++ {
		addr := c.nextAddress()
		body, _ := json.Marshal(reqBody)
		httpReq, _ := http.NewRequestWithContext(ctx, method, fmt.Sprintf("http://%s/leases", addr), bytes.NewReader(body))
		httpReq.Header.Set("Content-Type", "application/json")

		resp, err := c.client.Do(httpReq)
		if err != nil {
			continue
		}

		if resp.StatusCode == http.StatusServiceUnavailable {
			resp.Body.Close()
			continue
		}

		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			respBody, _ := io.ReadAll(resp.Body)
			return nil, fmt.Errorf("status %d: %s", resp.StatusCode, string(respBody))
		}

		var result lockstore.LeaseResult
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			return nil, err
		}
		return &result, nil
	}
	return nil, fmt.Errorf("all addresses failed")
}

func (c *DistLockClient) Acquire(ctx context.Context, lockName, ownerID string, leaseID uint64) (*lockstore.LockResult, error) {
	req := request.LockAcquireRequest{
		LockName: lockName,
		OwnerID:  ownerID,
		LeaseID:  leaseID,
	}
	return c.doLockRequest(ctx, http.MethodPost, req)
}

func (c *DistLockClient) AcquireWithWait(ctx context.Context, lockName, ownerID string, leaseID uint64, waitTimeout int64, requestID string) (*lockstore.LockResult, error) {
	req := request.LockAcquireRequest{
		LockName:           lockName,
		OwnerID:            ownerID,
		LeaseID:            leaseID,
		WaitTimeoutSeconds: waitTimeout,
		RequestID:          requestID,
	}
	return c.doLockRequest(ctx, http.MethodPost, req)
}

func (c *DistLockClient) Release(ctx context.Context, lockName, ownerID string, fencingToken uint64) (*lockstore.LockResult, error) {
	req := request.LockReleaseRequest{
		LockName:     lockName,
		OwnerID:      ownerID,
		FencingToken: fencingToken,
	}
	return c.doLockRequest(ctx, http.MethodDelete, req)
}

func (c *DistLockClient) doLockRequest(ctx context.Context, method string, reqBody any) (*lockstore.LockResult, error) {
	for i := 0; i < len(c.addresses); i++ {
		addr := c.nextAddress()
		body, _ := json.Marshal(reqBody)
		httpReq, _ := http.NewRequestWithContext(ctx, method, fmt.Sprintf("http://%s/locks", addr), bytes.NewReader(body))
		httpReq.Header.Set("Content-Type", "application/json")

		resp, err := c.client.Do(httpReq)
		if err != nil {
			continue
		}

		if resp.StatusCode == http.StatusServiceUnavailable {
			resp.Body.Close()
			continue
		}

		defer resp.Body.Close()
		if resp.StatusCode == http.StatusRequestTimeout {
			return nil, fmt.Errorf("wait timeout")
		}
		if resp.StatusCode != http.StatusOK {
			respBody, _ := io.ReadAll(resp.Body)
			return nil, fmt.Errorf("status %d: %s", resp.StatusCode, string(respBody))
		}

		var result lockstore.LockResult
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			return nil, err
		}
		return &result, nil
	}
	return nil, fmt.Errorf("all addresses failed")
}

func (c *DistLockClient) GetLockStatus(ctx context.Context, lockName string) (*lockstore.LockResult, error) {
	for i := 0; i < len(c.addresses); i++ {
		addr := c.nextAddress()
		httpReq, _ := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("http://%s/locks?name=%s", addr, lockName), nil)
		httpReq.Header.Set("X-Linearizable", "true")

		resp, err := c.client.Do(httpReq)
		if err != nil {
			continue
		}

		if resp.StatusCode == http.StatusServiceUnavailable {
			resp.Body.Close()
			continue
		}

		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			respBody, _ := io.ReadAll(resp.Body)
			return nil, fmt.Errorf("status %d: %s", resp.StatusCode, string(respBody))
		}

		var result lockstore.LockResult
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			return nil, err
		}
		return &result, nil
	}
	return nil, fmt.Errorf("all addresses failed")
}

func (c *DistLockClient) Close() error {
	return nil
}

type WaitResult struct {
	Result *lockstore.LockResult
	Err    error
}
