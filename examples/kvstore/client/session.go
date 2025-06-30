package client

import (
	"context"
	"fmt"
	"github.com/fanaujie/babuza/examples/kvstore/server/api"
	"github.com/fanaujie/babuza/examples/kvstore/server/response"
	"net/http"
	"net/url"
)

type ISession interface {
	Register(lb *leaderProxyClient, httpSessionPath string) error
	Unregister(ctx context.Context, lb *leaderProxyClient, httpSessionPath string) error
	ClientSession() Session
	Response(sequenceNum uint64)
}

type Session struct {
	SessionID                         uint64
	SequenceNumber                    uint64
	LowestSequenceNumberNotYetReplied uint64
}

type AutoIncrementSession struct {
	Session
}

func NewAutoIncrementSession() *AutoIncrementSession {
	return &AutoIncrementSession{}
}

func (s *AutoIncrementSession) Register(lb *leaderProxyClient, httpSessionPath string) error {
	var res response.RegisterSessionResponse
	if err := lb.SendRequest(context.Background(), func(reqCtx context.Context, leaderUrl url.URL) (*http.Request, error) {
		leaderUrl.Path = httpSessionPath
		return http.NewRequest(http.MethodPost, leaderUrl.String(), nil)
	}, &res); err != nil {
		return err
	}
	s.SessionID = res.SessionId
	return nil
}

func (s *AutoIncrementSession) Unregister(ctx context.Context, lb *leaderProxyClient, httpSessionPath string) error {
	var res response.UnregisterSessionResponse
	if err := lb.SendRequest(ctx, func(reqCtx context.Context, leaderUrl url.URL) (*http.Request, error) {
		leaderUrl.Path = httpSessionPath
		req, err := http.NewRequest(http.MethodDelete, leaderUrl.String(), nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set(api.SessionIDHeader, fmt.Sprintf("%d", s.SessionID))
		return req, nil
	}, &res); err != nil {
		return err
	}
	if res.IsUnregistered {
		if res.SessionID != s.SessionID {
			return fmt.Errorf("unregister: session id does not match")
		}
		return nil
	}
	return fmt.Errorf("unregister: not found session id to unregister")
}

func (s *AutoIncrementSession) ClientSession() Session {
	s.Session.SequenceNumber++
	return s.Session
}

func (s *AutoIncrementSession) Response(sequenceNumber uint64) {

}

type NoOpSession struct {
}

func NewNoOpSession() *NoOpSession {
	return &NoOpSession{}
}

func (s *NoOpSession) Register(*leaderProxyClient, string) error {
	return nil
}

func (s *NoOpSession) Unregister(context.Context, *leaderProxyClient, string) error {
	return nil
}

func (s *NoOpSession) ClientSession() Session {
	return Session{}
}

func (s *NoOpSession) Response(sequenceNumber uint64) {

}

type ManualIncrementSession struct {
	session Session
}

func NewManualIncrementSession() *ManualIncrementSession {
	return &ManualIncrementSession{}
}

func (s *ManualIncrementSession) Register(lb *leaderProxyClient, httpSessionPath string) error {
	var res response.RegisterSessionResponse
	if err := lb.SendRequest(context.Background(), func(reqCtx context.Context, leaderUrl url.URL) (*http.Request, error) {
		leaderUrl.Path = httpSessionPath
		return http.NewRequest(http.MethodPost, leaderUrl.String(), nil)
	}, &res); err != nil {
		return err
	}
	s.session.SessionID = res.SessionId
	return nil
}

func (s *ManualIncrementSession) Unregister(ctx context.Context, lb *leaderProxyClient, httpSessionPath string) error {
	var res response.UnregisterSessionResponse
	if err := lb.SendRequest(ctx, func(reqCtx context.Context, leaderUrl url.URL) (*http.Request, error) {
		leaderUrl.Path = httpSessionPath
		return http.NewRequest(http.MethodDelete, leaderUrl.String(), nil)
	}, &res); err != nil {
		return err
	}
	if res.IsUnregistered {
		if res.SessionID != s.session.SessionID {
			return fmt.Errorf("unregister: session id does not match")
		}
		return nil
	}
	return fmt.Errorf("unregister: not found session id to unregister")
}

func (s *ManualIncrementSession) ClientSession() Session {
	return s.session
}

func (s *ManualIncrementSession) Response(sequenceNumber uint64) {

}

func (s *ManualIncrementSession) SetSequenceNumber(sequenceNumber uint64) {
	s.session.SequenceNumber = sequenceNumber
}
