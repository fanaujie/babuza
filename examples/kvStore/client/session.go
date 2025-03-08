package client

import (
	"context"
	"github.com/fanaujie/babuza/examples/kvStore/server/response"
	"net/http"
	"net/url"
)

type ISession interface {
	Register(lb *proxy, httpSessionPath string) error
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

func (s *AutoIncrementSession) Register(lb *proxy, httpSessionPath string) error {
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

func (s *NoOpSession) Register(*proxy, string) error {
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

func (s *ManualIncrementSession) Register(lb *proxy, httpSessionPath string) error {
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

func (s *ManualIncrementSession) ClientSession() Session {
	return s.session
}

func (s *ManualIncrementSession) Response(sequenceNumber uint64) {

}

func (s *ManualIncrementSession) SetSequenceNumber(sequenceNumber uint64) {
	s.session.SequenceNumber = sequenceNumber
}
