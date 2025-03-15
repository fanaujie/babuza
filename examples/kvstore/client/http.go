package client

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
)

func newHttpClient(enableTLS bool) *http.Client {
	var roundTrip http.RoundTripper

	if enableTLS == false {
		roundTrip = &http.Transport{
			DialContext: func(ctx context.Context, network string, addr string) (net.Conn, error) {
				return net.Dial(network, addr)
			},
		}
	} else {
		roundTrip = &http.Transport{
			DialTLSContext: func(ctx context.Context, network string, addr string) (net.Conn, error) {
				return tls.Dial(network, addr, &tls.Config{InsecureSkipVerify: true})
			},
		}
	}
	return &http.Client{
		Transport: roundTrip,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}
